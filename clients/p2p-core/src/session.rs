use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex};

use crate::direct::{establish_direct_link, DirectLink, DirectLinkInfo};
use crate::nat::{prepare_connect_info, ConnectInfo, PreparedConnectInfo};
use crate::p2p_proto::{
    read_file_stream_header, read_p2p_message, write_file_stream_header, write_p2p_message,
    FileStreamHeader, P2pMessage,
};
use crate::signaling::{SignalingClient, SignalingCommand, SignalingEnvelope, SignalingRole};
use crate::transfer::{
    hash_file, metadata_for_path, open_chunk_sink, open_chunk_source, part_path, read_chunk_from,
    validate_offer_metadata, write_chunk_to, ChunkRange, FileMetadata, RangeSet, RawChunk,
    TransferDirection, TransferManifest, TransferStatus, TransferStore,
};

#[derive(Debug, Clone)]
pub enum SessionRole {
    /// 房主不携带房间码：码由服务器在 room-ready 中分配
    Host,
    Guest {
        room_code: String,
    },
}

#[derive(Debug, Clone)]
pub struct FileTransferProgress {
    pub transfer_id: String,
    pub file_name: String,
    pub direction: TransferDirection,
    pub status: TransferStatus,
    pub completed_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Debug)]
pub enum SessionEvent {
    Connected,
    /// 信令 WebSocket 已关闭；若直连已建立则聊天不受影响，仅无法重试直连
    SignalingClosed,
    /// 服务器在 room-ready 中下发的权威房间码
    RoomCodeAssigned(String),
    LocalCandidatesCollected(ConnectInfo),
    PeerCandidatesReceived(ConnectInfo),
    DirectLinkEstablished(DirectLinkInfo),
    DirectLinkFailed(String),
    DirectLinkLost(String),
    PeerConnected,
    PeerDisconnected,
    MessageReceived(String),
    FileOffered(FileMetadata),
    FileProgress(FileTransferProgress),
    FileCompleted {
        transfer_id: String,
        file_name: String,
        path: Option<PathBuf>,
    },
    FileFailed {
        transfer_id: String,
        file_name: String,
        message: String,
    },
    FileCancelled {
        transfer_id: String,
        file_name: String,
        reason: String,
    },
    Error(String),
}

#[derive(Debug)]
pub struct ChatSession {
    role: SessionRole,
    signaling_url: String,
}

#[derive(Clone, Debug)]
pub struct ChatSessionHandle {
    signaling_tx: mpsc::Sender<SignalingCommand>,
    direct_tx: mpsc::Sender<DirectCommand>,
    session_tx: mpsc::Sender<SessionCommand>,
}

#[derive(Debug)]
enum DirectCommand {
    Chat(String),
    OfferFile(PathBuf),
    AcceptFile {
        transfer_id: String,
        save_path: PathBuf,
    },
    RejectFile {
        transfer_id: String,
        reason: String,
    },
    PauseTransfer(String),
    ResumeTransfer(String),
    CancelTransfer {
        transfer_id: String,
        reason: String,
    },
}

#[derive(Debug)]
enum SessionCommand {
    RetryDirect,
}

struct TransferState {
    store: TransferStore,
    pending_offers: HashMap<String, FileMetadata>,
    senders: HashMap<String, TransferManifest>,
    receivers: HashMap<String, TransferManifest>,
    cancelled: HashSet<String>,
}

impl TransferState {
    fn new(store: TransferStore) -> Self {
        Self {
            store,
            pending_offers: HashMap::new(),
            senders: HashMap::new(),
            receivers: HashMap::new(),
            cancelled: HashSet::new(),
        }
    }
}

type SharedTransferState = Arc<Mutex<TransferState>>;

impl ChatSession {
    pub fn new(role: SessionRole, signaling_url: String) -> Self {
        Self {
            role,
            signaling_url,
        }
    }

    pub async fn start(self, event_tx: mpsc::Sender<SessionEvent>) -> Result<ChatSessionHandle> {
        let (signaling_tx, signaling_rx) = mpsc::channel::<SignalingCommand>(128);
        let (incoming_tx, mut incoming_rx) = mpsc::channel::<String>(128);
        let (direct_tx, direct_rx) = mpsc::channel::<DirectCommand>(128);
        let (direct_link_tx, direct_link_rx) = mpsc::channel::<DirectLink>(1);
        let (session_tx, mut session_rx) = mpsc::channel::<SessionCommand>(8);

        let client = SignalingClient::new(self.signaling_url.clone());
        let events = event_tx.clone();
        tokio::spawn(async move {
            if let Err(error) = client.connect(signaling_rx, incoming_tx).await {
                let _ = events.send(SessionEvent::Error(format!("{error:#}"))).await;
            }
        });

        tokio::spawn(run_direct_manager(
            direct_rx,
            direct_link_rx,
            event_tx.clone(),
        ));

        let dispatch_events = event_tx.clone();
        let dispatch_signaling_tx = signaling_tx.clone();
        let signaling_role = signaling_role_for_session(&self.role);
        tokio::spawn(async move {
            let mut peer_seen = false;
            let mut connect_info_sent = false;
            let mut local_direct = None;
            let mut peer_direct: Option<ConnectInfo> = None;
            let mut direct_started = false;
            let (direct_ready_tx, mut direct_ready_rx) = mpsc::channel::<PreparedConnectInfo>(1);

            loop {
                tokio::select! {
                    command = session_rx.recv() => {
                        // 命令通道关闭说明会话句柄已被丢弃，结束分发循环
                        let Some(SessionCommand::RetryDirect) = command else {
                            break;
                        };
                        connect_info_sent = false;
                        local_direct = None;
                        peer_direct = None;
                        direct_started = false;
                        announce_connect_info_once(
                            &mut connect_info_sent,
                            signaling_role.clone(),
                            dispatch_signaling_tx.clone(),
                            dispatch_events.clone(),
                            direct_ready_tx.clone(),
                        );
                    }
                    prepared = direct_ready_rx.recv() => {
                        let Some(prepared) = prepared else {
                            continue;
                        };
                        local_direct = Some(prepared);
                        start_direct_link_once(
                            &mut direct_started,
                            &mut local_direct,
                            peer_direct.clone(),
                            direct_link_tx.clone(),
                            dispatch_events.clone(),
                        );
                    }
                    raw = incoming_rx.recv() => {
                        let Some(raw) = raw else {
                            break;
                        };
                        match serde_json::from_str::<SignalingEnvelope>(&raw) {
                            Ok(SignalingEnvelope::PeerJoined { .. }) => {
                                if should_announce_peer(&mut peer_seen) {
                                    let _ = dispatch_events.send(SessionEvent::PeerConnected).await;
                                }
                                announce_connect_info_once(
                                    &mut connect_info_sent,
                                    signaling_role.clone(),
                                    dispatch_signaling_tx.clone(),
                                    dispatch_events.clone(),
                                    direct_ready_tx.clone(),
                                );
                            }
                            Ok(SignalingEnvelope::PeerLeft { .. }) => {
                                peer_seen = false;
                                connect_info_sent = false;
                                local_direct = None;
                                peer_direct = None;
                                direct_started = false;
                                let _ = dispatch_events.send(SessionEvent::PeerDisconnected).await;
                            }
                            Ok(SignalingEnvelope::Error { message }) => {
                                let _ = dispatch_events.send(SessionEvent::Error(message)).await;
                            }
                            Ok(SignalingEnvelope::RoomReady { room_code }) => {
                                if let Some(code) = room_code {
                                    let _ = dispatch_events
                                        .send(SessionEvent::RoomCodeAssigned(code))
                                        .await;
                                }
                                let _ = dispatch_events.send(SessionEvent::Connected).await;
                                if signaling_role == SignalingRole::Guest {
                                    announce_connect_info_once(
                                        &mut connect_info_sent,
                                        signaling_role.clone(),
                                        dispatch_signaling_tx.clone(),
                                        dispatch_events.clone(),
                                        direct_ready_tx.clone(),
                                    );
                                }
                            }
                            Ok(SignalingEnvelope::Signal { payload }) => {
                                if let Ok(info) = serde_json::from_value::<ConnectInfo>(payload) {
                                    if info.is_supported() {
                                        if should_announce_peer(&mut peer_seen) {
                                            let _ = dispatch_events.send(SessionEvent::PeerConnected).await;
                                        }

                                        let new_attempt = peer_direct
                                            .as_ref()
                                            .map(|old| {
                                                old.pairing_token != info.pairing_token
                                                    || old.cert_hash != info.cert_hash
                                            })
                                            .unwrap_or(false);
                                        if new_attempt {
                                            connect_info_sent = false;
                                            local_direct = None;
                                            direct_started = false;
                                        }

                                        peer_direct = Some(info.clone());
                                        let _ = dispatch_events
                                            .send(SessionEvent::PeerCandidatesReceived(info))
                                            .await;

                                        if new_attempt {
                                            announce_connect_info_once(
                                                &mut connect_info_sent,
                                                signaling_role.clone(),
                                                dispatch_signaling_tx.clone(),
                                                dispatch_events.clone(),
                                                direct_ready_tx.clone(),
                                            );
                                        }

                                        start_direct_link_once(
                                            &mut direct_started,
                                            &mut local_direct,
                                            peer_direct.clone(),
                                            direct_link_tx.clone(),
                                            dispatch_events.clone(),
                                        );
                                    }
                                }
                            }
                            Ok(SignalingEnvelope::Hello { .. }) | Ok(SignalingEnvelope::Bye) => {}
                            Err(_) => {}
                        }
                    }
                }
            }

            let _ = dispatch_events.send(SessionEvent::SignalingClosed).await;
        });

        let hello = match self.role {
            SessionRole::Host => SignalingEnvelope::Hello {
                role: SignalingRole::Host,
                room_code: None,
            },
            SessionRole::Guest { room_code } => SignalingEnvelope::Hello {
                role: SignalingRole::Guest,
                room_code: Some(room_code),
            },
        };

        signaling_tx
            .send(SignalingCommand::SendText(serde_json::to_string(&hello)?))
            .await?;

        Ok(ChatSessionHandle {
            signaling_tx,
            direct_tx,
            session_tx,
        })
    }
}

fn signaling_role_for_session(role: &SessionRole) -> SignalingRole {
    match role {
        SessionRole::Host => SignalingRole::Host,
        SessionRole::Guest { .. } => SignalingRole::Guest,
    }
}

fn announce_connect_info_once(
    sent: &mut bool,
    role: SignalingRole,
    signaling_tx: mpsc::Sender<SignalingCommand>,
    event_tx: mpsc::Sender<SessionEvent>,
    direct_ready_tx: mpsc::Sender<PreparedConnectInfo>,
) {
    if *sent {
        return;
    }
    *sent = true;

    tokio::spawn(async move {
        match prepare_connect_info(role).await {
            Ok(prepared) => {
                let info = prepared.info.clone();
                let payload = match serde_json::to_value(&info) {
                    Ok(payload) => payload,
                    Err(error) => {
                        let _ = event_tx
                            .send(SessionEvent::Error(format!(
                                "候选信息序列化失败：{error:#}"
                            )))
                            .await;
                        return;
                    }
                };
                let envelope = SignalingEnvelope::Signal { payload };
                match serde_json::to_string(&envelope) {
                    Ok(text) => {
                        let _ = event_tx
                            .send(SessionEvent::LocalCandidatesCollected(info))
                            .await;
                        if let Err(error) =
                            signaling_tx.send(SignalingCommand::SendText(text)).await
                        {
                            let _ = event_tx
                                .send(SessionEvent::Error(format!("发送候选信息失败：{error}")))
                                .await;
                            return;
                        }
                        let _ = direct_ready_tx.send(prepared).await;
                    }
                    Err(error) => {
                        let _ = event_tx
                            .send(SessionEvent::Error(format!(
                                "候选信令序列化失败：{error:#}"
                            )))
                            .await;
                    }
                }
            }
            Err(error) => {
                let _ = event_tx
                    .send(SessionEvent::Error(format!("候选收集失败：{error:#}")))
                    .await;
            }
        }
    });
}

fn start_direct_link_once(
    started: &mut bool,
    local: &mut Option<PreparedConnectInfo>,
    peer: Option<ConnectInfo>,
    direct_link_tx: mpsc::Sender<DirectLink>,
    event_tx: mpsc::Sender<SessionEvent>,
) {
    if *started {
        return;
    }

    let Some(prepared) = local.take() else {
        return;
    };
    let Some(peer) = peer else {
        *local = Some(prepared);
        return;
    };

    *started = true;
    tokio::spawn(async move {
        match establish_direct_link(prepared, peer).await {
            Ok(link) => {
                let _ = direct_link_tx.send(link).await;
            }
            Err(error) => {
                let _ = event_tx
                    .send(SessionEvent::DirectLinkFailed(format!("{error:#}")))
                    .await;
            }
        }
    });
}

async fn run_direct_manager(
    mut command_rx: mpsc::Receiver<DirectCommand>,
    mut link_rx: mpsc::Receiver<DirectLink>,
    event_tx: mpsc::Sender<SessionEvent>,
) {
    while let Some(link) = link_rx.recv().await {
        run_direct_link(link, &mut command_rx, event_tx.clone()).await;
    }
}

async fn run_direct_link(
    link: DirectLink,
    command_rx: &mut mpsc::Receiver<DirectCommand>,
    event_tx: mpsc::Sender<SessionEvent>,
) {
    let info = link.info().clone();
    let parts = link.into_parts();
    let connection = parts.connection;
    let endpoint = parts.endpoint;
    let mut send = parts.control_send;
    let mut recv = parts.control_recv;
    let mut ping = tokio::time::interval(Duration::from_secs(20));
    let store = TransferStore::platform_default().unwrap_or_else(|_| {
        TransferStore::new(std::env::temp_dir().join("p2p-signaling").join("transfers"))
    });
    let transfer_state = Arc::new(Mutex::new(TransferState::new(store)));
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<P2pMessage>(128);
    let uni_task = spawn_uni_stream_receiver(
        connection.clone(),
        transfer_state.clone(),
        outbound_tx.clone(),
        event_tx.clone(),
    );

    let _ = event_tx
        .send(SessionEvent::DirectLinkEstablished(info))
        .await;

    loop {
        tokio::select! {
            command = command_rx.recv() => {
                match command {
                    Some(DirectCommand::Chat(text)) => {
                        let _ = outbound_tx.send(P2pMessage::Chat { text }).await;
                    }
                    Some(DirectCommand::OfferFile(path)) => {
                        if let Err(error) = offer_file(path, transfer_state.clone(), outbound_tx.clone(), event_tx.clone()).await {
                            let _ = event_tx.send(SessionEvent::Error(format!("{error:#}"))).await;
                        }
                    }
                    Some(DirectCommand::AcceptFile { transfer_id, save_path }) => {
                        if let Err(error) = accept_file_offer(transfer_id, save_path, transfer_state.clone(), outbound_tx.clone(), event_tx.clone()).await {
                            let _ = event_tx.send(SessionEvent::Error(format!("{error:#}"))).await;
                        }
                    }
                    Some(DirectCommand::RejectFile { transfer_id, reason }) => {
                        if let Err(error) = reject_file_offer(transfer_id, reason, transfer_state.clone(), outbound_tx.clone(), event_tx.clone()).await {
                            let _ = event_tx.send(SessionEvent::Error(format!("{error:#}"))).await;
                        }
                    }
                    Some(DirectCommand::PauseTransfer(transfer_id)) => {
                        if let Err(error) = pause_transfer(transfer_id, transfer_state.clone(), outbound_tx.clone(), event_tx.clone()).await {
                            let _ = event_tx.send(SessionEvent::Error(format!("{error:#}"))).await;
                        }
                    }
                    Some(DirectCommand::ResumeTransfer(transfer_id)) => {
                        if let Err(error) = resume_transfer(transfer_id, transfer_state.clone(), outbound_tx.clone(), event_tx.clone()).await {
                            let _ = event_tx.send(SessionEvent::Error(format!("{error:#}"))).await;
                        }
                    }
                    Some(DirectCommand::CancelTransfer { transfer_id, reason }) => {
                        if let Err(error) = cancel_transfer(transfer_id, reason, transfer_state.clone(), outbound_tx.clone(), event_tx.clone()).await {
                            let _ = event_tx.send(SessionEvent::Error(format!("{error:#}"))).await;
                        }
                    }
                    None => {
                        connection.close(0_u32.into(), b"session closed");
                        break;
                    }
                }
            }
            outbound = outbound_rx.recv() => {
                let Some(message) = outbound else {
                    break;
                };
                if let Err(error) = write_p2p_message(&mut send, &message).await {
                    let _ = event_tx.send(SessionEvent::DirectLinkLost(format!("{error:#}"))).await;
                    break;
                }
            }
            message = read_p2p_message(&mut recv) => {
                match message {
                    Ok(P2pMessage::Chat { text }) => {
                        let _ = event_tx.send(SessionEvent::MessageReceived(text)).await;
                    }
                    Ok(P2pMessage::Ping) => {
                        let _ = outbound_tx.send(P2pMessage::Pong).await;
                    }
                    Ok(P2pMessage::Pong) => {}
                    Ok(P2pMessage::FileOffer { metadata }) => {
                        if let Err(error) = receive_file_offer(metadata, transfer_state.clone(), event_tx.clone()).await {
                            let _ = event_tx.send(SessionEvent::Error(format!("{error:#}"))).await;
                        }
                    }
                    Ok(P2pMessage::FileAccept { transfer_id, completed_chunks })
                    | Ok(P2pMessage::FileResume { transfer_id, completed_chunks }) => {
                        start_sending_file(
                            transfer_id,
                            completed_chunks,
                            connection.clone(),
                            transfer_state.clone(),
                            outbound_tx.clone(),
                            event_tx.clone(),
                        );
                    }
                    Ok(P2pMessage::FileReject { transfer_id, reason })
                    | Ok(P2pMessage::FileCancel { transfer_id, reason }) => {
                        if let Err(error) = mark_transfer_cancelled(transfer_id, reason, transfer_state.clone(), event_tx.clone()).await {
                            let _ = event_tx.send(SessionEvent::Error(format!("{error:#}"))).await;
                        }
                    }
                    Ok(P2pMessage::FileAck { transfer_id, chunks }) => {
                        if let Err(error) = acknowledge_sent_chunks(transfer_id, chunks, transfer_state.clone(), event_tx.clone()).await {
                            let _ = event_tx.send(SessionEvent::Error(format!("{error:#}"))).await;
                        }
                    }
                    Ok(P2pMessage::FileComplete { transfer_id }) => {
                        if let Err(error) = complete_sent_transfer(transfer_id, transfer_state.clone(), event_tx.clone()).await {
                            let _ = event_tx.send(SessionEvent::Error(format!("{error:#}"))).await;
                        }
                    }
                    Ok(P2pMessage::Hello { .. }) => {
                        let _ = event_tx
                            .send(SessionEvent::DirectLinkLost("直连控制流收到重复 Hello".into()))
                            .await;
                        break;
                    }
                    Err(error) => {
                        let _ = event_tx.send(SessionEvent::DirectLinkLost(format!("{error:#}"))).await;
                        break;
                    }
                }
            }
            _ = ping.tick() => {
                if let Err(error) = write_p2p_message(&mut send, &P2pMessage::Ping).await {
                    let _ = event_tx.send(SessionEvent::DirectLinkLost(format!("{error:#}"))).await;
                    break;
                }
            }
            reason = connection.closed() => {
                let _ = event_tx.send(SessionEvent::DirectLinkLost(reason.to_string())).await;
                break;
            }
        }
    }

    uni_task.abort();
    connection.close(0_u32.into(), b"direct link stopped");
    endpoint.wait_idle().await;
}

async fn offer_file(
    path: PathBuf,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let metadata = metadata_for_path(&path).await?;
    let manifest = TransferManifest::new_sender(metadata.clone(), path);

    {
        let mut state = state.lock().await;
        state.store.save(&manifest).await?;
        state
            .senders
            .insert(manifest.metadata.transfer_id.clone(), manifest.clone());
        state.cancelled.remove(&manifest.metadata.transfer_id);
    }

    send_progress(&event_tx, &manifest).await;
    outbound_tx.send(P2pMessage::FileOffer { metadata }).await?;
    Ok(())
}

async fn receive_file_offer(
    mut metadata: FileMetadata,
    state: SharedTransferState,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    validate_offer_metadata(&mut metadata).map_err(anyhow::Error::msg)?;
    {
        let mut state = state.lock().await;
        state
            .pending_offers
            .insert(metadata.transfer_id.clone(), metadata.clone());
    }
    let _ = event_tx.send(SessionEvent::FileOffered(metadata)).await;
    Ok(())
}

async fn accept_file_offer(
    transfer_id: String,
    save_path: PathBuf,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let manifest = {
        let mut state = state.lock().await;
        let metadata = state
            .pending_offers
            .remove(&transfer_id)
            .ok_or_else(|| anyhow::anyhow!("找不到待接收文件：{transfer_id}"))?;
        let mut manifest = match state.store.load(&transfer_id).await? {
            Some(existing)
                if existing.direction == TransferDirection::Receive
                    && existing.metadata.file_hash == metadata.file_hash =>
            {
                let mut existing = existing;
                existing.status = TransferStatus::Accepted;
                existing.output_path = Some(save_path.clone());
                existing.temp_path = Some(part_path(&save_path));
                existing
            }
            _ => TransferManifest::new_receiver(metadata, save_path),
        };
        manifest.status = TransferStatus::Accepted;
        state.store.save(&manifest).await?;
        state
            .receivers
            .insert(manifest.metadata.transfer_id.clone(), manifest.clone());
        state.cancelled.remove(&transfer_id);
        manifest
    };

    send_progress(&event_tx, &manifest).await;

    if manifest.metadata.total_chunks == 0 {
        complete_empty_receiver(manifest, state, outbound_tx, event_tx).await?;
        return Ok(());
    }

    outbound_tx
        .send(P2pMessage::FileAccept {
            transfer_id,
            completed_chunks: manifest.completed_chunks.clone(),
        })
        .await?;
    Ok(())
}

async fn complete_empty_receiver(
    mut manifest: TransferManifest,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let output_path = manifest
        .output_path
        .clone()
        .ok_or_else(|| anyhow::anyhow!("接收文件缺少保存路径"))?;
    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&output_path, []).await?;
    manifest.status = TransferStatus::Complete;
    {
        let mut state = state.lock().await;
        state.store.save(&manifest).await?;
        state
            .receivers
            .insert(manifest.metadata.transfer_id.clone(), manifest.clone());
    }
    send_progress(&event_tx, &manifest).await;
    let _ = event_tx
        .send(SessionEvent::FileCompleted {
            transfer_id: manifest.metadata.transfer_id.clone(),
            file_name: manifest.metadata.file_name.clone(),
            path: Some(output_path),
        })
        .await;
    outbound_tx
        .send(P2pMessage::FileComplete {
            transfer_id: manifest.metadata.transfer_id,
        })
        .await?;
    Ok(())
}

async fn reject_file_offer(
    transfer_id: String,
    reason: String,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let file_name = {
        let mut state = state.lock().await;
        state
            .pending_offers
            .remove(&transfer_id)
            .map(|metadata| metadata.file_name)
            .unwrap_or_else(|| transfer_id.clone())
    };
    let _ = event_tx
        .send(SessionEvent::FileCancelled {
            transfer_id: transfer_id.clone(),
            file_name,
            reason: reason.clone(),
        })
        .await;
    outbound_tx
        .send(P2pMessage::FileReject {
            transfer_id,
            reason,
        })
        .await?;
    Ok(())
}

async fn pause_transfer(
    transfer_id: String,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    update_transfer_status(
        &transfer_id,
        TransferStatus::Paused,
        state.clone(),
        event_tx,
    )
    .await?;
    {
        let mut state = state.lock().await;
        state.cancelled.insert(transfer_id.clone());
    }
    outbound_tx
        .send(P2pMessage::FileCancel {
            transfer_id,
            reason: "用户暂停".into(),
        })
        .await?;
    Ok(())
}

async fn resume_transfer(
    transfer_id: String,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let message = {
        let mut state = state.lock().await;
        state.cancelled.remove(&transfer_id);
        if let Some(manifest) = state.receivers.get_mut(&transfer_id) {
            manifest.status = TransferStatus::Accepted;
            let manifest = manifest.clone();
            state.store.save(&manifest).await?;
            send_progress(&event_tx, &manifest).await;
            Some(P2pMessage::FileResume {
                transfer_id: transfer_id.clone(),
                completed_chunks: manifest.completed_chunks,
            })
        } else if let Some(manifest) = state.senders.get_mut(&transfer_id) {
            manifest.status = TransferStatus::Offered;
            let manifest = manifest.clone();
            state.store.save(&manifest).await?;
            send_progress(&event_tx, &manifest).await;
            Some(P2pMessage::FileOffer {
                metadata: manifest.metadata,
            })
        } else {
            None
        }
    };

    let Some(message) = message else {
        anyhow::bail!("找不到可继续的传输：{transfer_id}");
    };
    outbound_tx.send(message).await?;
    Ok(())
}

async fn cancel_transfer(
    transfer_id: String,
    reason: String,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    mark_transfer_cancelled(transfer_id.clone(), reason.clone(), state, event_tx).await?;
    outbound_tx
        .send(P2pMessage::FileCancel {
            transfer_id,
            reason,
        })
        .await?;
    Ok(())
}

async fn update_transfer_status(
    transfer_id: &str,
    status: TransferStatus,
    state: SharedTransferState,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let manifest = {
        let mut state = state.lock().await;
        if let Some(manifest) = state.senders.get_mut(transfer_id) {
            manifest.status = status.clone();
            Some(manifest.clone())
        } else if let Some(manifest) = state.receivers.get_mut(transfer_id) {
            manifest.status = status;
            Some(manifest.clone())
        } else {
            None
        }
    };

    let Some(manifest) = manifest else {
        anyhow::bail!("找不到传输：{transfer_id}");
    };
    {
        let state = state.lock().await;
        state.store.save(&manifest).await?;
    }
    send_progress(&event_tx, &manifest).await;
    Ok(())
}

fn start_sending_file(
    transfer_id: String,
    completed_chunks: Vec<ChunkRange>,
    connection: quinn::Connection,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) {
    tokio::spawn(async move {
        if let Err(error) = send_file_ranges(
            transfer_id.clone(),
            completed_chunks,
            connection,
            state.clone(),
            outbound_tx.clone(),
            event_tx.clone(),
        )
        .await
        {
            let _ = mark_transfer_failed(
                transfer_id.clone(),
                format!("{error:#}"),
                state.clone(),
                event_tx.clone(),
            )
            .await;
            let _ = outbound_tx
                .send(P2pMessage::FileCancel {
                    transfer_id,
                    reason: format!("{error:#}"),
                })
                .await;
        }
    });
}

async fn send_file_ranges(
    transfer_id: String,
    completed_chunks: Vec<ChunkRange>,
    connection: quinn::Connection,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let (manifest, source_path, missing_ranges) = {
        let mut state = state.lock().await;
        state.cancelled.remove(&transfer_id);
        let manifest = state
            .senders
            .get_mut(&transfer_id)
            .ok_or_else(|| anyhow::anyhow!("找不到待发送文件：{transfer_id}"))?;
        manifest.status = TransferStatus::Accepted;
        let source_path = manifest
            .source_path
            .clone()
            .ok_or_else(|| anyhow::anyhow!("发送文件缺少源路径"))?;
        let remote_completed = RangeSet::from_ranges(completed_chunks);
        let missing_ranges = remote_completed.missing_ranges(manifest.metadata.total_chunks);
        let manifest = manifest.clone();
        state.store.save(&manifest).await?;
        (manifest, source_path, missing_ranges)
    };

    send_progress(&event_tx, &manifest).await;

    if missing_ranges.is_empty() {
        outbound_tx
            .send(P2pMessage::FileComplete {
                transfer_id: transfer_id.clone(),
            })
            .await?;
        return Ok(());
    }

    let mut source = open_chunk_source(&source_path).await?;
    for range in missing_ranges {
        ensure_not_cancelled(&transfer_id, &state).await?;
        let mut stream = connection.open_uni().await?;
        write_file_stream_header(
            &mut stream,
            &FileStreamHeader {
                transfer_id: transfer_id.clone(),
                start_chunk: range.start,
                end_chunk: range.end,
            },
        )
        .await?;

        for index in range.start..range.end {
            ensure_not_cancelled(&transfer_id, &state).await?;
            let chunk = read_chunk_from(&mut source, index, manifest.metadata.chunk_size).await?;
            stream.write_all(&chunk.bytes).await?;
            let _ = event_tx
                .send(SessionEvent::FileProgress(FileTransferProgress {
                    transfer_id: transfer_id.clone(),
                    file_name: manifest.metadata.file_name.clone(),
                    direction: TransferDirection::Send,
                    status: TransferStatus::Accepted,
                    completed_bytes: (chunk.offset + chunk.bytes.len() as u64)
                        .min(manifest.metadata.file_size),
                    total_bytes: manifest.metadata.file_size,
                }))
                .await;
        }
        stream.finish()?;
    }

    Ok(())
}

async fn ensure_not_cancelled(transfer_id: &str, state: &SharedTransferState) -> Result<()> {
    if state.lock().await.cancelled.contains(transfer_id) {
        anyhow::bail!("传输已暂停或取消：{transfer_id}");
    }
    Ok(())
}

fn spawn_uni_stream_receiver(
    connection: quinn::Connection,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let stream = match connection.accept_uni().await {
                Ok(stream) => stream,
                Err(_) => break,
            };
            tokio::spawn(receive_file_stream(
                stream,
                state.clone(),
                outbound_tx.clone(),
                event_tx.clone(),
            ));
        }
    })
}

async fn receive_file_stream(
    mut stream: quinn::RecvStream,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) {
    if let Err(error) = receive_file_stream_inner(
        &mut stream,
        state.clone(),
        outbound_tx.clone(),
        event_tx.clone(),
    )
    .await
    {
        let _ = event_tx
            .send(SessionEvent::Error(format!("接收文件流失败：{error:#}")))
            .await;
    }
}

async fn receive_file_stream_inner(
    stream: &mut quinn::RecvStream,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let header = read_file_stream_header(stream).await?;
    if header.start_chunk >= header.end_chunk {
        anyhow::bail!("文件流分块范围无效");
    }

    let manifest = {
        let state = state.lock().await;
        state
            .receivers
            .get(&header.transfer_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("收到未接受的文件流：{}", header.transfer_id))?
    };
    if header.end_chunk > manifest.metadata.total_chunks {
        anyhow::bail!("文件流分块范围越界");
    }

    let temp_path = manifest
        .temp_path
        .clone()
        .ok_or_else(|| anyhow::anyhow!("接收文件缺少临时路径"))?;
    let mut sink = open_chunk_sink(&temp_path).await?;

    for index in header.start_chunk..header.end_chunk {
        ensure_not_cancelled(&header.transfer_id, &state).await?;
        let expected_len = expected_chunk_len(&manifest.metadata, index)?;
        let offset = index.saturating_mul(manifest.metadata.chunk_size);
        let mut bytes = vec![0_u8; expected_len];
        stream.read_exact(&mut bytes).await?;
        write_chunk_to(
            &mut sink,
            &RawChunk {
                index,
                offset,
                bytes,
            },
            manifest.metadata.chunk_size,
        )
        .await?;

        let updated = record_received_chunk(&header.transfer_id, index, state.clone()).await?;
        send_progress(&event_tx, &updated).await;
        outbound_tx
            .send(P2pMessage::FileAck {
                transfer_id: header.transfer_id.clone(),
                chunks: vec![ChunkRange::new(index, index + 1)],
            })
            .await?;
    }

    sink.flush().await?;
    finalize_received_transfer(header.transfer_id, state, outbound_tx, event_tx).await?;
    Ok(())
}

fn expected_chunk_len(metadata: &FileMetadata, index: u64) -> Result<usize> {
    if index >= metadata.total_chunks {
        anyhow::bail!("chunk {index} 超出文件范围");
    }
    let offset = index.saturating_mul(metadata.chunk_size);
    let remaining = metadata.file_size.saturating_sub(offset);
    Ok(remaining.min(metadata.chunk_size) as usize)
}

async fn record_received_chunk(
    transfer_id: &str,
    index: u64,
    state: SharedTransferState,
) -> Result<TransferManifest> {
    let mut state = state.lock().await;
    let manifest = state
        .receivers
        .get_mut(transfer_id)
        .ok_or_else(|| anyhow::anyhow!("找不到接收中的文件：{transfer_id}"))?;
    let mut completed = manifest.completed_set();
    completed.insert(index);
    manifest.completed_chunks = completed.into_ranges();
    manifest.status = TransferStatus::Accepted;
    let manifest = manifest.clone();
    state.store.save(&manifest).await?;
    Ok(manifest)
}

async fn finalize_received_transfer(
    transfer_id: String,
    state: SharedTransferState,
    outbound_tx: mpsc::Sender<P2pMessage>,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let manifest = {
        let state = state.lock().await;
        let Some(manifest) = state.receivers.get(&transfer_id) else {
            return Ok(());
        };
        if manifest.status == TransferStatus::Complete || !manifest.is_complete() {
            return Ok(());
        }
        manifest.clone()
    };

    let temp_path = manifest
        .temp_path
        .clone()
        .ok_or_else(|| anyhow::anyhow!("接收文件缺少临时路径"))?;
    let output_path = manifest
        .output_path
        .clone()
        .ok_or_else(|| anyhow::anyhow!("接收文件缺少保存路径"))?;
    let actual_hash = hash_file(&temp_path).await?;
    if actual_hash != manifest.metadata.file_hash {
        mark_transfer_failed(
            transfer_id.clone(),
            "文件哈希校验失败".into(),
            state,
            event_tx,
        )
        .await?;
        return Ok(());
    }

    if tokio::fs::try_exists(&output_path).await? {
        tokio::fs::remove_file(&output_path).await?;
    }
    tokio::fs::rename(&temp_path, &output_path).await?;

    let completed = {
        let mut state = state.lock().await;
        let manifest = state
            .receivers
            .get_mut(&transfer_id)
            .ok_or_else(|| anyhow::anyhow!("找不到接收中的文件：{transfer_id}"))?;
        manifest.status = TransferStatus::Complete;
        let manifest = manifest.clone();
        state.store.save(&manifest).await?;
        manifest
    };

    send_progress(&event_tx, &completed).await;
    let _ = event_tx
        .send(SessionEvent::FileCompleted {
            transfer_id: transfer_id.clone(),
            file_name: completed.metadata.file_name.clone(),
            path: Some(output_path),
        })
        .await;
    outbound_tx
        .send(P2pMessage::FileComplete { transfer_id })
        .await?;
    Ok(())
}

async fn acknowledge_sent_chunks(
    transfer_id: String,
    chunks: Vec<ChunkRange>,
    state: SharedTransferState,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let manifest = {
        let mut state = state.lock().await;
        let manifest = state
            .senders
            .get_mut(&transfer_id)
            .ok_or_else(|| anyhow::anyhow!("找不到发送中的文件：{transfer_id}"))?;
        let mut completed = manifest.completed_set();
        for range in chunks {
            completed.insert_range(range);
        }
        manifest.completed_chunks = completed.into_ranges();
        manifest.status = TransferStatus::Accepted;
        let manifest = manifest.clone();
        state.store.save(&manifest).await?;
        manifest
    };
    send_progress(&event_tx, &manifest).await;
    Ok(())
}

async fn complete_sent_transfer(
    transfer_id: String,
    state: SharedTransferState,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let manifest = {
        let mut state = state.lock().await;
        let manifest = state
            .senders
            .get_mut(&transfer_id)
            .ok_or_else(|| anyhow::anyhow!("找不到发送中的文件：{transfer_id}"))?;
        manifest.status = TransferStatus::Complete;
        let manifest = manifest.clone();
        state.store.save(&manifest).await?;
        manifest
    };
    send_progress(&event_tx, &manifest).await;
    let _ = event_tx
        .send(SessionEvent::FileCompleted {
            transfer_id,
            file_name: manifest.metadata.file_name.clone(),
            path: manifest.source_path.clone(),
        })
        .await;
    Ok(())
}

async fn mark_transfer_cancelled(
    transfer_id: String,
    reason: String,
    state: SharedTransferState,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let manifest = {
        let mut state = state.lock().await;
        state.cancelled.insert(transfer_id.clone());
        state.pending_offers.remove(&transfer_id);
        if let Some(manifest) = state.senders.get_mut(&transfer_id) {
            manifest.status = TransferStatus::Cancelled;
            Some(manifest.clone())
        } else if let Some(manifest) = state.receivers.get_mut(&transfer_id) {
            manifest.status = TransferStatus::Cancelled;
            Some(manifest.clone())
        } else {
            None
        }
    };

    if let Some(manifest) = manifest {
        {
            let state = state.lock().await;
            state.store.save(&manifest).await?;
        }
        send_progress(&event_tx, &manifest).await;
        let _ = event_tx
            .send(SessionEvent::FileCancelled {
                transfer_id,
                file_name: manifest.metadata.file_name,
                reason,
            })
            .await;
    }
    Ok(())
}

async fn mark_transfer_failed(
    transfer_id: String,
    message: String,
    state: SharedTransferState,
    event_tx: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let manifest = {
        let mut state = state.lock().await;
        if let Some(manifest) = state.senders.get_mut(&transfer_id) {
            manifest.status = TransferStatus::Failed;
            manifest.failure = Some(message.clone());
            Some(manifest.clone())
        } else if let Some(manifest) = state.receivers.get_mut(&transfer_id) {
            manifest.status = TransferStatus::Failed;
            manifest.failure = Some(message.clone());
            Some(manifest.clone())
        } else {
            None
        }
    };

    if let Some(manifest) = manifest {
        {
            let state = state.lock().await;
            state.store.save(&manifest).await?;
        }
        send_progress(&event_tx, &manifest).await;
        let _ = event_tx
            .send(SessionEvent::FileFailed {
                transfer_id,
                file_name: manifest.metadata.file_name,
                message,
            })
            .await;
    }
    Ok(())
}

async fn send_progress(event_tx: &mpsc::Sender<SessionEvent>, manifest: &TransferManifest) {
    let _ = event_tx
        .send(SessionEvent::FileProgress(FileTransferProgress {
            transfer_id: manifest.metadata.transfer_id.clone(),
            file_name: manifest.metadata.file_name.clone(),
            direction: manifest.direction.clone(),
            status: manifest.status.clone(),
            completed_bytes: manifest.completed_bytes(),
            total_bytes: manifest.metadata.file_size,
        }))
        .await;
}

impl ChatSessionHandle {
    pub async fn send_text(&self, text: String) -> Result<()> {
        self.direct_tx.send(DirectCommand::Chat(text)).await?;
        Ok(())
    }

    pub async fn retry_direct(&self) -> Result<()> {
        self.session_tx.send(SessionCommand::RetryDirect).await?;
        Ok(())
    }

    pub async fn send_file(&self, path: PathBuf) -> Result<()> {
        self.direct_tx.send(DirectCommand::OfferFile(path)).await?;
        Ok(())
    }

    pub async fn accept_file(&self, transfer_id: String, save_path: PathBuf) -> Result<()> {
        self.direct_tx
            .send(DirectCommand::AcceptFile {
                transfer_id,
                save_path,
            })
            .await?;
        Ok(())
    }

    pub async fn reject_file(&self, transfer_id: String, reason: String) -> Result<()> {
        self.direct_tx
            .send(DirectCommand::RejectFile {
                transfer_id,
                reason,
            })
            .await?;
        Ok(())
    }

    pub async fn pause_transfer(&self, transfer_id: String) -> Result<()> {
        self.direct_tx
            .send(DirectCommand::PauseTransfer(transfer_id))
            .await?;
        Ok(())
    }

    pub async fn resume_transfer(&self, transfer_id: String) -> Result<()> {
        self.direct_tx
            .send(DirectCommand::ResumeTransfer(transfer_id))
            .await?;
        Ok(())
    }

    pub async fn cancel_transfer(&self, transfer_id: String, reason: String) -> Result<()> {
        self.direct_tx
            .send(DirectCommand::CancelTransfer {
                transfer_id,
                reason,
            })
            .await?;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.signaling_tx.send(SignalingCommand::Close).await?;
        Ok(())
    }
}

fn should_announce_peer(peer_seen: &mut bool) -> bool {
    if *peer_seen {
        false
    } else {
        *peer_seen = true;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_announcement_only_fires_for_first_peer_event() {
        let mut peer_seen = false;

        assert!(should_announce_peer(&mut peer_seen));
        assert!(!should_announce_peer(&mut peer_seen));

        peer_seen = false;
        assert!(should_announce_peer(&mut peer_seen));
    }
}
