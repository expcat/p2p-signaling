use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::direct::{establish_direct_link, DirectLink, DirectLinkInfo};
use crate::nat::{prepare_connect_info, ConnectInfo, PreparedConnectInfo};
use crate::p2p_proto::{read_p2p_message, write_p2p_message, P2pMessage};
use crate::signaling::{SignalingClient, SignalingCommand, SignalingEnvelope, SignalingRole};
use crate::transfer::{FileMetadata, TransferDirection, TransferStatus};

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
}

#[derive(Debug)]
enum SessionCommand {
    RetryDirect,
}

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
                        match command {
                            Some(SessionCommand::RetryDirect) => {
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
                            None => {}
                        }
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

    let _ = event_tx
        .send(SessionEvent::DirectLinkEstablished(info))
        .await;

    loop {
        tokio::select! {
            command = command_rx.recv() => {
                match command {
                    Some(DirectCommand::Chat(text)) => {
                        if let Err(error) = write_p2p_message(&mut send, &P2pMessage::Chat { text }).await {
                            let _ = event_tx.send(SessionEvent::DirectLinkLost(format!("{error:#}"))).await;
                            break;
                        }
                    }
                    None => {
                        connection.close(0_u32.into(), b"session closed");
                        break;
                    }
                }
            }
            message = read_p2p_message(&mut recv) => {
                match message {
                    Ok(P2pMessage::Chat { text }) => {
                        let _ = event_tx.send(SessionEvent::MessageReceived(text)).await;
                    }
                    Ok(P2pMessage::Ping) => {
                        if let Err(error) = write_p2p_message(&mut send, &P2pMessage::Pong).await {
                            let _ = event_tx.send(SessionEvent::DirectLinkLost(format!("{error:#}"))).await;
                            break;
                        }
                    }
                    Ok(P2pMessage::Pong) => {}
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

    connection.close(0_u32.into(), b"direct link stopped");
    endpoint.wait_idle().await;
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
        let _ = path;
        anyhow::bail!("文件传输将在 Phase 5 接入 QUIC，当前不会回退到 Worker 中继")
    }

    pub async fn accept_file(&self, transfer_id: String, save_path: PathBuf) -> Result<()> {
        let _ = (transfer_id, save_path);
        anyhow::bail!("文件传输将在 Phase 5 接入 QUIC")
    }

    pub async fn reject_file(&self, transfer_id: String, reason: String) -> Result<()> {
        let _ = (transfer_id, reason);
        anyhow::bail!("文件传输将在 Phase 5 接入 QUIC")
    }

    pub async fn pause_transfer(&self, transfer_id: String) -> Result<()> {
        let _ = transfer_id;
        anyhow::bail!("文件传输将在 Phase 5 接入 QUIC")
    }

    pub async fn resume_transfer(&self, transfer_id: String) -> Result<()> {
        let _ = transfer_id;
        anyhow::bail!("文件传输将在 Phase 5 接入 QUIC")
    }

    pub async fn cancel_transfer(&self, transfer_id: String, reason: String) -> Result<()> {
        let _ = (transfer_id, reason);
        anyhow::bail!("文件传输将在 Phase 5 接入 QUIC")
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
