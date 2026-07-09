use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::nat::{collect_connect_info, ConnectInfo};
use crate::signaling::{SignalingClient, SignalingCommand, SignalingEnvelope, SignalingRole};
use crate::transfer::{
    decode_chunk, hash_file, metadata_for_path, open_chunk_sink, open_chunk_source,
    read_chunk_from, validate_offer_metadata, write_chunk_to, ChunkRange, FileMetadata, RangeSet,
    TransferDirection, TransferManifest, TransferStatus, TransferStore,
};

/// 接收端每收到多少个分块就持久化一次 manifest 并回执一次 FileAck/进度。
const PERSIST_EVERY_CHUNKS: u64 = 32;

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
    command_tx: mpsc::Sender<SignalingCommand>,
    transfer_tx: mpsc::Sender<TransferCommand>,
}

#[derive(Debug)]
enum TransferCommand {
    SendFile(PathBuf),
    AcceptFile {
        transfer_id: String,
        save_path: PathBuf,
    },
    RejectFile {
        transfer_id: String,
        reason: String,
    },
    Pause {
        transfer_id: String,
    },
    Resume {
        transfer_id: String,
    },
    Cancel {
        transfer_id: String,
        reason: String,
    },
    PeerAvailable,
}

impl ChatSession {
    pub fn new(role: SessionRole, signaling_url: String) -> Self {
        Self {
            role,
            signaling_url,
        }
    }

    pub async fn start(self, event_tx: mpsc::Sender<SessionEvent>) -> Result<ChatSessionHandle> {
        let (command_tx, command_rx) = mpsc::channel::<SignalingCommand>(128);
        let (incoming_tx, mut incoming_rx) = mpsc::channel::<String>(128);
        let (transfer_tx, transfer_rx) = mpsc::channel::<TransferCommand>(64);
        let (peer_file_tx, peer_file_rx) = mpsc::channel::<SignalingEnvelope>(128);

        let client = SignalingClient::new(self.signaling_url.clone());
        let events = event_tx.clone();

        tokio::spawn(async move {
            if let Err(error) = client.connect(command_rx, incoming_tx).await {
                let _ = events.send(SessionEvent::Error(format!("{error:#}"))).await;
            }
        });

        let transfer_events = event_tx.clone();
        let transfer_commands = command_tx.clone();
        tokio::spawn(async move {
            let mut runtime = match TransferRuntime::new(
                transfer_rx,
                peer_file_rx,
                transfer_commands,
                transfer_events.clone(),
            )
            .await
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = transfer_events
                        .send(SessionEvent::Error(format!(
                            "文件传输初始化失败：{error:#}"
                        )))
                        .await;
                    return;
                }
            };

            runtime.run().await;
        });

        let dispatch_events = event_tx.clone();
        let dispatch_transfer_tx = transfer_tx.clone();
        let dispatch_commands = command_tx.clone();
        let signaling_role = signaling_role_for_session(&self.role);
        tokio::spawn(async move {
            let mut peer_seen = false;
            let mut connect_info_sent = false;
            while let Some(raw) = incoming_rx.recv().await {
                match serde_json::from_str::<SignalingEnvelope>(&raw) {
                    Ok(SignalingEnvelope::Chat { text }) => {
                        if should_announce_peer(&mut peer_seen) {
                            let _ = dispatch_events.send(SessionEvent::PeerConnected).await;
                            let _ = dispatch_transfer_tx
                                .send(TransferCommand::PeerAvailable)
                                .await;
                        }
                        let _ = dispatch_events
                            .send(SessionEvent::MessageReceived(text))
                            .await;
                    }
                    Ok(SignalingEnvelope::PeerJoined { .. }) => {
                        if should_announce_peer(&mut peer_seen) {
                            let _ = dispatch_events.send(SessionEvent::PeerConnected).await;
                            let _ = dispatch_transfer_tx
                                .send(TransferCommand::PeerAvailable)
                                .await;
                        }
                        announce_connect_info_once(
                            &mut connect_info_sent,
                            signaling_role.clone(),
                            dispatch_commands.clone(),
                            dispatch_events.clone(),
                        );
                    }
                    Ok(SignalingEnvelope::PeerLeft { .. }) => {
                        peer_seen = false;
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
                                dispatch_commands.clone(),
                                dispatch_events.clone(),
                            );
                        }
                    }
                    Ok(envelope @ SignalingEnvelope::FileOffer { .. })
                    | Ok(envelope @ SignalingEnvelope::FileAccept { .. })
                    | Ok(envelope @ SignalingEnvelope::FileReject { .. })
                    | Ok(envelope @ SignalingEnvelope::FileResume { .. })
                    | Ok(envelope @ SignalingEnvelope::FileChunk { .. })
                    | Ok(envelope @ SignalingEnvelope::FileAck { .. })
                    | Ok(envelope @ SignalingEnvelope::FileComplete { .. })
                    | Ok(envelope @ SignalingEnvelope::FileCancel { .. }) => {
                        if should_announce_peer(&mut peer_seen) {
                            let _ = dispatch_events.send(SessionEvent::PeerConnected).await;
                            let _ = dispatch_transfer_tx
                                .send(TransferCommand::PeerAvailable)
                                .await;
                        }
                        let _ = peer_file_tx.send(envelope).await;
                    }
                    Ok(SignalingEnvelope::Signal { payload }) => {
                        if let Ok(info) = serde_json::from_value::<ConnectInfo>(payload) {
                            if info.is_supported() {
                                if should_announce_peer(&mut peer_seen) {
                                    let _ = dispatch_events.send(SessionEvent::PeerConnected).await;
                                    let _ = dispatch_transfer_tx
                                        .send(TransferCommand::PeerAvailable)
                                        .await;
                                }
                                let _ = dispatch_events
                                    .send(SessionEvent::PeerCandidatesReceived(info))
                                    .await;
                            }
                        }
                    }
                    Ok(SignalingEnvelope::Hello { .. }) | Ok(SignalingEnvelope::Bye) => {}
                    // 无法解析的帧是协议噪声，不能冒充对方的聊天消息
                    Err(_) => {}
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

        command_tx
            .send(SignalingCommand::SendText(serde_json::to_string(&hello)?))
            .await?;

        Ok(ChatSessionHandle {
            command_tx,
            transfer_tx,
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
    command_tx: mpsc::Sender<SignalingCommand>,
    event_tx: mpsc::Sender<SessionEvent>,
) {
    if *sent {
        return;
    }
    *sent = true;

    tokio::spawn(async move {
        match collect_connect_info(role).await {
            Ok(info) => {
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
                        if let Err(error) = command_tx.send(SignalingCommand::SendText(text)).await
                        {
                            let _ = event_tx
                                .send(SessionEvent::Error(format!("发送候选信息失败：{error}")))
                                .await;
                        }
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

impl ChatSessionHandle {
    pub async fn send_text(&self, text: String) -> Result<()> {
        let message = serde_json::to_string(&SignalingEnvelope::Chat { text })?;
        self.command_tx
            .send(SignalingCommand::SendText(message))
            .await?;
        Ok(())
    }

    pub async fn send_file(&self, path: PathBuf) -> Result<()> {
        self.transfer_tx
            .send(TransferCommand::SendFile(path))
            .await?;
        Ok(())
    }

    pub async fn accept_file(&self, transfer_id: String, save_path: PathBuf) -> Result<()> {
        self.transfer_tx
            .send(TransferCommand::AcceptFile {
                transfer_id,
                save_path,
            })
            .await?;
        Ok(())
    }

    pub async fn reject_file(&self, transfer_id: String, reason: String) -> Result<()> {
        self.transfer_tx
            .send(TransferCommand::RejectFile {
                transfer_id,
                reason,
            })
            .await?;
        Ok(())
    }

    pub async fn pause_transfer(&self, transfer_id: String) -> Result<()> {
        self.transfer_tx
            .send(TransferCommand::Pause { transfer_id })
            .await?;
        Ok(())
    }

    pub async fn resume_transfer(&self, transfer_id: String) -> Result<()> {
        self.transfer_tx
            .send(TransferCommand::Resume { transfer_id })
            .await?;
        Ok(())
    }

    pub async fn cancel_transfer(&self, transfer_id: String, reason: String) -> Result<()> {
        self.transfer_tx
            .send(TransferCommand::Cancel {
                transfer_id,
                reason,
            })
            .await?;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.command_tx.send(SignalingCommand::Close).await?;
        Ok(())
    }
}

struct TransferRuntime {
    command_rx: mpsc::Receiver<TransferCommand>,
    peer_rx: mpsc::Receiver<SignalingEnvelope>,
    signal_tx: mpsc::Sender<SignalingCommand>,
    event_tx: mpsc::Sender<SessionEvent>,
    store: TransferStore,
    manifests: HashMap<String, TransferManifest>,
    offers: HashMap<String, FileMetadata>,
    active_sends: HashMap<String, tokio::task::JoinHandle<()>>,
    open_files: HashMap<String, tokio::fs::File>,
    chunks_since_persist: HashMap<String, u64>,
}

impl TransferRuntime {
    async fn new(
        command_rx: mpsc::Receiver<TransferCommand>,
        peer_rx: mpsc::Receiver<SignalingEnvelope>,
        signal_tx: mpsc::Sender<SignalingCommand>,
        event_tx: mpsc::Sender<SessionEvent>,
    ) -> Result<Self> {
        let store = TransferStore::platform_default()?;
        let manifests = store
            .load_pending()
            .await?
            .into_iter()
            .map(|manifest| (manifest.metadata.transfer_id.clone(), manifest))
            .collect();

        Ok(Self {
            command_rx,
            peer_rx,
            signal_tx,
            event_tx,
            store,
            manifests,
            offers: HashMap::new(),
            active_sends: HashMap::new(),
            open_files: HashMap::new(),
            chunks_since_persist: HashMap::new(),
        })
    }

    fn abort_send(&mut self, transfer_id: &str) {
        if let Some(handle) = self.active_sends.remove(transfer_id) {
            handle.abort();
        }
    }

    async fn close_receive_file(&mut self, transfer_id: &str) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        self.chunks_since_persist.remove(transfer_id);
        if let Some(mut file) = self.open_files.remove(transfer_id) {
            file.flush().await?;
        }
        Ok(())
    }

    async fn run(&mut self) {
        self.emit_loaded_pending().await;

        loop {
            tokio::select! {
                command = self.command_rx.recv() => {
                    let Some(command) = command else { break; };
                    if let Err(error) = self.handle_command(command).await {
                        let _ = self.event_tx.send(SessionEvent::Error(format!("文件传输失败：{error:#}"))).await;
                    }
                }
                envelope = self.peer_rx.recv() => {
                    let Some(envelope) = envelope else { break; };
                    if let Err(error) = self.handle_peer_envelope(envelope).await {
                        let _ = self.event_tx.send(SessionEvent::Error(format!("文件传输失败：{error:#}"))).await;
                    }
                }
            }
        }
    }

    async fn emit_loaded_pending(&self) {
        for manifest in self.manifests.values() {
            self.emit_progress(manifest).await;
        }
    }

    async fn handle_command(&mut self, command: TransferCommand) -> Result<()> {
        match command {
            TransferCommand::SendFile(path) => self.send_file_offer(path).await,
            TransferCommand::AcceptFile {
                transfer_id,
                save_path,
            } => self.accept_file(transfer_id, save_path).await,
            TransferCommand::RejectFile {
                transfer_id,
                reason,
            } => {
                self.send_envelope(SignalingEnvelope::FileReject {
                    transfer_id,
                    reason,
                })
                .await
            }
            TransferCommand::Pause { transfer_id } => self.pause_transfer(&transfer_id).await,
            TransferCommand::Resume { transfer_id } => self.resume_transfer(&transfer_id).await,
            TransferCommand::Cancel {
                transfer_id,
                reason,
            } => self.cancel_transfer(&transfer_id, reason).await,
            TransferCommand::PeerAvailable => self.announce_pending().await,
        }
    }

    async fn handle_peer_envelope(&mut self, envelope: SignalingEnvelope) -> Result<()> {
        match envelope {
            SignalingEnvelope::FileOffer { metadata } => self.handle_offer(metadata).await,
            SignalingEnvelope::FileAccept {
                transfer_id,
                missing,
            }
            | SignalingEnvelope::FileResume {
                transfer_id,
                missing,
            } => self.send_missing_chunks(transfer_id, missing).await,
            SignalingEnvelope::FileReject {
                transfer_id,
                reason,
            } => self.mark_failed(&transfer_id, reason).await,
            SignalingEnvelope::FileChunk { chunk } => self.receive_chunk(chunk).await,
            SignalingEnvelope::FileAck {
                transfer_id,
                received,
            } => self.receive_ack(&transfer_id, received).await,
            SignalingEnvelope::FileComplete {
                transfer_id,
                file_hash: _,
            } => self.mark_complete(&transfer_id).await,
            SignalingEnvelope::FileCancel {
                transfer_id,
                reason,
            } => self.mark_cancelled(&transfer_id, reason).await,
            _ => Ok(()),
        }
    }

    async fn send_file_offer(&mut self, path: PathBuf) -> Result<()> {
        let metadata = metadata_for_path(&path).await?;
        let mut manifest = TransferManifest::new_sender(metadata.clone(), path);
        manifest.status = TransferStatus::Offered;
        self.store.save(&manifest).await?;
        self.manifests
            .insert(metadata.transfer_id.clone(), manifest.clone());

        self.emit_progress(&manifest).await;
        self.send_envelope(SignalingEnvelope::FileOffer { metadata })
            .await
    }

    async fn accept_file(&mut self, transfer_id: String, save_path: PathBuf) -> Result<()> {
        let metadata = self
            .offers
            .remove(&transfer_id)
            .or_else(|| {
                self.manifests
                    .get(&transfer_id)
                    .map(|manifest| manifest.metadata.clone())
            })
            .ok_or_else(|| anyhow::anyhow!("找不到待接收文件：{transfer_id}"))?;

        let manifest = match self.store.load(&transfer_id).await? {
            Some(mut existing) if existing.direction == TransferDirection::Receive => {
                let path_changed = existing.output_path.as_deref() != Some(save_path.as_path());
                existing.output_path = Some(save_path.clone());
                existing.temp_path = Some(crate::transfer::part_path(&save_path));
                // 换了保存位置或临时文件已丢失时，旧进度对应的数据不存在，必须重新请求
                let temp_missing = match existing.temp_path.as_deref() {
                    Some(temp) => tokio::fs::metadata(temp).await.is_err(),
                    None => true,
                };
                if path_changed || temp_missing {
                    existing.completed_chunks.clear();
                }
                existing.status = TransferStatus::Accepted;
                existing
            }
            _ => TransferManifest::new_receiver(metadata, save_path),
        };

        let missing = manifest.missing_ranges();
        self.store.save(&manifest).await?;
        self.manifests.insert(transfer_id.clone(), manifest.clone());
        self.emit_progress(&manifest).await;

        if missing.is_empty() {
            // 0 字节文件（或已全部接收）不会再有分块到达，直接走收尾流程
            self.ensure_temp_file(&manifest).await?;
            return self.finish_receive(&transfer_id).await;
        }

        self.send_envelope(SignalingEnvelope::FileAccept {
            transfer_id,
            missing,
        })
        .await
    }

    async fn ensure_temp_file(&self, manifest: &TransferManifest) -> Result<()> {
        let Some(temp) = manifest.temp_path.as_deref() else {
            return Ok(());
        };
        if tokio::fs::metadata(temp).await.is_err() {
            drop(open_chunk_sink(temp).await?);
        }
        Ok(())
    }

    async fn handle_offer(&mut self, mut metadata: FileMetadata) -> Result<()> {
        if let Err(reason) = validate_offer_metadata(&mut metadata) {
            return self
                .send_envelope(SignalingEnvelope::FileReject {
                    transfer_id: metadata.transfer_id,
                    reason,
                })
                .await;
        }

        if let Some(mut existing) = self.store.load(&metadata.transfer_id).await? {
            if existing.direction == TransferDirection::Receive && !existing.is_complete() {
                existing.metadata = metadata;
                existing.status = TransferStatus::Accepted;
                // 临时文件已丢失时旧进度无效，必须从头请求
                let temp_missing = match existing.temp_path.as_deref() {
                    Some(temp) => tokio::fs::metadata(temp).await.is_err(),
                    None => true,
                };
                if temp_missing {
                    existing.completed_chunks.clear();
                }
                let missing = existing.missing_ranges();
                self.store.save(&existing).await?;
                let transfer_id = existing.metadata.transfer_id.clone();
                self.manifests.insert(transfer_id.clone(), existing.clone());
                self.emit_progress(&existing).await;

                if missing.is_empty() {
                    self.ensure_temp_file(&existing).await?;
                    return self.finish_receive(&transfer_id).await;
                }

                return self
                    .send_envelope(SignalingEnvelope::FileResume {
                        transfer_id,
                        missing,
                    })
                    .await;
            }
        }

        let transfer_id = metadata.transfer_id.clone();
        self.offers.insert(transfer_id, metadata.clone());
        self.event_tx
            .send(SessionEvent::FileOffered(metadata))
            .await
            .ok();
        Ok(())
    }

    async fn send_missing_chunks(
        &mut self,
        transfer_id: String,
        missing: Vec<ChunkRange>,
    ) -> Result<()> {
        let Some(manifest) = self.manifests.get_mut(&transfer_id) else {
            return Ok(());
        };
        if manifest.direction != TransferDirection::Send {
            return Ok(());
        }
        if manifest.status == TransferStatus::Paused {
            return Ok(());
        }

        manifest.status = TransferStatus::Accepted;
        self.store.save(manifest).await?;
        let progress_manifest = manifest.clone();

        let source_path = manifest
            .source_path
            .clone()
            .ok_or_else(|| anyhow::anyhow!("缺少源文件路径"))?;
        let metadata = manifest.metadata.clone();
        let signal_tx = self.signal_tx.clone();
        let event_tx = self.event_tx.clone();
        self.emit_progress(&progress_manifest).await;

        // 同一传输只保留一个发送循环；暂停/取消时由 abort_send 终止
        self.abort_send(&transfer_id);
        let handle = tokio::spawn(async move {
            let fail = |message: String| {
                let event_tx = event_tx.clone();
                let transfer_id = metadata.transfer_id.clone();
                let file_name = metadata.file_name.clone();
                async move {
                    let _ = event_tx
                        .send(SessionEvent::FileFailed {
                            transfer_id,
                            file_name,
                            message,
                        })
                        .await;
                }
            };

            let mut file = match open_chunk_source(&source_path).await {
                Ok(file) => file,
                Err(error) => return fail(format!("{error:#}")).await,
            };

            for range in missing {
                for index in range.start..range.end {
                    match read_chunk_from(&mut file, index, metadata.chunk_size).await {
                        Ok(mut chunk) => {
                            chunk.transfer_id = metadata.transfer_id.clone();
                            let envelope = SignalingEnvelope::FileChunk { chunk };
                            let send_result = match serde_json::to_string(&envelope) {
                                Ok(text) => signal_tx.send(SignalingCommand::SendText(text)).await,
                                Err(error) => return fail(format!("{error:#}")).await,
                            };

                            if let Err(error) = send_result {
                                return fail(format!("发送分段失败：{error}")).await;
                            }
                        }
                        Err(error) => return fail(format!("{error:#}")).await,
                    }
                }
            }
        });
        self.active_sends.insert(transfer_id, handle);

        Ok(())
    }

    async fn receive_chunk(&mut self, chunk: crate::transfer::FileChunk) -> Result<()> {
        let transfer_id = chunk.transfer_id.clone();
        let decoded = decode_chunk(&chunk)?;

        let (temp_path, chunk_size) = {
            let Some(manifest) = self.manifests.get(&transfer_id) else {
                return Ok(());
            };
            if manifest.direction != TransferDirection::Receive
                || manifest.status == TransferStatus::Paused
            {
                return Ok(());
            }
            if decoded.index >= manifest.metadata.total_chunks
                || decoded.bytes.len() as u64 > manifest.metadata.chunk_size
            {
                anyhow::bail!("chunk {} 越界", decoded.index);
            }

            (
                manifest
                    .temp_path
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("缺少临时接收路径"))?,
                manifest.metadata.chunk_size,
            )
        };

        if !self.open_files.contains_key(&transfer_id) {
            let file = open_chunk_sink(&temp_path).await?;
            self.open_files.insert(transfer_id.clone(), file);
        }
        let file = self
            .open_files
            .get_mut(&transfer_id)
            .expect("接收句柄刚刚插入");
        write_chunk_to(file, &decoded, chunk_size).await?;

        let (completed, complete, should_report) = {
            let Some(manifest) = self.manifests.get_mut(&transfer_id) else {
                return Ok(());
            };
            let mut completed = manifest.completed_set();
            completed.insert(decoded.index);
            manifest.completed_chunks = completed.clone().into_ranges();
            let complete = completed.completed_chunks() >= manifest.metadata.total_chunks;

            // manifest 落盘、FileAck 与进度事件按分块数节流，避免每 32KB 一次全量开销
            let counter = self
                .chunks_since_persist
                .entry(transfer_id.clone())
                .or_insert(0);
            *counter += 1;
            let should_report = complete || *counter >= PERSIST_EVERY_CHUNKS;
            if should_report {
                *counter = 0;
                self.store.save(manifest).await?;
                let progress_manifest = manifest.clone();
                self.emit_progress(&progress_manifest).await;
            }
            (completed, complete, should_report)
        };

        if should_report {
            self.send_envelope(SignalingEnvelope::FileAck {
                transfer_id: transfer_id.clone(),
                received: completed.ranges().to_vec(),
            })
            .await?;
        }

        if complete {
            self.finish_receive(&transfer_id).await?;
        }

        Ok(())
    }

    async fn finish_receive(&mut self, transfer_id: &str) -> Result<()> {
        let Some(snapshot) = self.manifests.get(transfer_id).cloned() else {
            return Ok(());
        };
        let temp_path = snapshot
            .temp_path
            .clone()
            .ok_or_else(|| anyhow::anyhow!("缺少临时接收路径"))?;
        let output_path = snapshot
            .output_path
            .clone()
            .ok_or_else(|| anyhow::anyhow!("缺少保存路径"))?;
        // 校验和改名前必须刷新并关闭写入句柄（Windows 下句柄未关闭无法 rename）
        self.close_receive_file(transfer_id).await?;
        let actual_hash = hash_file(&temp_path).await?;

        if actual_hash != snapshot.metadata.file_hash {
            let mut failed_event = None;
            let mut missing = Vec::new();
            if let Some(manifest) = self.manifests.get_mut(transfer_id) {
                manifest.status = TransferStatus::Failed;
                manifest.failure = Some("文件校验失败，等待重传".into());
                manifest.completed_chunks.clear();
                missing = vec![ChunkRange::new(0, manifest.metadata.total_chunks)];
                failed_event = Some((
                    manifest.metadata.transfer_id.clone(),
                    manifest.metadata.file_name.clone(),
                ));
                self.store.save(manifest).await?;
            }

            if let Some((transfer_id, file_name)) = failed_event {
                self.event_tx
                    .send(SessionEvent::FileFailed {
                        transfer_id,
                        file_name,
                        message: "文件校验失败，等待重传".into(),
                    })
                    .await
                    .ok();
            }

            return self
                .send_envelope(SignalingEnvelope::FileResume {
                    transfer_id: transfer_id.to_string(),
                    missing,
                })
                .await;
        }

        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::rename(&temp_path, &output_path).await?;

        let mut completed_event = None;
        if let Some(manifest) = self.manifests.get_mut(transfer_id) {
            manifest.status = TransferStatus::Complete;
            self.store.save(manifest).await?;
            let progress_manifest = manifest.clone();
            completed_event = Some((
                manifest.metadata.transfer_id.clone(),
                manifest.metadata.file_name.clone(),
                output_path.clone(),
                progress_manifest,
            ));
        }

        if let Some((transfer_id, file_name, output_path, progress_manifest)) = completed_event {
            self.emit_progress(&progress_manifest).await;
            self.event_tx
                .send(SessionEvent::FileCompleted {
                    transfer_id,
                    file_name,
                    path: Some(output_path),
                })
                .await
                .ok();
        }

        self.send_envelope(SignalingEnvelope::FileComplete {
            transfer_id: transfer_id.to_string(),
            file_hash: actual_hash,
        })
        .await
    }

    async fn receive_ack(&mut self, transfer_id: &str, received: Vec<ChunkRange>) -> Result<()> {
        let mut progress_manifest = None;
        if let Some(manifest) = self.manifests.get_mut(transfer_id) {
            if manifest.direction != TransferDirection::Send {
                return Ok(());
            }

            manifest.completed_chunks = RangeSet::from_ranges(received).into_ranges();
            self.store.save(manifest).await?;
            progress_manifest = Some(manifest.clone());
        }

        if let Some(manifest) = progress_manifest {
            self.emit_progress(&manifest).await;
        }
        Ok(())
    }

    async fn mark_complete(&mut self, transfer_id: &str) -> Result<()> {
        self.abort_send(transfer_id);
        self.close_receive_file(transfer_id).await?;
        let mut completed_event = None;
        if let Some(manifest) = self.manifests.get_mut(transfer_id) {
            manifest.status = TransferStatus::Complete;
            manifest.completed_chunks = vec![ChunkRange::new(0, manifest.metadata.total_chunks)];
            self.store.save(manifest).await?;
            completed_event = Some((
                manifest.clone(),
                manifest.metadata.transfer_id.clone(),
                manifest.metadata.file_name.clone(),
                manifest.source_path.clone(),
            ));
        }

        if let Some((manifest, transfer_id, file_name, path)) = completed_event {
            self.emit_progress(&manifest).await;
            self.event_tx
                .send(SessionEvent::FileCompleted {
                    transfer_id,
                    file_name,
                    path,
                })
                .await
                .ok();
        }
        Ok(())
    }

    async fn mark_failed(&mut self, transfer_id: &str, reason: String) -> Result<()> {
        self.abort_send(transfer_id);
        self.close_receive_file(transfer_id).await?;
        let mut failed_event = None;
        if let Some(manifest) = self.manifests.get_mut(transfer_id) {
            manifest.status = TransferStatus::Failed;
            manifest.failure = Some(reason.clone());
            self.store.save(manifest).await?;
            failed_event = Some((
                manifest.metadata.transfer_id.clone(),
                manifest.metadata.file_name.clone(),
            ));
        }
        if let Some((transfer_id, file_name)) = failed_event {
            self.event_tx
                .send(SessionEvent::FileFailed {
                    transfer_id,
                    file_name,
                    message: reason,
                })
                .await
                .ok();
        }
        Ok(())
    }

    async fn mark_cancelled(&mut self, transfer_id: &str, reason: String) -> Result<()> {
        self.abort_send(transfer_id);
        self.close_receive_file(transfer_id).await?;
        let mut cancelled_event = None;
        if let Some(manifest) = self.manifests.get_mut(transfer_id) {
            manifest.status = TransferStatus::Cancelled;
            self.store.save(manifest).await?;
            cancelled_event = Some((
                manifest.metadata.transfer_id.clone(),
                manifest.metadata.file_name.clone(),
            ));
        }

        if let Some((transfer_id, file_name)) = cancelled_event {
            self.event_tx
                .send(SessionEvent::FileCancelled {
                    transfer_id,
                    file_name,
                    reason,
                })
                .await
                .ok();
        }
        Ok(())
    }

    async fn pause_transfer(&mut self, transfer_id: &str) -> Result<()> {
        self.abort_send(transfer_id);
        self.close_receive_file(transfer_id).await?;
        let mut progress_manifest = None;
        if let Some(manifest) = self.manifests.get_mut(transfer_id) {
            manifest.status = TransferStatus::Paused;
            self.store.save(manifest).await?;
            progress_manifest = Some(manifest.clone());
        }

        if let Some(manifest) = progress_manifest {
            self.emit_progress(&manifest).await;
        }
        Ok(())
    }

    async fn resume_transfer(&mut self, transfer_id: &str) -> Result<()> {
        let Some((progress_manifest, envelope)) = ({
            let Some(manifest) = self.manifests.get_mut(transfer_id) else {
                return Ok(());
            };

            manifest.status = TransferStatus::Accepted;
            self.store.save(manifest).await?;
            let envelope = match manifest.direction {
                TransferDirection::Receive => SignalingEnvelope::FileResume {
                    transfer_id: transfer_id.to_string(),
                    missing: manifest.missing_ranges(),
                },
                TransferDirection::Send => SignalingEnvelope::FileOffer {
                    metadata: manifest.metadata.clone(),
                },
            };
            Some((manifest.clone(), envelope))
        }) else {
            return Ok(());
        };

        self.emit_progress(&progress_manifest).await;
        self.send_envelope(envelope).await
    }

    async fn cancel_transfer(&mut self, transfer_id: &str, reason: String) -> Result<()> {
        self.mark_cancelled(transfer_id, reason.clone()).await?;
        self.send_envelope(SignalingEnvelope::FileCancel {
            transfer_id: transfer_id.to_string(),
            reason,
        })
        .await
    }

    async fn announce_pending(&mut self) -> Result<()> {
        let manifests = self.manifests.values().cloned().collect::<Vec<_>>();
        for manifest in manifests {
            // 已完成、已取消或用户主动暂停的传输不随对方重新上线自动恢复
            if manifest.is_complete()
                || matches!(
                    manifest.status,
                    TransferStatus::Cancelled | TransferStatus::Paused
                )
            {
                continue;
            }

            match manifest.direction {
                TransferDirection::Send => {
                    self.send_envelope(SignalingEnvelope::FileOffer {
                        metadata: manifest.metadata,
                    })
                    .await?;
                }
                TransferDirection::Receive => {
                    let transfer_id = manifest.metadata.transfer_id.clone();
                    let missing = manifest.missing_ranges();
                    self.send_envelope(SignalingEnvelope::FileResume {
                        transfer_id,
                        missing,
                    })
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn send_envelope(&self, envelope: SignalingEnvelope) -> Result<()> {
        self.signal_tx
            .send(SignalingCommand::SendText(serde_json::to_string(
                &envelope,
            )?))
            .await?;
        Ok(())
    }

    async fn emit_progress(&self, manifest: &TransferManifest) {
        self.event_tx
            .send(SessionEvent::FileProgress(FileTransferProgress {
                transfer_id: manifest.metadata.transfer_id.clone(),
                file_name: manifest.metadata.file_name.clone(),
                direction: manifest.direction.clone(),
                status: manifest.status.clone(),
                completed_bytes: manifest.completed_bytes(),
                total_bytes: manifest.metadata.file_size,
            }))
            .await
            .ok();
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
    use crate::transfer::{FileMetadata, DEFAULT_CHUNK_SIZE};

    #[test]
    fn file_progress_caps_completed_bytes_to_file_size() {
        let manifest = TransferManifest {
            version: 1,
            direction: TransferDirection::Receive,
            status: TransferStatus::Accepted,
            metadata: FileMetadata {
                transfer_id: "file-test".into(),
                file_name: "demo.bin".into(),
                file_size: DEFAULT_CHUNK_SIZE + 7,
                chunk_size: DEFAULT_CHUNK_SIZE,
                total_chunks: 2,
                modified_millis: None,
                sample_hash: "sample".into(),
                file_hash: "hash".into(),
            },
            source_path: None,
            output_path: Some(PathBuf::from("/tmp/demo.bin")),
            temp_path: Some(PathBuf::from("/tmp/demo.bin.part")),
            completed_chunks: vec![ChunkRange::new(0, 2)],
            failure: None,
        };

        assert_eq!(manifest.completed_bytes(), DEFAULT_CHUNK_SIZE + 7);
    }

    #[test]
    fn peer_announcement_only_fires_for_first_peer_event() {
        let mut peer_seen = false;

        assert!(should_announce_peer(&mut peer_seen));
        assert!(!should_announce_peer(&mut peer_seen));

        peer_seen = false;
        assert!(should_announce_peer(&mut peer_seen));
    }
}
