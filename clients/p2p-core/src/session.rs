use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use serde_json::json;
use tokio::sync::mpsc;

use crate::signaling::{SignalingClient, SignalingCommand, SignalingEnvelope, SignalingRole};
use crate::transfer::{
    decode_chunk, hash_file, metadata_for_path, read_chunk, write_chunk, ChunkRange, FileMetadata,
    RangeSet, TransferDirection, TransferManifest, TransferStatus, TransferStore,
    RTC_FALLBACK_AFTER_MS,
};

#[derive(Debug, Clone)]
pub enum SessionRole {
    Host { room_code: String },
    Guest { room_code: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferTransport {
    WebRtc,
    WorkerFallback,
}

#[derive(Debug, Clone)]
pub struct FileTransferProgress {
    pub transfer_id: String,
    pub file_name: String,
    pub direction: TransferDirection,
    pub status: TransferStatus,
    pub completed_bytes: u64,
    pub total_bytes: u64,
    pub transport: TransferTransport,
}

#[derive(Debug)]
pub enum SessionEvent {
    Connected,
    RoomCodeGenerated(String),
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
        tokio::spawn(async move {
            while let Some(raw) = incoming_rx.recv().await {
                match serde_json::from_str::<SignalingEnvelope>(&raw) {
                    Ok(SignalingEnvelope::Chat { text }) => {
                        let _ = dispatch_events
                            .send(SessionEvent::MessageReceived(text))
                            .await;
                    }
                    Ok(SignalingEnvelope::PeerJoined { .. }) => {
                        let _ = dispatch_events.send(SessionEvent::PeerConnected).await;
                        let _ = dispatch_transfer_tx
                            .send(TransferCommand::PeerAvailable)
                            .await;
                    }
                    Ok(SignalingEnvelope::PeerLeft { .. }) => {
                        let _ = dispatch_events.send(SessionEvent::PeerDisconnected).await;
                    }
                    Ok(SignalingEnvelope::Error { message }) => {
                        let _ = dispatch_events.send(SessionEvent::Error(message)).await;
                    }
                    Ok(SignalingEnvelope::RoomReady) => {
                        let _ = dispatch_events.send(SessionEvent::Connected).await;
                    }
                    Ok(envelope @ SignalingEnvelope::FileOffer { .. })
                    | Ok(envelope @ SignalingEnvelope::FileAccept { .. })
                    | Ok(envelope @ SignalingEnvelope::FileReject { .. })
                    | Ok(envelope @ SignalingEnvelope::FileResume { .. })
                    | Ok(envelope @ SignalingEnvelope::FileChunk { .. })
                    | Ok(envelope @ SignalingEnvelope::FileAck { .. })
                    | Ok(envelope @ SignalingEnvelope::FileComplete { .. })
                    | Ok(envelope @ SignalingEnvelope::FileCancel { .. }) => {
                        let _ = peer_file_tx.send(envelope).await;
                    }
                    Ok(SignalingEnvelope::Signal { .. }) => {}
                    Ok(SignalingEnvelope::Hello { .. }) | Ok(SignalingEnvelope::Bye) => {}
                    Err(_) => {
                        let _ = dispatch_events
                            .send(SessionEvent::MessageReceived(raw))
                            .await;
                    }
                }
            }
        });

        let hello = match self.role {
            SessionRole::Host { room_code } => {
                event_tx
                    .send(SessionEvent::RoomCodeGenerated(room_code))
                    .await?;
                SignalingEnvelope::Hello {
                    role: SignalingRole::Host,
                    room_code: None,
                }
            }
            SessionRole::Guest { room_code } => {
                event_tx
                    .send(SessionEvent::RoomCodeGenerated(room_code.clone()))
                    .await?;
                SignalingEnvelope::Hello {
                    role: SignalingRole::Guest,
                    room_code: Some(room_code),
                }
            }
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
        })
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
            self.emit_progress(manifest, TransferTransport::WorkerFallback)
                .await;
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

        self.emit_progress(&manifest, TransferTransport::WebRtc)
            .await;
        self.send_rtc_probe(&metadata.transfer_id).await?;
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
                existing.output_path = Some(save_path.clone());
                existing.temp_path = Some(crate::transfer::part_path(&save_path));
                existing.status = TransferStatus::Accepted;
                existing
            }
            _ => TransferManifest::new_receiver(metadata, save_path),
        };

        let missing = manifest.missing_ranges();
        self.store.save(&manifest).await?;
        self.manifests.insert(transfer_id.clone(), manifest.clone());
        self.emit_progress(&manifest, TransferTransport::WorkerFallback)
            .await;

        self.send_envelope(SignalingEnvelope::FileAccept {
            transfer_id,
            missing,
        })
        .await
    }

    async fn handle_offer(&mut self, metadata: FileMetadata) -> Result<()> {
        if let Some(mut existing) = self.store.load(&metadata.transfer_id).await? {
            if existing.direction == TransferDirection::Receive && !existing.is_complete() {
                existing.metadata = metadata;
                existing.status = TransferStatus::Accepted;
                let missing = existing.missing_ranges();
                self.store.save(&existing).await?;
                self.manifests
                    .insert(existing.metadata.transfer_id.clone(), existing.clone());
                self.emit_progress(&existing, TransferTransport::WorkerFallback)
                    .await;
                return self
                    .send_envelope(SignalingEnvelope::FileResume {
                        transfer_id: existing.metadata.transfer_id,
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
        let store = TransferStore::new(self.store.root().to_path_buf());
        let mut task_manifest = progress_manifest.clone();
        self.emit_progress(&progress_manifest, TransferTransport::WorkerFallback)
            .await;

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(RTC_FALLBACK_AFTER_MS)).await;
            for range in missing {
                for index in range.start..range.end {
                    if task_manifest.status == TransferStatus::Paused {
                        return;
                    }

                    match read_chunk(&source_path, index, metadata.chunk_size).await {
                        Ok(mut chunk) => {
                            chunk.transfer_id = metadata.transfer_id.clone();
                            let envelope = SignalingEnvelope::FileChunk { chunk };
                            let send_result = match serde_json::to_string(&envelope) {
                                Ok(text) => signal_tx.send(SignalingCommand::SendText(text)).await,
                                Err(error) => {
                                    let _ = event_tx
                                        .send(SessionEvent::FileFailed {
                                            transfer_id: metadata.transfer_id.clone(),
                                            file_name: metadata.file_name.clone(),
                                            message: format!("{error:#}"),
                                        })
                                        .await;
                                    return;
                                }
                            };

                            if let Err(error) = send_result {
                                let _ = event_tx
                                    .send(SessionEvent::FileFailed {
                                        transfer_id: metadata.transfer_id.clone(),
                                        file_name: metadata.file_name.clone(),
                                        message: format!("发送分段失败：{error}"),
                                    })
                                    .await;
                                return;
                            }
                        }
                        Err(error) => {
                            task_manifest.status = TransferStatus::Failed;
                            task_manifest.failure = Some(format!("{error:#}"));
                            let _ = store.save(&task_manifest).await;
                            let _ = event_tx
                                .send(SessionEvent::FileFailed {
                                    transfer_id: metadata.transfer_id.clone(),
                                    file_name: metadata.file_name.clone(),
                                    message: format!("{error:#}"),
                                })
                                .await;
                            return;
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn receive_chunk(&mut self, chunk: crate::transfer::FileChunk) -> Result<()> {
        let transfer_id = chunk.transfer_id.clone();
        let decoded = decode_chunk(&chunk)?;
        let (completed, complete) = {
            let Some(manifest) = self.manifests.get_mut(&transfer_id) else {
                return Ok(());
            };
            if manifest.direction != TransferDirection::Receive
                || manifest.status == TransferStatus::Paused
            {
                return Ok(());
            }

            let temp_path = manifest
                .temp_path
                .clone()
                .ok_or_else(|| anyhow::anyhow!("缺少临时接收路径"))?;
            write_chunk(&temp_path, &decoded, manifest.metadata.chunk_size).await?;

            let mut completed = manifest.completed_set();
            completed.insert(decoded.index);
            manifest.completed_chunks = completed.clone().into_ranges();
            let complete = completed.completed_chunks() >= manifest.metadata.total_chunks;
            self.store.save(manifest).await?;
            let progress_manifest = manifest.clone();
            self.emit_progress(&progress_manifest, TransferTransport::WorkerFallback)
                .await;
            (completed, complete)
        };

        self.send_envelope(SignalingEnvelope::FileAck {
            transfer_id: transfer_id.clone(),
            received: completed.ranges().to_vec(),
        })
        .await?;

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
            self.emit_progress(&progress_manifest, TransferTransport::WorkerFallback)
                .await;
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
            self.emit_progress(&manifest, TransferTransport::WorkerFallback)
                .await;
        }
        Ok(())
    }

    async fn mark_complete(&mut self, transfer_id: &str) -> Result<()> {
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
            self.emit_progress(&manifest, TransferTransport::WorkerFallback)
                .await;
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
        let mut progress_manifest = None;
        if let Some(manifest) = self.manifests.get_mut(transfer_id) {
            manifest.status = TransferStatus::Paused;
            self.store.save(manifest).await?;
            progress_manifest = Some(manifest.clone());
        }

        if let Some(manifest) = progress_manifest {
            self.emit_progress(&manifest, TransferTransport::WorkerFallback)
                .await;
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

        self.emit_progress(&progress_manifest, TransferTransport::WorkerFallback)
            .await;
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
            if manifest.is_complete() {
                continue;
            }

            match manifest.direction {
                TransferDirection::Send => {
                    self.send_rtc_probe(&manifest.metadata.transfer_id).await?;
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

    async fn send_rtc_probe(&self, transfer_id: &str) -> Result<()> {
        let payload = json!({
            "kind": "rtc-transfer-probe",
            "transferId": transfer_id,
            "stun": ["stun:stun.l.google.com:19302"],
            "fallbackAfterMs": RTC_FALLBACK_AFTER_MS
        });

        self.send_envelope(SignalingEnvelope::Signal { payload })
            .await
    }

    async fn send_envelope(&self, envelope: SignalingEnvelope) -> Result<()> {
        self.signal_tx
            .send(SignalingCommand::SendText(serde_json::to_string(
                &envelope,
            )?))
            .await?;
        Ok(())
    }

    async fn emit_progress(&self, manifest: &TransferManifest, transport: TransferTransport) {
        self.event_tx
            .send(SessionEvent::FileProgress(FileTransferProgress {
                transfer_id: manifest.metadata.transfer_id.clone(),
                file_name: manifest.metadata.file_name.clone(),
                direction: manifest.direction.clone(),
                status: manifest.status.clone(),
                completed_bytes: manifest.completed_bytes(),
                total_bytes: manifest.metadata.file_size,
                transport,
            }))
            .await
            .ok();
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
}
