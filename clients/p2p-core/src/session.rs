use anyhow::Result;
use tokio::sync::mpsc;

use crate::signaling::{SignalingClient, SignalingCommand, SignalingEnvelope, SignalingRole};

#[derive(Debug, Clone)]
pub enum SessionRole {
    Host { room_code: String },
    Guest { room_code: String },
}

#[derive(Debug)]
pub enum SessionEvent {
    RoomCodeGenerated(String),
    PeerConnected,
    PeerDisconnected,
    MessageReceived(String),
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
}

impl ChatSession {
    pub fn new(role: SessionRole, signaling_url: String) -> Self {
        Self {
            role,
            signaling_url,
        }
    }

    pub async fn start(self, event_tx: mpsc::Sender<SessionEvent>) -> Result<ChatSessionHandle> {
        let (command_tx, command_rx) = mpsc::channel::<SignalingCommand>(32);
        let (incoming_tx, mut incoming_rx) = mpsc::channel::<String>(32);

        let client = SignalingClient::new(self.signaling_url.clone());
        let events = event_tx.clone();

        tokio::spawn(async move {
            if let Err(error) = client.connect(command_rx, incoming_tx).await {
                let _ = events.send(SessionEvent::Error(format!("{error:#}"))).await;
            }
        });

        let dispatch_events = event_tx.clone();
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
                    }
                    Ok(SignalingEnvelope::PeerLeft { .. }) => {
                        let _ = dispatch_events.send(SessionEvent::PeerDisconnected).await;
                    }
                    Ok(SignalingEnvelope::Error { message }) => {
                        let _ = dispatch_events.send(SessionEvent::Error(message)).await;
                    }
                    Ok(SignalingEnvelope::RoomReady)
                    | Ok(SignalingEnvelope::Hello { .. })
                    | Ok(SignalingEnvelope::Signal { .. })
                    | Ok(SignalingEnvelope::Bye) => {}
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

        Ok(ChatSessionHandle { command_tx })
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

    pub async fn close(&self) -> Result<()> {
        self.command_tx.send(SignalingCommand::Close).await?;
        Ok(())
    }
}
