use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Debug, Clone)]
pub struct SignalingClient {
    url: String,
}

#[derive(Debug)]
pub enum SignalingCommand {
    SendText(String),
    Close,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SignalingRole {
    Host,
    Guest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum SignalingEnvelope {
    Hello {
        role: SignalingRole,
        #[serde(rename = "roomCode", skip_serializing_if = "Option::is_none")]
        room_code: Option<String>,
    },
    RoomReady {
        #[serde(rename = "roomCode", default, skip_serializing_if = "Option::is_none")]
        room_code: Option<String>,
    },
    PeerJoined {
        role: SignalingRole,
    },
    PeerLeft {
        role: SignalingRole,
    },
    Signal {
        payload: Value,
    },
    Error {
        message: String,
    },
    Bye,
}

impl SignalingClient {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }

    pub async fn connect(
        self,
        mut commands: mpsc::Receiver<SignalingCommand>,
        incoming: mpsc::Sender<String>,
    ) -> Result<()> {
        let (socket, _) = connect_async(&self.url)
            .await
            .with_context(|| format!("failed to connect signaling websocket: {}", self.url))?;

        let (mut writer, mut reader) = socket.split();

        loop {
            tokio::select! {
                command = commands.recv() => {
                    match command {
                        Some(SignalingCommand::SendText(text)) => {
                            writer.send(Message::Text(text)).await?;
                        }
                        Some(SignalingCommand::Close) | None => {
                            let _ = writer.send(Message::Close(None)).await;
                            break;
                        }
                    }
                }
                message = reader.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            if incoming.send(text).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Err(error.into()),
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_camel_case_room_ready() {
        let raw = r#"{
            "type": "room-ready",
            "roomCode": "1234"
        }"#;

        let envelope: SignalingEnvelope = serde_json::from_str(raw).unwrap();

        match envelope {
            SignalingEnvelope::RoomReady { room_code } => {
                assert_eq!(room_code.as_deref(), Some("1234"));
            }
            other => panic!("expected room ready, got {other:?}"),
        }
    }
}
