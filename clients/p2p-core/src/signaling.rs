use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::transfer::{ChunkRange, FileChunk, FileMetadata};

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
    Chat {
        text: String,
    },
    FileOffer {
        metadata: FileMetadata,
    },
    FileAccept {
        #[serde(rename = "transferId")]
        transfer_id: String,
        missing: Vec<ChunkRange>,
    },
    FileReject {
        #[serde(rename = "transferId")]
        transfer_id: String,
        reason: String,
    },
    FileResume {
        #[serde(rename = "transferId")]
        transfer_id: String,
        missing: Vec<ChunkRange>,
    },
    FileChunk {
        chunk: FileChunk,
    },
    FileAck {
        #[serde(rename = "transferId")]
        transfer_id: String,
        received: Vec<ChunkRange>,
    },
    FileComplete {
        #[serde(rename = "transferId")]
        transfer_id: String,
        #[serde(rename = "fileHash")]
        file_hash: String,
    },
    FileCancel {
        #[serde(rename = "transferId")]
        transfer_id: String,
        reason: String,
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
    fn parses_camel_case_file_offer() {
        let raw = r#"{
            "type": "file-offer",
            "metadata": {
                "transferId": "file-test",
                "fileName": "photo.png",
                "fileSize": 42,
                "chunkSize": 32768,
                "totalChunks": 1,
                "modifiedMillis": null,
                "sampleHash": "sample",
                "fileHash": "hash"
            }
        }"#;

        let envelope: SignalingEnvelope = serde_json::from_str(raw).unwrap();

        match envelope {
            SignalingEnvelope::FileOffer { metadata } => {
                assert_eq!(metadata.transfer_id, "file-test");
                assert_eq!(metadata.file_name, "photo.png");
            }
            other => panic!("expected file offer, got {other:?}"),
        }
    }
}
