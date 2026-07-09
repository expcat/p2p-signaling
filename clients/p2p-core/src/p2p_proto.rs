use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::transfer::{ChunkRange, FileMetadata};

const MAX_FRAME_LEN: usize = 64 * 1024;
const MAX_STREAM_HEADER_LEN: usize = 4 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum P2pMessage {
    Hello {
        token: String,
    },
    Ping,
    Pong,
    Chat {
        text: String,
    },
    FileOffer {
        metadata: FileMetadata,
    },
    FileAccept {
        #[serde(rename = "transferId")]
        transfer_id: String,
        #[serde(rename = "completedChunks")]
        completed_chunks: Vec<ChunkRange>,
    },
    FileReject {
        #[serde(rename = "transferId")]
        transfer_id: String,
        reason: String,
    },
    FileResume {
        #[serde(rename = "transferId")]
        transfer_id: String,
        #[serde(rename = "completedChunks")]
        completed_chunks: Vec<ChunkRange>,
    },
    FileAck {
        #[serde(rename = "transferId")]
        transfer_id: String,
        chunks: Vec<ChunkRange>,
    },
    FileComplete {
        #[serde(rename = "transferId")]
        transfer_id: String,
    },
    FileCancel {
        #[serde(rename = "transferId")]
        transfer_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FileStreamHeader {
    pub transfer_id: String,
    pub start_chunk: u64,
    pub end_chunk: u64,
}

pub async fn write_p2p_message<W>(writer: &mut W, message: &P2pMessage) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(message).context("序列化直连消息失败")?;
    if payload.len() > MAX_FRAME_LEN {
        anyhow::bail!("直连消息过大");
    }

    writer
        .write_all(&(payload.len() as u32).to_le_bytes())
        .await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_p2p_message<R>(reader: &mut R) -> Result<P2pMessage>
where
    R: AsyncRead + Unpin,
{
    let mut len_bytes = [0_u8; 4];
    reader.read_exact(&mut len_bytes).await?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len == 0 || len > MAX_FRAME_LEN {
        anyhow::bail!("直连消息长度无效：{len}");
    }

    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload).await?;
    serde_json::from_slice(&payload).context("解析直连消息失败")
}

pub async fn write_file_stream_header<W>(writer: &mut W, header: &FileStreamHeader) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(header).context("序列化文件流头失败")?;
    if payload.len() > MAX_STREAM_HEADER_LEN {
        anyhow::bail!("文件流头过大");
    }

    writer
        .write_all(&(payload.len() as u32).to_le_bytes())
        .await?;
    writer.write_all(&payload).await?;
    Ok(())
}

pub async fn read_file_stream_header<R>(reader: &mut R) -> Result<FileStreamHeader>
where
    R: AsyncRead + Unpin,
{
    let mut len_bytes = [0_u8; 4];
    reader.read_exact(&mut len_bytes).await?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len == 0 || len > MAX_STREAM_HEADER_LEN {
        anyhow::bail!("文件流头长度无效：{len}");
    }

    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload).await?;
    serde_json::from_slice(&payload).context("解析文件流头失败")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trips_length_prefixed_chat() {
        let mut bytes = Vec::new();
        write_p2p_message(
            &mut bytes,
            &P2pMessage::Chat {
                text: "hello".into(),
            },
        )
        .await
        .unwrap();

        assert_eq!(
            u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize,
            bytes.len() - 4
        );

        let mut cursor = std::io::Cursor::new(bytes);
        let message = read_p2p_message(&mut cursor).await.unwrap();
        assert_eq!(
            message,
            P2pMessage::Chat {
                text: "hello".into()
            }
        );
    }

    #[tokio::test]
    async fn round_trips_file_offer_metadata() {
        let message = P2pMessage::FileOffer {
            metadata: FileMetadata {
                transfer_id: "file-test".into(),
                file_name: "sample.bin".into(),
                file_size: 42,
                chunk_size: 32768,
                total_chunks: 1,
                modified_millis: Some(1_788_888_888_000),
                sample_hash: "sample".into(),
                file_hash: "hash".into(),
            },
        };
        let mut bytes = Vec::new();

        write_p2p_message(&mut bytes, &message).await.unwrap();

        let mut cursor = std::io::Cursor::new(bytes);
        assert_eq!(read_p2p_message(&mut cursor).await.unwrap(), message);
    }

    #[tokio::test]
    async fn round_trips_file_stream_header() {
        let header = FileStreamHeader {
            transfer_id: "file-test".into(),
            start_chunk: 2,
            end_chunk: 5,
        };
        let mut bytes = Vec::new();

        write_file_stream_header(&mut bytes, &header).await.unwrap();

        let mut cursor = std::io::Cursor::new(bytes);
        assert_eq!(read_file_stream_header(&mut cursor).await.unwrap(), header);
    }
}
