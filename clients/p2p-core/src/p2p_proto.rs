use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_FRAME_LEN: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum P2pMessage {
    Hello { token: String },
    Ping,
    Pong,
    Chat { text: String },
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
}
