use anyhow::Result;
use serde::{Deserialize, Serialize};

pub const REMOTE_DESKTOP_CAPABILITY: &str = "remote-desktop-v1";
pub const REMOTE_DESKTOP_STREAM_TYPE: &str = "desktop-frame";
pub const MAX_DESKTOP_WIDTH: u32 = 1280;
pub const MAX_DESKTOP_HEIGHT: u32 = 720;
pub const MAX_DESKTOP_FPS: u8 = 15;
pub const MAX_DESKTOP_PATCHES: usize = 60;
pub const MAX_DESKTOP_PAYLOAD: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteDisplay {
    pub id: String,
    pub name: String,
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum RemoteDesktopPlatform {
    #[default]
    Windows,
    Macos,
}

impl RemoteDesktopPlatform {
    pub const fn current() -> Option<Self> {
        if cfg!(target_os = "windows") {
            Some(Self::Windows)
        } else if cfg!(target_os = "macos") {
            Some(Self::Macos)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct RemoteDesktopConfig {
    pub width: u32,
    pub height: u32,
    pub max_fps: u8,
}

impl Default for RemoteDesktopConfig {
    fn default() -> Self {
        Self {
            width: MAX_DESKTOP_WIDTH,
            height: MAX_DESKTOP_HEIGHT,
            max_fps: MAX_DESKTOP_FPS,
        }
    }
}

impl RemoteDesktopConfig {
    pub fn validate(self) -> Result<Self> {
        if self.width == 0
            || self.height == 0
            || self.width > MAX_DESKTOP_WIDTH
            || self.height > MAX_DESKTOP_HEIGHT
        {
            anyhow::bail!("远程桌面尺寸无效：{}x{}", self.width, self.height);
        }
        if self.max_fps == 0 || self.max_fps > MAX_DESKTOP_FPS {
            anyhow::bail!("远程桌面帧率无效：{}", self.max_fps);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteDesktopOffer {
    pub session_id: String,
    #[serde(default)]
    pub platform: RemoteDesktopPlatform,
    pub display: RemoteDisplay,
    pub config: RemoteDesktopConfig,
    pub allow_control: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum RemoteDesktopControl {
    Offer {
        offer: RemoteDesktopOffer,
    },
    Answer {
        #[serde(rename = "sessionId")]
        session_id: String,
        accepted: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    Permission {
        #[serde(rename = "sessionId")]
        session_id: String,
        #[serde(rename = "allowControl")]
        allow_control: bool,
    },
    Stop {
        #[serde(rename = "sessionId")]
        session_id: String,
        reason: String,
    },
    Input {
        #[serde(rename = "sessionId")]
        session_id: String,
        sequence: u64,
        event: RemoteInputEvent,
    },
    KeyframeRequest {
        #[serde(rename = "sessionId")]
        session_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum RemoteInputEvent {
    PointerMove {
        x: u16,
        y: u16,
    },
    PointerButton {
        button: RemotePointerButton,
        pressed: bool,
    },
    Wheel {
        horizontal: bool,
        delta: i32,
    },
    Key {
        scan_code: u16,
        extended: bool,
        pressed: bool,
    },
    Text {
        text: String,
    },
    ReleaseAll,
}

impl RemoteInputEvent {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Wheel { delta, .. } if !(-1200..=1200).contains(delta) => {
                anyhow::bail!("远程滚轮增量超出范围")
            }
            Self::Text { text } if text.chars().count() > 64 => {
                anyhow::bail!("远程文本输入过长")
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum RemotePointerButton {
    Left,
    Right,
    Middle,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteDesktopPatch {
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
    pub encoded_len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteDesktopFrameHeader {
    #[serde(rename = "streamType")]
    pub stream_type: String,
    pub session_id: String,
    pub frame_id: u64,
    pub width: u32,
    pub height: u32,
    pub keyframe: bool,
    pub patches: Vec<RemoteDesktopPatch>,
}

impl RemoteDesktopFrameHeader {
    pub fn validate(&self) -> Result<usize> {
        if self.stream_type != REMOTE_DESKTOP_STREAM_TYPE {
            anyhow::bail!("远程桌面流类型无效")
        }
        if self.session_id.is_empty() {
            anyhow::bail!("远程桌面会话 ID 为空")
        }
        RemoteDesktopConfig {
            width: self.width,
            height: self.height,
            max_fps: 1,
        }
        .validate()?;
        if self.patches.len() > MAX_DESKTOP_PATCHES {
            anyhow::bail!("远程桌面补丁数量过多")
        }

        let mut payload_len = 0_usize;
        for patch in &self.patches {
            if patch.width == 0
                || patch.height == 0
                || patch.x.saturating_add(patch.width) > self.width
                || patch.y.saturating_add(patch.height) > self.height
            {
                anyhow::bail!("远程桌面补丁范围无效")
            }
            payload_len = payload_len
                .checked_add(patch.encoded_len as usize)
                .ok_or_else(|| anyhow::anyhow!("远程桌面帧长度溢出"))?;
        }
        if payload_len > MAX_DESKTOP_PAYLOAD {
            anyhow::bail!("远程桌面帧过大")
        }
        Ok(payload_len)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDesktopFrame {
    pub header: RemoteDesktopFrameHeader,
    pub payload: Vec<u8>,
}

impl RemoteDesktopFrame {
    pub fn validate(&self) -> Result<()> {
        let expected = self.header.validate()?;
        if expected != self.payload.len() {
            anyhow::bail!("远程桌面帧载荷长度不匹配")
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteDesktopState {
    Idle,
    OutgoingPending(RemoteDesktopOffer),
    IncomingPending(RemoteDesktopOffer),
    Sharing {
        session_id: String,
        allow_control: bool,
    },
    Viewing {
        session_id: String,
        can_control: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteDesktopEvent {
    PeerAvailabilityChanged(bool),
    IncomingOffer(RemoteDesktopOffer),
    SharingStarted(RemoteDesktopOffer),
    StateChanged(RemoteDesktopState),
    FrameAvailable {
        session_id: String,
        frame_id: u64,
    },
    Input(RemoteInputEvent),
    KeyframeRequested(String),
    Error {
        session_id: Option<String>,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_frame_payload_and_bounds() {
        let header = RemoteDesktopFrameHeader {
            stream_type: REMOTE_DESKTOP_STREAM_TYPE.into(),
            session_id: "desktop-1".into(),
            frame_id: 1,
            width: 640,
            height: 360,
            keyframe: true,
            patches: vec![RemoteDesktopPatch {
                x: 0,
                y: 0,
                width: 128,
                height: 128,
                encoded_len: 4,
            }],
        };
        assert_eq!(header.validate().unwrap(), 4);
        assert!(RemoteDesktopFrame {
            header: header.clone(),
            payload: vec![1, 2, 3, 4],
        }
        .validate()
        .is_ok());

        let mut invalid = header;
        invalid.patches[0].width = 641;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn rejects_oversized_text_and_wheel() {
        assert!(RemoteInputEvent::Text {
            text: "x".repeat(65)
        }
        .validate()
        .is_err());
        assert!(RemoteInputEvent::Wheel {
            horizontal: false,
            delta: 1201,
        }
        .validate()
        .is_err());
    }

    #[test]
    fn old_offer_defaults_to_windows_platform() {
        let offer: RemoteDesktopOffer = serde_json::from_value(serde_json::json!({
            "sessionId": "desktop-1",
            "display": {
                "id": "display-1",
                "name": "Display 1",
                "width": 1920,
                "height": 1080
            },
            "config": {
                "width": 1280,
                "height": 720,
                "maxFps": 15
            },
            "allowControl": false
        }))
        .unwrap();
        assert_eq!(offer.platform, RemoteDesktopPlatform::Windows);
    }

    #[test]
    fn macos_offer_serializes_platform() {
        let offer = RemoteDesktopOffer {
            session_id: "desktop-1".into(),
            platform: RemoteDesktopPlatform::Macos,
            display: RemoteDisplay {
                id: "42".into(),
                name: "Main Display".into(),
                width: 1920,
                height: 1080,
            },
            config: RemoteDesktopConfig::default(),
            allow_control: true,
        };
        let value = serde_json::to_value(&offer).unwrap();
        assert_eq!(value["platform"], "macos");
        assert_eq!(
            serde_json::from_value::<RemoteDesktopOffer>(value).unwrap(),
            offer
        );
    }
}
