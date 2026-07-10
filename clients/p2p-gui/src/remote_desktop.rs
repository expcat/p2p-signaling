use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use image::codecs::png::{CompressionType, FilterType, PngEncoder};
use image::{ColorType, ImageEncoder, ImageFormat};
use p2p_core::remote_desktop::{
    RemoteDesktopConfig, RemoteDesktopFrame, RemoteDesktopFrameHeader, RemoteDesktopOffer,
    RemoteDesktopPatch, RemoteDisplay, RemoteInputEvent, MAX_DESKTOP_FPS, MAX_DESKTOP_HEIGHT,
    MAX_DESKTOP_WIDTH, REMOTE_DESKTOP_STREAM_TYPE,
};
use p2p_core::ChatSessionHandle;

#[cfg(target_os = "windows")]
#[path = "remote_desktop_windows.rs"]
mod platform;

#[cfg(target_os = "macos")]
#[path = "remote_desktop_macos.rs"]
mod platform;

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
mod platform {
    use anyhow::Result;
    use p2p_core::remote_desktop::{RemoteDesktopConfig, RemoteDisplay, RemoteInputEvent};

    use super::RawFrame;

    pub fn available_displays() -> Result<Vec<RemoteDisplay>> {
        Ok(Vec::new())
    }

    pub fn ensure_screen_capture_permission() -> Result<()> {
        anyhow::bail!("当前平台暂不支持远程桌面")
    }

    pub fn ensure_input_permission() -> Result<()> {
        anyhow::bail!("当前平台暂不支持远程控制")
    }

    pub struct Capture;

    impl Capture {
        pub fn new(_display_id: &str, _config: RemoteDesktopConfig) -> Result<Self> {
            anyhow::bail!("当前平台暂不支持远程桌面")
        }

        pub fn capture(&mut self) -> Result<Option<RawFrame>> {
            anyhow::bail!("当前平台暂不支持远程桌面")
        }
    }

    pub struct InputInjector;

    impl InputInjector {
        pub fn new(_display_id: &str) -> Result<Self> {
            anyhow::bail!("当前平台暂不支持远程控制")
        }

        pub fn inject(&mut self, _event: &RemoteInputEvent) -> Result<()> {
            anyhow::bail!("当前平台暂不支持远程控制")
        }
    }
}

const TILE_SIZE: u32 = 128;

#[derive(Debug, Clone)]
pub struct RawFrame {
    pub width: u32,
    pub height: u32,
    pub rgba: Vec<u8>,
}

#[derive(Debug)]
pub enum CaptureEvent {
    Error(String),
    Stopped,
}

pub fn is_supported() -> bool {
    cfg!(any(target_os = "windows", target_os = "macos"))
}

pub fn available_displays() -> Result<Vec<RemoteDisplay>> {
    platform::available_displays()
}

pub fn ensure_screen_capture_permission() -> Result<()> {
    platform::ensure_screen_capture_permission()
}

pub fn ensure_input_permission() -> Result<()> {
    platform::ensure_input_permission()
}

pub fn fit_dimensions(source_width: u32, source_height: u32) -> RemoteDesktopConfig {
    if source_width == 0 || source_height == 0 {
        return RemoteDesktopConfig::default();
    }
    let scale = (MAX_DESKTOP_WIDTH as f64 / source_width as f64)
        .min(MAX_DESKTOP_HEIGHT as f64 / source_height as f64)
        .min(1.0);
    RemoteDesktopConfig {
        width: ((source_width as f64 * scale).round() as u32).max(1),
        height: ((source_height as f64 * scale).round() as u32).max(1),
        max_fps: MAX_DESKTOP_FPS,
    }
}

pub struct CaptureWorker {
    stop: Arc<AtomicBool>,
    force_keyframe: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl CaptureWorker {
    pub fn start(
        offer: RemoteDesktopOffer,
        handle: ChatSessionHandle,
        event_tx: mpsc::Sender<CaptureEvent>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let force_keyframe = Arc::new(AtomicBool::new(true));
        let worker_stop = stop.clone();
        let worker_keyframe = force_keyframe.clone();
        let join = std::thread::spawn(move || {
            if let Err(error) = capture_loop(offer, handle, worker_stop, worker_keyframe) {
                let _ = event_tx.send(CaptureEvent::Error(format!("{error:#}")));
            }
            let _ = event_tx.send(CaptureEvent::Stopped);
        });
        Self {
            stop,
            force_keyframe,
            join: Some(join),
        }
    }

    pub fn force_keyframe(&self) {
        self.force_keyframe.store(true, Ordering::Release);
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for CaptureWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

fn capture_loop(
    offer: RemoteDesktopOffer,
    handle: ChatSessionHandle,
    stop: Arc<AtomicBool>,
    force_keyframe: Arc<AtomicBool>,
) -> Result<()> {
    let mut capture = platform::Capture::new(&offer.display.id, offer.config)?;
    let mut encoder = FrameEncoder::new(offer.session_id.clone());
    let frame_interval = Duration::from_secs_f64(1.0 / offer.config.max_fps as f64);
    let mut next_frame = Instant::now();
    let mut consecutive_failures = 0_u8;

    while !stop.load(Ordering::Acquire) {
        if force_keyframe.swap(false, Ordering::AcqRel) {
            encoder.force_keyframe();
        }
        match capture.capture() {
            Ok(Some(frame)) => {
                consecutive_failures = 0;
                if let Some(encoded) = encoder.encode(&frame)? {
                    if handle.try_send_remote_desktop_frame(encoded)? {
                        encoder.commit(&frame);
                    }
                }
            }
            Ok(None) => {}
            Err(error) => {
                consecutive_failures += 1;
                if consecutive_failures >= 3 {
                    return Err(error).context("连续三次恢复屏幕采集失败");
                }
                std::thread::sleep(Duration::from_millis(250));
                capture = platform::Capture::new(&offer.display.id, offer.config)?;
                encoder.force_keyframe();
            }
        }

        next_frame += frame_interval;
        if let Some(delay) = next_frame.checked_duration_since(Instant::now()) {
            std::thread::sleep(delay);
        } else {
            next_frame = Instant::now();
        }
    }
    Ok(())
}

pub struct FrameDecoder {
    session_id: String,
    width: u32,
    height: u32,
    last_frame_id: u64,
    rgba: Vec<u8>,
    ready: bool,
}

impl FrameDecoder {
    pub fn new(session_id: String) -> Self {
        Self {
            session_id,
            width: 0,
            height: 0,
            last_frame_id: 0,
            rgba: Vec::new(),
            ready: false,
        }
    }

    pub fn apply(&mut self, frame: RemoteDesktopFrame) -> Result<()> {
        frame.validate()?;
        let header = &frame.header;
        if header.session_id != self.session_id {
            anyhow::bail!("远程桌面帧不属于当前会话")
        }
        if header.frame_id <= self.last_frame_id {
            anyhow::bail!("远程桌面帧序号未递增")
        }
        // 增量帧依赖前一帧的画布内容，跳号意味着中间帧已丢失，
        // 直接应用会造成无声的画面损坏，只能通过关键帧恢复
        if !header.keyframe && self.ready && header.frame_id != self.last_frame_id + 1 {
            anyhow::bail!("远程桌面增量帧不连续")
        }
        if (header.width != self.width || header.height != self.height) && !header.keyframe {
            anyhow::bail!("远程桌面尺寸变化缺少关键帧")
        }
        if header.keyframe {
            self.width = header.width;
            self.height = header.height;
            self.rgba = vec![0; (self.width * self.height * 4) as usize];
            self.ready = false;
        } else if !self.ready {
            anyhow::bail!("尚未收到远程桌面关键帧")
        }

        let mut offset = 0_usize;
        for patch in &header.patches {
            let end = offset + patch.encoded_len as usize;
            let image =
                image::load_from_memory_with_format(&frame.payload[offset..end], ImageFormat::Png)?
                    .to_rgba8();
            if image.width() != patch.width || image.height() != patch.height {
                anyhow::bail!("远程桌面补丁尺寸不匹配")
            }
            copy_patch_into_canvas(&mut self.rgba, self.width, patch, image.as_raw());
            offset = end;
        }
        self.last_frame_id = header.frame_id;
        self.ready = true;
        Ok(())
    }

    pub fn size(&self) -> (u32, u32) {
        (self.width, self.height)
    }

    pub fn canvas(&self) -> &[u8] {
        &self.rgba
    }
}

pub struct InputInjector {
    inner: platform::InputInjector,
}

impl InputInjector {
    pub fn new(display_id: &str) -> Result<Self> {
        Ok(Self {
            inner: platform::InputInjector::new(display_id)?,
        })
    }

    pub fn inject(&mut self, event: &RemoteInputEvent) -> Result<()> {
        event.validate()?;
        self.inner.inject(event)
    }
}

struct FrameEncoder {
    session_id: String,
    frame_id: u64,
    previous: Vec<u8>,
    width: u32,
    height: u32,
    force_keyframe: bool,
}

impl FrameEncoder {
    fn new(session_id: String) -> Self {
        Self {
            session_id,
            frame_id: 0,
            previous: Vec::new(),
            width: 0,
            height: 0,
            force_keyframe: true,
        }
    }

    fn force_keyframe(&mut self) {
        self.force_keyframe = true;
    }

    /// 只计算差分并编码，不修改基准帧；帧真正入队后必须调用 [`Self::commit`]。
    /// 被丢弃的帧不提交，下一帧的差分会自然覆盖累积变化，避免每次丢帧都强制全量关键帧
    fn encode(&mut self, frame: &RawFrame) -> Result<Option<RemoteDesktopFrame>> {
        let expected = (frame.width * frame.height * 4) as usize;
        if frame.rgba.len() != expected {
            anyhow::bail!("屏幕帧像素长度不匹配")
        }
        let keyframe = self.force_keyframe
            || self.width != frame.width
            || self.height != frame.height
            || self.previous.len() != frame.rgba.len();

        let mut patches = Vec::new();
        let mut payload = Vec::new();
        for y in (0..frame.height).step_by(TILE_SIZE as usize) {
            for x in (0..frame.width).step_by(TILE_SIZE as usize) {
                let width = TILE_SIZE.min(frame.width - x);
                let height = TILE_SIZE.min(frame.height - y);
                if !keyframe
                    && tile_matches(
                        &frame.rgba,
                        &self.previous,
                        frame.width,
                        x,
                        y,
                        width,
                        height,
                    )
                {
                    continue;
                }
                let tile = extract_tile(&frame.rgba, frame.width, x, y, width, height);
                let encoded = encode_png(&tile, width, height)?;
                patches.push(RemoteDesktopPatch {
                    x,
                    y,
                    width,
                    height,
                    encoded_len: encoded.len() as u32,
                });
                payload.extend_from_slice(&encoded);
            }
        }
        if patches.is_empty() {
            return Ok(None);
        }

        let result = RemoteDesktopFrame {
            header: RemoteDesktopFrameHeader {
                stream_type: REMOTE_DESKTOP_STREAM_TYPE.into(),
                session_id: self.session_id.clone(),
                frame_id: self.frame_id + 1,
                width: frame.width,
                height: frame.height,
                keyframe,
                patches,
            },
            payload,
        };
        result.validate()?;
        Ok(Some(result))
    }

    fn commit(&mut self, frame: &RawFrame) {
        self.width = frame.width;
        self.height = frame.height;
        self.frame_id += 1;
        if self.previous.len() == frame.rgba.len() {
            self.previous.copy_from_slice(&frame.rgba);
        } else {
            self.previous = frame.rgba.clone();
        }
        self.force_keyframe = false;
    }
}

fn encode_png(rgba: &[u8], width: u32, height: u32) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    PngEncoder::new_with_quality(&mut bytes, CompressionType::Fast, FilterType::Adaptive)
        .write_image(rgba, width, height, ColorType::Rgba8.into())?;
    Ok(bytes)
}

fn tile_matches(
    current: &[u8],
    previous: &[u8],
    canvas_width: u32,
    x: u32,
    y: u32,
    width: u32,
    height: u32,
) -> bool {
    let row_len = (width * 4) as usize;
    (0..height).all(|row| {
        let start = (((y + row) * canvas_width + x) * 4) as usize;
        current[start..start + row_len] == previous[start..start + row_len]
    })
}

fn extract_tile(
    rgba: &[u8],
    canvas_width: u32,
    x: u32,
    y: u32,
    width: u32,
    height: u32,
) -> Vec<u8> {
    let row_len = (width * 4) as usize;
    let mut tile = Vec::with_capacity(row_len * height as usize);
    for row in 0..height {
        let start = (((y + row) * canvas_width + x) * 4) as usize;
        tile.extend_from_slice(&rgba[start..start + row_len]);
    }
    tile
}

fn copy_patch_into_canvas(
    canvas: &mut [u8],
    canvas_width: u32,
    patch: &RemoteDesktopPatch,
    rgba: &[u8],
) {
    let row_len = (patch.width * 4) as usize;
    for row in 0..patch.height {
        let source = row as usize * row_len;
        let destination = (((patch.y + row) * canvas_width + patch.x) * 4) as usize;
        canvas[destination..destination + row_len].copy_from_slice(&rgba[source..source + row_len]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fits_1080p_into_720p() {
        assert_eq!(
            fit_dimensions(1920, 1080),
            RemoteDesktopConfig {
                width: 1280,
                height: 720,
                max_fps: 15,
            }
        );
    }

    #[test]
    fn keyframe_and_delta_round_trip() {
        let mut encoder = FrameEncoder::new("desktop-1".into());
        let mut decoder = FrameDecoder::new("desktop-1".into());
        let mut raw = RawFrame {
            width: 160,
            height: 90,
            rgba: vec![10; 160 * 90 * 4],
        };
        let first = encoder.encode(&raw).unwrap().unwrap();
        encoder.commit(&raw);
        assert!(first.header.keyframe);
        decoder.apply(first).unwrap();
        assert_eq!(decoder.canvas(), raw.rgba);

        raw.rgba[0] = 20;
        let second = encoder.encode(&raw).unwrap().unwrap();
        encoder.commit(&raw);
        assert!(!second.header.keyframe);
        assert_eq!(second.header.patches.len(), 1);
        decoder.apply(second).unwrap();
        assert_eq!(decoder.canvas(), raw.rgba);
        assert_eq!(decoder.size(), (160, 90));
        assert!(encoder.encode(&raw).unwrap().is_none());
    }

    #[test]
    fn uncommitted_frame_accumulates_into_next_delta() {
        let mut encoder = FrameEncoder::new("desktop-1".into());
        let mut raw = RawFrame {
            width: 160,
            height: 90,
            rgba: vec![10; 160 * 90 * 4],
        };
        let first = encoder.encode(&raw).unwrap().unwrap();
        assert!(first.header.keyframe);
        encoder.commit(&raw);

        // 第一个增量帧被丢弃（未 commit），左上角的变化必须保留在下一帧里
        raw.rgba[0] = 20;
        let dropped = encoder.encode(&raw).unwrap().unwrap();
        // 再改动另一个 tile（x=128 起第二列）
        let offset = (128 * 4) as usize;
        raw.rgba[offset] = 30;
        let retried = encoder.encode(&raw).unwrap().unwrap();
        assert_eq!(retried.header.frame_id, dropped.header.frame_id);
        assert!(!retried.header.keyframe);
        assert_eq!(retried.header.patches.len(), 2);
    }

    #[test]
    fn decoder_rejects_delta_gap_and_recovers_with_keyframe() {
        let mut encoder = FrameEncoder::new("desktop-1".into());
        let mut decoder = FrameDecoder::new("desktop-1".into());
        let mut raw = RawFrame {
            width: 160,
            height: 90,
            rgba: vec![10; 160 * 90 * 4],
        };
        let first = encoder.encode(&raw).unwrap().unwrap();
        encoder.commit(&raw);
        decoder.apply(first).unwrap();

        raw.rgba[0] = 20;
        let second = encoder.encode(&raw).unwrap().unwrap();
        encoder.commit(&raw);
        raw.rgba[0] = 30;
        let third = encoder.encode(&raw).unwrap().unwrap();
        encoder.commit(&raw);

        // 丢失第二帧后第三帧不能直接应用
        drop(second);
        assert!(decoder.apply(third).is_err());

        // 关键帧可以从任意序号恢复
        encoder.force_keyframe();
        let recovery = encoder.encode(&raw).unwrap().unwrap();
        encoder.commit(&raw);
        assert!(recovery.header.keyframe);
        decoder.apply(recovery).unwrap();
        assert_eq!(decoder.canvas(), raw.rgba);
    }

    #[test]
    fn maximum_keyframe_header_fits_stream_limit() {
        let mut encoder = FrameEncoder::new("desktop-1".into());
        let raw = RawFrame {
            width: 1280,
            height: 720,
            rgba: vec![0; 1280 * 720 * 4],
        };
        let frame = encoder.encode(&raw).unwrap().unwrap();
        assert_eq!(frame.header.patches.len(), 60);
        assert!(serde_json::to_vec(&frame.header).unwrap().len() <= 4 * 1024);
    }
}
