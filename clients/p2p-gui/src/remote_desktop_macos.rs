use std::collections::HashSet;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use block2::RcBlock;
use dispatch2::DispatchQueue;
use objc2::rc::Retained;
use objc2::runtime::ProtocolObject;
use objc2::{define_class, msg_send, AnyThread, DefinedClass};
use objc2_core_foundation::{CGPoint, CGRect};
use objc2_core_graphics::{
    CGDisplayBounds, CGDisplayPixelsHigh, CGDisplayPixelsWide, CGError, CGEvent, CGEventField,
    CGEventFlags, CGEventTapLocation, CGEventType, CGGetActiveDisplayList, CGMainDisplayID,
    CGMouseButton, CGPreflightPostEventAccess, CGPreflightScreenCaptureAccess,
    CGRequestPostEventAccess, CGRequestScreenCaptureAccess, CGScrollEventUnit,
};
use objc2_core_media::{CMSampleBuffer, CMTime};
use objc2_core_video::{
    kCVPixelFormatType_32BGRA, kCVReturnSuccess, CVPixelBufferGetBaseAddress,
    CVPixelBufferGetBytesPerRow, CVPixelBufferGetHeight, CVPixelBufferGetPixelFormatType,
    CVPixelBufferGetWidth, CVPixelBufferLockBaseAddress, CVPixelBufferLockFlags,
    CVPixelBufferUnlockBaseAddress,
};
use objc2_foundation::{NSArray, NSError, NSObject, NSObjectProtocol};
use objc2_screen_capture_kit::{
    SCContentFilter, SCDisplay, SCShareableContent, SCStream, SCStreamConfiguration,
    SCStreamDelegate, SCStreamOutput, SCStreamOutputType, SCWindow,
};
use p2p_core::remote_desktop::{
    RemoteDesktopConfig, RemoteDisplay, RemoteInputEvent, RemotePointerButton,
};

use super::RawFrame;

const MAX_DISPLAYS: usize = 32;
const CAPTURE_START_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct CaptureState {
    frame: Option<RawFrame>,
    error: Option<String>,
}

struct StreamOutputIvars {
    state: Arc<Mutex<CaptureState>>,
}

define_class!(
    // SAFETY: NSObject has no subclassing requirements. The ivars are synchronized and the
    // object does not implement Drop; ScreenCaptureKit may call it from its dispatch queue.
    #[unsafe(super = NSObject)]
    #[name = "P2PRemoteDesktopStreamOutput"]
    #[ivars = StreamOutputIvars]
    struct StreamOutput;

    // SAFETY: NSObjectProtocol has no additional requirements.
    unsafe impl NSObjectProtocol for StreamOutput {}

    // SAFETY: The selector and parameter types match SCStreamOutput.
    #[allow(non_snake_case)]
    unsafe impl SCStreamOutput for StreamOutput {
        #[unsafe(method(stream:didOutputSampleBuffer:ofType:))]
        unsafe fn stream_didOutputSampleBuffer_ofType(
            &self,
            _stream: &SCStream,
            sample_buffer: &CMSampleBuffer,
            output_type: SCStreamOutputType,
        ) {
            if output_type != SCStreamOutputType::Screen {
                return;
            }
            match unsafe { raw_frame_from_sample(sample_buffer) } {
                Ok(Some(frame)) => lock_state(&self.ivars().state).frame = Some(frame),
                Ok(None) => {}
                Err(error) => lock_state(&self.ivars().state).error = Some(format!("{error:#}")),
            }
        }
    }

    // SAFETY: The selector and parameter types match SCStreamDelegate.
    #[allow(non_snake_case)]
    unsafe impl SCStreamDelegate for StreamOutput {
        #[unsafe(method(stream:didStopWithError:))]
        unsafe fn stream_didStopWithError(&self, _stream: &SCStream, error: &NSError) {
            lock_state(&self.ivars().state).error = Some(error.to_string());
        }
    }
);

impl StreamOutput {
    fn new(state: Arc<Mutex<CaptureState>>) -> Retained<Self> {
        let this = Self::alloc().set_ivars(StreamOutputIvars { state });
        // SAFETY: NSObject's init signature is stable and this is a fresh allocation.
        unsafe { msg_send![super(this), init] }
    }
}

fn lock_state(state: &Mutex<CaptureState>) -> std::sync::MutexGuard<'_, CaptureState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub fn ensure_screen_capture_permission() -> Result<()> {
    if CGPreflightScreenCaptureAccess() || CGRequestScreenCaptureAccess() {
        Ok(())
    } else {
        anyhow::bail!("macOS 未授予屏幕录制权限；请在系统设置中授权后重启应用")
    }
}

pub fn ensure_input_permission() -> Result<()> {
    if CGPreflightPostEventAccess() || CGRequestPostEventAccess() {
        Ok(())
    } else {
        anyhow::bail!("macOS 未授予辅助功能控制权限")
    }
}

pub fn available_displays() -> Result<Vec<RemoteDisplay>> {
    let ids = active_display_ids()?;
    let main = CGMainDisplayID();
    Ok(ids
        .into_iter()
        .enumerate()
        .map(|(index, id)| RemoteDisplay {
            id: id.to_string(),
            name: if id == main {
                "主显示器".to_string()
            } else {
                format!("显示器 {}", index + 1)
            },
            width: CGDisplayPixelsWide(id).min(u32::MAX as usize) as u32,
            height: CGDisplayPixelsHigh(id).min(u32::MAX as usize) as u32,
        })
        .collect())
}

fn active_display_ids() -> Result<Vec<u32>> {
    let mut ids = [0_u32; MAX_DISPLAYS];
    let mut count = 0_u32;
    // SAFETY: Both pointers refer to writable storage sized for max_displays entries.
    let result =
        unsafe { CGGetActiveDisplayList(MAX_DISPLAYS as u32, ids.as_mut_ptr(), &mut count) };
    if result != CGError::Success {
        anyhow::bail!("枚举 macOS 显示器失败：{}", result.0)
    }
    Ok(ids[..count.min(MAX_DISPLAYS as u32) as usize].to_vec())
}

pub struct Capture {
    stream: Retained<SCStream>,
    _output: Retained<StreamOutput>,
    _queue: dispatch2::DispatchRetained<DispatchQueue>,
    state: Arc<Mutex<CaptureState>>,
}

impl Capture {
    pub fn new(display_id: &str, config: RemoteDesktopConfig) -> Result<Self> {
        config.validate()?;
        ensure_screen_capture_permission()?;
        let display_id = display_id
            .parse::<u32>()
            .with_context(|| format!("macOS 显示器 ID 无效：{display_id}"))?;
        let display = shareable_display(display_id)?;
        let excluded = NSArray::<SCWindow>::new();
        // SAFETY: The display and exclusion array are retained for initialization.
        let filter = unsafe {
            SCContentFilter::initWithDisplay_excludingWindows(
                SCContentFilter::alloc(),
                &display,
                &excluded,
            )
        };
        // SAFETY: SCStreamConfiguration supports the default initializer.
        let stream_config = unsafe { SCStreamConfiguration::new() };
        // SAFETY: All values satisfy RemoteDesktopConfig validation and ScreenCaptureKit ranges.
        unsafe {
            stream_config.setWidth(config.width as usize);
            stream_config.setHeight(config.height as usize);
            stream_config.setMinimumFrameInterval(CMTime::new(1, config.max_fps as i32));
            stream_config.setPixelFormat(kCVPixelFormatType_32BGRA);
            stream_config.setQueueDepth(2);
            stream_config.setShowsCursor(true);
        }

        let state = Arc::new(Mutex::new(CaptureState::default()));
        let output = StreamOutput::new(state.clone());
        let output_protocol = ProtocolObject::<dyn SCStreamOutput>::from_ref(&*output);
        let delegate_protocol = ProtocolObject::<dyn SCStreamDelegate>::from_ref(&*output);
        // SAFETY: Filter, configuration and delegate are valid retained Objective-C objects.
        let stream = unsafe {
            SCStream::initWithFilter_configuration_delegate(
                SCStream::alloc(),
                &filter,
                &stream_config,
                Some(delegate_protocol),
            )
        };
        let queue = DispatchQueue::new("studio.yizhe.p2p-signaling.screen-capture", None);
        // SAFETY: The output remains retained by Capture for the stream's full lifetime.
        unsafe {
            stream
                .addStreamOutput_type_sampleHandlerQueue_error(
                    output_protocol,
                    SCStreamOutputType::Screen,
                    Some(&queue),
                )
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }
        start_capture(&stream)?;
        Ok(Self {
            stream,
            _output: output,
            _queue: queue,
            state,
        })
    }

    pub fn capture(&mut self) -> Result<Option<RawFrame>> {
        let mut state = lock_state(&self.state);
        if let Some(error) = state.error.take() {
            anyhow::bail!(error)
        }
        Ok(state.frame.take())
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        let (tx, rx) = mpsc::channel();
        let completion = RcBlock::new(move |_error: *mut NSError| {
            let _ = tx.send(());
        });
        // SAFETY: The stream is valid and the completion block outlives this call.
        unsafe {
            self.stream
                .stopCaptureWithCompletionHandler(Some(&completion))
        };
        let _ = rx.recv_timeout(Duration::from_secs(2));
    }
}

fn shareable_display(display_id: u32) -> Result<Retained<SCDisplay>> {
    let (tx, rx) = mpsc::channel();
    let completion = RcBlock::new(
        move |content: *mut SCShareableContent, error: *mut NSError| {
            let result = if let Some(error) = unsafe { error.as_ref() } {
                Err(error.to_string())
            } else if let Some(content) = unsafe { content.as_ref() } {
                let displays = unsafe { content.displays() };
                let mut found = None;
                for index in 0..displays.count() {
                    let display = displays.objectAtIndex(index);
                    if unsafe { display.displayID() } == display_id {
                        found = Some(display);
                        break;
                    }
                }
                found.ok_or_else(|| format!("找不到可采集显示器：{display_id}"))
            } else {
                Err("ScreenCaptureKit 未返回可共享内容".to_string())
            };
            let _ = tx.send(result);
        },
    );
    // SAFETY: The completion block is retained by ScreenCaptureKit until invocation.
    unsafe {
        SCShareableContent::getShareableContentExcludingDesktopWindows_onScreenWindowsOnly_completionHandler(
            false,
            true,
            &completion,
        )
    };
    rx.recv_timeout(CAPTURE_START_TIMEOUT)
        .context("获取 macOS 可共享屏幕超时")?
        .map_err(anyhow::Error::msg)
}

fn start_capture(stream: &SCStream) -> Result<()> {
    let (tx, rx) = mpsc::channel();
    let completion = RcBlock::new(move |error: *mut NSError| {
        let result = unsafe { error.as_ref() }
            .map(ToString::to_string)
            .map_or(Ok(()), Err);
        let _ = tx.send(result);
    });
    // SAFETY: The completion block is retained by ScreenCaptureKit until invocation.
    unsafe { stream.startCaptureWithCompletionHandler(Some(&completion)) };
    rx.recv_timeout(CAPTURE_START_TIMEOUT)
        .context("启动 macOS 屏幕采集超时")?
        .map_err(anyhow::Error::msg)
}

unsafe fn raw_frame_from_sample(sample: &CMSampleBuffer) -> Result<Option<RawFrame>> {
    if !unsafe { sample.is_valid() } || !unsafe { sample.data_is_ready() } {
        return Ok(None);
    }
    let pixel = unsafe { sample.image_buffer() }.context("屏幕帧缺少像素缓冲")?;
    if CVPixelBufferGetPixelFormatType(&pixel) != kCVPixelFormatType_32BGRA {
        anyhow::bail!("ScreenCaptureKit 返回了非 BGRA 像素格式")
    }
    let flags = CVPixelBufferLockFlags::ReadOnly;
    let lock_result = CVPixelBufferLockBaseAddress(&pixel, flags);
    if lock_result != kCVReturnSuccess {
        anyhow::bail!("锁定屏幕像素缓冲失败：{lock_result}")
    }

    let result = (|| {
        let width = CVPixelBufferGetWidth(&pixel);
        let height = CVPixelBufferGetHeight(&pixel);
        let row_bytes = CVPixelBufferGetBytesPerRow(&pixel);
        let data_len = row_bytes
            .checked_mul(height)
            .context("屏幕像素缓冲长度溢出")?;
        let base = CVPixelBufferGetBaseAddress(&pixel).cast::<u8>();
        if base.is_null() {
            anyhow::bail!("屏幕像素缓冲地址为空")
        }
        // SAFETY: The pixel buffer is locked and reports data_len accessible bytes.
        let source = unsafe { std::slice::from_raw_parts(base, data_len) };
        let rgba = bgra_rows_to_rgba(width, height, row_bytes, source)?;
        Ok(RawFrame {
            width: u32::try_from(width).context("屏幕宽度超出范围")?,
            height: u32::try_from(height).context("屏幕高度超出范围")?,
            rgba,
        })
    })();
    let unlock_result = CVPixelBufferUnlockBaseAddress(&pixel, flags);
    if unlock_result != kCVReturnSuccess {
        return Err(anyhow::anyhow!("解锁屏幕像素缓冲失败：{unlock_result}"));
    }
    result.map(Some)
}

fn bgra_rows_to_rgba(
    width: usize,
    height: usize,
    row_bytes: usize,
    source: &[u8],
) -> Result<Vec<u8>> {
    let pixel_bytes = width.checked_mul(4).context("屏幕行长度溢出")?;
    if row_bytes < pixel_bytes || source.len() < row_bytes.saturating_mul(height) {
        anyhow::bail!("屏幕像素缓冲尺寸无效")
    }
    let mut rgba = vec![0_u8; pixel_bytes.saturating_mul(height)];
    for y in 0..height {
        let source_row = &source[y * row_bytes..y * row_bytes + pixel_bytes];
        let target_row = &mut rgba[y * pixel_bytes..(y + 1) * pixel_bytes];
        for (bgra, rgba) in source_row
            .chunks_exact(4)
            .zip(target_row.chunks_exact_mut(4))
        {
            rgba.copy_from_slice(&[bgra[2], bgra[1], bgra[0], bgra[3]]);
        }
    }
    Ok(rgba)
}

const DOUBLE_CLICK_INTERVAL: Duration = Duration::from_millis(500);
const DOUBLE_CLICK_SLOP: f64 = 5.0;

#[derive(Debug, Clone, Copy)]
struct LastClick {
    at: Instant,
    x: f64,
    y: f64,
    button: RemotePointerButton,
    count: i64,
}

pub struct InputInjector {
    bounds: CGRect,
    current: CGPoint,
    pressed_keys: HashSet<(u16, bool)>,
    pressed_buttons: HashSet<RemotePointerButton>,
    last_click: Option<LastClick>,
    click_count: i64,
}

impl InputInjector {
    pub fn new(display_id: &str) -> Result<Self> {
        ensure_input_permission()?;
        let display_id = display_id
            .parse::<u32>()
            .with_context(|| format!("macOS 显示器 ID 无效：{display_id}"))?;
        if !active_display_ids()?.contains(&display_id) {
            anyhow::bail!("找不到远程控制显示器：{display_id}")
        }
        let bounds = CGDisplayBounds(display_id);
        let current = CGPoint::new(
            bounds.origin.x + bounds.size.width / 2.0,
            bounds.origin.y + bounds.size.height / 2.0,
        );
        Ok(Self {
            bounds,
            current,
            pressed_keys: HashSet::new(),
            pressed_buttons: HashSet::new(),
            last_click: None,
            click_count: 1,
        })
    }

    pub fn inject(&mut self, event: &RemoteInputEvent) -> Result<()> {
        if !CGPreflightPostEventAccess() {
            anyhow::bail!("macOS 辅助功能控制权限已失效")
        }
        match event {
            RemoteInputEvent::PointerMove { x, y } => self.pointer_move(*x, *y),
            RemoteInputEvent::PointerButton { button, pressed } => {
                self.pointer_button(*button, *pressed)
            }
            RemoteInputEvent::Wheel { horizontal, delta } => {
                let (vertical, horizontal) = if *horizontal {
                    (0, *delta)
                } else {
                    (*delta, 0)
                };
                let event = CGEvent::new_scroll_wheel_event2(
                    None,
                    CGScrollEventUnit::Pixel,
                    2,
                    vertical,
                    horizontal,
                    0,
                )
                .context("创建 macOS 滚轮事件失败")?;
                CGEvent::set_flags(Some(&event), self.modifier_flags());
                CGEvent::post(CGEventTapLocation::HIDEventTap, Some(&event));
                Ok(())
            }
            RemoteInputEvent::Key {
                scan_code,
                extended,
                pressed,
            } => self.key(*scan_code, *extended, *pressed),
            RemoteInputEvent::Text { text } => self.text(text),
            RemoteInputEvent::ReleaseAll => self.release_all(),
        }
    }

    fn pointer_move(&mut self, x: u16, y: u16) -> Result<()> {
        self.current = normalized_point(self.bounds, x, y);
        let (event_type, button) = if self.pressed_buttons.contains(&RemotePointerButton::Left) {
            (CGEventType::LeftMouseDragged, CGMouseButton::Left)
        } else if self.pressed_buttons.contains(&RemotePointerButton::Right) {
            (CGEventType::RightMouseDragged, CGMouseButton::Right)
        } else if self.pressed_buttons.contains(&RemotePointerButton::Middle) {
            (CGEventType::OtherMouseDragged, CGMouseButton::Center)
        } else {
            (CGEventType::MouseMoved, CGMouseButton::Left)
        };
        self.post_mouse(event_type, button)
    }

    fn pointer_button(&mut self, button: RemotePointerButton, pressed: bool) -> Result<()> {
        let (event_type, cg_button) = match (button, pressed) {
            (RemotePointerButton::Left, true) => (CGEventType::LeftMouseDown, CGMouseButton::Left),
            (RemotePointerButton::Left, false) => (CGEventType::LeftMouseUp, CGMouseButton::Left),
            (RemotePointerButton::Right, true) => {
                (CGEventType::RightMouseDown, CGMouseButton::Right)
            }
            (RemotePointerButton::Right, false) => {
                (CGEventType::RightMouseUp, CGMouseButton::Right)
            }
            (RemotePointerButton::Middle, true) => {
                (CGEventType::OtherMouseDown, CGMouseButton::Center)
            }
            (RemotePointerButton::Middle, false) => {
                (CGEventType::OtherMouseUp, CGMouseButton::Center)
            }
        };
        if pressed {
            let now = Instant::now();
            self.click_count =
                next_click_count(self.last_click.as_ref(), now, self.current, button);
            self.last_click = Some(LastClick {
                at: now,
                x: self.current.x,
                y: self.current.y,
                button,
                count: self.click_count,
            });
        }
        self.post_mouse(event_type, cg_button)?;
        if pressed {
            self.pressed_buttons.insert(button);
        } else {
            self.pressed_buttons.remove(&button);
        }
        Ok(())
    }

    fn post_mouse(&self, event_type: CGEventType, button: CGMouseButton) -> Result<()> {
        let event = CGEvent::new_mouse_event(None, event_type, self.current, button)
            .context("创建 macOS 鼠标事件失败")?;
        // 双击识别依赖 clickState；纯移动事件（MouseMoved）无需设置
        if event_type != CGEventType::MouseMoved {
            CGEvent::set_integer_value_field(
                Some(&event),
                CGEventField::MouseEventClickState,
                self.click_count,
            );
        }
        CGEvent::set_flags(Some(&event), self.modifier_flags());
        CGEvent::post(CGEventTapLocation::HIDEventTap, Some(&event));
        Ok(())
    }

    fn key(&mut self, scan_code: u16, extended: bool, pressed: bool) -> Result<()> {
        let key_code = scan_code_to_macos(scan_code, extended)
            .ok_or_else(|| anyhow::anyhow!("不支持的远程扫描码：{scan_code:#x}"))?;
        // 先更新按键集合再取 flags：修饰键按下事件携带自身掩码、抬起事件不携带，
        // 与 macOS flagsChanged 的语义一致
        if pressed {
            self.pressed_keys.insert((scan_code, extended));
        } else {
            self.pressed_keys.remove(&(scan_code, extended));
        }
        let event = CGEvent::new_keyboard_event(None, key_code, pressed)
            .context("创建 macOS 键盘事件失败")?;
        CGEvent::set_flags(Some(&event), self.modifier_flags());
        CGEvent::post(CGEventTapLocation::HIDEventTap, Some(&event));
        Ok(())
    }

    fn text(&self, text: &str) -> Result<()> {
        let units = text.encode_utf16().collect::<Vec<_>>();
        for pressed in [true, false] {
            let event =
                CGEvent::new_keyboard_event(None, 0, pressed).context("创建 macOS 文本事件失败")?;
            // SAFETY: units points to a valid UTF-16 buffer for the duration of the call.
            unsafe {
                CGEvent::keyboard_set_unicode_string(
                    Some(&event),
                    units.len() as u64,
                    units.as_ptr(),
                )
            };
            CGEvent::set_flags(Some(&event), self.modifier_flags());
            CGEvent::post(CGEventTapLocation::HIDEventTap, Some(&event));
        }
        Ok(())
    }

    fn modifier_flags(&self) -> CGEventFlags {
        flags_for_pressed(&self.pressed_keys)
    }

    fn release_all(&mut self) -> Result<()> {
        let keys = self.pressed_keys.drain().collect::<Vec<_>>();
        let buttons = self.pressed_buttons.drain().collect::<Vec<_>>();
        for (scan_code, extended) in keys {
            self.key(scan_code, extended, false)?;
        }
        for button in buttons {
            self.pointer_button(button, false)?;
        }
        Ok(())
    }
}

fn normalized_point(bounds: CGRect, x: u16, y: u16) -> CGPoint {
    // 与 Windows 端一致按 (宽高 - 1) 缩放，65535 恰好落在显示器最后一像素上
    CGPoint::new(
        bounds.origin.x + x as f64 * (bounds.size.width - 1.0).max(0.0) / 65535.0,
        bounds.origin.y + y as f64 * (bounds.size.height - 1.0).max(0.0) / 65535.0,
    )
}

fn flags_for_pressed(pressed_keys: &HashSet<(u16, bool)>) -> CGEventFlags {
    let mut flags = CGEventFlags::empty();
    for &(scan_code, extended) in pressed_keys {
        match (scan_code, extended) {
            (0x2A | 0x36, false) => flags |= CGEventFlags::MaskShift,
            (0x1D, _) => flags |= CGEventFlags::MaskControl,
            (0x38, _) => flags |= CGEventFlags::MaskAlternate,
            (0x5B | 0x5C, true) => flags |= CGEventFlags::MaskCommand,
            _ => {}
        }
    }
    flags
}

fn next_click_count(
    previous: Option<&LastClick>,
    now: Instant,
    point: CGPoint,
    button: RemotePointerButton,
) -> i64 {
    match previous {
        Some(last)
            if last.button == button
                && now.duration_since(last.at) <= DOUBLE_CLICK_INTERVAL
                && (point.x - last.x).abs() <= DOUBLE_CLICK_SLOP
                && (point.y - last.y).abs() <= DOUBLE_CLICK_SLOP =>
        {
            last.count + 1
        }
        _ => 1,
    }
}

fn scan_code_to_macos(scan_code: u16, extended: bool) -> Option<u16> {
    if extended {
        return Some(match scan_code {
            0x1D => 0x3E,
            0x38 => 0x3D,
            0x47 => 0x73,
            0x48 => 0x7E,
            0x49 => 0x74,
            0x4B => 0x7B,
            0x4D => 0x7C,
            0x4F => 0x77,
            0x50 => 0x7D,
            0x51 => 0x79,
            0x52 => 0x72,
            0x53 => 0x75,
            0x5B => 0x37,
            0x5C => 0x36,
            _ => return None,
        });
    }
    Some(match scan_code {
        0x01 => 0x35,
        0x02 => 0x12,
        0x03 => 0x13,
        0x04 => 0x14,
        0x05 => 0x15,
        0x06 => 0x17,
        0x07 => 0x16,
        0x08 => 0x1A,
        0x09 => 0x1C,
        0x0A => 0x19,
        0x0B => 0x1D,
        0x0C => 0x1B,
        0x0D => 0x18,
        0x0E => 0x33,
        0x0F => 0x30,
        0x10 => 0x0C,
        0x11 => 0x0D,
        0x12 => 0x0E,
        0x13 => 0x0F,
        0x14 => 0x11,
        0x15 => 0x10,
        0x16 => 0x20,
        0x17 => 0x22,
        0x18 => 0x1F,
        0x19 => 0x23,
        0x1A => 0x21,
        0x1B => 0x1E,
        0x1C => 0x24,
        0x1D => 0x3B,
        0x1E => 0x00,
        0x1F => 0x01,
        0x20 => 0x02,
        0x21 => 0x03,
        0x22 => 0x05,
        0x23 => 0x04,
        0x24 => 0x26,
        0x25 => 0x28,
        0x26 => 0x25,
        0x27 => 0x29,
        0x28 => 0x27,
        0x29 => 0x32,
        0x2A => 0x38,
        0x2B => 0x2A,
        0x2C => 0x06,
        0x2D => 0x07,
        0x2E => 0x08,
        0x2F => 0x09,
        0x30 => 0x0B,
        0x31 => 0x2D,
        0x32 => 0x2E,
        0x33 => 0x2B,
        0x34 => 0x2F,
        0x35 => 0x2C,
        0x36 => 0x3C,
        0x38 => 0x3A,
        0x39 => 0x31,
        0x3B => 0x7A,
        0x3C => 0x78,
        0x3D => 0x63,
        0x3E => 0x76,
        0x3F => 0x60,
        0x40 => 0x61,
        0x41 => 0x62,
        0x42 => 0x64,
        0x43 => 0x65,
        0x44 => 0x6D,
        0x57 => 0x67,
        0x58 => 0x6F,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_padded_bgra_rows() {
        let source = [
            1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 9, 10, 11, 12, 13, 14, 15, 16, 0, 0,
        ];
        assert_eq!(
            bgra_rows_to_rgba(2, 2, 10, &source).unwrap(),
            [3, 2, 1, 4, 7, 6, 5, 8, 11, 10, 9, 12, 15, 14, 13, 16]
        );
    }

    #[test]
    fn maps_pc_scan_codes_to_macos_keys() {
        assert_eq!(scan_code_to_macos(0x1E, false), Some(0x00));
        assert_eq!(scan_code_to_macos(0x48, true), Some(0x7E));
        assert_eq!(scan_code_to_macos(0x5B, true), Some(0x37));
        assert_eq!(scan_code_to_macos(0x7F, false), None);
    }

    #[test]
    fn maps_punctuation_scan_codes() {
        // 减号、等号、分号、引号、逗号、句号、斜杠、反斜杠、反引号、方括号
        for (pc, mac) in [
            (0x0C_u16, 0x1B_u16),
            (0x0D, 0x18),
            (0x1A, 0x21),
            (0x1B, 0x1E),
            (0x27, 0x29),
            (0x28, 0x27),
            (0x29, 0x32),
            (0x2B, 0x2A),
            (0x33, 0x2B),
            (0x34, 0x2F),
            (0x35, 0x2C),
        ] {
            assert_eq!(scan_code_to_macos(pc, false), Some(mac), "扫描码 {pc:#x}");
        }
    }

    #[test]
    fn maps_normalized_points_into_offset_display() {
        let bounds = CGRect::new(
            CGPoint::new(-1920.0, 100.0),
            objc2_core_foundation::CGSize::new(1920.0, 1080.0),
        );
        assert_eq!(normalized_point(bounds, 0, 0), CGPoint::new(-1920.0, 100.0));
        assert_eq!(
            normalized_point(bounds, 65535, 65535),
            CGPoint::new(-1.0, 1179.0)
        );
    }

    #[test]
    fn accumulates_modifier_flags_from_pressed_keys() {
        let mut pressed = HashSet::new();
        assert_eq!(flags_for_pressed(&pressed), CGEventFlags::empty());
        pressed.insert((0x1D, false));
        pressed.insert((0x2A, false));
        assert_eq!(
            flags_for_pressed(&pressed),
            CGEventFlags::MaskControl | CGEventFlags::MaskShift
        );
        pressed.insert((0x5B, true));
        pressed.insert((0x38, false));
        pressed.insert((0x2E, false));
        assert_eq!(
            flags_for_pressed(&pressed),
            CGEventFlags::MaskControl
                | CGEventFlags::MaskShift
                | CGEventFlags::MaskCommand
                | CGEventFlags::MaskAlternate
        );
        // 非扩展 0x5B 不是 Command
        assert_eq!(
            flags_for_pressed(&HashSet::from([(0x5B, false)])),
            CGEventFlags::empty()
        );
    }

    #[test]
    fn counts_double_clicks_within_threshold() {
        let start = Instant::now();
        let point = CGPoint::new(10.0, 10.0);
        let first = next_click_count(None, start, point, RemotePointerButton::Left);
        assert_eq!(first, 1);
        let last = LastClick {
            at: start,
            x: point.x,
            y: point.y,
            button: RemotePointerButton::Left,
            count: first,
        };
        assert_eq!(
            next_click_count(
                Some(&last),
                start + Duration::from_millis(200),
                CGPoint::new(12.0, 9.0),
                RemotePointerButton::Left,
            ),
            2
        );
        // 超时、移动过远、按键不同都会重置
        assert_eq!(
            next_click_count(
                Some(&last),
                start + Duration::from_millis(800),
                point,
                RemotePointerButton::Left,
            ),
            1
        );
        assert_eq!(
            next_click_count(
                Some(&last),
                start + Duration::from_millis(200),
                CGPoint::new(30.0, 10.0),
                RemotePointerButton::Left,
            ),
            1
        );
        assert_eq!(
            next_click_count(
                Some(&last),
                start + Duration::from_millis(200),
                point,
                RemotePointerButton::Right,
            ),
            1
        );
    }

    #[test]
    #[ignore = "requires an interactive macOS desktop and Screen Recording permission"]
    fn captures_first_display() {
        let display = available_displays()
            .unwrap()
            .into_iter()
            .next()
            .expect("at least one active display");
        let scale = (1280.0_f64 / display.width as f64)
            .min(720.0_f64 / display.height as f64)
            .min(1.0);
        let config = RemoteDesktopConfig {
            width: (display.width as f64 * scale).round() as u32,
            height: (display.height as f64 * scale).round() as u32,
            max_fps: 15,
        };
        let mut capture = Capture::new(&display.id, config).unwrap();
        let frame = (0..120)
            .find_map(|_| {
                let frame = capture.capture().transpose();
                std::thread::sleep(Duration::from_millis(16));
                frame
            })
            .expect("capture result")
            .expect("desktop frame");
        assert_eq!(frame.width, config.width);
        assert_eq!(frame.height, config.height);
        assert_eq!(
            frame.rgba.len(),
            (config.width * config.height * 4) as usize
        );
    }
}
