use std::collections::HashSet;
use std::mem::{size_of, zeroed};

use anyhow::{Context, Result};
use image::{imageops::FilterType, RgbaImage};
use p2p_core::remote_desktop::{
    RemoteDesktopConfig, RemoteDisplay, RemoteInputEvent, RemotePointerButton,
};
use windows::core::Interface;
use windows::Win32::Foundation::{HMODULE, RECT};
use windows::Win32::Graphics::Direct3D::{D3D_DRIVER_TYPE_UNKNOWN, D3D_FEATURE_LEVEL};
use windows::Win32::Graphics::Direct3D11::{
    D3D11CreateDevice, ID3D11Device, ID3D11DeviceContext, ID3D11Texture2D, D3D11_CPU_ACCESS_READ,
    D3D11_CREATE_DEVICE_BGRA_SUPPORT, D3D11_MAPPED_SUBRESOURCE, D3D11_MAP_READ, D3D11_SDK_VERSION,
    D3D11_TEXTURE2D_DESC, D3D11_USAGE_STAGING,
};
use windows::Win32::Graphics::Dxgi::Common::DXGI_SAMPLE_DESC;
use windows::Win32::Graphics::Dxgi::{
    CreateDXGIFactory1, IDXGIAdapter, IDXGIAdapter1, IDXGIFactory1, IDXGIOutput, IDXGIOutput1,
    IDXGIOutputDuplication, IDXGIResource, DXGI_ERROR_ACCESS_LOST, DXGI_ERROR_NOT_FOUND,
    DXGI_ERROR_WAIT_TIMEOUT, DXGI_OUTDUPL_FRAME_INFO, DXGI_OUTPUT_DESC,
};
use windows::Win32::UI::Input::KeyboardAndMouse::{
    SendInput, INPUT, INPUT_0, INPUT_KEYBOARD, INPUT_MOUSE, KEYBDINPUT, KEYBD_EVENT_FLAGS,
    KEYEVENTF_EXTENDEDKEY, KEYEVENTF_KEYUP, KEYEVENTF_SCANCODE, KEYEVENTF_UNICODE,
    MOUSEEVENTF_ABSOLUTE, MOUSEEVENTF_HWHEEL, MOUSEEVENTF_LEFTDOWN, MOUSEEVENTF_LEFTUP,
    MOUSEEVENTF_MIDDLEDOWN, MOUSEEVENTF_MIDDLEUP, MOUSEEVENTF_MOVE, MOUSEEVENTF_RIGHTDOWN,
    MOUSEEVENTF_RIGHTUP, MOUSEEVENTF_VIRTUALDESK, MOUSEEVENTF_WHEEL, MOUSEINPUT, MOUSE_EVENT_FLAGS,
    VIRTUAL_KEY,
};
use windows::Win32::UI::WindowsAndMessaging::{
    GetSystemMetrics, SM_CXVIRTUALSCREEN, SM_CYVIRTUALSCREEN, SM_XVIRTUALSCREEN, SM_YVIRTUALSCREEN,
};

use super::RawFrame;

struct OutputEntry {
    id: String,
    adapter: IDXGIAdapter1,
    output: IDXGIOutput,
    desc: DXGI_OUTPUT_DESC,
}

pub fn available_displays() -> Result<Vec<RemoteDisplay>> {
    Ok(enumerate_outputs()?
        .into_iter()
        .map(|entry| {
            let bounds = entry.desc.DesktopCoordinates;
            RemoteDisplay {
                id: entry.id,
                name: wide_name(&entry.desc.DeviceName),
                width: (bounds.right - bounds.left).max(0) as u32,
                height: (bounds.bottom - bounds.top).max(0) as u32,
            }
        })
        .collect())
}

fn enumerate_outputs() -> Result<Vec<OutputEntry>> {
    let factory: IDXGIFactory1 = unsafe { CreateDXGIFactory1()? };
    let mut entries = Vec::new();
    for adapter_index in 0_u32.. {
        let adapter = match unsafe { factory.EnumAdapters1(adapter_index) } {
            Ok(adapter) => adapter,
            Err(error) if error.code() == DXGI_ERROR_NOT_FOUND => break,
            Err(error) => return Err(error.into()),
        };
        let base_adapter: IDXGIAdapter = adapter.cast()?;
        for output_index in 0_u32.. {
            let output = match unsafe { base_adapter.EnumOutputs(output_index) } {
                Ok(output) => output,
                Err(error) if error.code() == DXGI_ERROR_NOT_FOUND => break,
                Err(error) => return Err(error.into()),
            };
            let desc = unsafe { output.GetDesc()? };
            if !desc.AttachedToDesktop.as_bool() {
                continue;
            }
            entries.push(OutputEntry {
                id: format!("{adapter_index}:{output_index}"),
                adapter: adapter.clone(),
                output,
                desc,
            });
        }
    }
    Ok(entries)
}

fn wide_name(value: &[u16]) -> String {
    let len = value
        .iter()
        .position(|value| *value == 0)
        .unwrap_or(value.len());
    String::from_utf16_lossy(&value[..len])
}

pub struct Capture {
    device: ID3D11Device,
    context: ID3D11DeviceContext,
    duplication: IDXGIOutputDuplication,
    staging: Option<ID3D11Texture2D>,
    staging_width: u32,
    staging_height: u32,
    bounds: RECT,
    config: RemoteDesktopConfig,
}

impl Capture {
    pub fn new(display_id: &str, config: RemoteDesktopConfig) -> Result<Self> {
        config.validate()?;
        let entry = enumerate_outputs()?
            .into_iter()
            .find(|entry| entry.id == display_id)
            .ok_or_else(|| anyhow::anyhow!("找不到显示器：{display_id}"))?;
        let adapter: IDXGIAdapter = entry.adapter.cast()?;
        let mut device = None;
        let mut context = None;
        let mut feature_level = D3D_FEATURE_LEVEL::default();
        unsafe {
            D3D11CreateDevice(
                &adapter,
                D3D_DRIVER_TYPE_UNKNOWN,
                HMODULE::default(),
                D3D11_CREATE_DEVICE_BGRA_SUPPORT,
                None,
                D3D11_SDK_VERSION,
                Some(&mut device),
                Some(&mut feature_level),
                Some(&mut context),
            )?;
        }
        let device = device.context("创建 D3D11 设备失败")?;
        let output: IDXGIOutput1 = entry.output.cast()?;
        let duplication = unsafe { output.DuplicateOutput(&device)? };
        Ok(Self {
            device,
            context: context.context("创建 D3D11 上下文失败")?,
            duplication,
            staging: None,
            staging_width: 0,
            staging_height: 0,
            bounds: entry.desc.DesktopCoordinates,
            config,
        })
    }

    pub fn capture(&mut self) -> Result<Option<RawFrame>> {
        let mut frame_info = DXGI_OUTDUPL_FRAME_INFO::default();
        let mut resource: Option<IDXGIResource> = None;
        match unsafe {
            self.duplication
                .AcquireNextFrame(16, &mut frame_info, &mut resource)
        } {
            Ok(()) => {}
            Err(error) if error.code() == DXGI_ERROR_WAIT_TIMEOUT => return Ok(None),
            Err(error) if error.code() == DXGI_ERROR_ACCESS_LOST => {
                return Err(error).context("屏幕采集访问已失效")
            }
            Err(error) => return Err(error).context("获取屏幕帧失败"),
        }

        let result = self.copy_acquired_frame(resource, &frame_info);
        let release = unsafe { self.duplication.ReleaseFrame() };
        result.and_then(|frame| {
            release.context("释放屏幕帧失败")?;
            Ok(Some(frame))
        })
    }

    fn copy_acquired_frame(
        &mut self,
        resource: Option<IDXGIResource>,
        frame_info: &DXGI_OUTDUPL_FRAME_INFO,
    ) -> Result<RawFrame> {
        let resource = resource.context("屏幕帧缺少 DXGI 资源")?;
        let texture: ID3D11Texture2D = resource.cast()?;
        let mut desc = D3D11_TEXTURE2D_DESC::default();
        unsafe { texture.GetDesc(&mut desc) };
        self.ensure_staging(&desc)?;
        let staging = self.staging.as_ref().context("屏幕暂存纹理未创建")?;
        unsafe { self.context.CopyResource(staging, &texture) };

        let mut mapped: D3D11_MAPPED_SUBRESOURCE = unsafe { zeroed() };
        unsafe {
            self.context
                .Map(staging, 0, D3D11_MAP_READ, 0, Some(&mut mapped))?;
        }
        let mut rgba = vec![0_u8; (desc.Width * desc.Height * 4) as usize];
        for y in 0..desc.Height as usize {
            let source = unsafe {
                std::slice::from_raw_parts(
                    (mapped.pData as *const u8).add(y * mapped.RowPitch as usize),
                    desc.Width as usize * 4,
                )
            };
            let destination =
                &mut rgba[y * desc.Width as usize * 4..(y + 1) * desc.Width as usize * 4];
            for (source, destination) in source.chunks_exact(4).zip(destination.chunks_exact_mut(4))
            {
                destination.copy_from_slice(&[source[2], source[1], source[0], 255]);
            }
        }
        unsafe { self.context.Unmap(staging, 0) };

        let source =
            RgbaImage::from_raw(desc.Width, desc.Height, rgba).context("构造屏幕图像失败")?;
        let mut resized = if desc.Width == self.config.width && desc.Height == self.config.height {
            source
        } else {
            image::imageops::resize(
                &source,
                self.config.width,
                self.config.height,
                FilterType::Triangle,
            )
        };
        if frame_info.PointerPosition.Visible.as_bool() {
            let x = frame_info.PointerPosition.Position.x - self.bounds.left;
            let y = frame_info.PointerPosition.Position.y - self.bounds.top;
            if x >= 0 && y >= 0 && x < desc.Width as i32 && y < desc.Height as i32 {
                let scaled_x = x as u32 * self.config.width / desc.Width.max(1);
                let scaled_y = y as u32 * self.config.height / desc.Height.max(1);
                draw_pointer_marker(&mut resized, scaled_x, scaled_y);
            }
        }
        Ok(RawFrame {
            width: resized.width(),
            height: resized.height(),
            rgba: resized.into_raw(),
        })
    }

    fn ensure_staging(&mut self, source: &D3D11_TEXTURE2D_DESC) -> Result<()> {
        if self.staging.is_some()
            && self.staging_width == source.Width
            && self.staging_height == source.Height
        {
            return Ok(());
        }
        let desc = D3D11_TEXTURE2D_DESC {
            Width: source.Width,
            Height: source.Height,
            MipLevels: 1,
            ArraySize: 1,
            Format: source.Format,
            SampleDesc: DXGI_SAMPLE_DESC {
                Count: 1,
                Quality: 0,
            },
            Usage: D3D11_USAGE_STAGING,
            BindFlags: 0,
            CPUAccessFlags: D3D11_CPU_ACCESS_READ.0 as u32,
            MiscFlags: 0,
        };
        let mut staging = None;
        unsafe {
            self.device
                .CreateTexture2D(&desc, None, Some(&mut staging))?;
        }
        self.staging = Some(staging.context("创建屏幕暂存纹理失败")?);
        self.staging_width = source.Width;
        self.staging_height = source.Height;
        Ok(())
    }
}

fn draw_pointer_marker(image: &mut RgbaImage, x: u32, y: u32) {
    for offset in 0..12_u32 {
        if x + offset < image.width() {
            image.put_pixel(
                x + offset,
                y.min(image.height() - 1),
                image::Rgba([255, 255, 255, 255]),
            );
        }
        if y + offset < image.height() {
            image.put_pixel(
                x.min(image.width() - 1),
                y + offset,
                image::Rgba([255, 255, 255, 255]),
            );
        }
    }
}

pub struct InputInjector {
    bounds: RECT,
    pressed_keys: HashSet<(u16, bool)>,
    pressed_buttons: HashSet<RemotePointerButton>,
}

impl InputInjector {
    pub fn new(display_id: &str) -> Result<Self> {
        let bounds = enumerate_outputs()?
            .into_iter()
            .find(|entry| entry.id == display_id)
            .map(|entry| entry.desc.DesktopCoordinates)
            .ok_or_else(|| anyhow::anyhow!("找不到远程控制显示器：{display_id}"))?;
        Ok(Self {
            bounds,
            pressed_keys: HashSet::new(),
            pressed_buttons: HashSet::new(),
        })
    }

    pub fn inject(&mut self, event: &RemoteInputEvent) -> Result<()> {
        match event {
            RemoteInputEvent::PointerMove { x, y } => self.pointer_move(*x, *y),
            RemoteInputEvent::PointerButton { button, pressed } => {
                self.pointer_button(*button, *pressed)
            }
            RemoteInputEvent::Wheel { horizontal, delta } => {
                let flags = if *horizontal {
                    MOUSEEVENTF_HWHEEL
                } else {
                    MOUSEEVENTF_WHEEL
                };
                send_inputs(&[mouse_input(0, 0, *delta as u32, flags)])
            }
            RemoteInputEvent::Key {
                scan_code,
                extended,
                pressed,
            } => self.key(*scan_code, *extended, *pressed),
            RemoteInputEvent::Text { text } => {
                let mut inputs = Vec::new();
                for unit in text.encode_utf16() {
                    inputs.push(key_input(unit, KEYEVENTF_UNICODE));
                    inputs.push(key_input(
                        unit,
                        KEYBD_EVENT_FLAGS(KEYEVENTF_UNICODE.0 | KEYEVENTF_KEYUP.0),
                    ));
                }
                send_inputs(&inputs)
            }
            RemoteInputEvent::ReleaseAll => self.release_all(),
        }
    }

    fn pointer_move(&self, x: u16, y: u16) -> Result<()> {
        let virtual_left = unsafe { GetSystemMetrics(SM_XVIRTUALSCREEN) };
        let virtual_top = unsafe { GetSystemMetrics(SM_YVIRTUALSCREEN) };
        let virtual_width = unsafe { GetSystemMetrics(SM_CXVIRTUALSCREEN) }.max(1);
        let virtual_height = unsafe { GetSystemMetrics(SM_CYVIRTUALSCREEN) }.max(1);
        let display_width = (self.bounds.right - self.bounds.left).max(1);
        let display_height = (self.bounds.bottom - self.bounds.top).max(1);
        let pixel_x = self.bounds.left + (x as i64 * (display_width - 1) as i64 / 65535) as i32;
        let pixel_y = self.bounds.top + (y as i64 * (display_height - 1) as i64 / 65535) as i32;
        let absolute_x =
            ((pixel_x - virtual_left) as i64 * 65535 / (virtual_width - 1).max(1) as i64) as i32;
        let absolute_y =
            ((pixel_y - virtual_top) as i64 * 65535 / (virtual_height - 1).max(1) as i64) as i32;
        send_inputs(&[mouse_input(
            absolute_x,
            absolute_y,
            0,
            MOUSE_EVENT_FLAGS(
                MOUSEEVENTF_MOVE.0 | MOUSEEVENTF_ABSOLUTE.0 | MOUSEEVENTF_VIRTUALDESK.0,
            ),
        )])
    }

    fn pointer_button(&mut self, button: RemotePointerButton, pressed: bool) -> Result<()> {
        let flag = match (button, pressed) {
            (RemotePointerButton::Left, true) => MOUSEEVENTF_LEFTDOWN,
            (RemotePointerButton::Left, false) => MOUSEEVENTF_LEFTUP,
            (RemotePointerButton::Right, true) => MOUSEEVENTF_RIGHTDOWN,
            (RemotePointerButton::Right, false) => MOUSEEVENTF_RIGHTUP,
            (RemotePointerButton::Middle, true) => MOUSEEVENTF_MIDDLEDOWN,
            (RemotePointerButton::Middle, false) => MOUSEEVENTF_MIDDLEUP,
        };
        send_inputs(&[mouse_input(0, 0, 0, flag)])?;
        if pressed {
            self.pressed_buttons.insert(button);
        } else {
            self.pressed_buttons.remove(&button);
        }
        Ok(())
    }

    fn key(&mut self, scan_code: u16, extended: bool, pressed: bool) -> Result<()> {
        let mut flags = KEYEVENTF_SCANCODE.0;
        if extended {
            flags |= KEYEVENTF_EXTENDEDKEY.0;
        }
        if !pressed {
            flags |= KEYEVENTF_KEYUP.0;
        }
        send_inputs(&[key_input(scan_code, KEYBD_EVENT_FLAGS(flags))])?;
        if pressed {
            self.pressed_keys.insert((scan_code, extended));
        } else {
            self.pressed_keys.remove(&(scan_code, extended));
        }
        Ok(())
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

fn mouse_input(dx: i32, dy: i32, mouse_data: u32, flags: MOUSE_EVENT_FLAGS) -> INPUT {
    INPUT {
        r#type: INPUT_MOUSE,
        Anonymous: INPUT_0 {
            mi: MOUSEINPUT {
                dx,
                dy,
                mouseData: mouse_data,
                dwFlags: flags,
                time: 0,
                dwExtraInfo: 0,
            },
        },
    }
}

fn key_input(scan_code: u16, flags: KEYBD_EVENT_FLAGS) -> INPUT {
    INPUT {
        r#type: INPUT_KEYBOARD,
        Anonymous: INPUT_0 {
            ki: KEYBDINPUT {
                wVk: VIRTUAL_KEY(0),
                wScan: scan_code,
                dwFlags: flags,
                time: 0,
                dwExtraInfo: 0,
            },
        },
    }
}

fn send_inputs(inputs: &[INPUT]) -> Result<()> {
    if inputs.is_empty() {
        return Ok(());
    }
    let sent = unsafe { SendInput(inputs, size_of::<INPUT>() as i32) };
    if sent != inputs.len() as u32 {
        anyhow::bail!("Windows 拒绝远程输入；高权限窗口或 UIPI 可能阻止了注入")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires an interactive Windows desktop"]
    fn captures_first_display() {
        let display = available_displays()
            .unwrap()
            .into_iter()
            .next()
            .expect("at least one attached display");
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
            .find_map(|_| capture.capture().transpose())
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
