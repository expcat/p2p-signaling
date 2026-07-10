use std::future::Future;
use std::path::PathBuf;
use std::sync::mpsc as std_mpsc;
use std::thread::JoinHandle;

use eframe::egui::{
    self, Align, Color32, Context, CornerRadius, FontData, FontDefinitions, FontFamily, FontId,
    Frame, Layout, RichText, ScrollArea, Sense, Stroke, TextEdit, TextureHandle, TextureOptions,
    Ui, Vec2,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder, Handle};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::oneshot;

use p2p_core::transfer::{FileMetadata, TransferDirection, TransferStatus};
use p2p_core::{
    CandidateKind, ChatSession, ChatSessionHandle, ConnectInfo, FileTransferProgress, SessionEvent,
    SessionRole,
};
use p2p_core::{
    RemoteDesktopEvent, RemoteDesktopOffer, RemoteDesktopPlatform, RemoteDesktopState,
    RemoteDisplay, RemoteInputEvent, RemotePointerButton,
};

mod remote_desktop;

use remote_desktop::{CaptureEvent, CaptureWorker, FrameDecoder, InputInjector};

const DEFAULT_SERVER: &str = "p2p-signaling.yizhe.studio";

fn main() -> eframe::Result<()> {
    let initial_config =
        ClientConfig::from_args(std::env::args().skip(1)).unwrap_or_else(|error| {
            eprintln!("{error:#}");
            print_usage();
            std::process::exit(2);
        });

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([920.0, 640.0])
            .with_min_inner_size([760.0, 520.0]),
        ..Default::default()
    };

    eframe::run_native(
        "P2P Signaling Chat",
        native_options,
        Box::new(move |cc| Ok(Box::new(P2pChatApp::new(cc, initial_config)))),
    )
}

#[derive(Debug, Clone)]
struct ClientConfig {
    server: String,
    room: String,
    role: RoleChoice,
    server_explicit: bool,
    test_message: Option<String>,
}

impl ClientConfig {
    fn from_args(args: impl IntoIterator<Item = String>) -> anyhow::Result<Self> {
        let mut server =
            std::env::var("P2P_SIGNALING_SERVER").unwrap_or_else(|_| DEFAULT_SERVER.to_string());
        let mut room = std::env::var("P2P_SIGNALING_ROOM").unwrap_or_default();
        let mut role = std::env::var("P2P_SIGNALING_ROLE").unwrap_or_else(|_| "host".into());
        let mut server_explicit = std::env::var("P2P_SIGNALING_SERVER").is_ok();
        let mut test_message = std::env::var("P2P_SIGNALING_TEST_MESSAGE").ok();

        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--server" | "-s" => {
                    server = require_value(arg.as_str(), args.next())?;
                    server_explicit = true;
                }
                "--room" | "-r" => {
                    room = require_value(arg.as_str(), args.next())?;
                }
                "--role" => {
                    role = require_value(arg.as_str(), args.next())?;
                }
                "--test-message" => {
                    test_message = Some(require_value(arg.as_str(), args.next())?);
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                value if !value.starts_with('-') => {
                    server = value.to_string();
                    server_explicit = true;
                }
                _ => anyhow::bail!("unknown argument: {arg}"),
            }
        }

        Ok(Self {
            server,
            room: normalize_room_input(&room),
            role: RoleChoice::parse(&role)?,
            server_explicit,
            test_message,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoleChoice {
    Host,
    Guest,
}

impl RoleChoice {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "host" => Ok(Self::Host),
            "guest" => Ok(Self::Guest),
            _ => anyhow::bail!("--role must be either host or guest"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    Idle,
    Connecting,
    Connected,
    Paired,
    Direct,
    DirectFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChatAuthor {
    Mine,
    Peer,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MainView {
    Chat,
    RemoteDesktop,
}

#[derive(Debug, Clone)]
struct ChatLine {
    author: ChatAuthor,
    text: String,
}

#[derive(Debug, Clone)]
struct TransferLine {
    transfer_id: String,
    file_name: String,
    direction: TransferDirection,
    status: TransferStatus,
    completed_bytes: u64,
    total_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
enum TransferAction {
    Accept,
    Reject,
    Pause,
    Resume,
    Cancel,
}

#[derive(Debug)]
enum UiNotice {
    Error(String),
}

/// 从后台任务向 UI 报错并立刻请求重绘，保证用户不动鼠标也能看到提示。
#[derive(Clone)]
struct UiNotifier {
    tx: std_mpsc::Sender<UiNotice>,
    ctx: Context,
}

impl UiNotifier {
    fn error(&self, message: String) {
        let _ = self.tx.send(UiNotice::Error(message));
        self.ctx.request_repaint();
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StoredConfig {
    server: String,
}

struct P2pChatApp {
    runtime: AsyncRuntime,
    egui_ctx: Context,
    server: String,
    room_input: String,
    active_room: Option<String>,
    message_input: String,
    messages: Vec<ChatLine>,
    pending_offers: Vec<FileMetadata>,
    transfers: Vec<TransferLine>,
    status: String,
    state: ConnectionState,
    handle: Option<ChatSessionHandle>,
    event_rx: Option<std_mpsc::Receiver<SessionEvent>>,
    pending_test_message: Option<String>,
    notifier: UiNotifier,
    notice_rx: std_mpsc::Receiver<UiNotice>,
    config_path: Option<PathBuf>,
    main_view: MainView,
    remote_displays: Vec<RemoteDisplay>,
    remote_display_index: usize,
    remote_peer_supported: bool,
    remote_state: RemoteDesktopState,
    remote_offer: Option<RemoteDesktopOffer>,
    remote_allow_control: bool,
    remote_texture: Option<TextureHandle>,
    remote_frame_size: Option<[u32; 2]>,
    remote_decoder: Option<FrameDecoder>,
    capture_worker: Option<CaptureWorker>,
    capture_event_tx: std_mpsc::Sender<CaptureEvent>,
    capture_event_rx: std_mpsc::Receiver<CaptureEvent>,
    input_injector: Option<InputInjector>,
    remote_input_sequence: u64,
    remote_keyboard_captured: bool,
    remote_modifiers: egui::Modifiers,
    remote_last_pointer: Option<(u16, u16)>,
}

impl P2pChatApp {
    fn new(cc: &eframe::CreationContext<'_>, initial_config: ClientConfig) -> Self {
        configure_style(&cc.egui_ctx);

        let runtime = AsyncRuntime::new().expect("failed to create tokio runtime");
        let (notice_tx, notice_rx) = std_mpsc::channel();
        let (capture_event_tx, capture_event_rx) = std_mpsc::channel();
        let config_path = config_path();
        let stored = config_path.as_ref().and_then(load_config);
        let initial_room = initial_config.room.clone();
        let initial_role = initial_config.role;
        let pending_test_message = initial_config.test_message;
        let server = if initial_config.server_explicit {
            initial_config.server
        } else {
            stored
                .filter(|config| !config.server.trim().is_empty())
                .map(|config| config.server)
                .unwrap_or(initial_config.server)
        };

        let mut app = Self {
            runtime,
            egui_ctx: cc.egui_ctx.clone(),
            server,
            room_input: initial_config.room,
            active_room: None,
            message_input: String::new(),
            messages: Vec::new(),
            pending_offers: Vec::new(),
            transfers: Vec::new(),
            status: "未连接".into(),
            state: ConnectionState::Idle,
            handle: None,
            event_rx: None,
            pending_test_message,
            notifier: UiNotifier {
                tx: notice_tx,
                ctx: cc.egui_ctx.clone(),
            },
            notice_rx,
            config_path,
            main_view: MainView::Chat,
            remote_displays: remote_desktop::available_displays().unwrap_or_default(),
            remote_display_index: 0,
            remote_peer_supported: false,
            remote_state: RemoteDesktopState::Idle,
            remote_offer: None,
            remote_allow_control: false,
            remote_texture: None,
            remote_frame_size: None,
            remote_decoder: None,
            capture_worker: None,
            capture_event_tx,
            capture_event_rx,
            input_injector: None,
            remote_input_sequence: 0,
            remote_keyboard_captured: false,
            remote_modifiers: egui::Modifiers::NONE,
            remote_last_pointer: None,
        };

        match initial_role {
            RoleChoice::Host => {
                // 房间码由服务器分配；提供了 --room 视为希望自动开房，但码本身被忽略
                if !initial_room.is_empty() {
                    app.push_system("房间号由服务器分配，已忽略 --room/P2P_SIGNALING_ROOM。");
                    app.room_input.clear();
                    app.start_session(SessionRole::Host);
                }
            }
            RoleChoice::Guest => {
                if is_valid_room(&initial_room) {
                    app.start_session(SessionRole::Guest {
                        room_code: initial_room,
                    });
                } else {
                    app.status = "已填入房间号，可直接加入".into();
                }
            }
        }

        app
    }

    fn create_room(&mut self) {
        self.start_session(SessionRole::Host);
    }

    fn join_room(&mut self) {
        normalize_room_input_in_place(&mut self.room_input);
        let room = self.room_input.clone();

        if !is_valid_room(&room) {
            self.push_system("请输入 4 位数字房间号。");
            self.status = "房间号需要 4 位数字".into();
            return;
        }

        self.start_session(SessionRole::Guest { room_code: room });
    }

    fn start_session(&mut self, role: SessionRole) {
        let room = match &role {
            SessionRole::Host => None,
            SessionRole::Guest { room_code } => Some(room_code.clone()),
        };

        let signaling_url = match &room {
            None => build_host_signaling_url(&self.server),
            Some(room) => build_signaling_url(&self.server, room),
        };
        let signaling_url = match signaling_url {
            Ok(url) => url,
            Err(error) => {
                self.status = format!("{error:#}");
                self.push_system(self.status.clone());
                return;
            }
        };

        self.close_current_session();
        self.save_server();

        let (event_tx, mut event_rx) = tokio_mpsc::channel::<SessionEvent>(64);
        let session = ChatSession::new(role, signaling_url);
        match self.runtime.wait(session.start(event_tx)) {
            Ok(Ok(handle)) => {
                // 后台事件经转发任务进入 std channel，同时请求重绘——
                // egui 按需渲染，没有这一步空闲时收到的消息/进度不会显示
                let (ui_event_tx, ui_event_rx) = std_mpsc::channel();
                let ctx = self.egui_ctx.clone();
                self.runtime.spawn(async move {
                    while let Some(event) = event_rx.recv().await {
                        if ui_event_tx.send(event).is_err() {
                            break;
                        }
                        ctx.request_repaint();
                    }
                });

                self.handle = Some(handle);
                self.event_rx = Some(ui_event_rx);
                self.active_room = room.clone();
                self.messages.clear();
                self.pending_offers.clear();
                self.transfers.clear();
                self.state = ConnectionState::Connecting;
                match &room {
                    Some(room) => {
                        self.status = format!("正在加入房间 {room}");
                        self.push_system(format!("正在加入房间 {room}，连接信令服务中。"));
                    }
                    None => {
                        self.status = "正在创建房间".into();
                        self.push_system("正在连接信令服务，等待服务器分配房间号。");
                    }
                }
                if let Some(message) = &self.pending_test_message {
                    let notice = format!("将在直连建立后自动发送测试消息：{message}");
                    self.push_system(notice);
                }
            }
            Ok(Err(error)) | Err(error) => {
                self.status = format!("{error:#}");
                self.push_system(format!("连接失败：{error:#}"));
            }
        }
    }

    fn send_message(&mut self) {
        let text = self.message_input.trim().to_string();
        if text.is_empty() {
            return;
        }

        if self.handle.is_none() {
            self.push_system("请先创建或加入房间。");
            return;
        }

        if !can_send_to_room(self.state, self.handle.is_some()) {
            self.push_system("请等待连接房间后再发送消息。");
            return;
        }

        self.message_input.clear();
        self.send_text_message(text);
    }

    fn send_text_message(&mut self, text: String) {
        let Some(handle) = self.handle.clone() else {
            self.push_system("请先创建或加入房间。");
            return;
        };

        self.push_message(ChatAuthor::Mine, text.clone());

        let notifier = self.notifier.clone();
        self.runtime.spawn(async move {
            if let Err(error) = handle.send_text(text).await {
                notifier.error(format!("发送失败：{error:#}"));
            }
        });
    }

    fn choose_and_send_file(&mut self) {
        let Some(handle) = self.handle.clone() else {
            self.push_system("请先创建或加入房间。");
            return;
        };

        if self.state != ConnectionState::Direct {
            self.push_system("请等待直连建立后再发送文件。");
            return;
        }

        let Some(path) = rfd::FileDialog::new().pick_file() else {
            return;
        };
        self.push_system(format!("准备发送文件：{}", display_file_name(&path)));
        let notifier = self.notifier.clone();
        self.runtime.spawn(async move {
            if let Err(error) = handle.send_file(path).await {
                notifier.error(format!("发送文件失败：{error:#}"));
            }
        });
    }

    fn accept_file_offer(&mut self, metadata: FileMetadata) {
        let Some(handle) = self.handle.clone() else {
            return;
        };

        let save_path = rfd::FileDialog::new()
            .set_file_name(&metadata.file_name)
            .save_file();
        let transfer_id = metadata.transfer_id.clone();

        match save_path {
            Some(path) => {
                self.pending_offers
                    .retain(|offer| offer.transfer_id != transfer_id);
                self.push_system(format!("开始接收文件：{}", metadata.file_name));
                let notifier = self.notifier.clone();
                self.runtime.spawn(async move {
                    if let Err(error) = handle.accept_file(transfer_id, path).await {
                        notifier.error(format!("接收文件失败：{error:#}"));
                    }
                });
            }
            None => {
                self.push_system(format!(
                    "已取消选择保存位置，可稍后接收或拒绝：{}",
                    metadata.file_name
                ));
            }
        }
    }

    fn reject_file_offer(&mut self, transfer_id: String) {
        let Some(handle) = self.handle.clone() else {
            return;
        };

        let Some(index) = self
            .pending_offers
            .iter()
            .position(|offer| offer.transfer_id == transfer_id)
        else {
            return;
        };

        let metadata = self.pending_offers.remove(index);
        self.push_system(format!("已拒绝文件：{}", metadata.file_name));
        let notifier = self.notifier.clone();
        self.runtime.spawn(async move {
            if let Err(error) = handle.reject_file(transfer_id, "用户拒绝接收".into()).await {
                notifier.error(format!("拒绝文件失败：{error:#}"));
            }
        });
    }

    fn add_pending_offer(&mut self, metadata: FileMetadata) {
        if let Some(offer) = self
            .pending_offers
            .iter_mut()
            .find(|offer| offer.transfer_id == metadata.transfer_id)
        {
            *offer = metadata;
            return;
        }

        self.pending_offers.push(metadata);
    }

    fn update_transfer(&mut self, progress: FileTransferProgress) {
        self.pending_offers
            .retain(|offer| offer.transfer_id != progress.transfer_id);

        if let Some(line) = self
            .transfers
            .iter_mut()
            .find(|line| line.transfer_id == progress.transfer_id)
        {
            line.file_name = progress.file_name;
            line.direction = progress.direction;
            line.status = progress.status;
            line.completed_bytes = progress.completed_bytes;
            line.total_bytes = progress.total_bytes;
            return;
        }

        self.transfers.push(TransferLine {
            transfer_id: progress.transfer_id,
            file_name: progress.file_name,
            direction: progress.direction,
            status: progress.status,
            completed_bytes: progress.completed_bytes,
            total_bytes: progress.total_bytes,
        });
    }

    fn pause_transfer(&mut self, transfer_id: String) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let notifier = self.notifier.clone();
        self.runtime.spawn(async move {
            if let Err(error) = handle.pause_transfer(transfer_id).await {
                notifier.error(format!("暂停失败：{error:#}"));
            }
        });
    }

    fn resume_transfer(&mut self, transfer_id: String) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let notifier = self.notifier.clone();
        self.runtime.spawn(async move {
            if let Err(error) = handle.resume_transfer(transfer_id).await {
                notifier.error(format!("继续失败：{error:#}"));
            }
        });
    }

    fn cancel_transfer(&mut self, transfer_id: String) {
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let notifier = self.notifier.clone();
        self.runtime.spawn(async move {
            if let Err(error) = handle.cancel_transfer(transfer_id, "用户取消".into()).await {
                notifier.error(format!("取消失败：{error:#}"));
            }
        });
    }

    fn close_current_session(&mut self) {
        self.cleanup_remote_desktop();
        if let Some(handle) = self.handle.take() {
            self.runtime.spawn(async move {
                let _ = handle.close().await;
            });
        }

        self.event_rx = None;
        self.active_room = None;
        self.state = ConnectionState::Idle;
    }

    fn disconnect(&mut self) {
        self.close_current_session();
        self.status = "已断开".into();
        self.push_system("连接已断开。");
    }

    fn retry_direct(&mut self) {
        let Some(handle) = self.handle.clone() else {
            self.push_system("请先创建或加入房间。");
            return;
        };

        self.state = ConnectionState::Paired;
        self.status = "正在重试直连".into();
        self.push_system("正在重新收集候选并尝试直连。");
        let notifier = self.notifier.clone();
        self.runtime.spawn(async move {
            if let Err(error) = handle.retry_direct().await {
                notifier.error(format!("重试直连失败：{error:#}"));
            }
        });
    }

    fn start_remote_desktop_offer(&mut self) {
        if self.state != ConnectionState::Direct {
            self.push_system("请等待直连建立后再共享屏幕。");
            return;
        }
        if !self.remote_peer_supported {
            self.push_system("对方客户端不支持远程桌面。");
            return;
        }
        if !matches!(self.remote_state, RemoteDesktopState::Idle) {
            self.push_system("已有远程桌面会话。");
            return;
        }
        if let Err(error) = remote_desktop::ensure_screen_capture_permission() {
            self.push_system(format!("无法共享屏幕：{error:#}"));
            return;
        }
        if self.remote_allow_control {
            if let Err(error) = remote_desktop::ensure_input_permission() {
                self.push_system(format!(
                    "无法授予远程控制：{error:#}；可关闭控制权限后仅共享画面。"
                ));
                return;
            }
        }
        let Some(display) = self.remote_displays.get(self.remote_display_index).cloned() else {
            self.push_system("没有可共享的显示器。");
            return;
        };
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let config = remote_desktop::fit_dimensions(display.width, display.height);
        let allow_control = self.remote_allow_control;
        let notifier = self.notifier.clone();
        self.main_view = MainView::RemoteDesktop;
        self.runtime.spawn(async move {
            if let Err(error) = handle
                .offer_remote_desktop(display, config, allow_control)
                .await
            {
                notifier.error(format!("发起屏幕共享失败：{error:#}"));
            }
        });
    }

    fn answer_remote_desktop_offer(&mut self, accepted: bool) {
        let Some(offer) = self.remote_offer.clone() else {
            return;
        };
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let notifier = self.notifier.clone();
        self.runtime.spawn(async move {
            if let Err(error) = handle
                .answer_remote_desktop(
                    offer.session_id,
                    accepted,
                    (!accepted).then(|| "用户拒绝观看共享屏幕".into()),
                )
                .await
            {
                notifier.error(format!("处理屏幕共享请求失败：{error:#}"));
            }
        });
    }

    fn update_remote_control_permission(&mut self, allow_control: bool) {
        let RemoteDesktopState::Sharing { session_id, .. } = &self.remote_state else {
            return;
        };
        let Some(handle) = self.handle.clone() else {
            return;
        };
        if allow_control && self.input_injector.is_none() {
            let Some(display_id) = self
                .remote_offer
                .as_ref()
                .map(|offer| offer.display.id.clone())
            else {
                self.push_system("找不到当前共享的显示器。");
                return;
            };
            match InputInjector::new(&display_id) {
                Ok(injector) => self.input_injector = Some(injector),
                Err(error) => {
                    self.push_system(format!("无法启用远程控制：{error:#}"));
                    return;
                }
            }
        } else if !allow_control {
            if let Some(injector) = self.input_injector.as_mut() {
                let _ = injector.inject(&RemoteInputEvent::ReleaseAll);
            }
            self.input_injector = None;
        }
        let session_id = session_id.clone();
        let notifier = self.notifier.clone();
        self.runtime.spawn(async move {
            if let Err(error) = handle
                .set_remote_desktop_permission(session_id, allow_control)
                .await
            {
                notifier.error(format!("更新远程控制权限失败：{error:#}"));
            }
        });
    }

    fn stop_remote_desktop(&mut self, reason: &str) {
        let Some(session_id) = remote_state_session_id(&self.remote_state).map(str::to_owned)
        else {
            return;
        };
        let Some(handle) = self.handle.clone() else {
            return;
        };
        let reason = reason.to_string();
        let notifier = self.notifier.clone();
        self.runtime.spawn(async move {
            if let Err(error) = handle.stop_remote_desktop(session_id, reason).await {
                notifier.error(format!("停止远程桌面失败：{error:#}"));
            }
        });
    }

    fn send_remote_input_event(&mut self, event: RemoteInputEvent) {
        let RemoteDesktopState::Viewing {
            session_id,
            can_control: true,
        } = &self.remote_state
        else {
            return;
        };
        let Some(handle) = self.handle.clone() else {
            return;
        };
        self.remote_input_sequence = self.remote_input_sequence.saturating_add(1);
        let sequence = self.remote_input_sequence;
        let session_id = session_id.clone();
        let notifier = self.notifier.clone();
        self.runtime.spawn(async move {
            if let Err(error) = handle.send_remote_input(session_id, sequence, event).await {
                notifier.error(format!("发送远程输入失败：{error:#}"));
            }
        });
    }

    fn handle_remote_desktop_event(&mut self, event: RemoteDesktopEvent) {
        match event {
            RemoteDesktopEvent::PeerAvailabilityChanged(supported) => {
                self.remote_peer_supported = supported;
                if supported {
                    self.push_system("对方支持远程桌面。");
                }
            }
            RemoteDesktopEvent::IncomingOffer(offer) => {
                self.remote_offer = Some(offer.clone());
                self.remote_state = RemoteDesktopState::IncomingPending(offer);
                self.main_view = MainView::RemoteDesktop;
                self.push_system("对方请求共享屏幕。");
            }
            RemoteDesktopEvent::SharingStarted(offer) => {
                self.start_capture_worker(offer);
                self.main_view = MainView::RemoteDesktop;
            }
            RemoteDesktopEvent::StateChanged(state) => {
                let became_idle = matches!(state, RemoteDesktopState::Idle);
                if let RemoteDesktopState::Viewing { session_id, .. } = &state {
                    let replace = self
                        .remote_decoder
                        .as_ref()
                        .is_none_or(|_| !matches!(self.remote_state, RemoteDesktopState::Viewing { session_id: ref old, .. } if old == session_id));
                    if replace {
                        self.remote_decoder = Some(FrameDecoder::new(session_id.clone()));
                        self.remote_texture = None;
                        self.remote_frame_size = None;
                    }
                    self.main_view = MainView::RemoteDesktop;
                }
                self.remote_state = state;
                if became_idle {
                    self.cleanup_remote_desktop_resources();
                }
            }
            RemoteDesktopEvent::FrameAvailable { session_id, .. } => {
                self.consume_remote_frame(&session_id);
            }
            RemoteDesktopEvent::Input(event) => {
                let error = self
                    .input_injector
                    .as_mut()
                    .and_then(|injector| injector.inject(&event).err());
                if let Some(error) = error {
                    self.status = format!("远程输入失败：{error:#}");
                    self.push_system(self.status.clone());
                    self.input_injector = None;
                    self.update_remote_control_permission(false);
                }
            }
            RemoteDesktopEvent::KeyframeRequested(_) => {
                if let Some(worker) = &self.capture_worker {
                    worker.force_keyframe();
                }
            }
            RemoteDesktopEvent::Error { message, .. } => {
                self.status = format!("远程桌面错误：{message}");
                self.push_system(self.status.clone());
            }
        }
    }

    fn start_capture_worker(&mut self, offer: RemoteDesktopOffer) {
        self.cleanup_remote_desktop_resources();
        let Some(handle) = self.handle.clone() else {
            return;
        };
        self.remote_offer = Some(offer.clone());
        if offer.allow_control {
            match InputInjector::new(&offer.display.id) {
                Ok(injector) => self.input_injector = Some(injector),
                Err(error) => {
                    self.push_system(format!("初始化远程输入失败：{error:#}"));
                    self.input_injector = None;
                    let session_id = offer.session_id.clone();
                    let permission_handle = handle.clone();
                    self.runtime.spawn(async move {
                        let _ = permission_handle
                            .set_remote_desktop_permission(session_id, false)
                            .await;
                    });
                }
            }
        }
        self.capture_worker = Some(CaptureWorker::start(
            offer,
            handle,
            self.capture_event_tx.clone(),
        ));
    }

    fn consume_remote_frame(&mut self, session_id: &str) {
        let Some(frame) = self
            .handle
            .as_ref()
            .and_then(ChatSessionHandle::take_remote_desktop_frame)
        else {
            return;
        };
        if self.remote_decoder.is_none() {
            self.remote_decoder = Some(FrameDecoder::new(session_id.to_string()));
        }
        let result = self
            .remote_decoder
            .as_mut()
            .expect("remote decoder initialized")
            .apply(frame);
        match result {
            Ok(decoded) => {
                let image = egui::ColorImage::from_rgba_unmultiplied(
                    [decoded.width as usize, decoded.height as usize],
                    &decoded.rgba,
                );
                if let Some(texture) = self.remote_texture.as_mut() {
                    texture.set(image, TextureOptions::LINEAR);
                } else {
                    self.remote_texture = Some(self.egui_ctx.load_texture(
                        "remote-desktop",
                        image,
                        TextureOptions::LINEAR,
                    ));
                }
                self.remote_frame_size = Some([decoded.width, decoded.height]);
            }
            Err(error) => {
                let Some(handle) = self.handle.clone() else {
                    return;
                };
                let session_id = session_id.to_string();
                self.push_system(format!("远程画面需要关键帧：{error:#}"));
                self.runtime.spawn(async move {
                    let _ = handle.request_remote_keyframe(session_id).await;
                });
            }
        }
    }

    fn cleanup_remote_desktop_resources(&mut self) {
        if let Some(mut worker) = self.capture_worker.take() {
            worker.stop();
        }
        if let Some(injector) = self.input_injector.as_mut() {
            let _ = injector.inject(&RemoteInputEvent::ReleaseAll);
        }
        self.input_injector = None;
        self.remote_offer = None;
        self.remote_decoder = None;
        self.remote_texture = None;
        self.remote_frame_size = None;
        self.remote_keyboard_captured = false;
        self.remote_input_sequence = 0;
        self.remote_modifiers = egui::Modifiers::NONE;
        self.remote_last_pointer = None;
    }

    fn remote_desktop_view(&mut self, ui: &mut Ui) {
        let state = self.remote_state.clone();
        match state {
            RemoteDesktopState::Idle => {
                ui.vertical_centered(|ui| {
                    ui.add_space(80.0);
                    ui.heading("远程桌面未启动");
                    ui.label("直连建立后，可从左侧选择显示器并发起共享。");
                });
            }
            RemoteDesktopState::OutgoingPending(offer) => {
                ui.vertical_centered(|ui| {
                    ui.add_space(80.0);
                    ui.spinner();
                    ui.heading("等待对方接受共享");
                    ui.label(format!(
                        "{} · {}×{}",
                        offer.display.name, offer.config.width, offer.config.height
                    ));
                });
            }
            RemoteDesktopState::IncomingPending(offer) => {
                Frame::new()
                    .fill(Color32::from_rgb(18, 24, 33))
                    .corner_radius(CornerRadius::same(8))
                    .stroke(Stroke::new(1.0, Color32::from_rgb(55, 78, 103)))
                    .inner_margin(egui::Margin::symmetric(18, 16))
                    .show(ui, |ui| {
                        ui.heading("对方请求共享屏幕");
                        ui.label(format!(
                            "{} · {}×{} · 最高 {} FPS",
                            offer.display.name,
                            offer.config.width,
                            offer.config.height,
                            offer.config.max_fps
                        ));
                        ui.label(if offer.allow_control {
                            "共享方已允许本次会话临时控制"
                        } else {
                            "本次会话仅观看"
                        });
                        ui.add_space(12.0);
                        ui.horizontal(|ui| {
                            if ui.button("接受").clicked() {
                                self.answer_remote_desktop_offer(true);
                            }
                            if ui.button("拒绝").clicked() {
                                self.answer_remote_desktop_offer(false);
                            }
                        });
                    });
            }
            RemoteDesktopState::Sharing { allow_control, .. } => {
                ui.vertical_centered(|ui| {
                    ui.add_space(80.0);
                    ui.heading("正在共享本机屏幕");
                    ui.label(if allow_control {
                        "对方已获本次会话临时控制权限"
                    } else {
                        "对方只能观看；可在左侧随时开启控制"
                    });
                    ui.label("停止共享或直连断开时，授权会立即撤销。");
                });
            }
            RemoteDesktopState::Viewing {
                can_control,
                session_id: _,
            } => {
                let Some(texture) = self.remote_texture.as_ref() else {
                    ui.vertical_centered(|ui| {
                        ui.add_space(80.0);
                        ui.spinner();
                        ui.heading("等待远程画面");
                    });
                    return;
                };
                let [width, height] = self.remote_frame_size.unwrap_or([1280, 720]);
                let available = ui.available_size();
                let scale = (available.x / width as f32)
                    .min(available.y / height as f32)
                    .max(0.01);
                let size = Vec2::new(width as f32 * scale, height as f32 * scale);
                ui.vertical_centered(|ui| {
                    ui.label(if can_control {
                        if self.remote_keyboard_captured {
                            "控制已启用 · 按 Esc 释放键盘"
                        } else {
                            "控制已授权 · 点击画面接管键盘"
                        }
                    } else {
                        "仅观看"
                    });
                });
                let response =
                    ui.add(egui::Image::new((texture.id(), size)).sense(Sense::click_and_drag()));
                if can_control {
                    self.handle_remote_canvas_input(ui, &response);
                }
            }
        }
    }

    fn handle_remote_canvas_input(&mut self, ui: &Ui, response: &egui::Response) {
        if response.clicked() {
            self.remote_keyboard_captured = true;
        }
        let escape = ui.input(|input| input.key_pressed(egui::Key::Escape));
        if escape && self.remote_keyboard_captured {
            self.remote_keyboard_captured = false;
            self.send_remote_input_event(RemoteInputEvent::ReleaseAll);
            self.remote_modifiers = egui::Modifiers::NONE;
        }

        if let Some(position) = response.hover_pos() {
            let x = (((position.x - response.rect.left()) / response.rect.width()) * 65535.0)
                .clamp(0.0, 65535.0) as u16;
            let y = (((position.y - response.rect.top()) / response.rect.height()) * 65535.0)
                .clamp(0.0, 65535.0) as u16;
            if self.remote_last_pointer != Some((x, y)) {
                self.remote_last_pointer = Some((x, y));
                self.send_remote_input_event(RemoteInputEvent::PointerMove { x, y });
            }
        }

        let buttons = ui.input(|input| {
            [
                (
                    RemotePointerButton::Left,
                    input.pointer.button_pressed(egui::PointerButton::Primary),
                    input.pointer.button_released(egui::PointerButton::Primary),
                ),
                (
                    RemotePointerButton::Right,
                    input.pointer.button_pressed(egui::PointerButton::Secondary),
                    input
                        .pointer
                        .button_released(egui::PointerButton::Secondary),
                ),
                (
                    RemotePointerButton::Middle,
                    input.pointer.button_pressed(egui::PointerButton::Middle),
                    input.pointer.button_released(egui::PointerButton::Middle),
                ),
            ]
        });
        for (button, pressed, released) in buttons {
            if pressed && response.hovered() {
                self.send_remote_input_event(RemoteInputEvent::PointerButton {
                    button,
                    pressed: true,
                });
            }
            if released {
                self.send_remote_input_event(RemoteInputEvent::PointerButton {
                    button,
                    pressed: false,
                });
            }
        }
        if response.hovered() {
            let scroll = ui.input(|input| input.smooth_scroll_delta);
            if scroll.y != 0.0 {
                self.send_remote_input_event(RemoteInputEvent::Wheel {
                    horizontal: false,
                    delta: scroll.y.clamp(-1200.0, 1200.0) as i32,
                });
            }
            if scroll.x != 0.0 {
                self.send_remote_input_event(RemoteInputEvent::Wheel {
                    horizontal: true,
                    delta: scroll.x.clamp(-1200.0, 1200.0) as i32,
                });
            }
        }

        if !self.remote_keyboard_captured {
            return;
        }
        let (modifiers, events) = ui.input(|input| (input.modifiers, input.events.clone()));
        self.send_remote_modifier_changes(modifiers);
        for event in events {
            match event {
                egui::Event::Copy => {
                    self.send_remote_shortcut_key(egui::Key::C);
                }
                egui::Event::Cut => {
                    self.send_remote_shortcut_key(egui::Key::X);
                }
                egui::Event::Paste(_) => {
                    self.send_remote_shortcut_key(egui::Key::V);
                }
                egui::Event::Text(text)
                    if !text.is_empty()
                        && !modifiers.command
                        && !modifiers.ctrl
                        && !modifiers.alt =>
                {
                    self.send_remote_input_event(RemoteInputEvent::Text { text });
                }
                egui::Event::Key {
                    key,
                    pressed,
                    repeat,
                    ..
                } if key != egui::Key::Escape && (!repeat || !pressed) => {
                    if let Some((scan_code, extended, printable)) = egui_key_to_scan_code(key) {
                        if !printable || modifiers.command || modifiers.ctrl || modifiers.alt {
                            self.send_remote_input_event(RemoteInputEvent::Key {
                                scan_code,
                                extended,
                                pressed,
                            });
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn send_remote_shortcut_key(&mut self, key: egui::Key) {
        if let Some((scan_code, extended, _)) = egui_key_to_scan_code(key) {
            self.send_remote_input_event(RemoteInputEvent::Key {
                scan_code,
                extended,
                pressed: true,
            });
        }
    }

    fn send_remote_modifier_changes(&mut self, next: egui::Modifiers) {
        let platform = self
            .remote_offer
            .as_ref()
            .map(|offer| offer.platform)
            .unwrap_or(RemoteDesktopPlatform::Windows);
        for (was, is, scan_code, extended) in
            remote_modifier_changes(self.remote_modifiers, next, platform)
        {
            if was != is {
                self.send_remote_input_event(RemoteInputEvent::Key {
                    scan_code,
                    extended,
                    pressed: is,
                });
            }
        }
        self.remote_modifiers = next;
    }

    fn cleanup_remote_desktop(&mut self) {
        self.cleanup_remote_desktop_resources();
        self.remote_state = RemoteDesktopState::Idle;
        self.remote_peer_supported = false;
    }

    fn poll_background_events(&mut self) {
        while let Ok(notice) = self.notice_rx.try_recv() {
            match notice {
                UiNotice::Error(message) => {
                    self.status = message.clone();
                    self.push_system(message);
                }
            }
        }

        while let Ok(event) = self.capture_event_rx.try_recv() {
            match event {
                CaptureEvent::Error(message) => {
                    self.status = format!("屏幕采集失败：{message}");
                    self.push_system(self.status.clone());
                    self.stop_remote_desktop("屏幕采集失败");
                }
                CaptureEvent::Stopped => {}
            }
        }

        let mut events = Vec::new();
        if let Some(event_rx) = self.event_rx.as_mut() {
            while let Ok(event) = event_rx.try_recv() {
                events.push(event);
            }
        }

        for event in events {
            match event {
                SessionEvent::Connected => {
                    let next_state = state_after_connected_event(self.state);
                    if next_state != self.state {
                        self.state = next_state;
                        self.status = "已连接信令服务，等待对方加入".into();
                        self.push_system("已连接信令服务。");
                    }
                }
                SessionEvent::RoomCodeAssigned(room) => {
                    if self.active_room.as_deref() != Some(room.as_str()) {
                        self.push_system(format!("服务器已分配房间号：{room}"));
                    }
                    self.active_room = Some(room);
                }
                SessionEvent::LocalCandidatesCollected(info) => {
                    self.push_system(format_connect_info("本端候选", &info));
                }
                SessionEvent::PeerCandidatesReceived(info) => {
                    self.mark_peer_available(None);
                    self.push_system(format_connect_info("对端候选", &info));
                }
                SessionEvent::DirectLinkEstablished(info) => {
                    self.state = ConnectionState::Direct;
                    self.status = format!("直连已建立：{}", info.remote_addr);
                    self.push_system(format!("直连已建立：{}", info.remote_addr));
                    if let Some(message) = self.pending_test_message.take() {
                        self.send_text_message(message);
                    }
                }
                SessionEvent::DirectLinkFailed(message) => {
                    self.state = ConnectionState::DirectFailed;
                    self.status = "直连建立失败".into();
                    self.push_system(format!("直连建立失败：{message}"));
                }
                SessionEvent::DirectLinkLost(reason) => {
                    self.state = ConnectionState::DirectFailed;
                    self.status = "直连已断开".into();
                    self.push_system(format!("直连已断开：{reason}"));
                }
                SessionEvent::PeerConnected => {
                    self.mark_peer_available(Some("另一客户端已加入房间。"));
                }
                SessionEvent::PeerDisconnected => {
                    if matches!(
                        self.state,
                        ConnectionState::Paired
                            | ConnectionState::Direct
                            | ConnectionState::DirectFailed
                    ) {
                        self.state = ConnectionState::Connected;
                    }
                    self.status = "对方已离开，等待重新加入".into();
                    self.push_system("对方已离开房间。");
                }
                SessionEvent::MessageReceived(text) => {
                    self.mark_peer_available(None);
                    self.push_message(ChatAuthor::Peer, text);
                }
                SessionEvent::FileOffered(metadata) => {
                    self.mark_peer_available(None);
                    self.push_system(format!(
                        "对方请求发送文件：{} ({})",
                        metadata.file_name,
                        format_bytes(metadata.file_size)
                    ));
                    self.add_pending_offer(metadata);
                }
                SessionEvent::FileProgress(progress) => {
                    self.update_transfer(progress);
                }
                SessionEvent::FileCompleted {
                    transfer_id: _,
                    file_name,
                    path,
                } => {
                    let suffix = path
                        .as_ref()
                        .map(|path| format!("：{}", path.display()))
                        .unwrap_or_default();
                    self.push_system(format!("文件已完成：{file_name}{suffix}"));
                }
                SessionEvent::FileFailed {
                    transfer_id: _,
                    file_name,
                    message,
                } => {
                    self.push_system(format!("文件失败：{file_name}，{message}"));
                }
                SessionEvent::FileCancelled {
                    transfer_id: _,
                    file_name,
                    reason,
                } => {
                    self.push_system(format!("文件已取消：{file_name}，{reason}"));
                }
                SessionEvent::RemoteDesktop(event) => {
                    self.handle_remote_desktop_event(event);
                }
                SessionEvent::SignalingClosed => {
                    if self.state == ConnectionState::Direct {
                        // 直连已建立时信令通道只影响重试直连，不影响聊天
                        self.push_system("信令连接已关闭，直连聊天不受影响。");
                    } else if self.handle.is_some() {
                        self.handle = None;
                        self.active_room = None;
                        self.state = ConnectionState::Idle;
                        self.status = "信令连接已断开".into();
                        self.push_system("信令连接已断开，可重新创建或加入房间。");
                    }
                }
                SessionEvent::Error(message) => {
                    // 错误仅提示，不拆会话：致命断开由 SignalingClosed / DirectLinkLost 负责
                    self.status = format!("错误：{message}");
                    self.push_system(format!("错误：{message}"));
                }
            }
        }
    }

    fn mark_peer_available(&mut self, system_message: Option<&str>) {
        let next_state = state_after_peer_activity(self.state, self.handle.is_some());
        if next_state == self.state {
            return;
        }

        self.state = next_state;
        self.status = "已对接，正在建立直连".into();
        if let Some(message) = system_message {
            self.push_system(message);
        }
    }

    fn save_server(&mut self) {
        let server = self.server.trim().to_string();
        if server.is_empty() {
            self.status = "信令服务器地址不能为空".into();
            return;
        }

        self.server = server.clone();
        let config = StoredConfig { server };
        match save_config(self.config_path.as_ref(), &config) {
            Ok(()) => {
                if self.state == ConnectionState::Idle {
                    self.status = "服务器地址已保存".into();
                }
            }
            Err(error) => {
                self.status = format!("保存失败：{error:#}");
            }
        }
    }

    fn push_message(&mut self, author: ChatAuthor, text: String) {
        self.messages.push(ChatLine { author, text });
    }

    fn push_system(&mut self, text: impl Into<String>) {
        self.push_message(ChatAuthor::System, text.into());
    }

    fn room_label(&self) -> &str {
        self.active_room
            .as_deref()
            .filter(|room| !room.is_empty())
            .unwrap_or("----")
    }
}

struct AsyncRuntime {
    handle: Handle,
    shutdown_tx: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl AsyncRuntime {
    fn new() -> anyhow::Result<Self> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = std_mpsc::sync_channel(1);

        let thread = std::thread::Builder::new()
            .name("p2p-chat-runtime".into())
            .spawn(move || {
                let runtime = match Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(format!("{error:#}")));
                        return;
                    }
                };

                let _ = ready_tx.send(Ok(runtime.handle().clone()));
                runtime.block_on(async {
                    let _ = shutdown_rx.await;
                });
            })?;

        let handle = ready_rx
            .recv()
            .map_err(|error| anyhow::anyhow!("runtime thread failed to start: {error}"))?
            .map_err(|error| anyhow::anyhow!("failed to create tokio runtime: {error}"))?;

        Ok(Self {
            handle,
            shutdown_tx: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        std::mem::drop(self.handle.spawn(future));
    }

    fn wait<F>(&self, future: F) -> anyhow::Result<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (result_tx, result_rx) = std_mpsc::sync_channel(1);
        self.spawn(async move {
            let _ = result_tx.send(future.await);
        });

        result_rx
            .recv()
            .map_err(|error| anyhow::anyhow!("runtime task failed: {error}"))
    }
}

impl Drop for AsyncRuntime {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl eframe::App for P2pChatApp {
    fn ui(&mut self, ui: &mut Ui, _frame: &mut eframe::Frame) {
        self.poll_background_events();

        egui::Panel::top("top_bar").show(ui, |ui| {
            ui.add_space(10.0);
            ui.horizontal(|ui| {
                ui.heading("P2P 信令聊天");
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    status_chip(ui, self.state, &self.status);
                });
            });
            ui.add_space(8.0);
        });

        egui::Panel::left("connection_panel")
            .resizable(false)
            .min_size(284.0)
            .max_size(284.0)
            .show(ui, |ui| {
                ui.add_space(8.0);
                ui.label(RichText::new("信令服务器").strong());
                ui.add_space(6.0);
                ui.add(
                    TextEdit::singleline(&mut self.server)
                        .hint_text("p2p-signaling.yizhe.studio")
                        .desired_width(f32::INFINITY),
                );
                ui.add_space(8.0);
                if ui.button("保存地址").clicked() {
                    self.save_server();
                }

                ui.separator();
                ui.label(RichText::new("房间").strong());
                ui.add_space(6.0);
                let room_label = self.room_label().to_string();
                let active_room = self.active_room.clone().filter(|room| !room.is_empty());
                let mut copied_room = None;
                let mut disconnect_clicked = false;
                ui.horizontal(|ui| {
                    ui.label(RichText::new(room_label).font(FontId::monospace(32.0)));
                    if let Some(room) = active_room {
                        if ui.small_button("复制").clicked() {
                            ui.ctx().copy_text(room.clone());
                            copied_room = Some(room);
                        }
                    }
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if self.handle.is_some() && ui.button("断开").clicked() {
                            disconnect_clicked = true;
                        }
                    });
                });
                if let Some(room) = copied_room {
                    self.status = format!("已复制房间号 {room}");
                }
                if disconnect_clicked {
                    self.disconnect();
                }
                ui.add_space(8.0);

                let can_connect = !self.server.trim().is_empty();
                if full_width_button(ui, can_connect, "创建房间", 36.0).clicked() {
                    self.create_room();
                }

                ui.add_space(14.0);
                ui.label("输入房间号");
                let room_response = ui.add(
                    TextEdit::singleline(&mut self.room_input)
                        .hint_text("0000")
                        .char_limit(4)
                        .desired_width(f32::INFINITY),
                );
                if room_response.changed() {
                    normalize_room_input_in_place(&mut self.room_input);
                }

                let join_pressed = room_response.lost_focus()
                    && ui.input(|input| input.key_pressed(egui::Key::Enter));
                if full_width_button(
                    ui,
                    can_connect && is_valid_room(&self.room_input),
                    "加入房间",
                    36.0,
                )
                .clicked()
                    || join_pressed
                {
                    self.join_room();
                }

                if full_width_button(
                    ui,
                    self.handle.is_some() && self.state == ConnectionState::DirectFailed,
                    "重试直连",
                    36.0,
                )
                .clicked()
                {
                    self.retry_direct();
                }

                ui.separator();
                ui.label(RichText::new("远程桌面").strong());
                ui.add_space(6.0);
                if !remote_desktop::is_supported() {
                    ui.label("当前平台暂不支持共享屏幕");
                } else if self.state == ConnectionState::Direct && !self.remote_peer_supported {
                    ui.label("对方客户端不支持远程桌面");
                }

                if matches!(self.remote_state, RemoteDesktopState::Idle) {
                    let selected_name = self
                        .remote_displays
                        .get(self.remote_display_index)
                        .map(|display| display.name.clone())
                        .unwrap_or_else(|| "无可用显示器".into());
                    egui::ComboBox::from_id_salt("remote-display")
                        .selected_text(selected_name)
                        .width(ui.available_width())
                        .show_ui(ui, |ui| {
                            for (index, display) in self.remote_displays.iter().enumerate() {
                                ui.selectable_value(
                                    &mut self.remote_display_index,
                                    index,
                                    format!(
                                        "{} ({}×{})",
                                        display.name, display.width, display.height
                                    ),
                                );
                            }
                        });
                    ui.checkbox(&mut self.remote_allow_control, "允许对方临时控制");
                    let can_share = self.state == ConnectionState::Direct
                        && self.remote_peer_supported
                        && !self.remote_displays.is_empty();
                    if full_width_button(ui, can_share, "共享屏幕", 36.0).clicked() {
                        self.start_remote_desktop_offer();
                    }
                } else {
                    match &self.remote_state {
                        RemoteDesktopState::Sharing { allow_control, .. } => {
                            let mut next = *allow_control;
                            if ui.checkbox(&mut next, "允许对方临时控制").changed() {
                                self.update_remote_control_permission(next);
                            }
                            ui.label("正在共享本机屏幕");
                        }
                        RemoteDesktopState::Viewing { can_control, .. } => {
                            ui.label(if *can_control {
                                "正在观看，可临时控制"
                            } else {
                                "正在观看，仅查看"
                            });
                        }
                        RemoteDesktopState::OutgoingPending(_) => {
                            ui.label("等待对方接受共享");
                        }
                        RemoteDesktopState::IncomingPending(_) => {
                            ui.label("收到屏幕共享请求");
                        }
                        RemoteDesktopState::Idle => {}
                    }
                    if full_width_button(ui, true, "停止远程桌面", 36.0).clicked() {
                        self.stop_remote_desktop("用户停止远程桌面");
                    }
                }

                ui.with_layout(Layout::bottom_up(Align::LEFT), |ui| {
                    ui.label(RichText::new(&self.status).color(Color32::from_rgb(158, 169, 185)));
                });
            });

        egui::CentralPanel::default().show(ui, |ui| {
            ui.add_space(8.0);
            let previous_view = self.main_view;
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.main_view, MainView::Chat, "聊天与文件");
                ui.selectable_value(&mut self.main_view, MainView::RemoteDesktop, "远程桌面");
            });
            if previous_view != self.main_view && self.remote_keyboard_captured {
                self.remote_keyboard_captured = false;
                self.send_remote_input_event(RemoteInputEvent::ReleaseAll);
                self.remote_modifiers = egui::Modifiers::NONE;
            }
            ui.separator();
            if self.main_view == MainView::RemoteDesktop {
                self.remote_desktop_view(ui);
                return;
            }
            let mut transfer_action = None;
            if !self.pending_offers.is_empty() || !self.transfers.is_empty() {
                Frame::new()
                    .fill(Color32::from_rgb(16, 22, 30))
                    .corner_radius(CornerRadius::same(8))
                    .stroke(Stroke::new(1.0, Color32::from_rgb(39, 51, 67)))
                    .inner_margin(egui::Margin::symmetric(12, 10))
                    .show(ui, |ui| {
                        ui.label(RichText::new("文件传输").strong());
                        ui.add_space(4.0);
                        for offer in &self.pending_offers {
                            if let Some(action) = pending_offer_row(ui, offer) {
                                transfer_action = Some((offer.transfer_id.clone(), action));
                            }
                            ui.add_space(6.0);
                        }
                        for transfer in &self.transfers {
                            if let Some(action) = transfer_row(ui, transfer) {
                                transfer_action = Some((transfer.transfer_id.clone(), action));
                            }
                            ui.add_space(6.0);
                        }
                    });
                ui.add_space(10.0);
            }

            if let Some((transfer_id, action)) = transfer_action {
                match action {
                    TransferAction::Accept => {
                        if let Some(metadata) = self
                            .pending_offers
                            .iter()
                            .find(|offer| offer.transfer_id == transfer_id)
                            .cloned()
                        {
                            self.accept_file_offer(metadata);
                        }
                    }
                    TransferAction::Reject => self.reject_file_offer(transfer_id),
                    TransferAction::Pause => self.pause_transfer(transfer_id),
                    TransferAction::Resume => self.resume_transfer(transfer_id),
                    TransferAction::Cancel => self.cancel_transfer(transfer_id),
                }
            }

            let chat_height = (ui.available_height() - 58.0).max(120.0);
            Frame::new()
                .fill(Color32::from_rgb(18, 24, 33))
                .corner_radius(CornerRadius::same(8))
                .stroke(Stroke::new(1.0, Color32::from_rgb(39, 51, 67)))
                .show(ui, |ui| {
                    ui.set_height(chat_height);
                    ScrollArea::vertical()
                        .stick_to_bottom(true)
                        .auto_shrink([false, false])
                        .max_height(chat_height)
                        .show(ui, |ui| {
                            ui.set_max_width(finite_width(ui.available_width(), 120.0));
                            ui.add_space(10.0);
                            if self.messages.is_empty() {
                                empty_chat(ui);
                            } else {
                                for line in &self.messages {
                                    chat_line(ui, line);
                                    ui.add_space(8.0);
                                }
                            }
                            ui.add_space(10.0);
                        });
                });

            ui.add_space(10.0);
            ui.horizontal(|ui| {
                let can_send = can_send_to_room(self.state, self.handle.is_some());
                let message_hint = if can_send {
                    "输入消息"
                } else {
                    "直连建立后可发送消息"
                };
                let button_size = Vec2::new(72.0, 32.0);
                let composer_button_width = button_size.x * 2.0 + ui.spacing().item_spacing.x * 2.0;
                let message_input_width =
                    finite_width(ui.available_width() - composer_button_width, 100.0);
                let response = ui
                    .add_enabled_ui(can_send, |ui| {
                        ui.add_sized(
                            Vec2::new(message_input_width, 32.0),
                            TextEdit::singleline(&mut self.message_input).hint_text(message_hint),
                        )
                    })
                    .inner;
                let send_pressed = can_send
                    && response.lost_focus()
                    && ui.input(|input| input.key_pressed(egui::Key::Enter));

                if ui
                    .add_enabled(can_send, egui::Button::new("发送").min_size(button_size))
                    .clicked()
                    || send_pressed
                {
                    self.send_message();
                }

                if ui
                    .add_enabled(can_send, egui::Button::new("文件").min_size(button_size))
                    .clicked()
                {
                    self.choose_and_send_file();
                }
            });
        });
    }
}

impl Drop for P2pChatApp {
    fn drop(&mut self) {
        self.close_current_session();
    }
}

fn configure_style(ctx: &Context) {
    configure_fonts(ctx);

    ctx.set_theme(egui::Theme::Dark);
    let mut style = (*ctx.style_of(egui::Theme::Dark)).clone();
    style.spacing.item_spacing = Vec2::new(10.0, 10.0);
    style.spacing.button_padding = Vec2::new(12.0, 8.0);
    style.visuals = egui::Visuals::dark();
    style.visuals.panel_fill = Color32::from_rgb(13, 18, 26);
    style.visuals.window_fill = Color32::from_rgb(18, 24, 33);
    style.visuals.extreme_bg_color = Color32::from_rgb(10, 15, 22);
    style.visuals.faint_bg_color = Color32::from_rgb(24, 32, 43);
    style.visuals.weak_text_color = Some(Color32::from_rgb(150, 161, 176));
    style.visuals.widgets.noninteractive.fg_stroke.color = Color32::from_rgb(218, 226, 235);
    style.visuals.widgets.noninteractive.bg_stroke =
        Stroke::new(1.0, Color32::from_rgb(39, 51, 67));
    style.visuals.widgets.inactive.bg_fill = Color32::from_rgb(30, 39, 52);
    style.visuals.widgets.inactive.fg_stroke.color = Color32::from_rgb(225, 231, 239);
    style.visuals.widgets.hovered.bg_fill = Color32::from_rgb(46, 59, 76);
    style.visuals.widgets.hovered.fg_stroke.color = Color32::WHITE;
    style.visuals.widgets.active.bg_fill = Color32::from_rgb(53, 103, 132);
    style.visuals.widgets.active.fg_stroke.color = Color32::WHITE;
    style.visuals.selection.bg_fill = Color32::from_rgb(64, 127, 164);
    ctx.set_style_of(egui::Theme::Dark, style);
}

fn configure_fonts(ctx: &Context) {
    let Some((font_name, font_data)) = load_cjk_font() else {
        return;
    };

    let mut fonts = FontDefinitions::default();
    fonts
        .font_data
        .insert(font_name.clone(), FontData::from_owned(font_data).into());

    for family in [FontFamily::Proportional, FontFamily::Monospace] {
        fonts
            .families
            .entry(family)
            .or_default()
            .push(font_name.clone());
    }

    ctx.set_fonts(fonts);
}

fn load_cjk_font() -> Option<(String, Vec<u8>)> {
    const CANDIDATES: &[(&str, &str)] = &[
        (
            "Arial Unicode",
            "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        ),
        ("Arial Unicode", "/Library/Fonts/Arial Unicode.ttf"),
        ("PingFang", "/System/Library/Fonts/PingFang.ttc"),
        ("STHeiti", "/System/Library/Fonts/STHeiti Light.ttc"),
        ("STHeiti", "/System/Library/Fonts/STHeiti Medium.ttc"),
        ("Microsoft YaHei", r"C:\Windows\Fonts\msyh.ttc"),
        ("SimSun", r"C:\Windows\Fonts\simsun.ttc"),
        (
            "Noto Sans CJK",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        ),
        (
            "Noto Sans CJK",
            "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
        ),
    ];

    CANDIDATES.iter().find_map(|(name, path)| {
        std::fs::read(path)
            .ok()
            .map(|data| (format!("p2p-cjk-{name}"), data))
    })
}

fn status_chip(ui: &mut Ui, state: ConnectionState, text: &str) {
    let (label, color) = match state {
        ConnectionState::Idle => ("空闲", Color32::from_rgb(122, 132, 146)),
        ConnectionState::Connecting => ("连接中", Color32::from_rgb(212, 159, 70)),
        ConnectionState::Connected => ("已连接", Color32::from_rgb(70, 146, 190)),
        ConnectionState::Paired => ("已对接", Color32::from_rgb(75, 172, 123)),
        ConnectionState::Direct => ("直连", Color32::from_rgb(57, 190, 112)),
        ConnectionState::DirectFailed => ("直连失败", Color32::from_rgb(220, 94, 94)),
    };

    Frame::new()
        .fill(Color32::from_rgb(24, 32, 43))
        .corner_radius(CornerRadius::same(8))
        .stroke(Stroke::new(1.0, color))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(RichText::new(label).color(color).strong());
                ui.label(RichText::new(text).color(Color32::from_rgb(188, 197, 208)));
            });
        });
}

fn chat_line(ui: &mut Ui, line: &ChatLine) {
    match line.author {
        ChatAuthor::Mine => {
            ui.with_layout(Layout::right_to_left(Align::TOP), |ui| {
                bubble(ui, "我", &line.text, Color32::from_rgb(48, 106, 138));
            });
        }
        ChatAuthor::Peer => {
            ui.with_layout(Layout::left_to_right(Align::TOP), |ui| {
                bubble(ui, "对方", &line.text, Color32::from_rgb(43, 57, 74));
            });
        }
        ChatAuthor::System => {
            let width = finite_width(ui.available_width(), 120.0);
            ui.scope(|ui| {
                ui.set_width(width);
                ui.add(
                    egui::Label::new(
                        RichText::new(&line.text).color(Color32::from_rgb(146, 157, 171)),
                    )
                    .wrap()
                    .halign(Align::Center),
                );
            });
        }
    }
}

fn bubble(ui: &mut Ui, name: &str, text: &str, color: Color32) {
    let width = finite_width(ui.available_width().min(420.0), 120.0);

    Frame::new()
        .fill(color)
        .corner_radius(CornerRadius::same(8))
        .inner_margin(egui::Margin::symmetric(12, 9))
        .show(ui, |ui| {
            ui.set_max_width(width);
            ui.label(
                RichText::new(name)
                    .small()
                    .color(Color32::from_rgb(202, 211, 222)),
            );
            ui.add(
                egui::Label::new(RichText::new(text).color(Color32::WHITE))
                    .wrap()
                    .selectable(false),
            );
        });
}

fn full_width_button(ui: &mut Ui, enabled: bool, label: &str, height: f32) -> egui::Response {
    let width = finite_width(ui.available_width(), 120.0);
    ui.add_enabled(
        enabled,
        egui::Button::new(label).min_size(Vec2::new(width, height)),
    )
}

fn row_text_width(available_width: f32) -> f32 {
    finite_width(available_width - 184.0, 120.0)
}

fn finite_width(width: f32, fallback: f32) -> f32 {
    if width.is_finite() && width > 0.0 {
        width
    } else {
        fallback
    }
}

fn transfer_row(ui: &mut Ui, transfer: &TransferLine) -> Option<TransferAction> {
    let mut action = None;
    let fraction = if transfer.total_bytes == 0 {
        1.0
    } else {
        (transfer.completed_bytes as f32 / transfer.total_bytes as f32).clamp(0.0, 1.0)
    };
    let text_width = row_text_width(ui.available_width());

    ui.horizontal(|ui| {
        let direction = match transfer.direction {
            TransferDirection::Send => "发送",
            TransferDirection::Receive => "接收",
        };
        let status = transfer_status_label(&transfer.status);

        ui.vertical(|ui| {
            ui.set_width(text_width);
            ui.add(egui::Label::new(RichText::new(&transfer.file_name).strong()).wrap());
            ui.add(
                egui::Label::new(
                    RichText::new(format!(
                        "{direction} · {status} · {} / {}",
                        format_bytes(transfer.completed_bytes),
                        format_bytes(transfer.total_bytes)
                    ))
                    .small()
                    .color(Color32::from_rgb(158, 169, 185)),
                )
                .wrap(),
            );
            ui.add(
                egui::ProgressBar::new(fraction)
                    .desired_width(text_width)
                    .show_percentage(),
            );
        });

        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            let cancellable = !matches!(
                transfer.status,
                TransferStatus::Complete | TransferStatus::Cancelled
            );
            if ui
                .add_enabled(cancellable, egui::Button::new("取消"))
                .clicked()
            {
                action = Some(TransferAction::Cancel);
            }

            match transfer.status {
                TransferStatus::Paused | TransferStatus::Failed => {
                    if ui.button("继续").clicked() {
                        action = Some(TransferAction::Resume);
                    }
                }
                TransferStatus::Offered | TransferStatus::Accepted => {
                    if ui.button("暂停").clicked() {
                        action = Some(TransferAction::Pause);
                    }
                }
                TransferStatus::Complete | TransferStatus::Cancelled => {}
            }
        });
    });

    action
}

fn pending_offer_row(ui: &mut Ui, metadata: &FileMetadata) -> Option<TransferAction> {
    let mut action = None;
    let text_width = row_text_width(ui.available_width());

    ui.horizontal(|ui| {
        ui.vertical(|ui| {
            ui.set_width(text_width);
            ui.add(egui::Label::new(RichText::new(&metadata.file_name).strong()).wrap());
            ui.add(
                egui::Label::new(
                    RichText::new(format!(
                        "待接收 · {} · {} 个分段",
                        format_bytes(metadata.file_size),
                        metadata.total_chunks
                    ))
                    .small()
                    .color(Color32::from_rgb(158, 169, 185)),
                )
                .wrap(),
            );
        });

        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            if ui.button("拒绝").clicked() {
                action = Some(TransferAction::Reject);
            }
            if ui.button("接收").clicked() {
                action = Some(TransferAction::Accept);
            }
        });
    });

    action
}

fn transfer_status_label(status: &TransferStatus) -> &'static str {
    match status {
        TransferStatus::Offered => "等待确认",
        TransferStatus::Accepted => "传输中",
        TransferStatus::Paused => "已暂停",
        TransferStatus::Complete => "已完成",
        TransferStatus::Cancelled => "已取消",
        TransferStatus::Failed => "失败",
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn display_file_name(path: &std::path::Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| path.display().to_string())
}

fn format_connect_info(label: &str, info: &ConnectInfo) -> String {
    let candidates = if info.candidates.is_empty() {
        "无".to_string()
    } else {
        info.candidates
            .iter()
            .map(|candidate| {
                let kind = match candidate.kind {
                    CandidateKind::Local => "local",
                    CandidateKind::ServerReflexive => "server-reflexive",
                };
                format!("{} ({kind})", candidate.addr)
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    format!("{label} [{}]：{candidates}", role_label(&info.role))
}

fn role_label(role: &p2p_core::signaling::SignalingRole) -> &'static str {
    match role {
        p2p_core::signaling::SignalingRole::Host => "host",
        p2p_core::signaling::SignalingRole::Guest => "guest",
    }
}

fn empty_chat(ui: &mut Ui) {
    ui.vertical_centered(|ui| {
        ui.add_space(180.0);
        ui.label(RichText::new("创建房间或输入房间号后开始聊天").size(18.0));
        ui.label(RichText::new("消息会显示在这里").color(Color32::from_rgb(146, 157, 171)));
    });
}

fn config_path() -> Option<PathBuf> {
    config_dir().map(|dir| dir.join("p2p-signaling").join("gui.json"))
}

#[cfg(target_os = "windows")]
fn config_dir() -> Option<PathBuf> {
    env_path("APPDATA").or_else(|| env_path("LOCALAPPDATA"))
}

#[cfg(target_os = "macos")]
fn config_dir() -> Option<PathBuf> {
    home_dir().map(|home| home.join("Library").join("Application Support"))
}

#[cfg(all(unix, not(target_os = "macos")))]
fn config_dir() -> Option<PathBuf> {
    env_path("XDG_CONFIG_HOME").or_else(|| home_dir().map(|home| home.join(".config")))
}

#[cfg(not(any(target_os = "windows", target_os = "macos", unix)))]
fn config_dir() -> Option<PathBuf> {
    None
}

fn env_path(key: &str) -> Option<PathBuf> {
    let value = std::env::var_os(key)?;
    if value.is_empty() {
        None
    } else {
        Some(PathBuf::from(value))
    }
}

#[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
fn home_dir() -> Option<PathBuf> {
    env_path("HOME")
}

fn load_config(path: &PathBuf) -> Option<StoredConfig> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

fn save_config(path: Option<&PathBuf>, config: &StoredConfig) -> anyhow::Result<()> {
    let path = path.ok_or_else(|| anyhow::anyhow!("找不到系统配置目录"))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_string_pretty(config)?)?;
    Ok(())
}

fn require_value(flag: &str, value: Option<String>) -> anyhow::Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("{flag} requires a value"))
}

fn normalize_room_input(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_digit())
        .take(4)
        .collect()
}

fn normalize_room_input_in_place(value: &mut String) {
    let normalized = normalize_room_input(value);
    if *value != normalized {
        *value = normalized;
    }
}

fn is_valid_room(room: &str) -> bool {
    room.len() == 4 && room.chars().all(|character| character.is_ascii_digit())
}

fn can_send_to_room(state: ConnectionState, has_handle: bool) -> bool {
    has_handle && state == ConnectionState::Direct
}

fn remote_state_session_id(state: &RemoteDesktopState) -> Option<&str> {
    match state {
        RemoteDesktopState::Idle => None,
        RemoteDesktopState::OutgoingPending(offer) | RemoteDesktopState::IncomingPending(offer) => {
            Some(&offer.session_id)
        }
        RemoteDesktopState::Sharing { session_id, .. }
        | RemoteDesktopState::Viewing { session_id, .. } => Some(session_id),
    }
}

fn egui_key_to_scan_code(key: egui::Key) -> Option<(u16, bool, bool)> {
    use egui::Key;
    let value = match key {
        Key::Escape => (0x01, false, false),
        Key::Num1 => (0x02, false, true),
        Key::Num2 => (0x03, false, true),
        Key::Num3 => (0x04, false, true),
        Key::Num4 => (0x05, false, true),
        Key::Num5 => (0x06, false, true),
        Key::Num6 => (0x07, false, true),
        Key::Num7 => (0x08, false, true),
        Key::Num8 => (0x09, false, true),
        Key::Num9 => (0x0A, false, true),
        Key::Num0 => (0x0B, false, true),
        Key::Backspace => (0x0E, false, false),
        Key::Tab => (0x0F, false, false),
        Key::Q => (0x10, false, true),
        Key::W => (0x11, false, true),
        Key::E => (0x12, false, true),
        Key::R => (0x13, false, true),
        Key::T => (0x14, false, true),
        Key::Y => (0x15, false, true),
        Key::U => (0x16, false, true),
        Key::I => (0x17, false, true),
        Key::O => (0x18, false, true),
        Key::P => (0x19, false, true),
        Key::Enter => (0x1C, false, false),
        Key::A => (0x1E, false, true),
        Key::S => (0x1F, false, true),
        Key::D => (0x20, false, true),
        Key::F => (0x21, false, true),
        Key::G => (0x22, false, true),
        Key::H => (0x23, false, true),
        Key::J => (0x24, false, true),
        Key::K => (0x25, false, true),
        Key::L => (0x26, false, true),
        Key::Z => (0x2C, false, true),
        Key::X => (0x2D, false, true),
        Key::C => (0x2E, false, true),
        Key::V => (0x2F, false, true),
        Key::B => (0x30, false, true),
        Key::N => (0x31, false, true),
        Key::M => (0x32, false, true),
        Key::Space => (0x39, false, false),
        Key::F1 => (0x3B, false, false),
        Key::F2 => (0x3C, false, false),
        Key::F3 => (0x3D, false, false),
        Key::F4 => (0x3E, false, false),
        Key::F5 => (0x3F, false, false),
        Key::F6 => (0x40, false, false),
        Key::F7 => (0x41, false, false),
        Key::F8 => (0x42, false, false),
        Key::F9 => (0x43, false, false),
        Key::F10 => (0x44, false, false),
        Key::Home => (0x47, true, false),
        Key::ArrowUp => (0x48, true, false),
        Key::PageUp => (0x49, true, false),
        Key::ArrowLeft => (0x4B, true, false),
        Key::ArrowRight => (0x4D, true, false),
        Key::End => (0x4F, true, false),
        Key::ArrowDown => (0x50, true, false),
        Key::PageDown => (0x51, true, false),
        Key::Insert => (0x52, true, false),
        Key::Delete => (0x53, true, false),
        Key::F11 => (0x57, false, false),
        Key::F12 => (0x58, false, false),
        _ => return None,
    };
    Some(value)
}

fn remote_modifier_changes(
    previous: egui::Modifiers,
    next: egui::Modifiers,
    platform: RemoteDesktopPlatform,
) -> Vec<(bool, bool, u16, bool)> {
    let mut changes = vec![
        (previous.shift, next.shift, 0x2A, false),
        (previous.alt, next.alt, 0x38, false),
    ];
    match platform {
        RemoteDesktopPlatform::Windows => {
            changes.push((
                previous.command || previous.ctrl,
                next.command || next.ctrl,
                0x1D,
                false,
            ));
        }
        RemoteDesktopPlatform::Macos if cfg!(target_os = "macos") => {
            changes.push((previous.ctrl, next.ctrl, 0x1D, false));
            changes.push((previous.mac_cmd, next.mac_cmd, 0x5B, true));
        }
        RemoteDesktopPlatform::Macos => {
            changes.push((
                previous.command || previous.ctrl,
                next.command || next.ctrl,
                0x5B,
                true,
            ));
        }
    }
    changes
}

fn state_after_connected_event(state: ConnectionState) -> ConnectionState {
    match state {
        ConnectionState::Connecting => ConnectionState::Connected,
        _ => state,
    }
}

fn state_after_peer_activity(state: ConnectionState, has_handle: bool) -> ConnectionState {
    if has_handle
        && matches!(
            state,
            ConnectionState::Connecting | ConnectionState::Connected
        )
    {
        ConnectionState::Paired
    } else {
        state
    }
}

fn signaling_base(server: &str) -> anyhow::Result<String> {
    let server = server.trim().trim_end_matches('/');

    if server.is_empty() {
        anyhow::bail!("信令服务器地址不能为空");
    }

    let base = if server.starts_with("wss://") || server.starts_with("ws://") {
        server.to_string()
    } else if let Some(host) = server.strip_prefix("https://") {
        format!("wss://{host}")
    } else if let Some(host) = server.strip_prefix("http://") {
        format!("ws://{host}")
    } else {
        let scheme = if is_local_server(server) { "ws" } else { "wss" };
        format!("{scheme}://{server}")
    };

    Ok(base)
}

fn build_signaling_url(server: &str, room: &str) -> anyhow::Result<String> {
    let room = room.trim().trim_matches('/');

    if !is_valid_room(room) {
        anyhow::bail!("房间号需要 4 位数字");
    }

    Ok(append_room_path(&signaling_base(server)?, room))
}

/// 房主连接 /rooms/new，由服务器分配房间码
fn build_host_signaling_url(server: &str) -> anyhow::Result<String> {
    Ok(append_room_path(&signaling_base(server)?, "new"))
}

fn append_room_path(base: &str, room: &str) -> String {
    if base.contains("/rooms/") {
        base.to_string()
    } else {
        format!("{base}/rooms/{room}")
    }
}

fn is_local_server(server: &str) -> bool {
    server.starts_with("localhost")
        || server.starts_with("127.")
        || server.starts_with("[::1]")
        || server.starts_with("::1")
}

fn print_usage() {
    println!(
        "Usage: p2p-gui [SERVER] [--server SERVER] [--room ROOM] [--role host|guest]\n\
         \n\
         --room is only used by guests; the host's room code is assigned by the server.\n\
         Optional test automation: --test-message TEXT\n\
         \n\
         SERVER may be a domain, IP, http(s) URL, or ws(s) URL.\n\
         Default: {DEFAULT_SERVER}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_wss_url_from_domain() {
        let url = build_signaling_url("p2p-signaling.yizhe.studio", "1234").unwrap();
        assert_eq!(url, "wss://p2p-signaling.yizhe.studio/rooms/1234");
    }

    #[test]
    fn builds_ws_url_from_local_ip() {
        let url = build_signaling_url("127.0.0.1:8787", "1234").unwrap();
        assert_eq!(url, "ws://127.0.0.1:8787/rooms/1234");
    }

    #[test]
    fn converts_https_to_wss() {
        let url = build_signaling_url("https://example.com/", "1234").unwrap();
        assert_eq!(url, "wss://example.com/rooms/1234");
    }

    #[test]
    fn keeps_full_websocket_room_url() {
        let url = build_signaling_url("wss://example.com/rooms/5678", "1234").unwrap();
        assert_eq!(url, "wss://example.com/rooms/5678");
    }

    #[test]
    fn builds_host_url_without_room() {
        let url = build_host_signaling_url("p2p-signaling.yizhe.studio").unwrap();
        assert_eq!(url, "wss://p2p-signaling.yizhe.studio/rooms/new");
    }

    #[test]
    fn builds_local_host_url() {
        let url = build_host_signaling_url("127.0.0.1:8787").unwrap();
        assert_eq!(url, "ws://127.0.0.1:8787/rooms/new");
    }

    #[test]
    fn room_input_is_digits_only_and_four_chars() {
        assert_eq!(normalize_room_input("A12-345"), "1234");
        assert!(is_valid_room("0000"));
        assert!(!is_valid_room("123"));
    }

    #[test]
    fn chat_send_requires_peer_connection() {
        assert!(!can_send_to_room(ConnectionState::Idle, false));
        assert!(!can_send_to_room(ConnectionState::Connecting, true));
        assert!(!can_send_to_room(ConnectionState::Connected, true));
        assert!(!can_send_to_room(ConnectionState::Paired, true));
        assert!(can_send_to_room(ConnectionState::Direct, true));
        assert!(!can_send_to_room(ConnectionState::DirectFailed, true));
        assert!(!can_send_to_room(ConnectionState::Paired, false));
    }

    #[test]
    fn connected_event_does_not_downgrade_paired_state() {
        assert_eq!(
            state_after_connected_event(ConnectionState::Connecting),
            ConnectionState::Connected
        );
        assert_eq!(
            state_after_connected_event(ConnectionState::Paired),
            ConnectionState::Paired
        );
        assert_eq!(
            state_after_connected_event(ConnectionState::Direct),
            ConnectionState::Direct
        );
    }

    #[test]
    fn peer_activity_marks_active_session_as_paired() {
        assert_eq!(
            state_after_peer_activity(ConnectionState::Connected, true),
            ConnectionState::Paired
        );
        assert_eq!(
            state_after_peer_activity(ConnectionState::Connecting, true),
            ConnectionState::Paired
        );
        assert_eq!(
            state_after_peer_activity(ConnectionState::Connected, false),
            ConnectionState::Connected
        );
    }

    #[test]
    fn row_widths_never_produce_infinite_layout_sizes() {
        assert_eq!(finite_width(f32::INFINITY, 120.0), 120.0);
        assert_eq!(finite_width(f32::NAN, 120.0), 120.0);
        assert_eq!(row_text_width(f32::INFINITY), 120.0);
        assert_eq!(row_text_width(420.0), 236.0);
    }

    #[test]
    fn maps_primary_shortcut_to_windows_control() {
        let next = egui::Modifiers {
            command: true,
            ..egui::Modifiers::NONE
        };
        assert!(remote_modifier_changes(
            egui::Modifiers::NONE,
            next,
            RemoteDesktopPlatform::Windows
        )
        .contains(&(false, true, 0x1D, false)));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn keeps_macos_control_and_command_distinct() {
        let next = egui::Modifiers {
            ctrl: true,
            mac_cmd: true,
            command: true,
            ..egui::Modifiers::NONE
        };
        let changes =
            remote_modifier_changes(egui::Modifiers::NONE, next, RemoteDesktopPlatform::Macos);
        assert!(changes.contains(&(false, true, 0x1D, false)));
        assert!(changes.contains(&(false, true, 0x5B, true)));
    }
}
