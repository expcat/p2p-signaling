use std::future::Future;
use std::path::PathBuf;
use std::sync::mpsc as std_mpsc;
use std::thread::JoinHandle;

use eframe::egui::{
    self, Align, Color32, Context, CornerRadius, FontData, FontDefinitions, FontFamily, FontId,
    Frame, Layout, RichText, ScrollArea, Stroke, TextEdit, Ui, Vec2,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::{Builder, Handle};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::oneshot;

use p2p_core::transfer::{FileMetadata, TransferDirection, TransferStatus};
use p2p_core::{ChatSession, ChatSessionHandle, FileTransferProgress, SessionEvent, SessionRole};

const DEFAULT_SERVER: &str = "p2p-signaling.yizhe.studio";
const DEFAULT_ROOM: &str = "";

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
        let mut room = std::env::var("P2P_SIGNALING_ROOM").unwrap_or_else(|_| DEFAULT_ROOM.into());
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChatAuthor {
    Mine,
    Peer,
    System,
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
}

impl P2pChatApp {
    fn new(cc: &eframe::CreationContext<'_>, initial_config: ClientConfig) -> Self {
        configure_style(&cc.egui_ctx);

        let runtime = AsyncRuntime::new().expect("failed to create tokio runtime");
        let (notice_tx, notice_rx) = std_mpsc::channel();
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
        };

        if is_valid_room(&initial_room) {
            let role = match initial_role {
                RoleChoice::Host => SessionRole::Host {
                    room_code: initial_room,
                },
                RoleChoice::Guest => SessionRole::Guest {
                    room_code: initial_room,
                },
            };
            app.start_session(role);
        } else if initial_role == RoleChoice::Guest {
            app.status = "已填入房间号，可直接加入".into();
        }

        app
    }

    fn create_room(&mut self) {
        let room = random_room_code();
        self.room_input = room.clone();
        self.start_session(SessionRole::Host {
            room_code: room.clone(),
        });
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
        let action = if matches!(role, SessionRole::Host { .. }) {
            "已创建"
        } else {
            "正在加入"
        };
        let room = match &role {
            SessionRole::Host { room_code } | SessionRole::Guest { room_code } => room_code.clone(),
        };

        let signaling_url = match build_signaling_url(&self.server, &room) {
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

                let test_handle = handle.clone();
                self.handle = Some(handle);
                self.event_rx = Some(ui_event_rx);
                self.active_room = Some(room.clone());
                self.messages.clear();
                self.pending_offers.clear();
                self.transfers.clear();
                self.state = ConnectionState::Connecting;
                self.status = format!("正在连接房间 {room}");
                self.push_system(format!("房间 {room} {action}，正在连接信令服务。"));
                if let Some(message) = self.pending_test_message.take() {
                    self.push_system(format!("将在连接后自动发送测试消息：{message}"));
                    let notifier = self.notifier.clone();
                    self.runtime.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(1500)).await;
                        if let Err(error) = test_handle.send_text(message).await {
                            notifier.error(format!("发送测试消息失败：{error:#}"));
                        }
                    });
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

        if !can_send_to_room(self.state, true) {
            self.push_system("请等待连接房间后再发送文件。");
            return;
        }

        let Some(path) = rfd::FileDialog::new().pick_file() else {
            return;
        };
        let label = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("文件")
            .to_string();
        self.push_system(format!("准备发送文件：{label}"));

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

    fn poll_background_events(&mut self) {
        while let Ok(notice) = self.notice_rx.try_recv() {
            match notice {
                UiNotice::Error(message) => {
                    self.status = message.clone();
                    self.push_system(message);
                }
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
                SessionEvent::RoomCodeGenerated(room) => {
                    self.active_room = Some(room);
                }
                SessionEvent::PeerConnected => {
                    self.mark_peer_available(Some("另一客户端已加入房间。"));
                }
                SessionEvent::PeerDisconnected => {
                    if self.state == ConnectionState::Paired {
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
                SessionEvent::Error(message) => {
                    self.state = ConnectionState::Idle;
                    self.handle = None;
                    self.event_rx = None;
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
        self.status = "已对接，开始聊天或传文件".into();
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
                ui.horizontal(|ui| {
                    ui.label(RichText::new(self.room_label()).font(FontId::monospace(32.0)));
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if self.handle.is_some() && ui.button("断开").clicked() {
                            self.disconnect();
                        }
                    });
                });
                ui.add_space(8.0);

                let can_connect = !self.server.trim().is_empty();
                if full_width_button(ui, can_connect, "创建 4 位随机房间", 36.0).clicked() {
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

                ui.with_layout(Layout::bottom_up(Align::LEFT), |ui| {
                    ui.label(RichText::new(&self.status).color(Color32::from_rgb(158, 169, 185)));
                });
            });

        egui::CentralPanel::default().show(ui, |ui| {
            ui.add_space(8.0);
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
                    "连接房间后可发送消息"
                };
                let composer_button_width = 86.0 + 76.0 + ui.spacing().item_spacing.x * 2.0;
                let message_input_width = (ui.available_width() - composer_button_width).max(120.0);
                let response = ui.add_enabled(
                    can_send,
                    TextEdit::singleline(&mut self.message_input)
                        .hint_text(message_hint)
                        .desired_width(message_input_width),
                );
                let send_pressed = can_send
                    && response.lost_focus()
                    && ui.input(|input| input.key_pressed(egui::Key::Enter));

                if ui
                    .add_enabled(
                        can_send,
                        egui::Button::new("发送").min_size(Vec2::new(86.0, 32.0)),
                    )
                    .clicked()
                    || send_pressed
                {
                    self.send_message();
                }

                if ui
                    .add_enabled(
                        can_send,
                        egui::Button::new("文件").min_size(Vec2::new(76.0, 32.0)),
                    )
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

    let mut style = (*ctx.style_of(egui::Theme::Dark)).clone();
    style.spacing.item_spacing = Vec2::new(10.0, 10.0);
    style.spacing.button_padding = Vec2::new(12.0, 8.0);
    style.visuals = egui::Visuals::dark();
    style.visuals.panel_fill = Color32::from_rgb(13, 18, 26);
    style.visuals.window_fill = Color32::from_rgb(18, 24, 33);
    style.visuals.widgets.inactive.bg_fill = Color32::from_rgb(30, 39, 52);
    style.visuals.widgets.hovered.bg_fill = Color32::from_rgb(46, 59, 76);
    style.visuals.widgets.active.bg_fill = Color32::from_rgb(53, 103, 132);
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
            ui.horizontal_centered(|ui| {
                ui.label(RichText::new(&line.text).color(Color32::from_rgb(146, 157, 171)));
            });
        }
    }
}

fn bubble(ui: &mut Ui, name: &str, text: &str, color: Color32) {
    Frame::new()
        .fill(color)
        .corner_radius(CornerRadius::same(8))
        .inner_margin(egui::Margin::symmetric(12, 9))
        .show(ui, |ui| {
            ui.set_max_width(420.0);
            ui.label(
                RichText::new(name)
                    .small()
                    .color(Color32::from_rgb(202, 211, 222)),
            );
            ui.label(RichText::new(text).color(Color32::WHITE));
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
            ui.label(RichText::new(&transfer.file_name).strong());
            ui.label(
                RichText::new(format!(
                    "{direction} · {status} · {} / {}",
                    format_bytes(transfer.completed_bytes),
                    format_bytes(transfer.total_bytes)
                ))
                .small()
                .color(Color32::from_rgb(158, 169, 185)),
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
            ui.label(RichText::new(&metadata.file_name).strong());
            ui.label(
                RichText::new(format!(
                    "待接收 · {} · {} 个分段",
                    format_bytes(metadata.file_size),
                    metadata.total_chunks
                ))
                .small()
                .color(Color32::from_rgb(158, 169, 185)),
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

fn empty_chat(ui: &mut Ui) {
    ui.vertical_centered(|ui| {
        ui.add_space(180.0);
        ui.label(RichText::new("创建房间或输入房间号后开始聊天").size(18.0));
        ui.label(RichText::new("消息会显示在这里").color(Color32::from_rgb(146, 157, 171)));
    });
}

fn config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|dir| dir.join("p2p-signaling").join("gui.json"))
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

fn random_room_code() -> String {
    static ROOM_COUNTER: AtomicU64 = AtomicU64::new(0);

    let mut bytes = [0_u8; 2];
    if getrandom::getrandom(&mut bytes).is_ok() {
        return format!("{:04}", u16::from_ne_bytes(bytes) % 10_000);
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64);
    let sequence = ROOM_COUNTER.fetch_add(1, Ordering::Relaxed);

    format!("{:04}", now.wrapping_add(sequence) % 10_000)
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
    has_handle && matches!(state, ConnectionState::Connected | ConnectionState::Paired)
}

fn state_after_connected_event(state: ConnectionState) -> ConnectionState {
    match state {
        ConnectionState::Connecting => ConnectionState::Connected,
        ConnectionState::Paired => ConnectionState::Paired,
        ConnectionState::Connected | ConnectionState::Idle => state,
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

fn build_signaling_url(server: &str, room: &str) -> anyhow::Result<String> {
    let server = server.trim().trim_end_matches('/');
    let room = room.trim().trim_matches('/');

    if server.is_empty() {
        anyhow::bail!("信令服务器地址不能为空");
    }
    if !is_valid_room(room) {
        anyhow::bail!("房间号需要 4 位数字");
    }

    let url = if server.starts_with("wss://") || server.starts_with("ws://") {
        append_room_path(server, room)
    } else if let Some(host) = server.strip_prefix("https://") {
        append_room_path(&format!("wss://{host}"), room)
    } else if let Some(host) = server.strip_prefix("http://") {
        append_room_path(&format!("ws://{host}"), room)
    } else {
        let scheme = if is_local_server(server) { "ws" } else { "wss" };
        append_room_path(&format!("{scheme}://{server}"), room)
    };

    Ok(url)
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
    fn room_input_is_digits_only_and_four_chars() {
        assert_eq!(normalize_room_input("A12-345"), "1234");
        assert!(is_valid_room("0000"));
        assert!(!is_valid_room("123"));
    }

    #[test]
    fn chat_send_requires_peer_connection() {
        assert!(!can_send_to_room(ConnectionState::Idle, false));
        assert!(!can_send_to_room(ConnectionState::Connecting, true));
        assert!(can_send_to_room(ConnectionState::Connected, true));
        assert!(can_send_to_room(ConnectionState::Paired, true));
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
}
