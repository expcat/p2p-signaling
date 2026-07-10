pub mod direct;
pub mod nat;
pub mod p2p_proto;
pub mod remote_desktop;
pub mod session;
pub mod signaling;
pub mod transfer;

mod platform_dirs;

pub use direct::DirectLinkInfo;
pub use nat::{Candidate, CandidateKind, ConnectInfo, ConnectInfoKind};
pub use remote_desktop::{
    RemoteDesktopConfig, RemoteDesktopEvent, RemoteDesktopFrame, RemoteDesktopOffer,
    RemoteDesktopState, RemoteDisplay, RemoteInputEvent, RemotePointerButton,
};
pub use session::{
    ChatSession, ChatSessionHandle, FileTransferProgress, SessionEvent, SessionRole,
};
