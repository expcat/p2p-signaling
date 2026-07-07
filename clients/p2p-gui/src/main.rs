use tokio::sync::mpsc;

use p2p_core::{ChatSession, SessionEvent, SessionRole};

const DEFAULT_SERVER: &str = "p2p-signaling.yizhe.studio";
const DEFAULT_ROOM: &str = "LOCALHOST";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel::<SessionEvent>(32);

    let config = ClientConfig::from_args(std::env::args().skip(1))?;
    println!("Connecting to {}", config.signaling_url);
    let session = ChatSession::new(config.role, config.signaling_url);
    let handle = session.start(tx).await?;

    let send_handle = handle.clone();
    tokio::spawn(async move {
        let _ = send_handle
            .send_text("hello from the GUI shell".to_string())
            .await;
    });

    while let Some(event) = rx.recv().await {
        println!("EVENT: {event:?}");

        if matches!(event, SessionEvent::Error(_)) {
            break;
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct ClientConfig {
    role: SessionRole,
    signaling_url: String,
}

impl ClientConfig {
    fn from_args(args: impl IntoIterator<Item = String>) -> anyhow::Result<Self> {
        let mut server = DEFAULT_SERVER.to_string();
        let mut room = DEFAULT_ROOM.to_string();
        let mut role = "host".to_string();

        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--server" | "-s" => {
                    server = require_value(arg.as_str(), args.next())?;
                }
                "--room" | "-r" => {
                    room = require_value(arg.as_str(), args.next())?;
                }
                "--role" => {
                    role = require_value(arg.as_str(), args.next())?;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                value if !value.starts_with('-') => {
                    server = value.to_string();
                }
                _ => anyhow::bail!("unknown argument: {arg}"),
            }
        }

        let signaling_url = build_signaling_url(&server, &room)?;
        let role = match role.as_str() {
            "host" => SessionRole::Host {
                room_code: room.clone(),
            },
            "guest" => SessionRole::Guest { room_code: room },
            _ => anyhow::bail!("--role must be either host or guest"),
        };

        Ok(Self {
            role,
            signaling_url,
        })
    }
}

fn require_value(flag: &str, value: Option<String>) -> anyhow::Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("{flag} requires a value"))
}

fn build_signaling_url(server: &str, room: &str) -> anyhow::Result<String> {
    let server = server.trim().trim_end_matches('/');
    let room = room.trim().trim_matches('/');

    if server.is_empty() {
        anyhow::bail!("server must not be empty");
    }
    if room.is_empty() {
        anyhow::bail!("room must not be empty");
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
         SERVER may be a domain, IP, http(s) URL, or ws(s) URL.\n\
         Default: {DEFAULT_SERVER}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_wss_url_from_domain() {
        let url = build_signaling_url("p2p-signaling.yizhe.studio", "ROOM1").unwrap();
        assert_eq!(url, "wss://p2p-signaling.yizhe.studio/rooms/ROOM1");
    }

    #[test]
    fn builds_ws_url_from_local_ip() {
        let url = build_signaling_url("127.0.0.1:8787", "ROOM1").unwrap();
        assert_eq!(url, "ws://127.0.0.1:8787/rooms/ROOM1");
    }

    #[test]
    fn converts_https_to_wss() {
        let url = build_signaling_url("https://example.com/", "ROOM1").unwrap();
        assert_eq!(url, "wss://example.com/rooms/ROOM1");
    }

    #[test]
    fn keeps_full_websocket_room_url() {
        let url = build_signaling_url("wss://example.com/rooms/ROOM2", "ROOM1").unwrap();
        assert_eq!(url, "wss://example.com/rooms/ROOM2");
    }
}
