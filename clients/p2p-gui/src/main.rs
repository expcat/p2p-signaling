use tokio::sync::mpsc;

use p2p_core::{ChatSession, SessionEvent, SessionRole};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel::<SessionEvent>(32);

    let role = SessionRole::Host;
    let session = ChatSession::new(role, "wss://your-domain/rooms/LOCALHOST".to_string());
    let handle = session.start(tx).await?;

    tokio::spawn(async move {
        let _ = handle
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

