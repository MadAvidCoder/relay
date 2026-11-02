use futures_util::{SinkExt, StreamExt};
use relay_common::{ClientProtocol};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (ws_stream, resp) = connect_async("ws://127.0.0.1:3000/ws").await?;
    let (mut tx, mut rx) = ws_stream.split();

    let username = "test_user".to_string();

    tx.send(Message::Text(ClientProtocol::Auth { username }.to_json().unwrap_or("".into()).into())).await?;

    while let Some(msg) = rx.next().await {
        match msg {
            Ok(msg) => match msg {
                Message::Close(frame) => {
                    println!("Connection closed: {:?}", frame);
                    break;
                }
                _ => println!("Received message: {:?}", msg),
            },
            Err(e) => {
                eprintln!("Error receiving message: {:?}", e);
                break;
            }
        }
    }

    Ok(())
}
