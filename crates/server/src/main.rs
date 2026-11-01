use axum::{
    Router,
    extract::{State, WebSocketUpgrade, ws::WebSocket},
    response::Response,
    routing::any,
};
use relay_common::Message;
use tokio::{net::TcpListener, sync::broadcast};

async fn handle_socket(mut socket: WebSocket, tx_bus: broadcast::Sender<Message>) {
    let mut rx_bus = tx_bus.subscribe();
    let mut id: Option<String> = None;
    loop {
        tokio::select! {
            msg = socket.recv() => {
                if let Some(msg) = msg {
                    if let Ok(msg) = msg {
                        let decoded = match msg.to_text().map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)).and_then(|text| Message::from_json(text).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))) {
                            Ok(decoded) => decoded,
                            Err(e) => {
                                eprintln!("Error decoding message: {:?}", e);
                                continue;
                            }
                        };
                        match &decoded {
                            Message::Auth { user_id } => {
                                println!("User {} connected", user_id);
                                id = Some(user_id.clone());
                            }
                            Message::Chat { user_id, .. } => {
                                if id.is_none() || id.as_deref() != Some(user_id) {
                                    // Must be logged in as the user sending the message
                                    continue;
                                }
                                tx_bus.send(decoded).unwrap_or(0);
                            }
                            Message::Ack { .. } => {}
                        }
                    }
                } else {
                    // WS closed
                    break;
                }
            },
            msg = rx_bus.recv() => {
                if id.is_some() {
                    match msg {
                        Ok(msg) => {
                            if id.is_none() {
                                continue;
                            }
                            if let Message::Chat { user_id, .. } = &msg {
                                    if id.as_deref() == Some(user_id) {
                                        // Don't resend self
                                        continue;
                                    }
                                socket.send(msg.to_json().unwrap_or_default().into()).await.unwrap_or(());
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => eprintln!("Lagged :("),
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            },
        };
    }
}

async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    let tx = state.bus_tx.clone();
    ws.on_upgrade(move |ws| handle_socket(ws, tx))
}

#[derive(Debug, Clone)]
struct AppState {
    bus_tx: broadcast::Sender<Message>,
}

#[tokio::main]
async fn main() {
    let (tx_bus, _) = broadcast::channel(32);

    let app: Router = Router::new()
        .route("/ws", any(handler))
        .with_state(AppState { bus_tx: tx_bus });
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Listening on 127.0.0.1:3000");

    axum::serve(listener, app).await.unwrap();
}
