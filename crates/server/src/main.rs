use axum::{
    Router,
    extract::{State, WebSocketUpgrade, ws::WebSocket},
    response::Response,
    routing::any,
};
use tokio::{net::TcpListener, sync::broadcast};

async fn handle_socket(mut socket: WebSocket, tx_bus: broadcast::Sender<String>) {
    let mut rx_bus = tx_bus.subscribe();
    loop {
        tokio::select! {
            msg = socket.recv() => {
                if let Some(msg) = msg {
                    if let Ok(msg) = msg {
                        tx_bus.send(msg.to_text().unwrap_or("").into()).unwrap_or(0);
                    }
                } else {
                    // WS closed
                    break;
                }
            },
            msg = rx_bus.recv() => {
                match msg {
                    Ok(msg) => socket.send(msg.into()).await.unwrap_or(()),
                    Err(broadcast::error::RecvError::Lagged(_)) => eprintln!("Lagged :("),
                    Err(broadcast::error::RecvError::Closed) => break,
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
    bus_tx: broadcast::Sender<String>,
}

#[tokio::main]
async fn main() {
    let (tx_bus, _) = broadcast::channel(16);

    let app: Router = Router::new()
        .route("/ws", any(handler))
        .with_state(AppState { bus_tx: tx_bus });
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Listening on 127.0.0.1:3000");

    axum::serve(listener, app).await.unwrap();
}
