use axum::{
    Router,
    extract::{WebSocketUpgrade, ws::WebSocket},
    response::Response,
    routing::any,
};
use tokio::net::TcpListener;

async fn handle_socket(mut socket: WebSocket) {
    while let Some(msg) = socket.recv().await {
        if let Ok(msg) = msg {
            socket.send(msg).await.unwrap();
        } else {
            break;
        }
    }
}

async fn handler(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_socket)
}

#[tokio::main]
async fn main() {
    let app: Router = Router::new().route("/ws", any(handler));
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Listening on 127.0.0.1:3000");

    axum::serve(listener, app).await.unwrap();
}
