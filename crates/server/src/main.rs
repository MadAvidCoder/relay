mod connection_manager;

use connection_manager::ConnectionManager;
use relay_common::{AuthError, ClientProtocol, ServerProtocol};
use std::sync::Arc;
use tokio::sync::RwLock;

use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::Response,
    routing::{any, get},
};
use tokio::net::TcpListener;

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let (conns_tx, mut conns_rx) = tokio::sync::mpsc::channel::<ServerProtocol>(16);
    let mut user_id: Option<uuid::Uuid> = None;
    let mut conn_id: Option<uuid::Uuid> = None;
    loop {
        tokio::select! {
            msg = conns_rx.recv() => {
                match msg {
                    Some(msg) => {
                        if let Err(e) = socket.send(Message::Text(msg.to_json().unwrap_or("".into()).into())).await {
                            eprintln!("Error sending message to client: {:?}", e);
                            break;
                        }
                    },
                    None => {
                        eprintln!("Connection manager channel closed.");
                        break;
                    }
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(msg)) => match msg {
                        Message::Text(text) => {
                            match ClientProtocol::from_json(&text) {
                                Ok(client_msg) => {
                                    match client_msg {
                                        ClientProtocol::Auth { user_id: recv_user_id } => {
                                            if user_id.is_some() {
                                                eprintln!("User is already authenticated. Ignoring auth request.");
                                                continue;
                                            }
                                            let connection = relay_common::Connection::new(recv_user_id);
                                            conn_id = Some(connection.id);
                                            let active_connection = connection_manager::ActiveConnection {
                                                connection: connection.clone(),
                                                sender: conns_tx.clone(),
                                            };
                                            let mut conns_perms = state.conns.write().await;
                                            let res = conns_perms.register(recv_user_id, active_connection);
                                            drop(conns_perms);
                                            match res {
                                                Ok(_) => {
                                                    user_id = Some(recv_user_id);
                                                    socket.send(Message::Text(ServerProtocol::AuthSuccess { user_id: recv_user_id, timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() }.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending auth success message to client: {:?}", e);
                                                    });
                                                }
                                                Err(e) => {
                                                    eprintln!("Error registering connection: {:?}", e);
                                                    socket.send(Message::Text(ServerProtocol::AuthFailure { user_id: recv_user_id, reason: AuthError::UnknownError, timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() }.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending auth failure message to client: {:?}", e);
                                                    });
                                                }
                                            }
                                        }
                                        ClientProtocol::SendMessage { message, recipient_id } => {
                                            if user_id.is_none() {
                                                eprintln!("User is not authenticated. Cannot send message.");
                                                continue;
                                            }
                                            let sender_id = user_id.unwrap();
                                            let msg = relay_common::Message::new(sender_id, recipient_id, message);
                                            let state_read = state.conns.read().await;
                                            let sender = state_read.get(recipient_id).map(|recipient_conn| recipient_conn.sender.clone());
                                            drop(state_read);
                                            if let Some(sender) = sender {
                                                sender.send(ServerProtocol::NewMessage { message_id: msg.id, sender_id: msg.sender_id, recipient_id: msg.recipient_id, message: msg.content, timestamp: msg.timestamp }).await.unwrap_or_else(|e| {
                                                    eprintln!("Error sending message to recipient: {:?}", e);
                                                });

                                            } else {
                                                eprintln!("Recipient is not connected. Cannot send message.");
                                            }

                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Error parsing client message: {:?}", e);
                                }
                            }
                        }
                        Message::Close(frame) => {
                            println!("Client disconnected: {:?}", frame);
                            break;
                        }
                        _ => {}
                    },
                    _ => {
                        println!("Client disconnected or error occurred.");
                        break;
                    }
                }
            }
        }
    }
    if let (Some(uid), Some(conn_id)) = (user_id, conn_id) {
        let mut state_write_perms = state.conns.write().await;
        state_write_perms.remove(uid, conn_id).unwrap_or_else(|e| {
            eprintln!("Error removing connection: {:?}", e);
        });
    }
}

async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |ws| handle_socket(ws, state))
}

#[derive(Debug, Clone)]
struct AppState {
    conns: Arc<RwLock<ConnectionManager>>,
}

#[tokio::main]
async fn main() {
    let app: Router = Router::new()
        .route("/ws", get(handler))
        .with_state(AppState {
            conns: Arc::new(RwLock::new(ConnectionManager::new())),
        });
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Listening on 127.0.0.1:3000");

    axum::serve(listener, app).await.unwrap();
}
