mod connection_manager;

use connection_manager::ConnectionManager;
use relay_common::{AuthError, ClientProtocol, Connection, ErrorCodes, ServerProtocol};
use std::sync::Arc;
use tokio::sync::RwLock;

use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::Response,
    routing::get,
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
                        if let Err(e) = if let Ok(msg) = msg.to_json() {
                            socket.send(Message::Text(msg.into())).await
                        } else {
                            socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::InternalError, timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()}.to_json().unwrap_or("".into()).into())).await
                        }
                        {
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
                                        ClientProtocol::Auth { username } => {
                                            if let Some(user_id) = user_id {
                                                eprintln!("User is already authenticated. Ignoring auth request.");
                                                socket.send(Message::Text(ServerProtocol::AuthFailure { user_id, reason: AuthError::AlreadyAuthenticated, timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() }.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                    eprintln!("Error sending auth failure message to client: {:?}", e);
                                                });
                                                continue;
                                            }
                                            let (new, user) = if state.users.read().await.values().any(|name| name == &username) {
                                                (false, relay_common::User {
                                                    id: state.users.read().await.iter().find(|(_, name)| *name == &username).map(|(id, _)| *id).unwrap(),
                                                    username: username.clone(),
                                                })
                                            } else {
                                                (true, relay_common::User::new(username.clone()))
                                            };
                                            if new {
                                                state.users.write().await.insert(user.id, username.clone());
                                            }
                                            let connection = Connection::new(user.id);
                                            conn_id = Some(connection.id);
                                            let active_connection = connection_manager::ActiveConnection {
                                                connection: connection.clone(),
                                                sender: conns_tx.clone(),
                                            };
                                            let mut conns_perms = state.conns.write().await;
                                            let res = conns_perms.register(user.id, active_connection);
                                            drop(conns_perms);
                                            match res {
                                                Ok(_) => {
                                                    user_id = Some(user.id);
                                                    socket.send(Message::Text(ServerProtocol::AuthSuccess { user_id: user.id, timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() }.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending auth success message to client: {:?}", e);
                                                    });
                                                }
                                                Err(e) => {
                                                    eprintln!("Error registering connection: {:?}", e);
                                                    socket.send(Message::Text(ServerProtocol::AuthFailure { user_id: user.id, reason: AuthError::AlreadyConnected, timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() }.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending auth failure message to client: {:?}", e);
                                                    });
                                                }
                                            }
                                        }
                                        ClientProtocol::SendMessage { message, recipient_id } => {
                                            if user_id.is_none() {
                                                eprintln!("User is not authenticated. Cannot send message.");
                                                socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::NotAuthenticated, timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                    eprintln!("Error sending unauthenticated errror msg to client: {:?}", e);
                                                });
                                                continue;
                                            }
                                            let sender_id = user_id.unwrap();
                                            let msg = relay_common::Message::new(sender_id, recipient_id, message);
                                            let state_read = state.conns.read().await;
                                            let sender = state_read.get(recipient_id).map(|recipient_conn| recipient_conn.sender.clone());
                                            drop(state_read);
                                            if let Some(sender) = sender {
                                                if let Err(e) = sender.send(ServerProtocol::NewMessage { message_id: msg.id, sender_id: msg.sender_id, recipient_id: msg.recipient_id, message: msg.content, timestamp: msg.timestamp }).await
                                                {
                                                    eprintln!("Error sending message to recipient: {:?}", e);
                                                    socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::InternalError, timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending internal error to client: {:?}", e);
                                                    });
                                                }
                                            } else {
                                                eprintln!("Recipient is not connected. Cannot send message.");
                                                socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::RecipientNotFound, timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                    eprintln!("Error sending recipient not found error to client: {:?}", e);
                                                });
                                            }

                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Error parsing client message: {:?}", e);
                                    socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::InvalidRequest, timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                        eprintln!("Error sending invalid request error to client: {:?}", e);
                                    });
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
        drop(state_write_perms);
    }
}

async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |ws| handle_socket(ws, state))
}

#[derive(Debug, Clone)]
struct AppState {
    conns: Arc<RwLock<ConnectionManager>>,
    users: Arc<RwLock<std::collections::HashMap<uuid::Uuid, String>>>,
}

#[tokio::main]
async fn main() {
    let app: Router = Router::new()
        .route("/ws", get(handler))
        .with_state(AppState {
            conns: Arc::new(RwLock::new(ConnectionManager::new())),
            users: Arc::new(RwLock::new(std::collections::HashMap::new())),
        });
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Listening on 127.0.0.1:3000");

    axum::serve(listener, app).await.unwrap();
}
