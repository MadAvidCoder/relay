mod connection_manager;
mod db;

use connection_manager::ConnectionManager;
use relay_common::{AuthError, ClientProtocol, Connection, ErrorCodes, ServerProtocol};
use sqlx::postgres::PgPool;
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
                            socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::InternalError, timestamp: chrono::Utc::now() }.to_json().unwrap_or("".into()).into())).await
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
                                                socket.send(Message::Text(ServerProtocol::AuthFailure { user_id, reason: AuthError::AlreadyAuthenticated, timestamp: chrono::Utc::now() }.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                    eprintln!("Error sending auth failure message to client: {:?}", e);
                                                });
                                                continue;
                                            }
                                            let user = if let Some(user) = match db::users::get_user_by_username(&state.db, username.clone()).await {
                                                Ok(u) => u,
                                                Err(e) => {
                                                    eprintln!("Error querying user from database: {:?}", e);
                                                    socket.send(Message::Text(ServerProtocol::AuthFailure { user_id: uuid::Uuid::nil(), reason: AuthError::UnknownError, timestamp: chrono::Utc::now() }.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending auth failure message to client: {:?}", e);
                                                    });
                                                    continue;
                                                }
                                            } {
                                                user
                                            } else {
                                                match db::users::create_user(&state.db, username.clone(), username.clone()).await {
                                                    Ok(user) => user,
                                                    Err(e) => {
                                                        eprintln!("Error creating user in database: {:?}", e);
                                                        socket.send(Message::Text(ServerProtocol::AuthFailure { user_id: uuid::Uuid::nil(), reason: AuthError::UnknownError, timestamp: chrono::Utc::now() }.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                            eprintln!("Error sending auth failure message to client: {:?}", e);
                                                        });
                                                        continue;
                                                    }
                                                }
                                            };
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
                                                    socket.send(Message::Text(ServerProtocol::AuthSuccess { user_id: user.id, timestamp: chrono::Utc::now() }.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending auth success message to client: {:?}", e);
                                                    });
                                                }
                                                Err(e) => {
                                                    eprintln!("Error registering connection: {:?}", e);
                                                    socket.send(Message::Text(ServerProtocol::AuthFailure { user_id: user.id, reason: AuthError::AlreadyConnected, timestamp: chrono::Utc::now() }.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending auth failure message to client: {:?}", e);
                                                    });
                                                }
                                            }
                                        }
                                        ClientProtocol::CreateConversation { users, name } => {
                                            if user_id.is_none() {
                                                eprintln!("User is not authenticated. Cannot send message.");
                                                socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::NotAuthenticated, timestamp: chrono::Utc::now()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                    eprintln!("Error sending unauthenticated errror msg to client: {:?}", e);
                                                });
                                                continue;
                                            }
                                            if !users.iter().any(|&uid| uid == user_id.unwrap()) {
                                                eprintln!("Authenticated user must be part of the conversation.");
                                                socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::InvalidRequest, timestamp: chrono::Utc::now()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                    eprintln!("Error sending invalid request error to client: {:?}", e);
                                                });
                                                continue;
                                            }
                                            match db::conversations::create_conversation(&state.db, users, name).await {
                                                Ok(conv) => {
                                                    println!("Conversation created: {:?}", conv);
                                                    socket.send(Message::Text(ServerProtocol::ConversationCreated { conversation_id: conv.id, timestamp: chrono::Utc::now() }.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending conversation created message to client: {:?}", e);
                                                    });
                                                }
                                                Err(e) => {
                                                    eprintln!("Error creating conversation in database: {:?}", e);
                                                    socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::InternalError, timestamp: chrono::Utc::now()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending internal error to client: {:?}", e);
                                                    });
                                                }
                                            }
                                        }
                                        ClientProtocol::SendMessage { message, conversation_id } => {
                                            if user_id.is_none() {
                                                eprintln!("User is not authenticated. Cannot send message.");
                                                socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::NotAuthenticated, timestamp: chrono::Utc::now()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                    eprintln!("Error sending unauthenticated errror msg to client: {:?}", e);
                                                });
                                                continue;
                                            }
                                            let sender_id = user_id.unwrap();
                                            let users = match db::conversations::get_conversation_members(&state.db, conversation_id).await {
                                                Ok(users) => {
                                                    if users.is_empty() {
                                                        eprintln!("Conversation not found. Cannot send message.");
                                                        socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::ConversationNotFound, timestamp: chrono::Utc::now()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                            eprintln!("Error sending conversation not found error to client: {:?}", e);
                                                        });
                                                        continue;
                                                    }
                                                    if !users.iter().any(|user| user.id == sender_id) {
                                                        eprintln!("Sender is not a member of the conversation. Cannot send message.");
                                                        socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::Unauthorised, timestamp: chrono::Utc::now()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                            eprintln!("Error sending unauthorized error to client: {:?}", e);
                                                        });
                                                        continue;
                                                    }
                                                    users
                                                },
                                                Err(e) => {
                                                    eprintln!("Error querying conversation members from database: {:?}", e);
                                                    socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::InternalError, timestamp: chrono::Utc::now()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending internal error to client: {:?}", e);
                                                    });
                                                    continue;
                                                }
                                            };

                                            let msg = match db::messages::create_message(&state.db, sender_id, conversation_id, message).await {
                                                Ok(msg) => msg,
                                                Err(e) => {
                                                    eprintln!("Error saving message to database: {:?}", e);
                                                    socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::InternalError, timestamp: chrono::Utc::now()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending internal error to client: {:?}", e);
                                                    });
                                                    continue;
                                                }
                                            };

                                            for user in users {
                                                let recipient_id = user.id;

                                                let state_read = state.conns.read().await;
                                                let sender = state_read.get(recipient_id).map(|recipient_conn| recipient_conn.sender.clone());
                                                drop(state_read);
                                                if let Some(sender) = sender {
                                                    if let Err(e) = sender.send(ServerProtocol::NewMessage { message_id: msg.id, sender_id: msg.sender_id, conversation_id: msg.conversation_id, message: msg.content.clone(), timestamp: msg.created_at }).await
                                                    {
                                                        eprintln!("Error sending message to recipient: {:?}", e);
                                                        socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::InternalError, timestamp: chrono::Utc::now()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                            eprintln!("Error sending internal error to client: {:?}", e);
                                                        });
                                                    }
                                                } else {
                                                    eprintln!("Recipient is not connected. Cannot send message.");
                                                    // todo : send messages when recipient connects
                                                    socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::RecipientNotFound, timestamp: chrono::Utc::now()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
                                                        eprintln!("Error sending recipient not found error to client: {:?}", e);
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Error parsing client message: {:?}", e);
                                    socket.send(Message::Text(ServerProtocol::Error {code: ErrorCodes::InvalidRequest, timestamp: chrono::Utc::now()}.to_json().unwrap_or("".into()).into())).await.unwrap_or_else(|e| {
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
    db: PgPool,
}

#[tokio::main]
async fn main() {
    if let Err(e) = dotenvy::dotenv() {
        panic!("Failed to load .env file: {:?}", e);
    }

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file");
    let pool = db::get_pool(db_url)
        .await
        .expect("fatal: Failed to create database pool");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("fatal: Failed to run database migrations");

    let app: Router = Router::new()
        .route("/ws", get(handler))
        .with_state(AppState {
            conns: Arc::new(RwLock::new(ConnectionManager::new())),
            db: pool,
        });
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Listening on 127.0.0.1:3000");

    axum::serve(listener, app).await.unwrap();
}
