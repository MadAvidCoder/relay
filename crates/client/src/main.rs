mod local_db;

use anyhow::{Error, Result};
use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers, MouseEventKind};
use futures_util::{SinkExt, StreamExt};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Layout, Margin, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{
        Block, Borders, Padding, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState,
    },
};
use ratatui_textarea::TextArea;
use relay_common::{ClientProtocol, Conversation, ServerProtocol, User};
use relay_common::{Message, MessageCursor};
use sqlx::SqlitePool;
use std::{collections::HashMap, ops::Deref};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite};
use uuid::Uuid;

use crate::local_db::get_messages;

enum InboundMessage {
    Server(ServerProtocol),
    Connection(ConnectionStatus),
}

#[derive(Debug, PartialEq)]
enum InputModes {
    Drafting,
    Shortcutting,
    Searching,
}

#[derive(Debug, PartialEq)]
enum LoginFields {
    Username,
    Password,
}
enum Screens {
    Login {
        username: TextArea<'static>,
        password: TextArea<'static>,
        active: LoginFields,
    },
    Loading,
    Main {
        draft: TextArea<'static>,
        user: User,
        selected_conv: Option<Uuid>,
        scroll: ScrollbarState,
    },
}
enum ConnectionStatus {
    Disconnected,
    Connecting,
    Connected,
}
struct App {
    running: bool,
    conversations: Vec<Conversation>,
    connection_status: ConnectionStatus,
    messages: HashMap<Uuid, Vec<Message>>,
    db: SqlitePool,
    input_mode: InputModes,
    tx: mpsc::Sender<ClientProtocol>,
    rx: mpsc::Receiver<InboundMessage>,
    screen: Screens,
}
impl App {
    fn new(
        rx: mpsc::Receiver<InboundMessage>,
        tx: mpsc::Sender<ClientProtocol>,
        db: SqlitePool,
    ) -> Self {
        let mut username = TextArea::default();
        username.set_cursor_line_style(Style::default());
        username.set_block(Block::default().borders(Borders::ALL).title("  Username  "));
        let mut password = TextArea::default();
        password.set_cursor_line_style(Style::default());
        password.set_block(Block::default().borders(Borders::ALL).title("  Password  "));
        Self {
            running: true,
            rx,
            tx,
            db,
            input_mode: InputModes::Drafting,
            messages: HashMap::new(),
            conversations: Vec::new(),
            connection_status: ConnectionStatus::Disconnected,
            screen: Screens::Login {
                username,
                password,
                active: LoginFields::Username,
            },
        }
    }

    async fn run(&mut self) -> Result<()> {
        let mut term = ratatui::init();
        crossterm::execute!(std::io::stdout(), crossterm::event::EnableMouseCapture)?;
        let mut events = EventStream::new();

        let mut tick = tokio::time::interval(std::time::Duration::from_millis(50));
        while self.running {
            tokio::select! {
                Some(msg) = self.rx.recv() => {
                    match msg {
                        InboundMessage::Server(m) => {
                            match self.handle_server_message(m).await {
                                Ok(_) => {},
                                Err(_) => println!("Error"),
                            }
                        },
                        InboundMessage::Connection(s) => self.connection_status = s,
                    }
                }
                Some(Ok(event)) = events.next() => {
                    self.handle_event(event).await?;
                }
                _ = tick.tick() => {
                    term.draw(|frame| self.draw(frame))?;
                }
            }
        }

        Ok(())
    }

    async fn handle_server_message(&mut self, msg: ServerProtocol) -> Result<()> {
        match msg {
            ServerProtocol::AuthSuccess { user, timestamp: _ } => {
                let mut t = TextArea::default();
                t.set_block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("  Draft a Message  "),
                );
                self.screen = Screens::Main {
                    draft: t,
                    user,
                    selected_conv: None,
                    scroll: ScrollbarState::new(0),
                };
                self.tx.send(ClientProtocol::GetConversations).await?;
            }
            ServerProtocol::AuthFailure {
                user_id: _,
                reason,
                timestamp: _,
            } => {
                //TODO: show reason on screen
                let mut username = TextArea::default();
                username.set_cursor_line_style(Style::default());
                username.set_block(Block::default().borders(Borders::ALL).title("  Username  "));
                let mut password = TextArea::default();
                password.set_cursor_line_style(Style::default());
                password.set_block(Block::default().borders(Borders::ALL).title("  Password  "));

                self.screen = Screens::Login {
                    username,
                    password,
                    active: LoginFields::Username,
                }
            }
            ServerProtocol::ConversationCreated {
                conversation_id,
                timestamp,
            } => {} //TBA
            ServerProtocol::NewMessage {
                message_id,
                sender_id,
                conversation_id,
                message,
                timestamp,
            } => {
                let msg = relay_common::Message {
                    id: message_id,
                    sender_id,
                    conversation_id,
                    content: message,
                    created_at: timestamp,
                    edited_at: None,
                    deleted_at: None,
                };
                local_db::save_message(&msg, &self.db).await?;
                let history = self.messages.entry(conversation_id).or_default();
                if !history.iter().any(|e| e.id == msg.id) {
                    history.push(msg.clone());
                }
                history.sort_by_key(|msg| (msg.created_at, msg.id));

                if let Some(c) = self
                    .conversations
                    .iter_mut()
                    .find(|c| c.id == conversation_id)
                {
                    c.latest_message = Some(msg)
                }
                //TODO: noti
            }
            ServerProtocol::Error { code, timestamp } => {} // Handle Error maybe show popover
            ServerProtocol::ConversationList {
                conversations,
                timestamp: _,
            } => {
                local_db::save_conversations(&conversations, &self.db).await?;
                if let Screens::Main { scroll, .. } = &mut self.screen {
                    *scroll = scroll.content_length(conversations.len());
                }
                self.conversations = conversations;
                for conv in &self.conversations {
                    let history = self.messages.entry(conv.id).or_default();
                    *history = get_messages(&conv.id, &self.db).await?;

                    self.tx
                        .send(ClientProtocol::GetMessages {
                            conversation_id: conv.id,
                            limit: 100,
                            after: history
                                .iter()
                                .max_by_key(|m| (m.created_at, m.id))
                                .map(|l| MessageCursor {
                                    created_at: l.created_at,
                                    id: l.id,
                                }),
                        })
                        .await?;
                }
            }
            ServerProtocol::MessageHistory {
                conversation_id,
                messages,
                timestamp: _,
            } => {
                local_db::save_messages(&messages, &self.db).await?;
                let history = self.messages.entry(conversation_id).or_default();
                for msg in messages {
                    if !history.iter().any(|e| e.id == msg.id) {
                        history.push(msg);
                    }
                }
                history.sort_by_key(|msg| (msg.created_at, msg.id));
                if let Some(c) = self
                    .conversations
                    .iter_mut()
                    .find(|c| c.id == conversation_id)
                    && let Some(latest_msg) = history.last()
                {
                    c.latest_message = Some(latest_msg.clone());
                }
            }
        };
        Ok(())
    }

    async fn handle_event(&mut self, event: Event) -> Result<()> {
        if let Event::Key(key) = event
            && key.is_press()
            && ((key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
                || (key.code == KeyCode::Esc))
        {
            self.running = false;
            return Ok(());
        }
        let mut change_screen = None;
        match &mut self.screen {
            Screens::Login {
                username,
                password,
                active,
            } => {
                let mut used: bool = false;
                if let Event::Key(key) = event
                    && key.is_press()
                {
                    match key.code {
                        KeyCode::Enter => {
                            used = true;
                            self.tx
                                .send(ClientProtocol::Auth {
                                    username: username.lines().join(""),
                                })
                                .await?;
                            change_screen = Some(Screens::Loading);
                        }
                        KeyCode::Tab | KeyCode::BackTab => {
                            *active = match active {
                                LoginFields::Password => LoginFields::Username,
                                LoginFields::Username => LoginFields::Password,
                            };
                            used = true;
                        }
                        _ => {}
                    }
                }
                if !used {
                    match active {
                        LoginFields::Username => {
                            username.input(event);
                        }
                        LoginFields::Password => {
                            password.input(event);
                        }
                    }
                }
            }
            Screens::Loading => {}
            Screens::Main {
                draft,
                user: _,
                selected_conv: _,
                scroll,
            } => {
                if let Event::Mouse(e) = event {
                    match e.kind {
                        MouseEventKind::ScrollUp => {
                            scroll.prev();
                            scroll.prev();
                            scroll.prev();
                        }
                        MouseEventKind::ScrollDown => {
                            scroll.next();
                            scroll.next();
                            scroll.next();
                        }
                        _ => {}
                    }
                } else if self.input_mode == InputModes::Drafting {
                    draft.input(event);
                }
            }
        }
        if let Some(new) = change_screen {
            self.screen = new
        };
        Ok(())
    }

    fn draw_conv_list(
        frame: &mut Frame,
        area: Rect,
        conversations: &[Conversation],
        _sel: &Option<Uuid>,
        bar: &mut ScrollbarState,
    ) {
        let [scrollbar_area, _, content_area] = Layout::horizontal([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .areas(area);
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalLeft),
            scrollbar_area,
            bar,
        );
        frame.render_widget(
            Paragraph::new(
                conversations
                    .iter()
                    .map(|conv| Line::from(conv.name.as_deref().unwrap_or("")))
                    .collect::<Vec<Line>>(),
            )
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Conversations"),
            )
            .scroll((bar.get_position() as u16, 0)),
            content_area,
        );
    }

    fn draw_messages(frame: &mut Frame, area: Rect) {
        frame.render_widget(
            Paragraph::new("Hello World").block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Main Message Area"),
            ),
            area,
        );
    }

    fn draw_input(frame: &mut Frame, area: Rect, draft: &TextArea) {
        frame.render_widget(draft, area);
    }

    fn draw_name(frame: &mut Frame, area: Rect, username: &str) {
        frame.render_widget(Paragraph::new(username).centered(), area);
    }

    fn draw(&mut self, frame: &mut Frame) {
        match &mut self.screen {
            Screens::Main {
                draft,
                user,
                selected_conv,
                scroll,
            } => {
                let cols = Layout::horizontal(vec![
                    Constraint::Percentage(15),
                    Constraint::Percentage(80),
                ])
                .split(frame.area());
                let l_rows = Layout::vertical(vec![
                    Constraint::Percentage(100),
                    Constraint::Min(1),
                    Constraint::Length(1),
                ])
                .split(cols[0]);
                let rows = Layout::vertical(vec![
                    Constraint::Min(5),
                    Constraint::Length((draft.lines().len() as u16 + 2).clamp(3, 8)),
                ])
                .split(cols[1]);

                Self::draw_conv_list(
                    frame,
                    l_rows[0],
                    self.conversations.as_ref(),
                    selected_conv,
                    scroll,
                );
                Self::draw_name(frame, l_rows[1], &user.username);
                Self::draw_messages(frame, rows[0]);
                Self::draw_input(frame, rows[1], draft.deref());
            }
            Screens::Loading => {
                frame.render_widget(
                    Paragraph::new("Waiting for Auth...")
                        .block(
                            Block::new()
                                .padding(Padding::new(6, 6, 2, 2))
                                .borders(Borders::ALL),
                        )
                        .alignment(Alignment::Center),
                    frame
                        .area()
                        .centered(Constraint::Percentage(50), Constraint::Percentage(50)),
                );
            }
            Screens::Login {
                username,
                password,
                active,
            } => {
                let vert = Layout::vertical(vec![
                    Constraint::Percentage(30),
                    Constraint::Length(1),
                    Constraint::Length(1),
                    Constraint::Length(3),
                    Constraint::Length(3),
                    Constraint::Length(1),
                    Constraint::Percentage(30),
                ])
                .split(frame.area());
                let centre = Layout::horizontal(vec![
                    Constraint::Percentage(25),
                    Constraint::Percentage(50),
                    Constraint::Percentage(25),
                ])
                .split(vert[3]);
                let centre_password = Layout::horizontal(vec![
                    Constraint::Percentage(25),
                    Constraint::Percentage(50),
                    Constraint::Percentage(25),
                ])
                .split(vert[4]);

                frame.render_widget(
                    Paragraph::new("LOGIN")
                        .alignment(Alignment::Center)
                        .style(Style::default().bold()),
                    vert[1],
                );
                username.set_cursor_style(if *active == LoginFields::Username {
                    Style::default().reversed().rapid_blink()
                } else {
                    Style::default()
                });
                username.set_block(
                    Block::default()
                        .title("  Username  ")
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(if *active == LoginFields::Username {
                            Color::Yellow
                        } else {
                            Color::DarkGray
                        })),
                );
                password.set_cursor_style(if *active == LoginFields::Password {
                    Style::default().reversed().rapid_blink()
                } else {
                    Style::default()
                });
                password.set_mask_char('\u{2022}');
                password.set_block(
                    Block::default()
                        .title("  Password  ")
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(if *active == LoginFields::Password {
                            Color::Yellow
                        } else {
                            Color::DarkGray
                        })),
                );
                frame.render_widget(username.deref(), centre[1]);
                frame.render_widget(password.deref(), centre_password[1]);
                frame.render_widget(
                    Paragraph::new(Line::from(vec![
                        Span::styled("TAB", Style::default().bold()),
                        Span::from(": Switch field  |  "),
                        Span::styled("ENTER", Style::default().bold()),
                        Span::from(": Login  |  "),
                        Span::styled("ESC", Style::default().bold()),
                        Span::from(": Quit"),
                    ]))
                    .centered()
                    .style(Style::default().fg(Color::DarkGray))
                    .italic(),
                    vert[5],
                );
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let db = local_db::init().await?;
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<ClientProtocol>(100);
    let (inbound_tx, inbound_rx) = mpsc::channel::<InboundMessage>(100);

    let mut app = App::new(inbound_rx, outbound_tx, db);

    let websocket_task: tokio::task::JoinHandle<Result<(), Error>> = tokio::spawn(async move {
        inbound_tx
            .send(InboundMessage::Connection(ConnectionStatus::Connecting))
            .await?;
        let (ws_stream, _) = connect_async("ws://127.0.0.1:3000/ws").await?;
        let (mut tx, mut rx) = ws_stream.split();
        inbound_tx
            .send(InboundMessage::Connection(ConnectionStatus::Connected))
            .await?;

        loop {
            tokio::select!(
                Some(msg) = outbound_rx.recv() => {
                    tx.send(tungstenite::Message::Text(msg.to_json()?.into())).await?;
                },
                msg = rx.next() => {
                    match msg {
                        Some(Ok(msg)) => match msg {
                            tungstenite::Message::Close(_) => {
                                break;
                            }
                            tungstenite::Message::Text(t) => {
                                inbound_tx.send(InboundMessage::Server(ServerProtocol::from_json(t.as_str())?)).await?;
                            },
                            _ => {},
                        },
                        Some(Err(_)) => {
                            break;
                        },
                        None => {
                            break;
                        }
                    }
                },
                else => {
                    break;
                }
            )
        }
        inbound_tx
            .send(InboundMessage::Connection(ConnectionStatus::Disconnected))
            .await?;

        Ok(())
    });

    let res = app.run().await;

    crossterm::execute!(std::io::stdout(), crossterm::event::DisableMouseCapture)?;
    ratatui::restore();
    websocket_task.abort();

    res
}
