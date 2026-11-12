use std::{ops::Deref, sync::Arc};

use anyhow::{Error, Result};
use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use futures_util::{SinkExt, StreamExt};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Padding, Paragraph},
};
use ratatui_textarea::{Key, TextArea};
use relay_common::{ClientProtocol, Conversation, ServerProtocol, User};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

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
    },
}
struct App {
    running: bool,
    cur_conversation: Option<Conversation>,
    messages: Vec<Message>,
    input_mode: InputModes,
    tx: mpsc::Sender<ClientProtocol>,
    rx: mpsc::Receiver<ServerProtocol>,
    screen: Screens,
}
impl App {
    fn new(rx: mpsc::Receiver<ServerProtocol>, tx: mpsc::Sender<ClientProtocol>) -> Self {
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
            cur_conversation: None,
            input_mode: InputModes::Drafting,
            messages: Vec::new(),
            screen: Screens::Login {
                username,
                password,
                active: LoginFields::Username,
            },
        }
    }

    async fn run(&mut self) -> Result<()> {
        let mut term = ratatui::init();
        let mut events = EventStream::new();

        let mut tick = tokio::time::interval(std::time::Duration::from_millis(50));
        while self.running {
            tokio::select! {
                Some(msg) = self.rx.recv() => {
                    self.handle_server_message(msg);
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

    fn handle_server_message(&mut self, msg: ServerProtocol) {
        match msg {
            ServerProtocol::AuthSuccess { user, timestamp } => {
                let mut t = TextArea::default();
                t.set_block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("  Draft a Message  "),
                );
                self.screen = Screens::Main { draft: t, user }
            }
            ServerProtocol::AuthFailure {
                user_id,
                reason,
                timestamp,
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
            } => {} // Will add message to local db and show uread noti,
            ServerProtocol::Error { code, timestamp } => {} // Handle Error maybe show popover
            ServerProtocol::ConversationList {
                conversations,
                timestamp,
            } => {} // Will update conversations and show them
            ServerProtocol::MessageHistory {
                conversation_id,
                messages,
                timestamp,
            } => {} // Will be proccessed with loading
        }
    }

    async fn handle_event(&mut self, event: Event) -> Result<()> {
        if let Event::Key(key) = event
            && key.is_press()
            && ((key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
                || (key.code == KeyCode::Esc))
        {
            self.running = false;
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
            Screens::Main { draft, user: _ } => {
                if self.input_mode == InputModes::Drafting {
                    draft.input(event);
                }
            }
        }
        if let Some(new) = change_screen {
            self.screen = new
        };
        Ok(())
    }

    fn draw_conv_list(frame: &mut Frame, area: Rect) {
        frame.render_widget(
            Paragraph::new("Hello World").block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Conversations"),
            ),
            area,
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
            Screens::Main { draft, user } => {
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

                Self::draw_conv_list(frame, l_rows[0]);
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
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<ClientProtocol>(100);
    let (inbound_tx, inbound_rx) = mpsc::channel::<ServerProtocol>(100);

    let websocket_task: tokio::task::JoinHandle<Result<(), Error>> = tokio::spawn(async move {
        let (ws_stream, _) = connect_async("ws://127.0.0.1:3000/ws").await?;
        let (mut tx, mut rx) = ws_stream.split();

        loop {
            tokio::select!(
                Some(msg) = outbound_rx.recv() => {
                    tx.send(Message::Text(msg.to_json()?.into())).await?;
                },
                Some(msg) = rx.next() => {
                    match msg {
                        Ok(msg) => match msg {
                            Message::Close(_) => {
                                break;
                            }
                            Message::Text(t) => {
                                inbound_tx.send(ServerProtocol::from_json(t.as_str())?).await?;
                            },
                            _ => {},
                        },
                        Err(_) => {
                            break;
                        }
                    }
                }
            )
        }

        Ok(())
    });

    let mut app = App::new(inbound_rx, outbound_tx);
    let res = app.run().await;

    ratatui::restore();
    websocket_task.abort();

    res
}
