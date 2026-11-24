mod local_db;

use anyhow::{Error, Result};
use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers, MouseEventKind};
use futures_util::{SinkExt, StreamExt};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{
        Block, Borders, Clear, Padding, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState,
    },
};
use ratatui_textarea::TextArea;
use relay_common::{ClientProtocol, Conversation, ConversationType, ServerProtocol, User};
use relay_common::{Message, MessageCursor};
use sqlx::SqlitePool;
use std::{
    collections::HashMap,
    io::{self, Write},
};
use strum::Display;
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

#[derive(Debug, PartialEq, Copy, Clone)]
enum FocusPoints {
    Conversations,
    Messages,
    Draft,
}

impl FocusPoints {
    fn next(self) -> Self {
        match self {
            Self::Conversations => Self::Messages,
            Self::Messages => Self::Draft,
            Self::Draft => Self::Conversations,
        }
    }
    fn prev(self) -> Self {
        match self {
            Self::Conversations => Self::Draft,
            Self::Messages => Self::Conversations,
            Self::Draft => Self::Messages,
        }
    }
}

enum CommandMode {
    Search,
    Command,
}

enum Screens {
    Login {
        username: Box<TextArea<'static>>,
        password: Box<TextArea<'static>>,
        active: LoginFields,
    },
    Loading,
    Main {
        draft: Box<TextArea<'static>>,
        user: User,
        focus: FocusPoints,
        selected_conv: Option<Uuid>,
        active_conv: Option<Uuid>,
        scroll: ScrollbarState,
        conv_height: usize,
    },
}

#[derive(Debug, Clone, Copy, Display, PartialEq, Eq)]
enum Command {
    #[strum(to_string = "New DM")]
    NewDM,
    #[strum(to_string = "New Group")]
    NewGroup,
}

impl Command {
    fn all() -> Vec<Self> {
        vec![Self::NewDM, Self::NewGroup]
    }

    fn execute(&self, app: &mut App) -> Result<()> {
        match self {
            Command::NewDM => {
                app.popup = Some(Popup::Info(InfoPopup {
                    title: "New DM".to_string(),
                    message: "TBA".to_string(),
                    colour: Color::Green,
                }));
                Ok(())
            }
            Command::NewGroup => {
                let mut name = Box::new(TextArea::default());
                name.set_cursor_line_style(Style::default());
                name.set_block(
                    Block::default()
                        .borders(Borders::BOTTOM)
                        .title_bottom(Line::from(vec![
                            "⎯⎯ Group Name".into(),
                            " (optional) ".italic().dim(),
                        ]))
                        .border_style(Style::new().yellow()),
                );
                name.set_cursor_style(Style::default().slow_blink().reversed());
                let mut search = Box::new(TextArea::default());
                search.set_cursor_line_style(Style::default());
                search.set_block(
                    Block::default()
                        .borders(Borders::BOTTOM)
                        .title_bottom(Line::from(vec![
                            "── Search Users ".into(),
                            "(Enter to add) ".italic().dim(),
                        ]))
                        .title_bottom(
                            Line::from(vec![Span::from(" 0 selected "), Span::from("─")])
                                .right_aligned(),
                        ),
                );
                search.set_cursor_style(Style::default());
                let all: Vec<Uuid> = app.contacts.iter().map(|a| a.id).collect();
                app.popup = Some(Popup::NewGroup(NewGroupPopup {
                    name,
                    selection: all.first().copied(),
                    search,
                    results: all,
                    added: Vec::new(),
                    focus: NewGroupFocus::Name,
                    scroll: ScrollbarState::new(0).viewport_content_length(10),
                }));
                Ok(())
            }
        }
    }

    fn ranked(query: &str, thresh: f64) -> Vec<Self> {
        let query = query.to_lowercase();
        if query.trim().is_empty() {
            return Self::all();
        }
        let mut all: Vec<_> = Self::all()
            .into_iter()
            .map(|f| {
                let score = f.matches(query.trim());
                (f, score)
            })
            .filter(|(_, score)| *score >= thresh)
            .collect();
        all.sort_by(|a, b| b.1.total_cmp(&a.1));
        all.into_iter().map(|(com, _)| com).collect()
    }

    fn matches(&self, query: &str) -> f64 {
        let score = strsim::jaro_winkler(&self.to_string().to_lowercase(), query);
        if score.is_finite() { score } else { 0.0f64 }
    }
}

enum ConnectionStatus {
    Disconnected,
    Connecting,
    Connected,
}

struct InfoPopup {
    title: String,
    message: String,
    colour: Color,
}

struct PalettePopup {
    text: Box<TextArea<'static>>,
    selection: Option<Command>,
    results: Vec<Command>,
    scroll: ScrollbarState,
    mode: CommandMode,
}

#[derive(Debug, PartialEq)]
enum NewGroupFocus {
    Name,
    Search,
}

struct NewGroupPopup {
    name: Box<TextArea<'static>>,
    search: Box<TextArea<'static>>,
    results: Vec<Uuid>,
    added: Vec<Uuid>,
    focus: NewGroupFocus,
    selection: Option<Uuid>,
    scroll: ScrollbarState,
}
enum Popup {
    Info(InfoPopup),
    CommandPalette(PalettePopup),
    NewGroup(NewGroupPopup),
}

const POPUP_SIZE: (Constraint, Constraint) = (Constraint::Percentage(50), Constraint::Length(12));

struct App {
    running: bool,
    conversations: Vec<Conversation>,
    connection_status: ConnectionStatus,
    messages: HashMap<Uuid, Vec<Message>>,
    contacts: Vec<User>,
    db: SqlitePool,
    input_mode: InputModes,
    tx: mpsc::Sender<ClientProtocol>,
    rx: mpsc::Receiver<InboundMessage>,
    screen: Screens,
    popup: Option<Popup>,
}
impl App {
    async fn new(
        rx: mpsc::Receiver<InboundMessage>,
        tx: mpsc::Sender<ClientProtocol>,
        db: SqlitePool,
    ) -> Result<Self> {
        let mut username = Box::new(TextArea::default());
        username.set_cursor_line_style(Style::default());
        username.set_block(Block::default().borders(Borders::ALL).title("  Username  "));
        let mut password = Box::new(TextArea::default());
        password.set_cursor_line_style(Style::default());
        password.set_block(Block::default().borders(Borders::ALL).title("  Password  "));
        Ok(Self {
            running: true,
            popup: None,
            rx,
            tx,
            input_mode: InputModes::Drafting,
            messages: HashMap::new(),
            conversations: Vec::new(),
            connection_status: ConnectionStatus::Disconnected,
            screen: Screens::Login {
                username,
                password,
                active: LoginFields::Username,
            },
            contacts: local_db::get_contacts(&db).await?,
            db,
        })
    }

    async fn run(&mut self) -> Result<()> {
        let mut term = ratatui::init();
        crossterm::execute!(std::io::stdout(), crossterm::event::EnableMouseCapture)?;
        let mut events = EventStream::new();

        let mut tick = tokio::time::interval(std::time::Duration::from_millis(50));
        while self.running {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(InboundMessage::Server(m)) => {
                            match self.handle_server_message(m).await {
                                Ok(_) => {},
                                Err(e) => self.popup = Some(Popup::Info( InfoPopup{
                                    title: "Server Error".to_string(),
                                    message: e.to_string(),
                                    colour: Color::LightRed
                                })),
                            }
                        },
                        Some(InboundMessage::Connection(s)) => self.connection_status = s,
                        None => {
                            self.running = false;
                        },
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
                let mut t = Box::new(TextArea::default());
                t.set_cursor_line_style(Style::default());
                t.set_cursor_style(Style::default());
                t.set_block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("  Draft a Message  "),
                );
                self.screen = Screens::Main {
                    draft: t,
                    user,
                    selected_conv: self.conversations.first().map(|a| a.id),
                    active_conv: self.conversations.first().map(|a| a.id),
                    scroll: ScrollbarState::new(0),
                    focus: FocusPoints::Conversations,
                    conv_height: 16,
                };
                self.tx.send(ClientProtocol::GetConversations).await?;
            }
            ServerProtocol::AuthFailure {
                user_id: _,
                reason,
                timestamp: _,
            } => {
                self.popup = Some(Popup::Info(InfoPopup {
                    title: "Auth Failed".to_string(),
                    message: reason.to_string(),
                    colour: Color::Yellow,
                }));

                let mut username = Box::new(TextArea::default());
                username.set_cursor_line_style(Style::default());
                username.set_block(Block::default().borders(Borders::ALL).title("  Username  "));
                let mut password = Box::new(TextArea::default());
                password.set_cursor_line_style(Style::default());
                password.set_block(Block::default().borders(Borders::ALL).title("  Password  "));

                self.screen = Screens::Login {
                    username,
                    password,
                    active: LoginFields::Username,
                }
            }
            ServerProtocol::ConversationCreated {
                conversation,
                timestamp: _,
            } => {
                local_db::save_conversation(&conversation, &self.db).await?;
                self.messages.insert(
                    conversation.id,
                    if let Some(msg) = &conversation.latest_message {
                        vec![msg.clone()]
                    } else {
                        Vec::new()
                    },
                );
                if let Screens::Main {
                    selected_conv,
                    active_conv,
                    ..
                } = &mut self.screen
                {
                    *selected_conv = Some(conversation.id);
                    *active_conv = *selected_conv
                }
                self.conversations.push(conversation);
            }
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
                    c.latest_message = history.last().cloned();
                }
                //TODO: noti
            }
            ServerProtocol::Error { code, timestamp: _ } => {
                self.popup = Some(Popup::Info(InfoPopup {
                    title: "Server Error".to_string(),
                    message: code.to_string(),
                    colour: Color::LightRed,
                }))
            }
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
                if let Screens::Main {
                    selected_conv,
                    active_conv,
                    ..
                } = &mut self.screen
                    && active_conv.is_none()
                {
                    *selected_conv = self.conversations.first().map(|c| c.id);
                    *active_conv = *selected_conv;
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

    fn search_users(users: &[User], query: &str, thresh: f64) -> Vec<Uuid> {
        let query = query.to_lowercase();
        let query = query.trim();

        if query.is_empty() {
            return users.iter().map(|u| u.id).collect();
        }

        let mut scored: Vec<_> = users
            .iter()
            .map(|u| {
                let display_score =
                    strsim::jaro_winkler(&u.display_name.to_string().to_lowercase(), query);
                let name_score =
                    strsim::jaro_winkler(&u.username.to_string().to_lowercase(), query);
                (u, display_score.max(name_score))
            })
            .filter(|(_, score)| *score >= thresh)
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        scored.into_iter().map(|(com, _)| com.id).collect()
    }

    async fn handle_event(&mut self, event: Event) -> Result<()> {
        if let Event::Key(key) = event
            && key.is_press()
            && (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
        {
            self.running = false;
            return Ok(());
        }
        if let Some(Popup::Info(_)) = self.popup
            && let Event::Key(k) = event
            && k.is_press()
            && ([KeyCode::Char(' '), KeyCode::Enter, KeyCode::Esc].contains(&k.code))
        {
            self.popup = None;
            return Ok(());
        }
        let mut change_screen = None;
        match &mut self.screen {
            Screens::Login {
                username,
                password,
                active,
            } => {
                if let Event::Key(key) = event
                    && key.is_press()
                    && match key.code {
                        KeyCode::Enter => {
                            self.tx
                                .send(ClientProtocol::Auth {
                                    username: username.lines().join(""),
                                })
                                .await?;
                            change_screen = Some(Screens::Loading);
                            true
                        }
                        KeyCode::Tab | KeyCode::BackTab => {
                            *active = match active {
                                LoginFields::Password => LoginFields::Username,
                                LoginFields::Username => LoginFields::Password,
                            };
                            true
                        }
                        _ => false,
                    }
                {
                } else {
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
                user,
                selected_conv,
                active_conv,
                focus,
                scroll,
                conv_height,
            } => {
                if let Event::Key(k) = event
                    && k.is_press()
                    && k.code == KeyCode::Char('k')
                    && k.modifiers.contains(KeyModifiers::CONTROL)
                {
                    if let Some(Popup::CommandPalette(..)) = self.popup {
                        self.popup = None;
                    } else {
                        let mut t = Box::new(TextArea::default());
                        t.set_cursor_line_style(Style::default());
                        t.set_cursor_style(Style::default().reversed().slow_blink());
                        t.set_block(Block::default().borders(Borders::BOTTOM));
                        let all = Command::all();
                        self.popup = Some(Popup::CommandPalette(PalettePopup {
                            text: t,
                            scroll: ScrollbarState::new(all.len()).viewport_content_length(10),
                            selection: all.first().copied(),
                            results: all,
                            mode: CommandMode::Command,
                        }));
                    }
                } else if let Some(Popup::CommandPalette(p)) = &mut self.popup {
                    if let Event::Key(k) = event
                        && k.is_press()
                        && k.code == KeyCode::Esc
                    {
                        self.popup = None;
                    } else {
                        if let Event::Key(k) = event
                            && k.is_press()
                        {
                            match k.code {
                                KeyCode::Up => {
                                    if let Some(sel) = p.selection {
                                        p.selection = p
                                            .results
                                            .get(
                                                p.results
                                                    .iter()
                                                    .position(|a| *a == sel)
                                                    .unwrap_or(0)
                                                    .saturating_sub(1),
                                            )
                                            .copied();
                                    } else {
                                        p.selection = p.results.first().copied();
                                    }
                                }
                                KeyCode::Down => {
                                    if let Some(sel) = p.selection {
                                        p.selection = p
                                            .results
                                            .get(
                                                p.results
                                                    .iter()
                                                    .position(|a| *a == sel)
                                                    .unwrap_or(0)
                                                    .saturating_add(1)
                                                    .min(p.results.len().saturating_sub(1)),
                                            )
                                            .copied();
                                    } else {
                                        p.selection = p.results.first().copied();
                                    }
                                }
                                KeyCode::Enter => {
                                    if let Some(sel) = p.selection {
                                        sel.execute(self).unwrap_or_else(|err| {
                                            self.popup = Some(Popup::Info(InfoPopup {
                                                title: "Error".to_string(),
                                                message: err.to_string(),
                                                colour: Color::Red,
                                            }))
                                        });
                                    }
                                }
                                KeyCode::Tab => {} // nothing
                                _ => {
                                    p.text.input(event);
                                    p.results = Command::ranked(&p.text.lines()[0], 0.65);
                                    if !p.selection.is_some_and(|sel| p.results.contains(&sel)) {
                                        p.selection = p.results.first().copied();
                                    }
                                }
                            };
                        }
                    }
                } else if let Some(Popup::NewGroup(g)) = &mut self.popup {
                    if let Event::Key(k) = event
                        && k.is_press()
                        && k.code == KeyCode::Esc
                    {
                        self.popup = None;
                    } else {
                        if let Event::Key(k) = event
                            && k.is_press()
                        {
                            match k.code {
                                KeyCode::Up => {
                                    if let Some(sel) = g.selection {
                                        g.selection = g
                                            .results
                                            .get(
                                                g.results
                                                    .iter()
                                                    .position(|a| *a == sel)
                                                    .unwrap_or(0)
                                                    .saturating_sub(1),
                                            )
                                            .copied();
                                    } else {
                                        g.selection = g.results.first().copied();
                                    }
                                }
                                KeyCode::Down => {
                                    if g.focus == NewGroupFocus::Search {
                                        if let Some(sel) = g.selection {
                                            g.selection = g
                                                .results
                                                .get(
                                                    g.results
                                                        .iter()
                                                        .position(|a| *a == sel)
                                                        .unwrap_or(0)
                                                        .saturating_add(1)
                                                        .min(g.results.len().saturating_sub(1)),
                                                )
                                                .copied();
                                        } else {
                                            g.selection = g.results.first().copied();
                                        }
                                    }
                                }
                                KeyCode::Enter => {
                                    if k.modifiers.contains(KeyModifiers::CONTROL) {
                                        if !g.added.is_empty() {
                                            if !g.added.contains(&user.id) {
                                                g.added.push(user.id);
                                            }
                                            let name = g.name.lines().join("").trim().to_string();
                                            self.tx
                                                .send(ClientProtocol::CreateConversation {
                                                    users: g.added.clone(),
                                                    name: if name.is_empty() {
                                                        None
                                                    } else {
                                                        Some(name)
                                                    },
                                                    mode: ConversationType::Group,
                                                })
                                                .await?;
                                            self.popup = None;
                                        }
                                    } else if g.focus == NewGroupFocus::Search
                                        && let Some(sel) = g.selection
                                    {
                                        if let Some(ind) = g.added.iter().position(|x| *x == sel) {
                                            g.added.remove(ind);
                                        } else {
                                            g.added.push(sel);
                                        }
                                    }
                                }
                                KeyCode::Tab | KeyCode::BackTab => {
                                    g.focus = match g.focus {
                                        NewGroupFocus::Name => {
                                            g.search.set_cursor_style(
                                                Style::default().slow_blink().reversed(),
                                            );
                                            g.name.set_cursor_style(Style::default());
                                            NewGroupFocus::Search
                                        }
                                        NewGroupFocus::Search => {
                                            g.search.set_cursor_style(Style::default());
                                            g.name.set_cursor_style(
                                                Style::default().slow_blink().reversed(),
                                            );
                                            NewGroupFocus::Name
                                        }
                                    }
                                }
                                _ => match g.focus {
                                    NewGroupFocus::Name => {
                                        g.name.input(event);
                                    }
                                    NewGroupFocus::Search => {
                                        g.search.input(event);
                                        g.results = Self::search_users(
                                            &self.contacts,
                                            &g.search.lines().join(""),
                                            0.65,
                                        );
                                    }
                                },
                            };
                        }
                    }
                } else if let Event::Key(k) = event
                    && k.is_press()
                    && k.code == KeyCode::Enter
                {
                    match *focus {
                        FocusPoints::Conversations => {
                            *active_conv = *selected_conv;
                        }
                        FocusPoints::Draft => {
                            if k.modifiers.contains(KeyModifiers::CONTROL)
                                && let Some(conversation_id) = *active_conv
                                && let message = draft.lines().join("\n")
                                && !message.is_empty()
                            {
                                self.tx
                                    .send(ClientProtocol::SendMessage {
                                        message,
                                        conversation_id,
                                    })
                                    .await?;
                                draft.select_all();
                                draft.cut();
                            } else {
                                draft.input(event);
                            }
                        }
                        FocusPoints::Messages => {
                            // prob nothing for now
                        }
                    }
                } else if let Event::Key(k) = event
                    && k.is_press()
                    && k.code == KeyCode::Tab
                {
                    *focus = focus.next();
                    if *focus == FocusPoints::Draft {
                        draft.set_cursor_style(Style::default().reversed().slow_blink());
                    } else {
                        draft.set_cursor_style(Style::default());
                    }
                } else if let Event::Key(k) = event
                    && k.is_press()
                    && k.code == KeyCode::BackTab
                {
                    *focus = focus.prev();
                    if *focus == FocusPoints::Draft {
                        draft.set_cursor_style(Style::default().reversed().slow_blink());
                    } else {
                        draft.set_cursor_style(Style::default());
                    }
                } else if let Event::Mouse(e) = event {
                    match focus {
                        FocusPoints::Conversations => match e.kind {
                            MouseEventKind::ScrollUp => {
                                scroll.prev();
                                scroll.prev();
                            }
                            MouseEventKind::ScrollDown => {
                                scroll.next();
                                scroll.next();
                            }
                            _ => {}
                        },
                        FocusPoints::Messages => {}
                        _ => {}
                    }
                } else if let Event::Key(k) = event
                    && k.is_press()
                    && *focus == FocusPoints::Conversations
                    && k.code == KeyCode::Down
                {
                    if let Some(sel) = selected_conv
                        && !self.conversations.is_empty()
                    {
                        *selected_conv = Some(
                            self.conversations
                                .get(
                                    self.conversations
                                        .iter()
                                        .position(|x| x.id == *sel)
                                        .unwrap_or(0)
                                        .saturating_add(1)
                                        .min(self.conversations.len().saturating_sub(1)),
                                )
                                .unwrap_or(self.conversations.last().unwrap())
                                .id,
                        )
                    } else {
                        *selected_conv = self.conversations.first().map(|c| c.id);
                    }

                    if let Some(sel) = selected_conv
                        && let Some(pos) = self.conversations.iter().position(|a| a.id == *sel)
                    {
                        if pos < scroll.get_position() {
                            *scroll = scroll.position(pos);
                        } else if pos >= scroll.get_position() + *conv_height {
                            *scroll = scroll.position(pos - *conv_height + 1);
                        }
                    };
                } else if let Event::Key(k) = event
                    && k.is_press()
                    && *focus == FocusPoints::Conversations
                    && k.code == KeyCode::Up
                {
                    if let Some(sel) = selected_conv
                        && !self.conversations.is_empty()
                    {
                        *selected_conv = Some(
                            self.conversations
                                .get(
                                    self.conversations
                                        .iter()
                                        .position(|x| x.id == *sel)
                                        .unwrap_or(0)
                                        .saturating_sub(1),
                                )
                                .unwrap_or(self.conversations.first().unwrap())
                                .id,
                        )
                    } else {
                        *selected_conv = self.conversations.first().map(|c| c.id);
                    }

                    if let Some(sel) = selected_conv
                        && let Some(pos) = self.conversations.iter().position(|a| a.id == *sel)
                    {
                        if pos < scroll.get_position() {
                            *scroll = scroll.position(pos);
                        } else if pos >= scroll.get_position() + *conv_height {
                            *scroll = scroll.position(pos - *conv_height + 1);
                        }
                    };
                } else if self.input_mode == InputModes::Drafting && *focus == FocusPoints::Draft {
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
        focused: bool,
        conversations: &[Conversation],
        sel: &Option<Uuid>,
        active: &Option<Uuid>,
        bar: &mut ScrollbarState,
        conv_height: &mut usize,
    ) {
        let [scrollbar_area, _, content_area] = Layout::horizontal([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .areas(area);

        let content_height = content_area.height.saturating_sub(2) as usize;
        *conv_height = content_height;

        *bar = bar.position(
            bar.get_position()
                .min(conversations.len().saturating_sub(content_height)),
        );

        *bar = bar
            .viewport_content_length(content_height)
            .content_length(conversations.len().saturating_sub(content_height).max(1));
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalLeft),
            scrollbar_area,
            bar,
        );
        frame.render_widget(
            Paragraph::new(
                conversations
                    .iter()
                    .map(|conv| {
                        Line::from(conv.name.as_deref().unwrap_or("")).style(
                            Style::default().bold().fg(
                                if let Some(sel) = sel
                                    && let Some(active) = active
                                {
                                    if &conv.id == sel && &conv.id == active {
                                        Color::Green
                                    } else if &conv.id == active {
                                        Color::LightGreen
                                    } else if &conv.id == sel {
                                        Color::Cyan
                                    } else {
                                        Color::Reset
                                    }
                                } else {
                                    Color::Reset
                                },
                            ),
                        )
                    })
                    .collect::<Vec<Line>>(),
            )
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(if focused {
                        Style::default().yellow()
                    } else {
                        Style::default()
                    })
                    .title("Conversations"),
            )
            .scroll((bar.get_position() as u16, 0)),
            content_area,
        );
    }

    fn draw_messages(
        frame: &mut Frame,
        messages: &[Message],
        focused: bool,
        contacts: &[User],
        area: Rect,
    ) {
        frame.render_widget(
            Paragraph::new(
                messages
                    .iter()
                    .flat_map(|f| {
                        let sender = contacts
                            .iter()
                            .find(|x| x.id == f.id)
                            .map(|f| {
                                if f.display_name.trim().is_empty() {
                                    f.username.clone()
                                } else {
                                    f.display_name.clone()
                                }
                            })
                            .unwrap_or("Unknown".to_string());
                        [
                            Line::from(f.content.clone()).left_aligned(),
                            Line::from(sender.clone()).italic().dim().right_aligned(),
                        ]
                    })
                    .collect::<Vec<Line>>(),
            )
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(if focused {
                        Style::default().yellow()
                    } else {
                        Style::default()
                    })
                    .title("Main Message Area"),
            ),
            area,
        );
    }

    fn draw_input(frame: &mut Frame, area: Rect, focused: bool, draft: &mut TextArea) {
        draft.set_block(
            draft
                .block()
                .cloned()
                .unwrap_or(Block::default())
                .border_style(if focused {
                    Style::default().yellow()
                } else {
                    Style::default()
                }),
        );
        frame.render_widget(&*draft, area);
    }

    fn draw_name(frame: &mut Frame, area: Rect, username: &str) {
        frame.render_widget(Paragraph::new(username).centered(), area);
    }

    fn draw_popup(frame: &mut Frame, pop: &mut Popup, contacts: &[User]) {
        match pop {
            Popup::Info(i) => Self::draw_info(frame, i),
            Popup::CommandPalette(c) => Self::draw_palette(frame, c),
            Popup::NewGroup(g) => Self::draw_new_group(frame, g, contacts),
        }
    }

    fn draw_new_group(frame: &mut Frame, g: &mut NewGroupPopup, contacts: &[User]) {
        let area = frame.area().centered(POPUP_SIZE.0, POPUP_SIZE.1);
        frame.render_widget(Clear, area);
        let block = Block::default()
            .title(" New Group ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow))
            .style(Style::default().bg(Color::Black));
        frame.render_widget(&block, area);

        let inner = block.inner(area);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Length(2),
                Constraint::Fill(1),
                Constraint::Length(2),
            ])
            .split(inner);

        let res_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Fill(1),
            ])
            .split(chunks[2]);
        if g.selection.is_none_or(|sel| !g.results.contains(&sel)) {
            g.selection = g.results.first().copied();
        }

        g.scroll = g
            .scroll
            .content_length(
                g.results
                    .len()
                    .saturating_sub(res_chunks[2].height as usize)
                    .max(1),
            )
            .viewport_content_length(res_chunks[2].height as usize);
        if let Some(sel) = g.selection
            && let Some(pos) = g.results.iter().position(|a| *a == sel)
        {
            if pos < g.scroll.get_position() {
                g.scroll = g.scroll.position(pos);
            } else if pos >= g.scroll.get_position() + (res_chunks[2].height as usize) {
                g.scroll = g.scroll.position(pos - (res_chunks[2].height as usize) + 1);
            }
        };
        g.scroll = g.scroll.position(
            g.scroll.get_position().min(
                g.results
                    .len()
                    .saturating_sub((res_chunks[2].height) as usize),
            ),
        );

        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalLeft)
                .begin_symbol(None)
                .end_symbol(None),
            res_chunks[0],
            &mut g.scroll,
        );

        g.name.set_block(
            g.name
                .block()
                .cloned()
                .unwrap_or(
                    Block::default()
                        .borders(Borders::BOTTOM)
                        .title_bottom(Line::from(vec![
                            "── Group Name".into(),
                            " (optional) ".italic().dim(),
                        ])),
                )
                .border_style(if g.focus == NewGroupFocus::Name {
                    Style::new().cyan()
                } else {
                    Style::new().dark_gray()
                }),
        );
        g.search.set_block(
            Block::default()
                .borders(Borders::BOTTOM)
                .title_bottom(Line::from(vec![
                    "── Search Users ".into(),
                    "(Enter to add) ".italic().dim(),
                ]))
                .title_bottom(
                    Line::from(vec![
                        Span::from(format!(" {} selected ", g.added.len())),
                        Span::from("─"),
                    ])
                    .right_aligned(),
                )
                .border_style(if g.focus == NewGroupFocus::Search {
                    Style::new().cyan()
                } else {
                    Style::new().dark_gray()
                }),
        );

        frame.render_widget(&*g.name, chunks[0]);
        frame.render_widget(&*g.search, chunks[1]);

        frame.render_widget(
            Paragraph::new(
                g.results
                    .iter()
                    .filter_map(|uid| {
                        let user = contacts.iter().find(|c| c.id == *uid)?;
                        Some(
                            Line::from(if !user.display_name.trim().is_empty() {
                                vec![
                                    Span::from(user.display_name.clone()),
                                    Span::from(" "),
                                    Span::from(format!("@{} ", user.username.clone()))
                                        .dim()
                                        .italic(),
                                ]
                            } else {
                                vec![Span::from(format!("@{} ", user.username.clone())).italic()]
                            })
                            .style(if g.added.contains(uid) {
                                if let Some(sel) = g.selection
                                    && user.id == sel
                                    && g.focus == NewGroupFocus::Search
                                {
                                    Style::new().fg(Color::Green).bold()
                                } else {
                                    Style::new().fg(Color::Green)
                                }
                            } else if let Some(sel) = g.selection
                                && user.id == sel
                                && g.focus == NewGroupFocus::Search
                            {
                                Style::new().bold().fg(Color::Cyan)
                            } else {
                                Style::new()
                            }),
                        )
                    })
                    .collect::<Vec<Line>>(),
            )
            .scroll((g.scroll.get_position() as u16, 0)),
            res_chunks[2],
        );
        frame.render_widget(
            Paragraph::new("Tab: switch field | Ctrl+Enter: confirm |  Esc: cancel")
                .style(Style::default().italic().gray().dim())
                .block(Block::default().borders(Borders::TOP))
                .centered(),
            chunks[3],
        );
    }

    fn draw_info(frame: &mut Frame, pop: &InfoPopup) {
        let area = frame.area().centered(POPUP_SIZE.0, POPUP_SIZE.1);
        frame.render_widget(Clear, area);

        let block = Block::default()
            .title(format!(" {} ", pop.title))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(pop.colour))
            .style(Style::default().bg(Color::Black));
        let inner = block.inner(area);

        frame.render_widget(block, area);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(1), Constraint::Length(1)])
            .split(inner);

        frame.render_widget(
            Paragraph::new(pop.message.as_str())
                .centered()
                .style(Style::default().bg(Color::Black)),
            chunks[0],
        );
        frame.render_widget(
            Paragraph::new("Press Space or Enter to close")
                .centered()
                .style(Style::default().italic().fg(Color::Gray).bg(Color::Black)),
            chunks[1],
        );
    }

    fn draw_palette(frame: &mut Frame, p: &mut PalettePopup) {
        let area = frame.area().centered(POPUP_SIZE.0, POPUP_SIZE.1);
        frame.render_widget(Clear, area);
        let block = Block::default()
            .title(" Command Palette ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow))
            .style(Style::default().bg(Color::Black));
        frame.render_widget(&block, area);

        let inner = block.inner(area);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Fill(1),
                Constraint::Length(2),
            ])
            .split(inner);

        let res_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Fill(1),
            ])
            .split(chunks[1]);

        p.scroll = p
            .scroll
            .content_length(
                p.results
                    .len()
                    .saturating_sub(res_chunks[2].height as usize)
                    .max(1),
            )
            .viewport_content_length(res_chunks[2].height as usize);
        if let Some(sel) = &p.selection
            && let Some(pos) = p.results.iter().position(|a| a == sel)
        {
            if pos < p.scroll.get_position() {
                p.scroll = p.scroll.position(pos);
            } else if pos >= p.scroll.get_position() + (res_chunks[2].height as usize) {
                p.scroll = p.scroll.position(pos - (res_chunks[2].height as usize) + 1);
            }
        };
        p.scroll = p.scroll.position(
            p.scroll.get_position().min(
                p.results
                    .len()
                    .saturating_sub((res_chunks[2].height) as usize),
            ),
        );

        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalLeft)
                .begin_symbol(None)
                .end_symbol(None),
            res_chunks[0],
            &mut p.scroll,
        );
        frame.render_widget(&*p.text, chunks[0]);

        frame.render_widget(
            Paragraph::new(
                p.results
                    .iter()
                    .map(|comm| {
                        Line::from(comm.to_string()).style(Style::default().bold().fg(
                            if let Some(sel) = &p.selection
                                && comm == sel
                            {
                                Color::Cyan
                            } else {
                                Color::Reset
                            },
                        ))
                    })
                    .collect::<Vec<Line>>(),
            )
            .scroll((p.scroll.get_position() as u16, 0)),
            res_chunks[2],
        );
        frame.render_widget(
            Paragraph::new("%: Search | Enter: select | ↑/↓: navigate | Esc: close")
                .style(Style::default().italic().gray().dim())
                .block(Block::default().borders(Borders::TOP))
                .centered(),
            chunks[2],
        );
    }

    fn draw(&mut self, frame: &mut Frame) {
        match &mut self.screen {
            Screens::Main {
                draft,
                user,
                focus,
                selected_conv,
                active_conv,
                scroll,
                conv_height,
            } => {
                let cols = Layout::horizontal(vec![
                    Constraint::Percentage(18),
                    Constraint::Percentage(82),
                ])
                .split(frame.area());
                let l_rows = Layout::vertical(vec![
                    Constraint::Percentage(100),
                    Constraint::Length(1),
                    Constraint::Min(1),
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
                    *focus == FocusPoints::Conversations,
                    self.conversations.as_ref(),
                    selected_conv,
                    active_conv,
                    scroll,
                    conv_height,
                );
                Self::draw_name(frame, l_rows[1], &user.username);
                let x = Vec::new();
                Self::draw_messages(
                    frame,
                    if let Some(act) = active_conv {
                        self.messages.get(act).unwrap_or(&x)
                    } else {
                        &x
                    },
                    *focus == FocusPoints::Messages,
                    &self.contacts,
                    rows[0],
                );
                Self::draw_input(frame, rows[1], *focus == FocusPoints::Draft, draft);
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
                password.set_mask_char('*');
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
                frame.render_widget(&**username, centre[1]);
                frame.render_widget(&**password, centre_password[1]);
                frame.render_widget(
                    Paragraph::new(Line::from(vec![
                        Span::styled("TAB", Style::default().bold()),
                        Span::from(": Switch field  |  "),
                        Span::styled("ENTER", Style::default().bold()),
                        Span::from(": Login  |  "),
                        Span::styled("CTRL+C", Style::default().bold()),
                        Span::from(": Quit"),
                    ]))
                    .centered()
                    .style(Style::default().fg(Color::DarkGray))
                    .italic(),
                    vert[5],
                );
            }
        }
        if let Some(pop) = &mut self.popup {
            Self::draw_popup(frame, pop, &self.contacts);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let db = local_db::init().await?;
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<ClientProtocol>(100);
    let (inbound_tx, inbound_rx) = mpsc::channel::<InboundMessage>(100);

    let mut app = App::new(inbound_rx, outbound_tx, db).await?;

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
