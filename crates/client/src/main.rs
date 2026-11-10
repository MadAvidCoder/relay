use anyhow::{Error, Result};
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use futures_util::{SinkExt, StreamExt};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};
use relay_common::{ClientProtocol, Conversation, ServerProtocol};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Debug, PartialEq)]
enum InputModes {
    Drafting,
    Shortcutting,
    Searching,
}

struct App {
    running: bool,
    cur_conversation: Option<Conversation>,
    cur_draft: String,
    messages: Vec<ServerProtocol>,
    input_mode: InputModes,
    tx: mpsc::Sender<ClientProtocol>,
    draft_curs_pos: usize,
    rx: mpsc::Receiver<ServerProtocol>,
}
impl App {
    fn new(rx: mpsc::Receiver<ServerProtocol>, tx: mpsc::Sender<ClientProtocol>) -> Self {
        Self {
            running: true,
            rx,
            tx,
            cur_draft: String::new(),
            cur_conversation: None,
            input_mode: InputModes::Drafting,
            draft_curs_pos: 0,
            messages: Vec::new(),
        }
    }

    async fn run(&mut self) -> Result<()> {
        let mut term = ratatui::init();
        while self.running {
            while let Ok(msg) = self.rx.try_recv() {
                self.messages.push(msg);
            }
            term.draw(|frame| self.draw(frame))?;

            self.handle_events()?;
        }

        Ok(())
    }

    fn handle_events(&mut self) -> Result<()> {
        if event::poll(std::time::Duration::from_millis(50))?
            && let Event::Key(key) = event::read()?
            && key.is_press()
        {
            match key.code {
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.running = false
                }
                KeyCode::Char(c) if self.input_mode == InputModes::Drafting => {
                    if let Some((b_ind, _)) = self.cur_draft.char_indices().nth(self.draft_curs_pos)
                    {
                        self.cur_draft.insert(b_ind, c);
                    } else {
                        self.cur_draft.push(c);
                    }
                    self.draft_curs_pos = self.draft_curs_pos.saturating_add(1).min(self.cur_draft.len());
                }
                KeyCode::Left if self.input_mode == InputModes::Drafting => {
                    self.draft_curs_pos = self.draft_curs_pos.saturating_sub(1);
                }
                KeyCode::Right if self.input_mode == InputModes::Drafting => {
                    self.draft_curs_pos = self.draft_curs_pos.saturating_add(1).min(self.cur_draft.len());
                }
                KeyCode::Backspace
                    if self.input_mode == InputModes::Drafting
                        && self.draft_curs_pos > 0
                        && let Some((b_ind, _)) =
                            self.cur_draft.char_indices().nth(self.draft_curs_pos - 1) =>
                {
                    self.cur_draft.remove(b_ind);
                    self.draft_curs_pos = self.draft_curs_pos.saturating_sub(1);
                }
                KeyCode::Delete
                    if self.input_mode == InputModes::Drafting
                        && self.draft_curs_pos < self.cur_draft.len()
                        && let Some((b_ind, _)) =
                            self.cur_draft.char_indices().nth(self.draft_curs_pos) =>
                {
                    self.cur_draft.remove(b_ind);
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn draw_conv_list(&self, frame: &mut Frame, area: Rect) {
        frame.render_widget(
            Paragraph::new("Hello World").block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Conversations"),
            ),
            area,
        );
    }

    fn draw_messages(&self, frame: &mut Frame, area: Rect) {
        frame.render_widget(
            Paragraph::new("Hello World").block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Main Message Area"),
            ),
            area,
        );
    }

    fn split_str<'a>(&self, s: &'a str, ind: usize) -> (&'a str, &'a str) {
        match s.char_indices().nth(ind) {
            Some((b_ind, _)) => s.split_at(b_ind),
            None => (s, ""),
        }
    }

    fn draw_input(&self, frame: &mut Frame, area: Rect) {
        let split = self.split_str(&self.cur_draft, self.draft_curs_pos);
        let spans = vec![
            Span::from(split.0),
            // Span::styled("|", Style::new().yellow()),
            Span::from(split.1),
        ];
        frame.render_widget(
            Paragraph::new(Line::from(spans))
                .block(Block::default().borders(Borders::ALL).title("New Message")),
            area,
        );
        let x = area.x + 1 + split.0.chars().count() as u16;
        let y = area.y + 1;

        frame.set_cursor_position((x, y));
    }

    fn draw(&self, frame: &mut Frame) {
        let cols = Layout::horizontal(vec![Constraint::Percentage(15), Constraint::Percentage(80)])
            .split(frame.area());
        let rows = Layout::vertical(vec![Constraint::Percentage(100), Constraint::Length(3)])
            .split(cols[1]);

        self.draw_conv_list(frame, cols[0]);
        self.draw_messages(frame, rows[0]);
        self.draw_input(frame, rows[1]);
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
