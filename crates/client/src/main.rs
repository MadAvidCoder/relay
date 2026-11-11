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
use ratatui_textarea::TextArea;
use relay_common::{ClientProtocol, Conversation, ServerProtocol};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

#[derive(Debug, PartialEq)]
enum InputModes {
    Drafting,
    Shortcutting,
    Searching,
}
struct App {
    running: bool,
    cur_conversation: Option<Conversation>,
    text_area: TextArea<'static>,
    messages: Vec<ServerProtocol>,
    input_mode: InputModes,
    tx: mpsc::Sender<ClientProtocol>,
    rx: mpsc::Receiver<ServerProtocol>,
}
impl App {
    fn new(rx: mpsc::Receiver<ServerProtocol>, tx: mpsc::Sender<ClientProtocol>) -> Self {
        let mut text_area = TextArea::default();
        text_area.set_cursor_line_style(Style::default());
        text_area.set_block(
            Block::default()
                .borders(Borders::ALL)
                .title("Send a Mesasage"),
        );
        Self {
            running: true,
            rx,
            tx,
            text_area,
            cur_conversation: None,
            input_mode: InputModes::Drafting,
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
        if event::poll(std::time::Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key)
                    if key.is_press()
                        && key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    self.running = false
                }
                e if self.input_mode == InputModes::Drafting => {
                    self.text_area.input(e);
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
        match s.grapheme_indices(true).nth(ind) {
            Some((b_ind, _)) => s.split_at(b_ind),
            None => (s, ""),
        }
    }

    fn draw_input(&self, frame: &mut Frame, area: Rect) {
        frame.render_widget(&self.text_area, area);
    }

    fn draw(&self, frame: &mut Frame) {
        let cols = Layout::horizontal(vec![Constraint::Percentage(15), Constraint::Percentage(80)])
            .split(frame.area());
        let rows = Layout::vertical(vec![
            Constraint::Min(5),
            Constraint::Length((self.text_area.lines().len() as u16 + 2).clamp(3, 8)),
        ])
        .split(cols[1]);

        self.draw_conv_list(frame, cols[0]);
        self.draw_messages(frame, rows[0]);
        self.draw_input(frame, rows[1]);
        // self.text_area.delete_line_b
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
