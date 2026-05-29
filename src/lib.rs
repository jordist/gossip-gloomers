use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
use std::sync::mpsc;
use std::time::Duration;

pub struct Message<'a> {
    pub src: &'a str,
    pub msg_type: &'a str,
    pub body: &'a Value,
}

pub struct Node {
    pub id: String,
    pub node_ids: Vec<String>,
    msg_id: u64,
    out: io::Stdout,
}

impl Node {
    // ── helpers ──────────────────────────────────────────────────────────────

    pub fn next_msg_id(&mut self) -> u64 {
        self.msg_id += 1;
        self.msg_id
    }

    pub fn send(&mut self, dest: &str, body: Value) -> u64 {
        let mut body = body;
        let id = self.next_msg_id();
        body["msg_id"] = json!(id);
        let msg = json!({ "src": self.id, "dest": dest, "body": body });
        let s = serde_json::to_string(&msg).unwrap();
        log::info!("send: {s}");
        let mut out = self.out.lock();
        writeln!(out, "{s}").expect("failed to write");
        out.flush().expect("failed to flush");
        id
    }

    fn init_logger() {
        env_logger::Builder::new()
            .target(env_logger::Target::Stderr)
            .filter_level(log::LevelFilter::Info)
            .init();
    }

    /// Moves stdin onto a background thread and returns the receiving end of a
    /// channel so the main loop can read lines without blocking indefinitely.
    fn spawn_stdin_reader() -> mpsc::Receiver<String> {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in io::stdin().lock().lines() {
                if tx.send(line.expect("failed to read line")).is_err() {
                    break; // main thread dropped the receiver
                }
            }
        });
        rx
    }

    fn new() -> Self {
        Node {
            id: String::new(),
            node_ids: vec![],
            msg_id: 0,
            out: io::stdout(),
        }
    }

    fn process_message<F>(&mut self, line: String, handler: &mut F)
    where
        F: FnMut(&mut Node, &Message) -> Option<Value>,
    {
        log::info!("recv: {line}");
        let msg: Value = serde_json::from_str(&line).expect("invalid JSON");
        let src = msg["src"].as_str().unwrap_or("").to_string();
        let body = &msg["body"];
        let msg_type = body["type"].as_str().unwrap_or("");
        let in_reply_to = body["msg_id"].clone();

        let reply_body = if msg_type == "init" {
            self.id = body["node_id"].as_str().unwrap_or("").to_string();
            self.node_ids = body["node_ids"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            log::info!("initialized as {}", self.id);
            Some(json!({ "type": "init_ok" }))
        } else {
            handler(
                self,
                &Message {
                    src: &src,
                    msg_type,
                    body,
                },
            )
        };

        if let Some(mut rb) = reply_body {
            rb["in_reply_to"] = in_reply_to;
            self.send(&src, rb);
        }
    }

    // ── entry points ─────────────────────────────────────────────────────────

    pub fn run<F>(mut handler: F)
    where
        F: FnMut(&mut Node, &Message) -> Option<Value>,
    {
        Self::init_logger();
        let rx = Self::spawn_stdin_reader();
        let mut node = Self::new();

        loop {
            match rx.recv() {
                Ok(line) => node.process_message(line, &mut handler),
                Err(_) => break, // stdin closed
            }
        }
    }

    /// Like `run`, but also fires `handler.tick()` every `interval` when no
    /// message arrives. Use this to implement retries, gossip sweeps, etc.
    pub fn run_with_tick<H: NodeHandler>(mut handler: H, interval: Duration) {
        Self::init_logger();
        let rx = Self::spawn_stdin_reader();
        let mut node = Self::new();

        loop {
            match rx.recv_timeout(interval) {
                Ok(line) => node.process_message(line, &mut |n, m| handler.handle(n, m)),
                Err(mpsc::RecvTimeoutError::Timeout) => handler.tick(&mut node),
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }
}

pub trait NodeHandler {
    fn handle(&mut self, node: &mut Node, msg: &Message) -> Option<Value>;
    fn tick(&mut self, node: &mut Node);
}
