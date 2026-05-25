use std::io::{self, BufRead, Write};
use serde_json::{json, Value};

fn main() {
    env_logger::Builder::new()
        .target(env_logger::Target::Stderr)
        .filter_level(log::LevelFilter::Info)
        .init();

    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let mut msg_id: u64 = 0;
    let mut node_id = String::new();

    for line in stdin.lock().lines() {
        let line = line.expect("failed to read line");
        log::info!("recv: {line}");

        let msg: Value = serde_json::from_str(&line).expect("invalid JSON");
        let src = msg["src"].as_str().unwrap_or("");
        let dest = msg["dest"].as_str().unwrap_or("");
        let body = &msg["body"];
        let msg_type = body["type"].as_str().unwrap_or("");

        msg_id += 1;

        let reply_body = match msg_type {
            "init" => {
                node_id = body["node_id"].as_str().unwrap_or("").to_string();
                log::info!("initialized as {node_id}");
                json!({ "type": "init_ok", "in_reply_to": body["msg_id"], "msg_id": msg_id })
            }
            "echo" => {
                json!({ "type": "echo_ok", "in_reply_to": body["msg_id"], "msg_id": msg_id, "echo": body["echo"] })
            }
            other => {
                log::warn!("unknown message type: {other}");
                continue;
            }
        };

        let reply = json!({ "src": dest, "dest": src, "body": reply_body });
        let reply_str = serde_json::to_string(&reply).unwrap();
        log::info!("send: {reply_str}");
        writeln!(out, "{reply_str}").expect("failed to write");
        out.flush().expect("failed to flush");
    }
}
