use std::io::{self, BufRead, Write};
use serde_json::{json, Value};

pub struct Node {
    pub id: String,
    pub node_ids: Vec<String>,
    msg_id: u64,
}

impl Node {
    pub fn run<F>(mut handler: F)
    where
        F: FnMut(&mut Node, &str, &Value) -> Option<Value>,
    {
        env_logger::Builder::new()
            .target(env_logger::Target::Stderr)
            .filter_level(log::LevelFilter::Info)
            .init();

        let stdin = io::stdin();
        let stdout = io::stdout();
        let mut out = stdout.lock();
        let mut node = Node { id: String::new(), node_ids: vec![], msg_id: 0 };

        for line in stdin.lock().lines() {
            let line = line.expect("failed to read line");
            log::info!("recv: {line}");

            let msg: Value = serde_json::from_str(&line).expect("invalid JSON");
            let src = msg["src"].as_str().unwrap_or("").to_string();
            let dest = msg["dest"].as_str().unwrap_or("").to_string();
            let body = &msg["body"];
            let msg_type = body["type"].as_str().unwrap_or("");
            let in_reply_to = body["msg_id"].clone();

            node.msg_id += 1;

            let reply_body = if msg_type == "init" {
                node.id = body["node_id"].as_str().unwrap_or("").to_string();
                node.node_ids = body["node_ids"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                    .unwrap_or_default();
                log::info!("initialized as {}", node.id);
                Some(json!({ "type": "init_ok" }))
            } else {
                handler(&mut node, msg_type, body)
            };

            if let Some(mut rb) = reply_body {
                rb["msg_id"] = json!(node.msg_id);
                rb["in_reply_to"] = in_reply_to;
                let reply = json!({ "src": dest, "dest": src, "body": rb });
                let reply_str = serde_json::to_string(&reply).unwrap();
                log::info!("send: {reply_str}");
                writeln!(out, "{reply_str}").expect("failed to write");
                out.flush().expect("failed to flush");
            }
        }
    }
}
