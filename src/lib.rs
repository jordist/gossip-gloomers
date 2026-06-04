use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{oneshot, Mutex};

pub struct Message {
    pub src: String,
    pub msg_type: String,
    pub body: Value,
}

struct NodeInner {
    id: Mutex<String>,
    node_ids: Mutex<Vec<String>>,
    msg_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Value>>>,
    out: Mutex<tokio::io::Stdout>,
}

#[derive(Clone)]
pub struct Node {
    inner: Arc<NodeInner>,
}

impl Node {
    pub async fn id(&self) -> String {
        self.inner.id.lock().await.clone()
    }

    pub async fn node_ids(&self) -> Vec<String> {
        self.inner.node_ids.lock().await.clone()
    }

    pub fn new() -> Self {
        Node {
            inner: Arc::new(NodeInner {
                id: Mutex::new(String::new()),
                node_ids: Mutex::new(vec![]),
                msg_id: AtomicU64::new(0),
                pending: Mutex::new(HashMap::new()),
                out: Mutex::new(tokio::io::stdout()),
            }),
        }
    }

    fn init_logger() {
        env_logger::Builder::new()
            .target(env_logger::Target::Stderr)
            .filter_level(log::LevelFilter::Info)
            .init();
    }

    fn next_msg_id(&self) -> u64 {
        self.inner.msg_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    // Stamps `body` with `msg_id` and writes the framed message to stdout.
    async fn write_message(&self, dest: &str, mut body: Value, msg_id: u64) {
        body["msg_id"] = json!(msg_id);
        let src = self.inner.id.lock().await.clone();
        let msg = json!({ "src": src, "dest": dest, "body": body });
        let s = serde_json::to_string(&msg).unwrap();
        log::info!("send: {s}");
        let mut out = self.inner.out.lock().await;
        out.write_all(s.as_bytes()).await.expect("write failed");
        out.write_all(b"\n").await.expect("write failed");
        out.flush().await.expect("flush failed");
    }

    pub async fn send(&self, dest: &str, body: Value) -> u64 {
        let id = self.next_msg_id();
        self.write_message(dest, body, id).await;
        id
    }

    // Sends `body` to `dest` and suspends until the matching reply arrives.
    pub async fn rpc(&self, dest: &str, body: Value) -> Value {
        let (tx, rx) = oneshot::channel();
        let id = self.next_msg_id();
        // Register the pending slot *before* writing so a fast reply can't be
        // observed by the dispatch loop before we're ready to receive it.
        self.inner.pending.lock().await.insert(id, tx);
        self.write_message(dest, body, id).await;
        rx.await.unwrap()
    }

    // Like [`rpc`], but gives up after `timeout`. Returns `Some(reply)` if the
    // reply arrived in time, or `None` on timeout (cleaning up the pending slot
    // so retries don't leak entries).
    pub async fn rpc_timeout(&self, dest: &str, body: Value, timeout: Duration) -> Option<Value> {
        let (tx, rx) = oneshot::channel();
        let id = self.next_msg_id();
        self.inner.pending.lock().await.insert(id, tx);
        self.write_message(dest, body, id).await;
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(reply)) => Some(reply),
            _ => {
                self.inner.pending.lock().await.remove(&id);
                None
            }
        }
    }

    async fn process_line<F, Fut>(node: &Node, line: String, handler: &Arc<F>)
    where
        F: Fn(Node, Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<Value>> + Send + 'static,
    {
        log::info!("recv: {line}");
        let msg: Value = match serde_json::from_str(&line) {
            Ok(msg) => msg,
            Err(e) => {
                log::error!("ignoring malformed line ({e}): {line}");
                return;
            }
        };
        let src = msg["src"].as_str().unwrap_or("").to_string();
        let body = msg["body"].clone();
        let msg_type = body["type"].as_str().unwrap_or("").to_string();

        // Deliver replies to waiting rpc() callers
        if let Some(reply_id) = body["in_reply_to"].as_u64() {
            if let Some(tx) = node.inner.pending.lock().await.remove(&reply_id) {
                let _ = tx.send(body);
                return;
            }
        }

        if msg_type == "init" {
            *node.inner.id.lock().await = body["node_id"].as_str().unwrap_or("").to_string();
            *node.inner.node_ids.lock().await = body["node_ids"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            log::info!("initialized as {}", node.inner.id.lock().await);
            node.send(
                &src,
                json!({ "type": "init_ok", "in_reply_to": body["msg_id"] }),
            )
            .await;
            return;
        }

        // Spawn so the dispatch loop keeps running while the handler awaits.
        let node = node.clone();
        let handler = handler.clone();
        let orig_msg_id = body["msg_id"].clone();
        tokio::spawn(async move {
            let msg = Message {
                src: src.clone(),
                msg_type,
                body,
            };
            if let Some(mut reply) = handler(node.clone(), msg).await {
                reply["in_reply_to"] = orig_msg_id;
                node.send(&src, reply).await;
            }
        });
    }

    pub async fn run<F, Fut>(&self, handler: F)
    where
        F: Fn(Node, Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<Value>> + Send + 'static,
    {
        Self::init_logger();
        let handler = Arc::new(handler);
        let mut lines = BufReader::new(tokio::io::stdin()).lines();

        while let Ok(Some(line)) = lines.next_line().await {
            Self::process_line(self, line, &handler).await;
        }
    }
}
