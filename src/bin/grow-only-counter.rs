use flyio::{Message, Node};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};
use std::time::Duration;

const RPC_TIMEOUT: Duration = Duration::from_secs(1);

struct KVStore {
    node: Node,
    id: String,
}

impl KVStore {
    async fn read_int(&self, key: &str) -> i64 {
        loop {
            match self
                .node
                .rpc_timeout(&self.id, json!({"type": "read", "key": key}), RPC_TIMEOUT)
                .await
            {
                None => continue,
                Some(reply) => return reply["value"].as_i64().unwrap_or(0),
            }
        }
    }

    async fn write_int(&self, key: &str, val: i64) -> Option<()> {
        self.node
            .rpc_timeout(
                &self.id,
                json!({"type": "write", "key": key, "value": val}),
                RPC_TIMEOUT,
            )
            .await?;
        Some(())
    }
}

struct State {
    kv_store: KVStore,
    local_val: AtomicI64,
    last_published: AtomicI64,
}

async fn process_add(state: Arc<State>, msg: Message) {
    let delta = msg.body["delta"].as_i64().unwrap();
    state.local_val.fetch_add(delta, Ordering::Relaxed);
}

async fn process_read(node: Node, state: Arc<State>) -> Value {
    let my_id = node.id().await;
    let reads: Vec<_> = node
        .node_ids()
        .await
        .into_iter()
        .filter(|id| id != &my_id)
        .map(|id| {
            let state = Arc::clone(&state);
            async move { state.kv_store.read_int(&id).await }
        })
        .collect();
    let remote_sum: i64 = futures::future::join_all(reads).await.into_iter().sum();
    let local_val = state.local_val.load(Ordering::Relaxed);
    json!({"type": "read_ok", "value": remote_sum + local_val})
}

async fn publish_counter(node: Node, state: Arc<State>) {
    let local_val = state.local_val.load(Ordering::Relaxed);
    if local_val > state.last_published.load(Ordering::Relaxed) {
        let key = node.id().await;
        if state.kv_store.write_int(&key, local_val).await.is_some() {
            state.last_published.store(local_val, Ordering::Relaxed);
        }
    }
}

#[tokio::main]
async fn main() {
    let node = Node::new();
    let state = Arc::new(State {
        kv_store: KVStore {
            node: node.clone(),
            id: "seq-kv".to_string(),
        },
        local_val: AtomicI64::new(0),
        last_published: AtomicI64::new(0),
    });

    let state_bg = Arc::clone(&state);
    let node_bg = node.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_micros(500));
        loop {
            interval.tick().await;
            publish_counter(node_bg.clone(), Arc::clone(&state_bg)).await;
        }
    });

    node.run(move |node, msg| {
        let state = Arc::clone(&state);
        async move {
            match msg.msg_type.as_str() {
                "add" => {
                    process_add(Arc::clone(&state), msg).await;
                    Some(json!({"type": "add_ok"}))
                }
                "read" => Some(process_read(node, state).await),
                other => {
                    panic!("unknown message type: {other}");
                }
            }
        }
    })
    .await;
}
