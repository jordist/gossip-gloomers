use flyio::{Message, Node};
use futures::future::join_all;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};

use crate::KVStoreError::{ExpectationMismatch, KeyNotFound};

// Improves part B by batching together the offset reservations in one single CAS request.
// Could improve further by combining multiple log entries into a single kv store buket entry.
const RPC_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_ITEMS_PER_POLL: u64 = 10;
const BATCH_INTERVAL: Duration = Duration::from_millis(10);

type LogId = String;
type Offset = u64;

struct State {
    kv_store_lin: KVStore,
    kv_store_seq: KVStore,
    batcher: mpsc::UnboundedSender<(LogId, oneshot::Sender<Offset>)>,
}

struct KVStore {
    name: String,
    node: Node,
}

enum KVStoreError {
    ExpectationMismatch,
    KeyNotFound,
}

impl KVStore {
    async fn read(&self, key: &str) -> Result<Value, KVStoreError> {
        loop {
            let mut req = json!({"type": "read", "key": key});
            match self
                .node
                .rpc_timeout(&self.name, req.take(), RPC_TIMEOUT)
                .await
            {
                None => continue, // timeout
                Some(mut reply) => {
                    if reply["type"] == "error" && reply["code"] == 20 {
                        return Err(KeyNotFound);
                    } else {
                        return Ok(reply["value"].take());
                    }
                }
            }
        }
    }

    async fn write(&self, key: &str, val: &Value) {
        loop {
            let mut req = json!({"type": "write", "key": key, "value": val});
            match self
                .node
                .rpc_timeout(&self.name, req.take(), RPC_TIMEOUT)
                .await
            {
                None => continue, // timeout
                Some(reply) => {
                    if reply["type"] != "write_ok" {
                        panic!()
                    }
                    return;
                }
            }
        }
    }

    async fn cas(
        &self,
        key: &str,
        expect: &Value,
        new: &Value,
        create_if_not_exists: bool,
    ) -> Result<(), KVStoreError> {
        loop {
            let mut req = json!({"type": "cas", "key": key, "from": expect, "to": new, "create_if_not_exists": create_if_not_exists});
            match self
                .node
                .rpc_timeout(&self.name, req.take(), RPC_TIMEOUT)
                .await
            {
                None => continue, // timeout
                Some(reply) => {
                    if reply["type"] == "cas_ok" {
                        return Ok(());
                    } else if reply["type"] == "error" && reply["code"] == 22 {
                        return Err(ExpectationMismatch);
                    } else {
                        panic!()
                    }
                }
            }
        }
    }
}

async fn reserve_offsets(kv: &KVStore, log_id: &str, count: u64) -> Offset {
    let next_offset_key = format!("{log_id}_next");
    loop {
        let current_val = match kv.read(&next_offset_key).await {
            Ok(val) => val,
            Err(KeyNotFound) => json!(0),
            Err(_) => panic!(),
        };

        let offset = current_val.as_u64().unwrap_or(0);
        let next = offset + count;

        match kv
            .cas(&next_offset_key, &current_val, &json!(next), true)
            .await
        {
            Ok(()) => return offset,
            Err(ExpectationMismatch) => continue,
            Err(_) => panic!(),
        }
    }
}

async fn run_batcher(
    state: Arc<State>,
    mut rx: mpsc::UnboundedReceiver<(LogId, oneshot::Sender<Offset>)>,
) {
    loop {
        tokio::time::sleep(BATCH_INTERVAL).await;

        let mut batch: HashMap<LogId, Vec<oneshot::Sender<Offset>>> = HashMap::new();
        while let Ok((log_id, tx)) = rx.try_recv() {
            batch.entry(log_id).or_default().push(tx);
        }

        if batch.is_empty() {
            continue;
        }

        let futures = batch.into_iter().map(|(log_id, senders)| {
            let state = Arc::clone(&state);
            async move {
                let count = senders.len() as u64;
                let start = reserve_offsets(&state.kv_store_lin, &log_id, count).await;
                for (i, tx) in senders.into_iter().enumerate() {
                    let _ = tx.send(start + i as u64);
                }
            }
        });
        join_all(futures).await;
    }
}

async fn process_send(state: &State, msg: Message) -> Offset {
    let log_id = msg.body["key"].as_str().unwrap().to_string();
    let val = &msg.body["msg"];

    let (tx, rx) = oneshot::channel();
    state.batcher.send((log_id.clone(), tx)).unwrap();
    let offset = rx.await.unwrap();

    let log_and_offset_key = format!("{log_id}_{offset}");
    state.kv_store_seq.write(&log_and_offset_key, val).await;
    offset
}

async fn read_log(
    kv_store: &KVStore,
    log_id: &LogId,
    from_offset: Offset,
    max_items: u64,
) -> Vec<Value> {
    let mut entries = Vec::new();
    let mut offset = from_offset;
    let mut read_items = 0;
    loop {
        let key = format!("{log_id}_{offset}");
        let val = kv_store.read(&key).await;
        match val {
            Ok(val) => {
                entries.push(json!([offset, val]));
                offset += 1;
            }
            Err(KeyNotFound) => {
                break;
            }
            Err(_) => {
                panic!()
            }
        }

        read_items += 1;
        if read_items >= max_items {
            break;
        }
    }

    return entries;
}

async fn process_poll(state: &State, msg: Message) -> Value {
    let mut res = serde_json::Map::new();

    let offsets = msg.body["offsets"].as_object().unwrap();
    for (log_name, val) in offsets {
        let offset = val.as_u64().unwrap();
        let msgs = read_log(&state.kv_store_seq, log_name, offset, MAX_ITEMS_PER_POLL).await;
        res.insert(log_name.clone(), json!(msgs));
    }

    json!(res)
}

async fn process_commited_offsets(state: &State, msg: Message) {
    let offsets = msg.body["offsets"].as_object().unwrap();
    for (log_name, val) in offsets {
        let new_offset = val.as_u64().unwrap();
        let committed_key = format!("{log_name}_committed");

        state
            .kv_store_seq
            .write(&committed_key, &json!(new_offset))
            .await;
    }
}

async fn process_list_commited_offsets(state: &State, msg: Message) -> Value {
    let mut res = serde_json::Map::new();

    let keys = msg.body["keys"].as_array().unwrap();
    for key in keys {
        let log_name = key.as_str().unwrap();
        let committed_key = format!("{log_name}_committed");
        let offset = match state.kv_store_seq.read(&committed_key).await {
            Ok(val) => val.as_u64().unwrap(),
            Err(KeyNotFound) => 0,
            Err(_) => panic!(),
        };
        res.insert(log_name.to_string(), json!(offset));
    }

    json!(res)
}

#[tokio::main]
async fn main() {
    let node = Node::new();
    let (batcher_tx, batcher_rx) = mpsc::unbounded_channel();
    let state = Arc::new(State {
        kv_store_lin: KVStore {
            name: "lin-kv".to_string(),
            node: node.clone(),
        },
        kv_store_seq: KVStore {
            name: "seq-kv".to_string(),
            node: node.clone(),
        },
        batcher: batcher_tx,
    });

    tokio::spawn(run_batcher(Arc::clone(&state), batcher_rx));

    node.run(move |_node, msg| {
        let state = Arc::clone(&state);
        async move {
            match msg.msg_type.as_str() {
                "send" => {
                    let offset = process_send(&state, msg).await;
                    Some(json!({"type": "send_ok", "offset": offset}))
                }
                "poll" => {
                    let msgs = process_poll(&state, msg).await;
                    Some(json!({"type": "poll_ok", "msgs": msgs}))
                }
                "commit_offsets" => {
                    process_commited_offsets(&state, msg).await;
                    Some(json!({"type": "commit_offsets_ok"}))
                }
                "list_committed_offsets" => {
                    let offsets = process_list_commited_offsets(&state, msg).await;
                    Some(json!({"type": "list_committed_offsets_ok", "offsets": offsets}))
                }
                other => {
                    panic!("unknown message type: {other}");
                }
            }
        }
    })
    .await;
}
