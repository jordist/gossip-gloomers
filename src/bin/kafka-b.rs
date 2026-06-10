use flyio::{Message, Node};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

use crate::KVStoreError::{ExpectationMismatch, KeyNotFound};

const RPC_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_ITEMS_PER_POLL: u64 = 10;

type LogId = String;
type Offset = u64;

struct State {
    kv_store_lin: KVStore,
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

    async fn write(&self, key: &str, val: &Value) -> Result<(), KVStoreError> {
        loop {
            let mut req = json!({"type": "write", "key": key, "value": val});
            match self
                .node
                .rpc_timeout(&self.name, req.take(), RPC_TIMEOUT)
                .await
            {
                None => continue, // timeout
                Some(_) => return Ok(()),
            }
        }
    }

    async fn cas(&self, key: &str, expect: &Value, new: &Value, create_if_not_exists: bool) -> Result<(), KVStoreError> {
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

async fn process_send(state: &State, msg: Message) -> Offset {
    let log_id = msg.body["key"].as_str().unwrap().to_string();
    let val = &msg.body["msg"];

    let next_offset_key = format!("{log_id}_next");

    let offset = loop {
        let current_val = match state.kv_store_lin.read(&next_offset_key).await {
            Ok(val) => val,
            Err(KeyNotFound) => json!(0),
            Err(_) => panic!(),
        };

        let offset = current_val.as_u64().unwrap_or(0);
        let next = offset + 1;

        match state.kv_store_lin.cas(&next_offset_key, &current_val, &json!(next), true).await {
            Ok(()) => break offset,
            Err(ExpectationMismatch) => continue,
            Err(_) => panic!(),
        }
    };

    let log_and_offset_key = format!("{log_id}_{offset}");
    loop {
        match state.kv_store_lin.write(&log_and_offset_key, val).await {
            Ok(()) => break,
            Err(_) => continue,
        }
    }
    return offset;
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
        let msgs = read_log(&state.kv_store_lin, log_name, offset, MAX_ITEMS_PER_POLL).await;
        res.insert(log_name.clone(), json!(msgs));
    }

    json!(res)
}

async fn process_commited_offsets(state: &State, msg: Message) {
    let offsets = msg.body["offsets"].as_object().unwrap();
    for (log_name, val) in offsets {
        let new_offset = val.as_u64().unwrap();
        let committed_key = format!("{log_name}_committed");

        loop {
            let current = match state.kv_store_lin.read(&committed_key).await {
                Ok(val) => val,
                Err(KeyNotFound) => json!(0),
                Err(_) => panic!(),
            };

            if new_offset <= current.as_u64().unwrap() {
                break;
            }

            match state.kv_store_lin.cas(&committed_key, &current, &json!(new_offset), true).await {
                Ok(()) => break,
                Err(ExpectationMismatch) => continue,
                Err(_) => panic!(),
            }
        }
    }
}

async fn process_list_commited_offsets(state: &State, msg: Message) -> Value {
    let mut res = serde_json::Map::new();

    let keys = msg.body["keys"].as_array().unwrap();
    for key in keys {
        let log_name = key.as_str().unwrap();
        let committed_key = format!("{log_name}_committed");
        let offset = match state.kv_store_lin.read(&committed_key).await {
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
    let state = Arc::new(State {
        kv_store_lin: KVStore {
            name: "lin-kv".to_string(),
            node: node.clone(),
        },
    });

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
