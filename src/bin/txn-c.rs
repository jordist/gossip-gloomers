use core::panic;
use std::{collections::HashMap, sync::Arc, time::Duration};

use flyio::{Message, Node};
use serde_json::{json, Value};

type Key = u64;
type Val = u64;

const RPC_TIMEOUT: Duration = Duration::from_secs(1);

struct State {
    db: std::sync::Mutex<HashMap<Key, Val>>,
}

fn process_read(state: &State, key: Key) -> Option<Val> {
    return state.db.lock().unwrap().get(&key).copied();
}

fn commit_writes(state: &State, write_buffer: &[(Key, Val)]) {
    let mut db = state.db.lock().unwrap();
    for &(k, v) in write_buffer {
        db.insert(k, v);
    }
}

async fn schedule_replicate_txn(node: Node, writes: Vec<(Key, Val)>) {
    let node_id = node.id().await;
    for neighbour in node.node_ids().await {
        if neighbour == node_id {
            continue;
        }
        let node = node.clone();
        let writes = writes.clone();
        tokio::spawn(async move {
            loop {
                let res = node
                    .rpc_timeout(
                        &neighbour,
                        json!({"type": "repl", "repl": writes}),
                        RPC_TIMEOUT,
                    )
                    .await;
                if res.is_some() {
                    break;
                }
            }
        });
    }
}

async fn process_txn(state: &State, msg: &Message, node: Node) -> Value {
    let mut res: Vec<Value> = Vec::new();
    let mut write_buffer: Vec<(Key, Val)> = Vec::new();

    let txn = msg.body["txn"].as_array().unwrap();
    for stmt in txn {
        let stmt_arr = stmt.as_array().unwrap();
        let op = stmt_arr[0].as_str().unwrap();
        let arg1 = stmt_arr[1].as_u64().unwrap();
        let arg2 = stmt_arr[2].as_u64();

        match op {
            "r" => {
                let val = process_read(state, arg1);
                res.push(json!(["r", arg1, val]));
            }
            "w" => {
                let val = arg2.unwrap();
                write_buffer.push((arg1, val));
                res.push(json!(["w", arg1, val]));
            }
            _ => {
                panic!();
            }
        }
    }

    commit_writes(state, &write_buffer);
    schedule_replicate_txn(node, write_buffer).await;
    serde_json::Value::Array(res)
}

fn process_repl(state: &State, msg: &Message) {
    let mut db = state.db.lock().unwrap();
    for repl in msg.body["repl"].as_array().unwrap() {
        let [key, val] = repl.as_array().unwrap().as_slice() else {
            panic!()
        };
        db.insert(key.as_u64().unwrap(), val.as_u64().unwrap());
    }
}

#[tokio::main]
async fn main() {
    let node = Node::new();
    let state = Arc::new(State {
        db: std::sync::Mutex::new(HashMap::new()),
    });

    node.run(move |node, msg| {
        let state = Arc::clone(&state);
        async move {
            match msg.msg_type.as_str() {
                "txn" => {
                    let res = process_txn(&state, &msg, node).await;
                    Some(json!({"type": "txn_ok", "txn": res}))
                }
                "repl" => {
                    process_repl(&state, &msg);
                    Some(json!({"type": "repl_ok"}))
                }
                other => {
                    panic!("unknown message type: {other}");
                }
            }
        }
    })
    .await;
}
