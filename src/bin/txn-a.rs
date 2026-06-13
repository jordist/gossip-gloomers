use std::{collections::HashMap, sync::Arc};

use flyio::{Message, Node};
use serde_json::{json, Value};

type Key = u64;
type Val = u64;

struct State {
    db: std::sync::Mutex<HashMap<Key, Val>>,
}

fn process_read(state: &State, key: Key) -> Option<Val> {
    return state.db.lock().unwrap().get(&key).copied();
}

fn process_write(state: &State, key: Key, val: Val) {
    state.db.lock().unwrap().insert(key, val);
}

fn process_txn(state: &State, msg: &Message) -> Value {
    let mut res: Vec<Value> = Vec::new();

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
                process_write(state, arg1, arg2.unwrap());
                res.push(json!(["w", arg1, arg2]));
            }
            _ => {
                panic!();
            }
        }
    }
    serde_json::Value::Array(res)
}

#[tokio::main]
async fn main() {
    let node = Node::new();
    let state = Arc::new(State {
        db: std::sync::Mutex::new(HashMap::new()),
    });

    node.run(move |_node, msg| {
        let state = Arc::clone(&state);
        async move {
            match msg.msg_type.as_str() {
                "txn" => {
                    let res = process_txn(&state, &msg);
                    Some(json!({"type": "txn_ok", "txn": res}))
                }
                other => {
                    panic!("unknown message type: {other}");
                }
            }
        }
    })
    .await;
}
