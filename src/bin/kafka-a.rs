use flyio::{Message, Node};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

type LogId = String;
type Offset = u64;
type Log = BTreeMap<Offset, Value>;

struct LogHolder {
    log: Log,
    last_offset: Offset,
    commited_offset: Offset,
}

struct State {
    logs: HashMap<LogId, LogHolder>,
}

fn process_send(state: &mut State, msg: Message) -> Offset {
    let log_id = msg.body["key"].as_str().unwrap().to_string();
    let val = &msg.body["msg"];

    let log_holder = state.logs.entry(log_id).or_insert(LogHolder {
        log: Log::new(),
        last_offset: 0,
        commited_offset: 0,
    });
    let offset = log_holder.last_offset + 1;
    log_holder.log.insert(offset, val.clone());
    log_holder.last_offset = offset;
    offset
}

fn process_poll(state: &State, msg: Message) -> Value {
    let mut res = serde_json::Map::new();

    let offsets = msg.body["offsets"].as_object().unwrap();
    for (log_name, val) in offsets {
        let offset = val.as_u64().unwrap();

        let msgs: Vec<Value> = if let Some(log_holder) = state.logs.get(log_name) {
            log_holder
                .log
                .range(offset..)
                .map(|(off, v)| json!([off, v]))
                .collect()
        } else {
            vec![]
        };

        res.insert(log_name.clone(), json!(msgs));
    }

    json!(res)
}

fn process_commited_offsets(state: &mut State, msg: Message) {
    let offsets = msg.body["offsets"].as_object().unwrap();
    for (log_name, val) in offsets {
        let offset = val.as_u64().unwrap();
        if let Some(log_holder) = state.logs.get_mut(log_name) {
            assert!(offset <= log_holder.last_offset);
            log_holder.commited_offset = offset;
        }
    }
}

fn process_list_commited_offsets(state: &State, msg: Message) -> Value {
    let mut res = serde_json::Map::new();

    let keys = msg.body["keys"].as_array().unwrap();
    for key in keys {
        let offset = state
            .logs
            .get(key.as_str().unwrap())
            .map_or(0, |lh| lh.commited_offset);
        res.insert(key.to_string(), json!(offset));
    }

    json!(res)
}

#[tokio::main]
async fn main() {
    let node = Node::new();
    let state = Arc::new(Mutex::new(State {
        logs: HashMap::new(),
    }));

    node.run(move |_node, msg| {
        let state = Arc::clone(&state);
        async move {
            match msg.msg_type.as_str() {
                "send" => {
                    let offset = process_send(&mut state.lock().unwrap(), msg);
                    Some(json!({"type": "send_ok", "offset": offset}))
                }
                "poll" => {
                    let msgs = process_poll(&state.lock().unwrap(), msg);
                    Some(json!({"type": "poll_ok", "msgs": msgs}))
                }
                "commit_offsets" => {
                    process_commited_offsets(&mut state.lock().unwrap(), msg);
                    Some(json!({"type": "commit_offsets_ok"}))
                }
                "list_committed_offsets" => {
                    let offsets = process_list_commited_offsets(&state.lock().unwrap(), msg);
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
