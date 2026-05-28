use flyio::Node;
use serde_json::json;
use std::collections::HashSet;

fn broadcast_to_neighbors(node: &mut flyio::Node, neighbors: &Vec<String>, message: &serde_json::Value) {
    for neighbor in neighbors {
        node.send(neighbor, json!({ "type": "broadcast", "message": message }));
    }
}

fn main() {
    let mut seen_messages = HashSet::new();
    let mut my_neighbors = Vec::new();

    Node::run(
        |node: &mut flyio::Node, msg| match msg.msg_type {
            "broadcast" => {
                let body = &msg.body["message"];
                let inserted = seen_messages.insert(body.clone());
                if inserted {
                    broadcast_to_neighbors(node, &my_neighbors, body);
                }
                Some(json!({ "type": "broadcast_ok"}))
            }
            "read" => {
                let messages_arr = seen_messages.iter().cloned().collect::<Vec<_>>();
                Some(json!({"type":"read_ok", "messages": messages_arr}))
            }
            "topology" => {
                my_neighbors = msg.body["topology"].as_object().unwrap()[node.id.as_str()]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|x| x.as_str().unwrap().to_string())
                    .collect();
                Some(json!({"type": "topology_ok"}))
            }
            "broadcast_ok" => {
                // noop for now
                None
            }
            other => {
                log::warn!("unknown message type: {other}");
                None
            }
        },
    );
}
