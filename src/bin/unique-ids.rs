use flyio::Node;
use serde_json::json;

fn main() {
    Node::run(|_node, msg_type, _body| match msg_type {
        "generate" => {
            // TODO: generate a unique ID
            Some(json!({ "type": "generate_ok", "id": "TODO" }))
        }
        other => {
            log::warn!("unknown message type: {other}");
            None
        }
    });
}
