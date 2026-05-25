use flyio::Node;
use serde_json::json;

fn main() {
    Node::run(|_node, msg_type, body| match msg_type {
        "echo" => {
            log::info!("echoing back: {}", body["echo"]);
            Some(json!({ "type": "echo_ok", "echo": body["echo"] }))
        }
        other => {
            log::warn!("unknown message type: {other}");
            None
        }
    });
}
