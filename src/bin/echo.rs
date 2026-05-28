use flyio::Node;
use serde_json::json;

fn main() {
    Node::run(|_node, msg| match msg.msg_type {
        "echo" => {
            log::info!("echoing back: {}", msg.body["echo"]);
            Some(json!({ "type": "echo_ok", "echo": msg.body["echo"] }))
        }
        other => {
            log::warn!("unknown message type: {other}");
            None
        }
    });
}
