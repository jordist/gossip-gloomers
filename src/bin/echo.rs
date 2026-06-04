use flyio::Node;
use serde_json::json;

#[tokio::main]
async fn main() {
    Node::new()
        .run(|_node, msg| async move {
            match msg.msg_type.as_str() {
                "echo" => {
                    log::info!("echoing back: {}", msg.body["echo"]);
                    Some(json!({ "type": "echo_ok", "echo": msg.body["echo"] }))
                }
                other => {
                    log::warn!("unknown message type: {other}");
                    None
                }
            }
        })
        .await;
}
