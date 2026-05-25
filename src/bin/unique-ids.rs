use flyio::Node;
use serde_json::json;

fn main() {
    let mut counter = 0;
    Node::run(|node: &mut flyio::Node, msg_type, _body| match msg_type {
        "generate" => {
            let unique_id = format!("{}-{}", node.id, counter);
            counter += 1;
            Some(json!({ "type": "generate_ok", "id": unique_id }))
        }
        other => {
            log::warn!("unknown message type: {other}");
            None
        }
    });
}
