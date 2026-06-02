use flyio::Node;
use serde_json::json;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[tokio::main]
async fn main() {
    let counter = Arc::new(AtomicU64::new(0));
    Node::run(move |node, msg| {
        let counter = counter.clone();
        async move {
            match msg.msg_type.as_str() {
                "generate" => {
                    let c = counter.fetch_add(1, Ordering::Relaxed);
                    let unique_id = format!("{}-{}", node.id().await, c);
                    Some(json!({ "type": "generate_ok", "id": unique_id }))
                }
                other => {
                    log::warn!("unknown message type: {other}");
                    None
                }
            }
        }
    })
    .await;
}
