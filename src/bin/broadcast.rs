use flyio::{Node, NodeHandler};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

const BROADCAST_TIMEOUT: Duration = Duration::from_secs(1);

struct BroadcastItem {
    message: serde_json::Value,
    destination: String,
    sent_time: std::time::Instant,
}

struct Broadcaster {
    my_neighbors: Vec<String>,
    pending_acks: HashMap<u64, BroadcastItem>,
}

impl Broadcaster {
    fn new(neighbors: &Vec<String>) -> Self {
        Self {
            my_neighbors: neighbors.clone(),
            pending_acks: HashMap::new(),
        }
    }

    fn broadcast(
        &mut self,
        node: &mut flyio::Node,
        message: &serde_json::Value,
        except: Option<&str>,
    ) {
        for neighbor in &self.my_neighbors {
            if let Some(except_id) = except {
                if neighbor == except_id {
                    continue;
                }
            }
            let id = node.send(neighbor, json!({ "type": "broadcast", "message": message }));
            self.pending_acks.insert(
                id,
                BroadcastItem {
                    message: message.clone(),
                    destination: neighbor.clone(),
                    sent_time: std::time::Instant::now(),
                },
            );
        }
    }

    fn handle_ack(&mut self, id: u64) {
        self.pending_acks.remove(&id);
    }

    fn tick(&mut self, node: &mut flyio::Node) {
        let to_retry: Vec<(u64, serde_json::Value, String)> = self
            .pending_acks
            .iter()
            .filter(|(_, item)| item.sent_time.elapsed() > BROADCAST_TIMEOUT)
            .map(|(id, item)| (*id, item.message.clone(), item.destination.clone()))
            .collect();

        for (old_id, message, destination) in to_retry {
            log::info!("Retrying broadcast id={old_id} to {destination}: {message}");
            let new_id = node.send(
                &destination,
                json!({ "type": "broadcast", "message": message }),
            );
            self.pending_acks.remove(&old_id);
            self.pending_acks.insert(
                new_id,
                BroadcastItem {
                    message,
                    destination,
                    sent_time: std::time::Instant::now(),
                },
            );
        }
    }
}

struct BroadcastHandler {
    seen_messages: HashSet<serde_json::Value>,
    broadcaster: Option<Broadcaster>,
}

impl BroadcastHandler {
    fn new() -> Self {
        Self {
            seen_messages: HashSet::new(),
            broadcaster: None,
        }
    }
}

impl NodeHandler for BroadcastHandler {
    fn handle(&mut self, node: &mut Node, msg: &flyio::Message) -> Option<serde_json::Value> {
        match msg.msg_type {
            "broadcast" => {
                let body = &msg.body["message"];
                let inserted = self.seen_messages.insert(body.clone());
                if inserted {
                    self.broadcaster
                        .as_mut()
                        .unwrap()
                        .broadcast(node, body, Some(msg.src));
                }
                Some(json!({ "type": "broadcast_ok" }))
            }
            "read" => {
                let messages_arr = self.seen_messages.iter().cloned().collect::<Vec<_>>();
                Some(json!({ "type": "read_ok", "messages": messages_arr }))
            }
            "topology" => {
                let neighbors: Vec<String> = msg.body["topology"].as_object().unwrap()
                    [node.id.as_str()]
                .as_array()
                .unwrap()
                .iter()
                .map(|x| x.as_str().unwrap().to_string())
                .collect();
                self.broadcaster = Some(Broadcaster::new(&neighbors));
                Some(json!({ "type": "topology_ok" }))
            }
            "broadcast_ok" => {
                let id = msg.body["in_reply_to"].as_u64().unwrap();
                self.broadcaster.as_mut().unwrap().handle_ack(id);
                None
            }
            other => {
                log::warn!("unknown message type: {other}");
                None
            }
        }
    }

    fn tick(&mut self, node: &mut Node) {
        if let Some(b) = self.broadcaster.as_mut() {
            b.tick(node);
        }
    }
}

fn main() {
    Node::run_with_tick(BroadcastHandler::new(), Duration::from_millis(100));
}
