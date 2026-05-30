use flyio::{Node, NodeHandler};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

const BROADCAST_TIMEOUT: Duration = Duration::from_secs(1);
const POLICY: PolicyKind = PolicyKind::Group;
const GROUP_SIZE: usize = 5; // members per group for GroupPolicy

// Computes the next hops for a broadcast message based on the incoming metadata.
// Returns a list of (destination, metadata) pairs to forward to. The metadata is opaque to the
// broadcaster.
trait ForwardPolicy {
    fn next_hops(&self, incoming_meta: &serde_json::Value) -> Vec<(String, serde_json::Value)>;
}

// ---------------------------------------------------------------------------
// Simple policy. Forwards to all neighbors except the immediate sender.
struct SimplePolicy {
    node_id: String,
    neighbors: Vec<String>,
}

impl SimplePolicy {
    fn new(neighbors: Vec<String>, node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            neighbors,
        }
    }
}

impl ForwardPolicy for SimplePolicy {
    fn next_hops(&self, incoming_meta: &serde_json::Value) -> Vec<(String, serde_json::Value)> {
        let sender = incoming_meta.as_str().unwrap_or("");
        self.neighbors
            .iter()
            .filter(|n| n.as_str() != sender)
            .map(|n| (n.clone(), json!(self.node_id)))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Direct broadcast. The first recipient of a client message blasts it to every
// other node in the cluster simultaneously (1 hop).
struct DirectPolicy {
    node_id: String,
    all_others: Vec<String>,
}

impl DirectPolicy {
    fn new(topology: &HashMap<String, Vec<String>>, node_id: &str) -> Self {
        let mut all_others: Vec<String> = topology.keys()
            .filter(|n| n.as_str() != node_id)
            .cloned()
            .collect();
        all_others.sort();
        Self { node_id: node_id.to_string(), all_others }
    }
}

impl ForwardPolicy for DirectPolicy {
    fn next_hops(&self, incoming_meta: &serde_json::Value) -> Vec<(String, serde_json::Value)> {
        if incoming_meta.as_str().is_some() {
            // Arrived via a node relay — originator already covered everyone.
            vec![]
        } else {
            // First receipt from a client (meta = null): blast to all.
            self.all_others.iter()
                .map(|n| (n.clone(), json!(self.node_id)))
                .collect()
        }
    }
}

// ---------------------------------------------------------------------------
// Group broadcast policy
// Nodes are divided into groups of GROUP_SIZE. On first receipt (meta=null), the entry node
// forwards to its own group ("intra") and to one representative per other group ("inter").
// Representatives forward to their group ("intra"). Intra-receivers do not forward.
struct GroupPolicy {
    node_id: String,
    group_members: Vec<String>,    // other nodes in our group
    other_group_reps: Vec<String>, // first node of every other group
}

impl GroupPolicy {
    fn new(topology: &HashMap<String, Vec<String>>, node_id: &str) -> Self {
        let mut nodes = topology.keys().collect::<Vec<_>>();
        nodes.sort();
        let num_nodes = nodes.len();

        let our_idx = nodes.iter().position(|&n| n == node_id).unwrap();
        let our_group = our_idx / GROUP_SIZE;
        let group_start = our_group * GROUP_SIZE;
        let group_end = (group_start + GROUP_SIZE).min(num_nodes);

        let group_members = nodes[group_start..group_end]
            .iter()
            .filter(|&&n| n != node_id)
            .map(|n| n.to_string())
            .collect();

        let other_group_reps = (0..)
            .map(|g| g * GROUP_SIZE)
            .take_while(|&start| start < num_nodes)
            .filter(|&start| start / GROUP_SIZE != our_group)
            .map(|start| nodes[start].to_string())
            .collect();

        Self { node_id: node_id.to_string(), group_members, other_group_reps }
    }
}

impl ForwardPolicy for GroupPolicy {
    fn next_hops(&self, incoming_meta: &serde_json::Value) -> Vec<(String, serde_json::Value)> {
        match incoming_meta.as_str() {
            Some("intra") => vec![],
            Some("inter") => self.group_members.iter()
                .map(|n| (n.clone(), json!("intra")))
                .collect(),
            _ => {
                // entry point: fan out to own group and one rep per other group
                let mut hops: Vec<(String, serde_json::Value)> = self.group_members.iter()
                    .map(|n| (n.clone(), json!("intra")))
                    .collect();
                hops.extend(self.other_group_reps.iter().map(|n| (n.clone(), json!("inter"))));
                hops
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Policy factory:
#[derive(Clone, Copy)]
enum PolicyKind {
    Simple,
    Direct,
    Group,
}

fn make_policy(
    kind: PolicyKind,
    topology: &HashMap<String, Vec<String>>,
    node_id: &str,
) -> Box<dyn ForwardPolicy> {
    match kind {
        PolicyKind::Simple => {
            let neighbors = topology.get(node_id).cloned().unwrap_or_default();
            Box::new(SimplePolicy::new(neighbors, node_id))
        }
        PolicyKind::Direct => Box::new(DirectPolicy::new(topology, node_id)),
        PolicyKind::Group => Box::new(GroupPolicy::new(topology, node_id)),
    }
}

// ---------------------------------------------------------------------------
// Broadcaster machinery
struct BroadcastItem {
    message: serde_json::Value,
    destination: String,
    sent_time: std::time::Instant,
    meta: serde_json::Value,
}

struct Broadcaster {
    policy: Box<dyn ForwardPolicy>,
    pending_acks: HashMap<u64, BroadcastItem>,
}

impl Broadcaster {
    fn new(policy: Box<dyn ForwardPolicy>) -> Self {
        Self {
            policy,
            pending_acks: HashMap::new(),
        }
    }

    fn broadcast(
        &mut self,
        node: &mut flyio::Node,
        message: &serde_json::Value,
        incoming_meta: &serde_json::Value,
    ) {
        for (dest, meta) in self.policy.next_hops(incoming_meta) {
            let id = node.send(
                &dest,
                json!({ "type": "broadcast", "message": message, "meta": meta }),
            );
            self.pending_acks.insert(
                id,
                BroadcastItem {
                    message: message.clone(),
                    destination: dest,
                    sent_time: std::time::Instant::now(),
                    meta,
                },
            );
        }
    }

    fn handle_ack(&mut self, id: u64) {
        self.pending_acks.remove(&id);
    }

    fn tick(&mut self, node: &mut flyio::Node) {
        let to_retry: Vec<(u64, serde_json::Value, String, serde_json::Value)> = self
            .pending_acks
            .iter()
            .filter(|(_, item)| item.sent_time.elapsed() > BROADCAST_TIMEOUT)
            .map(|(id, item)| {
                (
                    *id,
                    item.message.clone(),
                    item.destination.clone(),
                    item.meta.clone(),
                )
            })
            .collect();

        for (old_id, message, destination, meta) in to_retry {
            let new_id = node.send(
                &destination,
                json!({ "type": "broadcast", "message": message, "meta": meta }),
            );
            self.pending_acks.remove(&old_id);
            self.pending_acks.insert(
                new_id,
                BroadcastItem {
                    message,
                    destination,
                    sent_time: std::time::Instant::now(),
                    meta,
                },
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Server handler:
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
                        .broadcast(node, body, &msg.body["meta"]);
                }
                Some(json!({ "type": "broadcast_ok" }))
            }
            "read" => {
                let messages: Vec<_> = self.seen_messages.iter().cloned().collect();
                Some(json!({ "type": "read_ok", "messages": messages }))
            }
            "topology" => {
                let topology: HashMap<String, Vec<String>> = msg.body["topology"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| {
                        let neighbors = v
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|x| x.as_str().unwrap().to_string())
                            .collect();
                        (k.clone(), neighbors)
                    })
                    .collect();
                self.broadcaster = Some(Broadcaster::new(make_policy(POLICY, &topology, &node.id)));
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
