use flyio::{Message, Node};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;

const BROADCAST_TIMEOUT: Duration = Duration::from_secs(1);
const POLICY: PolicyKind = PolicyKind::Group;
const EXECUTOR: BroadcastExecutorKind = BroadcastExecutorKind::Immediate;
const GROUP_SIZE: usize = 5; // members per group for GroupPolicy

// Computes the next hops for a broadcast message based on the incoming metadata.
// Returns a list of (destination, metadata) pairs to forward to. The metadata is opaque to the
// broadcaster.
trait ForwardPolicy: Send {
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
        let mut all_others: Vec<String> = topology
            .keys()
            .filter(|n| n.as_str() != node_id)
            .cloned()
            .collect();
        all_others.sort();
        Self {
            node_id: node_id.to_string(),
            all_others,
        }
    }
}

impl ForwardPolicy for DirectPolicy {
    fn next_hops(&self, incoming_meta: &serde_json::Value) -> Vec<(String, serde_json::Value)> {
        if incoming_meta.as_str().is_some() {
            // Arrived via a node relay — originator already covered everyone.
            vec![]
        } else {
            // First receipt from a client (meta = null): blast to all.
            self.all_others
                .iter()
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

        Self {
            group_members,
            other_group_reps,
        }
    }
}

impl ForwardPolicy for GroupPolicy {
    fn next_hops(&self, incoming_meta: &serde_json::Value) -> Vec<(String, serde_json::Value)> {
        match incoming_meta.as_str() {
            Some("intra") => vec![],
            Some("inter") => self
                .group_members
                .iter()
                .map(|n| (n.clone(), json!("intra")))
                .collect(),
            _ => {
                // entry point: fan out to own group and one rep per other group
                let mut hops: Vec<(String, serde_json::Value)> = self
                    .group_members
                    .iter()
                    .map(|n| (n.clone(), json!("intra")))
                    .collect();
                hops.extend(
                    self.other_group_reps
                        .iter()
                        .map(|n| (n.clone(), json!("inter"))),
                );
                hops
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Policy factory:
#[derive(Clone, Copy)]
#[allow(dead_code)]
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
// Broadcast executor: decides how forwarded hops are actually put on the wire.
// `dispatch` is called once per (dest, meta) hop. An executor may send each hop
// immediately or accumulate hops and flush them together later.
trait BroadcastExecutor: Send + Sync {
    fn dispatch(&self, node: &Node, message: Value, dest: String, meta: Value);
}

// ---------------------------------------------------------------------------
// Immediate executor. Each hop is sent right away as its own broadcast RPC,
// retried until acknowledged.
struct ImmediateExecutor;

impl BroadcastExecutor for ImmediateExecutor {
    fn dispatch(&self, node: &Node, message: Value, dest: String, meta: Value) {
        let node = node.clone();
        tokio::spawn(async move {
            let body = json!({ "type": "broadcast", "message": message, "meta": meta });
            while node
                .rpc_timeout(&dest, body.clone(), BROADCAST_TIMEOUT)
                .await
                .is_none()
            {}
        });
    }
}

// ---------------------------------------------------------------------------
// Broadcast executor factory:
#[derive(Clone, Copy)]
#[allow(dead_code)]
enum BroadcastExecutorKind {
    Immediate,
    Batched,
}

fn make_executor(kind: BroadcastExecutorKind) -> Arc<dyn BroadcastExecutor> {
    match kind {
        BroadcastExecutorKind::Immediate => Arc::new(ImmediateExecutor),
        BroadcastExecutorKind::Batched => todo!("batched executor"),
    }
}

// ---------------------------------------------------------------------------
// Node state
struct NodeState {
    seen_messages: HashSet<Value>,
    policy: Option<Box<dyn ForwardPolicy>>,
}

impl NodeState {
    fn new() -> Self {
        Self {
            seen_messages: HashSet::new(),
            policy: None,
        }
    }
}

async fn handle(
    node: Node,
    msg: Message,
    state: Arc<Mutex<NodeState>>,
    broadcast_executor: Arc<dyn BroadcastExecutor>,
) -> Option<Value> {
    let body = msg.body;
    match msg.msg_type.as_str() {
        "broadcast" => {
            let message = body["message"].clone();
            let meta = body["meta"].clone();
            let hops = {
                let mut state = state.lock().await;
                if state.seen_messages.insert(message.clone()) {
                    state
                        .policy
                        .as_ref()
                        .map(|p| p.next_hops(&meta))
                        .unwrap_or_default()
                } else {
                    vec![]
                }
            };
            for (dest, hop_meta) in hops {
                broadcast_executor.dispatch(&node, message.clone(), dest, hop_meta);
            }
            Some(json!({ "type": "broadcast_ok" }))
        }
        "read" => {
            let messages: Vec<_> = state.lock().await.seen_messages.iter().cloned().collect();
            Some(json!({ "type": "read_ok", "messages": messages }))
        }
        "topology" => {
            let topology: HashMap<String, Vec<String>> = body["topology"]
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
            let node_id = node.id().await;
            state.lock().await.policy = Some(make_policy(POLICY, &topology, &node_id));
            Some(json!({ "type": "topology_ok" }))
        }
        other => {
            log::warn!("unknown message type: {other}");
            None
        }
    }
}

#[tokio::main]
async fn main() {
    let state = Arc::new(Mutex::new(NodeState::new()));
    let broadcast_executor = make_executor(EXECUTOR);
    Node::run(move |node, msg| handle(node, msg, state.clone(), broadcast_executor.clone())).await;
}
