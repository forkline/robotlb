//! Detect node changes that actually affect Hetzner target selection.
//! Kubelet heartbeats must not trigger reconciling every `LoadBalancer` Service.

use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
};

use k8s_openapi::api::core::v1::Node;
use kube::{runtime::watcher::Event, ResourceExt};

#[derive(Debug, Default)]
pub struct NodeChanges {
    seen: HashMap<String, u64>,
    relisted: HashMap<String, u64>,
    synced: bool,
}

impl NodeChanges {
    /// True for a node change affecting targets, including relist/deletions.
    pub fn observe(&mut self, event: &Event<Node>) -> bool {
        match event {
            Event::Init => {
                self.relisted.clear();
                false
            }
            Event::InitApply(node) => {
                self.relisted.insert(node.name_any(), fingerprint(node));
                false
            }
            Event::InitDone => {
                let changed = !self.synced || self.relisted != self.seen;
                self.seen = std::mem::take(&mut self.relisted);
                self.synced = true;
                changed
            }
            Event::Apply(node) => {
                let print = fingerprint(node);
                self.seen.insert(node.name_any(), print) != Some(print)
            }
            Event::Delete(node) => self.seen.remove(&node.name_any()).is_some(),
        }
    }
}

fn fingerprint(node: &Node) -> u64 {
    let mut hasher = DefaultHasher::new();
    node.labels().hash(&mut hasher);
    node.spec
        .as_ref()
        .and_then(|spec| spec.unschedulable)
        .hash(&mut hasher);
    if let Some(status) = &node.status {
        status
            .conditions
            .iter()
            .flatten()
            .find(|condition| condition.type_ == "Ready")
            .map(|condition| condition.status.as_str())
            .hash(&mut hasher);
        for address in status.addresses.iter().flatten() {
            (address.type_.as_str(), address.address.as_str()).hash(&mut hasher);
        }
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::{
        api::core::v1::{NodeAddress, NodeCondition, NodeSpec, NodeStatus},
        apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };

    fn node(name: &str, ready: &str) -> Node {
        Node {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            spec: Some(NodeSpec::default()),
            status: Some(NodeStatus {
                addresses: Some(vec![NodeAddress {
                    type_: "InternalIP".to_string(),
                    address: "192.0.2.10".to_string(),
                }]),
                conditions: Some(vec![NodeCondition {
                    type_: "Ready".to_string(),
                    status: ready.to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn ignores_heartbeats_but_reacts_to_node_target_fields() {
        let mut changes = NodeChanges::default();
        assert!(!changes.observe(&Event::Init));
        assert!(!changes.observe(&Event::InitApply(node("a", "True"))));
        assert!(changes.observe(&Event::InitDone));
        let mut heartbeat = node("a", "True");
        heartbeat
            .status
            .as_mut()
            .unwrap()
            .conditions
            .as_mut()
            .unwrap()[0]
            .message = Some("kubelet heartbeat".to_string());
        assert!(!changes.observe(&Event::Apply(heartbeat)));
        assert!(changes.observe(&Event::Apply(node("a", "False"))));
        assert!(!changes.observe(&Event::Apply(node("a", "False"))));

        let mut cordoned = node("a", "False");
        cordoned.spec.as_mut().unwrap().unschedulable = Some(true);
        assert!(changes.observe(&Event::Apply(cordoned.clone())));
        cordoned
            .status
            .as_mut()
            .unwrap()
            .addresses
            .as_mut()
            .unwrap()[0]
            .address = "192.0.2.11".to_string();
        assert!(changes.observe(&Event::Apply(cordoned)));
        assert!(changes.observe(&Event::Delete(node("a", "False"))));
    }

    #[test]
    fn relist_and_new_node_detection() {
        let mut changes = NodeChanges::default();
        changes.observe(&Event::Init);
        changes.observe(&Event::InitApply(node("a", "True")));
        assert!(changes.observe(&Event::InitDone));
        changes.observe(&Event::Init);
        changes.observe(&Event::InitApply(node("a", "True")));
        assert!(!changes.observe(&Event::InitDone));
        changes.observe(&Event::Init);
        changes.observe(&Event::InitApply(node("a", "False")));
        assert!(changes.observe(&Event::InitDone));
        assert!(changes.observe(&Event::Apply(node("b", "True"))));
    }
}
