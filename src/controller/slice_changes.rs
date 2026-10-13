//! Track target-relevant EndpointSlice changes without reconciling on every
//! endpoint readiness or address update. A second metadata-only watch detects
//! deletions, which kube's controller watch mapper cannot identify.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{Mutex, PoisonError},
};

use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::{Resource, ResourceExt, runtime::watcher::Event};

type SliceKey = (String, String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SliceTargetMode {
    /// For selector-based Local Services, actual targets are based on Pod
    /// placement, not on the readiness of individual endpoints.
    PodNodes,
    /// Selectorless Services use ready EndpointSlice nodes for targets.
    ReadyEndpointNodes,
}

fn key<K: Resource>(slice: &K) -> SliceKey {
    (slice.namespace().unwrap_or_default(), slice.name_any())
}

fn target_nodes(slice: &EndpointSlice, mode: SliceTargetMode) -> BTreeSet<String> {
    slice
        .endpoints
        .iter()
        .filter(|endpoint| {
            mode == SliceTargetMode::PodNodes
                || endpoint
                    .conditions
                    .as_ref()
                    .and_then(|conditions| conditions.ready)
                    != Some(false)
        })
        .filter_map(|endpoint| endpoint.node_name.clone())
        .collect()
}

#[derive(Debug, Default)]
struct State {
    observed: HashMap<SliceKey, BTreeSet<String>>,
    live: HashSet<SliceKey>,
    relisted: HashSet<SliceKey>,
}

/// A shared cache for the controller's EndpointSlice mapper and the deletion watch.
#[derive(Debug, Default)]
pub(super) struct SliceChanges {
    state: Mutex<State>,
}

impl SliceChanges {
    /// True when this slice's relevant node set changed or first appeared.
    pub fn observe(&self, slice: &EndpointSlice, mode: SliceTargetMode) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let next = target_nodes(slice, mode);
        state.observed.insert(key(slice), next.clone()) != Some(next)
    }

    /// Remove stale snapshots when a Service no longer selects this resource.
    pub fn forget(&self, slice: &EndpointSlice) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observed
            .remove(&key(slice));
    }

    /// True when a previously observed slice with target nodes disappears.
    ///
    /// A metadata-only watch avoids transferring full endpoint lists twice.
    pub fn track<K: Resource>(&self, event: &Event<K>) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match event {
            Event::Init => {
                state.relisted.clear();
                false
            }
            Event::InitApply(slice) => {
                state.relisted.insert(key(slice));
                false
            }
            Event::InitDone => {
                let live = std::mem::take(&mut state.relisted);
                let removed = state.observed.iter().any(|(name, nodes)| {
                    !live.contains(name) && !nodes.is_empty()
                });
                state.observed.retain(|name, _| live.contains(name));
                state.live = live;
                removed
            }
            Event::Apply(slice) => {
                state.live.insert(key(slice));
                false
            }
            Event::Delete(slice) => {
                let name = key(slice);
                state.live.remove(&name);
                state
                    .observed
                    .remove(&name)
                    .is_some_and(|nodes| !nodes.is_empty())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::{
        api::discovery::v1::{Endpoint, EndpointConditions},
        apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };

    fn slice(ready: bool, node: &str) -> EndpointSlice {
        EndpointSlice {
            metadata: ObjectMeta {
                name: Some("web-abc".to_string()),
                namespace: Some("shop".to_string()),
                ..Default::default()
            },
            endpoints: vec![Endpoint {
                addresses: vec!["192.0.2.10".to_string()],
                node_name: Some(node.to_string()),
                conditions: Some(EndpointConditions {
                    ready: Some(ready),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn pod_placement_mode_ignores_readiness_and_address_churn() {
        let changes = SliceChanges::default();
        let ready = slice(true, "node-a");
        assert!(changes.observe(&ready, SliceTargetMode::PodNodes));
        let mut unready = slice(false, "node-a");
        unready.endpoints[0].addresses = vec!["192.0.2.11".to_string()];
        assert!(!changes.observe(&unready, SliceTargetMode::PodNodes));
        assert!(!changes.observe(&ready, SliceTargetMode::PodNodes));
        assert!(changes.observe(&slice(true, "node-b"), SliceTargetMode::PodNodes));
    }

    #[test]
    fn selectorless_mode_tracks_last_ready_endpoint_on_each_node() {
        let changes = SliceChanges::default();
        assert!(changes.observe(&slice(true, "node-a"), SliceTargetMode::ReadyEndpointNodes));
        assert!(changes.observe(&slice(false, "node-a"), SliceTargetMode::ReadyEndpointNodes));
        assert!(!changes.observe(&slice(false, "node-a"), SliceTargetMode::ReadyEndpointNodes));
        assert!(changes.observe(&slice(true, "node-a"), SliceTargetMode::ReadyEndpointNodes));
    }

    #[test]
    fn a_deleted_target_slice_triggers_reconciliation() {
        let changes = SliceChanges::default();
        let initial = slice(true, "node-a");
        changes.observe(&initial, SliceTargetMode::ReadyEndpointNodes);
        assert!(!changes.track(&Event::<EndpointSlice>::Init));
        assert!(!changes.track(&Event::InitApply(initial.clone())));
        assert!(!changes.track(&Event::<EndpointSlice>::InitDone));
        assert!(changes.track(&Event::Delete(initial.clone())));
        assert!(!changes.track(&Event::Delete(initial)));
    }

    #[test]
    fn relisting_detects_deleted_slices_even_if_deletion_event_was_lost() {
        let changes = SliceChanges::default();
        let initial = slice(true, "node-a");
        changes.observe(&initial, SliceTargetMode::ReadyEndpointNodes);
        changes.track(&Event::<EndpointSlice>::Init);
        changes.track(&Event::InitApply(initial));
        assert!(!changes.track(&Event::<EndpointSlice>::InitDone));
        assert!(!changes.track(&Event::<EndpointSlice>::Init));
        assert!(changes.track(&Event::<EndpointSlice>::InitDone));
    }

    #[test]
    fn unobserved_and_empty_slices_do_not_trigger_delete_signals() {
        let changes = SliceChanges::default();
        let initial = slice(false, "node-a");
        changes.observe(&initial, SliceTargetMode::ReadyEndpointNodes);
        assert!(!changes.track(&Event::Delete(initial)));
    }

    #[test]
    fn forgetting_a_slice_restores_first_observation() {
        let changes = SliceChanges::default();
        let initial = slice(true, "node-a");
        assert!(changes.observe(&initial, SliceTargetMode::PodNodes));
        changes.forget(&initial);
        assert!(changes.observe(&initial, SliceTargetMode::PodNodes));
    }
}
