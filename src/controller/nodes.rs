//! Node discovery for load balancer targets.
//!
//! This module provides functions for discovering target nodes based on
//! either pod placement (dynamic) or node selectors (static).

use std::{collections::HashSet, str::FromStr, sync::Arc};

use k8s_openapi::api::{
    core::v1::{Node, Pod, Service},
    discovery::v1::EndpointSlice,
};
use kube::{ResourceExt, api::ListParams};

use crate::{
    CurrentContext, consts,
    error::{RobotLBError, RobotLBResult},
    label_filter::LabelFilter,
    lb::LoadBalancer,
};

/// Determine the IP type to use for targets based on load balancer configuration.
#[must_use]
pub const fn node_ip_type(lb: &LoadBalancer) -> &'static str {
    if lb.network_name.is_none() {
        "ExternalIP"
    } else {
        "InternalIP"
    }
}

/// Extract target IPs from nodes based on the desired IP type.
#[must_use]
pub fn derive_targets(nodes: Vec<Node>, desired_ip_type: &str) -> Vec<String> {
    let mut targets = Vec::new();

    for node in nodes {
        let Some(status) = node.status else {
            continue;
        };
        let Some(addresses) = status.addresses else {
            continue;
        };
        for addr in addresses {
            if addr.type_ == desired_ip_type {
                targets.push(addr.address);
            }
        }
    }

    targets
}

/// Extract services (port mappings) from a Kubernetes service.
#[must_use]
pub fn derive_services(svc: &Service) -> Vec<(i32, i32)> {
    let mut services = Vec::new();

    for port in svc
        .spec
        .as_ref()
        .and_then(|spec| spec.ports.as_ref())
        .cloned()
        .unwrap_or_default()
    {
        let protocol = port.protocol.unwrap_or_else(|| "TCP".to_string());
        if protocol != "TCP" {
            tracing::warn!("Protocol {} is not supported. Skipping...", protocol);
            continue;
        }

        let Some(node_port) = port.node_port else {
            tracing::warn!(
                "Node port is not set for target_port {}. Skipping...",
                port.port
            );
            continue;
        };

        services.push((port.port, node_port));
    }

    services
}

/// Where a Service gets its targets from, according to Kubernetes traffic policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeSource {
    Annotation,
    ServiceEndpoints,
    AllNodes,
}

fn node_source(svc: &Service, dynamic_node_selector: bool) -> NodeSource {
    if !dynamic_node_selector {
        NodeSource::Annotation
    } else if svc
        .spec
        .as_ref()
        .and_then(|spec| spec.external_traffic_policy.as_deref())
        == Some("Local")
    {
        NodeSource::ServiceEndpoints
    } else {
        NodeSource::AllNodes
    }
}

fn excluded_from_lb(node: &Node) -> bool {
    node.labels().contains_key(consts::EXCLUDE_FROM_LB_LABEL)
}

fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.type_ == "Ready")
        })
        .is_some_and(|condition| condition.status == "True")
}

fn eligible_node(node: &Node) -> bool {
    !excluded_from_lb(node)
        && node_ready(node)
        && node.spec.as_ref().and_then(|spec| spec.unschedulable) != Some(true)
}

async fn all_eligible_nodes(context: &Arc<CurrentContext>) -> RobotLBResult<Vec<Node>> {
    let api = kube::Api::<Node>::all(context.client.clone());
    Ok(api
        .list(&ListParams::default())
        .await?
        .into_iter()
        .filter(eligible_node)
        .collect())
}

/// Discover target nodes for a service.
///
/// This method uses either dynamic node selection (based on pod placement)
/// or static node selection (based on node selector annotations).
pub async fn discover_target_nodes(
    svc: &Arc<Service>,
    context: &Arc<CurrentContext>,
) -> RobotLBResult<Vec<Node>> {
    match node_source(svc, context.config.dynamic_node_selector) {
        NodeSource::Annotation => get_nodes_by_selector(svc, context).await,
        NodeSource::ServiceEndpoints => get_nodes_dynamically(svc, context).await,
        NodeSource::AllNodes => all_eligible_nodes(context).await,
    }
}

/// Apply desired state to a load balancer.
pub fn apply_desired_state(
    lb: &mut LoadBalancer,
    desired_targets: Vec<String>,
    desired_services: Vec<(i32, i32)>,
) {
    for target in desired_targets {
        lb.add_target(&target);
    }

    for (listen_port, target_port) in desired_services {
        lb.add_service(listen_port, target_port);
    }
}

/// Get nodes dynamically based on pod placement.
///
/// This method finds the nodes where the target pods are deployed
/// using the pod selector from the service.
async fn get_nodes_dynamically(
    svc: &Arc<Service>,
    context: &Arc<CurrentContext>,
) -> RobotLBResult<Vec<Node>> {
    let pod_api = kube::Api::<Pod>::namespaced(
        context.client.clone(),
        svc.namespace().as_deref().unwrap_or_else(|| context.client.default_namespace()),
    );

    let Some(pod_selector) = svc
        .spec
        .as_ref()
        .and_then(|spec| spec.selector.clone())
        .filter(|selector| !selector.is_empty())
    else {
        return get_nodes_from_endpoint_slices(svc, context).await;
    };

    let label_selector = pod_selector
        .iter()
        .map(|(key, val)| format!("{key}={val}"))
        .collect::<Vec<_>>()
        .join(",");

    let pods = pod_api
        .list(&ListParams {
            label_selector: Some(label_selector),
            ..Default::default()
        })
        .await?;

    let target_nodes = pods
        .iter()
        .filter_map(|pod| pod.spec.clone().unwrap_or_default().node_name)
        .collect::<HashSet<_>>();

    let nodes_api = kube::Api::<Node>::all(context.client.clone());
    let nodes = nodes_api
        .list(&ListParams::default())
        .await?
        .into_iter()
        .filter(|node| {
            target_nodes.contains(&node.name_any()) && !excluded_from_lb(node) && node_ready(node)
        })
        .collect::<Vec<_>>();

    Ok(nodes)
}

/// Resolve selectorless Services through their ready `EndpointSlice` endpoints.
/// Some controllers (e.g. `KubeVirt`) manage endpoints without a Service selector.
async fn get_nodes_from_endpoint_slices(
    svc: &Arc<Service>,
    context: &Arc<CurrentContext>,
) -> RobotLBResult<Vec<Node>> {
    let namespace = svc
        .namespace()
        .unwrap_or_else(|| context.client.default_namespace().to_string());
    let slices_api = kube::Api::<EndpointSlice>::namespaced(context.client.clone(), &namespace);
    let slices = slices_api
        .list(&ListParams {
            label_selector: Some(format!("kubernetes.io/service-name={}", svc.name_any())),
            ..Default::default()
        })
        .await?;
    let target_names: HashSet<_> = slices
        .into_iter()
        .flat_map(|slice| slice.endpoints)
        .filter(|endpoint| {
            endpoint
                .conditions
                .as_ref()
                .and_then(|conditions| conditions.ready)
                != Some(false)
        })
        .filter_map(|endpoint| endpoint.node_name)
        .collect();
    let nodes_api = kube::Api::<Node>::all(context.client.clone());
    Ok(nodes_api
        .list(&ListParams::default())
        .await?
        .into_iter()
        .filter(|node| {
            target_names.contains(&node.name_any()) && !excluded_from_lb(node) && node_ready(node)
        })
        .collect())
}

/// Get nodes based on the node selector annotation.
///
/// This method finds nodes matching the label selector from the
/// service's `robotlb/node-selector` annotation.
async fn get_nodes_by_selector(
    svc: &Arc<Service>,
    context: &Arc<CurrentContext>,
) -> RobotLBResult<Vec<Node>> {
    let node_selector = svc
        .annotations()
        .get(consts::LB_NODE_SELECTOR_ANNOTATION)
        .map(String::as_str)
        .ok_or(RobotLBError::ServiceWithoutSelector)?;
    let label_filter = LabelFilter::from_str(node_selector)?;
    let nodes_api = kube::Api::<Node>::all(context.client.clone());
    let nodes = nodes_api
        .list(&ListParams::default())
        .await?
        .into_iter()
        .filter(|node| {
            label_filter.check(node.labels()) && !excluded_from_lb(node) && node_ready(node)
        })
        .collect::<Vec<_>>();
    Ok(nodes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::fixtures::{service_spec, service_with_spec};
    use k8s_openapi::api::core::v1::{NodeAddress, NodeStatus, ServicePort};

    #[test]
    fn uses_all_nodes_under_cluster_policy() {
        let svc = service_with_spec(service_spec("LoadBalancer", None, vec![]));
        assert_eq!(node_source(&svc, true), NodeSource::AllNodes);
        assert_eq!(node_source(&svc, false), NodeSource::Annotation);
    }

    #[test]
    fn uses_endpoints_under_local_policy() {
        let mut svc = service_with_spec(service_spec("LoadBalancer", None, vec![]));
        svc.spec.as_mut().expect("spec").external_traffic_policy = Some("Local".to_string());
        assert_eq!(node_source(&svc, true), NodeSource::ServiceEndpoints);
    }

    #[test]
    fn excludes_marked_or_not_ready_nodes() {
        let mut node = Node::default();
        assert!(!eligible_node(&node));
        node.status = Some(NodeStatus {
            conditions: Some(vec![k8s_openapi::api::core::v1::NodeCondition {
                type_: "Ready".to_string(),
                status: "True".to_string(),
                ..Default::default()
            }]),
            ..Default::default()
        });
        assert!(eligible_node(&node));
        node.metadata.labels = Some(std::collections::BTreeMap::from([(
            consts::EXCLUDE_FROM_LB_LABEL.to_string(),
            String::new(),
        )]));
        assert!(!eligible_node(&node));
    }

    #[test]
    fn derive_targets_picks_matching_address_type() {
        let nodes = vec![
            Node {
                status: Some(NodeStatus {
                    addresses: Some(vec![
                        NodeAddress {
                            address: "10.0.0.10".to_string(),
                            type_: "InternalIP".to_string(),
                        },
                        NodeAddress {
                            address: "203.0.113.10".to_string(),
                            type_: "ExternalIP".to_string(),
                        },
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Node {
                status: Some(NodeStatus {
                    addresses: Some(vec![NodeAddress {
                        address: "203.0.113.11".to_string(),
                        type_: "ExternalIP".to_string(),
                    }]),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ];

        let targets = derive_targets(nodes, "ExternalIP");
        assert_eq!(targets, vec!["203.0.113.10", "203.0.113.11"]);
    }

    #[test]
    fn derive_services_keeps_only_tcp_node_port_pairs() {
        let svc = service_with_spec(service_spec(
            "LoadBalancer",
            None,
            vec![
                ServicePort {
                    port: 80,
                    protocol: Some("TCP".to_string()),
                    node_port: Some(30080),
                    ..Default::default()
                },
                ServicePort {
                    port: 443,
                    protocol: Some("UDP".to_string()),
                    node_port: Some(30443),
                    ..Default::default()
                },
                ServicePort {
                    port: 8080,
                    protocol: Some("TCP".to_string()),
                    node_port: None,
                    ..Default::default()
                },
            ],
        ));

        let services = derive_services(&svc);
        assert_eq!(services, vec![(80, 30080)]);
    }
}
