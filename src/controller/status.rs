//! Service status management.
//!
//! This module provides functions for updating the status of Kubernetes
//! services, particularly for setting the load balancer ingress status.

use k8s_openapi::{api::core::v1::{LoadBalancerIngress, Service}, serde_json::json};
use kube::{ResourceExt, api::Patch, api::PatchParams};

use crate::CurrentContext;
use crate::error::RobotLBResult;

/// Build ingress status entries from a Hetzner Cloud load balancer.
#[must_use]
pub fn build_ingress(
    hcloud_lb: &hcloud::models::LoadBalancer,
    enable_ipv6: bool,
    proxy_mode: bool,
    public_interface: bool,
) -> Vec<LoadBalancerIngress> {
    let ip_mode = if proxy_mode { "Proxy" } else { "VIP" };
    let mut ingress = vec![];

    if !public_interface {
        for private_net in &hcloud_lb.private_net {
            if let Some(ip) = &private_net.ip {
                ingress.push(LoadBalancerIngress {
                    ip: Some(ip.clone()),
                    ip_mode: Some(ip_mode.to_string()),
                    ..Default::default()
                });
            }
        }
        return ingress;
    }

    let ipv4 = hcloud_lb.public_net.ipv4.ip.clone().flatten();
    let ipv6 = hcloud_lb.public_net.ipv6.ip.clone().flatten();

    if let Some(ipv4) = &ipv4 {
        ingress.push(LoadBalancerIngress {
            ip: Some(ipv4.clone()),
            ip_mode: Some(ip_mode.to_string()),
            ..Default::default()
        });
    }

    if enable_ipv6 && let Some(ipv6) = &ipv6 {
        ingress.push(LoadBalancerIngress {
            ip: Some(ipv6.clone()),
            ip_mode: Some(ip_mode.to_string()),
            ..Default::default()
        });
    }

    ingress
}

/// Compare structured Kubernetes status, not intermediary provider JSON.
fn ingress_is_current(svc: &Service, ingress: &[LoadBalancerIngress]) -> bool {
    svc.status
        .as_ref()
        .and_then(|status| status.load_balancer.as_ref())
        .and_then(|balancer| balancer.ingress.as_ref())
        .is_some_and(|entries| entries == ingress)
}

/// Patch the service status with the load balancer ingress.
pub async fn patch_ingress_status(
    svc: &Service,
    context: &CurrentContext,
    ingress: Vec<LoadBalancerIngress>,
) -> RobotLBResult<()> {
    if ingress.is_empty() {
        return Ok(());
    }

    // A status-only watch event must not trigger an identical status write,
    // otherwise two controllers can amplify a reconciliation loop.
    if ingress_is_current(svc, &ingress) {
        return Ok(());
    }

    let svc_api = kube::Api::<Service>::namespaced(
        context.client.clone(),
        svc.namespace()
            .unwrap_or_else(|| context.client.default_namespace().to_string())
            .as_str(),
    );

    svc_api
        .patch_status(
            svc.name_any().as_str(),
            &PatchParams::default(),
            &Patch::Merge(json!({
                "status": {
                    "loadBalancer": {
                        "ingress": ingress
                    }
                }
            })),
        )
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_load_balancer() -> hcloud::models::LoadBalancer {
        hcloud::models::LoadBalancer {
            public_net: Box::new(hcloud::models::LoadBalancerPublicNet {
                ipv4: Box::new(hcloud::models::LoadBalancerPublicNetIpv4 {
                    ip: Some(Some("203.0.113.10".to_string())),
                    dns_ptr: Some(Some("public.example.com".to_string())),
                }),
                ipv6: Box::new(hcloud::models::LoadBalancerPublicNetIpv6 {
                    ip: Some(Some("2001:db8::10".to_string())),
                    dns_ptr: Some(Some("public-v6.example.com".to_string())),
                }),
                ..Default::default()
            }),
            private_net: vec![hcloud::models::LoadBalancerPrivateNet {
                ip: Some("10.10.0.5".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn build_ingress_uses_public_addresses_when_enabled() {
        let ingress = build_ingress(&test_load_balancer(), true, false, true);

        assert_eq!(
            ingress,
            vec![
                LoadBalancerIngress { ip: Some("203.0.113.10".to_string()), ip_mode: Some("VIP".to_string()), ..Default::default() },
                LoadBalancerIngress { ip: Some("2001:db8::10".to_string()), ip_mode: Some("VIP".to_string()), ..Default::default() }
            ]
        );
    }

    #[test]
    fn unchanged_ingress_does_not_require_a_status_patch() {
        use k8s_openapi::api::core::v1::{LoadBalancerStatus, ServiceStatus};

        let ingress = build_ingress(&test_load_balancer(), false, false, true);
        let mut svc = Service {
            status: Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: Some(ingress.clone()),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(ingress_is_current(&svc, &ingress));

        let changed = build_ingress(&test_load_balancer(), true, false, true);
        assert!(!ingress_is_current(&svc, &changed));
        svc.status = None;
        assert!(!ingress_is_current(&svc, &ingress));
    }

    #[test]
    fn ingress_uses_kubernetes_serializable_fields() {
        let ingress = build_ingress(&test_load_balancer(), false, false, true);
        let value = k8s_openapi::serde_json::to_value(&ingress).expect("serialize");
        assert_eq!(value, json!([{"ip": "203.0.113.10", "ipMode": "VIP"}]));
        let parsed: Vec<LoadBalancerIngress> = k8s_openapi::serde_json::from_value(value).expect("roundtrip");
        assert_eq!(ingress, parsed);
    }

    #[test]
    fn build_ingress_uses_private_address_when_public_interface_is_disabled() {
        let ingress = build_ingress(&test_load_balancer(), true, false, false);

        assert_eq!(
            ingress,
            vec![LoadBalancerIngress { ip: Some("10.10.0.5".to_string()), ip_mode: Some("VIP".to_string()), ..Default::default() }]
        );
    }
}
