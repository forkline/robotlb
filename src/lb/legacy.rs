//! Conservative, one-time proof for adopting load balancers created before UID labels.
//! An LB name by itself is never sufficient to prove ownership.

use std::collections::{HashMap, HashSet};

use hcloud::models::LoadBalancer;
use k8s_openapi::api::core::v1::Service;
use kube::ResourceExt;

use super::config::default_name;
use crate::{config::OperatorConfig, consts, finalizers};

#[derive(Debug)]
pub(super) struct LegacyOwnership {
    names: Vec<String>,
    ingress_ips: Vec<String>,
}

impl LegacyOwnership {
    /// Version 0.6.0 left a RobotLB finalizer and published the actual LB IP.
    /// Both must still be present before we can claim the unlabelled resource.
    pub(super) fn from_service(svc: &Service, config: &OperatorConfig) -> Option<Self> {
        if !finalizers::check(svc) {
            return None;
        }
        let ingress = svc
            .status
            .as_ref()?
            .load_balancer
            .as_ref()?
            .ingress
            .as_ref()?;
        let ingress_ips = ingress
            .iter()
            .map(|entry| entry.ip.as_ref().filter(|ip| !ip.is_empty()).cloned())
            .collect::<Option<Vec<_>>>()?;
        if ingress_ips.is_empty() {
            return None;
        }

        let names = if let Some(explicit) = svc.annotations().get(consts::LB_NAME_ANNOTATION) {
            if explicit.is_empty() {
                return None;
            }
            vec![explicit.clone()]
        } else {
            let namespace = svc.namespace().unwrap_or_else(|| "default".to_string());
            let name = svc.name_any();
            let mut names = Vec::new();
            if let Some(cluster) = config.cluster_name.as_deref() {
                names.push(default_name(Some(cluster), &name, &namespace));
            }
            // Upgrades may enable ROBOTLB_CLUSTER_NAME at the same time.
            names.push(default_name(None, &name, &namespace));
            names
        };
        Some(Self { names, ingress_ips })
    }

    pub(super) fn names(&self) -> &[String] {
        &self.names
    }

    /// Compare against provider-owned addresses, not the current desired targets
    /// or mutable listener configuration (which may legitimately have drifted).
    fn matches(&self, lb: &LoadBalancer) -> bool {
        if !self.names.contains(&lb.name) {
            return false;
        }
        let mut addresses: HashSet<String> = [
            lb.public_net.ipv4.ip.clone().flatten(),
            lb.public_net.ipv6.ip.clone().flatten(),
        ]
        .into_iter()
        .flatten()
        .collect();
        addresses.extend(lb.private_net.iter().filter_map(|net| net.ip.clone()));
        self.ingress_ips.iter().all(|ip| addresses.contains(ip))
    }

    /// Never overwrite any existing owner label; preserve every unrelated label.
    pub(super) fn labels_for_adoption(
        &self,
        lb: &LoadBalancer,
        uid: &str,
    ) -> Option<HashMap<String, String>> {
        if uid.is_empty() || lb.labels.contains_key(consts::LB_OWNER_LABEL) || !self.matches(lb) {
            return None;
        }
        let mut labels = lb.labels.clone();
        labels.insert(consts::LB_OWNER_LABEL.to_string(), uid.to_string());
        Some(labels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use k8s_openapi::{
        api::core::v1::{LoadBalancerIngress, LoadBalancerStatus, ServiceStatus},
        apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };

    fn config() -> OperatorConfig {
        OperatorConfig::parse_from(["robotlb", "--hcloud-token", "dummy"])
    }

    fn service() -> Service {
        Service {
            metadata: ObjectMeta {
                name: Some("web".into()),
                namespace: Some("default".into()),
                finalizers: Some(vec![consts::FINALIZER_NAME.into()]),
                ..Default::default()
            },
            status: Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: Some(vec![LoadBalancerIngress {
                        ip: Some("203.0.113.10".into()),
                        ..Default::default()
                    }]),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn balancer() -> LoadBalancer {
        LoadBalancer {
            id: 42,
            name: "web.default".into(),
            public_net: Box::new(hcloud::models::LoadBalancerPublicNet {
                ipv4: Box::new(hcloud::models::LoadBalancerPublicNetIpv4 {
                    ip: Some(Some("203.0.113.10".into())),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn adopts_legacy_lb_and_preserves_other_labels() {
        let proof = LegacyOwnership::from_service(&service(), &config()).unwrap();
        let mut lb = balancer();
        lb.labels.insert("team".into(), "platform".into());
        let labels = proof.labels_for_adoption(&lb, "service-uid").unwrap();
        assert_eq!(labels.get("team").map(String::as_str), Some("platform"));
        assert_eq!(
            labels.get(consts::LB_OWNER_LABEL).map(String::as_str),
            Some("service-uid")
        );
        assert_eq!(lb.id, 42); // The migration does not replace the LB.
    }

    #[test]
    fn rejects_missing_or_unreliable_kubernetes_evidence() {
        let mut svc = service();
        svc.metadata.finalizers = None;
        assert!(LegacyOwnership::from_service(&svc, &config()).is_none());

        let mut svc = service();
        svc.status = None;
        assert!(LegacyOwnership::from_service(&svc, &config()).is_none());

        let mut svc = service();
        svc.status.as_mut().unwrap().load_balancer.as_mut().unwrap().ingress =
            Some(vec![LoadBalancerIngress {
                hostname: Some("unverified.example".into()),
                ..Default::default()
            }]);
        assert!(LegacyOwnership::from_service(&svc, &config()).is_none());
    }

    #[test]
    fn never_adopts_a_name_collision_or_foreign_owner() {
        let proof = LegacyOwnership::from_service(&service(), &config()).unwrap();
        let mut lb = balancer();
        lb.public_net.ipv4.ip = Some(Some("198.51.100.20".into()));
        assert!(proof.labels_for_adoption(&lb, "service-uid").is_none());

        let mut lb = balancer();
        lb.name = "another.default".into();
        assert!(proof.labels_for_adoption(&lb, "service-uid").is_none());

        let mut lb = balancer();
        lb.labels.insert(consts::LB_OWNER_LABEL.into(), "other-uid".into());
        assert!(proof.labels_for_adoption(&lb, "service-uid").is_none());
        lb.labels.insert(consts::LB_OWNER_LABEL.into(), "service-uid".into());
        assert!(proof.labels_for_adoption(&lb, "service-uid").is_none());
    }

    #[test]
    fn matches_private_only_load_balancers_and_requires_all_ingress_ips() {
        let mut svc = service();
        svc.status.as_mut().unwrap().load_balancer.as_mut().unwrap().ingress =
            Some(vec![LoadBalancerIngress {
                ip: Some("10.0.0.10".into()),
                ..Default::default()
            }]);
        let proof = LegacyOwnership::from_service(&svc, &config()).unwrap();
        let mut lb = balancer();
        lb.private_net = vec![hcloud::models::LoadBalancerPrivateNet {
            ip: Some("10.0.0.10".into()),
            ..Default::default()
        }];
        assert!(proof.labels_for_adoption(&lb, "service-uid").is_some());

        svc.status.as_mut().unwrap().load_balancer.as_mut().unwrap().ingress.as_mut().unwrap()
            .push(LoadBalancerIngress {
                ip: Some("198.51.100.99".into()),
                ..Default::default()
            });
        let proof = LegacyOwnership::from_service(&svc, &config()).unwrap();
        assert!(proof.labels_for_adoption(&lb, "service-uid").is_none());
    }

    #[test]
    fn considers_old_unprefixed_name_when_a_cluster_prefix_is_enabled() {
        let mut config = config();
        config.cluster_name = Some("prod".into());
        let proof = LegacyOwnership::from_service(&service(), &config).unwrap();
        assert_eq!(proof.names(), &["prod.web.default", "web.default"]);
        assert!(proof.labels_for_adoption(&balancer(), "service-uid").is_some());

        let mut svc = service();
        svc.metadata.annotations = Some(std::collections::BTreeMap::from([(
            consts::LB_NAME_ANNOTATION.into(),
            "custom".into(),
        )]));
        let proof = LegacyOwnership::from_service(&svc, &config).unwrap();
        assert_eq!(proof.names(), &["custom"]);
        assert!(proof.labels_for_adoption(&balancer(), "service-uid").is_none());
    }
}
