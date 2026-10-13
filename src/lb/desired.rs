//! Typed desired state, independent of Kubernetes and Hetzner API clients.
//! A Service is parsed once, then target discovery fills in its effective backends.

use super::{LoadBalancer, legacy::LegacyOwnership, types::ParsedLoadBalancerConfig};
use crate::{config::OperatorConfig, error::RobotLBResult};
use hcloud::apis::configuration::Configuration as HcloudConfig;
use k8s_openapi::api::core::v1::Service;
use kube::ResourceExt;
use std::collections::HashMap;

#[derive(Debug)]
pub struct DesiredLoadBalancer {
    config: ParsedLoadBalancerConfig,
    service_uid: String,
    legacy_ownership: Option<LegacyOwnership>,
    targets: Vec<String>,
    services: HashMap<i32, i32>,
}

impl DesiredLoadBalancer {
    pub(crate) fn from_service(svc: &Service, config: &OperatorConfig) -> RobotLBResult<Self> {
        Ok(Self {
            config: super::parse_load_balancer_config(svc, config)?,
            service_uid: svc.uid().unwrap_or_default(),
            legacy_ownership: LegacyOwnership::from_service(svc, config),
            targets: Vec::new(),
            services: HashMap::new(),
        })
    }

    pub(crate) const fn ip_type(&self) -> &'static str {
        if self.config.network_name.is_some() {
            "InternalIP"
        } else {
            "ExternalIP"
        }
    }

    pub(crate) fn add_service(&mut self, listen_port: i32, target_port: i32) {
        self.services.insert(listen_port, target_port);
    }

    pub(crate) fn add_target(&mut self, ip: &str) {
        self.targets.push(ip.to_string());
    }

    /// Only the executor is given API credentials; deriving desired state needs none.
    pub(crate) fn into_load_balancer(self, hcloud_config: HcloudConfig) -> LoadBalancer {
        let c = self.config;
        LoadBalancer {
            name: c.name,
            service_uid: self.service_uid,
            legacy_ownership: self.legacy_ownership,
            services: self.services,
            targets: self.targets,
            private_ip: c.private_ip,
            public_interface: c.public_interface,
            check_interval: c.check_interval,
            timeout: c.timeout,
            retries: c.retries,
            proxy_mode: c.proxy_mode,
            location: c.location,
            balancer_type: c.balancer_type,
            algorithm: c.algorithm,
            network_name: c.network_name,
            hcloud_config,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::fixtures::{service_spec, service_with_spec};
    use clap::Parser;

    #[test]
    fn desired_backends_are_deterministic_and_do_not_require_api_credentials() {
        let svc = service_with_spec(service_spec("LoadBalancer", None, vec![]));
        let mut desired = DesiredLoadBalancer::from_service(
            &svc,
            &OperatorConfig::parse_from(["robotlb", "--hcloud-token", "dummy-token"]),
        )
        .expect("valid defaults");
        assert_eq!(desired.ip_type(), "ExternalIP");
        desired.add_service(443, 30443);
        desired.add_target("192.0.2.10");
        assert_eq!(desired.services.get(&443), Some(&30443));
        assert_eq!(desired.targets, vec!["192.0.2.10"]);
    }

    #[test]
    fn desired_state_conversion_preserves_effective_backends() {
        let svc = service_with_spec(service_spec("LoadBalancer", None, vec![]));
        let cfg = OperatorConfig::parse_from(["robotlb", "--hcloud-token", "dummy-token"]);
        let mut desired = DesiredLoadBalancer::from_service(&svc, &cfg).unwrap();
        desired.add_service(80, 30080);
        desired.add_target("198.51.100.7");
        let lb = desired.into_load_balancer(HcloudConfig::new());
        assert_eq!(lb.services.get(&80), Some(&30080));
        assert_eq!(lb.targets, vec!["198.51.100.7"]);
    }
}
