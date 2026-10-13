//! Load balancer management for Hetzner Cloud.
//!
//! This module provides the core load balancer functionality, including:
//! - Configuration parsing from Kubernetes service annotations
//! - Reconciliation of load balancer state with Hetzner Cloud
//! - Cleanup of load balancer resources

mod api;
mod config;
mod types;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    time::Duration,
};

use hcloud::{
    apis::configuration::Configuration as HcloudConfig,
    models::{
        LoadBalancerAlgorithm, LoadBalancerService, LoadBalancerServiceHealthCheck,
        UpdateLoadBalancerService,
    },
};
use k8s_openapi::api::core::v1::Service;

use crate::{
    CurrentContext,
    error::{RobotLBError, RobotLBResult},
};

pub(crate) use api::{HcloudLoadBalancerApi, LiveHcloudLoadBalancerApi};
pub(crate) use config::parse_load_balancer_config;
use types::{ServiceReconcileAction, TargetReconcileAction, normalize_ip, service_matches_desired};

/// Struct representing a load balancer.
///
/// It holds all the necessary information to manage the load balancer
/// in Hetzner Cloud.
#[derive(Debug)]
pub struct LoadBalancer {
    /// Requested load balancer name.
    pub name: String,
    /// Service UID, used as the stable ownership identity in Hetzner.
    service_uid: String,
    /// Services exposed by the load balancer (`listen_port` -> `target_port`).
    pub services: HashMap<i32, i32>,
    /// Target IPs for the load balancer.
    pub targets: Vec<String>,
    /// Optional private IP for the load balancer.
    pub private_ip: Option<String>,
    /// Whether the public interface should be enabled.
    pub public_interface: bool,

    /// Health check interval in seconds.
    pub check_interval: i32,
    /// Health check timeout in seconds.
    pub timeout: i32,
    /// Number of health check retries.
    pub retries: i32,
    /// Whether proxy protocol is enabled.
    pub proxy_mode: bool,

    /// Hetzner Cloud location.
    pub location: String,
    /// Load balancer type (e.g., "lb11").
    pub balancer_type: String,
    /// Load balancing algorithm.
    pub algorithm: LoadBalancerAlgorithm,
    /// Optional network name for private networking.
    pub network_name: Option<String>,

    /// Hetzner Cloud API configuration.
    pub hcloud_config: HcloudConfig,
}

impl LoadBalancer {
    /// Create a new `LoadBalancer` instance from a Kubernetes service
    /// and the current context.
    ///
    /// This method will try to extract all the necessary information
    /// from the service annotations and the context.
    /// If some of the required information is missing, the method will
    /// try to use the default values from the context.
    ///
    /// # Errors
    ///
    /// Returns an error if required annotations are missing or invalid.
    pub fn try_from_svc(svc: &Service, context: &CurrentContext) -> RobotLBResult<Self> {
        let parsed = parse_load_balancer_config(svc, &context.config)?;

        Ok(Self {
            name: parsed.name,
            service_uid: svc.metadata.uid.clone().unwrap_or_default(),
            private_ip: parsed.private_ip,
            public_interface: parsed.public_interface,
            balancer_type: parsed.balancer_type,
            check_interval: parsed.check_interval,
            timeout: parsed.timeout,
            retries: parsed.retries,
            location: parsed.location,
            proxy_mode: parsed.proxy_mode,
            network_name: parsed.network_name,
            algorithm: parsed.algorithm,
            services: HashMap::default(),
            targets: Vec::default(),
            hcloud_config: context.hcloud_config.clone(),
        })
    }

    /// Add a service to the load balancer.
    ///
    /// The service will listen on the `listen_port` and forward the
    /// traffic to the `target_port` to all targets.
    pub fn add_service(&mut self, listen_port: i32, target_port: i32) {
        self.services.insert(listen_port, target_port);
    }

    /// Add a target to the load balancer.
    ///
    /// The target will receive the traffic from the services.
    /// The target is identified by its IP address.
    pub fn add_target(&mut self, ip: &str) {
        self.targets.push(ip.to_string());
    }

    /// Reconcile the load balancer to match the desired configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the Hetzner Cloud API call fails.
    #[tracing::instrument(skip(self), fields(lb_name = self.name))]
    pub async fn reconcile(&self) -> RobotLBResult<hcloud::models::LoadBalancer> {
        let api = LiveHcloudLoadBalancerApi {
            hcloud_config: self.hcloud_config.clone(),
        };
        let hcloud_balancer = self.get_or_create_hcloud_lb().await?;
        // Type changes are the highest-risk mutation; reject them before
        // modifying the algorithm, network, listeners or targets.
        self.reconcile_lb_type(&hcloud_balancer, &api).await?;
        self.reconcile_algorithm(&hcloud_balancer, &api).await?;
        self.reconcile_network(&hcloud_balancer).await?;
        self.reconcile_services(&hcloud_balancer, &api).await?;
        self.reconcile_targets(&hcloud_balancer, &api).await?;
        Ok(hcloud_balancer)
    }

    /// Reconcile the services of the load balancer.
    ///
    /// This method will compare the desired configuration of the services
    /// with the current configuration of the services in the load balancer.
    /// If the configuration does not match, the method will update the service.
    async fn reconcile_services(
        &self,
        hcloud_balancer: &hcloud::models::LoadBalancer,
        api: &impl HcloudLoadBalancerApi,
    ) -> RobotLBResult<()> {
        for action in self.plan_service_actions(hcloud_balancer) {
            match action {
                ServiceReconcileAction::Update {
                    listen_port,
                    destination_port,
                } => {
                    tracing::info!(
                        "Desired service configuration for port {} does not match current configuration. Updating ...",
                        listen_port,
                    );
                    let desired = UpdateLoadBalancerService {
                            http: None,
                            protocol: Some(
                                hcloud::models::update_load_balancer_service::Protocol::Tcp,
                            ),
                            listen_port,
                            destination_port: Some(destination_port),
                            proxyprotocol: Some(self.proxy_mode),
                            health_check: Some(Box::new(
                                hcloud::models::UpdateLoadBalancerServiceHealthCheck {
                                    protocol: Some(
                                        hcloud::models::update_load_balancer_service_health_check::Protocol::Tcp,
                                    ),
                                    http: None,
                                    interval: Some(self.check_interval),
                                    port: Some(destination_port),
                                    retries: Some(self.retries),
                                    timeout: Some(self.timeout),
                                },
                            )),
                        };
                    retry_temporary(|| api.update_service(hcloud_balancer.id, desired.clone()))
                        .await?;
                }
                ServiceReconcileAction::Delete { listen_port } => {
                    tracing::info!(
                        "Deleting service that listens for port {} from load-balancer {}",
                        listen_port,
                        hcloud_balancer.name,
                    );
                    retry_temporary(|| api.delete_service(hcloud_balancer.id, listen_port)).await?;
                }
                ServiceReconcileAction::Add {
                    listen_port,
                    destination_port,
                } => {
                    tracing::info!(
                        "Found missing service. Adding service that listens for port {}",
                        listen_port
                    );
                    let desired = LoadBalancerService {
                        http: None,
                        listen_port,
                        destination_port,
                        protocol: hcloud::models::load_balancer_service::Protocol::Tcp,
                        proxyprotocol: self.proxy_mode,
                        health_check: Box::new(LoadBalancerServiceHealthCheck {
                            http: None,
                            interval: self.check_interval,
                            port: destination_port,
                            protocol:
                                hcloud::models::load_balancer_service_health_check::Protocol::Tcp,
                            retries: self.retries,
                            timeout: self.timeout,
                        }),
                    };
                    retry_temporary(|| api.add_service(hcloud_balancer.id, desired.clone()))
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Reconcile the targets of the load balancer.
    ///
    /// This method will compare the desired configuration of the targets
    /// with the current configuration of the targets in the load balancer.
    /// If the configuration does not match, the method will update the target.
    async fn reconcile_targets(
        &self,
        hcloud_balancer: &hcloud::models::LoadBalancer,
        api: &impl HcloudLoadBalancerApi,
    ) -> RobotLBResult<()> {
        for action in self.plan_target_actions(hcloud_balancer) {
            match action {
                TargetReconcileAction::Remove { target_ip } => {
                    tracing::info!("Removing target {}", target_ip);
                    retry_temporary(|| api.remove_target(hcloud_balancer.id, target_ip.clone()))
                        .await?;
                }
                TargetReconcileAction::Add { target_ip } => {
                    tracing::info!("Adding target {}", target_ip);
                    match retry_temporary(|| api.add_target(hcloud_balancer.id, target_ip.clone()))
                        .await
                    {
                        Err(error) if error.target_already_defined() => {
                            tracing::debug!("Target was already added in an earlier attempt");
                        }
                        result => result?,
                    }
                }
            }
        }
        Ok(())
    }

    /// Reconcile the load balancer algorithm.
    ///
    /// This method will compare the desired algorithm configuration
    /// and update it if it does not match the current configuration.
    async fn reconcile_algorithm(
        &self,
        hcloud_balancer: &hcloud::models::LoadBalancer,
        api: &impl HcloudLoadBalancerApi,
    ) -> RobotLBResult<()> {
        if let Some(algorithm) = self.plan_algorithm_action(hcloud_balancer) {
            tracing::info!(
                "Changing load balancer algorithm from {:?} to {:?}",
                hcloud_balancer.algorithm,
                algorithm
            );
            retry_temporary(|| api.change_algorithm(hcloud_balancer.id, algorithm.clone())).await?;
        }
        Ok(())
    }

    /// Reconcile the load balancer type.
    async fn reconcile_lb_type(
        &self,
        hcloud_balancer: &hcloud::models::LoadBalancer,
        api: &impl HcloudLoadBalancerApi,
    ) -> RobotLBResult<()> {
        if let Some(balancer_type) = self.plan_lb_type_action(hcloud_balancer) {
            tracing::info!(
                "Changing load balancer type from {} to {}",
                hcloud_balancer.load_balancer_type.name,
                balancer_type
            );
            retry_temporary(|| api.change_type(hcloud_balancer.id, balancer_type.clone())).await?;
        }
        Ok(())
    }

    fn plan_service_actions(
        &self,
        hcloud_balancer: &hcloud::models::LoadBalancer,
    ) -> Vec<ServiceReconcileAction> {
        let mut actions = Vec::new();

        for service in &hcloud_balancer.services {
            if let Some(destination_port) = self.services.get(&service.listen_port) {
                if !service_matches_desired(
                    service,
                    *destination_port,
                    self.check_interval,
                    self.retries,
                    self.timeout,
                    self.proxy_mode,
                ) {
                    actions.push(ServiceReconcileAction::Update {
                        listen_port: service.listen_port,
                        destination_port: *destination_port,
                    });
                }
            } else {
                actions.push(ServiceReconcileAction::Delete {
                    listen_port: service.listen_port,
                });
            }
        }

        let mut missing_services = self
            .services
            .iter()
            .filter(|(listen_port, _)| {
                !hcloud_balancer
                    .services
                    .iter()
                    .any(|service| service.listen_port == **listen_port)
            })
            .map(
                |(listen_port, destination_port)| ServiceReconcileAction::Add {
                    listen_port: *listen_port,
                    destination_port: *destination_port,
                },
            )
            .collect::<Vec<_>>();

        missing_services.sort_by_key(|action| match action {
            ServiceReconcileAction::Add { listen_port, .. } => *listen_port,
            _ => 0,
        });

        actions.extend(missing_services);
        actions
    }

    fn plan_target_actions(
        &self,
        hcloud_balancer: &hcloud::models::LoadBalancer,
    ) -> Vec<TargetReconcileAction> {
        let mut actions = Vec::new();

        // Capacity is validated before any Hetzner mutation. Never silently drop
        // eligible nodes to fit a smaller balancer type.
        let desired_targets: HashSet<String> =
            self.targets.iter().map(|ip| normalize_ip(ip)).collect();
        let current_targets: HashSet<String> = hcloud_balancer
            .targets
            .iter()
            .filter_map(|target| {
                target
                    .ip
                    .as_ref()
                    .map(|target_ip| normalize_ip(&target_ip.ip))
            })
            .collect();

        for target in &hcloud_balancer.targets {
            let Some(target_ip) = target.ip.clone() else {
                continue;
            };
            let normalized_target_ip = normalize_ip(&target_ip.ip);
            if !desired_targets.contains(&normalized_target_ip) {
                actions.push(TargetReconcileAction::Remove {
                    target_ip: target_ip.ip,
                });
            }
        }

        for ip in desired_targets {
            if !current_targets.contains(&ip) {
                actions.push(TargetReconcileAction::Add { target_ip: ip });
            }
        }

        actions
    }

    fn plan_algorithm_action(
        &self,
        hcloud_balancer: &hcloud::models::LoadBalancer,
    ) -> Option<LoadBalancerAlgorithm> {
        if *hcloud_balancer.algorithm == self.algorithm {
            None
        } else {
            Some(self.algorithm.clone())
        }
    }

    fn plan_lb_type_action(
        &self,
        hcloud_balancer: &hcloud::models::LoadBalancer,
    ) -> Option<String> {
        if hcloud_balancer.load_balancer_type.name == self.balancer_type {
            None
        } else {
            Some(self.balancer_type.clone())
        }
    }

    /// Reconcile the network of the load balancer.
    ///
    /// This method will compare the desired network configuration
    /// with the current network configuration of the load balancer.
    /// If the configuration does not match, the method will update the
    /// network configuration.
    async fn reconcile_network(
        &self,
        hcloud_balancer: &hcloud::models::LoadBalancer,
    ) -> RobotLBResult<()> {
        if self.network_name.is_none() && hcloud_balancer.private_net.is_empty() {
            return Ok(());
        }

        let desired_network = self.get_network().await?.map(|network| network.id);

        let is_attached = self
            .detach_unwanted_networks(hcloud_balancer, desired_network)
            .await?;

        if !is_attached && let Some(network_id) = desired_network {
            tracing::info!("Attaching balancer to network {}", network_id);
            retry_temporary(|| {
                api::attach_to_network(
                    &self.hcloud_config,
                    hcloud_balancer.id,
                    network_id,
                    self.private_ip.clone(),
                )
            })
            .await?;
        }
        Ok(())
    }

    async fn detach_unwanted_networks(
        &self,
        hcloud_balancer: &hcloud::models::LoadBalancer,
        desired_network: Option<i64>,
    ) -> RobotLBResult<bool> {
        let mut is_attached = false;

        for private_net in &hcloud_balancer.private_net {
            let Some(private_net_id) = private_net.network else {
                continue;
            };

            if desired_network == Some(private_net_id)
                && private_net
                    .ip
                    .as_ref()
                    .is_some_and(|ip| self.matches_desired_ip(ip))
            {
                is_attached = true;
                continue;
            }

            tracing::info!("Detaching balancer from network {}", private_net_id);
            retry_temporary(|| {
                api::detach_from_network(&self.hcloud_config, hcloud_balancer.id, private_net_id)
            })
            .await?;
        }

        Ok(is_attached)
    }

    fn matches_desired_ip(&self, current_ip: &str) -> bool {
        self.private_ip
            .as_ref()
            .is_none_or(|desired_ip| desired_ip == current_ip)
    }

    /// Delete only a balancer carrying this Service's ownership label.
    /// A mutable name can never authorize deletion.
    ///
    /// # Errors
    /// Returns errors from Hetzner Cloud.
    pub async fn cleanup(&self) -> RobotLBResult<()> {
        let Some(lb) = api::get_owned_load_balancer(&self.hcloud_config, &self.service_uid).await?
        else {
            return Ok(());
        };
        retry_temporary(|| api::delete_load_balancer(&self.hcloud_config, lb.id)).await
    }

    /// Release without parsing annotations: malformed config cannot block deletion.
    ///
    /// # Errors
    /// Returns errors from Hetzner Cloud or a missing Service UID.
    pub async fn cleanup_service(svc: &Service, context: &CurrentContext) -> RobotLBResult<()> {
        let uid = svc
            .metadata
            .uid
            .as_deref()
            .ok_or(RobotLBError::SkipService)?;
        let Some(lb) = api::get_owned_load_balancer(&context.hcloud_config, uid).await? else {
            return Ok(());
        };
        retry_temporary(|| api::delete_load_balancer(&context.hcloud_config, lb.id)).await
    }

    /// Existing resources must have the matching Service UID label.
    /// IP targets and mutable names are never used as proof of ownership.
    async fn get_hcloud_lb(&self) -> RobotLBResult<Option<hcloud::models::LoadBalancer>> {
        api::get_owned_load_balancer(&self.hcloud_config, &self.service_uid).await
    }

    /// Reject an oversized configuration before creating or modifying an LB.
    fn validate_capacity(
        &self,
        lb_type: &hcloud::models::LoadBalancerType,
        existing: Option<&hcloud::models::LoadBalancer>,
    ) -> RobotLBResult<()> {
        let max_targets = usize::try_from(lb_type.max_targets).map_err(|_| {
            RobotLBError::InvalidLoadBalancerConfiguration(format!(
                "Invalid target capacity reported for LB type {}",
                lb_type.name
            ))
        })?;
        let max_services = usize::try_from(lb_type.max_services).map_err(|_| {
            RobotLBError::InvalidLoadBalancerConfiguration(format!(
                "Invalid listener capacity reported for LB type {}",
                lb_type.name
            ))
        })?;
        if max_targets == 0 || max_services == 0 {
            return Err(RobotLBError::InvalidLoadBalancerConfiguration(format!(
                "LB type {} has unavailable capacity limits",
                lb_type.name
            )));
        }

        let targets = self
            .targets
            .iter()
            .map(|ip| normalize_ip(ip))
            .collect::<HashSet<_>>()
            .len();
        let listeners = self.services.len();
        if targets > max_targets || listeners > max_services {
            return Err(RobotLBError::InvalidLoadBalancerConfiguration(format!(
                "LB type {} supports at most {max_targets} targets and {max_services} listeners, but Service requires {targets} targets and {listeners} listeners; no changes applied",
                lb_type.name
            )));
        }

        // Do not remove healthy existing targets merely to permit a downgrade.
        if let Some(existing) = existing
            && existing.load_balancer_type.name != lb_type.name
            && (existing.targets.len() > max_targets || existing.services.len() > max_services)
        {
            return Err(RobotLBError::InvalidLoadBalancerConfiguration(format!(
                "Cannot change existing LB {} to {}: it currently has {} targets and {} listeners, exceeding the requested type's capacity ({max_targets} targets, {max_services} listeners); no changes applied",
                existing.name,
                lb_type.name,
                existing.targets.len(),
                existing.services.len()
            )));
        }
        Ok(())
    }

    /// Get an owned LB or create a new one after validating capacity and name.
    async fn get_or_create_hcloud_lb(&self) -> RobotLBResult<hcloud::models::LoadBalancer> {
        let existing = self.get_hcloud_lb().await?;
        // Reuse capacity data on an unchanged type; querying Hetzner's catalog
        // is required only for creation or type changes.
        if let Some(lb) = existing.as_ref()
            && lb.load_balancer_type.name == self.balancer_type
            && lb.load_balancer_type.max_targets > 0
            && lb.load_balancer_type.max_services > 0
        {
            self.validate_capacity(&lb.load_balancer_type, existing.as_ref())?;
        } else {
            let lb_type =
                api::get_load_balancer_type(&self.hcloud_config, &self.balancer_type).await?;
            self.validate_capacity(&lb_type, existing.as_ref())?;
        }
        if let Some(lb) = existing {
            return Ok(lb);
        }

        if api::get_load_balancer(&self.hcloud_config, &self.name)
            .await?
            .is_some()
        {
            return Err(RobotLBError::InvalidLoadBalancerConfiguration(format!(
                "LB name {} is already in use by an unowned resource; refusing to adopt or modify it",
                self.name
            )));
        }

        api::create_load_balancer(
            &self.hcloud_config,
            &self.name,
            &self.location,
            &self.balancer_type,
            self.algorithm.clone(),
            self.public_interface,
            &self.service_uid,
        )
        .await
    }

    /// Get the network from Hetzner Cloud.
    ///
    /// This method will try to find the network with the name
    /// specified in the `LoadBalancer` struct. It returns `None` only
    /// in case the network name is not provided. If the network was not found,
    /// the error is returned.
    async fn get_network(&self) -> RobotLBResult<Option<hcloud::models::Network>> {
        let Some(network_name) = self.network_name.clone() else {
            return Ok(None);
        };
        let response = api::list_networks(&self.hcloud_config, Some(network_name.clone())).await?;
        let networks: Vec<_> = response
            .networks
            .into_iter()
            .filter(|network| network.name == network_name)
            .collect();

        if networks.len() > 1 {
            tracing::warn!(
                "Found more than one network with name {}, skipping",
                network_name
            );
            return Err(RobotLBError::HCloudError(format!(
                "Found more than one network with name {network_name}"
            )));
        }
        if networks.is_empty() {
            tracing::warn!("Network with name {} not found", network_name);
            return Err(RobotLBError::HCloudError(format!(
                "Network with name {network_name} not found"
            )));
        }

        Ok(networks.into_iter().next())
    }
}

/// Retry temporary Hetzner conflicts without treating permanent errors or 429s as transient.
async fn retry_temporary<T, F, Fut>(mut operation: F) -> RobotLBResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = RobotLBResult<T>>,
{
    for attempt in 0..=2_u64 {
        match operation().await {
            Err(error) if error.is_transient_hcloud() && attempt < 2 => {
                tokio::time::sleep(Duration::from_secs(attempt + 1)).await;
            }
            result => return result,
        }
    }
    unreachable!("all retry paths return on the final attempt")
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use hcloud::apis::configuration::Configuration as HcloudConfig;
    use std::sync::Mutex;

    fn delete_error(status: u16) -> RobotLBError {
        RobotLBError::HcloudLBDeleteError(hcloud::apis::Error::ResponseError(
            hcloud::apis::ResponseContent {
                status: status.try_into().expect("valid HTTP status"),
                content: String::new(),
                entity: None,
            },
        ))
    }

    #[tokio::test]
    async fn delete_retries_locked_then_succeeds() {
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        retry_temporary(|| {
            let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                if attempt == 0 {
                    Err(delete_error(423))
                } else {
                    Ok(())
                }
            }
        })
        .await
        .expect("second deletion succeeds");
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn delete_does_not_retry_permanent_or_rate_limit_errors() {
        for status in [403, 429] {
            let attempts = std::sync::atomic::AtomicUsize::new(0);
            let result: RobotLBResult<()> = retry_temporary(|| {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async move { Err(delete_error(status)) }
            })
            .await;
            assert!(result.is_err());
            assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
        }
    }

    #[test]
    fn capacity_validation_rejects_oversized_configuration() {
        let mut lb = test_load_balancer();
        lb.targets = (1..=26).map(|i| format!("192.0.2.{i}")).collect();
        let lb_type = hcloud::models::LoadBalancerType {
            name: "lb11".to_string(),
            max_targets: 25,
            max_services: 5,
            ..Default::default()
        };
        assert!(matches!(
            lb.validate_capacity(&lb_type, None),
            Err(RobotLBError::InvalidLoadBalancerConfiguration(_))
        ));
        lb.targets.pop();
        assert!(lb.validate_capacity(&lb_type, None).is_ok());
    }

    #[test]
    fn capacity_validation_never_shrinks_existing_lb_implicitly() {
        let lb = test_load_balancer();
        let mut current = hcloud::models::LoadBalancer {
            name: "existing".to_string(),
            ..Default::default()
        };
        current.load_balancer_type.name = "lb21".to_string();
        current.targets = vec![hcloud::models::LoadBalancerTarget::default(); 26];
        let desired = hcloud::models::LoadBalancerType {
            name: "lb11".to_string(),
            max_targets: 25,
            max_services: 5,
            ..Default::default()
        };
        assert!(matches!(
            lb.validate_capacity(&desired, Some(&current)),
            Err(RobotLBError::InvalidLoadBalancerConfiguration(_))
        ));
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ApiCall {
        UpdateService {
            listen_port: i32,
            destination_port: i32,
        },
        DeleteService {
            listen_port: i32,
        },
        AddService {
            listen_port: i32,
            destination_port: i32,
        },
        RemoveTarget {
            ip: String,
        },
        AddTarget {
            ip: String,
        },
        ChangeAlgorithm,
        ChangeType {
            balancer_type: String,
        },
    }

    #[derive(Default)]
    struct MockHcloudApi {
        calls: Mutex<Vec<ApiCall>>,
    }

    impl MockHcloudApi {
        fn calls(&self) -> Vec<ApiCall> {
            self.calls
                .lock()
                .expect("lock should not be poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl HcloudLoadBalancerApi for MockHcloudApi {
        async fn update_service(
            &self,
            _load_balancer_id: i64,
            service: UpdateLoadBalancerService,
        ) -> RobotLBResult<()> {
            self.calls
                .lock()
                .expect("lock should not be poisoned")
                .push(ApiCall::UpdateService {
                    listen_port: service.listen_port,
                    destination_port: service
                        .destination_port
                        .expect("destination port should be present"),
                });
            Ok(())
        }

        async fn delete_service(
            &self,
            _load_balancer_id: i64,
            listen_port: i32,
        ) -> RobotLBResult<()> {
            self.calls
                .lock()
                .expect("lock should not be poisoned")
                .push(ApiCall::DeleteService { listen_port });
            Ok(())
        }

        async fn add_service(
            &self,
            _load_balancer_id: i64,
            service: LoadBalancerService,
        ) -> RobotLBResult<()> {
            self.calls
                .lock()
                .expect("lock should not be poisoned")
                .push(ApiCall::AddService {
                    listen_port: service.listen_port,
                    destination_port: service.destination_port,
                });
            Ok(())
        }

        async fn remove_target(
            &self,
            _load_balancer_id: i64,
            target_ip: String,
        ) -> RobotLBResult<()> {
            self.calls
                .lock()
                .expect("lock should not be poisoned")
                .push(ApiCall::RemoveTarget { ip: target_ip });
            Ok(())
        }

        async fn add_target(&self, _load_balancer_id: i64, target_ip: String) -> RobotLBResult<()> {
            self.calls
                .lock()
                .expect("lock should not be poisoned")
                .push(ApiCall::AddTarget { ip: target_ip });
            Ok(())
        }

        async fn change_algorithm(
            &self,
            _load_balancer_id: i64,
            _algorithm: LoadBalancerAlgorithm,
        ) -> RobotLBResult<()> {
            self.calls
                .lock()
                .expect("lock should not be poisoned")
                .push(ApiCall::ChangeAlgorithm);
            Ok(())
        }

        async fn change_type(
            &self,
            _load_balancer_id: i64,
            balancer_type: String,
        ) -> RobotLBResult<()> {
            self.calls
                .lock()
                .expect("lock should not be poisoned")
                .push(ApiCall::ChangeType { balancer_type });
            Ok(())
        }
    }

    fn test_load_balancer() -> LoadBalancer {
        LoadBalancer {
            name: "svc-name".to_string(),
            service_uid: "test-uid".to_string(),
            services: HashMap::new(),
            targets: vec![],
            private_ip: None,
            public_interface: true,
            check_interval: 15,
            timeout: 10,
            retries: 3,
            proxy_mode: false,
            location: "hel1".to_string(),
            balancer_type: "lb11".to_string(),
            algorithm: LoadBalancerAlgorithm {
                r#type: hcloud::models::load_balancer_algorithm::Type::LeastConnections,
            },
            network_name: None,
            hcloud_config: HcloudConfig::new(),
        }
    }

    fn existing_service(listen_port: i32, destination_port: i32) -> LoadBalancerService {
        LoadBalancerService {
            http: None,
            listen_port,
            destination_port,
            protocol: hcloud::models::load_balancer_service::Protocol::Tcp,
            proxyprotocol: false,
            health_check: Box::new(LoadBalancerServiceHealthCheck {
                http: None,
                interval: 5,
                port: destination_port,
                protocol: hcloud::models::load_balancer_service_health_check::Protocol::Tcp,
                retries: 1,
                timeout: 2,
            }),
        }
    }

    fn remote_balancer_with_services(
        services: Vec<LoadBalancerService>,
        targets: Vec<&str>,
    ) -> hcloud::models::LoadBalancer {
        hcloud::models::LoadBalancer {
            id: 42,
            name: "svc-name".to_string(),
            services,
            targets: targets
                .into_iter()
                .map(|ip| hcloud::models::LoadBalancerTarget {
                    ip: Some(Box::new(hcloud::models::LoadBalancerTargetIp {
                        ip: ip.to_string(),
                    })),
                    ..Default::default()
                })
                .collect(),
            algorithm: Box::new(LoadBalancerAlgorithm {
                r#type: hcloud::models::load_balancer_algorithm::Type::RoundRobin,
            }),
            load_balancer_type: Box::new(hcloud::models::LoadBalancerType {
                id: 1,
                name: "lb11".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn reconcile_services_uses_mock_api_for_drift_and_missing_ports() {
        let mut lb = test_load_balancer();
        lb.services.insert(80, 30080);
        lb.services.insert(443, 30443);

        let remote = remote_balancer_with_services(
            vec![existing_service(80, 30081), existing_service(8080, 38080)],
            vec![],
        );
        let api = MockHcloudApi::default();

        lb.reconcile_services(&remote, &api)
            .await
            .expect("service reconcile should succeed");

        assert_eq!(
            api.calls(),
            vec![
                ApiCall::UpdateService {
                    listen_port: 80,
                    destination_port: 30080,
                },
                ApiCall::DeleteService { listen_port: 8080 },
                ApiCall::AddService {
                    listen_port: 443,
                    destination_port: 30443,
                }
            ]
        );
    }

    #[tokio::test]
    async fn reconcile_targets_uses_mock_api_for_target_diff() {
        let mut lb = test_load_balancer();
        lb.targets = vec!["10.0.0.1".to_string(), "10.0.0.2".to_string()];

        let remote = remote_balancer_with_services(vec![], vec!["10.0.0.2", "10.0.0.3"]);
        let api = MockHcloudApi::default();

        lb.reconcile_targets(&remote, &api)
            .await
            .expect("target reconcile should succeed");

        assert_eq!(
            api.calls(),
            vec![
                ApiCall::RemoveTarget {
                    ip: "10.0.0.3".to_string(),
                },
                ApiCall::AddTarget {
                    ip: "10.0.0.1".to_string(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn reconcile_algorithm_and_type_uses_mock_api_when_values_change() {
        let mut lb = test_load_balancer();
        lb.balancer_type = "lb21".to_string();

        let remote = remote_balancer_with_services(vec![], vec![]);
        let api = MockHcloudApi::default();

        lb.reconcile_algorithm(&remote, &api)
            .await
            .expect("algorithm reconcile should succeed");
        lb.reconcile_lb_type(&remote, &api)
            .await
            .expect("type reconcile should succeed");

        assert_eq!(
            api.calls(),
            vec![
                ApiCall::ChangeAlgorithm,
                ApiCall::ChangeType {
                    balancer_type: "lb21".to_string(),
                }
            ]
        );
    }

    #[test]
    fn plan_service_actions_builds_expected_diff() {
        let mut lb = test_load_balancer();
        lb.services.insert(80, 30080);
        lb.services.insert(443, 30443);

        let remote = remote_balancer_with_services(
            vec![existing_service(80, 30081), existing_service(8080, 38080)],
            vec![],
        );

        let plan = lb.plan_service_actions(&remote);

        assert_eq!(
            plan,
            vec![
                ServiceReconcileAction::Update {
                    listen_port: 80,
                    destination_port: 30080,
                },
                ServiceReconcileAction::Delete { listen_port: 8080 },
                ServiceReconcileAction::Add {
                    listen_port: 443,
                    destination_port: 30443,
                },
            ]
        );
    }

    #[test]
    fn plan_target_actions_builds_expected_diff() {
        let mut lb = test_load_balancer();
        lb.targets = vec!["10.0.0.1".to_string(), "10.0.0.2".to_string()];

        let remote = remote_balancer_with_services(vec![], vec!["10.0.0.2", "10.0.0.3"]);

        let plan = lb.plan_target_actions(&remote);

        assert_eq!(
            plan,
            vec![
                TargetReconcileAction::Remove {
                    target_ip: "10.0.0.3".to_string(),
                },
                TargetReconcileAction::Add {
                    target_ip: "10.0.0.1".to_string(),
                },
            ]
        );
    }
}
