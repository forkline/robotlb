//! Hetzner Cloud Load Balancer API abstraction.
//!
//! This module provides a trait for interacting with the Hetzner Cloud API,
//! allowing for easier testing through mock implementations.

// Newer Clippy considers async_trait's generated must-use futures redundant.
#![allow(clippy::double_must_use)]

use async_trait::async_trait;
use hcloud::{
    apis::{
        configuration::Configuration as HcloudConfig,
        load_balancers_api::{
            AddServiceParams, AddTargetParams, AttachLoadBalancerToNetworkParams,
            ChangeAlgorithmParams, ChangeTypeOfLoadBalancerParams, DeleteLoadBalancerParams,
            DeleteServiceParams, DetachLoadBalancerFromNetworkParams, GetLoadBalancerActionParams,
            ListLoadBalancersParams, RemoveTargetParams, ReplaceLoadBalancerParams,
            UpdateServiceParams,
        },
        networks_api::ListNetworksParams,
    },
    models::{
        AttachLoadBalancerToNetworkRequest, ChangeTypeOfLoadBalancerRequest, DeleteServiceRequest,
        DetachLoadBalancerFromNetworkRequest, LoadBalancerAddTarget, LoadBalancerAlgorithm,
        LoadBalancerService, RemoveTargetRequest, ReplaceLoadBalancerRequest,
        UpdateLoadBalancerService,
    },
};

use super::legacy::LegacyOwnership;
use crate::{
    consts,
    error::{RobotLBError, RobotLBResult},
};

/// Trait for interacting with the Hetzner Cloud Load Balancer API.
///
/// This trait abstracts the Hetzner Cloud API operations, allowing for
/// mock implementations in tests.
#[async_trait]
pub trait HcloudLoadBalancerApi: Send + Sync {
    /// Update an existing service on the load balancer.
    async fn update_service(
        &self,
        load_balancer_id: i64,
        service: UpdateLoadBalancerService,
    ) -> RobotLBResult<()>;

    /// Delete a service from the load balancer.
    async fn delete_service(&self, load_balancer_id: i64, listen_port: i32) -> RobotLBResult<()>;

    /// Add a new service to the load balancer.
    async fn add_service(
        &self,
        load_balancer_id: i64,
        service: LoadBalancerService,
    ) -> RobotLBResult<()>;

    /// Remove a target from the load balancer.
    async fn remove_target(
        &self,
        load_balancer_id: i64,
        target_ip: String,
    ) -> RobotLBResult<hcloud::models::Action>;

    /// Add a target to the load balancer.
    async fn add_target(
        &self,
        load_balancer_id: i64,
        target_ip: String,
    ) -> RobotLBResult<hcloud::models::Action>;

    /// Change the load balancing algorithm.
    async fn change_algorithm(
        &self,
        load_balancer_id: i64,
        algorithm: LoadBalancerAlgorithm,
    ) -> RobotLBResult<()>;

    /// Change the load balancer type.
    async fn change_type(
        &self,
        load_balancer_id: i64,
        balancer_type: String,
    ) -> RobotLBResult<hcloud::models::Action>;

    async fn get_action(&self, action_id: i64) -> RobotLBResult<hcloud::models::Action>;
}

/// Live implementation of the Hetzner Cloud API.
pub struct LiveHcloudLoadBalancerApi {
    pub(crate) hcloud_config: HcloudConfig,
}

#[async_trait]
impl HcloudLoadBalancerApi for LiveHcloudLoadBalancerApi {
    async fn update_service(
        &self,
        load_balancer_id: i64,
        service: UpdateLoadBalancerService,
    ) -> RobotLBResult<()> {
        hcloud::apis::load_balancers_api::update_service(
            &self.hcloud_config,
            UpdateServiceParams {
                id: load_balancer_id,
                body: service,
            },
        )
        .await?;
        Ok(())
    }

    async fn delete_service(&self, load_balancer_id: i64, listen_port: i32) -> RobotLBResult<()> {
        hcloud::apis::load_balancers_api::delete_service(
            &self.hcloud_config,
            DeleteServiceParams {
                id: load_balancer_id,
                delete_service_request: DeleteServiceRequest { listen_port },
            },
        )
        .await?;
        Ok(())
    }

    async fn add_service(
        &self,
        load_balancer_id: i64,
        service: LoadBalancerService,
    ) -> RobotLBResult<()> {
        hcloud::apis::load_balancers_api::add_service(
            &self.hcloud_config,
            AddServiceParams {
                id: load_balancer_id,
                body: service,
            },
        )
        .await?;
        Ok(())
    }

    async fn remove_target(
        &self,
        load_balancer_id: i64,
        target_ip: String,
    ) -> RobotLBResult<hcloud::models::Action> {
        let response = hcloud::apis::load_balancers_api::remove_target(
            &self.hcloud_config,
            RemoveTargetParams {
                id: load_balancer_id,
                remove_target_request: RemoveTargetRequest {
                    ip: Some(Box::new(hcloud::models::LoadBalancerTargetIp {
                        ip: target_ip,
                    })),
                    ..Default::default()
                },
            },
        )
        .await?;
        Ok(*response.action)
    }

    async fn add_target(
        &self,
        load_balancer_id: i64,
        target_ip: String,
    ) -> RobotLBResult<hcloud::models::Action> {
        let response = hcloud::apis::load_balancers_api::add_target(
            &self.hcloud_config,
            AddTargetParams {
                id: load_balancer_id,
                body: LoadBalancerAddTarget {
                    ip: Some(Box::new(hcloud::models::LoadBalancerTargetIp {
                        ip: target_ip,
                    })),
                    ..Default::default()
                },
            },
        )
        .await?;
        Ok(*response.action)
    }

    async fn change_algorithm(
        &self,
        load_balancer_id: i64,
        algorithm: LoadBalancerAlgorithm,
    ) -> RobotLBResult<()> {
        hcloud::apis::load_balancers_api::change_algorithm(
            &self.hcloud_config,
            ChangeAlgorithmParams {
                id: load_balancer_id,
                body: algorithm,
            },
        )
        .await?;
        Ok(())
    }

    async fn change_type(
        &self,
        load_balancer_id: i64,
        balancer_type: String,
    ) -> RobotLBResult<hcloud::models::Action> {
        let response = hcloud::apis::load_balancers_api::change_type_of_load_balancer(
            &self.hcloud_config,
            ChangeTypeOfLoadBalancerParams {
                id: load_balancer_id,
                change_type_of_load_balancer_request: ChangeTypeOfLoadBalancerRequest {
                    load_balancer_type: balancer_type,
                },
            },
        )
        .await?;
        Ok(*response.action)
    }

    async fn get_action(&self, action_id: i64) -> RobotLBResult<hcloud::models::Action> {
        let response = hcloud::apis::load_balancers_api::get_load_balancer_action(
            &self.hcloud_config,
            GetLoadBalancerActionParams { id: action_id },
        )
        .await?;
        Ok(*response.action)
    }
}

/// List load balancers from Hetzner Cloud.
pub async fn list_load_balancers(
    hcloud_config: &HcloudConfig,
    name: Option<String>,
) -> RobotLBResult<hcloud::models::ListLoadBalancersResponse> {
    Ok(hcloud::apis::load_balancers_api::list_load_balancers(
        hcloud_config,
        ListLoadBalancersParams {
            name,
            ..Default::default()
        },
    )
    .await?)
}

/// Fetch only the balancer labelled with this Service's immutable UID.
pub async fn get_owned_load_balancer(
    hcloud_config: &HcloudConfig,
    uid: &str,
) -> RobotLBResult<Option<hcloud::models::LoadBalancer>> {
    if uid.is_empty() {
        return Err(RobotLBError::SkipService);
    }
    let response = hcloud::apis::load_balancers_api::list_load_balancers(
        hcloud_config,
        ListLoadBalancersParams {
            label_selector: Some(format!("{}={uid}", consts::LB_OWNER_LABEL)),
            ..Default::default()
        },
    )
    .await?;
    let mut owned: Vec<_> = response
        .load_balancers
        .into_iter()
        .filter(|lb| {
            lb.labels
                .get(consts::LB_OWNER_LABEL)
                .is_some_and(|owner| owner == uid)
        })
        .collect();
    if owned.len() > 1 {
        return Err(RobotLBError::HCloudError(format!(
            "Multiple balancers owned by UID {uid}"
        )));
    }
    Ok(owned.pop())
}

/// Look up the provider's actual limits instead of hard-coding LB types.
pub async fn get_load_balancer_type(
    hcloud_config: &HcloudConfig,
    name: &str,
) -> RobotLBResult<hcloud::models::LoadBalancerType> {
    let response = hcloud::apis::load_balancer_types_api::list_load_balancer_types(
        hcloud_config,
        hcloud::apis::load_balancer_types_api::ListLoadBalancerTypesParams::default(),
    )
    .await?;
    response
        .load_balancer_types
        .into_iter()
        .find(|lb_type| lb_type.name == name)
        .ok_or_else(|| {
            RobotLBError::InvalidLoadBalancerConfiguration(format!(
                "Hetzner load balancer type {name} is not available"
            ))
        })
}

/// Get a specific load balancer by name.
pub async fn get_load_balancer(
    hcloud_config: &HcloudConfig,
    name: &str,
) -> RobotLBResult<Option<hcloud::models::LoadBalancer>> {
    // Hetzner ignores an empty name filter and may return unrelated balancers.
    // Always match locally before considering a resource for reconciliation.
    let hcloud_balancers = list_load_balancers(hcloud_config, Some(name.to_string())).await?;
    let matching: Vec<_> = hcloud_balancers
        .load_balancers
        .into_iter()
        .filter(|balancer| balancer.name == name)
        .collect();
    if matching.len() > 1 {
        tracing::warn!("Found more than one balancer with name {name}, skipping");
        return Err(RobotLBError::SkipService);
    }
    Ok(matching.into_iter().next())
}

/// Discover an original balancer without modifying it. This is used when
/// 0.7.x already created a UID-owned replacement before discovering the
/// pre-0.7 resource. Multiple matching resources are unsafe to disambiguate.
pub(super) async fn find_legacy_load_balancer(
    hcloud_config: &HcloudConfig,
    uid: &str,
    proof: &LegacyOwnership,
) -> RobotLBResult<Option<hcloud::models::LoadBalancer>> {
    let mut candidate = None;
    for name in proof.names() {
        if let Some(lb) = get_load_balancer(hcloud_config, name).await?
            && proof.labels_for_adoption(&lb, uid).is_some()
        {
            if candidate.is_some() {
                return Err(RobotLBError::InvalidLoadBalancerConfiguration(format!(
                    "Multiple legacy load balancers match Service UID {uid}; refusing ambiguous recovery"
                )));
            }
            candidate = Some(lb);
        }
    }
    Ok(candidate)
}

/// Adopt an unlabelled 0.6.x LB only when the Service's old finalizer and
/// published ingress addresses match the Hetzner resource. No listeners,
/// targets, networks, IDs or IPs are changed during adoption.
///
/// If a name is occupied without verifiable evidence, preserve the 0.7.0
/// safety behavior and refuse to touch the resource.
pub(super) async fn find_or_adopt_legacy_load_balancer(
    hcloud_config: &HcloudConfig,
    uid: &str,
    desired_name: &str,
    proof: Option<&LegacyOwnership>,
) -> RobotLBResult<Option<hcloud::models::LoadBalancer>> {
    let mut names = vec![desired_name.to_string()];
    if let Some(proof) = proof {
        for name in proof.names() {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
    }

    let mut matching = Vec::new();
    let mut occupied = false;
    for name in names {
        if let Some(lb) = get_load_balancer(hcloud_config, &name).await? {
            occupied = true;
            if proof.is_some_and(|proof| proof.labels_for_adoption(&lb, uid).is_some()) {
                matching.push(lb);
            }
        }
    }
    if matching.len() > 1 {
        return Err(RobotLBError::InvalidLoadBalancerConfiguration(format!(
            "Multiple legacy load balancers match Service UID {uid}; refusing ambiguous adoption"
        )));
    }
    if let Some(candidate) = matching.pop() {
        // Re-fetch directly before the write to avoid replacing stale label maps.
        // A concurrent Hetzner update yields a conflict and the next reconcile
        // rechecks the full ownership evidence rather than retrying a stale PUT.
        let fresh = get_load_balancer(hcloud_config, &candidate.name)
            .await?
            .filter(|lb| lb.id == candidate.id)
            .ok_or_else(|| {
                RobotLBError::HCloudError(
                    "Legacy load balancer changed during adoption; retrying".to_string(),
                )
            })?;
        let labels = proof
            .and_then(|proof| proof.labels_for_adoption(&fresh, uid))
            .ok_or_else(|| {
                RobotLBError::InvalidLoadBalancerConfiguration(
                    "Legacy load balancer no longer matches Service ownership evidence".to_string(),
                )
            })?;
        hcloud::apis::load_balancers_api::replace_load_balancer(
            hcloud_config,
            ReplaceLoadBalancerParams {
                id: fresh.id,
                replace_load_balancer_request: ReplaceLoadBalancerRequest {
                    labels: Some(labels),
                    name: None,
                },
            },
        )
        .await?;
        // Never proceed with regular reconciliation or deletion until the UID
        // ownership lookup confirms that the same immutable LB ID is ours.
        let owned = get_owned_load_balancer(hcloud_config, uid).await?;
        if let Some(owned) = owned
            && owned.id == fresh.id
        {
            tracing::info!(
                lb_id = owned.id,
                lb_name = %owned.name,
                service_uid = uid,
                "Adopted legacy Hetzner load balancer in place"
            );
            return Ok(Some(owned));
        }
        return Err(RobotLBError::HCloudError(
            "Legacy load balancer label not yet verified; retrying".to_string(),
        ));
    }
    if occupied {
        return Err(RobotLBError::InvalidLoadBalancerConfiguration(format!(
            "LB name {desired_name} is already in use by an unowned resource; refusing to adopt or modify it"
        )));
    }
    Ok(None)
}

/// Create a new load balancer.
pub async fn create_load_balancer(
    hcloud_config: &HcloudConfig,
    name: &str,
    location: &str,
    balancer_type: &str,
    algorithm: LoadBalancerAlgorithm,
    public_interface: bool,
    owner_uid: &str,
) -> RobotLBResult<hcloud::models::LoadBalancer> {
    let response = hcloud::apis::load_balancers_api::create_load_balancer(
        hcloud_config,
        hcloud::apis::load_balancers_api::CreateLoadBalancerParams {
            create_load_balancer_request: hcloud::models::CreateLoadBalancerRequest {
                algorithm: Some(Box::new(algorithm)),
                labels: Some(std::collections::HashMap::from([(
                    consts::LB_OWNER_LABEL.to_string(),
                    owner_uid.to_string(),
                )])),
                load_balancer_type: balancer_type.to_string(),
                location: Some(location.to_string()),
                name: name.to_string(),
                network: None,
                network_zone: None,
                public_interface: Some(public_interface),
                services: Some(vec![]),
                targets: Some(vec![]),
            },
        },
    )
    .await;

    Ok(*response?.load_balancer)
}

/// Delete a load balancer.
pub async fn delete_load_balancer(hcloud_config: &HcloudConfig, id: i64) -> RobotLBResult<()> {
    hcloud::apis::load_balancers_api::delete_load_balancer(
        hcloud_config,
        DeleteLoadBalancerParams { id },
    )
    .await?;
    Ok(())
}

/// Attach a load balancer to a network.
pub async fn attach_to_network(
    hcloud_config: &HcloudConfig,
    id: i64,
    network_id: i64,
    ip: Option<String>,
) -> RobotLBResult<()> {
    hcloud::apis::load_balancers_api::attach_load_balancer_to_network(
        hcloud_config,
        AttachLoadBalancerToNetworkParams {
            id,
            attach_load_balancer_to_network_request: AttachLoadBalancerToNetworkRequest {
                ip,
                ip_range: None,
                network: network_id,
            },
        },
    )
    .await?;
    Ok(())
}

/// Detach a load balancer from a network.
pub async fn detach_from_network(
    hcloud_config: &HcloudConfig,
    id: i64,
    network_id: i64,
) -> RobotLBResult<()> {
    hcloud::apis::load_balancers_api::detach_load_balancer_from_network(
        hcloud_config,
        DetachLoadBalancerFromNetworkParams {
            id,
            detach_load_balancer_from_network_request: DetachLoadBalancerFromNetworkRequest {
                network: network_id,
            },
        },
    )
    .await?;
    Ok(())
}

/// List networks from Hetzner Cloud.
pub async fn list_networks(
    hcloud_config: &HcloudConfig,
    name: Option<String>,
) -> RobotLBResult<hcloud::models::ListNetworksResponse> {
    Ok(hcloud::apis::networks_api::list_networks(
        hcloud_config,
        ListNetworksParams {
            name,
            ..Default::default()
        },
    )
    .await?)
}
