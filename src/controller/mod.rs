//! Kubernetes controller for managing load balancer services.
//!
//! This module contains the reconciliation logic for Kubernetes services
//! with the `robotlb` load balancer class.

mod event_throttle;
mod events;
mod node_changes;
mod nodes;
mod reconcile;
mod slice_changes;
mod status;

pub(crate) use event_throttle::EventThrottle;
#[cfg(test)]
use events::{event_note, warning_reason};
#[cfg(test)]
use reconcile::{
    INGRESS_RETRY_INTERVAL, PendingReason, ReconcileOutcome, error_action,
    should_reset_failure_backoff, should_retry_missing_ingress,
};
use reconcile::{on_error, reconcile_service};
use slice_changes::{SliceChanges, SliceTargetMode};

use std::sync::Arc;

#[cfg(test)]
use kube::runtime::controller::Action;
#[cfg(test)]
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::Service;
use kube::{
    ResourceExt,
    runtime::{Controller, WatchStreamExt, reflector::ObjectRef, watcher},
};

use crate::{
    CurrentContext, consts,
    error::{RobotLBError, RobotLBResult},
};

pub(crate) use nodes::{
    apply_desired_state, derive_services, derive_targets, discover_target_nodes,
};
pub(crate) use status::{build_ingress, patch_ingress_status};

/// Check if a service is supported by robotlb.
///
/// Returns an error if the service is not a `LoadBalancer` type or
/// if the load balancer class is not `robotlb`.
///
/// # Errors
///
/// Returns `SkipService` if the service is not supported.
pub fn ensure_service_is_supported(svc: &Service) -> RobotLBResult<()> {
    let svc_type = svc
        .spec
        .as_ref()
        .and_then(|spec| spec.type_.as_ref())
        .map_or("ClusterIP", String::as_str);
    if svc_type != "LoadBalancer" {
        tracing::debug!(
            service_type = svc_type,
            "Service type is not LoadBalancer. Skipping..."
        );
        return Err(RobotLBError::SkipService);
    }

    let lb_class = svc
        .spec
        .as_ref()
        .and_then(|spec| spec.load_balancer_class.as_ref())
        .map_or(consts::ROBOTLB_LB_CLASS, String::as_str);
    if lb_class != consts::ROBOTLB_LB_CLASS {
        tracing::debug!(
            load_balancer_class = lb_class,
            "Load balancer class is not robotlb. Skipping..."
        );
        return Err(RobotLBError::SkipService);
    }

    Ok(())
}

/// Map an `EndpointSlice` to the parent Service.
#[must_use]
pub fn map_endpoint_slice_to_service(
    endpoint_slice: &k8s_openapi::api::discovery::v1::EndpointSlice,
) -> Option<ObjectRef<Service>> {
    let namespace = endpoint_slice.namespace()?;
    let service_name = endpoint_slice
        .metadata
        .labels
        .as_ref()?
        .get("kubernetes.io/service-name")?
        .clone();

    Some(ObjectRef::new(&service_name).within(&namespace))
}

/// Whether an `EndpointSlice` change can move the nodes selected by a Service.
/// Preserve Pod-based Local discovery rather than switching to `EndpointSlice` readiness.
fn slice_target_mode(svc: &Service, dynamic_node_selector: bool) -> Option<SliceTargetMode> {
    if !dynamic_node_selector || ensure_service_is_supported(svc).is_err() {
        return None;
    }
    let spec = svc.spec.as_ref()?;
    if spec.external_traffic_policy.as_deref() != Some("Local") {
        return None;
    }
    Some(
        if spec
            .selector
            .as_ref()
            .is_some_and(|selector| !selector.is_empty())
        {
            SliceTargetMode::PodNodes
        } else {
            SliceTargetMode::ReadyEndpointNodes
        },
    )
}

/// Wake all Services when target-relevant Nodes change or an observed
/// `EndpointSlice` disappears or changes its Service label.
/// Ordinary endpoint changes use the targeted mapper.
fn node_change_signals(
    client: kube::Client,
    slice_changes: Arc<SliceChanges>,
) -> impl futures::Stream<Item = ()> + Send + Sync {
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    let node_sender = sender.clone();
    let node_client = client.clone();
    tokio::spawn(async move {
        let mut changes = node_changes::NodeChanges::default();
        let mut events = std::pin::pin!(
            watcher::watcher(
                kube::Api::<k8s_openapi::api::core::v1::Node>::all(node_client),
                watcher::Config::default(),
            )
            .default_backoff()
        );
        while let Some(event) = events.next().await {
            match event {
                Ok(event) => {
                    if changes.observe(&event) && node_sender.unbounded_send(()).is_err() {
                        return;
                    }
                }
                Err(error) => tracing::warn!(%error, "Node watch failed"),
            }
        }
    });

    // kube-runtime's watch mapper receives an object but not the event kind.
    // A cheap second watch on only EndpointSlice metadata ensures deletion
    // events (and deletions during relists) cannot leave stale target nodes.
    tokio::spawn(async move {
        let mut events = std::pin::pin!(
            watcher::watcher(
                kube::Api::<
                    kube::core::PartialObjectMeta<k8s_openapi::api::discovery::v1::EndpointSlice>,
                >::all(client),
                watcher::Config::default(),
            )
            .default_backoff()
        );
        while let Some(event) = events.next().await {
            match event {
                Ok(event) => {
                    if slice_changes.track(&event) && sender.unbounded_send(()).is_err() {
                        return;
                    }
                }
                Err(error) => tracing::warn!(%error, "EndpointSlice metadata watch failed"),
            }
        }
    });
    receiver
}

/// Create and run the controller.
pub async fn run(
    kube_client: kube::Client,
    context: Arc<CurrentContext>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let controller_shutdown = shutdown.clone();
    let hcloud_token = context.config.hcloud_token.clone();
    let controller = Controller::new(
        kube::Api::<Service>::all(kube_client),
        watcher::Config::default(),
    );
    let services = controller.store();
    let slice_changes = Arc::new(SliceChanges::default());
    let dynamic_node_selector = context.config.dynamic_node_selector;
    controller
        .reconcile_all_on(node_change_signals(
            context.client.clone(),
            slice_changes.clone(),
        ))
        .watches(
            kube::Api::<k8s_openapi::api::discovery::v1::EndpointSlice>::all(
                context.client.clone(),
            ),
            watcher::Config::default(),
            move |endpoint_slice| {
                let service_ref = map_endpoint_slice_to_service(&endpoint_slice)?;
                let service = services.get(&service_ref)?;
                match slice_target_mode(&service, dynamic_node_selector) {
                    Some(mode) if slice_changes.observe(&endpoint_slice, mode) => Some(service_ref),
                    Some(_) => None,
                    None => {
                        slice_changes.forget(&endpoint_slice);
                        None
                    }
                }
            },
        )
        .run(reconcile_service, on_error, context)
        .take_until(controller_shutdown.cancelled())
        .for_each(|reconciliation_result| {
            let token = hcloud_token.clone();
            async move {
                match reconciliation_result {
                    Ok((service, _action)) => {
                        tracing::info!(
                            "Reconciliation of a service {} was successful",
                            service.name
                        );
                    }
                    Err(kube::runtime::controller::Error::ReconcilerFailed(
                        RobotLBError::SkipService,
                        _,
                    )) => {}
                    Err(kube::runtime::controller::Error::ReconcilerFailed(error, service)) => {
                        tracing::error!(
                            service = %service,
                            error = %crate::error::redact(&error.to_string(), &token),
                            "Service reconciliation failed"
                        );
                    }
                    Err(error) => {
                        tracing::error!(
                            error = %crate::error::redact(&error.to_string(), &token),
                            "Controller reconciliation failed"
                        );
                    }
                }
            }
        })
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::fixtures::{service_spec, service_with_spec};

    #[test]
    fn service_filter_rejects_non_load_balancer_type() {
        let svc = service_with_spec(service_spec("ClusterIP", None, vec![]));
        let result = ensure_service_is_supported(&svc);
        assert!(matches!(result, Err(RobotLBError::SkipService)));
    }

    #[test]
    fn service_filter_rejects_foreign_load_balancer_class() {
        let svc = service_with_spec(service_spec(
            "LoadBalancer",
            Some("other-controller"),
            vec![],
        ));
        let result = ensure_service_is_supported(&svc);
        assert!(matches!(result, Err(RobotLBError::SkipService)));
    }

    #[test]
    fn service_filter_accepts_default_robotlb_class() {
        let svc = service_with_spec(service_spec("LoadBalancer", None, vec![]));
        assert!(ensure_service_is_supported(&svc).is_ok());
    }

    #[test]
    fn all_errors_are_reported_except_skipped_services() {
        assert_eq!(
            warning_reason(&RobotLBError::InvalidLoadBalancerConfiguration(
                "too many targets".to_string()
            )),
            "InvalidLoadBalancerConfiguration"
        );
        assert_eq!(
            warning_reason(&RobotLBError::HCloudError("error".to_string())),
            "SyncLoadBalancerFailed"
        );
    }

    #[test]
    fn warning_notes_redact_hetzner_token_and_fit_event_limit() {
        let token = "0123456789abcdef";
        let error = RobotLBError::HCloudError(format!(
            "invalid token {} {}",
            &token[..12],
            "é".repeat(600)
        ));
        let note = event_note(&error, token);
        assert!(note.contains("[REDACTED]"));
        assert!(!note.contains(&token[..12]));
        assert!(note.len() <= 1024);
        assert!(note.is_char_boundary(note.len()));
    }

    #[test]
    fn deferred_reconciliations_preserve_backoff() {
        let rate_limit = crate::rate_limit::RateLimitGate::default();
        let backoff = crate::backoff::ErrorBackoff::default();
        let now = std::time::Instant::now();
        let svc = "shop/web";
        let error = RobotLBError::HCloudError("temporary outage".to_string());
        let first = error_action(&error, &rate_limit, &backoff, now, svc);
        assert_eq!(first, Action::requeue(Duration::from_secs(5)));
        let delayed = Ok(ReconcileOutcome::Deferred(
            PendingReason::RateLimited,
            Action::requeue(Duration::from_secs(60)),
        ));
        assert!(!should_reset_failure_backoff(&delayed));
        // This is the exact shared-429 deferral path: it must not call forget().
        if should_reset_failure_backoff(&delayed) {
            backoff.forget(svc);
        }
        assert_eq!(
            error_action(&error, &rate_limit, &backoff, now, svc),
            Action::requeue(Duration::from_secs(10))
        );
        assert!(should_reset_failure_backoff(&Ok(
            ReconcileOutcome::Deleted(Action::await_change())
        )));
        assert!(should_reset_failure_backoff(&Err(
            RobotLBError::SkipService
        )));
        assert!(!should_reset_failure_backoff(&Err(error)));
    }

    #[test]
    fn provisioning_timeout_emits_a_warning_and_keeps_a_fixed_retry() {
        let gate = crate::rate_limit::RateLimitGate::default();
        let backoff = crate::backoff::ErrorBackoff::default();
        let error = RobotLBError::LoadBalancerProvisioningTimeout;
        assert_eq!(warning_reason(&error), "LoadBalancerProvisioningTimeout");
        assert_eq!(
            error_action(
                &error,
                &gate,
                &backoff,
                std::time::Instant::now(),
                "shop/web"
            ),
            Action::requeue(INGRESS_RETRY_INTERVAL)
        );
    }

    #[test]
    fn an_unassigned_lb_address_is_not_considered_reconciled() {
        // Neither a new public balancer nor a private network attachment should
        // be cached while Hetzner has not returned a routable ingress address.
        assert!(should_retry_missing_ingress(&[]));
        let ingress = vec![k8s_openapi::api::core::v1::LoadBalancerIngress {
            ip: Some("192.0.2.2".to_string()),
            ..Default::default()
        }];
        assert!(!should_retry_missing_ingress(&ingress));
    }

    #[test]
    fn slice_watches_are_only_used_for_dynamic_local_targets() {
        let mut svc = service_with_spec(service_spec("LoadBalancer", None, vec![]));
        assert_eq!(slice_target_mode(&svc, true), None);
        svc.spec.as_mut().unwrap().external_traffic_policy = Some("Local".to_string());
        assert_eq!(
            slice_target_mode(&svc, true),
            Some(SliceTargetMode::ReadyEndpointNodes)
        );
        svc.spec.as_mut().unwrap().selector = Some([("app".to_string(), "web".to_string())].into());
        assert_eq!(
            slice_target_mode(&svc, true),
            Some(SliceTargetMode::PodNodes)
        );
        assert_eq!(slice_target_mode(&svc, false), None);
        svc.spec.as_mut().unwrap().load_balancer_class = Some("other".to_string());
        assert_eq!(slice_target_mode(&svc, true), None);
    }

    #[test]
    fn failure_backoff_is_independent_for_each_service() {
        let gate = crate::rate_limit::RateLimitGate::default();
        let backoff = crate::backoff::ErrorBackoff::default();
        let now = std::time::Instant::now();
        let error = RobotLBError::HCloudError("test".to_string());
        assert_eq!(
            error_action(&error, &gate, &backoff, now, "shop/web"),
            Action::requeue(Duration::from_secs(5))
        );
        assert_eq!(
            error_action(&error, &gate, &backoff, now, "shop/web"),
            Action::requeue(Duration::from_secs(10))
        );
        assert_eq!(
            error_action(&error, &gate, &backoff, now, "shop/api"),
            Action::requeue(Duration::from_secs(5))
        );
        assert_eq!(
            error_action(&RobotLBError::SkipService, &gate, &backoff, now, "shop/web"),
            Action::await_change()
        );
    }
}
