//! Explicit Service reconciliation lifecycle and kube-rs scheduling policy.

use super::events::publish_reconciliation_warning;
use super::{
    apply_desired_state, build_ingress, derive_services, derive_targets, discover_target_nodes,
    ensure_service_is_supported, patch_ingress_status,
};
use crate::{
    CurrentContext,
    error::{RobotLBError, RobotLBResult},
    finalizers,
    lb::{DesiredLoadBalancer, LoadBalancer, LoadBalancerReconcile},
    memo,
    metrics::ReconcileTimer,
};
use k8s_openapi::api::core::v1::Service;
use kube::{Resource, ResourceExt, runtime::controller::Action};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

/// Whether reconciliation completed or intentionally deferred without applying state.
/// Deferred attempts must not clear the Service's failure backoff or populate the memo.
/// The outcome determines backoff/memo policy independently of kube-rs scheduling.
#[derive(Debug)]
pub(super) enum ReconcileOutcome {
    Synced(Action),
    Unchanged(Action),
    Deleted(Action),
    Deferred(PendingReason, Action),
}

#[derive(Debug, Clone, Copy)]
pub(super) enum PendingReason {
    RateLimited,
    Provisioning,
    TypeChange,
    TargetChange,
}

impl ReconcileOutcome {
    const fn was_successful(&self) -> bool {
        matches!(
            self,
            Self::Synced(_) | Self::Unchanged(_) | Self::Deleted(_)
        )
    }

    fn into_action(self) -> Action {
        match self {
            Self::Synced(action) | Self::Unchanged(action) | Self::Deleted(action) => action,
            Self::Deferred(reason, action) => {
                tracing::debug!(?reason, "reconciliation deferred");
                action
            }
        }
    }
}

pub(super) const INGRESS_RETRY_INTERVAL: Duration = Duration::from_secs(15);
const INGRESS_PROVISIONING_TIMEOUT: Duration = Duration::from_secs(180);

pub(super) const fn should_reset_failure_backoff(result: &RobotLBResult<ReconcileOutcome>) -> bool {
    matches!(
        result,
        Ok(ReconcileOutcome::Synced(_)
            | ReconcileOutcome::Unchanged(_)
            | ReconcileOutcome::Deleted(_))
            | Err(RobotLBError::SkipService)
    )
}

/// Reconcile the service.
///
/// This function is called by the controller for each service.
/// It will create or update the load balancer based on the service.
/// If the service is being deleted, it will clean up the resources.
///
/// # Errors
///
/// Returns an error if the service is not supported, or if the Kubernetes/Hetzner API call fails.
#[tracing::instrument(skip(svc, context), fields(service = svc.name_any()))]
pub(super) async fn reconcile_service(
    svc: Arc<Service>,
    context: Arc<CurrentContext>,
) -> RobotLBResult<Action> {
    let mut timer = ReconcileTimer::new(context.metrics.clone());
    let result = reconcile_service_inner(svc.clone(), context.clone()).await;
    context.memo.settle(
        &svc,
        result.as_ref().is_ok_and(ReconcileOutcome::was_successful),
    );
    if should_reset_failure_backoff(&result) {
        context.error_backoff.forget(&service_key(&svc));
    }
    if let Err(error) = &result
        && !matches!(error, RobotLBError::SkipService)
    {
        timer.set_failed();
        publish_reconciliation_warning(&svc, &context, error).await;
    }
    result.map(ReconcileOutcome::into_action)
}

/// Pure lifecycle classification: a previously managed Service must be cleaned up
/// even if its type or loadBalancerClass was changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceDisposition {
    Ignore,
    Cleanup,
    Manage,
}

fn classify_service(svc: &Service) -> ServiceDisposition {
    // Deletion is always eligible for a best-effort UID-verified cleanup.
    // A finalizer guards deletion but is not the source of cloud ownership:
    // it might have been removed even when an owned LB still exists.
    if svc.meta().deletion_timestamp.is_some()
        || (finalizers::check(svc) && ensure_service_is_supported(svc).is_err())
    {
        ServiceDisposition::Cleanup
    } else if ensure_service_is_supported(svc).is_err() {
        ServiceDisposition::Ignore
    } else {
        ServiceDisposition::Manage
    }
}

async fn reconcile_service_inner(
    svc: Arc<Service>,
    context: Arc<CurrentContext>,
) -> RobotLBResult<ReconcileOutcome> {
    if !context.is_leader.load(Ordering::Relaxed) {
        return Err(RobotLBError::SkipService);
    }
    if let Some(delay) = context.rate_limit.remaining(std::time::Instant::now()) {
        tracing::debug!(
            ?delay,
            "Hetzner API rate limit pause; skipping reconciliation"
        );
        return Ok(ReconcileOutcome::Deferred(
            PendingReason::RateLimited,
            Action::requeue(delay),
        ));
    }

    // Decide cleanup before trying to parse potentially invalid annotations.
    match classify_service(&svc) {
        ServiceDisposition::Cleanup => {
            LoadBalancer::cleanup_service(&svc, &context).await?;
            finalizers::remove(context.client.clone(), &svc).await?;
            return Ok(ReconcileOutcome::Deleted(Action::await_change()));
        }
        ServiceDisposition::Ignore => return Err(RobotLBError::SkipService),
        ServiceDisposition::Manage => {}
    }

    tracing::info!("Starting service reconciliation");

    let desired = DesiredLoadBalancer::from_service(&svc, &context.config)?;

    if !finalizers::check(&svc) {
        finalizers::add(context.client.clone(), &svc).await?;
    }

    reconcile_load_balancer(desired, svc, context).await
}

fn service_key(svc: &Service) -> String {
    svc.uid()
        .unwrap_or_else(|| format!("{}/{}", svc.namespace().unwrap_or_default(), svc.name_any()))
}

/// Reconcile the `LoadBalancer` type of service.
///
/// This function will find the nodes based on the node selector
/// and create or update the load balancer.
///
/// # Errors
///
/// Returns an error if node discovery fails, or if the Kubernetes/Hetzner API call fails.
async fn reconcile_load_balancer(
    mut desired: DesiredLoadBalancer,
    svc: Arc<Service>,
    context: Arc<CurrentContext>,
) -> RobotLBResult<ReconcileOutcome> {
    let desired_ip_type = desired.ip_type();

    let nodes = discover_target_nodes(&svc, &context).await?;
    let desired_targets = derive_targets(nodes, desired_ip_type);
    let desired_services = derive_services(&svc);

    // A Service without an allocated TCP nodePort cannot forward any traffic.
    // Do not provision a billed but unusable Hetzner balancer.
    if desired_services.is_empty() {
        return Err(RobotLBError::Generic(
            "Service has no TCP port with an allocated nodePort".to_string(),
        ));
    }

    apply_desired_state(&mut desired, desired_targets, desired_services);
    let lb = desired.into_load_balancer(context.hcloud_config.clone());

    let uid = svc.uid();
    let fingerprint = memo::fingerprint(&svc, &lb.targets, &lb.services);
    if let Some((wait, ingress)) = uid.as_deref().and_then(|uid| {
        context
            .memo
            .unchanged(uid, fingerprint, std::time::Instant::now())
    }) {
        tracing::debug!("No effective configuration change; skipping Hetzner API");
        // Status is not part of the fingerprint: repair it without Hetzner calls.
        patch_ingress_status(&svc, &context, ingress).await?;
        return Ok(ReconcileOutcome::Unchanged(Action::requeue(wait)));
    }

    let (hcloud_lb, targets_pending) = match lb.reconcile().await? {
        LoadBalancerReconcile::Reconciled(lb) => (lb, false),
        LoadBalancerReconcile::TypeChangePending => {
            // The resize Action finished; read the new provider capacity on
            // the next pass before touching any targets.
            return Ok(ReconcileOutcome::Deferred(
                PendingReason::TypeChange,
                Action::requeue(Duration::from_secs(1)),
            ));
        }
        LoadBalancerReconcile::TargetsPending(lb) => (lb, true),
    };

    let ingress = build_ingress(
        &hcloud_lb,
        context.config.ipv6_ingress,
        lb.proxy_mode,
        lb.public_interface,
    );
    // Ingress publication must not wait for all target changes. ExternalDNS
    // can use the LB address while Hetzner completes target reconciliation.
    // Never memoize the unfinished target set.
    if !ingress.is_empty() {
        patch_ingress_status(&svc, &context, ingress.clone()).await?;
        if let Some(uid) = uid.as_deref() {
            context.memo.clear_pending(uid);
        }
    }
    if targets_pending {
        return Ok(ReconcileOutcome::Deferred(
            PendingReason::TargetChange,
            Action::requeue(Duration::from_secs(1)),
        ));
    }

    // Hetzner may acknowledge creation or network attachment before assigning an
    // address. A successful API response is not a complete LoadBalancer reconcile
    // until Kubernetes can advertise an ingress address. In particular, never
    // memoize an empty result and postpone address discovery for five minutes.
    if should_retry_missing_ingress(&ingress) {
        if uid.as_deref().is_some_and(|uid| {
            context.memo.missing_ingress_overdue(
                uid,
                fingerprint,
                std::time::Instant::now(),
                INGRESS_PROVISIONING_TIMEOUT,
            )
        }) {
            return Err(RobotLBError::LoadBalancerProvisioningTimeout);
        }
        tracing::info!(
            service = %svc.name_any(),
            "Load balancer has no ingress address yet; retrying shortly"
        );
        return Ok(ReconcileOutcome::Deferred(
            PendingReason::Provisioning,
            Action::requeue(INGRESS_RETRY_INTERVAL),
        ));
    }
    let resync = Duration::from_secs(context.config.resync_interval);
    if let Some(uid) = uid {
        context.memo.record(
            &uid,
            fingerprint,
            std::time::Instant::now(),
            resync,
            ingress,
        );
    }
    Ok(ReconcileOutcome::Synced(Action::requeue(resync)))
}

pub(super) const fn should_retry_missing_ingress(
    ingress: &[k8s_openapi::api::core::v1::LoadBalancerIngress],
) -> bool {
    ingress.is_empty()
}

/// Schedule failures independently, but pause the entire project on rate limits.
#[allow(clippy::needless_pass_by_value)]
pub(super) fn on_error(
    svc: Arc<Service>,
    error: &RobotLBError,
    context: Arc<CurrentContext>,
) -> Action {
    error_action(
        error,
        &context.rate_limit,
        &context.error_backoff,
        std::time::Instant::now(),
        &service_key(&svc),
    )
}

pub(super) fn error_action(
    error: &RobotLBError,
    rate_limit: &crate::rate_limit::RateLimitGate,
    backoff: &crate::backoff::ErrorBackoff,
    now: std::time::Instant,
    service: &str,
) -> Action {
    match error {
        RobotLBError::SkipService => Action::await_change(),
        // Provisioning keeps a fixed polling interval even after emitting an error.
        RobotLBError::LoadBalancerProvisioningTimeout => Action::requeue(INGRESS_RETRY_INTERVAL),
        error if error.is_rate_limited() => {
            let delay = rate_limit.on_rate_limit(now);
            tracing::warn!(
                ?delay,
                "Hetzner API rate limit reached; pausing all Services"
            );
            Action::requeue(delay)
        }
        _ => Action::requeue(backoff.on_failure(service, now)),
    }
}

#[cfg(test)]
mod policy_tests {
    use super::*;
    use crate::consts;
    use crate::test_utils::fixtures::{service_spec, service_with_spec};

    #[test]
    fn service_lifecycle_preserves_cleanup_when_type_or_class_changes() {
        let mut svc = service_with_spec(service_spec("LoadBalancer", None, vec![]));
        assert_eq!(classify_service(&svc), ServiceDisposition::Manage);

        svc.spec.as_mut().unwrap().load_balancer_class = Some("other".to_string());
        assert_eq!(classify_service(&svc), ServiceDisposition::Ignore);

        svc.metadata.finalizers = Some(vec![consts::FINALIZER_NAME.to_string()]);
        assert_eq!(classify_service(&svc), ServiceDisposition::Cleanup);

        svc.spec.as_mut().unwrap().type_ = Some("ClusterIP".to_string());
        assert_eq!(classify_service(&svc), ServiceDisposition::Cleanup);

        svc.metadata.finalizers = None;
        assert_eq!(classify_service(&svc), ServiceDisposition::Ignore);
    }

    #[test]
    fn deleting_managed_service_always_enters_cleanup() {
        let mut svc = service_with_spec(service_spec("LoadBalancer", None, vec![]));
        svc.metadata.deletion_timestamp =
            Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                k8s_openapi::jiff::Timestamp::now(),
            ));
        // Even without a finalizer, an owned Hetzner resource may still exist.
        assert_eq!(classify_service(&svc), ServiceDisposition::Cleanup);
        svc.metadata.finalizers = Some(vec![consts::FINALIZER_NAME.to_string()]);
        assert_eq!(classify_service(&svc), ServiceDisposition::Cleanup);
    }

    #[test]
    fn typed_outcomes_preserve_requeue_and_cache_policy() {
        for outcome in [
            ReconcileOutcome::Synced(Action::requeue(Duration::from_secs(300))),
            ReconcileOutcome::Unchanged(Action::requeue(Duration::from_secs(300))),
            ReconcileOutcome::Deleted(Action::await_change()),
        ] {
            assert!(outcome.was_successful());
            assert!(should_reset_failure_backoff(&Ok(outcome)));
        }
        for reason in [
            PendingReason::RateLimited,
            PendingReason::Provisioning,
            PendingReason::TypeChange,
            PendingReason::TargetChange,
        ] {
            let pending =
                ReconcileOutcome::Deferred(reason, Action::requeue(Duration::from_secs(15)));
            assert!(!pending.was_successful());
            assert!(!should_reset_failure_backoff(&Ok(pending)));
            let scheduled =
                ReconcileOutcome::Deferred(reason, Action::requeue(Duration::from_secs(15)));
            assert_eq!(
                scheduled.into_action(),
                Action::requeue(Duration::from_secs(15))
            );
        }
    }

    #[test]
    fn ignored_service_is_not_reported_as_a_retryable_failure() {
        let ignored: RobotLBResult<ReconcileOutcome> = Err(RobotLBError::SkipService);
        assert!(should_reset_failure_backoff(&ignored));
        assert_eq!(
            error_action(
                &RobotLBError::SkipService,
                &crate::rate_limit::RateLimitGate::default(),
                &crate::backoff::ErrorBackoff::default(),
                std::time::Instant::now(),
                "default/svc",
            ),
            Action::await_change(),
        );
    }

    #[test]
    fn failed_reconciliation_invalidates_success_memo() {
        let mut svc = service_with_spec(service_spec("LoadBalancer", None, vec![]));
        svc.metadata.uid = Some("service-uid".to_string());
        let cache = memo::ReconcileMemo::default();
        let now = std::time::Instant::now();
        cache.record("service-uid", 123, now, Duration::from_secs(300), vec![]);
        assert!(cache.unchanged("service-uid", 123, now).is_some());

        cache.settle(&svc, true);
        assert!(cache.unchanged("service-uid", 123, now).is_some());

        cache.settle(&svc, false);
        assert!(cache.unchanged("service-uid", 123, now).is_none());
    }
}
