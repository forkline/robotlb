//! Kubernetes Service events: stable reasons, redaction and publication throttling.

use super::*;
use super::event_throttle::EventKey;
use kube::runtime::events::{Event, EventType};

/// Display the last error on the Service without leaking Hetzner API credentials.
/// Event publishing errors must not hide the original reconciliation failure.
pub(super) async fn publish_reconciliation_warning(
    svc: &Service,
    context: &CurrentContext,
    error: &RobotLBError,
) {
    let reason = warning_reason(error);
    let note = event_note(error, &context.config.hcloud_token);
    let event_key = EventKey {
        service_uid: svc.metadata.uid.clone().unwrap_or_else(|| {
            format!("{}/{}", svc.namespace().unwrap_or_default(), svc.name_any())
        }),
        reason: reason.to_string(),
        note: note.clone(),
    };
    let reserved_at = std::time::Instant::now();
    if !context.event_throttle.reserve(&event_key, reserved_at) {
        return;
    }

    let event = Event {
        type_: EventType::Warning,
        action: "Reconcile".to_string(),
        reason: reason.to_string(),
        note: Some(note),
        secondary: None,
    };
    if let Err(publish_error) = context.recorder.publish(&event, &svc.object_ref(&())).await {
        context.event_throttle.release(&event_key, reserved_at);
        tracing::warn!(
            error = %publish_error,
            "Cannot publish reconciliation Warning Event"
        );
    }
}

pub(super) const fn warning_reason(error: &RobotLBError) -> &'static str {
    EventReason::from_error(error).as_str()
}

/// Stable machine-readable reasons; error text is kept only in the Event note.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventReason {
    InvalidConfiguration,
    ProvisioningTimeout,
    SyncFailed,
}

impl EventReason {
    const fn from_error(error: &RobotLBError) -> Self {
        match error {
            RobotLBError::InvalidLoadBalancerConfiguration(_) => Self::InvalidConfiguration,
            RobotLBError::LoadBalancerProvisioningTimeout => Self::ProvisioningTimeout,
            _ => Self::SyncFailed,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfiguration => "InvalidLoadBalancerConfiguration",
            Self::ProvisioningTimeout => "LoadBalancerProvisioningTimeout",
            Self::SyncFailed => "SyncLoadBalancerFailed",
        }
    }
}

/// Kubernetes Events cap notes at 1024 bytes, not characters.
pub(super) fn event_note(error: &RobotLBError, token: &str) -> String {
    const MAX_NOTE_BYTES: usize = 1024;
    let mut note = crate::error::redact(&error.to_string(), token);
    if note.len() > MAX_NOTE_BYTES {
        let mut end = MAX_NOTE_BYTES;
        while !note.is_char_boundary(end) {
            end -= 1;
        }
        note.truncate(end);
    }
    note
}

