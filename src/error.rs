//! Error types for the robotlb operator.
//!
//! This module defines all error types used throughout the operator,
//! including Kubernetes API errors, Hetzner Cloud API errors, and
//! internal validation errors.

use thiserror::Error;

/// Result type alias for robotlb operations.
pub type RobotLBResult<T> = Result<T, RobotLBError>;

fn is_rate_limit<E>(err: &hcloud::apis::Error<E>) -> bool {
    matches!(err, hcloud::apis::Error::ResponseError(response) if response.status.as_u16() == 429)
}

/// Remove the Hetzner API token (including a quoted prefix) from diagnostics.
///
/// Hetzner error bodies can contain a token prefix, not only the complete token.
/// Eight bytes is the shortest prefix we treat as secret so short unrelated
/// text does not get unnecessarily replaced.
#[must_use]
pub fn redact(message: &str, token: &str) -> String {
    const MIN_PREFIX: usize = 8;
    let mut safe = message.to_string();
    for len in (MIN_PREFIX..=token.len()).rev() {
        if let Some(prefix) = token.get(..len) {
            safe = safe.replace(prefix, "[REDACTED]");
        }
    }
    safe
}

/// Format a Hetzner error with its concrete network/TLS/DNS cause.
/// The caller redacts the configured token before writing it to logs or Events.
fn describe_hcloud<T>(error: &hcloud::apis::Error<T>) -> String {
    use std::error::Error as _;
    let (mut description, mut cause): (String, Option<&(dyn std::error::Error + 'static)>) =
        match error {
            hcloud::apis::Error::Reqwest(inner) => (inner.to_string(), inner.source()),
            hcloud::apis::Error::Io(inner) => (inner.to_string(), inner.source()),
            hcloud::apis::Error::Serde(inner) => (inner.to_string(), inner.source()),
            hcloud::apis::Error::ResponseError(response) => {
                let detail = k8s_openapi::serde_json::from_str::<k8s_openapi::serde_json::Value>(
                    &response.content,
                )
                .ok()
                .and_then(|value| {
                    Some(format!(
                        "{}: {}",
                        value.pointer("/error/code")?.as_str()?,
                        value.pointer("/error/message")?.as_str()?,
                    ))
                });
                let message = detail.map_or_else(
                    || response.status.to_string(),
                    |text| format!("{}: {text}", response.status),
                );
                (message, None)
            }
        };
    while let Some(source) = cause {
        description.push_str(": ");
        description.push_str(&source.to_string());
        cause = source.source();
    }
    // The same message is also emitted in Kubernetes Events, which are line-oriented.
    description.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// All possible errors that can occur in the robotlb operator.
#[derive(Debug, Error)]
pub enum RobotLBError {
    #[error("Cannot parse node filter: {0}")]
    InvalidNodeFilter(String),
    #[error("Unsupported service type")]
    UnsupportedServiceType,
    #[error("Service was skipped")]
    SkipService,
    #[error("Cannot parse integer value: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("Cannot parse boolean value: {0}")]
    ParseBoolError(#[from] std::str::ParseBoolError),
    #[error("HCloud error: {0}")]
    HCloudError(String),
    #[error("Invalid load balancer configuration: {0}")]
    InvalidLoadBalancerConfiguration(String),
    #[error("Cannot list Hetzner load balancer types: {}", describe_hcloud(.0))]
    HCloudListLoadBalancerTypesError(
        #[from]
        hcloud::apis::Error<hcloud::apis::load_balancer_types_api::ListLoadBalancerTypesError>,
    ),
    #[error("Kube error: {0}")]
    KubeError(#[from] kube::Error),
    #[error("Unknown load balancing algorithm")]
    UnknownLBAlgorithm,
    #[error("Cannot get target nodes because the service has no selector")]
    ServiceWithoutSelector,

    // HCloud API errors
    #[error("Cannot attach load balancer to a network. Reason: {}", describe_hcloud(.0))]
    HCloudLBAttachToNetworkError(
        #[from]
        hcloud::apis::Error<hcloud::apis::load_balancers_api::AttachLoadBalancerToNetworkError>,
    ),
    #[error("Cannot detach load balancer from network. Reason: {}", describe_hcloud(.0))]
    HcloudLBDetachFromNetworkError(
        #[from]
        hcloud::apis::Error<hcloud::apis::load_balancers_api::DetachLoadBalancerFromNetworkError>,
    ),
    #[error("Cannot add load balancer target. Reason: {}", describe_hcloud(.0))]
    HcloudLBAddTargetError(
        #[from] hcloud::apis::Error<hcloud::apis::load_balancers_api::AddTargetError>,
    ),
    #[error("Cannot remove load balancer target. Reason: {}", describe_hcloud(.0))]
    HcloudLBRemoveTargetError(
        #[from] hcloud::apis::Error<hcloud::apis::load_balancers_api::RemoveTargetError>,
    ),
    #[error("Cannot add service to load balancer. Reason: {}", describe_hcloud(.0))]
    HcloudLBAddServiceError(
        #[from] hcloud::apis::Error<hcloud::apis::load_balancers_api::AddServiceError>,
    ),
    #[error("Cannot remove service from load balancer. Reason: {}", describe_hcloud(.0))]
    HcloudLBRemoveServiceError(
        #[from] hcloud::apis::Error<hcloud::apis::load_balancers_api::DeleteServiceError>,
    ),
    #[error("Cannot create load balancer. Reason: {}", describe_hcloud(.0))]
    HcloudLBCreateError(
        #[from] hcloud::apis::Error<hcloud::apis::load_balancers_api::CreateLoadBalancerError>,
    ),
    #[error("Cannot delete load balancer. Reason: {}", describe_hcloud(.0))]
    HcloudLBDeleteError(
        #[from] hcloud::apis::Error<hcloud::apis::load_balancers_api::DeleteLoadBalancerError>,
    ),
    #[error("Cannot get load balancer. Reason: {}", describe_hcloud(.0))]
    HcloudLBGetError(
        #[from] hcloud::apis::Error<hcloud::apis::load_balancers_api::GetLoadBalancerError>,
    ),
    #[error("Cannot update service. Reason: {}", describe_hcloud(.0))]
    HcloudLBUpdateServiceError(
        #[from] hcloud::apis::Error<hcloud::apis::load_balancers_api::UpdateServiceError>,
    ),
    #[error("Cannot change type of load balancer. Reason: {}", describe_hcloud(.0))]
    HcloudLBChangeType(
        #[from]
        hcloud::apis::Error<hcloud::apis::load_balancers_api::ChangeTypeOfLoadBalancerError>,
    ),
    #[error("Cannot change algorithm of load balancer. Reason: {}", describe_hcloud(.0))]
    HcloudLBChangeAlgorithm(
        #[from] hcloud::apis::Error<hcloud::apis::load_balancers_api::ChangeAlgorithmError>,
    ),
    #[error("Cannot list networks. Reason: {}", describe_hcloud(.0))]
    HcloudListNetworksError(
        #[from] hcloud::apis::Error<hcloud::apis::networks_api::ListNetworksError>,
    ),
    #[error("Cannot list load balancers. Reason: {}", describe_hcloud(.0))]
    HcloudListLoadBalancersError(
        #[from] hcloud::apis::Error<hcloud::apis::load_balancers_api::ListLoadBalancersError>,
    ),
    #[error("{0}")]
    Generic(String),
}

impl RobotLBError {
    /// Whether Hetzner rejected an operation due to the project-wide request quota.
    pub fn is_rate_limited(&self) -> bool {
        match self {
            Self::HCloudLBAttachToNetworkError(e) => is_rate_limit(e),
            Self::HcloudLBDetachFromNetworkError(e) => is_rate_limit(e),
            Self::HcloudLBAddTargetError(e) => is_rate_limit(e),
            Self::HcloudLBRemoveTargetError(e) => is_rate_limit(e),
            Self::HcloudLBAddServiceError(e) => is_rate_limit(e),
            Self::HcloudLBRemoveServiceError(e) => is_rate_limit(e),
            Self::HcloudLBCreateError(e) => is_rate_limit(e),
            Self::HcloudLBDeleteError(e) => is_rate_limit(e),
            Self::HcloudLBGetError(e) => is_rate_limit(e),
            Self::HcloudLBUpdateServiceError(e) => is_rate_limit(e),
            Self::HcloudLBChangeType(e) => is_rate_limit(e),
            Self::HcloudLBChangeAlgorithm(e) => is_rate_limit(e),
            Self::HcloudListNetworksError(e) => is_rate_limit(e),
            Self::HcloudListLoadBalancersError(e) => is_rate_limit(e),
            Self::HCloudListLoadBalancerTypesError(e) => is_rate_limit(e),
            _ => false,
        }
    }
}

fn is_transient_hcloud<E>(err: &hcloud::apis::Error<E>) -> bool {
    let hcloud::apis::Error::ResponseError(response) = err else {
        return false;
    };
    if response.status.as_u16() == 409
        || response.status.as_u16() == 423
        || response.status.is_server_error()
    {
        return true;
    }
    k8s_openapi::serde_json::from_str::<k8s_openapi::serde_json::Value>(&response.content)
        .ok()
        .and_then(|json| {
            json.pointer("/error/code")
                .and_then(k8s_openapi::serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .is_some_and(|code| code == "robot_unavailable")
}

impl RobotLBError {
    /// Temporary Hetzner conflicts and server errors can be retried with a short backoff.
    /// Rate-limit errors (429) use the separate project-wide gate.
    pub fn is_transient_hcloud(&self) -> bool {
        match self {
            Self::HcloudLBAddTargetError(e) => is_transient_hcloud(e),
            Self::HcloudLBRemoveTargetError(e) => is_transient_hcloud(e),
            Self::HcloudLBAddServiceError(e) => is_transient_hcloud(e),
            Self::HcloudLBRemoveServiceError(e) => is_transient_hcloud(e),
            Self::HcloudLBUpdateServiceError(e) => is_transient_hcloud(e),
            Self::HcloudLBChangeType(e) => is_transient_hcloud(e),
            Self::HcloudLBChangeAlgorithm(e) => is_transient_hcloud(e),
            Self::HCloudLBAttachToNetworkError(e) => is_transient_hcloud(e),
            Self::HcloudLBDetachFromNetworkError(e) => is_transient_hcloud(e),
            Self::HcloudLBDeleteError(e) => is_transient_hcloud(e),
            _ => false,
        }
    }

    /// The target was added by a previous attempt whose response was lost.
    pub fn target_already_defined(&self) -> bool {
        let Self::HcloudLBAddTargetError(hcloud::apis::Error::ResponseError(response)) = self
        else {
            return false;
        };
        k8s_openapi::serde_json::from_str::<k8s_openapi::serde_json::Value>(&response.content)
            .ok()
            .and_then(|body| body.pointer("/error/code")?.as_str().map(str::to_owned))
            .is_some_and(|code| code == "target_already_defined")
    }
}

#[cfg(test)]
mod redaction_tests {
    use super::redact;

    const TOKEN: &str = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ01";

    #[test]
    fn redacts_complete_token_and_quoted_prefix() {
        assert_eq!(
            redact(&format!("bad token {TOKEN}"), TOKEN),
            "bad token [REDACTED]"
        );
        assert_eq!(
            redact(&format!("invalid token {}", &TOKEN[..24]), TOKEN),
            "invalid token [REDACTED]"
        );
    }

    #[test]
    fn leaves_unrelated_messages_and_short_prefixes_intact() {
        assert_eq!(redact("Hetzner 500", TOKEN), "Hetzner 500");
        assert_eq!(redact("short abcdefg", TOKEN), "short abcdefg");
        assert_eq!(redact("empty token", ""), "empty token");
    }

    #[test]
    fn hetzner_response_describes_status_code_and_message() {
        use hcloud::apis::{Error, ResponseContent, load_balancers_api::ListLoadBalancersError};

        let error = Error::<ListLoadBalancersError>::ResponseError(ResponseContent {
            status: 503_u16.try_into().unwrap(),
            content: r#"{"error":{"code":"robot_unavailable","message":"back end busy"}}"#.to_string(),
            entity: None,
        });
        let description = super::describe_hcloud(&error);
        assert!(description.contains("503"), "{description}");
        assert!(description.contains("robot_unavailable"), "{description}");
        assert!(description.contains("back end busy"), "{description}");
    }

    #[test]
    fn malformed_hetzner_response_falls_back_to_http_status() {
        use hcloud::apis::{Error, ResponseContent, load_balancers_api::ListLoadBalancersError};
        let error = Error::<ListLoadBalancersError>::ResponseError(ResponseContent {
            status: 502_u16.try_into().unwrap(),
            content: "<html>bad gateway</html>".to_string(),
            entity: None,
        });
        assert!(super::describe_hcloud(&error).contains("502"));
    }

    #[test]
    fn redacts_non_ascii_surrounding_text() {
        assert_eq!(
            redact(&format!("échec {} 🦀", &TOKEN[..8]), TOKEN),
            "échec [REDACTED] 🦀"
        );
    }
}
