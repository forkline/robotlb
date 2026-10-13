//! Constants used throughout the robotlb operator.
//!
//! This module defines annotation keys, label names, and default values
//! for load balancer configuration.

/// Annotation key for specifying the load balancer name.
pub const LB_NAME_ANNOTATION: &str = "robotlb/balancer";
/// Annotation key for custom node selector.
pub const LB_NODE_SELECTOR_ANNOTATION: &str = "robotlb/node-selector";

// LB config
/// Annotation key for health check interval (seconds).
pub const LB_CHECK_INTERVAL_ANNOTATION: &str = "robotlb/lb-check-interval";
/// Annotation key for health check timeout (seconds).
pub const LB_TIMEOUT_ANNOTATION: &str = "robotlb/lb-timeout";
/// Annotation key for health check retries.
pub const LB_RETRIES_ANNOTATION: &str = "robotlb/lb-retries";
/// Annotation key for enabling proxy mode.
pub const LB_PROXY_MODE_ANNOTATION: &str = "robotlb/lb-proxy-mode";
/// Annotation key for network name.
pub const LB_NETWORK_ANNOTATION: &str = "robotlb/lb-network";
/// Annotation key for private IP mode.
pub const LB_PRIVATE_IP_ANNOTATION: &str = "robotlb/lb-private-ip";
/// Annotation key for enabling the public interface.
pub const LB_PUBLIC_INTERFACE_ANNOTATION: &str = "robotlb/lb-public-interface";

/// Annotation key for load balancer location.
pub const LB_LOCATION_ANNOTATION: &str = "robotlb/lb-location";
/// Annotation key for load balancing algorithm.
pub const LB_ALGORITHM_ANNOTATION: &str = "robotlb/lb-algorithm";
/// Annotation key for load balancer type.
pub const LB_BALANCER_TYPE_ANNOTATION: &str = "robotlb/balancer-type";

/// Kubernetes node label excluding the node from external load balancer targets.
pub const EXCLUDE_FROM_LB_LABEL: &str = "node.kubernetes.io/exclude-from-external-load-balancers";

/// Hetzner label identifying the owning Kubernetes Service UID.
pub const LB_OWNER_LABEL: &str = "robotlb/service-uid";

/// Finalizer name used to prevent deletion before cleanup.
pub const FINALIZER_NAME: &str = "robotlb/finalizer";
/// Load balancer class name for Kubernetes services.
pub const ROBOTLB_LB_CLASS: &str = "robotlb";
