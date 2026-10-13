//! Configuration parsing for load balancers.
//!
//! This module handles parsing Kubernetes service annotations into
//! load balancer configuration.

use std::str::FromStr;

use k8s_openapi::api::core::v1::Service;
use kube::ResourceExt;

use crate::{
    config::OperatorConfig,
    consts,
    error::{RobotLBError, RobotLBResult},
};

use super::types::{LBAlgorithm, ParsedLoadBalancerConfig};

/// Parse load balancer configuration from a Kubernetes service.
///
/// This function extracts configuration from service annotations,
/// falling back to operator defaults when annotations are not present.
///
/// # Errors
///
/// Returns an error if annotation values cannot be parsed or if
/// an invalid algorithm is specified.
pub fn parse_load_balancer_config(
    svc: &Service,
    config: &OperatorConfig,
) -> RobotLBResult<ParsedLoadBalancerConfig> {
    let retries =
        parse_annotation(svc, consts::LB_RETRIES_ANNOTATION)?.unwrap_or(config.default_lb_retries);

    let timeout =
        parse_annotation(svc, consts::LB_TIMEOUT_ANNOTATION)?.unwrap_or(config.default_lb_timeout);

    let check_interval = parse_annotation(svc, consts::LB_CHECK_INTERVAL_ANNOTATION)?
        .unwrap_or(config.default_lb_interval);

    let proxy_mode = parse_annotation(svc, consts::LB_PROXY_MODE_ANNOTATION)?
        .unwrap_or(config.default_lb_proxy_mode_enabled);

    let location = svc
        .annotations()
        .get(consts::LB_LOCATION_ANNOTATION)
        .cloned()
        .unwrap_or_else(|| config.default_lb_location.clone());

    let balancer_type = svc
        .annotations()
        .get(consts::LB_BALANCER_TYPE_ANNOTATION)
        .cloned()
        .unwrap_or_else(|| config.default_balancer_type.clone());

    let algorithm = parse_algorithm(svc, config)?;

    let network_name = svc
        .annotations()
        .get(consts::LB_NETWORK_ANNOTATION)
        .or(config.default_network.as_ref())
        .cloned();

    let name = svc
        .annotations()
        .get(consts::LB_NAME_ANNOTATION)
        .cloned()
        .unwrap_or_else(|| {
            default_name(
                config.cluster_name.as_deref(),
                &svc.name_any(),
                &svc.namespace().unwrap_or_else(|| "default".to_string()),
            )
        });

    if name.is_empty() {
        return Err(RobotLBError::Generic(
            "robotlb/balancer must not be empty".to_string(),
        ));
    }
    if network_name.as_deref() == Some("") {
        return Err(RobotLBError::Generic(
            "robotlb/lb-network must not be empty".to_string(),
        ));
    }

    let private_ip = svc
        .annotations()
        .get(consts::LB_PRIVATE_IP_ANNOTATION)
        .cloned();

    let public_interface =
        parse_annotation(svc, consts::LB_PUBLIC_INTERFACE_ANNOTATION)?.unwrap_or(true);

    if !public_interface && network_name.is_none() {
        return Err(RobotLBError::Generic(format!(
            "{} requires {} to be set when disabled",
            consts::LB_PUBLIC_INTERFACE_ANNOTATION,
            consts::LB_NETWORK_ANNOTATION
        )));
    }

    Ok(ParsedLoadBalancerConfig {
        name,
        private_ip,
        public_interface,
        balancer_type,
        check_interval,
        timeout,
        retries,
        location,
        proxy_mode,
        network_name,
        algorithm: algorithm.into(),
    })
}

const MAX_LB_NAME: usize = 128;

/// A stable 64-bit name suffix. Balancer names are external identities, so the
/// hash algorithm must never change between operator releases.
fn fnv1a64(value: &str) -> u64 {
    value.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

fn default_name(cluster: Option<&str>, service: &str, namespace: &str) -> String {
    let name = cluster.map_or_else(
        || format!("{service}.{namespace}"),
        |cluster| format!("{cluster}.{service}.{namespace}"),
    );
    if name.len() <= MAX_LB_NAME {
        return name;
    }
    // Service, namespace and validated cluster names are ASCII DNS labels.
    let suffix = format!("-{:016x}", fnv1a64(&name));
    format!("{}{}", &name[..MAX_LB_NAME - suffix.len()], suffix)
}

/// Validate the DNS-label cluster prefix at startup instead of failing on an API call.
///
/// # Errors
///
/// Returns an error if the name is not a valid DNS label.
pub fn parse_cluster_name(value: &str) -> Result<String, String> {
    let ascii_alnum = |ch: char| ch.is_ascii_lowercase() || ch.is_ascii_digit();
    let valid = (1..=63).contains(&value.len())
        && value.chars().all(|ch| ascii_alnum(ch) || ch == '-')
        && value.starts_with(ascii_alnum)
        && value.ends_with(ascii_alnum);
    if valid {
        Ok(value.to_string())
    } else {
        Err("must be a DNS label: 1 to 63 lowercase letters, digits or '-', starting and ending with a letter or digit".to_string())
    }
}

/// Parse a numeric or boolean annotation from a service.
fn parse_annotation<T>(svc: &Service, key: &str) -> RobotLBResult<Option<T>>
where
    T: FromStr,
    RobotLBError: From<T::Err>,
{
    svc.annotations()
        .get(key)
        .map(String::as_str)
        .map(T::from_str)
        .transpose()
        .map_err(Into::into)
}

/// Parse the algorithm annotation or fall back to default.
fn parse_algorithm(svc: &Service, config: &OperatorConfig) -> RobotLBResult<LBAlgorithm> {
    svc.annotations()
        .get(consts::LB_ALGORITHM_ANNOTATION)
        .map(String::as_str)
        .or(Some(&config.default_lb_algorithm))
        .map(LBAlgorithm::from_str)
        .transpose()?
        .ok_or(RobotLBError::UnknownLBAlgorithm)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OperatorConfig;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;
    use tracing::level_filters::LevelFilter;

    fn base_config() -> OperatorConfig {
        OperatorConfig {
            hcloud_token: "token".to_string(),
            cluster_name: None,
            default_network: Some("default-net".to_string()),
            dynamic_node_selector: true,
            resync_interval: 300,
            default_lb_retries: 3,
            default_lb_timeout: 10,
            default_lb_interval: 15,
            default_lb_location: "hel1".to_string(),
            default_balancer_type: "lb11".to_string(),
            default_lb_algorithm: "least-connections".to_string(),
            default_lb_proxy_mode_enabled: false,
            ipv6_ingress: false,
            leader_election_namespace: None,
            leader_election_lease_name: "robotlb-leader-election".to_string(),
            leader_election_lease_ttl_secs: 15,
            leader_election_renew_interval_secs: 5,
            log_level: LevelFilter::INFO,
            tracing_enabled: false,
            otlp_endpoint: "http://localhost:4317".to_string(),
            tracing_sample_ratio: 1.0,
            service_name: "robotlb".to_string(),
        }
    }

    fn service_with_annotations(
        annotations: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> Service {
        let annotation_map = annotations
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<BTreeMap<_, _>>();

        Service {
            metadata: ObjectMeta {
                name: Some("svc-name".to_string()),
                annotations: Some(annotation_map),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn uses_defaults_when_annotations_are_missing() {
        let config = base_config();
        let svc = service_with_annotations([]);

        let parsed = parse_load_balancer_config(&svc, &config).expect("parse should succeed");

        assert_eq!(parsed.name, "svc-name.default");
        assert_eq!(parsed.retries, 3);
        assert_eq!(parsed.timeout, 10);
        assert_eq!(parsed.check_interval, 15);
        assert_eq!(parsed.location, "hel1");
        assert_eq!(parsed.balancer_type, "lb11");
        assert_eq!(parsed.network_name.as_deref(), Some("default-net"));
        assert!(!parsed.proxy_mode);
        assert!(parsed.public_interface);
    }

    #[test]
    fn cluster_prefix_only_changes_unannotated_names() {
        let mut config = base_config();
        config.cluster_name = Some("prod".to_string());
        let svc = service_with_annotations([]);
        let parsed = parse_load_balancer_config(&svc, &config).unwrap();
        assert_eq!(parsed.name, "prod.svc-name.default");
        let annotated = service_with_annotations([(consts::LB_NAME_ANNOTATION, "custom")]);
        assert_eq!(
            parse_load_balancer_config(&annotated, &config)
                .unwrap()
                .name,
            "custom"
        );
    }

    #[test]
    fn stable_hashed_names_fit_hetzner_limit() {
        let part = "a".repeat(63);
        let whole = format!("{part}.{part}.{part}");
        let generated = default_name(Some(&part), &part, &part);
        assert_eq!(generated.len(), 128);
        assert_eq!(
            generated,
            format!("{}-{:016x}", &whole[..111], fnv1a64(&whole))
        );
        assert_eq!(fnv1a64("foobar"), 0x8594_4171_f739_67e8);
        assert_ne!(
            default_name(Some(&part), &part, &"b".repeat(63)),
            default_name(Some(&part), &part, &"c".repeat(63))
        );
    }

    #[test]
    fn invalid_cluster_names_are_rejected() {
        for invalid in [
            "",
            "Prod",
            "prod.eu",
            "-prod",
            "prod-",
            "some_name",
            "prød",
            &"a".repeat(64),
        ] {
            assert!(parse_cluster_name(invalid).is_err(), "{invalid:?}");
        }
        for valid in ["a", "prod", "eu-1", &"a".repeat(63)] {
            assert_eq!(parse_cluster_name(valid).as_deref(), Ok(valid));
        }
    }

    #[test]
    fn namespace_is_part_of_default_balancer_name() {
        let config = base_config();
        let mut svc = service_with_annotations([]);
        svc.metadata.namespace = Some("production".to_string());
        let parsed = parse_load_balancer_config(&svc, &config).expect("valid config");
        assert_eq!(parsed.name, "svc-name.production");
    }

    #[test]
    fn rejects_empty_balancer_or_network_name() {
        let config = base_config();
        for (annotation, value) in [
            (consts::LB_NAME_ANNOTATION, ""),
            (consts::LB_NETWORK_ANNOTATION, ""),
        ] {
            let svc = service_with_annotations([(annotation, value)]);
            assert!(parse_load_balancer_config(&svc, &config).is_err());
        }
    }

    #[test]
    fn parses_annotations_into_load_balancer_config() {
        let mut config = base_config();
        config.default_network = None;
        let svc = service_with_annotations([
            (consts::LB_NAME_ANNOTATION, "custom-lb"),
            (consts::LB_RETRIES_ANNOTATION, "5"),
            (consts::LB_TIMEOUT_ANNOTATION, "8"),
            (consts::LB_CHECK_INTERVAL_ANNOTATION, "20"),
            (consts::LB_PROXY_MODE_ANNOTATION, "true"),
            (consts::LB_LOCATION_ANNOTATION, "nbg1"),
            (consts::LB_BALANCER_TYPE_ANNOTATION, "lb31"),
            (consts::LB_ALGORITHM_ANNOTATION, "round-robin"),
            (consts::LB_NETWORK_ANNOTATION, "private-net"),
            (consts::LB_PRIVATE_IP_ANNOTATION, "10.10.0.5"),
            (consts::LB_PUBLIC_INTERFACE_ANNOTATION, "false"),
        ]);

        let parsed = parse_load_balancer_config(&svc, &config).expect("parse should succeed");

        assert_eq!(parsed.name, "custom-lb");
        assert_eq!(parsed.retries, 5);
        assert_eq!(parsed.timeout, 8);
        assert_eq!(parsed.check_interval, 20);
        assert_eq!(parsed.location, "nbg1");
        assert_eq!(parsed.balancer_type, "lb31");
        assert_eq!(parsed.network_name.as_deref(), Some("private-net"));
        assert_eq!(parsed.private_ip.as_deref(), Some("10.10.0.5"));
        assert!(parsed.proxy_mode);
        assert!(!parsed.public_interface);
        assert_eq!(
            parsed.algorithm.r#type,
            hcloud::models::load_balancer_algorithm::Type::RoundRobin
        );
    }

    #[test]
    fn rejects_private_only_load_balancer_without_network() {
        let mut config = base_config();
        config.default_network = None;
        let svc = service_with_annotations([(consts::LB_PUBLIC_INTERFACE_ANNOTATION, "false")]);

        let result = parse_load_balancer_config(&svc, &config);

        assert!(matches!(result, Err(RobotLBError::Generic(_))));
    }

    #[test]
    fn returns_error_for_invalid_algorithm_annotation() {
        let config = base_config();
        let svc = service_with_annotations([(consts::LB_ALGORITHM_ANNOTATION, "weighted")]);

        let result = parse_load_balancer_config(&svc, &config);
        assert!(matches!(result, Err(RobotLBError::UnknownLBAlgorithm)));
    }
}
