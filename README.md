# Hetzner LoadBalancer for bare-metal robot clusters

This project is useful when you've deployed a bare-metal Kubernetes cluster on Hetzner Robot and
want to use Hetzner's cloud load balancer.

This small operator integrates them together, allowing you to use the `LoadBalancer` service type.

You can follow the [TUTORIAL.md](./tutorial.md) to see how to set up a cluster using RobotLB from
scratch.

## Prerequisites

Before using this operator, make sure:

1. You have a cluster deployed on [Hetzner robot](https://robot.hetzner.com/) (at least agent
   nodes);
2. You've created a [vSwitch](https://docs.hetzner.com/robot/dedicated-server/network/vswitch/) for
   these servers;
3. You've assigned IPs to your dedicated servers within the vSwitch network.
4. You have a cloud network with subnet that points to the vSwitch
   ([Tutorial](https://docs.hetzner.com/cloud/networks/connect-dedi-vswitch/));
5. You’ve specified node IPs using the `--node-ip` argument with the private IP.

If you meet all the requirements, you can deploy `robotlb`.

## Deploying

The recommended way to deploy this operator is using the Helm chart.

```bash
helm show values oci://ghcr.io/intreecom/charts/robotlb > values.yaml
# Edit values.yaml to suit your needs
# Set `envs.ROBOTLB_HCLOUD_TOKEN`.
helm install robotlb oci://ghcr.io/intreecom/charts/robotlb -f values.yaml
```

After the chart is installed, you should be able to create `LoadBalancer` services.

### High availability

`robotlb` supports safe multi-replica deployment via Kubernetes Lease-based leader election. Run at
least 2 replicas and use pod anti-affinity so only one pod is active leader while the other is
standby.

Example Helm values:

```yaml
replicaCount: 2

affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            app.kubernetes.io/name: robotlb
            app.kubernetes.io/instance: robotlb
        topologyKey: kubernetes.io/hostname
```

## How it works

The operator listens to the Kubernetes API for services of type `LoadBalancer` and creates Hetzner
load balancers that point to nodes based on `node-ip`.

With `ROBOTLB_DYNAMIC_NODE_SELECTOR=true`, target nodes respect the Service's
`externalTrafficPolicy`:

- `Cluster` (default): all Ready, schedulable nodes except those labelled
  `node.kubernetes.io/exclude-from-external-load-balancers`.
- `Local`: nodes hosting the Service's pods; selectorless Services instead use
  ready `EndpointSlice` endpoints. Unready and excluded nodes are omitted.
- With dynamic selection disabled, `robotlb/node-selector` selects targets, also
  excluding unready and excluded nodes.

The controller watches `EndpointSlice` changes and resyncs all Services periodically.
At least one TCP Service port must have an allocated `nodePort`; a Service with
no exposable ports does not create a billable but unusable balancer.
The number of targets and listeners must fit the chosen Hetzner LB type.

### Ownership and safe configuration

Every load balancer created by robotlb carries the Hetzner label
`robotlb/service-uid=<Kubernetes Service UID>`. Only a balancer with the
matching UID can be reconciled or deleted. Names and overlapping IP targets
are **not** evidence of ownership.

Without `robotlb/balancer`, new balancers are named
`<service>.<namespace>`. An unowned balancer using the desired name causes
reconciliation to fail; robotlb does not adopt or modify it automatically.
For an existing unlabelled balancer that must retain its IP, **verify the
ownership manually** before adding the `robotlb/service-uid` label.
Legacy unlabelled balancers are never deleted by robotlb. Audit them separately
when upgrading to avoid resource leakage.

The operator validates the desired Hetzner type's target and listener limits
before creating an LB or changing its configuration. **It never silently
truncates the target list and never removes existing healthy targets to
satisfy an LB type downgrade.** When limits are exceeded or ownership is
ambiguous, the operation fails without changing an existing load balancer
and emits a Kubernetes Warning Event on the Service. Reconciliation failures
are also logged and counted in `robotlb_reconcile_failures_total`; enable
`metrics.prometheusRules.enabled` to install the chart's optional
Prometheus reconciliation-failure alert rule, and connect your Alertmanager
to receive notifications.

Use `kubectl describe service -n <namespace> <service>` to inspect
Warning Events. Choose a larger LB type, reduce the set of eligible
nodes, or adjust the Service's listener count before retrying.

When multiple clusters share one Hetzner project, use distinct explicit
names or separate projects. Prefer `spec.loadBalancerClass: robotlb` on
new Services to avoid competing load-balancer controllers.

## Configuration

This project has two places for configuration: environment variables and service annotations.

### Envs

Environment variables are mainly used to override default arguments and provide sensitive
information.

Here’s a complete list of parameters for the operator's binary:

```
Usage: robotlb [OPTIONS] --hcloud-token <HCLOUD_TOKEN>

Options:
  -t, --hcloud-token <HCLOUD_TOKEN>
          `HCloud` API token [env: ROBOTLB_HCLOUD_TOKEN=]
      --default-network <DEFAULT_NETWORK>
          Default network to use for load balancers. If not set, then only network from the service annotation will be used [env: ROBOTLB_DEFAULT_NETWORK=]
      --dynamic-node-selector
          If enabled, the operator will try to find target nodes based on where the target pods are actually deployed. If disabled, the operator will try to find target nodes based on the node selector [env: ROBOTLB_DYNAMIC_NODE_SELECTOR=]
      --default-lb-retries <DEFAULT_LB_RETRIES>
          Default load balancer healthcheck retries cound [env: ROBOTLB_DEFAULT_LB_RETRIES=] [default: 3]
      --default-lb-timeout <DEFAULT_LB_TIMEOUT>
          Default load balancer healthcheck timeout [env: ROBOTLB_DEFAULT_LB_TIMEOUT=] [default: 10]
      --default-lb-interval <DEFAULT_LB_INTERVAL>
          Default load balancer healhcheck interval [env: ROBOTLB_DEFAULT_LB_INTERVAL=] [default: 15]
      --default-lb-location <DEFAULT_LB_LOCATION>
          Default location of a load balancer. https://docs.hetzner.com/cloud/general/locations/ [env: ROBOTLB_DEFAULT_LB_LOCATION=] [default: hel1]
      --default-balancer-type <DEFAULT_BALANCER_TYPE>
          Type of a load balancer. It differs in price, number of connections, target servers, etc. The default value is the smallest balancer. https://docs.hetzner.com/cloud/load-balancers/overview#pricing [env: ROBOTLB_DEFAULT_LB_TYPE=] [default: lb11]
      --default-lb-algorithm <DEFAULT_LB_ALGORITHM>
          Default load balancer algorithm. Possible values: * `least-connections` * `round-robin` https://docs.hetzner.com/cloud/load-balancers/overview#load-balancers [env: ROBOTLB_DEFAULT_LB_ALGORITHM=] [default: least-connections]
      --default-lb-proxy-mode-enabled
          Default load balancer proxy mode. If enabled, the load balancer will act as a proxy for the target servers. The default value is `false`. https://docs.hetzner.com/cloud/load-balancers/faq/#what-does-proxy-protocol-mean-and-should-i-enable-it [env: ROBOTLB_DEFAULT_LB_PROXY_MODE_ENABLED=]
      --ipv6-ingress
          Whether to enable IPv6 ingress for the load balancer. If enabled, the load balancer's IPv6 will be attached to the service as an external IP along with IPv4 [env: ROBOTLB_IPV6_INGRESS=]
      --leader-election-namespace <LEADER_ELECTION_NAMESPACE>
          Optional namespace for the leader election lease. If not set, robotlb auto-detects runtime namespace [env: ROBOTLB_LEADER_ELECTION_NAMESPACE=]
      --leader-election-lease-name <LEADER_ELECTION_LEASE_NAME>
          Name of the Kubernetes Lease object used for leader election [env: ROBOTLB_LEADER_ELECTION_LEASE_NAME=] [default: robotlb-leader-election]
      --leader-election-lease-ttl-secs <LEADER_ELECTION_LEASE_TTL_SECS>
          Lease TTL in seconds [env: ROBOTLB_LEADER_ELECTION_LEASE_TTL_SECS=] [default: 15]
      --leader-election-renew-interval-secs <LEADER_ELECTION_RENEW_INTERVAL_SECS>
          How often to attempt lease acquire/renew in seconds [env: ROBOTLB_LEADER_ELECTION_RENEW_INTERVAL_SECS=] [default: 5]
      --log-level <LOG_LEVEL>
          [env: ROBOTLB_LOG_LEVEL=] [default: INFO]
  -h, --help
          Print help
```

When deploying without the Helm chart, ensure RBAC includes:

- `discovery.k8s.io/endpointslices`: `get,list,watch`
- `coordination.k8s.io/leases`: `get,list,watch,create,patch,update`

### Service annotations

```yaml
apiVersion: v1
kind: Service
metadata:
  name: target
  annotations:
    # Custom name of the balancer to create on Hetzner. Defaults to <service>.<namespace>.
    robotlb/balancer: "custom name"
    # Hetzner cloud network. If this annotation is missing, the operator will try to
    # assign external IPs to the load balancer if available. Otherwise, the update won't happen.
    robotlb/lb-network: "my-net"
    # Requests specific IP address for the load balancer in the private network. If not specified,
    # a random one is given. This parameter does nothing in case if network is not specified.
    robotlb/lb-private-ip: "10.10.10.10"
    # Whether to keep the public interface enabled. Set this to false for an internal-only load
    # balancer. In that case, the private IP is announced in the Service status.
    robotlb/lb-public-interface: "false"
    # Node selector for the loadbalancer. Required when ROBOTLB_DYNAMIC_NODE_SELECTOR
    # is false; ignored by the dynamic selection mode.
    # This property helps you filter out nodes.
    # Filters are separated by commas and should have one of the following formats:
    # * key=value  -- checks that the node has a label `key` with value `value`;
    # * key!=value -- verifies that key either doesn't exist or isn't equal to `value`;
    # * !key       -- verifies that the node doesn't have a label `key`;
    # * key        -- verifies that the node has a label `key`.
    robotlb/node-selector: "node-role.kubernetes.io/control-plane!=true,beta.kubernetes.io/arch=amd64"
    ### Load balancer healthcheck options. ###
    # How often to run health probes.
    robotlb/lb-check-interval: "5"
    # Timeout for a single probe.
    robotlb/lb-timeout: "3"
    # How many failed probes before marking the node as unhealthy.
    robotlb/lb-retries: "3"

    ### Load balancer options ###
    # Whether to use proxy mode for this target.
    # https://docs.hetzner.com/cloud/load-balancers/faq/#what-does-proxy-protocol-mean-and-should-i-enable-it
    robotlb/lb-proxy-mode: "false"
    # Location of the load balancer. This expects the code of one of Hetzner's available locations.
    robotlb/lb-location: "hel1"
    # Balancing algorithm. Can be either
    # * least-connections
    # * round-robin
    robotlb/lb-algorithm: "least-connections"
    # Type of balancer.
    robotlb/balancer-type: "lb11"
spec:
  type: LoadBalancer
  # If dynamic node selector is enabled, nodes will be found
  # using this property.
  selector:
    app: target
  ports:
    # Currently only TCP protocol is supported. UDP will be ignored.
    - protocol: TCP
      port: 80 # This will become the listening port on the LB
      targetPort: 80
```

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=forkline/robotlb&type=Date)](https://star-history.com/#forkline/robotlb&Date)

## Reconciliation and multi-cluster operation

robotlb handles `LoadBalancer` Services whose `spec.loadBalancerClass` is unset
or set to `robotlb`. When other LoadBalancer controllers run in the cluster,
set `spec.loadBalancerClass: robotlb` **when creating the Service** to prevent
competing controllers from repeatedly overwriting `status.loadBalancer`.
The class is effectively immutable after creation; recreating the Service can
replace the LB and change its IP.

Node changes that affect targets (readiness, labels, cordon state or addresses)
trigger reconciliation immediately; kubelet heartbeats do not. For `Local`
Services, EndpointSlice updates reconcile only when relevant target nodes change:
Pod-selector Services ignore endpoint readiness/address churn, while selectorless
Services track ready endpoint nodes. EndpointSlice deletions and ownership-label
changes also trigger reconciliation. On otherwise unchanged Services, the operator
compares the desired Service spec, `robotlb/` annotations, target IPs and listener
ports with the last successful reconcile and skips Hetzner requests, but repairs
unexpected ingress status changes from the last successful status snapshot.
Every `ROBOTLB_RESYNC_INTERVAL` seconds (default **300**) it performs a full
check anyway to recover out-of-band changes made in Hetzner.

A failing Service retries independently with exponential backoff from 5 seconds
up to 5 minutes, without suppressing reconciliation triggered by Kubernetes
watch events. Project-wide Hetzner API rate limits use the separate shared gate
without resetting the per-Service failure backoff. After Hetzner creates an LB,
the operator retries every 15 seconds until an ingress address is available.
After 180 seconds without an ingress address, it reports a reconciliation error
and emits throttled Kubernetes Warning Events while continuing the same 15-second
polling interval. Incomplete provisioning is never memoized. Other errors use
the normal failure backoff.

When several clusters share one Hetzner Cloud project, set
`ROBOTLB_CLUSTER_NAME` (for example `prod`) to a DNS-label prefix
of 1–63 lowercase letters, digits and hyphens. Default LB names then become
`<cluster>.<service>.<namespace>`, with a stable hash suffix when exceeding
Hetzner's 128-character limit. Explicit `robotlb/balancer` names are not
prefixed. Existing owned balancers continue to be selected by immutable Service
UID labels, so changing the prefix does **not** rename or adopt them.
Legacy load balancers created by 0.6.x are automatically migrated in place on
upgrade, provided the Kubernetes Service still carries the `robotlb/finalizer`
and its published ingress IPs match the Hetzner LB with the expected name.
RobotLB adds `robotlb/service-uid` while preserving unrelated Hetzner labels,
then verifies ownership before changing any LB configuration. This also applies
when deleting a legacy Service after an upgrade. LB IDs and IPs are preserved;
no upgrade hook or manual migration is required. If the finalizer, ingress
status, or IP identity has been lost, RobotLB refuses adoption rather than
claiming an unrelated LB. Such exceptional cases require operator review.
