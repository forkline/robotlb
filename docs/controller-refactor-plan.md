# RobotLB kube-rs controller refactor plan

Goal: make the existing kube-rs reconciliation contract explicit and independently testable, while preserving RobotLB 0.7.0's behavior, ownership guarantees and small footprint.

## 1. Controller lifecycle and events (PR 1)
- Extract Service reconciliation from kube-rs watcher wiring.
- Use typed reconciliation outcomes (synced, unchanged, deleted, deferred with reason).
- Centralize stable Kubernetes Warning Event reasons and reuse one Recorder from controller context.
- Preserve current cleanup, retries, rate-limit gate, finalizer handling, memo semantics and event throttling.

## 2. Typed desired state (PR 2)
- Separate validated desired LB configuration from the API-backed LoadBalancer executor.
- Keep the existing Hetzner action planner and no-op/ownership checks.
- Add pure tests for desired-state conversion and effective configuration equality.

## 3. Reconciliation policy tests (PR 3)
- Verify outcome -> kube-rs Action/backoff/memo decisions for successful, deferred and failed reconciliation.
- Exercise Event classification, redaction and deduplication.
- Test cleanup and partial-convergence boundaries without contacting Hetzner.

## Guardrails
- No CRDs, status conditions on Service, controller framework or persistent reconciliation state.
- No behavioral changes to LB ownership, target selection, finalizer lifecycle, networking or HTTP operations.
- Requeue must continue to distinguish transient provider errors, rate limits, and pending IP allocation.
- Kubernetes Event publication remains best-effort: failure to publish cannot hide original failure.
- Keep each PR reviewable; run cargo fmt, clippy and tests before merging.
