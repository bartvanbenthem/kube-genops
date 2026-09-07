# multicontroller-operator

A Kubernetes operator written in Rust that runs **two independent controllers
side by side in a single process**, each managing its own CRD kind:

| Controller | Watches CR | Manages |
|---|---|---|
| `SecretSync` controller | `SecretSync` | `Secret` |
| `ServiceAccountSync` controller | `ServiceAccountSync` | `ServiceAccount` |

This is the pattern to reach for when an operator owns more than one CRD: build
each controller independently with its own `ControllerBuilder`, then drive both
loops on the same Tokio runtime. Within each loop, multiple CRs of that kind
are also reconciled concurrently — so reconciliation is parallel both *across*
controllers and *within* each controller.

## How it works

### Custom Resources

**`SecretSync`** — creates/maintains a `Secret` in a target namespace from
plaintext key/value pairs:

```yaml
apiVersion: example.io/v1alpha1
kind: SecretSync
metadata:
  name: db-credentials
  namespace: default
spec:
  targetNamespace: production
  stringData:
    username: app-user
    password: change-me
```

This creates a Secret named `ss-db-credentials` in the `production` namespace.

**`ServiceAccountSync`** — creates/maintains a `ServiceAccount` in a target
namespace with the given image-pull secrets:

```yaml
apiVersion: example.io/v1alpha1
kind: ServiceAccountSync
metadata:
  name: app-runner
  namespace: default
spec:
  targetNamespace: production
  automountToken: false
  imagePullSecrets:
    - regcred
```

This creates a ServiceAccount named `sas-app-runner` in the `production` namespace.

### Reconcile loop

Both reconcilers (`secretsync.rs`, `serviceaccountsync.rs`) follow the same
shape — only the managed resource type differs:

1. **Adds a finalizer** to the CR to prevent deletion before cleanup runs.
2. **Applies the managed resource** (`ss-<cr-name>` / `sas-<cr-name>`) in the
   target namespace using Server-Side Apply, labelled with
   `app.kubernetes.io/managed-by=multicontroller-<kind>sync`.
3. **Garbage collects** stale resources previously owned by the CR.
4. **Stamps a label** — adds `multicontroller.example.io/synced-to=<target-namespace>`
   to the CR.
5. **Patches status** — writes `ready`, `message`, and a `Ready=True` condition
   in a single SSA patch.
6. **On deletion** — removes the synced resource, then strips the finalizer.

Each controller requeues every **300 seconds** for drift correction, and
retries after **5 seconds** on error.

### Running controllers concurrently

[`src/main.rs`](src/main.rs) builds two `ControllerBuilder`s — one per CRD —
and drives both with `tokio::try_join!`:

```rust
let secretsync_controller = ControllerBuilder::new(secretsync_api)
    .health_port(8080)
    .metrics_port(9090)
    .leader_election(operator_ns.clone(), "secretsync-operator-leader")
    .concurrency(4)
    .run(SecretSyncReconciler, secret_ctx);

let serviceaccountsync_controller = ControllerBuilder::new(serviceaccountsync_api)
    .health_port(8081)
    .metrics_port(9091)
    .leader_election(operator_ns, "serviceaccountsync-operator-leader")
    .concurrency(4)
    .run(ServiceAccountSyncReconciler, serviceaccount_ctx);

tokio::try_join!(secretsync_controller, serviceaccountsync_controller)?;
```

A few things worth noting about composing controllers this way:

- Each `.run(...)` future drives its own watch + reconcile loop; polling them
  together means CRs of *either* kind are picked up and reconciled in parallel —
  a burst of `SecretSync` updates doesn't block `ServiceAccountSync` reconciles.
- `.concurrency(n)` additionally lets each controller reconcile up to `n` CRs
  of *its own* kind in parallel, so concurrency happens both across and within
  controllers.
- Operational features that bind shared resources need distinct values per
  controller: each gets its own health port (`8080`/`8081`), its own metrics
  port (`9090`/`9091`), and its own leader lease name, so the two controllers
  can be elected leader independently.
- `tokio::try_join!` propagates the first error and cancels the other loop —
  the same fail-fast behavior a single `.run()` call would have. Use
  `tokio::join!` instead if controllers should keep running independently of
  each other's failures.

### Status

```
kubectl get secretsyncs
NAME             TARGET       READY
db-credentials   production   true

kubectl get serviceaccountsyncs
NAME         TARGET       READY
app-runner   production   true
```

## Prerequisites

- Kubernetes cluster (1.26+)
- `kubectl` configured to point at the cluster
- Rust toolchain (edition 2024) — only needed to build from source

## Deploy

### 1. Install the CRDs, then apply the example CRs

The CRDs must be fully established before Kubernetes will accept instances of them.
Apply in two steps:

```bash
kubectl apply -f manifests/crd-secretsync.yaml -f manifests/crd-serviceaccountsync.yaml
kubectl apply -f manifests/example-cr.yaml
```

### 2. Build and run the operator

#### Local (out-of-cluster)

```bash
RUST_LOG=info cargo run --release
```

The operator uses the kubeconfig from `~/.kube/config` (or the `KUBECONFIG` env var) when running out-of-cluster.

#### In-cluster

Build a container image from the binary and deploy it as a `Deployment` with a `ServiceAccount` that has the necessary RBAC permissions (see below), then point it at the cluster by mounting the in-cluster service account token (the default when no kubeconfig is present).

### Required RBAC permissions

The operator needs the following permissions:

| Resource | Verbs |
|---|---|
| `secretsyncs` (example.io) | get, list, watch, patch, update |
| `secretsyncs/status` (example.io) | patch, update |
| `serviceaccountsyncs` (example.io) | get, list, watch, patch, update |
| `serviceaccountsyncs/status` (example.io) | patch, update |
| `secrets` (core) | get, list, watch, create, update, patch, delete |
| `serviceaccounts` (core) | get, list, watch, create, update, patch, delete |
| `leases` (coordination.k8s.io) | get, list, watch, create, update, patch |

## Project structure

```
src/
  main.rs                — wires up and runs both controllers concurrently
  secretsync.rs          — SecretSync reconciler (manages Secrets)
  serviceaccountsync.rs  — ServiceAccountSync reconciler (manages ServiceAccounts)
  types.rs               — SecretSync and ServiceAccountSync CRD definitions
manifests/
  crd-secretsync.yaml
  crd-serviceaccountsync.yaml
  example-cr.yaml
Cargo.toml
```

## Dependencies

| Crate | Version | Purpose |
|---|---|---|
| `kube` | 4.2.0 | Kubernetes client + controller runtime |
| `k8s-openapi` | 0.28.0 (v1_36) | Typed Kubernetes API objects |
| `koprs` | 0.6.1 | Helper abstractions (SSA, finalizers, status patching, conditions, label patching, GC) |
| `tokio` | 1.0 | Async runtime |
| `tracing` / `tracing-subscriber` | 0.1 / 0.3 | Structured logging |

Log level is controlled via the `RUST_LOG` environment variable (default: `info`).
