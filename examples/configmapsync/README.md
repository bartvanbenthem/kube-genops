# configmapsync-operator

A Kubernetes operator written in Rust that manages a custom resource called `ConfigMapSync`. It automatically creates and maintains a `ConfigMap` in a target namespace based on the desired state declared in a `ConfigMapSync` CR.

## How it works

### Custom Resource: `ConfigMapSync`

You create a `ConfigMapSync` CR in any namespace, specifying:
- `targetNamespace` — the namespace where the ConfigMap will be created/maintained
- `data` — the key/value pairs to populate in the ConfigMap

Example:

```yaml
apiVersion: example.io/v1alpha1
kind: ConfigMapSync
metadata:
  name: app-config
  namespace: default
spec:
  targetNamespace: production
  data:
    LOG_LEVEL: info
    FEATURE_FLAGS: "dark-mode,new-dashboard"
    MAX_CONNECTIONS: "100"
```

This will create a ConfigMap named `cms-app-config` in the `production` namespace.

### Reconcile loop

On each reconcile the operator:

1. **Adds a finalizer** to the CR to prevent deletion before cleanup runs.
2. **Applies the ConfigMap** (`cms-<cr-name>`) in the target namespace using Server-Side Apply. The ConfigMap is labelled with `app.kubernetes.io/managed-by=configmapsync-operator`.
3. **Garbage collects** any stale ConfigMaps previously owned by this CR.
4. **Stamps a label** — adds `configmapsync.example.io/synced-to=<target-namespace>` to the CR so the sync target is visible without reading the spec.
5. **Patches status** — writes `ready`, `message`, and a `Ready=True` condition (with `lastTransitionTime` and `observedGeneration`) in a single SSA patch. `lastTransitionTime` is preserved from the existing condition when `Ready` is already `True`, keeping the patch idempotent.
6. **On deletion** — removes the synced ConfigMap, then strips the finalizer to allow the CR to be fully deleted.

Requeues every **300 seconds** for drift correction. Retries after **30 seconds** on error.

### Drift detection

The controller also watches ConfigMaps carrying the `app.kubernetes.io/managed-by=configmapsync-operator` label. If one is modified externally (e.g. a manual `kubectl edit`), the owning CR is automatically re-queued to restore the desired state.

### Status

```
kubectl get configmapsyncs
NAME         TARGET       READY
app-config   production   true
```

## Prerequisites

- Kubernetes cluster (1.26+)
- `kubectl` configured to point at the cluster
- Rust toolchain (edition 2024) — only needed to build from source

## Deploy

### 1. Install the CRD, then apply the example CR

The CRD must be fully established before Kubernetes will accept instances of it.
Apply in two steps:

```bash
kubectl apply -f manifests/crd.yaml
kubectl apply -f manifests/example-cr.yaml
```

The first apply installs the CRD. The `wait` ensures it is registered before the
second apply creates the `app-config` CR.

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
| `configmapsyncs` (example.io) | get, list, watch, patch, update |
| `configmapsyncs/status` (example.io) | patch, update |
| `configmaps` (core) | get, list, watch, create, update, patch, delete |

## Project structure

```
src/
  main.rs        — wires the kube-rs Controller, sets up watches
  reconciler.rs  — core reconcile and cleanup logic
  types.rs       — ConfigMapSync CRD definition
manifests.yaml   — CRD manifest + example CR
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
