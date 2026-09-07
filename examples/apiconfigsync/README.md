# apiconfigsync-operator

A Kubernetes operator written in Rust that manages a custom resource called `ApiConfigSync`. It polls an external HTTP API on each reconcile and stores the response body as a `ConfigMap` in a target namespace. A co-located validating admission webhook enforces spec correctness before any `ApiConfigSync` is persisted.

This example demonstrates using `koprs`, `koprs-external`, and `koprs-admission` together in a single operator binary.

## How it works

### Custom Resource: `ApiConfigSync`

You create an `ApiConfigSync` CR in any namespace, specifying:
- `targetNamespace` — the namespace where the ConfigMap will be created/maintained
- `apiUrl` — the HTTP(S) endpoint to poll for configuration data
- `bearerToken` *(optional)* — sent as `Authorization: Bearer <token>` on every request
- `configKey` *(optional, default: `"config"`)* — the key name used in the resulting ConfigMap

Example:

```yaml
apiVersion: example.io/v1alpha1
kind: ApiConfigSync
metadata:
  name: app-config
  namespace: default
spec:
  targetNamespace: production
  apiUrl: https://config.example.com/api/v1/settings
  bearerToken: my-api-token
  configKey: settings.json
```

This creates a ConfigMap named `acs-app-config` in the `production` namespace with a single key `settings.json` whose value is the response body from the API endpoint.

### Reconcile loop

On each reconcile the operator:

1. **Adds a finalizer** to the CR to prevent deletion before cleanup runs.
2. **Polls the external API** once per reconcile using `koprs-external`'s `HttpPoller`. Sends a Bearer token when configured. The raw response body becomes the ConfigMap value.
3. **Applies the ConfigMap** (`acs-<cr-name>`) in the target namespace using Server-Side Apply. The ConfigMap is labelled with `app.kubernetes.io/managed-by=apiconfigsync-operator`.
4. **Garbage collects** any stale ConfigMaps previously owned by this CR.
5. **Stamps a label** — adds `apiconfigsync.example.io/synced-to=<target-namespace>` to the CR so the sync target is visible without reading the spec.
6. **Patches status** — writes `ready`, `message`, and a `Ready=True` condition (with `lastTransitionTime` and `observedGeneration`) in a single SSA patch.
7. **On deletion** — removes the synced ConfigMap, then strips the finalizer to allow the CR to be fully deleted.

Requeues every **300 seconds** for drift correction. Retries after **5 seconds** on error.

### Drift detection

The controller watches ConfigMaps carrying the `app.kubernetes.io/managed-by=apiconfigsync-operator` label. If one is modified externally, the owning CR is automatically re-queued to restore the desired state.

### Admission webhook

An admission webhook is registered at `POST /validate/apiconfigsync` and is invoked by Kubernetes before any `ApiConfigSync` is created or updated. It uses `koprs-admission`'s `WebhookBuilder` and `Validator` trait.

**Hard denials** (the resource is rejected):

| Rule | Error |
|------|-------|
| `spec.apiUrl` is empty | `spec.apiUrl must not be empty` |
| `spec.apiUrl` does not start with `http://` or `https://` | `spec.apiUrl must begin with http:// or https://` |
| `spec.targetNamespace` is empty | `spec.targetNamespace must not be empty` |
| `spec.configKey` is empty | `spec.configKey must not be empty` |

**Non-blocking warnings** (the resource is accepted, but `kubectl` surfaces a message):

| Condition | Warning |
|-----------|---------|
| `spec.apiUrl` uses plain HTTP | recommend HTTPS for production |
| `spec.bearerToken` is absent | polling will be unauthenticated |

The webhook server listens on port `8443`. When TLS certificate files are found at the paths set by `WEBHOOK_TLS_CERT` / `WEBHOOK_TLS_KEY` (default: `/tls/tls.crt`, `/tls/tls.key`) it serves HTTPS. When the files are absent it falls back to plain HTTP, which is useful for local development.

### Running controller and webhook concurrently

[`src/main.rs`](src/main.rs) drives both the controller loop and the webhook server on the same Tokio runtime:

```rust
tokio::try_join!(
    run_controller(client, operator_ns),
    run_webhook(),
)?;
```

`run_controller` builds a `ControllerBuilder` with health probes, Prometheus metrics, leader election, and a secondary ConfigMap watch. `run_webhook` builds a `WebhookBuilder` with the `ApiConfigSyncValidator` registered at `/validate/apiconfigsync`.

If either task returns an error, the other is cancelled and the process exits.

### Status

```
kubectl get apiconfigsyncs
NAME         TARGET       APIURL                                          READY
app-config   production   https://config.example.com/api/v1/settings      true
```

## Prerequisites

- Kubernetes cluster (1.26+)
- `kubectl` configured to point at the cluster
- Rust toolchain (edition 2024) — only needed to build from source
- A TLS certificate for the admission webhook (e.g. issued by [cert-manager](https://cert-manager.io/)) — only needed for in-cluster deployment

## Deploy

### 1. Install the CRD, then apply the example CR

```bash
kubectl apply -f manifests/crd.yaml
kubectl apply -f manifests/example-cr.yaml
```

### 2. Register the admission webhook

Apply the `ValidatingWebhookConfiguration` that points Kubernetes at the webhook server:

```bash
kubectl apply -f manifests/webhook.yaml
```

The webhook configuration must reference the `caBundle` of the TLS certificate used by the server. When using cert-manager, annotate the `ValidatingWebhookConfiguration` with `cert-manager.io/inject-ca-from` to have this injected automatically.

### 3. Build and run the operator

#### Local (out-of-cluster)

```bash
RUST_LOG=info cargo run --release
```

The operator uses the kubeconfig from `~/.kube/config` (or the `KUBECONFIG` env var) when running out-of-cluster. In this mode the webhook falls back to plain HTTP (no cert files present), which is fine for local testing.

#### In-cluster

Build a container image from the binary and deploy it as a `Deployment` with a `ServiceAccount` that has the necessary RBAC permissions (see below). Mount the TLS `Secret` produced by cert-manager as a volume at `/tls`.

### Required RBAC permissions

| Resource | Verbs |
|---|---|
| `apiconfigsyncs` (example.io) | get, list, watch, patch, update |
| `apiconfigsyncs/status` (example.io) | patch, update |
| `configmaps` (core) | get, list, watch, create, update, patch, delete |
| `events` (core) | create, patch |
| `leases` (coordination.k8s.io) | get, list, watch, create, update, patch |

## Project structure

```
src/
  main.rs        — wires controller and webhook; runs both with tokio::try_join!
  reconciler.rs  — ApiConfigSync reconciler; polls external API, manages ConfigMap
  validator.rs   — admission validator; enforces spec rules via koprs-admission
  types.rs       — ApiConfigSync CRD definition
Cargo.toml
```

## Dependencies

| Crate | Version | Purpose |
|---|---|---|
| `kube` | 4.2.0 | Kubernetes client + controller runtime |
| `k8s-openapi` | 0.28.0 (v1_36) | Typed Kubernetes API objects |
| `koprs` | workspace | Core operator abstractions (SSA, finalizers, status patching, GC, events) |
| `koprs-external` | workspace | HTTP polling via `HttpPoller` and `ExternalSource` |
| `koprs-admission` | workspace | Validating admission webhook server via `WebhookBuilder` and `Validator` |
| `tokio` | 1.0 | Async runtime |
| `tracing` / `tracing-subscriber` | 0.1 / 0.3 | Structured logging |

Log level is controlled via the `RUST_LOG` environment variable (default: `info`).
