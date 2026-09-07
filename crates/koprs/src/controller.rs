//! Controller framework for building Kubernetes operators.
//!
//! Provides [`Reconciler`], [`Context`], and [`ControllerBuilder`] — the three
//! building blocks needed to run a reconciliation loop on top of `kube-runtime`.
//!
//! [`ControllerBuilder`] handles the operational skeleton every production
//! operator needs: graceful shutdown on SIGTERM/SIGINT, `/healthz`/`/readyz`
//! HTTP probes, leader election, and per-reconcile timeouts. Implement
//! [`Reconciler`] for your custom resource and call [`ControllerBuilder::run`]
//! — everything else is automatic.
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use koprs::controller::{Action, Context, ControllerBuilder, Reconciler};
//! use koprs::error::KubeGenericError;
//! use kube::Client;
//!
//! struct MyOperator;
//! type MyCR = k8s_openapi::api::core::v1::ConfigMap;
//!
//! impl Reconciler<MyCR> for MyOperator {
//!     type Error = KubeGenericError;
//!
//!     async fn reconcile(
//!         &self,
//!         _cr: Arc<MyCR>,
//!         _ctx: Arc<Context>,
//!     ) -> Result<Action, Self::Error> {
//!         Ok(Action::await_change())
//!     }
//! }
//!
//! # async fn example(client: Client) -> Result<(), koprs::error::KubeGenericError> {
//! let ctx = Context::new(client.clone());
//! let api = kube::Api::<MyCR>::namespaced(client, "my-namespace");
//! ControllerBuilder::new(api)
//!     .health_port(8080)
//!     .graceful_shutdown()
//!     .reconcile_timeout(std::time::Duration::from_secs(300))
//!     .run(MyOperator, ctx)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Client;
use tracing::{debug, error, info, warn};

use crate::error::{KubeGenericError, Result};
use crate::observability::{Metrics, serve_metrics};
use crate::traits::KubeResource;

// ---------------------------------------------------------------------------
// Re-exports — operators only need `use koprs::controller::*`
// ---------------------------------------------------------------------------

pub use kube_runtime::controller::Action;
pub use kube_runtime::reflector::ObjectRef;
pub use kube_runtime::watcher;

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// Shared context passed to every reconcile and error-policy call.
///
/// Holds the Kubernetes [`Client`] and any user-supplied data `T`.
/// Use [`Context::new`] when no extra state is needed, or
/// [`Context::with_data`] to attach your own configuration, metrics
/// handles, or other shared state.
///
/// The `T = ()` default means [`Context`] is a valid type alias for
/// simple operators that do not need extra data.
pub struct Context<T = ()> {
    /// The Kubernetes API client.
    pub client: Client,
    /// User-supplied data shared across all reconcile calls.
    pub data: T,
}

impl Context<()> {
    /// Create a context with a Kubernetes client and no extra data.
    pub fn new(client: Client) -> Arc<Self> {
        Arc::new(Self { client, data: () })
    }
}

impl<T> Context<T> {
    /// Create a context with a Kubernetes client and user-supplied data.
    pub fn with_data(client: Client, data: T) -> Arc<Self> {
        Arc::new(Self { client, data })
    }
}

// ---------------------------------------------------------------------------
// Reconciler trait
// ---------------------------------------------------------------------------

/// Trait that operator authors implement to define reconciliation logic.
///
/// `CR` is the primary custom resource type being reconciled.
/// `T` is the shared context data type and defaults to `()`.
///
/// `error_policy` has a default implementation that requeues after 30 seconds.
/// Override it only if you need custom backoff or logging.
///
/// # Implementing
///
/// ```no_run
/// use std::sync::Arc;
/// use koprs::controller::{Action, Context, Reconciler};
/// use koprs::error::KubeGenericError;
///
/// struct MyOperator;
/// type MyCR = k8s_openapi::api::core::v1::ConfigMap;
///
/// impl Reconciler<MyCR> for MyOperator {
///     type Error = KubeGenericError;
///
///     async fn reconcile(
///         &self,
///         _cr: Arc<MyCR>,
///         _ctx: Arc<Context>,
///     ) -> Result<Action, Self::Error> {
///         Ok(Action::await_change())
///     }
///     // error_policy defaults to requeue(30s) — no need to implement it
/// }
/// ```
pub trait Reconciler<CR, T = ()>: Send + Sync + 'static
where
    CR: KubeResource,
    T: Send + Sync + 'static,
{
    /// The error type returned by [`reconcile`](Self::reconcile).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Reconcile the given resource to its desired state.
    ///
    /// Called whenever the resource changes or a requeue fires. Return
    /// [`Action::await_change`] to wait for the next watch event or
    /// [`Action::requeue`] to retry after a fixed duration.
    ///
    /// The returned future must be `Send` because the controller runtime
    /// executes reconciles on a multi-threaded executor.
    fn reconcile(
        &self,
        cr: Arc<CR>,
        ctx: Arc<Context<T>>,
    ) -> impl std::future::Future<Output = std::result::Result<Action, Self::Error>> + Send;

    /// Decide how to handle a failed reconcile.
    ///
    /// Defaults to requeue after 30 seconds. Override to implement custom
    /// backoff strategies or to add per-error logging.
    fn error_policy(&self, _cr: Arc<CR>, _err: &Self::Error, _ctx: Arc<Context<T>>) -> Action {
        Action::requeue(Duration::from_secs(30))
    }
}

// ---------------------------------------------------------------------------
// Internal: health server
// ---------------------------------------------------------------------------

/// Serve HTTP/1.1 health probes on an already-bound listener.
///
/// `GET /healthz` — `200 OK` always (liveness).
/// `GET /readyz`  — `200 OK` once `ready` flips to `true`, else `503` (readiness).
pub(crate) async fn serve_health(listener: tokio::net::TcpListener, ready: Arc<AtomicBool>) {
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;

    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let ready = ready.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req: Request<Incoming>| {
                let ready = ready.clone();
                async move {
                    let (status, body): (StatusCode, &'static str) =
                        if req.uri().path() == "/readyz" {
                            if ready.load(Ordering::Acquire) {
                                (StatusCode::OK, "ok")
                            } else {
                                (StatusCode::SERVICE_UNAVAILABLE, "not ready")
                            }
                        } else {
                            (StatusCode::OK, "ok")
                        };
                    Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .status(status)
                            .header("content-type", "text/plain")
                            .body(Full::new(Bytes::from_static(body.as_bytes())))
                            .unwrap(),
                    )
                }
            });
            let _ = http1::Builder::new().serve_connection(io, svc).await;
        });
    }
}

// ---------------------------------------------------------------------------
// Internal: graceful shutdown signal
// ---------------------------------------------------------------------------

/// Resolves on SIGTERM (Unix) or Ctrl+C (all platforms).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}

// ---------------------------------------------------------------------------
// Internal: leader election
// ---------------------------------------------------------------------------

pub(crate) struct LeaderElectionConfig {
    pub(crate) namespace: String,
    pub(crate) name: String,
    pub(crate) identity: String,
    pub(crate) lease_duration_secs: i32,
    pub(crate) renew_period: Duration,
    pub(crate) retry_period: Duration,
}

/// Attempt to acquire or renew the Lease.
///
/// Returns `true` if this identity now holds the lease.
/// Returns `false` if another identity holds a non-expired lease.
/// Optimistic locking: passes `resourceVersion` to detect concurrent writers.
pub(crate) async fn try_acquire_or_renew(
    config: &LeaderElectionConfig,
    client: &Client,
) -> Result<bool> {
    let api: kube::Api<Lease> = kube::Api::namespaced(client.clone(), &config.namespace);
    let now = k8s_openapi::jiff::Timestamp::now();
    let now_micro = MicroTime(now);

    let existing = api
        .get(&config.name)
        .await
        .map(Some)
        .or_else(|e| match &e {
            kube::Error::Api(ae) if ae.code == 404 => Ok(None),
            _ => Err(KubeGenericError::Kube(e)),
        })?;

    match existing {
        None => {
            let lease = build_lease(
                &config.name,
                &config.namespace,
                &config.identity,
                config.lease_duration_secs,
                now_micro.clone(),
                now_micro,
                None,
                Some(0),
            );
            match api.create(&kube::api::PostParams::default(), &lease).await {
                Ok(_) => Ok(true),
                Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
                Err(e) => Err(KubeGenericError::Kube(e)),
            }
        }
        Some(existing) => {
            let spec = existing.spec.as_ref();
            let holder = spec.and_then(|s| s.holder_identity.as_deref());
            let renew_time = spec.and_then(|s| s.renew_time.as_ref()).map(|t| t.0);
            let duration_secs = spec
                .and_then(|s| s.lease_duration_seconds)
                .unwrap_or(config.lease_duration_secs);

            let is_expired = renew_time.map_or(true, |rt| {
                now > rt + k8s_openapi::jiff::SignedDuration::new(duration_secs as i64, 0)
            });

            if holder != Some(config.identity.as_str()) && !is_expired {
                return Ok(false);
            }

            let transitioning = holder != Some(config.identity.as_str());
            let acquire_time = if transitioning {
                now_micro.clone()
            } else {
                spec.and_then(|s| s.acquire_time.clone())
                    .unwrap_or_else(|| now_micro.clone())
            };
            let transitions = spec
                .and_then(|s| s.lease_transitions)
                .map(|t| if transitioning { t + 1 } else { t });

            let new_lease = build_lease(
                &config.name,
                &config.namespace,
                &config.identity,
                config.lease_duration_secs,
                acquire_time,
                now_micro,
                existing.metadata.resource_version.clone(),
                transitions,
            );

            match api
                .replace(&config.name, &kube::api::PostParams::default(), &new_lease)
                .await
            {
                Ok(_) => Ok(true),
                Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
                Err(e) => Err(KubeGenericError::Kube(e)),
            }
        }
    }
}

pub(crate) fn build_lease(
    name: &str,
    namespace: &str,
    identity: &str,
    lease_duration_secs: i32,
    acquire_time: MicroTime,
    renew_time: MicroTime,
    resource_version: Option<String>,
    lease_transitions: Option<i32>,
) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            resource_version,
            ..Default::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: Some(identity.to_string()),
            lease_duration_seconds: Some(lease_duration_secs),
            acquire_time: Some(acquire_time),
            renew_time: Some(renew_time),
            lease_transitions,
            ..Default::default()
        }),
    }
}

/// Block until this identity successfully acquires the Lease.
async fn acquire_leader_lease(config: &LeaderElectionConfig, client: &Client) -> Result<()> {
    info!(
        identity = %config.identity,
        lease = %config.name,
        ns = %config.namespace,
        "Waiting to become leader"
    );
    loop {
        match try_acquire_or_renew(config, client).await {
            Ok(true) => {
                info!(identity = %config.identity, "Acquired leader lease");
                return Ok(());
            }
            Ok(false) => {
                debug!(identity = %config.identity, "Lease held by another, retrying");
                tokio::time::sleep(config.retry_period).await;
            }
            Err(e) => {
                warn!(error = %e, "Error acquiring leader lease, retrying");
                tokio::time::sleep(config.retry_period).await;
            }
        }
    }
}

/// Periodically renew the Lease; signals `stop_tx` when the lease is
/// definitively lost (taken by another replica or unrecoverable API errors).
async fn renew_leader_lease_loop(
    config: LeaderElectionConfig,
    client: Client,
    stop_tx: tokio::sync::watch::Sender<bool>,
) {
    let mut consecutive_errors: u32 = 0;
    loop {
        tokio::time::sleep(config.renew_period).await;
        match try_acquire_or_renew(&config, &client).await {
            Ok(true) => {
                consecutive_errors = 0;
                debug!(identity = %config.identity, "Leader lease renewed");
            }
            Ok(false) => {
                error!(
                    identity = %config.identity,
                    "Leader lease taken by another replica — stopping controller"
                );
                stop_tx.send(true).ok();
                return;
            }
            Err(e) => {
                consecutive_errors += 1;
                warn!(
                    error = %e,
                    consecutive_errors,
                    "Failed to renew leader lease"
                );
                if consecutive_errors >= 3 {
                    error!(
                        identity = %config.identity,
                        "Could not renew leader lease after 3 attempts — stopping controller"
                    );
                    stop_tx.send(true).ok();
                    return;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ControllerBuilder
// ---------------------------------------------------------------------------

type ConfigureFn<CR> =
    Box<dyn FnOnce(kube_runtime::Controller<CR>) -> kube_runtime::Controller<CR> + Send + 'static>;

/// Builder for a controller reconciliation loop.
///
/// Wraps `kube-runtime::Controller` and adds:
///
/// | Method | What it provides |
/// |--------|-----------------|
/// | `.health_port(port)` | `GET /healthz` + `GET /readyz` server |
/// | `.graceful_shutdown()` | Clean stop on SIGTERM or Ctrl+C |
/// | `.leader_election(ns, name)` | Kubernetes Lease-based HA leader election |
/// | `.reconcile_timeout(dur)` | Kill and requeue stuck reconciles |
///
/// # Example
///
/// ```no_run
/// use koprs::controller::{Action, Context, ControllerBuilder, ObjectRef, Reconciler, watcher};
/// use koprs::error::KubeGenericError;
/// use kube::{Api, Client};
/// use k8s_openapi::api::core::v1::ConfigMap;
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// struct MyOperator;
/// type MyCR = k8s_openapi::api::core::v1::ConfigMap;
///
/// impl Reconciler<MyCR> for MyOperator {
///     type Error = KubeGenericError;
///     async fn reconcile(&self, _cr: Arc<MyCR>, _ctx: Arc<Context>) -> Result<Action, KubeGenericError> {
///         Ok(Action::await_change())
///     }
/// }
///
/// # async fn example(client: Client, cm_api: Api<ConfigMap>) -> Result<(), KubeGenericError> {
/// let ctx = Context::new(client.clone());
/// let api = Api::<MyCR>::namespaced(client, "my-namespace");
/// ControllerBuilder::new(api)
///     .health_port(8080)
///     .graceful_shutdown()
///     .leader_election("my-namespace", "my-operator-leader")
///     .reconcile_timeout(Duration::from_secs(300))
///     .run(MyOperator, ctx)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ControllerBuilder<CR, T = ()>
where
    CR: KubeResource,
{
    api: kube::Api<CR>,
    pub(crate) watcher_config: watcher::Config,
    pub(crate) configure: Option<ConfigureFn<CR>>,
    pub(crate) health_port: Option<u16>,
    pub(crate) metrics_port: Option<u16>,
    pub(crate) graceful_shutdown: bool,
    pub(crate) reconcile_timeout: Option<Duration>,
    pub(crate) leader_election: Option<LeaderElectionConfig>,
    pub(crate) concurrency: Option<u16>,
    _phantom: PhantomData<T>,
}

impl<CR, T> ControllerBuilder<CR, T>
where
    CR: KubeResource,
    T: Send + Sync + 'static,
{
    /// Create a new builder from a pre-built [`kube::Api`].
    ///
    /// The `Api` determines the watch scope:
    /// - `Api::namespaced(client, "ns")` — one namespace
    /// - `Api::all(client)` — all namespaces / cluster-wide
    pub fn new(api: kube::Api<CR>) -> Self {
        Self {
            api,
            watcher_config: watcher::Config::default(),
            configure: None,
            health_port: None,
            metrics_port: None,
            graceful_shutdown: false,
            reconcile_timeout: None,
            leader_election: None,
            concurrency: None,
            _phantom: PhantomData,
        }
    }

    /// Start a health probe HTTP server on `0.0.0.0:<port>`.
    ///
    /// `GET /healthz` always returns `200 OK`.
    /// `GET /readyz` returns `503` until the first reconcile result, then `200 OK`.
    ///
    /// If the port is already in use, [`run`](ControllerBuilder::run) returns
    /// an error before the controller loop starts.
    pub fn health_port(mut self, port: u16) -> Self {
        self.health_port = Some(port);
        self
    }

    /// Start a Prometheus metrics server on `0.0.0.0:<port>`.
    ///
    /// `GET /metrics` returns reconcile counts, error counts (by kind and
    /// error), and reconcile duration histograms in Prometheus text
    /// exposition format. Recording happens automatically around every
    /// reconcile — see [`Metrics`] for the full list of collectors.
    ///
    /// If the port is already in use, [`run`](ControllerBuilder::run) returns
    /// an error before the controller loop starts.
    pub fn metrics_port(mut self, port: u16) -> Self {
        self.metrics_port = Some(port);
        self
    }

    /// Stop the controller loop cleanly on SIGTERM or Ctrl+C.
    ///
    /// The loop stops accepting new work; reconciles already running inside
    /// `kube-runtime` are allowed to complete.
    pub fn graceful_shutdown(mut self) -> Self {
        self.graceful_shutdown = true;
        self
    }

    /// Enable Kubernetes Lease-based leader election.
    ///
    /// [`run`](ControllerBuilder::run) blocks until this pod acquires the
    /// named `Lease` object in `namespace`, then starts the controller. A
    /// background task renews the lease every 5 seconds (lease duration: 15 s).
    /// If the lease is lost the controller stops cleanly.
    ///
    /// The pod's identity defaults to `$POD_NAME` (downward API) falling back
    /// to `$HOSTNAME`.
    pub fn leader_election(
        mut self,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        let identity = std::env::var("POD_NAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| "unknown".to_string());
        self.leader_election = Some(LeaderElectionConfig {
            namespace: namespace.into(),
            name: name.into(),
            identity,
            lease_duration_secs: 15,
            renew_period: Duration::from_secs(5),
            retry_period: Duration::from_secs(2),
        });
        self
    }

    /// Cancel and requeue any reconcile that runs longer than `timeout`.
    ///
    /// The reconciler is given `Action::requeue(timeout)` on expiry so
    /// the resource is retried after the same duration.
    pub fn reconcile_timeout(mut self, timeout: Duration) -> Self {
        self.reconcile_timeout = Some(timeout);
        self
    }

    /// Apply a label selector to the primary resource watch.
    pub fn label_selector(mut self, selector: impl Into<String>) -> Self {
        let s = selector.into();
        self.watcher_config = self.watcher_config.labels(&s);
        self
    }

    /// Override the watcher configuration for the primary resource watch.
    pub fn watcher_config(mut self, config: watcher::Config) -> Self {
        self.watcher_config = config;
        self
    }

    /// Limit the number of reconciles that may run concurrently.
    ///
    /// Defaults to `0` (unbounded). Set to a positive value to cap concurrent
    /// reconciles across all objects. A single object is never reconciled
    /// concurrently regardless of this setting.
    pub fn concurrency(mut self, n: u16) -> Self {
        self.concurrency = Some(n);
        self
    }

    /// Override the timing parameters for leader election.
    ///
    /// Must be called **after** [`.leader_election()`](ControllerBuilder::leader_election).
    ///
    /// | Parameter | Meaning |
    /// |-----------|---------|
    /// | `lease_duration` | How long the lease is valid without renewal |
    /// | `renew_period` | How often the leader renews the lease |
    /// | `retry_period` | How often a non-leader retries acquisition |
    ///
    /// # Panics
    ///
    /// Panics if called before `.leader_election()`.
    pub fn leader_election_timings(
        mut self,
        lease_duration: Duration,
        renew_period: Duration,
        retry_period: Duration,
    ) -> Self {
        let le = self
            .leader_election
            .as_mut()
            .expect("leader_election_timings must be called after leader_election");
        le.lease_duration_secs = lease_duration.as_secs() as i32;
        le.renew_period = renew_period;
        le.retry_period = retry_period;
        self
    }

    /// Watch child resources that this CR owns via Kubernetes owner references.
    ///
    /// Whenever a resource of type `Child` changes, `kube-runtime` traverses
    /// its owner references to find the parent CR and re-queues it. Use this
    /// for resources you create and set an owner reference on (e.g. Deployments,
    /// Services, ConfigMaps managed by your operator).
    ///
    /// The `Child` type must carry an [`OwnerReference`] pointing back at `CR`.
    ///
    /// Multiple calls compose — each call adds another owned type on top of
    /// previously registered watches.
    ///
    /// [`OwnerReference`]: k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference
    ///
    /// # Example
    ///
    /// ```no_run
    /// use k8s_openapi::api::apps::v1::Deployment;
    /// use k8s_openapi::api::core::v1::ConfigMap;
    /// use koprs::controller::{ControllerBuilder, watcher};
    ///
    /// async fn example(api: kube::Api<ConfigMap>, deploy_api: kube::Api<Deployment>) {
    ///     let _builder: ControllerBuilder<ConfigMap> = ControllerBuilder::new(api)
    ///         .owns(deploy_api, watcher::Config::default());
    /// }
    /// ```
    pub fn owns<Child>(mut self, api: kube::Api<Child>, config: watcher::Config) -> Self
    where
        Child: crate::traits::KubeResource,
    {
        let existing = self.configure.take();
        self.configure = Some(Box::new(move |ctl| {
            let ctl = ctl.owns(api, config);
            match existing {
                Some(f) => f(ctl),
                None => ctl,
            }
        }));
        self
    }

    /// Wire a single secondary (trigger) watch using a typed mapper.
    ///
    /// Whenever a resource of type `Other` changes, `mapper` converts it into
    /// zero or more [`ObjectRef`]s that identify CRs to re-queue. Returning
    /// `None` / an empty iterator drops the event. Returning
    /// `Some(ObjectRef::new("my-cr").within("my-ns"))` re-queues that CR.
    ///
    /// Multiple calls to `.watch()` compose — each watch is added on top of
    /// any previously registered watches and `.with_watches()` calls.
    ///
    /// Use [`owner_label_mapper`][crate::owners::owner_label_mapper] as the
    /// `mapper` argument when the trigger resource carries an owner label
    /// pointing at the CR.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use k8s_openapi::api::core::v1::{ConfigMap, Namespace};
    /// use koprs::controller::{ControllerBuilder, watcher};
    /// use koprs::owners::owner_label_mapper;
    ///
    /// async fn example(api: kube::Api<Namespace>, cm_api: kube::Api<ConfigMap>) {
    ///     // Re-queue the owning Namespace whenever a managed ConfigMap changes.
    ///     let _builder: ControllerBuilder<Namespace> = ControllerBuilder::new(api)
    ///         .watch(
    ///             cm_api,
    ///             watcher::Config::default().labels("app=my-operator"),
    ///             owner_label_mapper::<ConfigMap, Namespace>("my-operator/owner"),
    ///         );
    ///     // chain .health_port(), .leader_election(), .run(), etc.
    /// }
    /// ```
    pub fn watch<Other, I, F>(
        mut self,
        api: kube::Api<Other>,
        config: watcher::Config,
        mapper: F,
    ) -> Self
    where
        Other: crate::traits::KubeResource,
        I: IntoIterator<Item = kube_runtime::reflector::ObjectRef<CR>> + Send + 'static,
        I::IntoIter: Send,
        F: Fn(Other) -> I + Send + Sync + 'static,
    {
        let existing = self.configure.take();
        self.configure = Some(Box::new(move |ctl| {
            let ctl = ctl.watches(api, config, mapper);
            match existing {
                Some(f) => f(ctl),
                None => ctl,
            }
        }));
        self
    }

    /// Configure additional watches using the full `kube_runtime::Controller` API.
    ///
    /// The closure receives the inner controller and must return it, typically
    /// with `.watches()` or `.owns()` chained. Use this when you need access to
    /// advanced kube-runtime options not covered by [`watch`][ControllerBuilder::watch].
    ///
    /// Multiple calls compose — each call adds on top of previously registered
    /// watches.
    pub fn with_watches<F>(mut self, configure: F) -> Self
    where
        F: FnOnce(kube_runtime::Controller<CR>) -> kube_runtime::Controller<CR> + Send + 'static,
    {
        let existing = self.configure.take();
        self.configure = Some(Box::new(move |ctl| {
            let ctl = configure(ctl);
            match existing {
                Some(f) => f(ctl),
                None => ctl,
            }
        }));
        self
    }

    /// Start the reconciliation loop.
    ///
    /// In order:
    /// 1. Binds the health server if `.health_port()` was set.
    /// 2. Blocks until the leader lease is acquired if `.leader_election()` was set.
    /// 3. Starts the reconcile loop, wrapping each call with a timeout if set.
    /// 4. Stops cleanly on shutdown signal or lease loss.
    pub async fn run<R>(self, reconciler: R, context: Arc<Context<T>>) -> Result<()>
    where
        R: Reconciler<CR, T>,
    {
        let kind = CR::kind(&());
        let client = context.client.clone();
        info!(%kind, "Starting controller");

        // --- Health probes ---
        let ready = Arc::new(AtomicBool::new(false));
        if let Some(port) = self.health_port {
            let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
            info!(port, "Health server listening");
            tokio::spawn(serve_health(listener, ready.clone()));
        }

        // --- Metrics ---
        let metrics = if let Some(port) = self.metrics_port {
            let registry = prometheus::Registry::new();
            let metrics = Arc::new(Metrics::new_registered("", &registry)?);
            let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
            info!(port, "Metrics server listening");
            tokio::spawn(serve_metrics(listener, registry));
            Some(metrics)
        } else {
            None
        };

        // --- Stop signal channel (shared by shutdown + lease loss) ---
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let has_stop = self.graceful_shutdown || self.leader_election.is_some();

        if self.graceful_shutdown {
            let tx = stop_tx.clone();
            let kind_owned = kind.to_string();
            tokio::spawn(async move {
                shutdown_signal().await;
                info!(kind = %kind_owned, "Shutdown signal received");
                tx.send(true).ok();
            });
        }

        // --- Leader election ---
        if let Some(le) = self.leader_election {
            acquire_leader_lease(&le, &client).await?;
            let tx = stop_tx.clone();
            let client_le = client.clone();
            tokio::spawn(async move {
                renew_leader_lease_loop(le, client_le, tx).await;
            });
        }

        // --- Controller ---
        let mut ctl = kube_runtime::Controller::new(self.api, self.watcher_config);
        if let Some(n) = self.concurrency {
            ctl = ctl.with_config(kube_runtime::controller::Config::default().concurrency(n));
        }
        if let Some(configure) = self.configure {
            ctl = configure(ctl);
        }

        let reconciler = Arc::new(reconciler);
        let error_policy_r = reconciler.clone();
        let ready_ref = ready.clone();
        let reconcile_timeout = self.reconcile_timeout;
        let kind_owned = kind.to_string();

        let run_loop = ctl
            .run(
                move |cr, ctx| {
                    let r = reconciler.clone();
                    let metrics = metrics.clone();
                    let kind = kind_owned.clone();
                    async move {
                        let started = std::time::Instant::now();
                        let result = if let Some(t) = reconcile_timeout {
                            match tokio::time::timeout(t, r.reconcile(cr, ctx)).await {
                                Ok(result) => result,
                                Err(_) => {
                                    warn!("Reconcile timed out after {t:?}, requeueing");
                                    Ok(Action::requeue(t))
                                }
                            }
                        } else {
                            r.reconcile(cr, ctx).await
                        };
                        if let Some(m) = &metrics {
                            match &result {
                                Ok(_) => m.record_success(&kind, started.elapsed()),
                                Err(e) => {
                                    m.record_failure(&kind, &e.to_string(), started.elapsed())
                                }
                            }
                        }
                        result
                    }
                },
                move |cr, err, ctx| error_policy_r.error_policy(cr, err, ctx),
                context,
            )
            .for_each(move |result| {
                ready_ref.store(true, Ordering::Release);
                async move {
                    match result {
                        Ok((obj, _)) => info!(name = %obj.name, "Reconcile succeeded"),
                        Err(e) => warn!(error = %e, "Reconcile failed"),
                    }
                }
            });

        if has_stop {
            tokio::select! {
                _ = run_loop => {},
                _ = async move { stop_rx.wait_for(|v| *v).await.ok(); } => {},
            }
        } else {
            run_loop.await;
        }

        Ok(())
    }
}
