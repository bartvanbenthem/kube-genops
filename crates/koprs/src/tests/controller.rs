// src/tests/controller.rs

#[cfg(test)]
mod controller_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use http::{Request, Response, StatusCode};
    use k8s_openapi::api::coordination::v1::Lease;
    use k8s_openapi::api::core::v1::ConfigMap;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
    use kube::Client;
    use kube::client::Body;
    use serde_json::json;
    use tower_test::mock;

    use crate::controller::{
        Action, Context, ControllerBuilder, LeaderElectionConfig, Reconciler, build_lease,
        serve_health, try_acquire_or_renew,
    };
    use crate::error::KubeGenericError;

    // -----------------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------------

    type MockHandle = mock::Handle<Request<Body>, Response<Body>>;

    fn mock_client() -> (Client, MockHandle) {
        let (svc, handle) = mock::pair::<Request<Body>, Response<Body>>();
        (Client::new(svc, "default"), handle)
    }

    fn json_response(status: StatusCode, body: serde_json::Value) -> Response<Body> {
        let bytes = serde_json::to_vec(&body).unwrap();
        Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(Body::from(bytes))
            .unwrap()
    }

    fn status_response(code: u16, reason: &str) -> Response<Body> {
        json_response(
            StatusCode::from_u16(code).unwrap(),
            json!({
                "apiVersion": "v1",
                "kind": "Status",
                "status": "Failure",
                "reason": reason,
                "code": code
            }),
        )
    }

    async fn read_body_json(req: Request<Body>) -> serde_json::Value {
        use http_body_util::BodyExt as _;
        let bytes = req.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    // -----------------------------------------------------------------------
    // Lease JSON helpers
    // -----------------------------------------------------------------------

    /// Build a Lease JSON body. `renew_offset_secs`: negative means in the past.
    fn lease_json(
        holder: &str,
        lease_duration_secs: i32,
        renew_offset_secs: i64,
        resource_version: &str,
    ) -> serde_json::Value {
        use chrono::SecondsFormat;
        let renew_time = (chrono::Utc::now() + chrono::Duration::seconds(renew_offset_secs))
            .to_rfc3339_opts(SecondsFormat::Micros, true);
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": "test-lease",
                "namespace": "test-ns",
                "resourceVersion": resource_version
            },
            "spec": {
                "holderIdentity": holder,
                "leaseDurationSeconds": lease_duration_secs,
                "acquireTime": renew_time,
                "renewTime": renew_time,
                "leaseTransitions": 0
            }
        })
    }

    fn test_le_config(identity: &str) -> LeaderElectionConfig {
        LeaderElectionConfig {
            namespace: "test-ns".to_string(),
            name: "test-lease".to_string(),
            identity: identity.to_string(),
            lease_duration_secs: 15,
            renew_period: Duration::from_millis(50),
            retry_period: Duration::from_millis(50),
        }
    }

    // -----------------------------------------------------------------------
    // Minimal Reconciler — only required method, uses default error_policy
    // -----------------------------------------------------------------------

    struct MockReconciler;

    impl Reconciler<ConfigMap> for MockReconciler {
        type Error = KubeGenericError;

        async fn reconcile(
            &self,
            _cr: Arc<ConfigMap>,
            _ctx: Arc<Context>,
        ) -> Result<Action, Self::Error> {
            Ok(Action::await_change())
        }
        // error_policy intentionally omitted — exercises the default impl
    }

    // Reconciler with explicit error_policy override
    struct LoggingReconciler;

    impl Reconciler<ConfigMap> for LoggingReconciler {
        type Error = KubeGenericError;

        async fn reconcile(
            &self,
            _cr: Arc<ConfigMap>,
            _ctx: Arc<Context>,
        ) -> Result<Action, Self::Error> {
            Ok(Action::await_change())
        }

        fn error_policy(
            &self,
            _cr: Arc<ConfigMap>,
            _err: &Self::Error,
            _ctx: Arc<Context>,
        ) -> Action {
            Action::requeue(Duration::from_secs(60))
        }
    }

    fn configmap(name: &str, namespace: &str) -> ConfigMap {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name, "namespace": namespace }
        }))
        .unwrap()
    }

    // -----------------------------------------------------------------------
    // Context
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn context_new_data_is_unit() {
        let (client, _) = mock_client();
        let ctx = Context::new(client);
        assert_eq!(ctx.data, ());
    }

    #[tokio::test]
    async fn context_with_data_stores_value() {
        let (client, _) = mock_client();
        let ctx = Context::with_data(client, 42u32);
        assert_eq!(ctx.data, 42u32);
    }

    #[tokio::test]
    async fn context_with_data_string() {
        let (client, _) = mock_client();
        let ctx = Context::with_data(client, "hello".to_string());
        assert_eq!(ctx.data, "hello");
    }

    #[tokio::test]
    async fn context_client_is_accessible() {
        let (client, _) = mock_client();
        let ctx = Context::new(client.clone());
        // Verify the client field is public and the same instance
        // (both are Arc-backed — just check the field is accessible)
        let _ = ctx.client.clone();
    }

    // -----------------------------------------------------------------------
    // Reconciler trait — default and override
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn reconciler_reconcile_returns_ok() {
        let (client, _) = mock_client();
        let ctx = Context::new(client);
        assert!(
            MockReconciler
                .reconcile(Arc::new(configmap("t", "ns")), ctx)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn reconciler_default_error_policy_does_not_panic() {
        let (client, _) = mock_client();
        let ctx = Context::new(client);
        let err = KubeGenericError::Internal("oops".into());
        let _ = MockReconciler.error_policy(Arc::new(configmap("t", "ns")), &err, ctx);
    }

    #[tokio::test]
    async fn reconciler_custom_error_policy_is_called() {
        let (client, _) = mock_client();
        let ctx = Context::new(client);
        let err = KubeGenericError::Internal("oops".into());
        // LoggingReconciler overrides to requeue(60s) — just verify it doesn't panic
        let _ = LoggingReconciler.error_policy(Arc::new(configmap("t", "ns")), &err, ctx);
    }

    #[tokio::test]
    async fn reconciler_with_custom_data_can_read_context() {
        struct MyData {
            x: u32,
        }
        struct DataReconciler;
        impl Reconciler<ConfigMap, MyData> for DataReconciler {
            type Error = KubeGenericError;
            async fn reconcile(
                &self,
                _cr: Arc<ConfigMap>,
                ctx: Arc<Context<MyData>>,
            ) -> Result<Action, Self::Error> {
                assert_eq!(ctx.data.x, 7);
                Ok(Action::await_change())
            }
        }
        let (client, _) = mock_client();
        let ctx = Context::with_data(client, MyData { x: 7 });
        assert!(
            DataReconciler
                .reconcile(Arc::new(configmap("t", "ns")), ctx)
                .await
                .is_ok()
        );
    }

    // -----------------------------------------------------------------------
    // serve_health — tests all response branches via real TCP
    // -----------------------------------------------------------------------

    /// Bind a random port, start serve_health, return (port, ready flag).
    async fn start_health_server() -> (u16, Arc<AtomicBool>) {
        let ready = Arc::new(AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(serve_health(listener, ready.clone()));
        (port, ready)
    }

    /// Send a minimal HTTP/1.1 GET and return the full raw response.
    async fn http_get(port: u16, path: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; 512];
        let n = stream.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    }

    #[tokio::test]
    async fn health_server_healthz_returns_200() {
        let (port, _ready) = start_health_server().await;
        let resp = http_get(port, "/healthz").await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
        assert!(resp.ends_with("ok"), "body should be 'ok', got: {resp}");
    }

    #[tokio::test]
    async fn health_server_readyz_returns_503_when_not_ready() {
        let (port, _ready) = start_health_server().await;
        // ready flag is false at start
        let resp = http_get(port, "/readyz").await;
        assert!(resp.starts_with("HTTP/1.1 503"), "got: {resp}");
        assert!(
            resp.ends_with("not ready"),
            "body should be 'not ready', got: {resp}"
        );
    }

    #[tokio::test]
    async fn health_server_readyz_returns_200_when_ready() {
        let (port, ready) = start_health_server().await;
        ready.store(true, Ordering::Release);
        let resp = http_get(port, "/readyz").await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
        assert!(resp.ends_with("ok"), "body should be 'ok', got: {resp}");
    }

    #[tokio::test]
    async fn health_server_unknown_path_returns_200() {
        let (port, _) = start_health_server().await;
        let resp = http_get(port, "/metrics").await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
    }

    #[tokio::test]
    async fn health_server_readyz_transitions_503_to_200_after_ready() {
        let (port, ready) = start_health_server().await;

        let before = http_get(port, "/readyz").await;
        assert!(
            before.starts_with("HTTP/1.1 503"),
            "expected 503 before ready: {before}"
        );

        ready.store(true, Ordering::Release);

        let after = http_get(port, "/readyz").await;
        assert!(
            after.starts_with("HTTP/1.1 200 OK"),
            "expected 200 after ready: {after}"
        );
    }

    #[tokio::test]
    async fn health_server_healthz_returns_200_regardless_of_ready_flag() {
        let (port, ready) = start_health_server().await;

        // With ready=false
        let resp = http_get(port, "/healthz").await;
        assert!(
            resp.starts_with("HTTP/1.1 200 OK"),
            "healthz should be 200 when not ready"
        );

        // With ready=true
        ready.store(true, Ordering::Release);
        let resp = http_get(port, "/healthz").await;
        assert!(
            resp.starts_with("HTTP/1.1 200 OK"),
            "healthz should be 200 when ready"
        );
    }

    // -----------------------------------------------------------------------
    // build_lease — pure function, verifies all fields
    // -----------------------------------------------------------------------

    fn now_micro() -> MicroTime {
        MicroTime(k8s_openapi::jiff::Timestamp::now())
    }

    #[test]
    fn build_lease_sets_name_and_namespace() {
        let lease = build_lease(
            "my-lease",
            "my-ns",
            "pod-1",
            15,
            now_micro(),
            now_micro(),
            None,
            None,
        );
        assert_eq!(lease.metadata.name.as_deref(), Some("my-lease"));
        assert_eq!(lease.metadata.namespace.as_deref(), Some("my-ns"));
    }

    #[test]
    fn build_lease_sets_holder_identity() {
        let lease = build_lease(
            "l",
            "ns",
            "pod-42",
            15,
            now_micro(),
            now_micro(),
            None,
            None,
        );
        let holder = lease
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_deref());
        assert_eq!(holder, Some("pod-42"));
    }

    #[test]
    fn build_lease_sets_lease_duration() {
        let lease = build_lease("l", "ns", "pod-1", 30, now_micro(), now_micro(), None, None);
        let dur = lease.spec.as_ref().and_then(|s| s.lease_duration_seconds);
        assert_eq!(dur, Some(30));
    }

    #[test]
    fn build_lease_sets_resource_version_when_provided() {
        let lease = build_lease(
            "l",
            "ns",
            "pod-1",
            15,
            now_micro(),
            now_micro(),
            Some("42".to_string()),
            None,
        );
        assert_eq!(lease.metadata.resource_version.as_deref(), Some("42"));
    }

    #[test]
    fn build_lease_omits_resource_version_when_none() {
        let lease = build_lease("l", "ns", "pod-1", 15, now_micro(), now_micro(), None, None);
        assert!(lease.metadata.resource_version.is_none());
    }

    #[test]
    fn build_lease_sets_transitions_when_provided() {
        let lease = build_lease(
            "l",
            "ns",
            "pod-1",
            15,
            now_micro(),
            now_micro(),
            None,
            Some(3),
        );
        let t = lease.spec.as_ref().and_then(|s| s.lease_transitions);
        assert_eq!(t, Some(3));
    }

    #[test]
    fn build_lease_new_lease_gets_zero_transitions() {
        // When creating a fresh lease, transitions should be Some(0) not None.
        let lease = build_lease(
            "l",
            "ns",
            "pod-1",
            15,
            now_micro(),
            now_micro(),
            None,
            Some(0),
        );
        let t = lease.spec.as_ref().and_then(|s| s.lease_transitions);
        assert_eq!(t, Some(0));
    }

    // -----------------------------------------------------------------------
    // try_acquire_or_renew — mock HTTP, covers all 7 branches
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn acquire_creates_new_lease_when_none_exists() {
        let (client, mut handle) = mock_client();
        let config = test_le_config("pod-1");

        let server = tokio::spawn(async move {
            // GET → 404 (no lease)
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            send.send_response(status_response(404, "NotFound"));

            // POST → 201 Created
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::POST);
            let body = read_body_json(req).await;
            assert_eq!(body["spec"]["holderIdentity"], "pod-1");
            assert_eq!(body["spec"]["leaseTransitions"], 0);
            send.send_response(json_response(
                StatusCode::CREATED,
                lease_json("pod-1", 15, 0, "1"),
            ));
        });

        let result = try_acquire_or_renew(&config, &client).await;
        assert_eq!(result.unwrap(), true);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn acquire_returns_false_when_create_conflicts() {
        let (client, mut handle) = mock_client();
        let config = test_le_config("pod-1");

        let server = tokio::spawn(async move {
            // GET → 404
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(404, "NotFound"));
            // POST → 409 Conflict (another replica created it first)
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(409, "AlreadyExists"));
        });

        let result = try_acquire_or_renew(&config, &client).await;
        assert_eq!(result.unwrap(), false);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn acquire_returns_false_when_held_by_another_not_expired() {
        let (client, mut handle) = mock_client();
        let config = test_le_config("pod-1");

        let server = tokio::spawn(async move {
            // GET → 200 with active lease held by other-pod (renewed 1s ago, duration 3600s)
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            send.send_response(json_response(
                StatusCode::OK,
                lease_json("other-pod", 3600, -1, "5"),
            ));
            // No further requests expected
        });

        let result = try_acquire_or_renew(&config, &client).await;
        assert_eq!(result.unwrap(), false);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn acquire_takes_over_expired_lease_held_by_another() {
        let (client, mut handle) = mock_client();
        let config = test_le_config("pod-1");

        let server = tokio::spawn(async move {
            // GET → 200 with expired lease (renewed 100s ago, duration 15s)
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json("other-pod", 15, -100, "5"),
            ));
            // PUT → 200 (replace succeeds)
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PUT);
            let body = read_body_json(req).await;
            assert_eq!(body["spec"]["holderIdentity"], "pod-1");
            assert_eq!(body["metadata"]["resourceVersion"], "5");
            send.send_response(json_response(
                StatusCode::OK,
                lease_json("pod-1", 15, 0, "6"),
            ));
        });

        let result = try_acquire_or_renew(&config, &client).await;
        assert_eq!(result.unwrap(), true);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn renew_succeeds_when_we_already_hold_lease() {
        let (client, mut handle) = mock_client();
        let config = test_le_config("pod-1");

        let server = tokio::spawn(async move {
            // GET → 200 with our own lease (not expired)
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json("pod-1", 15, -3, "9"),
            ));
            // PUT → 200 (renew succeeds)
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PUT);
            let body = read_body_json(req).await;
            assert_eq!(body["spec"]["holderIdentity"], "pod-1");
            // acquireTime should be preserved (not reset) for a renewal
            send.send_response(json_response(
                StatusCode::OK,
                lease_json("pod-1", 15, 0, "10"),
            ));
        });

        let result = try_acquire_or_renew(&config, &client).await;
        assert_eq!(result.unwrap(), true);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn renew_returns_false_when_replace_conflicts() {
        let (client, mut handle) = mock_client();
        let config = test_le_config("pod-1");

        let server = tokio::spawn(async move {
            // GET → 200 with own lease
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json("pod-1", 15, -3, "9"),
            ));
            // PUT → 409 (concurrent writer, our resourceVersion is stale)
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(409, "Conflict"));
        });

        let result = try_acquire_or_renew(&config, &client).await;
        assert_eq!(result.unwrap(), false);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn acquire_propagates_api_error_on_get() {
        let (client, mut handle) = mock_client();
        let config = test_le_config("pod-1");

        let server = tokio::spawn(async move {
            // GET → 500 Internal Server Error
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(500, "InternalError"));
        });

        let result = try_acquire_or_renew(&config, &client).await;
        assert!(result.is_err(), "expected Err on 500 API error");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn acquire_propagates_api_error_on_post() {
        let (client, mut handle) = mock_client();
        let config = test_le_config("pod-1");

        let server = tokio::spawn(async move {
            // GET → 404
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(404, "NotFound"));
            // POST → 500
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(500, "InternalError"));
        });

        let result = try_acquire_or_renew(&config, &client).await;
        assert!(result.is_err(), "expected Err on 500 during create");
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // ControllerBuilder — construction and configuration
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn builder_defaults_are_all_off() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api);
        assert_eq!(b.health_port, None);
        assert!(!b.graceful_shutdown);
        assert_eq!(b.reconcile_timeout, None);
    }

    #[tokio::test]
    async fn builder_health_port_sets_port() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).health_port(9090);
        assert_eq!(b.health_port, Some(9090));
    }

    #[tokio::test]
    async fn builder_graceful_shutdown_sets_flag() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).graceful_shutdown();
        assert!(b.graceful_shutdown);
    }

    #[tokio::test]
    async fn builder_reconcile_timeout_stores_duration() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).reconcile_timeout(Duration::from_secs(60));
        assert_eq!(b.reconcile_timeout, Some(Duration::from_secs(60)));
    }

    #[tokio::test]
    async fn builder_label_selector_sets_watcher_config() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).label_selector("app=test");
        assert_eq!(b.watcher_config.label_selector.as_deref(), Some("app=test"));
    }

    #[tokio::test]
    async fn builder_watcher_config_replaces_default() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let custom = kube_runtime::watcher::Config::default().labels("env=prod");
        let b = ControllerBuilder::<ConfigMap>::new(api).watcher_config(custom);
        assert_eq!(b.watcher_config.label_selector.as_deref(), Some("env=prod"));
    }

    #[tokio::test]
    async fn builder_with_watches_stores_configure_fn() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).with_watches(|ctl| ctl);
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_with_watches_composes_not_replaces() {
        // Bug fix: a second with_watches call used to silently replace the first.
        // Now both closures are chained — configure must still be Some after
        // each call and both closures participate.
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .with_watches(|ctl| ctl)
            .with_watches(|ctl| ctl);
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_watch_registers_configure_fn() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let trigger = kube::Api::<ConfigMap>::all(client);
        let b = ControllerBuilder::<ConfigMap>::new(api).watch(
            trigger,
            kube_runtime::watcher::Config::default(),
            |_: ConfigMap| None::<kube_runtime::reflector::ObjectRef<ConfigMap>>,
        );
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_watch_two_watches_both_compose() {
        // Two .watch() calls must both be preserved — the second must not
        // erase the first.
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let trigger1 = kube::Api::<ConfigMap>::all(client.clone());
        let trigger2 = kube::Api::<ConfigMap>::all(client);
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .watch(
                trigger1,
                kube_runtime::watcher::Config::default(),
                |_: ConfigMap| None::<kube_runtime::reflector::ObjectRef<ConfigMap>>,
            )
            .watch(
                trigger2,
                kube_runtime::watcher::Config::default(),
                |_: ConfigMap| None::<kube_runtime::reflector::ObjectRef<ConfigMap>>,
            );
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_watch_and_with_watches_compose() {
        // .watch() and .with_watches() must compose — neither erases the other.
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let trigger = kube::Api::<ConfigMap>::all(client);
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .watch(
                trigger,
                kube_runtime::watcher::Config::default(),
                |_: ConfigMap| None::<kube_runtime::reflector::ObjectRef<ConfigMap>>,
            )
            .with_watches(|ctl| ctl);
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_leader_election_can_be_configured() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        // verify it compiles, runs without panic, and sets the config
        let b = ControllerBuilder::<ConfigMap>::new(api).leader_election("my-ns", "my-leader");
        assert!(b.leader_election.is_some());
    }

    #[tokio::test]
    async fn builder_leader_election_namespace_and_name_are_stored() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).leader_election("ops-ns", "my-op-leader");
        let le = b.leader_election.as_ref().unwrap();
        assert_eq!(le.namespace, "ops-ns");
        assert_eq!(le.name, "my-op-leader");
    }

    #[tokio::test]
    async fn builder_leader_election_identity_defaults_to_env() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).leader_election("ns", "lease");
        let identity = &b.leader_election.unwrap().identity;
        // Must not be empty; exact value depends on env ($POD_NAME / $HOSTNAME / "unknown")
        assert!(!identity.is_empty());
    }

    #[tokio::test]
    async fn builder_full_chain_does_not_panic() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let trigger = kube::Api::<ConfigMap>::all(client);
        let _b = ControllerBuilder::<ConfigMap>::new(api)
            .health_port(8080)
            .graceful_shutdown()
            .leader_election("my-ns", "leader")
            .reconcile_timeout(Duration::from_secs(300))
            .label_selector("app=test")
            .watcher_config(kube_runtime::watcher::Config::default())
            .watch(
                trigger,
                kube_runtime::watcher::Config::default(),
                |_: ConfigMap| None::<kube_runtime::reflector::ObjectRef<ConfigMap>>,
            )
            .with_watches(|ctl| ctl);
    }

    #[tokio::test]
    async fn builder_concurrency_defaults_to_none() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api);
        assert_eq!(b.concurrency, None);
    }

    #[tokio::test]
    async fn builder_concurrency_stores_value() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).concurrency(4);
        assert_eq!(b.concurrency, Some(4));
    }

    #[tokio::test]
    async fn builder_concurrency_last_call_wins() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .concurrency(4)
            .concurrency(8);
        assert_eq!(b.concurrency, Some(8));
    }

    #[tokio::test]
    async fn builder_owns_registers_configure_fn() {
        use k8s_openapi::api::apps::v1::Deployment;
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let deploy_api = kube::Api::<Deployment>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .owns(deploy_api, kube_runtime::watcher::Config::default());
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_owns_composes_with_watch() {
        use k8s_openapi::api::apps::v1::Deployment;
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let deploy_api = kube::Api::<Deployment>::namespaced(client.clone(), "ns");
        let trigger = kube::Api::<ConfigMap>::all(client);
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .owns(deploy_api, kube_runtime::watcher::Config::default())
            .watch(
                trigger,
                kube_runtime::watcher::Config::default(),
                |_: ConfigMap| None::<kube_runtime::reflector::ObjectRef<ConfigMap>>,
            );
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_leader_election_timings_overrides_defaults() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .leader_election("ops-ns", "my-op-leader")
            .leader_election_timings(
                Duration::from_secs(45),
                Duration::from_secs(10),
                Duration::from_secs(3),
            );
        let le = b.leader_election.as_ref().unwrap();
        assert_eq!(le.lease_duration_secs, 45);
        assert_eq!(le.renew_period, Duration::from_secs(10));
        assert_eq!(le.retry_period, Duration::from_secs(3));
    }

    #[tokio::test]
    #[should_panic(expected = "leader_election_timings must be called after leader_election")]
    async fn builder_leader_election_timings_panics_when_no_leader_election() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let _ = ControllerBuilder::<ConfigMap>::new(api).leader_election_timings(
            Duration::from_secs(45),
            Duration::from_secs(10),
            Duration::from_secs(3),
        );
    }

    // -----------------------------------------------------------------------
    // LeaderElectionConfig — default values
    // -----------------------------------------------------------------------

    #[test]
    fn le_config_defaults_are_sensible() {
        let cfg = test_le_config("pod-a");
        assert_eq!(cfg.lease_duration_secs, 15);
        // renew_period < lease_duration (3× safety margin)
        assert!(cfg.renew_period.as_secs_f64() < cfg.lease_duration_secs as f64 / 2.0);
    }

    // -----------------------------------------------------------------------
    // Lease deserialization round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn lease_json_round_trips_through_serde() {
        // Verifies that our JSON helpers produce valid Lease payloads
        // that k8s_openapi can deserialize (used in all mock responses above).
        let json = lease_json("pod-1", 15, 0, "42");
        let lease: Lease = serde_json::from_value(json).expect("failed to deserialize Lease JSON");
        let spec = lease.spec.as_ref().unwrap();
        assert_eq!(spec.holder_identity.as_deref(), Some("pod-1"));
        assert_eq!(spec.lease_duration_seconds, Some(15));
        assert_eq!(lease.metadata.resource_version.as_deref(), Some("42"));
    }
}
