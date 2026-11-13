use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use durable_db::{
    api::{App, RateLimits, router},
    auth::Auth,
    cache::Cache,
    engine::{Engine, Limits},
    storage::Remote,
};
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn unavailable_http1_response_releases_pooled_connection_for_retry() {
    use durable_db::{Error, Result, api::Backend, auth::Access, engine::Info};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::{net::TcpListener, task::JoinSet};

    struct Endpoint(bool);
    #[async_trait::async_trait]
    impl Backend for Endpoint {
        async fn memory_bytes(&self) -> usize {
            0
        }
        async fn info(&self, _: &str, _: &Access) -> Result<Info> {
            Err(if self.0 {
                Error::Storage(anyhow::anyhow!("unreachable owner"))
            } else {
                Error::NotFound
            })
        }
    }
    let auth = Arc::new(Auth::new(&"ab".repeat(32)).unwrap());
    let token = auth.issue("app".into(), false, 3600).unwrap();
    let unavailable = router(Arc::new(App::new(
        Arc::new(Endpoint(true)),
        auth.clone(),
        8,
    )));
    let available = router(Arc::new(App::new(Arc::new(Endpoint(false)), auth, 8)));
    // Hop-by-hop Connection headers must never be added to an HTTP/2 response.
    let response = unavailable
        .clone()
        .oneshot(
            Request::builder()
                .version(axum::http::Version::HTTP_2)
                .uri("/v1/databases/app")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(!response.headers().contains_key("connection"));

    let mut tasks = JoinSet::new();
    let mut endpoints = Vec::new();
    for app in [unavailable, available] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push(listener.local_addr().unwrap());
        tasks.spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = accepted.clone();
    // Like a Service, select an endpoint once per TCP connection. The first
    // connection reaches an unsuitable pod; the next reaches an available one.
    tasks.spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            let (mut client, _) = listener.accept().await.unwrap();
            let endpoint = endpoints[count.fetch_add(1, Ordering::Relaxed).min(1)];
            connections.spawn(async move {
                let mut backend = tokio::net::TcpStream::connect(endpoint).await.unwrap();
                let _ = tokio::io::copy_bidirectional(&mut client, &mut backend).await;
            });
        }
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .retry(reqwest::retry::never())
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap();
    let mut closed = Vec::new();
    for expected in [
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::NOT_FOUND,
        StatusCode::NOT_FOUND,
    ] {
        let response = client
            .get(format!("http://{address}/v1/databases/app"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        closed.push(response.headers().get("connection").is_some());
        let body: serde_json::Value = response.json().await.unwrap();
        if expected == StatusCode::SERVICE_UNAVAILABLE {
            assert_eq!(body["error"], "storage_unavailable");
            assert!(
                body["message"]
                    .as_str()
                    .unwrap()
                    .contains("retry the identical request")
            );
        }
    }
    assert_eq!(closed, [true, false, false]);
    assert_eq!(
        accepted.load(Ordering::Relaxed),
        2,
        "The healthy connection remains reusable"
    );
}

#[tokio::test(start_paused = true)]
async fn configured_request_budgets_weight_commits_and_share_by_identity() {
    let auth = Auth::new(&"aa".repeat(32)).unwrap();
    let writer = auth.issue("alpha".into(), true, 3600).unwrap();
    let reader = auth.issue("alpha".into(), false, 3600).unwrap();
    let other = auth.issue("beta".into(), false, 3600).unwrap();
    let admin = auth.admin_token();
    let metrics = auth.metrics_token(3600).unwrap();
    let storage =
        Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "budget").unwrap());
    let engine = Arc::new(Engine::new(storage, Arc::new(Cache::new(0, None).unwrap())));
    engine.create("alpha", Limits::default()).await.unwrap();
    engine.create("beta", Limits::default()).await.unwrap();
    let app = router(Arc::new(App::with_rate_limits(
        engine,
        auth,
        8,
        RateLimits::new(1, 5).unwrap(),
    )));
    async fn call(app: &axum::Router, path: &str, token: &str, body: Option<String>) -> StatusCode {
        app.clone()
            .oneshot(
                Request::builder()
                    .method(if body.is_some() { "POST" } else { "GET" })
                    .uri(path)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.unwrap_or_default()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }
    let commit = |revision| {
        Some(format!(
            r#"{{"expected_revision":{revision},"request_id":"write-{revision}","writes":[{{"key":"x","value":"eA=="}}]}}"#
        ))
    };
    assert_eq!(
        call(&app, "/v1/databases/alpha/commit", &writer, commit(0)).await,
        StatusCode::OK
    );
    // A fresh token or a different path cannot select a fresh budget.
    for path in ["/v1/databases/alpha", "/v1/databases/beta"] {
        assert_eq!(
            call(&app, path, &reader, None).await,
            StatusCode::TOO_MANY_REQUESTS
        );
    }
    for (path, token) in [
        ("/v1/databases/beta", &other),
        ("/metrics", &admin),
        ("/metrics", &metrics),
    ] {
        assert_eq!(call(&app, path, token, None).await, StatusCode::OK);
    }
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert_eq!(
        call(&app, "/v1/databases/alpha", &reader, None).await,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, "/v1/databases/alpha/commit", &writer, commit(1)).await,
        StatusCode::TOO_MANY_REQUESTS
    );
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    assert_eq!(
        call(&app, "/v1/databases/alpha/commit", &writer, commit(1)).await,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, "/v1/databases/alpha", &reader, None).await,
        StatusCode::TOO_MANY_REQUESTS
    );
}

#[tokio::test]
async fn http_enforces_auth_scopes_body_limits_and_consistent_results() {
    let auth = Auth::new(&"ab".repeat(32)).unwrap();
    let admin = auth.admin_token();
    let read = auth.issue("alpha".into(), false, 3600).unwrap();
    let write = auth.issue("alpha".into(), true, 3600).unwrap();
    let storage =
        Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "http").unwrap());
    let engine = Arc::new(Engine::new(
        storage,
        Arc::new(Cache::new(1024 * 1024, None).unwrap()),
    ));
    engine.create("alpha", Limits::default()).await.unwrap();
    engine.create("beta", Limits::default()).await.unwrap();
    let app = router(Arc::new(App::new(engine, auth, 8)));
    async fn call(
        app: &axum::Router,
        path: &str,
        token: Option<&str>,
        body: &str,
    ) -> axum::response::Response {
        let mut req = Request::builder()
            .uri(path)
            .method("POST")
            .header("content-type", "application/json");
        if let Some(token) = token {
            req = req.header("authorization", format!("Bearer {token}"));
        }
        app.clone()
            .oneshot(req.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap()
    }
    assert_eq!(
        call(&app, "/v1/databases/alpha/read", None, "{}")
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, "/v1/databases/beta/read", Some(&read), "{}")
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let body =
        r#"{"expected_revision":0,"request_id":"one","writes":[{"key":"x","value":"aGk="}]}"#;
    assert_eq!(
        call(&app, "/v1/databases/alpha/commit", Some(&read), body)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = call(&app, "/v1/databases/alpha/commit", Some(&write), body).await;
    assert_eq!(response.status(), StatusCode::OK);
    let response = call(
        &app,
        "/v1/databases/alpha/read",
        Some(&read),
        r#"{"keys":["x"]}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["values"][0], "aGk=");
    assert_eq!(value["revision"], 1);
    assert_eq!(
        call(
            &app,
            "/v1/databases/alpha/read",
            Some(&admin),
            &"x".repeat(1024 * 1024 + 1)
        )
        .await
        .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let mut forged = write.into_bytes();
    forged[0] = if forged[0] == b'A' { b'B' } else { b'A' };
    assert_eq!(
        call(
            &app,
            "/v1/databases/alpha/read",
            Some(std::str::from_utf8(&forged).unwrap()),
            "{}"
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    let metrics = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", format!("Bearer {admin}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics = String::from_utf8(
        metrics
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(metrics.contains(
        "durable_db_operation_duration_seconds_bucket{operation=\"http_read\",le=\"+Inf\"}"
    ));
    let unauthorized = metrics
        .lines()
        .find(|line| {
            line.starts_with(
                "durable_db_operations_total{operation=\"http_read\",outcome=\"unauthorized\"}",
            )
        })
        .unwrap();
    assert!(
        unauthorized
            .rsplit_once(' ')
            .unwrap()
            .1
            .parse::<u64>()
            .unwrap()
            >= 2
    );
    for private in ["alpha", "beta", &admin, &read, "aGk="] {
        assert!(!metrics.contains(private));
    }
}

#[tokio::test]
async fn a_database_cannot_hold_every_slot_and_cancelled_requests_release_capacity() {
    use bytes::Bytes;
    use std::convert::Infallible;
    use tokio::sync::Notify;
    let auth = Auth::new(&"cd".repeat(32)).unwrap();
    let admin = auth.admin_token();
    // This legal database name used to share the administrator's rate bucket.
    let hostile = auth.issue("_admin".into(), false, 3600).unwrap();
    let other = auth.issue("other".into(), false, 3600).unwrap();
    let storage =
        Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "fairness").unwrap());
    let engine = Arc::new(Engine::new(storage, Arc::new(Cache::new(0, None).unwrap())));
    engine.create("_admin", Limits::default()).await.unwrap();
    engine.create("other", Limits::default()).await.unwrap();
    let metrics = auth.metrics_token(3600).unwrap();
    let neighbors = [
        other.clone(),
        auth.issue("third".into(), false, 3600).unwrap(),
        auth.issue("fourth".into(), false, 3600).unwrap(),
    ];
    let app = router(Arc::new(App::new(engine, auth, 8)));
    async fn hold(
        app: &axum::Router,
        token: &str,
        path: &str,
    ) -> tokio::task::JoinHandle<Result<axum::response::Response, Infallible>> {
        let entered = Arc::new(Notify::new());
        let notify = entered.clone();
        let body = Body::from_stream(futures::stream::once(async move {
            notify.notify_one();
            std::future::pending::<()>().await;
            Ok::<_, Infallible>(Bytes::from_static(b"{}"))
        }));
        let req = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .body(body)
            .unwrap();
        let work = tokio::spawn(app.clone().oneshot(req));
        tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        work
    }
    let mut held = Vec::new();
    for _ in 0..2 {
        held.push(hold(&app, &hostile, "/v1/databases/_admin/read").await);
    }
    async fn status(app: &axum::Router, token: &str, path: &str, method: &str) -> StatusCode {
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .method(method)
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }
    assert_eq!(
        status(&app, &hostile, "/v1/databases/_admin/read", "POST").await,
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(
        status(&app, &other, "/v1/databases/other/read", "POST").await,
        StatusCode::OK
    );
    // Multiple tenants may fill the entire data pool, including body reads
    // or stalled forwarding. Recovery/monitoring still have bounded capacity.
    for (name, token) in ["other", "third", "fourth"].into_iter().zip(&neighbors) {
        for _ in 0..2 {
            held.push(hold(&app, token, &format!("/v1/databases/{name}/read")).await);
        }
    }
    assert_eq!(
        status(&app, &other, "/v1/databases/other/read", "POST").await,
        StatusCode::TOO_MANY_REQUESTS
    );
    for (token, path) in [
        (&admin, "/metrics"),
        (&metrics, "/metrics"),
        (&admin, "/v1/storage/upload-budget"),
    ] {
        assert_eq!(status(&app, token, path, "GET").await, StatusCode::OK);
    }
    // Control has two slots, not an unlimited bypass. Cancellation returns one
    // without freeing any tenant slot or accepting a new principal identity.
    let first = hold(&app, &admin, "/v1/tokens").await;
    let second = hold(&app, &admin, "/v1/tokens").await;
    assert_eq!(
        status(&app, &metrics, "/metrics", "GET").await,
        StatusCode::TOO_MANY_REQUESTS
    );
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert_eq!(
        status(&app, &metrics, "/metrics", "GET").await,
        StatusCode::OK
    );
    second.abort();
    assert!(second.await.unwrap_err().is_cancelled());
    let task = held.remove(0);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        status(&app, &hostile, "/v1/databases/_admin/read", "POST").await,
        StatusCode::OK
    );
    for task in held {
        task.abort();
        let _ = task.await;
    }
    // Ordinary administrator data requests keep the data pool and cannot
    // consume their operator's separate recovery/monitoring allowance.
    let first = hold(&app, &admin, "/v1/databases/_admin/read").await;
    let second = hold(&app, &admin, "/v1/databases/_admin/read").await;
    assert_eq!(
        status(&app, &admin, "/v1/storage/upload-budget", "GET").await,
        StatusCode::OK
    );
    first.abort();
    second.abort();
    let _ = first.await;
    let _ = second.await;
}

#[tokio::test]
async fn metrics_capabilities_cannot_access_data_or_mint_other_credentials() {
    let auth = Auth::new(&"ef".repeat(32)).unwrap();
    let metrics = auth.metrics_token(3600).unwrap();
    let tenant = auth.issue("_metrics".into(), false, 3600).unwrap();
    assert!(auth.is_metrics(&metrics));
    assert!(!auth.is_admin(&metrics));
    assert!(
        auth.claims(metrics.strip_prefix("ddb_metrics_").unwrap())
            .is_err()
    );
    assert!(!Auth::new(&"ee".repeat(32)).unwrap().is_metrics(&metrics));
    assert!(auth.metrics_token(0).is_err());
    assert!(auth.metrics_token(86400 * 30 + 1).is_err());
    let storage = Arc::new(
        Remote::new(
            Arc::new(object_store::memory::InMemory::new()),
            "metrics-scope",
        )
        .unwrap(),
    );
    let engine = Arc::new(Engine::new(storage, Arc::new(Cache::new(0, None).unwrap())));
    engine.create("_metrics", Limits::default()).await.unwrap();
    let app = router(Arc::new(App::new(engine, auth, 8)));
    for (method, path, token, status) in [
        ("GET", "/metrics", metrics.as_str(), StatusCode::OK),
        ("GET", "/metrics", tenant.as_str(), StatusCode::UNAUTHORIZED),
        (
            "GET",
            "/v1/databases/_metrics",
            metrics.as_str(),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            "/v1/databases/new",
            metrics.as_str(),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            "/v1/databases/_metrics/read",
            metrics.as_str(),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            "/v1/databases/_metrics/commit",
            metrics.as_str(),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            "/v1/databases/_metrics/gc",
            metrics.as_str(),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            "/v1/tokens",
            metrics.as_str(),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "GET",
            "/v1/databases/_metrics",
            tenant.as_str(),
            StatusCode::OK,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status, "{method} {path}");
    }
}

#[tokio::test]
async fn a_full_principal_table_keeps_administrator_and_metrics_access() {
    let auth = Auth::new(&"ad".repeat(32)).unwrap();
    let admin = auth.admin_token();
    let metrics = auth.metrics_token(3600).unwrap();
    let tokens: Vec<_> = (0..4097)
        .map(|n| auth.issue(format!("tenant-{n}"), false, 3600).unwrap())
        .collect();
    let storage = Arc::new(
        Remote::new(
            Arc::new(object_store::memory::InMemory::new()),
            "full-table",
        )
        .unwrap(),
    );
    let engine = Arc::new(Engine::new(storage, Arc::new(Cache::new(0, None).unwrap())));
    let app = router(Arc::new(App::new(engine, auth, 8)));
    for (n, token) in tokens.iter().enumerate() {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/databases/tenant-{n}"))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if n < 4096 {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::TOO_MANY_REQUESTS
            }
        );
    }
    for token in [&admin, &metrics] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test(start_paused = true)]
async fn principal_churn_does_not_reset_a_slowly_refilling_budget() {
    let auth = Auth::new(&"ae".repeat(32)).unwrap();
    let tokens: Vec<_> = (0..4097)
        .map(|n| auth.issue(format!("tenant-{n}"), false, 3600).unwrap())
        .collect();
    let storage =
        Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "eviction").unwrap());
    let engine = Arc::new(Engine::new(storage, Arc::new(Cache::new(0, None).unwrap())));
    let app = router(Arc::new(App::with_rate_limits(
        engine,
        auth,
        8,
        RateLimits::new(1, 100).unwrap(),
    )));
    async fn call(app: &axum::Router, token: &str, tenant: usize) -> StatusCode {
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/databases/tenant-{tenant}"))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }
    for _ in 0..100 {
        assert_eq!(call(&app, &tokens[0], 0).await, StatusCode::NOT_FOUND);
    }
    assert_eq!(
        call(&app, &tokens[0], 0).await,
        StatusCode::TOO_MANY_REQUESTS
    );
    for (n, token) in tokens.iter().enumerate().take(4096).skip(1) {
        assert_eq!(call(&app, token, n).await, StatusCode::NOT_FOUND);
    }
    assert_eq!(
        call(&app, &tokens[4096], 4096).await,
        StatusCode::TOO_MANY_REQUESTS
    );
    tokio::time::advance(std::time::Duration::from_secs(60)).await;
    // Churn evicts fully refilled principals, but tenant zero is still depleted.
    assert_eq!(call(&app, &tokens[4096], 4096).await, StatusCode::NOT_FOUND);
    for _ in 0..60 {
        assert_eq!(call(&app, &tokens[0], 0).await, StatusCode::NOT_FOUND);
    }
    assert_eq!(
        call(&app, &tokens[0], 0).await,
        StatusCode::TOO_MANY_REQUESTS
    );
}
