use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use durable_db::{
    api::{App, router},
    auth::Auth,
    cache::Cache,
    engine::{Engine, Limits},
    storage::Remote,
};
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

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
    let app = router(Arc::new(App::new(engine, auth, 8)));
    let mut held = Vec::new();
    for _ in 0..2 {
        let entered = Arc::new(Notify::new());
        let notify = entered.clone();
        let body = Body::from_stream(futures::stream::once(async move {
            notify.notify_one();
            std::future::pending::<()>().await;
            Ok::<_, Infallible>(Bytes::from_static(b"{}"))
        }));
        let req = Request::builder()
            .method("POST")
            .uri("/v1/databases/_admin/read")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {hostile}"))
            .body(body)
            .unwrap();
        held.push(tokio::spawn(app.clone().oneshot(req)));
        tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
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
    assert_eq!(
        status(&app, &admin, "/metrics", "GET").await,
        StatusCode::OK
    );
    let task = held.pop().unwrap();
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
