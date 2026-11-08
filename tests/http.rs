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
