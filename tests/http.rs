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
