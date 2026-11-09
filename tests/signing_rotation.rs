use durable_db::{
    api::{App, router},
    auth::Auth,
    cache::Cache,
    engine::Engine,
    storage::{Remote, Storage},
};
use serde_json::{Value, json};
use std::sync::Arc;

async fn server(auth: Auth, storage: Arc<dyn Storage>) -> (String, tokio::task::JoinHandle<()>) {
    let engine = Arc::new(Engine::new(storage, Arc::new(Cache::new(0, None).unwrap())));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router(Arc::new(App::new(engine, auth, 16))))
            .await
            .unwrap();
    });
    (url, task)
}

#[tokio::test]
async fn real_http_key_overlap_policy_revocation_and_cold_key_retirement() {
    let a = "76".repeat(32);
    let b = "87".repeat(32);
    let auth_a = Auth::new(&a).unwrap();
    let auth_b = Auth::new(&b).unwrap();
    let storage: Arc<dyn Storage> =
        Arc::new(Remote::new(Arc::new(object_store::memory::InMemory::new()), "rotation").unwrap());
    let (url_a, task_a) = server(
        Auth::new(&Auth::keyring_document(&a, vec![b.clone()], false).unwrap()).unwrap(),
        storage.clone(),
    )
    .await;
    let (url_b, task_b) = server(
        Auth::new(&Auth::keyring_document(&b, vec![a], false).unwrap()).unwrap(),
        storage.clone(),
    )
    .await;
    let client = reqwest::Client::new();
    let admins = [
        auth_a.administrator_token(3600).unwrap(),
        auth_b.administrator_token(3600).unwrap(),
    ];
    let created = client
        .put(format!("{url_a}/v1/databases/test"))
        .bearer_auth(&admins[1])
        .json(&json!({"request_id":"create", "limits":{"max_bytes":1048576,"max_keys":1000}}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    let mut tokens = Vec::new();
    for (url, admin) in [(&url_a, &admins[1]), (&url_b, &admins[0])] {
        let issued = client
            .post(format!("{url}/v1/tokens"))
            .bearer_auth(admin)
            .json(&json!({"database":"test","write":true,"ttl_seconds":3600}))
            .send()
            .await
            .unwrap();
        assert_eq!(issued.status(), 200);
        tokens.push(issued.json::<Value>().await.unwrap());
    }
    let tenant_a = tokens[0]["token"].as_str().unwrap();
    let tenant_b = tokens[1]["token"].as_str().unwrap();
    assert!(auth_a.claims(tenant_a).is_ok());
    assert!(auth_b.claims(tenant_a).is_err());
    assert!(auth_b.claims(tenant_b).is_ok());
    for url in [&url_a, &url_b] {
        for tenant in [tenant_a, tenant_b] {
            assert_eq!(
                client
                    .post(format!("{url}/v1/databases/test/read"))
                    .bearer_auth(tenant)
                    .json(&json!({}))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
        }
        for auth in [&auth_a, &auth_b] {
            assert_eq!(
                client
                    .get(format!("{url}/metrics"))
                    .bearer_auth(auth.metrics_token(3600).unwrap())
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            assert_eq!(
                client
                    .get(format!("{url}/metrics"))
                    .bearer_auth(auth.admin_token())
                    .send()
                    .await
                    .unwrap()
                    .status(),
                401
            );
        }
    }
    assert_eq!(
        client
            .post(format!("{url_b}/v1/databases/test/access"))
            .bearer_auth(&admins[0])
            .json(&json!({"request_id":"revoke", "expected_policy_revision":1,
            "change":{"action":"revoke", "token_id":tokens[0]["token_id"]}}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    for url in [&url_a, &url_b] {
        assert_eq!(
            client
                .post(format!("{url}/v1/databases/test/read"))
                .bearer_auth(tenant_a)
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
        assert_eq!(
            client
                .post(format!("{url}/v1/databases/test/read"))
                .bearer_auth(tenant_b)
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    task_a.abort();
    task_a.await.unwrap_err();
    task_b.abort();
    task_b.await.unwrap_err();
    // Forced retirement intentionally rejects still-live A capabilities. A
    // graceful operator rotation waits for their expiry before this phase.
    let (url, task) = server(
        Auth::new(&Auth::keyring_document(&b, vec![], false).unwrap()).unwrap(),
        storage,
    )
    .await;
    for (admin, status) in [(&admins[0], 401), (&admins[1], 200)] {
        assert_eq!(
            client
                .get(format!("{url}/v1/databases/test"))
                .bearer_auth(admin)
                .send()
                .await
                .unwrap()
                .status(),
            status
        );
    }
    assert_eq!(
        client
            .post(format!("{url}/v1/databases/test/read"))
            .bearer_auth(tenant_b)
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .get(format!("{url}/metrics"))
            .bearer_auth(auth_a.metrics_token(3600).unwrap())
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    task.abort();
    task.await.unwrap_err();
}
