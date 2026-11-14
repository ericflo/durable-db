//! The HTTP contract in `docs/api.md`, against a real server process.
mod common;
use common::{Server, admin_token, call, free_port};
use durable_db::auth::{Access, Grant, Keys};
use serde_json::{Value, json};

struct Fixture {
    server: Server,
    data: tempfile::TempDir,
    admin: String,
}

async fn fixture(extra: &[&str]) -> Fixture {
    let data = tempfile::tempdir().unwrap();
    let server = Server::start(data.path(), free_port(), None, extra).await;
    let admin = admin_token(data.path());
    Fixture {
        server,
        data,
        admin,
    }
}

impl Fixture {
    fn db(&self, name: &str) -> String {
        format!("{}/v1/databases/{name}", self.server.url)
    }

    async fn create(&self, name: &str) -> String {
        let (status, _) = call("PUT", &self.db(name), &self.admin, None).await;
        assert_eq!(status, 201);
        let (status, body) = call(
            "POST",
            &format!("{}/tokens", self.db(name)),
            &self.admin,
            Some(json!({"access": "write"})),
        )
        .await;
        assert_eq!(status, 200);
        body["token"].as_str().unwrap().to_owned()
    }
}

fn code(body: &Value) -> &str {
    body["error"]["code"].as_str().unwrap_or_default()
}

#[tokio::test]
async fn database_lifecycle() {
    let f = fixture(&[]).await;
    let url = f.db("app");
    assert_eq!(call("GET", &url, &f.admin, None).await.0, 404);
    assert_eq!(
        call("PUT", &url, &f.admin, Some(json!({"max_bytes": 1000000})))
            .await
            .0,
        201
    );
    assert_eq!(
        call("PUT", &url, &f.admin, Some(json!({"max_bytes": 1000000})))
            .await
            .0,
        200
    );
    let (status, body) = call("PUT", &url, &f.admin, Some(json!({"max_bytes": 5}))).await;
    assert_eq!((status, code(&body)), (409, "exists"));
    assert_eq!(call("PUT", &f.db("bad name"), &f.admin, None).await.0, 400);

    let (_, info) = call("GET", &url, &f.admin, None).await;
    assert_eq!(
        (
            info["name"].as_str(),
            info["revision"].as_u64(),
            info["max_bytes"].as_u64()
        ),
        (Some("app"), Some(0), Some(1000000))
    );
    let (_, info) = call("PATCH", &url, &f.admin, Some(json!({"max_bytes": 2000000}))).await;
    assert_eq!(info["max_bytes"], 2000000);

    call("PUT", &f.db("another"), &f.admin, None).await;
    let (_, listed) = call(
        "GET",
        &format!("{}/v1/databases", f.server.url),
        &f.admin,
        None,
    )
    .await;
    assert_eq!(listed["databases"], json!(["another", "app"]));
    let (_, page) = call(
        "GET",
        &format!("{}/v1/databases?limit=1", f.server.url),
        &f.admin,
        None,
    )
    .await;
    assert_eq!(
        (page["databases"].clone(), page["next"].clone()),
        (json!(["another"]), json!("another"))
    );

    assert_eq!(call("DELETE", &url, &f.admin, None).await.0, 200);
    let (status, body) = call("GET", &url, &f.admin, None).await;
    assert_eq!((status, code(&body)), (410, "deleted"));
    assert_eq!(call("PUT", &url, &f.admin, None).await.0, 410);
    let (_, listed) = call(
        "GET",
        &format!("{}/v1/databases", f.server.url),
        &f.admin,
        None,
    )
    .await;
    assert_eq!(listed["databases"], json!(["another"]));
}

#[tokio::test]
async fn tokens_scope_access() {
    let f = fixture(&[]).await;
    let write = f.create("app").await;
    f.create("other").await;
    let read = {
        let (_, body) = call(
            "POST",
            &format!("{}/tokens", f.db("app")),
            &f.admin,
            Some(json!({"access": "read"})),
        )
        .await;
        body["token"].as_str().unwrap().to_owned()
    };
    let commit = json!({"writes": [{"key": "k", "value": 1}]});
    let read_body = json!({"keys": ["k"]});

    assert_eq!(
        call(
            "POST",
            &format!("{}/read", f.db("app")),
            "",
            Some(read_body.clone())
        )
        .await
        .0,
        401
    );
    assert_eq!(
        call(
            "POST",
            &format!("{}/read", f.db("app")),
            "ddb_forged.token",
            Some(read_body.clone())
        )
        .await
        .0,
        401
    );
    assert_eq!(
        call(
            "POST",
            &format!("{}/commit", f.db("app")),
            &write,
            Some(commit.clone())
        )
        .await
        .0,
        200
    );
    assert_eq!(
        call(
            "POST",
            &format!("{}/read", f.db("app")),
            &read,
            Some(read_body.clone())
        )
        .await
        .0,
        200
    );
    // A read token cannot write; a token for one database cannot touch another
    // or manage anything.
    assert_eq!(
        call(
            "POST",
            &format!("{}/commit", f.db("app")),
            &read,
            Some(commit.clone())
        )
        .await
        .0,
        403
    );
    assert_eq!(
        call(
            "POST",
            &format!("{}/read", f.db("other")),
            &write,
            Some(read_body.clone())
        )
        .await
        .0,
        403
    );
    assert_eq!(call("DELETE", &f.db("app"), &write, None).await.0, 403);
    assert_eq!(
        call(
            "POST",
            &format!("{}/tokens", f.db("app")),
            &write,
            Some(json!({"access": "write"}))
        )
        .await
        .0,
        403
    );
    assert_eq!(
        call(
            "GET",
            &format!("{}/v1/databases", f.server.url),
            &write,
            None
        )
        .await
        .0,
        403
    );
    // A token signed with another key, or already expired, is refused.
    let foreign = Keys::from_secret(&[9; 32]).mint(&Grant::admin());
    assert_eq!(call("GET", &f.db("app"), &foreign, None).await.0, 401);
    let expired = Keys::load(&common::key_file(f.data.path()))
        .unwrap()
        .mint(&Grant {
            db: "app".into(),
            access: Access::Write,
            exp: 1,
            generation: 0,
        });
    assert_eq!(
        call(
            "POST",
            &format!("{}/read", f.db("app")),
            &expired,
            Some(read_body.clone())
        )
        .await
        .0,
        401
    );

    // Revocation voids every token issued so far; new ones work.
    assert_eq!(
        call(
            "PATCH",
            &f.db("app"),
            &f.admin,
            Some(json!({"revoke_tokens": true}))
        )
        .await
        .0,
        200
    );
    assert_eq!(
        call(
            "POST",
            &format!("{}/read", f.db("app")),
            &write,
            Some(read_body.clone())
        )
        .await
        .0,
        401
    );
    let (_, body) = call(
        "POST",
        &format!("{}/tokens", f.db("app")),
        &f.admin,
        Some(json!({"access": "write"})),
    )
    .await;
    let renewed = body["token"].as_str().unwrap().to_owned();
    assert_eq!(
        call(
            "POST",
            &format!("{}/read", f.db("app")),
            &renewed,
            Some(read_body)
        )
        .await
        .0,
        200
    );
}

#[tokio::test]
async fn reads_scans_and_transactions() {
    let f = fixture(&[]).await;
    let token = f.create("app").await;
    let (read, commit) = (
        format!("{}/read", f.db("app")),
        format!("{}/commit", f.db("app")),
    );

    // Values come back as the JSON that was sent.
    let value = json!({"text": "héllo \"quoted\"", "n": [1, 2.5, null, true], "nested": {"a": {}}});
    let writes: Vec<Value> = (0..25)
        .map(|i| json!({"key": format!("item/{i:02}"), "value": {"i": i}}))
        .collect();
    let (status, body) = call(
        "POST",
        &commit,
        &token,
        Some(json!({"writes": [{"key": "doc", "value": value}]})),
    )
    .await;
    assert_eq!((status, body["revision"].as_u64()), (200, Some(1)));
    assert_eq!(
        call("POST", &commit, &token, Some(json!({"writes": writes})))
            .await
            .1["revision"],
        2
    );

    let (_, body) = call(
        "POST",
        &read,
        &token,
        Some(json!({"keys": ["doc", "missing", "item/07"]})),
    )
    .await;
    assert_eq!(body["revision"], 2);
    assert_eq!(body["values"][0], json!({"value": value, "version": 1}));
    assert_eq!(body["values"][1], Value::Null);
    assert_eq!(body["values"][2], json!({"value": {"i": 7}, "version": 2}));

    // Pages follow `next` in either direction.
    let mut seen = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let mut scan = json!({"prefix": "item/", "limit": 10});
        if let Some(after) = &after {
            scan["after"] = json!(after);
        }
        let (_, body) = call("POST", &read, &token, Some(json!({"scans": [scan]}))).await;
        seen.extend(
            body["scans"][0]["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["key"].as_str().unwrap().to_owned()),
        );
        match body["scans"][0]["next"].as_str() {
            Some(next) => after = Some(next.to_owned()),
            None => break,
        }
    }
    assert_eq!(
        seen,
        (0..25).map(|i| format!("item/{i:02}")).collect::<Vec<_>>()
    );
    let scan = json!({"start": "item/10", "end": "item/20", "reverse": true, "limit": 3});
    let (_, body) = call("POST", &read, &token, Some(json!({"scans": [scan]}))).await;
    let keys: Vec<&str> = body["scans"][0]["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["key"].as_str().unwrap())
        .collect();
    assert_eq!(
        (keys, body["scans"][0]["next"].as_str()),
        (vec!["item/19", "item/18", "item/17"], Some("item/17"))
    );

    // Checks: by key version, by range, by database revision.
    let stale =
        json!({"checks": [{"key": "doc", "version": 0}], "writes": [{"key": "doc", "value": 1}]});
    let (status, body) = call("POST", &commit, &token, Some(stale)).await;
    assert_eq!((status, code(&body)), (409, "conflict"));
    // A conflict says which revision to read at before trying again.
    assert_eq!(body["error"]["revision"], 2);
    let current = json!({"checks": [{"key": "doc", "version": 1}, {"range": {"prefix": "item/"}, "revision": 2}], "writes": [{"key": "doc", "delete": true}]});
    assert_eq!(
        call("POST", &commit, &token, Some(current)).await.1["revision"],
        3
    );
    call(
        "POST",
        &commit,
        &token,
        Some(json!({"writes": [{"key": "item/99", "value": 0}]})),
    )
    .await;
    let ranged = json!({"checks": [{"range": {"prefix": "item/"}, "revision": 3}], "writes": [{"key": "x", "value": 0}]});
    assert_eq!(call("POST", &commit, &token, Some(ranged)).await.0, 409);
    let behind = json!({"if_revision": 3, "writes": [{"key": "x", "value": 0}]});
    assert_eq!(call("POST", &commit, &token, Some(behind)).await.0, 409);

    // Several transactions: in order, stopping at the first failure.
    let several = json!({"transactions": [
        {"writes": [{"key": "a", "value": 1}]},
        {"checks": [{"key": "a", "version": 0}], "writes": [{"key": "b", "value": 2}]},
        {"writes": [{"key": "c", "value": 3}]},
    ]});
    let (status, body) = call("POST", &commit, &token, Some(several)).await;
    assert_eq!(status, 200);
    assert_eq!(body["results"][0]["revision"], 5);
    assert_eq!(code(&body["results"][1]), "conflict");
    assert_eq!(code(&body["results"][2]), "conflict");

    // An identical retry returns the original revision.
    let once = json!({"writes": [{"key": "once", "value": 1}], "idempotency_key": "req-1"});
    assert_eq!(
        call("POST", &commit, &token, Some(once.clone())).await.1["revision"],
        6
    );
    assert_eq!(
        call("POST", &commit, &token, Some(once)).await.1["revision"],
        6
    );
    let reused = json!({"writes": [{"key": "once", "value": 2}], "idempotency_key": "req-1"});
    assert_eq!(call("POST", &commit, &token, Some(reused)).await.0, 400);

    // A session read at a revision the server has reached answers from memory.
    let (status, body) = call(
        "POST",
        &read,
        &token,
        Some(json!({"keys": ["once"], "consistency": "session", "min_revision": 6})),
    )
    .await;
    assert_eq!((status, body["revision"].as_u64()), (200, Some(6)));
    assert_eq!(
        call(
            "POST",
            &read,
            &token,
            Some(json!({"keys": ["once"], "consistency": "session", "min_revision": 999}))
        )
        .await
        .0,
        400
    );
}

#[tokio::test]
async fn single_key_endpoints() {
    let f = fixture(&[]).await;
    let token = f.create("app").await;
    let key = format!("{}/keys/users/a%20b%2Fc", f.db("app"));
    assert_eq!(call("GET", &key, &token, None).await.0, 404);
    assert_eq!(
        call("PUT", &key, &token, Some(json!({"name": "Ada"})))
            .await
            .1["revision"],
        1
    );
    let response = reqwest::Client::new()
        .get(&key)
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["x-ddb-version"], "1");
    let stored: Value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    assert_eq!(stored, json!({"name": "Ada"}));
    // The decoded key is what scans see.
    let (_, body) = call(
        "POST",
        &format!("{}/read", f.db("app")),
        &token,
        Some(json!({"scans": [{"prefix": "users/"}]})),
    )
    .await;
    assert_eq!(body["scans"][0]["items"][0]["key"], "users/a b/c");
    assert_eq!(call("DELETE", &key, &token, None).await.1["revision"], 2);
    assert_eq!(
        call("GET", &format!("{key}?consistency=session"), &token, None)
            .await
            .0,
        404
    );
}

#[tokio::test]
async fn malformed_requests_are_refused_before_anything_is_written() {
    let f = fixture(&["--max-value-bytes", "1024"]).await;
    let token = f.create("app").await;
    let commit = format!("{}/commit", f.db("app"));
    let bad = [
        json!({"writes": []}),
        json!({"writes": [{"key": "", "value": 1}]}),
        json!({"writes": [{"key": "k".repeat(1025), "value": 1}]}),
        json!({"writes": [{"key": "\u{0}internal", "value": 1}]}),
        json!({"writes": [{"key": "k", "value": null}]}),
        json!({"writes": [{"key": "k"}]}),
        json!({"writes": [{"key": "k", "value": 1, "delete": true}]}),
        json!({"writes": [{"key": "k", "value": 1}, {"key": "k", "value": 2}]}),
        json!({"writes": [{"key": "k", "value": "x".repeat(2000)}]}),
        json!({"writes": [{"key": "k", "value": 1}], "unknown": true}),
        json!({"writes": [{"key": "k", "value": 1}], "transactions": [{"writes": [{"key": "j", "value": 1}]}]}),
        json!({"writes": [{"key": "k", "value": 1}], "idempotency_key": ""}),
        json!({"transactions": [{"writes": [{"key": "j", "value": 1}]}], "idempotency_key": "top"}),
        json!({"transactions": [{"writes": [{"key": "j", "value": 1}]}, {"writes": []}]}),
    ];
    for body in bad {
        let (status, response) = call("POST", &commit, &token, Some(body.clone())).await;
        assert_eq!((status, code(&response)), (400, "invalid"), "{body}");
    }
    let read = format!("{}/read", f.db("app"));
    for body in [
        json!({"scans": [{"limit": 0}]}),
        json!({"scans": [{"limit": 10001}]}),
        json!({"keys": [""]}),
        json!("nope"),
    ] {
        assert_eq!(
            call("POST", &read, &token, Some(body.clone())).await.0,
            400,
            "{body}"
        );
    }
    // An oversized body gets the documented error shape, not a bare status.
    let huge = format!(
        "{{\"writes\":[{{\"key\":\"k\",\"value\":\"{}\"}}]}}",
        "x".repeat(33 << 20)
    );
    let response = reqwest::Client::new()
        .post(&commit)
        .bearer_auth(&token)
        .body(huge)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 413);
    let body: Value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    assert_eq!(code(&body), "too_large");
    assert_eq!(
        call("GET", &f.db("app"), &token, None).await.1["revision"],
        0
    );
    assert_eq!(
        call("POST", &format!("{}/nonsense", f.db("app")), &token, None)
            .await
            .0,
        400
    );
}

#[tokio::test]
async fn quotas_and_request_budgets_are_enforced() {
    let f = fixture(&[]).await;
    let limits = json!({"max_bytes": 4000, "max_requests_per_second": 5});
    assert_eq!(
        call("PUT", &f.db("small"), &f.admin, Some(limits)).await.0,
        201
    );
    let commit = format!("{}/commit", f.db("small"));
    assert_eq!(
        call(
            "POST",
            &commit,
            &f.admin,
            Some(json!({"writes": [{"key": "a", "value": "x".repeat(3000)}]}))
        )
        .await
        .0,
        200
    );
    let (status, body) = call(
        "POST",
        &commit,
        &f.admin,
        Some(json!({"writes": [{"key": "b", "value": "x".repeat(3000)}]})),
    )
    .await;
    assert_eq!((status, code(&body)), (507, "quota_exceeded"));
    assert_eq!(
        call(
            "POST",
            &commit,
            &f.admin,
            Some(json!({"writes": [{"key": "a", "delete": true}]}))
        )
        .await
        .0,
        200
    );

    // A database token is held to the database's request budget.
    let (_, body) = call(
        "POST",
        &format!("{}/tokens", f.db("small")),
        &f.admin,
        Some(json!({"access": "read"})),
    )
    .await;
    let token = body["token"].as_str().unwrap().to_owned();
    let mut limited = 0;
    for _ in 0..30 {
        let response = reqwest::Client::new()
            .post(format!("{}/read", f.db("small")))
            .bearer_auth(&token)
            .body(r#"{"keys":["a"],"consistency":"session"}"#)
            .send()
            .await
            .unwrap();
        if response.status() == 429 {
            assert_eq!(response.headers()["retry-after"], "1");
            limited += 1;
        }
    }
    assert!(limited >= 20, "{limited} of 30 requests were limited");
}

#[tokio::test]
async fn acknowledged_writes_survive_a_killed_server() {
    let data = tempfile::tempdir().unwrap();
    let port = free_port();
    let mut server = Server::start(data.path(), port, None, &[]).await;
    let admin = admin_token(data.path());
    let db = format!("{}/v1/databases/app", server.url);
    call("PUT", &db, &admin, None).await;
    for i in 0..40 {
        let (status, _) = call(
            "POST",
            &format!("{db}/commit"),
            &admin,
            Some(json!({"writes": [{"key": format!("k{i:02}"), "value": i}]})),
        )
        .await;
        assert_eq!(status, 200);
    }
    server.kill();
    let server = Server::start(data.path(), port, None, &[]).await;
    let db = format!("{}/v1/databases/app", server.url);
    let (_, body) = call(
        "POST",
        &format!("{db}/read"),
        &admin,
        Some(json!({"scans": [{"limit": 100}]})),
    )
    .await;
    assert_eq!(
        (
            body["revision"].as_u64(),
            body["scans"][0]["items"].as_array().unwrap().len()
        ),
        (Some(40), 40)
    );
    assert_eq!(
        call(
            "POST",
            &format!("{db}/commit"),
            &admin,
            Some(json!({"writes": [{"key": "after", "value": 1}]}))
        )
        .await
        .1["revision"],
        41
    );
}

#[tokio::test]
async fn metrics_and_health_reveal_no_tenant_data() {
    let f = fixture(&[]).await;
    let token = f.create("secret-tenant").await;
    call(
        "PUT",
        &format!("{}/keys/secret-key", f.db("secret-tenant")),
        &token,
        Some(json!("secret-value")),
    )
    .await;
    let metrics = reqwest::get(format!("{}/metrics", f.server.url))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("durable_db_request_seconds_count{kind=\"commit\"} 1"));
    assert!(metrics.contains("durable_db_open_databases 1"));
    assert!(!metrics.contains("secret"));
    assert_eq!(
        reqwest::get(format!("{}/healthz", f.server.url))
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        reqwest::get(format!("{}/readyz", f.server.url))
            .await
            .unwrap()
            .status(),
        200
    );
}
