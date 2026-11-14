//! What one caller can do to the others: the memory, connections and
//! routing that a request uses before and after it is served. Servers run
//! in this process, so that their time limits can be short and what they
//! hold can be read directly.
mod common;
use common::call;
use durable_db::{
    auth::{Access, Grant, Keys},
    cluster::Cluster,
    db::{Ctx, Options},
    http::{ApiLimits, App},
    io::Io,
    metrics::{Metered, Metrics},
    node::{Limits, Node},
    store::{ObjectStore, memory::MemoryStore},
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

struct Server {
    address: String,
    url: String,
    app: Arc<App>,
    keys: Keys,
    admin: String,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    done: Option<tokio::task::JoinHandle<()>>,
}

fn limits() -> ApiLimits {
    ApiLimits {
        body_timeout: Duration::from_millis(400),
        stall_timeout: Duration::from_millis(400),
        head_timeout: Duration::from_secs(5),
        drain_timeout: Duration::from_secs(2),
        ..ApiLimits::default()
    }
}

impl Server {
    async fn start(limits: ApiLimits) -> Self {
        Self::cluster(1, limits).await.remove(0)
    }

    /// `size` servers on one bucket, each knowing the others.
    async fn cluster(size: usize, limits: ApiLimits) -> Vec<Self> {
        let bucket: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let keys = Keys::from_secret(&[7; 32]);
        let mut listeners = Vec::new();
        for _ in 0..size {
            listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let addresses: Vec<String> = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().to_string())
            .collect();
        let mut servers = Vec::new();
        for (listener, address) in listeners.into_iter().zip(&addresses) {
            let metrics = Arc::new(Metrics::default());
            let store: Arc<dyn ObjectStore> = Arc::new(Metered {
                inner: bucket.clone(),
                metrics: metrics.clone(),
            });
            let ctx = Arc::new(Ctx {
                io: Io::new(store, 32 << 20, None),
                opts: Options::default(),
                root: String::new(),
            });
            let cluster = Cluster::new((size > 1).then(|| address.clone()));
            cluster.set_peers(addresses.clone());
            let app = App::new(
                Node::new(ctx, Limits::default(), metrics),
                keys.clone(),
                Arc::new(cluster),
                limits.clone(),
            );
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let serving = durable_db::server::serve(listener, app.clone(), async {
                let _ = stopped.await;
            });
            let done = tokio::spawn(async { serving.await.unwrap() });
            servers.push(Self {
                url: format!("http://{address}"),
                address: address.clone(),
                admin: keys.mint(&Grant::admin()),
                keys: keys.clone(),
                app,
                stop: Some(stop),
                done: Some(done),
            });
        }
        servers
    }

    fn db(&self, name: &str) -> String {
        format!("{}/v1/databases/{name}", self.url)
    }

    fn token(&self, name: &str, access: Access) -> String {
        self.keys.mint(&Grant {
            db: name.to_owned(),
            access,
            exp: 0,
            generation: 0,
        })
    }

    async fn create(&self, name: &str) -> String {
        let (status, body) = call("PUT", &self.db(name), &self.admin, None).await;
        assert_eq!(status, 201, "{body}");
        self.token(name, Access::Write)
    }

    async fn connect(&self) -> TcpStream {
        TcpStream::connect(&self.address).await.unwrap()
    }

    /// One request written as given, and its answer.
    async fn raw(&self, request: &str) -> (u16, Value) {
        let mut stream = self.connect().await;
        stream.write_all(request.as_bytes()).await.unwrap();
        answer(&mut stream).await.expect("the server answers")
    }

    fn rejected(&self) -> u64 {
        self.app.node.metrics.rejected.load(Ordering::Relaxed)
    }
}

/// The next response on `stream`: its status and body. `None` when the
/// server closed the connection instead.
async fn answer(stream: &mut TcpStream) -> Option<(u16, Value)> {
    let mut seen = Vec::new();
    let head = loop {
        if let Some(at) = seen.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
        let mut chunk = [0u8; 4096];
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => seen.extend_from_slice(&chunk[..n]),
        }
    };
    let text = String::from_utf8_lossy(&seen[..head]).to_ascii_lowercase();
    let status = text.split(' ').nth(1)?.parse().ok()?;
    let length: usize = text
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .map_or(0, |value| value.trim().parse().unwrap());
    while seen.len() < head + length {
        let mut chunk = vec![0u8; 65536];
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => seen.extend_from_slice(&chunk[..n]),
        }
    }
    let body = String::from_utf8_lossy(&seen[head..head + length]).into_owned();
    Some((
        status,
        serde_json::from_str(&body).unwrap_or(Value::String(body)),
    ))
}

fn code(body: &Value) -> &str {
    body["error"]["code"].as_str().unwrap_or_default()
}

fn message(body: &Value) -> &str {
    body["error"]["message"].as_str().unwrap_or_default()
}

/// Wait until `holds`, or fail with `what`.
async fn eventually(what: &str, holds: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !holds() {
        assert!(Instant::now() < deadline, "never happened: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn a_caller_without_a_token_holds_no_memory() {
    let server = Server::start(ApiLimits {
        max_body: 64 << 10,
        max_body_total: 128 << 10,
        ..limits()
    })
    .await;
    let token = server.create("app").await;
    let stranger = server.token("other", Access::Write);
    // Each announces a body as large as one database may hold, and sends
    // none of it. Neither an unsigned caller nor one with a token for
    // another database gets anything reserved.
    let mut stalled = Vec::new();
    for authorization in ["", "Bearer nonsense", &format!("Bearer {stranger}")] {
        for _ in 0..8 {
            let mut stream = server.connect().await;
            let head = format!(
                "POST /v1/databases/app/commit HTTP/1.1\r\nhost: x\r\nauthorization: {authorization}\r\ncontent-length: 65536\r\n\r\n{{"
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            stalled.push(stream);
        }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(server.app.bodies.in_use(), (0, 0));
    // The database's own callers are unaffected.
    let write = json!({"writes": [{"key": "k", "value": "v".repeat(40_000)}]});
    let (status, body) = call(
        "POST",
        &format!("{}/commit", server.db("app")),
        &token,
        Some(write),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    // Each is told why, once it is clear the body is not coming.
    for (i, mut stream) in stalled.into_iter().enumerate() {
        let (status, body) = answer(&mut stream).await.expect("an answer");
        let expected = if i < 16 {
            (401, "unauthorized")
        } else {
            (403, "forbidden")
        };
        assert_eq!((status, code(&body)), expected);
    }
    assert_eq!(server.rejected(), 0);
}

#[tokio::test]
async fn a_request_without_a_body_reserves_nothing() {
    // Room for exactly one body of the largest size.
    let server = Server::start(ApiLimits {
        max_body: 1 << 20,
        max_body_total: 1 << 20,
        ..limits()
    })
    .await;
    let token = server.create("app").await;
    let url = server.db("app");
    let answers = futures::future::join_all((0..64).map(|i| {
        let (url, token) = (url.clone(), token.clone());
        async move {
            match i % 2 {
                0 => call("GET", &url, &token, None).await.0,
                _ => {
                    call("GET", &format!("{url}/keys/absent"), &token, None)
                        .await
                        .0
                }
            }
        }
    }))
    .await;
    assert!(
        answers
            .iter()
            .enumerate()
            .all(|(i, status)| *status == if i % 2 == 0 { 200 } else { 404 }),
        "{answers:?}"
    );
    assert_eq!(server.rejected(), 0);
}

#[tokio::test]
async fn a_body_that_stops_arriving_is_given_up_on() {
    let server = Server::start(limits()).await;
    let token = server.create("app").await;
    let mut stream = server.connect().await;
    let head = format!(
        "POST /v1/databases/app/commit HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {token}\r\ncontent-length: 4096\r\n\r\n{{\"writes\":["
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    eventually("the body is reserved for", || {
        server.app.bodies.in_use() == (4096, 1)
    })
    .await;
    let started = Instant::now();
    let (status, body) = answer(&mut stream).await.expect("an answer");
    assert_eq!((status, code(&body)), (408, "timeout"));
    assert_eq!(body["error"]["retryable"], true);
    assert!(started.elapsed() < Duration::from_secs(3));
    eventually("the reservation is given back", || {
        server.app.bodies.in_use() == (0, 0)
    })
    .await;
    // Nothing was written.
    let (_, info) = call("GET", &server.db("app"), &token, None).await;
    assert_eq!(info["revision"], 0);
}

#[tokio::test]
async fn one_database_cannot_hold_every_request_body() {
    // A database's part is a quarter: 256 KiB, four bodies of the largest size.
    let server = Server::start(ApiLimits {
        max_body: 64 << 10,
        max_body_total: 1 << 20,
        body_timeout: Duration::from_secs(20),
        ..limits()
    })
    .await;
    let greedy = server.create("greedy").await;
    let modest = server.create("modest").await;
    let mut stalled = Vec::new();
    for _ in 0..4 {
        let mut stream = server.connect().await;
        let head = format!(
            "POST /v1/databases/greedy/commit HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {greedy}\r\ncontent-length: 65536\r\n\r\n{{"
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stalled.push(stream);
    }
    eventually("four bodies are reserved for", || {
        server.app.bodies.in_use() == (4 << 16, 1)
    })
    .await;
    let write = |size: usize| json!({"writes": [{"key": "k", "value": "v".repeat(size)}]});
    let commit = |name: &str| format!("{}/commit", server.db(name));
    // Its own next body is refused, and it is told to come back.
    let (status, body) = call("POST", &commit("greedy"), &greedy, Some(write(100))).await;
    assert_eq!((status, code(&body)), (503, "busy"));
    assert_eq!(body["error"]["retryable"], true);
    assert_eq!(server.rejected(), 1);
    // Requests without a body still go through, and so does everyone else.
    assert_eq!(
        call("GET", &server.db("greedy"), &greedy, None).await.0,
        200
    );
    let (status, body) = call("POST", &commit("modest"), &modest, Some(write(60_000))).await;
    assert_eq!(status, 200, "{body}");
    // A body of undeclared length is reserved for as it arrives, and is
    // refused at the same point.
    let chunked = |name: &'static str, token: String, size: usize| {
        let server = &server;
        let body = write(size).to_string();
        async move {
            let mut request = format!(
                "POST /v1/databases/{name}/commit HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {token}\r\ntransfer-encoding: chunked\r\n\r\n"
            );
            for part in body.as_bytes().chunks(1000) {
                request.push_str(&format!("{:x}\r\n", part.len()));
                request.push_str(std::str::from_utf8(part).unwrap());
                request.push_str("\r\n");
            }
            request.push_str("0\r\n\r\n");
            server.raw(&request).await.0
        }
    };
    assert_eq!(chunked("modest", modest.clone(), 50_000).await, 200);
    assert_eq!(chunked("greedy", greedy.clone(), 50_000).await, 503);
    // When the stalled callers go away, what they held comes back.
    drop(stalled);
    eventually("the reservations are given back", || {
        server.app.bodies.in_use() == (0, 0)
    })
    .await;
    let (status, body) = call("POST", &commit("greedy"), &greedy, Some(write(60_000))).await;
    assert_eq!(status, 200, "{body}");
}

#[tokio::test]
async fn a_path_that_could_be_rewritten_is_refused_by_every_node() {
    let servers = Server::cluster(2, limits()).await;
    let token = servers[0].create("app").await;
    servers[0].create("other").await;
    let admin = &servers[0].admin;
    let forwarded = |server: &Server| server.app.node.metrics.forwarded.load(Ordering::Relaxed);
    let before: Vec<u64> = servers.iter().map(forwarded).collect();
    for path in [
        "/v1/databases/app/keys/../../other/keys/x",
        "/v1/databases/app/keys/%2e%2E/x",
        "/v1/databases/app/keys/a/./b",
        "/v1/databases/app/keys/a/.%2e",
        "/v1/databases/app/keys/a\\..\\b",
        "/v1/databases/app/../other",
        "/v1/databases/./app",
    ] {
        for server in &servers {
            // An administrator's token is good for both databases, so only
            // the shape of the path can be what refuses it.
            for token in [&token, admin] {
                let (status, body) = server
                    .raw(&format!(
                        "GET {path} HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {token}\r\n\r\n"
                    ))
                    .await;
                assert_eq!((status, code(&body)), (400, "invalid"), "{path}");
            }
        }
    }
    // Nothing was passed to a peer to be read differently there.
    assert_eq!(servers.iter().map(forwarded).collect::<Vec<_>>(), before);
    // Dots that are part of a longer segment are ordinary, on either node.
    for (i, server) in servers.iter().enumerate() {
        let key = format!("{}/keys/a/..b/.../c.{i}", server.db("app"));
        assert_eq!(call("PUT", &key, &token, Some(json!(1))).await.0, 200);
        assert_eq!(call("GET", &key, &token, None).await.1, json!(1));
    }
    // Such a key is reached through the body of a request instead.
    let commit = format!("{}/commit", servers[1].db("app"));
    let write = json!({"writes": [{"key": "a/../b", "value": 2}]});
    assert_eq!(call("POST", &commit, &token, Some(write)).await.0, 200);
    let read = format!("{}/read", servers[0].db("app"));
    let (_, body) = call("POST", &read, &token, Some(json!({"keys": ["a/../b"]}))).await;
    assert_eq!(body["values"][0]["value"], 2);
}

#[tokio::test]
async fn only_a_peer_can_say_that_a_request_was_relayed() {
    let servers = Server::cluster(2, limits()).await;
    let token = servers[0].create("app").await;
    let serving = |i: usize| servers[i].app.node.serving("app");
    let owner = (0..2).find(|i| serving(*i)).expect("one node serves it");
    let other = &servers[1 - owner];
    let forwarded = || other.app.node.metrics.forwarded.load(Ordering::Relaxed);
    let get = |relayed: String| {
        let request = format!(
            "GET /v1/databases/app HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {token}\r\n{relayed}\r\n"
        );
        async move { other.raw(&request).await.0 }
    };
    // A caller that claims to be a peer is not believed: the request goes
    // to the owner as any other would, and ownership stays where it is.
    let before = forwarded();
    for forged in ["1", "true", &"0".repeat(64)] {
        assert_eq!(get(format!("x-ddb-forwarded: {forged}\r\n")).await, 200);
    }
    assert_eq!(forwarded(), before + 3);
    assert!(serving(owner) && !serving(1 - owner));
    // A peer, which holds the signing key, is: the node serves it itself.
    let proof = servers[owner].keys.relay_proof();
    assert_eq!(get(format!("x-ddb-forwarded: {proof}\r\n")).await, 200);
    assert_eq!(forwarded(), before + 3);
    assert!(serving(1 - owner));
}

#[tokio::test]
async fn a_request_is_counted_while_it_is_parsed() {
    let server = Server::start(ApiLimits {
        max_operations: 100,
        max_lookups: 100,
        ..limits()
    })
    .await;
    let token = server.create("app").await;
    let commit = format!("{}/commit", server.db("app"));
    let read = format!("{}/read", server.db("app"));
    let writes = |n: usize| -> Vec<Value> {
        (0..n)
            .map(|i| json!({"key": i.to_string(), "value": i}))
            .collect()
    };
    let refused = |answer: (u16, Value), rule: &str| {
        assert_eq!(
            (answer.0, code(&answer.1)),
            (400, "invalid"),
            "{}",
            answer.1
        );
        assert!(message(&answer.1).contains(rule), "{}", answer.1);
    };
    let too_many = "a commit has at most 100 writes and checks";
    // In one transaction, across several, and counting checks.
    refused(
        call(
            "POST",
            &commit,
            &token,
            Some(json!({"writes": writes(101)})),
        )
        .await,
        too_many,
    );
    refused(
        call(
            "POST",
            &commit,
            &token,
            Some(json!({"transactions": [{"writes": writes(60)}, {"writes": writes(41)}]})),
        )
        .await,
        too_many,
    );
    let checks: Vec<Value> = (0..60)
        .map(|i| json!({"key": i.to_string(), "version": 0}))
        .collect();
    refused(
        call(
            "POST",
            &commit,
            &token,
            Some(json!({"writes": writes(41), "checks": checks})),
        )
        .await,
        too_many,
    );
    // A body of very many small items is refused at the limit, not after
    // it has been read into as many values.
    let flood = format!("{{\"transactions\":[{}{{}}]}}", "{},".repeat(500_000));
    let response = reqwest::Client::new()
        .post(&commit)
        .bearer_auth(&token)
        .body(flood)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let body: Value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    assert!(message(&body).contains(too_many), "{body}");
    let keys: Vec<String> = (0..101).map(|i| i.to_string()).collect();
    refused(
        call("POST", &read, &token, Some(json!({"keys": keys}))).await,
        "a read has at most 100 keys and scans",
    );
    refused(
        call(
            "POST",
            &read,
            &token,
            Some(json!({"keys": keys[..60], "scans": vec![json!({}); 41]})),
        )
        .await,
        "a read has at most 100 keys and scans",
    );
    // A check is one of two shapes, and nothing else is read into memory
    // on its behalf.
    for check in [
        json!({"key": "a"}),
        json!({"key": "a", "version": 0, "revision": 0}),
        json!({"range": {}, "version": 0}),
        json!({}),
    ] {
        refused(
            call(
                "POST",
                &commit,
                &token,
                Some(json!({"writes": writes(1), "checks": [check]})),
            )
            .await,
            "a check has",
        );
    }
    refused(
        call(
            "POST",
            &commit,
            &token,
            Some(json!({"writes": writes(1), "checks": [{"key": "a", "version": 0, "extra": [[[]]]}]})),
        )
        .await,
        "unknown field",
    );
    // At the limit, everything is accepted, and nothing above was written.
    let (status, body) = call(
        "POST",
        &commit,
        &token,
        Some(json!({"transactions": [
            {"writes": writes(60), "checks": [{"key": "0", "version": 0}]},
            {"writes": writes(39)},
        ]})),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["results"][1]["revision"], 2);
    let (status, body) = call(
        "POST",
        &read,
        &token,
        Some(json!({"keys": keys[..60], "scans": vec![json!({"limit": 1}); 40]})),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

#[tokio::test]
async fn a_read_is_refused_when_its_result_passes_the_limit_not_after() {
    let server = Server::start(ApiLimits {
        max_read_bytes: 256 << 10,
        ..limits()
    })
    .await;
    let token = server.create("app").await;
    let commit = format!("{}/commit", server.db("app"));
    let read = format!("{}/read", server.db("app"));
    let value = "v".repeat(8 << 10);
    let writes: Vec<Value> = (0..100)
        .map(|i| json!({"key": format!("k{i:03}"), "value": value}))
        .collect();
    assert_eq!(
        call("POST", &commit, &token, Some(json!({"writes": writes})))
            .await
            .0,
        200
    );
    let keys = |n: usize| -> Vec<String> { (0..n).map(|i| format!("k{i:03}")).collect() };
    let too_large = |answer: (u16, Value)| {
        assert_eq!((answer.0, code(&answer.1)), (400, "invalid"));
        assert!(
            message(&answer.1).contains("larger than the 262144 bytes one read returns"),
            "{}",
            answer.1
        );
    };
    // Both from the writes still in memory and, below, from a table.
    for flushed in [false, true] {
        if flushed {
            let db = server.app.node.db("app").await.unwrap();
            db.request_flush();
            eventually("the writes reach a table", || db.info().tables > 0).await;
        }
        too_large(call("POST", &read, &token, Some(json!({"keys": keys(100)}))).await);
        too_large(
            call(
                "POST",
                &read,
                &token,
                Some(json!({"scans": [{"limit": 1000}]})),
            )
            .await,
        );
        // Many scans that each stay below the limit, together above it.
        too_large(
            call(
                "POST",
                &read,
                &token,
                Some(json!({"scans": vec![json!({"limit": 10}); 10]})),
            )
            .await,
        );
        let (status, body) = call("POST", &read, &token, Some(json!({"keys": keys(20)}))).await;
        assert_eq!(status, 200);
        assert_eq!(body["values"].as_array().unwrap().len(), 20);
        let (status, body) = call(
            "POST",
            &read,
            &token,
            Some(json!({"scans": [{"limit": 25}]})),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["scans"][0]["items"].as_array().unwrap().len(), 25);
        assert_eq!(body["scans"][0]["next"], "k024");
    }
    // What the refused and the answered reads reserved has been given back.
    eventually("the reservations are given back", || {
        server.app.results.in_use() == (0, 0)
    })
    .await;
}

#[tokio::test]
async fn an_answer_nobody_reads_is_not_kept() {
    let server = Server::start(ApiLimits {
        max_read_total: 80 << 20,
        stall_timeout: Duration::from_secs(1),
        ..limits()
    })
    .await;
    let token = server.create("app").await;
    let commit = format!("{}/commit", server.db("app"));
    let value = "v".repeat(1 << 20);
    for i in 0..24 {
        let write = json!({"writes": [{"key": format!("k{i:02}"), "value": value}]});
        assert_eq!(call("POST", &commit, &token, Some(write)).await.0, 200);
    }
    let request = |body: String| {
        format!(
            "POST /v1/databases/app/read HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {token}\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
    };
    // A scan answers with a part at a time; keys asked for by name all come.
    let keys: Vec<String> = (0..24).map(|i| format!("k{i:02}")).collect();
    let everything = request(json!({"keys": keys}).to_string());
    let held = || server.app.results.in_use().0;
    // Callers that ask for 24 MiB and never take it. Each answer is counted
    // twice while it is put together, and once while it waits to be sent.
    let mut idle = Vec::new();
    for waiting in 1..=2 {
        let mut stream = server.connect().await;
        stream.write_all(everything.as_bytes()).await.unwrap();
        idle.push(stream);
        eventually("the result waits to be sent", || {
            (waiting * (24 << 20)..waiting * (25 << 20)).contains(&held())
        })
        .await;
    }
    // A third does not fit beside them, and is told to come back. Small
    // reads carry on.
    let (status, body) = server.raw(&everything).await;
    assert_eq!((status, code(&body)), (503, "busy"), "{body}");
    assert!(message(&body).contains("too many read results"));
    assert_eq!(server.rejected(), 1);
    let (status, body) = server
        .raw(&request(json!({"keys": ["absent", "k00"]}).to_string()))
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["values"][0], Value::Null);
    // The connections are closed once nothing has been taken for a while,
    // and what they held is free again.
    let started = Instant::now();
    eventually("the results are released", || {
        server.app.results.in_use() == (0, 0)
    })
    .await;
    assert!(started.elapsed() < Duration::from_secs(4));
    for mut stream in idle {
        let mut rest = Vec::new();
        let _ = stream.read_to_end(&mut rest).await;
        assert!(rest.len() < 24 << 20, "the answer was cut short");
    }
    let (status, body) = server.raw(&everything).await;
    assert_eq!(status, 200);
    assert_eq!(body["values"].as_array().unwrap().len(), 24);
    eventually("an answer that was taken is released too", || {
        server.app.results.in_use() == (0, 0)
    })
    .await;
}

#[tokio::test]
async fn a_relayed_answer_is_counted_where_it_waits() {
    let servers = Server::cluster(2, limits()).await;
    let token = servers[0].create("app").await;
    let owner = (0..2)
        .find(|i| servers[*i].app.node.serving("app"))
        .unwrap();
    let relay = &servers[1 - owner];
    let commit = format!("{}/commit", servers[owner].db("app"));
    let value = "v".repeat(1 << 20);
    for i in 0..24 {
        let write = json!({"writes": [{"key": format!("k{i:02}"), "value": value}]});
        assert_eq!(call("POST", &commit, &token, Some(write)).await.0, 200);
    }
    // A scan answers with a part at a time; keys asked for by name all come.
    let keys: Vec<String> = (0..24).map(|i| format!("k{i:02}")).collect();
    let body = json!({"keys": keys}).to_string();
    let everything = format!(
        "POST /v1/databases/app/read HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {token}\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    );
    let mut idle = relay.connect().await;
    idle.write_all(everything.as_bytes()).await.unwrap();
    eventually("the relayed result is held by the relay", || {
        let (bytes, databases) = relay.app.results.in_use();
        databases == 1 && bytes >= 24 << 20
    })
    .await;
    assert!(!relay.app.node.serving("app"));
    eventually("and released when nobody takes it", || {
        relay.app.results.in_use() == (0, 0) && servers[owner].app.results.in_use() == (0, 0)
    })
    .await;
    // Read normally, it arrives whole.
    let (status, body) = relay.raw(&everything).await;
    assert_eq!(status, 200);
    assert_eq!(body["values"].as_array().unwrap().len(), 24);
    assert_eq!(body["values"][23]["value"].as_str().unwrap().len(), 1 << 20);
}

#[tokio::test]
async fn connections_are_limited_and_idle_ones_are_closed() {
    let server = Server::start(ApiLimits {
        max_connections: 4,
        head_timeout: Duration::from_millis(600),
        ..limits()
    })
    .await;
    let token = server.create("app").await;
    // Four callers connect and say nothing, or only begin to.
    let mut silent = Vec::new();
    for i in 0..4 {
        let mut stream = server.connect().await;
        if i % 2 == 1 {
            stream
                .write_all(b"GET /v1/databases/app HTTP/1.1\r\nhost")
                .await
                .unwrap();
        }
        silent.push(stream);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The fifth waits its turn, and gets it when the silent ones time out.
    let started = Instant::now();
    assert_eq!(call("GET", &server.db("app"), &token, None).await.0, 200);
    let waited = started.elapsed();
    assert!(
        waited > Duration::from_millis(300) && waited < Duration::from_secs(4),
        "{waited:?}"
    );
    for mut stream in silent {
        let mut rest = Vec::new();
        let closed = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut rest));
        closed.await.expect("the server closes the connection").ok();
        // One that began a request may be told that it took too long.
        assert!(rest.is_empty() || rest.starts_with(b"HTTP/1.1 408"));
    }
}

#[tokio::test]
async fn readiness_asks_storage_rarely() {
    let server = Server::start(limits()).await;
    let probes = || {
        let text = server.app.node.metrics.render(0, 0, 0, 0);
        let line = text
            .lines()
            .find(|line| line.starts_with("durable_db_storage_seconds_count{op=\"exists\"}"))
            .expect("the count is reported");
        line.rsplit(' ').next().unwrap().parse::<u64>().unwrap()
    };
    let before = probes();
    let answers = futures::future::join_all((0..200).map(|_| {
        let url = format!("{}/readyz", server.url);
        async move { reqwest::get(url).await.unwrap().status().as_u16() }
    }))
    .await;
    assert!(answers.iter().all(|status| *status == 200));
    assert_eq!(probes() - before, 1);
}

#[tokio::test]
async fn shutdown_lets_a_request_in_flight_finish() {
    let mut server = Server::start(ApiLimits {
        body_timeout: Duration::from_secs(10),
        ..limits()
    })
    .await;
    let token = server.create("app").await;
    // A request whose body is still arriving when the server is told to stop.
    let body = "{\"writes\":[{\"key\":\"k\",\"value\":1}]}";
    let (first, rest) = body.split_at(10);
    let mut stream = server.connect().await;
    let head = format!(
        "POST /v1/databases/app/commit HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {token}\r\ncontent-length: {}\r\n\r\n{first}",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    let idle = server.connect().await;
    eventually("the request is being read", || {
        server.app.bodies.in_use().0 > 0
    })
    .await;
    server.stop.take().unwrap().send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    // No new connection is accepted.
    let refused = reqwest::Client::new()
        .get(format!("{}/healthz", server.url))
        .timeout(Duration::from_millis(500))
        .send()
        .await;
    assert!(refused.is_err());
    // The one in flight is answered, and then the server is done.
    stream.write_all(rest.as_bytes()).await.unwrap();
    let (status, answer) = answer(&mut stream).await.expect("an answer");
    assert_eq!((status, &answer["revision"]), (200, &json!(1)));
    let done = server.done.take().unwrap();
    tokio::time::timeout(Duration::from_secs(3), done)
        .await
        .expect("the server stops")
        .unwrap();
    drop(idle);
}
