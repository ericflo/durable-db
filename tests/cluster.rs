//! Several server processes on one store: routing, takeover and handover.
//! The store is a shared directory; the protocol is the same as on a bucket.
mod common;
use common::{Server, admin_token, call, free_port};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

struct Cluster {
    data: tempfile::TempDir,
    ports: Vec<u16>,
    peers: String,
    servers: Vec<Option<Server>>,
    admin: String,
}

impl Cluster {
    async fn start(size: usize) -> Self {
        let data = tempfile::tempdir().unwrap();
        let ports: Vec<u16> = (0..size).map(|_| free_port()).collect();
        let peers = ports
            .iter()
            .map(|port| format!("127.0.0.1:{port}"))
            .collect::<Vec<_>>()
            .join(",");
        // The first server creates the signing key the others load.
        let mut servers = vec![Some(
            Server::start(data.path(), ports[0], Some(&peers), &[]).await,
        )];
        for port in &ports[1..] {
            servers.push(Some(
                Server::start(data.path(), *port, Some(&peers), &[]).await,
            ));
        }
        let admin = admin_token(data.path());
        Self {
            data,
            ports,
            peers,
            servers,
            admin,
        }
    }

    fn url(&self, node: usize) -> String {
        format!("http://127.0.0.1:{}", self.ports[node])
    }

    fn db(&self, node: usize, name: &str) -> String {
        format!("{}/v1/databases/{name}", self.url(node))
    }

    async fn metric(&self, node: usize, name: &str) -> u64 {
        let text = reqwest::get(format!("{}/metrics", self.url(node)))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let line = text
            .lines()
            .find(|line| line.starts_with(name))
            .unwrap_or_default();
        line.rsplit(' ')
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }

    /// The node that has a database open, when exactly one does.
    async fn owner(&self) -> Option<usize> {
        let mut owners = Vec::new();
        for node in 0..self.servers.len() {
            if self.servers[node].is_some()
                && self.metric(node, "durable_db_open_databases").await == 1
            {
                owners.push(node);
            }
        }
        (owners.len() == 1).then(|| owners[0])
    }

    async fn restart(&mut self, node: usize) {
        self.servers[node] =
            Some(Server::start(self.data.path(), self.ports[node], Some(&self.peers), &[]).await);
    }
}

/// Files of one kind left under a database: `c` claims, `l` frames, and so on.
fn objects(data: &Path, name: &str, kind: &str) -> usize {
    fn count(dir: &Path) -> usize {
        std::fs::read_dir(dir).map_or(0, |entries| {
            entries
                .flatten()
                .map(|e| {
                    if e.path().is_dir() {
                        count(&e.path())
                    } else {
                        1
                    }
                })
                .sum()
        })
    }
    count(&data.join("db").join(name).join(kind))
}

/// Commit through `url`, repeating the identical request while the outcome is unknown.
async fn commit(url: &str, token: &str, body: &Value) -> (u16, Value) {
    for _ in 0..200 {
        match reqwest::Client::new()
            .post(format!("{url}/commit"))
            .bearer_auth(token)
            .body(body.to_string())
            .send()
            .await
        {
            Ok(response) if response.status() != 503 => {
                let status = response.status().as_u16();
                return (
                    status,
                    serde_json::from_str(&response.text().await.unwrap()).unwrap(),
                );
            }
            _ => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    panic!("the commit never reached a definite answer");
}

#[tokio::test]
async fn any_node_answers_and_one_node_owns() {
    let cluster = Cluster::start(3).await;
    assert_eq!(
        call("PUT", &cluster.db(0, "app"), &cluster.admin, None)
            .await
            .0,
        201
    );
    // Give every node a chance to learn the membership.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    for i in 0..30 {
        let node = i % 3;
        let body = json!({"writes": [{"key": format!("k{i:02}"), "value": i}], "idempotency_key": format!("w{i}")});
        let (status, answer) = commit(&cluster.db(node, "app"), &cluster.admin, &body).await;
        assert_eq!(
            (status, answer["revision"].as_u64()),
            (200, Some(i as u64 + 1)),
            "write {i} via node {node}"
        );
    }
    for node in 0..3 {
        let (status, body) = call(
            "POST",
            &format!("{}/read", cluster.db(node, "app")),
            &cluster.admin,
            Some(json!({"scans": [{}]})),
        )
        .await;
        assert_eq!(
            (
                status,
                body["revision"].as_u64(),
                body["scans"][0]["items"].as_array().unwrap().len()
            ),
            (200, Some(30), 30)
        );
    }
    // Requests were relayed rather than ownership being taken back and forth.
    assert!(
        cluster.owner().await.is_some(),
        "exactly one node holds the database"
    );
    let mut forwarded = 0;
    for node in 0..3 {
        forwarded += cluster
            .metric(node, "durable_db_forwarded_requests_total")
            .await;
    }
    assert!(forwarded >= 20, "{forwarded} requests were forwarded");
}

#[tokio::test]
async fn a_killed_owner_is_replaced_and_nothing_acknowledged_is_lost() {
    let mut cluster = Cluster::start(3).await;
    call("PUT", &cluster.db(0, "app"), &cluster.admin, None).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    for i in 0..20 {
        let body = json!({"writes": [{"key": format!("before/{i:02}"), "value": i}], "idempotency_key": format!("b{i}")});
        assert_eq!(
            commit(&cluster.db(i % 3, "app"), &cluster.admin, &body)
                .await
                .0,
            200
        );
    }
    let owner = cluster.owner().await.expect("one owner");
    cluster.servers[owner].take().unwrap().kill();
    let survivors: Vec<usize> = (0..3).filter(|node| *node != owner).collect();
    for i in 0..20 {
        let node = survivors[i % 2];
        let body = json!({"writes": [{"key": format!("after/{i:02}"), "value": i}], "idempotency_key": format!("a{i}")});
        let (status, answer) = commit(&cluster.db(node, "app"), &cluster.admin, &body).await;
        assert_eq!(
            (status, answer["revision"].as_u64()),
            (200, Some(21 + i as u64))
        );
    }
    // The killed node returns with nothing but the shared store.
    cluster.restart(owner).await;
    let (_, body) = call(
        "POST",
        &format!("{}/read", cluster.db(owner, "app")),
        &cluster.admin,
        Some(json!({"scans": [{}]})),
    )
    .await;
    assert_eq!(
        (
            body["revision"].as_u64(),
            body["scans"][0]["items"].as_array().unwrap().len()
        ),
        (Some(40), 40)
    );
}

#[tokio::test]
async fn a_terminated_node_hands_its_databases_back_sealed() {
    let mut cluster = Cluster::start(2).await;
    call("PUT", &cluster.db(0, "app"), &cluster.admin, None).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    for i in 0..10 {
        let body = json!({"writes": [{"key": format!("k{i}"), "value": i}], "idempotency_key": format!("w{i}")});
        commit(&cluster.db(0, "app"), &cluster.admin, &body).await;
    }
    let owner = cluster.owner().await.expect("one owner");
    assert_eq!(objects(cluster.data.path(), "app", "c"), 1);
    assert!(objects(cluster.data.path(), "app", "l") > 0);
    cluster.servers[owner].take().unwrap().terminate();
    // Released: no claim, no log, one manifest, and the data in a table.
    assert_eq!(objects(cluster.data.path(), "app", "c"), 0);
    assert_eq!(objects(cluster.data.path(), "app", "l"), 0);
    assert_eq!(objects(cluster.data.path(), "app", "m"), 1);
    assert_eq!(objects(cluster.data.path(), "app", "t"), 1);
    let other = 1 - owner;
    let (_, body) = call(
        "POST",
        &format!("{}/read", cluster.db(other, "app")),
        &cluster.admin,
        Some(json!({"scans": [{}]})),
    )
    .await;
    assert_eq!(
        (
            body["revision"].as_u64(),
            body["scans"][0]["items"].as_array().unwrap().len()
        ),
        (Some(10), 10)
    );
}

#[tokio::test]
async fn a_joining_node_receives_its_share() {
    let data = tempfile::tempdir().unwrap();
    let ports = [free_port(), free_port()];
    let peers = format!("127.0.0.1:{},127.0.0.1:{}", ports[0], ports[1]);
    // One node alone serves every database, whatever the member list says.
    let first = Server::start(data.path(), ports[0], Some(&peers), &[]).await;
    let admin = admin_token(data.path());
    let names: Vec<String> = (0..12).map(|i| format!("db{i}")).collect();
    for name in &names {
        let url = format!("{}/v1/databases/{name}", first.url);
        assert_eq!(call("PUT", &url, &admin, None).await.0, 201);
        assert_eq!(
            call("PUT", &format!("{url}/keys/k"), &admin, Some(json!(name)))
                .await
                .0,
            200
        );
    }
    let open = |url: String| async move {
        let text = reqwest::get(format!("{url}/metrics"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let line = text
            .lines()
            .find(|l| l.starts_with("durable_db_open_databases"))
            .unwrap()
            .to_owned();
        line.rsplit(' ').next().unwrap().parse::<usize>().unwrap()
    };
    assert_eq!(open(first.url.clone()).await, 12);
    // A second node joins. The first hands over the databases that hash to it.
    let second = Server::start(data.path(), ports[1], Some(&peers), &[]).await;
    let mut moved = 0;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        moved = 12 - open(first.url.clone()).await;
        if moved >= 3 {
            break;
        }
    }
    assert!((3..=9).contains(&moved), "{moved} of 12 databases moved");
    // Every database still answers through either node, with its data.
    for (i, name) in names.iter().enumerate() {
        let node = if i % 2 == 0 { &first } else { &second };
        let (status, value) = call(
            "GET",
            &format!("{}/v1/databases/{name}/keys/k", node.url),
            &admin,
            None,
        )
        .await;
        assert_eq!((status, value), (200, json!(name)));
    }
    assert_eq!(
        open(first.url.clone()).await + open(second.url.clone()).await,
        12
    );
}

#[tokio::test]
async fn concurrent_clients_across_nodes_never_fork_a_counter() {
    let cluster = Arc::new(tokio::sync::Mutex::new(Cluster::start(3).await));
    let (admin, urls): (String, Vec<String>) = {
        let cluster = cluster.lock().await;
        call("PUT", &cluster.db(0, "app"), &cluster.admin, None).await;
        (
            cluster.admin.clone(),
            (0..3).map(|node| cluster.db(node, "app")).collect(),
        )
    };
    let acknowledged = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut clients = Vec::new();
    for client in 0..6 {
        let (admin, urls, acknowledged, stop) = (
            admin.clone(),
            urls.clone(),
            acknowledged.clone(),
            stop.clone(),
        );
        clients.push(tokio::spawn(async move {
            let mut attempt = 0u64;
            while !stop.load(Ordering::SeqCst) {
                attempt += 1;
                let url = &urls[(client + attempt as usize) % 3];
                let read = reqwest::Client::new()
                    .post(format!("{url}/read"))
                    .bearer_auth(&admin)
                    .body(r#"{"keys":["counter"]}"#)
                    .send()
                    .await;
                let Ok(read) = read else { continue };
                if read.status() != 200 {
                    continue;
                }
                let seen: Value = serde_json::from_str(&read.text().await.unwrap()).unwrap();
                let (value, version) = match &seen["values"][0] {
                    Value::Null => (0, 0),
                    entry => (
                        entry["value"].as_u64().unwrap(),
                        entry["version"].as_u64().unwrap(),
                    ),
                };
                let body = json!({
                    "checks": [{"key": "counter", "version": version}],
                    "writes": [{"key": "counter", "value": value + 1}],
                    "idempotency_key": format!("c{client}-{attempt}"),
                });
                if commit(url, &admin, &body).await.0 == 200 {
                    acknowledged.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    // Kill and restart a node while the clients run.
    tokio::time::sleep(Duration::from_millis(800)).await;
    for victim in [1usize, 2] {
        cluster.lock().await.servers[victim].take().unwrap().kill();
        tokio::time::sleep(Duration::from_millis(500)).await;
        cluster.lock().await.restart(victim).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    stop.store(true, Ordering::SeqCst);
    for client in clients {
        client.await.unwrap();
    }
    let cluster = cluster.lock().await;
    let (_, body) = call(
        "GET",
        &format!("{}/keys/counter", cluster.db(0, "app")),
        &cluster.admin,
        None,
    )
    .await;
    let acknowledged = acknowledged.load(Ordering::SeqCst);
    assert!(
        acknowledged > 20,
        "only {acknowledged} increments were acknowledged"
    );
    assert_eq!(body.as_u64(), Some(acknowledged));
}
