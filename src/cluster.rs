//! Which node should serve a database, and how to hand a request to it.
//!
//! Membership is a performance hint and nothing more. Every node may claim
//! any database at any time; the claim protocol keeps that safe. Routing
//! only makes it rare, so a database is normally served from one node's
//! memory instead of being taken back and forth.
use axum::{
    body::Bytes,
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

pub const FORWARDED: HeaderName = HeaderName::from_static("x-ddb-forwarded");

pub struct Cluster {
    /// This node's `host:port` as peers reach it; `None` when it has no peers.
    me: Option<String>,
    peers: RwLock<Vec<String>>,
    /// Peers that refused a connection recently.
    suspects: RwLock<HashMap<String, Instant>>,
    client: reqwest::Client,
}

fn score(db: &str, member: &str) -> u64 {
    let digest = Sha256::new()
        .chain_update(db)
        .chain_update([0])
        .chain_update(member)
        .finalize();
    u64::from_be_bytes(digest[..8].try_into().expect("eight bytes"))
}

impl Cluster {
    pub fn new(me: Option<String>) -> Self {
        Self {
            me,
            peers: RwLock::default(),
            suspects: RwLock::default(),
            client: reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(60))
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(60))
                .build()
                .expect("HTTP client builds"),
        }
    }

    pub fn enabled(&self) -> bool {
        self.me.is_some()
    }

    pub fn set_peers(&self, mut peers: Vec<String>) {
        peers.sort();
        peers.dedup();
        if *self.peers.read() != peers {
            tracing::info!(members = peers.len(), "cluster membership changed");
            *self.peers.write() = peers;
        }
    }

    /// The peer that should serve `db`, or `None` when this node should:
    /// rendezvous hashing over the reachable members.
    pub fn owner(&self, db: &str) -> Option<String> {
        let me = self.me.as_ref()?;
        let now = Instant::now();
        let suspects = self.suspects.read();
        let peers = self.peers.read();
        let best = peers
            .iter()
            .filter(|peer| *peer != me && suspects.get(*peer).is_none_or(|until| *until <= now))
            .max_by_key(|peer| score(db, peer))?;
        (score(db, best) > score(db, me)).then(|| best.clone())
    }

    fn suspect(&self, peer: &str) {
        self.suspects
            .write()
            .insert(peer.to_owned(), Instant::now() + Duration::from_secs(3));
    }

    /// Relay a request to `peer`. `None` means the peer could not be reached
    /// and the caller should serve the request itself.
    pub async fn forward(
        &self,
        peer: &str,
        method: Method,
        path_and_query: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Option<Response> {
        let mut request = self
            .client
            .request(method, format!("http://{peer}{path_and_query}"))
            .header(FORWARDED, "1")
            .body(body);
        for name in [header::AUTHORIZATION, header::CONTENT_TYPE] {
            if let Some(value) = headers.get(&name) {
                request = request.header(name, value);
            }
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) if error.is_connect() || error.is_timeout() => {
                tracing::debug!(peer, %error, "peer unreachable; serving locally");
                self.suspect(peer);
                return None;
            }
            // The request may have reached the peer: its outcome is unknown.
            Err(error) => {
                tracing::debug!(peer, %error, "forwarded request failed");
                return Some(crate::http::error_response(&crate::Error::Lost(
                    "the owning node did not answer".into(),
                )));
            }
        };
        let status =
            StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        let mut relayed = HeaderMap::new();
        for (name, value) in response.headers() {
            let name = name.as_str();
            if (name == "content-type" || name == "retry-after" || name.starts_with("x-ddb-"))
                && let (Ok(name), Ok(value)) = (
                    HeaderName::from_bytes(name.as_bytes()),
                    HeaderValue::from_bytes(value.as_bytes()),
                )
            {
                relayed.insert(name, value);
            }
        }
        match response.bytes().await {
            Ok(body) => Some((status, relayed, body).into_response()),
            Err(_) => Some(crate::http::error_response(&crate::Error::Lost(
                "the owning node did not finish its answer".into(),
            ))),
        }
    }

    /// Resolve a DNS name (a Kubernetes headless Service) to the current members.
    pub async fn resolve(&self, name: &str, port: u16) {
        match tokio::net::lookup_host((name, port)).await {
            Ok(addresses) => self.set_peers(addresses.map(|a| a.to_string()).collect()),
            Err(error) => {
                tracing::debug!(name, %error, "peer discovery failed; keeping the last membership")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_member_agrees_on_one_owner() {
        let members: Vec<String> = (1..=5).map(|i| format!("10.0.0.{i}:7654")).collect();
        let nodes: Vec<Cluster> = members
            .iter()
            .map(|me| {
                let cluster = Cluster::new(Some(me.clone()));
                cluster.set_peers(members.clone());
                cluster
            })
            .collect();
        let mut owned = vec![0; members.len()];
        for i in 0..1000 {
            let db = format!("db-{i}");
            let owners: Vec<usize> = nodes
                .iter()
                .enumerate()
                .map(|(index, node)| match node.owner(&db) {
                    None => index,
                    Some(peer) => members.iter().position(|m| *m == peer).unwrap(),
                })
                .collect();
            assert!(
                owners.iter().all(|owner| *owner == owners[0]),
                "{db}: {owners:?}"
            );
            owned[owners[0]] += 1;
        }
        assert!(owned.iter().all(|count| *count > 120), "{owned:?}");
        // Removing a member moves only that member's databases.
        let survivors: Vec<String> = members[..4].to_vec();
        nodes[0].set_peers(survivors);
        let moved = (0..1000)
            .filter(|i| {
                let db = format!("db-{i}");
                let before = nodes[1].owner(&db).unwrap_or(members[1].clone());
                let after = nodes[0].owner(&db).unwrap_or(members[0].clone());
                before != after && before != members[4]
            })
            .count();
        assert_eq!(moved, 0);
    }

    #[test]
    fn a_single_node_serves_everything() {
        assert_eq!(Cluster::new(None).owner("db"), None);
        let alone = Cluster::new(Some("10.0.0.1:7654".into()));
        alone.set_peers(vec!["10.0.0.1:7654".into()]);
        assert_eq!(alone.owner("db"), None);
    }
}
