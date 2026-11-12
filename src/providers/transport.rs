//! Explicit HTTP policy for the S3 SDK, shared by both provider bindings.
//!
//! SDK retries and HTTP redirects/retries are separate layers. In particular,
//! a 303 must not change a conditional PUT into a successful GET. Preserve the
//! SDK's environment-configurable transport settings while disabling both
//! lower-layer replay mechanisms. SDK read retries remain separately bounded.
use anyhow::{Context, bail};
use object_store::{
    ClientConfigKey as Key, ClientOptions, RetryConfig,
    aws::{AmazonS3, AmazonS3Builder},
    client::{HttpClient, HttpConnector},
};
use rand::seq::SliceRandom;
use reqwest::{Client, NoProxy, Proxy, dns};
use std::{str::FromStr, sync::Arc, time::Duration};

#[derive(Debug)]
pub(super) struct Connector;
impl HttpConnector for Connector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        client(options)
            .map(HttpClient::new)
            .map_err(|source| object_store::Error::Generic {
                store: "durable-db HTTP configuration",
                source: source.into(),
            })
    }
}

pub(super) fn observations(builder: AmazonS3Builder) -> object_store::Result<AmazonS3> {
    builder
        .with_http_connector(Connector)
        .with_retry(RetryConfig {
            max_retries: 2,
            retry_timeout: Duration::from_secs(10),
            ..Default::default()
        })
        .build()
}

pub(super) fn single_attempt(builder: AmazonS3Builder) -> object_store::Result<AmazonS3> {
    builder
        .with_http_connector(Connector)
        .with_retry(RetryConfig {
            max_retries: 0,
            ..Default::default()
        })
        .build()
}

fn value<T: FromStr>(options: &ClientOptions, key: Key) -> anyhow::Result<Option<T>> {
    options
        .get_config_value(&key)
        .map(|v| {
            v.parse()
                .map_err(|_| anyhow::anyhow!("invalid HTTP option: {}", key.as_ref()))
        })
        .transpose()
}
fn boolean(options: &ClientOptions, key: Key) -> anyhow::Result<bool> {
    Ok(value(options, key)?.unwrap_or(false))
}
fn duration(options: &ClientOptions, key: Key) -> anyhow::Result<Option<Duration>> {
    options
        .get_config_value(&key)
        .map(|v| {
            humantime::parse_duration(&v)
                .map_err(|_| anyhow::anyhow!("invalid HTTP duration: {}", key.as_ref()))
        })
        .transpose()
}

fn client(options: &ClientOptions) -> anyhow::Result<Client> {
    // This connector is private: callers construct SDK options only from the
    // environment or defaults. SDK-only programmatic fields (custom DNS,
    // default headers and root certificates) are deliberately not exposed.
    let mut builder = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate()
        .https_only(!boolean(options, Key::AllowHttp)?)
        .danger_accept_invalid_certs(boolean(options, Key::AllowInvalidCertificates)?)
        .user_agent(
            options
                .get_config_value(&Key::UserAgent)
                .unwrap_or_else(|| concat!("durable-db/", env!("CARGO_PKG_VERSION")).into()),
        );
    // There is no public CLI/env custom-root facility to replace system roots.
    // Refuse this rather than accidentally changing the requested trust policy.
    if boolean(options, Key::NoSystemCertificates)? {
        bail!("disable_system_certificates is unsupported; use the system trust store");
    }
    if let Some(url) = options.get_config_value(&Key::ProxyUrl) {
        let mut proxy = Proxy::all(url).map_err(|_| anyhow::anyhow!("invalid HTTP proxy"))?;
        if let Some(excludes) = options.get_config_value(&Key::ProxyExcludes) {
            proxy = proxy.no_proxy(NoProxy::from_string(&excludes));
        }
        if let Some(pem) = options.get_config_value(&Key::ProxyCaCertificate) {
            let certificate = reqwest::tls::Certificate::from_pem(pem.as_bytes())
                .map_err(|_| anyhow::anyhow!("invalid proxy CA certificate"))?;
            builder = builder.tls_certs_merge([certificate]);
        }
        builder = builder.proxy(proxy);
    }
    macro_rules! duration_option {
        ($key:ident, $method:ident) => {
            if let Some(v) = duration(options, Key::$key)? {
                builder = builder.$method(v);
            }
        };
    }
    duration_option!(Timeout, timeout);
    duration_option!(ConnectTimeout, connect_timeout);
    duration_option!(ReadTimeout, read_timeout);
    duration_option!(PoolIdleTimeout, pool_idle_timeout);
    duration_option!(Http2KeepAliveInterval, http2_keep_alive_interval);
    duration_option!(Http2KeepAliveTimeout, http2_keep_alive_timeout);
    if let Some(v) = value::<usize>(options, Key::PoolMaxIdlePerHost)? {
        builder = builder.pool_max_idle_per_host(v);
    }
    if let Some(v) = value::<u32>(options, Key::Http2MaxFrameSize)? {
        builder = builder.http2_max_frame_size(v);
    }
    builder = builder.http2_keep_alive_while_idle(boolean(options, Key::Http2KeepAliveWhileIdle)?);
    if boolean(options, Key::Http1Only)? {
        builder = builder.http1_only();
    }
    if boolean(options, Key::Http2Only)? {
        builder = builder.http2_prior_knowledge();
    }
    if boolean(options, Key::RandomizeAddresses)? {
        builder = builder.dns_resolver(Arc::new(ShuffleDns));
    }
    // DefaultContentType remains handled by the SDK's request builder.
    builder.build().context("construct provider HTTP client")
}

struct ShuffleDns;
impl dns::Resolve for ShuffleDns {
    fn resolve(&self, name: dns::Name) -> dns::Resolving {
        Box::pin(async move {
            let mut addresses: Vec<_> =
                tokio::net::lookup_host((name.as_str(), 0)).await?.collect();
            addresses.shuffle(&mut rand::rng());
            Ok(Box::new(addresses.into_iter()) as dns::Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Remote, Storage};
    use axum::{body::Body, extract::State, http::Method, response::Response, routing::any};
    use bytes::Bytes;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn builder(endpoint: String) -> AmazonS3Builder {
        AmazonS3Builder::new()
            .with_bucket_name("test")
            .with_region("us-east-1")
            .with_access_key_id("fixture")
            .with_secret_access_key("fixture")
            .with_endpoint(endpoint)
            .with_allow_http(true)
    }
    struct Wire {
        status: u16,
        calls: Vec<(Method, Bytes)>,
    }
    async fn response(
        State(wire): State<Arc<Mutex<Wire>>>,
        method: Method,
        body: Bytes,
    ) -> Response {
        let mut wire = wire.lock().unwrap();
        wire.calls.push((method, body));
        Response::builder()
            .status(wire.status)
            .header("etag", "\"fixture\"")
            .header("location", "/test/prefix/blocks/fixture")
            .body(Body::from("<Error><Code>Fixture</Code></Error>"))
            .unwrap()
    }
    #[tokio::test]
    async fn immutable_upload_has_one_attempt_including_empty_objects_and_redirects() {
        let wire = Arc::new(Mutex::new(Wire {
            status: 200,
            calls: vec![],
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let builder = builder(format!("http://{}", listener.local_addr().unwrap()));
        let app = axum::Router::new()
            .route("/test/prefix/blocks/fixture", any(response))
            .with_state(wire.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let remote = Remote::new(
            Arc::new(
                builder
                    .clone()
                    .with_http_connector(Connector)
                    .build()
                    .unwrap(),
            ),
            "prefix",
        )
        .unwrap()
        .with_uploads(Arc::new(single_attempt(builder).unwrap()));
        for bytes in [Bytes::new(), Bytes::from_static(b"fixture payload")] {
            for status in [200, 301, 302, 303, 307, 308, 429, 500, 503] {
                {
                    let mut state = wire.lock().unwrap();
                    state.calls.clear();
                    state.status = status;
                }
                let result = remote.put("blocks/fixture", bytes.clone()).await;
                assert_eq!(result.is_ok(), status == 200, "status {status}");
                assert_eq!(wire.lock().unwrap().calls, [(Method::PUT, bytes.clone())]);
            }
        }
        server.abort();
        server.await.unwrap_err();
    }

    #[tokio::test]
    async fn observations_bound_transient_retries_without_following_redirects() {
        let wire = Arc::new(Mutex::new(Wire {
            status: 503,
            calls: vec![],
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = Remote::new(
            Arc::new(
                observations(builder(format!(
                    "http://{}",
                    listener.local_addr().unwrap()
                )))
                .unwrap(),
            ),
            "prefix",
        )
        .unwrap();
        let app = axum::Router::new()
            .route("/test/prefix/blocks/fixture", any(response))
            .with_state(wire.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        for (status, attempts) in [(503, 3), (403, 1), (303, 1)] {
            {
                let mut state = wire.lock().unwrap();
                state.status = status;
                state.calls.clear();
            }
            assert!(remote.get("blocks/fixture", 1).await.is_err());
            assert_eq!(
                wire.lock().unwrap().calls,
                vec![(Method::GET, Bytes::new()); attempts]
            );
        }
        server.abort();
        server.await.unwrap_err();
    }

    #[tokio::test]
    async fn fully_received_upload_with_lost_response_is_not_replayed() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upload = single_attempt(builder(format!(
            "http://{}",
            listener.local_addr().unwrap()
        )))
        .unwrap();
        let wire = Arc::new(Mutex::new(Vec::new()));
        let received = wire.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut header = vec![];
                while !header.ends_with(b"\r\n\r\n") {
                    header.push(stream.read_u8().await.unwrap());
                    assert!(header.len() < 16384);
                }
                let text = std::str::from_utf8(&header).unwrap().to_ascii_lowercase();
                let length: usize = text
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                let mut body = vec![0; length];
                stream.read_exact(&mut body).await.unwrap();
                received.lock().unwrap().push(body);
                // Provider accepted the complete body, then the response vanished.
                stream.shutdown().await.unwrap();
            }
        });
        use object_store::ObjectStoreExt;
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            upload.put(&"fixture".into(), Bytes::from_static(b"delayed").into()),
        )
        .await
        .expect("upload should resolve without SDK retry backoff");
        assert!(result.is_err());
        assert_eq!(*wire.lock().unwrap(), [b"delayed".to_vec()]);
        server.abort();
        server.await.unwrap_err();
    }

    #[tokio::test]
    async fn stalled_observation_bodies_release_reads_ranges_and_listings() {
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let seen = entered.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let store = builder(format!("http://{}", listener.local_addr().unwrap()))
            .with_http_connector(Connector)
            .build()
            .unwrap();
        let remote = Arc::new(Remote::new(Arc::new(store), "prefix").unwrap());
        let app = axum::Router::new().fallback(move |headers: axum::http::HeaderMap| {
            seen.add_permits(1);
            async move {
                Response::builder()
                    .status(if headers.contains_key("range") {
                        206
                    } else {
                        200
                    })
                    .header("content-length", "1")
                    .header("content-range", "bytes 0-0/1")
                    .header("last-modified", "Wed, 11 Nov 2026 00:00:00 GMT")
                    .header("etag", "\"fixture\"")
                    .body(Body::from_stream(futures::stream::pending::<
                        Result<Bytes, std::io::Error>,
                    >()))
                    .unwrap()
            }
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut requests = Vec::new();
        for operation in 0..3 {
            let remote = remote.clone();
            requests.push(tokio::spawn(async move {
                match operation {
                    0 => remote.get("blocks/fixture", 1).await.map(|_| ()),
                    1 => remote
                        .get_range("blocks/fixture", 0..1, 1)
                        .await
                        .map(|_| ()),
                    _ => remote.list("blocks/", None, 1).await.map(|_| ()),
                }
            }));
        }
        tokio::time::timeout(Duration::from_secs(2), entered.acquire_many(3))
            .await
            .unwrap()
            .unwrap()
            .forget();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(10)).await;
        for request in requests {
            let result = tokio::time::timeout(Duration::from_secs(1), request)
                .await
                .expect("stalled observation must release its caller")
                .unwrap();
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("observation deadline")
            );
        }
        server.abort();
        server.await.unwrap_err();
    }

    #[test]
    fn transport_configuration_errors_do_not_echo_values() {
        let options = ClientOptions::new().with_config(Key::Timeout, "secret-sentinel");
        let error = client(&options).unwrap_err().to_string();
        assert!(error.contains("timeout"));
        assert!(!error.contains("secret-sentinel"));
        let options = ClientOptions::new().with_config(Key::NoSystemCertificates, "true");
        assert!(
            client(&options)
                .unwrap_err()
                .to_string()
                .contains("unsupported")
        );
    }
}
