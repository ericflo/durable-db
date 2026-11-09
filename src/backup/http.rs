use anyhow::{Context, ensure};
use reqwest::{
    Client, Method, StatusCode, Url,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    path::Path,
    time::{Duration, Instant},
};

#[derive(Debug, thiserror::Error)]
#[error("database HTTP request failed with status {0}")]
pub struct HttpStatus(pub StatusCode);
pub struct Remote {
    client: Client,
    endpoint: Url,
}
impl Remote {
    pub fn new(endpoint: &str, token_file: &Path) -> anyhow::Result<Self> {
        let endpoint = Url::parse(endpoint).map_err(|_| anyhow::anyhow!("invalid database URL"))?;
        let loopback = endpoint.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    // URL host strings retain brackets around IPv6 literals.
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        ensure!(
            (endpoint.scheme() == "https" || (endpoint.scheme() == "http" && loopback))
                && endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none()
                && endpoint.path() == "/",
            "use an HTTPS origin, or HTTP loopback, without embedded credentials or a path"
        );
        let mut file = std::fs::File::open(token_file)?;
        use std::io::Read;
        let mut bytes = Vec::new();
        (&mut file).take(2049).read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 2048, "credential file exceeds bound");
        let token = std::str::from_utf8(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid credential file"))?
            .trim();
        ensure!(!token.is_empty(), "empty credential file");
        let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| anyhow::anyhow!("invalid credential file"))?;
        value.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, value);
        let client = Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self { client, endpoint })
    }
    pub async fn call<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&impl Serialize>,
    ) -> anyhow::Result<T> {
        let url = self.endpoint.join(path).context("invalid request path")?;
        let body = body.map(serde_json::to_vec).transpose()?;
        ensure!(
            body.as_ref().is_none_or(|b| b.len() <= 1024 * 1024),
            "backup request exceeds bound"
        );
        let started = Instant::now();
        let mut attempts = 0;
        loop {
            let mut request = self.client.request(method.clone(), url.clone());
            if let Some(body) = &body {
                request = request
                    .header("content-type", "application/json")
                    .body(body.clone());
            }
            let response = request.send().await;
            if let Ok(mut response) = response {
                let status = response.status();
                if status.is_success() {
                    let mut bytes = Vec::new();
                    while let Some(chunk) = response.chunk().await.map_err(|_| {
                        anyhow::anyhow!(
                            "database response interrupted; repeat the identical command"
                        )
                    })? {
                        ensure!(
                            bytes.len() + chunk.len() <= 1024 * 1024,
                            "database response exceeds bound"
                        );
                        bytes.extend_from_slice(&chunk);
                    }
                    return serde_json::from_slice(&bytes).map_err(|_| {
                        anyhow::anyhow!("invalid database response; repeat the identical command")
                    });
                }
                // A global collector can fence a prepared restore chunk while
                // leaving its data revision unchanged. Retrying the identical
                // ID/revision/body is safe; a conflicting writer cannot be overwritten.
                let restore_conflict = status == StatusCode::CONFLICT
                    && method == Method::POST
                    && path.ends_with("/commit");
                if !restore_conflict && !matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504) {
                    return Err(HttpStatus(status).into());
                }
            }
            ensure!(
                started.elapsed() < Duration::from_secs(180),
                "database unavailable; a mutation may have committed; repeat the identical command"
            );
            attempts += 1;
            tokio::time::sleep(Duration::from_millis((attempts * 100).min(1000))).await;
        }
    }
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        self.call(Method::GET, path, None::<&()>).await
    }
}

#[cfg(test)]
mod tests {
    use super::Remote;

    #[test]
    fn credentials_require_https_or_an_exact_loopback_origin() {
        let directory = tempfile::tempdir().unwrap();
        let token = directory.path().join("token");
        std::fs::write(&token, "synthetic-capability").unwrap();
        for origin in [
            "http://127.0.0.1:7654",
            "http://[::1]:7654",
            "http://localhost:7654",
            "https://database.example.com",
        ] {
            assert!(Remote::new(origin, &token).is_ok(), "{origin}");
        }
        for origin in [
            "http://192.0.2.1",
            "http://[2001:db8::1]",
            "http://localhost.example.com",
            "https://user:password@database.example.com",
            "https://database.example.com/prefix",
            "https://database.example.com?credential=example",
            "https://database.example.com#fragment",
        ] {
            assert!(Remote::new(origin, &token).is_err(), "{origin}");
        }
    }
}
