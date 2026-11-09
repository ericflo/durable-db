//! Bounded retries for observations only; publication outcomes never pass here.
use anyhow::bail;
use reqwest::{Response, StatusCode};
use serde_json::Value;
use std::{future::Future, time::Duration};
use tokio::time::{Instant, timeout_at};

pub(super) struct Reply {
    pub status: StatusCode,
    pub body: Value,
    retry_after: Option<Duration>,
}
impl Reply {
    pub async fn read(response: Response) -> anyhow::Result<Self> {
        // B2 documents Retry-After as a number of seconds. Never shorten a
        // provider-requested wait to fit our budget: return the error instead.
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs);
        let (status, body) = super::json_response(response).await?;
        Ok(Self {
            status,
            body,
            retry_after,
        })
    }
}

pub(super) async fn read<F, Fut>(mut request: F) -> anyhow::Result<Reply>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<Reply>>,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    for attempt in 0..3 {
        let result = match timeout_at(deadline, request()).await {
            Ok(result) => result,
            Err(_) => bail!("B2 observation retry deadline exceeded"),
        };
        let (retry, retry_after) = match &result {
            Ok(reply) => (
                matches!(reply.status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504),
                reply.retry_after,
            ),
            Err(error) => (error.is::<reqwest::Error>(), None),
        };
        if !retry || attempt == 2 {
            return result;
        }
        let wait = retry_after.unwrap_or_else(|| {
            Duration::from_millis((1000 << attempt) + rand::random::<u64>() % 251)
        });
        if wait >= deadline.saturating_duration_since(Instant::now()) {
            return result;
        }
        tokio::time::sleep(wait).await;
    }
    unreachable!("three bounded attempts return their final result")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn response(status: u16, retry: &str) -> Response {
        axum::http::Response::builder()
            .status(status)
            .header("Retry-After", retry)
            .body(bytes::Bytes::from_static(b"gateway response without JSON"))
            .unwrap()
            .into()
    }
    #[tokio::test]
    async fn retry_after_and_permanent_errors_are_respected() {
        let calls = AtomicUsize::new(0);
        let started = Instant::now();
        let result = read(|| async {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            Reply::read(if call == 0 {
                response(503, "1")
            } else {
                response(403, "0")
            })
            .await
        })
        .await
        .unwrap();
        assert_eq!(result.status, StatusCode::FORBIDDEN);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(started.elapsed() >= Duration::from_secs(1));
        let calls = AtomicUsize::new(0);
        let result = read(|| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Reply::read(response(429, "60")).await
        })
        .await
        .unwrap();
        assert_eq!(result.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "never shorten Retry-After to fit the local budget"
        );
    }
}
