use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Response},
    routing::get,
};
use bytes::Bytes;
use durable_db::storage::{Local, MAX_RANGE_BYTES, Remote, Storage};
use object_store::{RetryConfig, aws::AmazonS3Builder};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

async fn ranged(
    State(calls): State<Arc<AtomicUsize>>,
    Path(mode): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    calls.fetch_add(1, Ordering::SeqCst);
    assert_eq!(headers.get("range").unwrap(), "bytes=128-191");
    let data: Vec<_> = (0..4096).map(|n| (n % 251) as u8).collect();
    let mut response = Response::builder()
        .status(206)
        .header("etag", "\"immutable\"")
        .header("last-modified", "Mon, 09 Nov 2026 00:00:00 GMT");
    let body = match mode.as_str() {
        "good" => {
            response = response.header("content-range", "bytes 128-191/4096");
            data[128..192].to_vec()
        }
        "ignored" => {
            response = response.status(200);
            data
        }
        "wrong-offset" => {
            response = response.header("content-range", "bytes 64-127/4096");
            data[64..128].to_vec()
        }
        "wrong-size" => {
            response = response.header("content-range", "bytes 128-191/4097");
            data[128..192].to_vec()
        }
        "short" => {
            response = response.header("content-range", "bytes 128-191/4096");
            data[128..191].to_vec()
        }
        "long" => {
            response = response.header("content-range", "bytes 128-191/4096");
            data[128..193].to_vec()
        }
        "missing" => {
            response = response.status(404);
            b"<Error><Code>NoSuchKey</Code></Error>".to_vec()
        }
        _ => panic!("unexpected fixture mode"),
    };
    response.body(Body::from(body)).unwrap()
}

#[tokio::test]
async fn remote_ranges_require_exact_metadata_and_bounded_complete_bodies() {
    let calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new()
        .route("/test/fixture/{mode}", get(ranged))
        .with_state(calls.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let remote = Remote::new(
        Arc::new(
            AmazonS3Builder::new()
                .with_bucket_name("test")
                .with_region("us-east-1")
                .with_access_key_id("fixture")
                .with_secret_access_key("fixture")
                .with_endpoint(endpoint)
                .with_allow_http(true)
                .with_retry(RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                })
                .build()
                .unwrap(),
        ),
        "fixture",
    )
    .unwrap();
    let result = remote.get_range("good", 128..192, 4096).await.unwrap();
    assert_eq!(
        result.as_ref(),
        (128..192).map(|n| (n % 251) as u8).collect::<Vec<_>>()
    );
    for mode in [
        "ignored",
        "wrong-offset",
        "wrong-size",
        "short",
        "long",
        "missing",
    ] {
        assert!(
            tokio::time::timeout(
                Duration::from_secs(5),
                remote.get_range(mode, 128..192, 4096)
            )
            .await
            .unwrap()
            .is_err(),
            "{mode}"
        );
    }
    let before = calls.load(Ordering::SeqCst);
    for (range, size) in [
        (0..0, 4096),
        (
            std::ops::Range {
                start: 192,
                end: 128,
            },
            4096,
        ),
        (0..4097, 4096),
        (0..MAX_RANGE_BYTES as u64 + 1, MAX_RANGE_BYTES as u64 + 1),
    ] {
        assert!(remote.get_range("good", range, size).await.is_err());
    }
    assert_eq!(before, calls.load(Ordering::SeqCst));
    task.abort();
}

#[tokio::test]
async fn local_ranges_seek_without_loosening_identity_size_or_path_checks() {
    let dir = tempfile::tempdir().unwrap();
    let local = Local::open(dir.path().into()).unwrap();
    let data = Bytes::from((0..4096).map(|n| (n % 251) as u8).collect::<Vec<_>>());
    local.put("journal/immutable", data.clone()).await.unwrap();
    for range in [0..1, 128..192, 4095..4096, 0..4096] {
        assert_eq!(
            local
                .get_range("journal/immutable", range.clone(), 4096)
                .await
                .unwrap(),
            data.slice(range.start as usize..range.end as usize)
        );
    }
    assert!(
        local
            .get_range("journal/immutable", 128..192, 4097)
            .await
            .is_err()
    );
    assert!(
        local
            .get_range("journal/missing", 128..192, 4096)
            .await
            .is_err()
    );
    assert!(local.get_range("journal/../LOCK", 0..1, 1).await.is_err());
    assert!(
        local
            .get_range("journal/immutable", 0..0, 4096)
            .await
            .is_err()
    );
    assert!(
        local
            .get_range(
                "journal/immutable",
                0..MAX_RANGE_BYTES as u64 + 1,
                MAX_RANGE_BYTES as u64 + 1
            )
            .await
            .is_err()
    );
}
