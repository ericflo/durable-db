use axum::{
    body::Body,
    extract::{Request, State},
    middleware::{self, Next},
    response::Response,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use durable_db::{
    api::{App, router},
    auth::Auth,
    backup,
    cache::Cache,
    engine::{CommitRequest, CreateRequest, Engine, Limits, ReadRequest, Write},
    storage::Remote,
};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::task::JoinHandle;

#[derive(Default)]
struct Faults {
    requests: AtomicUsize,
    commits: AtomicUsize,
    break_commit: AtomicUsize,
    unavailable_commit: AtomicUsize,
    conflict_invoke: AtomicUsize,
    change_read: AtomicUsize,
    collect_read: AtomicUsize,
    mutate_archive: Mutex<Option<PathBuf>>,
}
#[derive(Clone)]
struct Hooks {
    faults: Arc<Faults>,
    engine: Arc<Engine>,
}
async fn inject(State(hooks): State<Hooks>, request: Request, next: Next) -> Response {
    hooks.faults.requests.fetch_add(1, Ordering::SeqCst);
    let path = request.uri().path().to_owned();
    let put = request.method() == axum::http::Method::PUT;
    if path.ends_with("/commit") && hooks.faults.conflict_invoke.swap(0, Ordering::SeqCst) == 1 {
        return Response::builder().status(409).body(Body::empty()).unwrap();
    }
    let response = next.run(request).await;
    if put && let Some(file) = hooks.faults.mutate_archive.lock().unwrap().take() {
        std::fs::write(file, b"changed after verification").unwrap();
    }
    if path.ends_with("/commit") && response.status().is_success() {
        let count = hooks.faults.commits.fetch_add(1, Ordering::SeqCst) + 1;
        if hooks.faults.unavailable_commit.load(Ordering::SeqCst) == count {
            return Response::builder().status(503).body(Body::empty()).unwrap();
        }
        if hooks.faults.break_commit.load(Ordering::SeqCst) == count {
            return Response::new(Body::from("interrupted JSON"));
        }
    }
    if path == "/v1/databases/source/read" && response.status().is_success() {
        if hooks.faults.change_read.swap(0, Ordering::SeqCst) == 1 {
            let revision = hooks.engine.info("source").await.unwrap().revision;
            commit(
                &hooks.engine,
                "source",
                revision,
                vec![Write {
                    key: "00000".into(),
                    value: Some(STANDARD.encode(b"changed")),
                }],
            )
            .await;
        }
        if hooks.faults.collect_read.swap(0, Ordering::SeqCst) == 1 {
            hooks.engine.gc("source").await.unwrap();
        }
    }
    response
}
struct Fixture {
    temp: tempfile::TempDir,
    engine: Arc<Engine>,
    faults: Arc<Faults>,
    url: String,
    token: PathBuf,
    task: JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            Remote::new(Arc::new(object_store::memory::InMemory::new()), "backup").unwrap(),
        );
        let engine = Arc::new(Engine::new(storage, Arc::new(Cache::new(0, None).unwrap())));
        let auth = Auth::new(&"ab".repeat(32)).unwrap();
        let token = temp.path().join("token");
        std::fs::write(&token, auth.administrator_token(3600).unwrap()).unwrap();
        let faults = Arc::new(Faults::default());
        let app = router(Arc::new(App::new(engine.clone(), auth, 64))).layer(
            middleware::from_fn_with_state(
                Hooks {
                    faults: faults.clone(),
                    engine: engine.clone(),
                },
                inject,
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            temp,
            engine,
            faults,
            url,
            token,
            task,
        }
    }
    async fn seed(&self, count: usize, large: bool) {
        self.engine
            .create_protected(
                "source",
                &CreateRequest {
                    request_id: "seed".into(),
                    limits: Limits {
                        max_bytes: 16 * 1024 * 1024,
                        max_keys: 100000,
                    },
                },
            )
            .await
            .unwrap();
        for (revision, start) in (0..count).step_by(if large { 4 } else { 128 }).enumerate() {
            let size = if large { 4 } else { 128 };
            let writes = (start..(start + size).min(count))
                .map(|i| Write {
                    key: format!("{i:05}"),
                    value: Some(STANDARD.encode(if large {
                        vec![i as u8; 60 * 1024]
                    } else {
                        format!("value-{i}").into_bytes()
                    })),
                })
                .collect();
            commit(&self.engine, "source", revision as u64, writes).await;
        }
    }
    async fn export(&self) -> PathBuf {
        let path = self.temp.path().join("archive.jsonl");
        backup::export(&self.url, &self.token, "source", &path)
            .await
            .unwrap();
        path
    }
}
async fn commit(engine: &Engine, db: &str, revision: u64, writes: Vec<Write>) {
    engine
        .commit(
            db,
            &CommitRequest {
                expected_revision: revision,
                request_id: uuid::Uuid::new_v4().to_string(),
                checks: vec![],
                writes,
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn interrupted_restore_resumes_after_verifying_prefix_and_survives_receipt_eviction() {
    let f = Fixture::new().await;
    f.seed(16641, false).await;
    let archive = f.export().await;
    let inspected = backup::inspect(&archive).unwrap();
    assert_eq!(inspected.restore_chunks, 131);
    f.faults.break_commit.store(3, Ordering::SeqCst);
    assert!(
        backup::restore(&f.url, &f.token, "restored", &archive)
            .await
            .is_err()
    );
    assert_eq!(f.engine.info("restored").await.unwrap().revision, 3);
    let restored = backup::restore(&f.url, &f.token, "restored", &archive)
        .await
        .unwrap();
    assert_eq!(restored.verified_keys, 16641);
    assert_eq!(restored.destination.revision, 131);
    let calls = f.faults.commits.load(Ordering::SeqCst);
    let repeated = backup::restore(&f.url, &f.token, "restored", &archive)
        .await
        .unwrap();
    assert_eq!(repeated.destination.policy, restored.destination.policy);
    assert_eq!(f.faults.commits.load(Ordering::SeqCst), calls);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&archive).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    assert!(
        backup::export(&f.url, &f.token, "source", &archive)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn adaptive_export_preserves_large_values_and_archive_spool_is_immutable() {
    let f = Fixture::new().await;
    f.seed(20, true).await;
    f.faults.collect_read.store(1, Ordering::SeqCst);
    let archive = f.export().await;
    let digest = backup::inspect(&archive).unwrap().sha256;
    *f.faults.mutate_archive.lock().unwrap() = Some(archive.clone());
    let result = backup::restore(&f.url, &f.token, "large", &archive)
        .await
        .unwrap();
    assert_eq!(result.archive_sha256, digest);
    assert_eq!(result.verified_keys, 20);
    assert!(backup::inspect(&archive).is_err());
    let result = f
        .engine
        .read(
            "large",
            &ReadRequest {
                keys: vec!["00019".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        STANDARD.decode(result.values[0].as_ref().unwrap()).unwrap(),
        vec![19; 60 * 1024]
    );
}

#[tokio::test]
async fn source_changes_abort_export_without_publishing_a_partial_file() {
    let f = Fixture::new().await;
    f.seed(200, false).await;
    f.faults.change_read.store(1, Ordering::SeqCst);
    let archive = f.temp.path().join("failed.jsonl");
    assert!(
        backup::export(&f.url, &f.token, "source", &archive)
            .await
            .is_err()
    );
    assert!(!archive.exists());
    assert_eq!(std::fs::read_dir(f.temp.path()).unwrap().count(), 1); // token only; temporary removed
}

#[tokio::test]
async fn corrupt_archives_never_contact_server_and_foreign_destinations_are_preserved() {
    let f = Fixture::new().await;
    f.seed(2, false).await;
    let archive = f.export().await;
    let original = std::fs::read_to_string(&archive).unwrap();
    let lines: Vec<_> = original.lines().collect();
    let cases = vec![
        String::new(),
        lines[..lines.len() - 1].join("\n") + "\n",
        original.replace("value", "wrong"),
        original.replace("dmFsdWUtMA==", "c3BvaWxlZA=="),
        original.clone() + "{}\n",
        original.replace("\"keys\":2", "\"keys\":3"),
        "x".repeat(100 * 1024 + 1),
    ];
    let bad = f.temp.path().join("bad.jsonl");
    for contents in cases {
        std::fs::write(&bad, contents).unwrap();
        let calls = f.faults.requests.load(Ordering::SeqCst);
        assert!(
            backup::restore(&f.url, &f.token, "invalid", &bad)
                .await
                .is_err()
        );
        assert_eq!(calls, f.faults.requests.load(Ordering::SeqCst));
    }
    let oversized = std::fs::File::create(&bad).unwrap();
    oversized.set_len(1024 * 1024 * 1024 + 1).unwrap();
    let calls = f.faults.requests.load(Ordering::SeqCst);
    assert!(
        backup::restore(&f.url, &f.token, "invalid", &bad)
            .await
            .is_err()
    );
    assert_eq!(calls, f.faults.requests.load(Ordering::SeqCst));
    std::fs::remove_file(&bad).unwrap();
    f.engine
        .create_protected(
            "foreign",
            &CreateRequest {
                request_id: "another-owner".into(),
                limits: Limits::default(),
            },
        )
        .await
        .unwrap();
    commit(
        &f.engine,
        "foreign",
        0,
        vec![Write {
            key: "retained".into(),
            value: Some(STANDARD.encode(b"untouched")),
        }],
    )
    .await;
    assert!(
        backup::restore(&f.url, &f.token, "foreign", &archive)
            .await
            .is_err()
    );
    let read = f
        .engine
        .read(
            "foreign",
            &ReadRequest {
                keys: vec!["retained".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(read.revision, 1);
    assert_eq!(read.values, vec![Some(STANDARD.encode(b"untouched"))]);
}

#[tokio::test]
async fn divergent_partial_destination_stops_before_any_new_commit() {
    let f = Fixture::new().await;
    f.seed(400, false).await;
    let archive = f.export().await;
    f.faults.break_commit.store(1, Ordering::SeqCst);
    assert!(
        backup::restore(&f.url, &f.token, "partial", &archive)
            .await
            .is_err()
    );
    commit(
        &f.engine,
        "partial",
        1,
        vec![Write {
            key: "00000".into(),
            value: Some(STANDARD.encode(b"diverged")),
        }],
    )
    .await;
    let before = f.faults.commits.load(Ordering::SeqCst);
    assert!(
        backup::restore(&f.url, &f.token, "partial", &archive)
            .await
            .is_err()
    );
    assert_eq!(before, f.faults.commits.load(Ordering::SeqCst));
    assert_eq!(f.engine.info("partial").await.unwrap().revision, 2);
}

#[tokio::test]
async fn empty_archive_restores_and_cli_inspection_checks_integrity() {
    let f = Fixture::new().await;
    f.seed(0, false).await;
    let archive = f.export().await;
    let restored = backup::restore(&f.url, &f.token, "empty", &archive)
        .await
        .unwrap();
    assert_eq!(restored.destination.revision, 0);
    assert_eq!(restored.verified_keys, 0);
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_durable-db"))
        .args(["backup", "inspect", "--input"])
        .arg(&archive)
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["restore_chunks"], 0);
}

#[tokio::test]
async fn committed_service_error_retries_identical_chunk_and_cli_preserves_arbitrary_bytes() {
    let f = Fixture::new().await;
    f.seed(0, false).await;
    let keys = ["\u{1}\"\\", "a", "é", "日本語", "🦀"];
    let writes = keys
        .iter()
        .enumerate()
        .map(|(i, k)| Write {
            key: (*k).into(),
            value: Some(STANDARD.encode(if i == 0 {
                vec![]
            } else {
                (0..=255).collect::<Vec<u8>>()
            })),
        })
        .collect();
    commit(&f.engine, "source", 0, writes).await;
    let archive = f.temp.path().join("cli.jsonl");
    let binary = env!("CARGO_BIN_EXE_durable-db");
    let output = tokio::process::Command::new(binary)
        .args(["backup", "export", "--url", &f.url, "--token-file"])
        .arg(&f.token)
        .args(["--database", "source", "--output"])
        .arg(&archive)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    f.faults.unavailable_commit.store(1, Ordering::SeqCst);
    f.faults.conflict_invoke.store(1, Ordering::SeqCst);
    let output = tokio::process::Command::new(binary)
        .args(["backup", "restore", "--url", &f.url, "--token-file"])
        .arg(&f.token)
        .args(["--database", "cli-restored", "--input"])
        .arg(&archive)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result = f
        .engine
        .read(
            "cli-restored",
            &ReadRequest {
                keys: keys.iter().map(|k| (*k).into()).collect(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.revision, 1);
    assert_eq!(f.faults.commits.load(Ordering::SeqCst), 2);
    assert_eq!(result.values[0], Some(String::new()));
    assert_eq!(
        STANDARD.decode(result.values[4].as_ref().unwrap()).unwrap(),
        (0..=255).collect::<Vec<u8>>()
    );
}

#[tokio::test]
async fn backup_client_refuses_redirects_and_remote_plaintext_origins() {
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new().fallback(move || {
        let count = observed.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Response::builder()
                .status(302)
                .header("location", "/leaked")
                .body(Body::empty())
                .unwrap()
        }
    });
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let temp = tempfile::tempdir().unwrap();
    let token = temp.path().join("token");
    std::fs::write(&token, "synthetic-capability").unwrap();
    let output = temp.path().join("backup.jsonl");
    assert!(
        backup::export(&url, &token, "source", &output)
            .await
            .is_err()
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(
        backup::export("http://database.example.com", &token, "source", &output)
            .await
            .is_err()
    );
    assert!(!output.exists());
    task.abort();
}
