//! Physical process/VM qualification, not a production clock adapter.
mod clock;
use super::{fixture_connection, serve_fixture};
use crate::{
    cache::Cache,
    engine::{CommitRequest, ReadRequest},
    ownership::root::{Outcome, Profile, Root},
    providers::Connection,
    transactions::{
        live::{Limits, Opening, Owner},
        record::Database,
    },
};
use clock::BootClock;
use rustix::process::{Pid, Signal, getpid, kill_process};
use std::{io::Write, sync::Arc, time::Duration};
use tokio::{
    process::Command,
    time::{sleep, timeout},
};
use uuid::Uuid;

const DATABASE: Database<'static> = Database {
    name: "app",
    generation: Uuid::from_u128(42),
};
const CHILD: &str = "providers::s3::tests::timing::physical_owner_child";
const LIFETIME: Duration = Duration::from_secs(1);
const ERROR: Duration = Duration::from_millis(1);

fn write(revision: u64, id: &str) -> CommitRequest {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    serde_json::from_value(serde_json::json!({
        "expected_revision": revision, "request_id": id,
        "writes": [{"key":"note", "value":STANDARD.encode(id)}]
    }))
    .unwrap()
}
async fn activate(connection: &Connection, clock: BootClock) -> Root<BootClock> {
    let profile = Profile::new(LIFETIME, ERROR).unwrap();
    let mut root = connection.root(clock, profile).unwrap();
    let observed = root.observe().await.unwrap();
    let request = root.claim(observed).unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    // Wait longer than the normalized conservative drain, then renew before
    // activation. No sleep result or scheduler tick is itself permission.
    sleep(LIFETIME * 2 + ERROR * 3).await;
    let observed = root.observe().await.unwrap();
    let request = root.validate(observed).unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    let request = root.activate().unwrap();
    assert!(matches!(
        root.finish(request.execute().await).unwrap(),
        Outcome::Confirmed
    ));
    root
}
async fn open(root: Root<BootClock>) -> Owner<BootClock> {
    Opening::new(
        root,
        Limits::default(),
        Arc::new(Cache::new(1024 * 1024, None).unwrap()),
    )
    .unwrap()
    .recover()
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "subprocess of physical_owner_pause; never run independently"]
async fn physical_owner_child() {
    let endpoint = std::env::var("DURABLE_DB_PAUSE_ENDPOINT").unwrap();
    let report = std::env::var("DURABLE_DB_PAUSE_REPORT").unwrap();
    let freeze = std::env::var("DURABLE_DB_PAUSE_FREEZE").unwrap() == "1";
    let guest = std::env::var_os("DURABLE_DB_PAUSE_GUEST").is_some();
    let clock = if guest && !freeze {
        BootClock::guarded()
    } else {
        BootClock::default()
    };
    let connection = fixture_connection(&endpoint);
    let mut owner = open(activate(&connection, clock.clone()).await).await;
    let initial = owner
        .prepare_request(DATABASE, &write(0, "first"))
        .await
        .unwrap();
    owner
        .complete(initial.upload.unwrap().send().await)
        .await
        .unwrap();
    assert_eq!(
        owner
            .commit_result(initial.tickets[0])
            .unwrap()
            .unwrap()
            .revision,
        1
    );
    let late = owner
        .prepare_request(DATABASE, &write(1, "late"))
        .await
        .unwrap();
    if guest {
        println!("\nOWNER_READY");
        std::io::stdout().flush().unwrap();
    } else {
        if freeze {
            clock.freeze();
        }
        kill_process(getpid(), Signal::STOP).unwrap();
    }
    // Group-stop delivery is asynchronous across threads. The pipe prevents
    // the test thread from sending its upload before every thread has stopped.
    let mut resume = String::new();
    std::io::stdin().read_line(&mut resume).unwrap();
    assert_eq!(resume, "resume\n");
    // The parent has replaced this owner, sealed its prefix at one and committed
    // revision two. This old upload may still arrive but can never earn a reply.
    let completion = late.upload.unwrap().send().await;
    let applied = owner.complete(completion).await.is_ok();
    let acknowledged = owner
        .commit_result(late.tickets[0])
        .is_ok_and(|r| r.is_some());
    let served = owner
        .read_request(DATABASE, &ReadRequest::default())
        .await
        .is_ok();
    let result = serde_json::to_string(&(applied, acknowledged, served)).unwrap();
    if guest {
        println!("OWNER_RESULT {result}");
    } else {
        std::fs::write(report, result).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn physical_owner_pause_fences_reads_and_late_acknowledgements() {
    let vm = std::env::var_os("DURABLE_DB_PAUSE_VM_ROOT").is_some();
    for freeze in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let report = directory.path().join("result.json");
        let (endpoint, _state, server) = serve_fixture(200, true).await;
        let mut command = if vm {
            let mut command = Command::new("python3");
            command.args(["scripts/check-vm-clock.py", "--owner-child"]);
            command
        } else {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command.args(["--exact", CHILD, "--ignored", "--nocapture"]);
            command
        };
        let mut child = command
            .env("DURABLE_DB_PAUSE_ENDPOINT", &endpoint)
            .env("DURABLE_DB_PAUSE_REPORT", &report)
            .env("DURABLE_DB_PAUSE_FREEZE", if freeze { "1" } else { "0" })
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = Pid::from_raw(child.id().unwrap() as i32).unwrap();
        timeout(Duration::from_secs(30), async {
            loop {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "child exited before stopping"
                );
                let stopped = if vm {
                    report.with_extension("ready").exists()
                } else {
                    let status =
                        std::fs::read_to_string(format!("/proc/{}/status", pid.as_raw_nonzero()))
                            .unwrap();
                    status.lines().any(|l| l.starts_with("State:\tT"))
                };
                if stopped {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("owner did not stop");
        let connection = fixture_connection(&endpoint);
        let mut root = activate(&connection, BootClock::default()).await;
        let mut recovery = root.recovery().unwrap();
        let mut journal = recovery.next().await.unwrap().unwrap();
        while journal.read_next().await.unwrap().is_some() {}
        assert_eq!(journal.finished_prefix().unwrap().position(), 1);
        let request = root.select_prefix(journal).unwrap();
        assert!(matches!(
            root.finish(request.execute().await).unwrap(),
            Outcome::Confirmed
        ));
        let mut owner = open(root).await;
        let replacement = write(1, "replacement");
        let prepared = owner.prepare_request(DATABASE, &replacement).await.unwrap();
        owner
            .complete(prepared.upload.unwrap().send().await)
            .await
            .unwrap();
        assert_eq!(
            owner
                .commit_result(prepared.tickets[0])
                .unwrap()
                .unwrap()
                .revision,
            2
        );
        if !vm {
            kill_process(pid, Signal::CONT).unwrap();
        }
        use tokio::io::AsyncWriteExt;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"resume\n")
            .await
            .unwrap();
        assert!(
            timeout(Duration::from_secs(10), child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        let result: (bool, bool, bool) =
            serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
        assert_eq!(
            result,
            (freeze, freeze, freeze),
            "frozen or unguarded restored clocks must demonstrate the safety violation"
        );
        // The retained successor receipt and view are unchanged by the late PUT.
        let retry = owner.prepare_request(DATABASE, &replacement).await.unwrap();
        assert!(
            owner
                .commit_result(retry.tickets[0])
                .unwrap()
                .unwrap()
                .replayed
        );
        let read = owner
            .read_request(
                DATABASE,
                &ReadRequest {
                    keys: vec!["note".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(read.revision, 2);
        assert_eq!(read.values, vec![Some("cmVwbGFjZW1lbnQ=".into())]);
        server.abort();
        server.await.unwrap_err();
    }
}
