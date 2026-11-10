//! Physical process-pause qualification, not a production clock adapter.
//! SIGSTOP exercises descheduling; it does not emulate VM pause or restore.
use super::{fixture_connection, serve_fixture};
use crate::{
    cache::Cache,
    engine::{CommitRequest, ReadRequest},
    ownership::{
        root::{Outcome, Profile, Root},
        timing::Clock,
    },
    providers::Connection,
    transactions::{
        live::{Limits, Opening, Owner},
        record::Database,
    },
};
use rustix::{
    process::{Pid, Signal, getpid, kill_process},
    time::{ClockId, clock_gettime},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
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

#[derive(Clone, Default)]
struct BootClock(Arc<AtomicU64>);
impl Clock for BootClock {
    fn read(&self) -> Option<Duration> {
        let frozen = self.0.load(Ordering::SeqCst);
        if frozen != 0 {
            return Some(Duration::from_nanos(frozen));
        }
        let now = clock_gettime(ClockId::Boottime);
        let nanos = u64::try_from(now.tv_sec)
            .ok()?
            .checked_mul(1_000_000_000)?
            .checked_add(u64::try_from(now.tv_nsec).ok()?)?;
        // Center a nominally unit-rate source inside the model's [1,2] range.
        // This experiment cannot establish that range for every host/platform.
        Some(Duration::from_nanos(nanos.checked_mul(3)? / 2))
    }
}
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
    let clock = BootClock::default();
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
    if freeze {
        clock
            .0
            .store(clock.read().unwrap().as_nanos() as u64, Ordering::SeqCst);
    }
    kill_process(getpid(), Signal::STOP).unwrap();
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
    std::fs::write(
        report,
        serde_json::to_vec(&(applied, acknowledged, served)).unwrap(),
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn physical_owner_pause_fences_reads_and_late_acknowledgements() {
    for freeze in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let report = directory.path().join("result.json");
        let (endpoint, _state, server) = serve_fixture(200, true).await;
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD, "--ignored", "--nocapture"])
            .env("DURABLE_DB_PAUSE_ENDPOINT", &endpoint)
            .env("DURABLE_DB_PAUSE_REPORT", &report)
            .env("DURABLE_DB_PAUSE_FREEZE", if freeze { "1" } else { "0" })
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = Pid::from_raw(child.id().unwrap() as i32).unwrap();
        timeout(Duration::from_secs(15), async {
            loop {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "child exited before stopping"
                );
                let status =
                    std::fs::read_to_string(format!("/proc/{}/status", pid.as_raw_nonzero()))
                        .unwrap();
                if status.lines().any(|l| l.starts_with("State:\tT")) {
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
        kill_process(pid, Signal::CONT).unwrap();
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
            "frozen clocks must demonstrate the safety violation"
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
