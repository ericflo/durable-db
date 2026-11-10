use super::{
    catalog::{scan, seal, successor},
    *,
};
use crate::owned_files::{self, Change, Condition, Limits, Namespace, View};

fn set<'a>(name: &'a str, value: &'static [u8]) -> Change<'a> {
    Change {
        name,
        value: Some(value),
        condition: Condition::Any,
    }
}

#[tokio::test]
async fn logical_files_reopen_across_selected_owners_and_exclude_late_mutations() {
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut old = active(&provider, &clock).await;
    let mut files = Namespace::new(
        View::empty(Limits::default()),
        old.take_journal(QueueLimits::default()).unwrap(),
    )
    .unwrap();
    let before = provider.calls.load(Ordering::SeqCst);
    let upload = files
        .prepare(&[set("manifest/1", b"selected"), set("wal/1", b"bytes")])
        .unwrap();
    let version = upload.versions()[0];
    files.complete(upload.send().await).unwrap();
    let late = files.prepare(&[set("manifest/1", b"excluded")]).unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), before);
    let mut next = successor(&provider, &clock).await;
    let journal = scan(&mut next).await;
    seal(&mut next, journal).await;
    let _ = late.send().await;
    let mut partial = next.replay().unwrap();
    assert!(partial.read_next().await.unwrap().is_some());
    assert!(
        owned_files::restore(partial, Limits::default())
            .await
            .is_err()
    );
    assert!(
        owned_files::restore(next.replay().unwrap(), Limits::new(1, 1024).unwrap())
            .await
            .is_err()
    );
    let view = owned_files::restore(next.replay().unwrap(), Limits::default())
        .await
        .unwrap();
    assert_eq!(view.get("manifest/1").unwrap().bytes(), b"selected");
    assert_eq!(view.get("manifest/1").unwrap().version(), version);
    assert_eq!(view.revision(), 2);
    let mut files =
        Namespace::new(view, next.take_journal(QueueLimits::default()).unwrap()).unwrap();
    let upload = files
        .prepare(&[
            Change {
                name: "manifest/1",
                value: Some(b"successor"),
                condition: Condition::Matches(version),
            },
            Change {
                name: "wal/1",
                value: None,
                condition: Condition::Any,
            },
        ])
        .unwrap();
    assert_ne!(upload.versions()[0], version);
    files.complete(upload.send().await).unwrap();
    let mut last = successor(&provider, &clock).await;
    let journal = scan(&mut last).await;
    seal(&mut last, journal).await;
    let replay = last.replay().unwrap();
    clock.set(clock.now.load(Ordering::SeqCst) + 10);
    let view = owned_files::restore(replay, Limits::default())
        .await
        .unwrap();
    assert_eq!(view.get("manifest/1").unwrap().bytes(), b"successor");
    assert!(view.get("wal/1").is_none());
    assert_eq!(view.revision(), 4);
    assert!(last.permission().unwrap().is_none());
}
