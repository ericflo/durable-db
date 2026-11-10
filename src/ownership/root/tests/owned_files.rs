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
    assert_eq!(
        view.get("manifest/1").unwrap().bytes().unwrap(),
        b"selected"
    );
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
    assert_eq!(
        view.get("manifest/1").unwrap().bytes().unwrap(),
        b"successor"
    );
    assert!(view.get("wal/1").is_none());
    assert_eq!(view.revision(), 4);
    assert!(last.permission().unwrap().is_none());
}

#[tokio::test]
async fn selected_external_files_restore_without_resident_payloads_and_deletion_keeps_old_bytes() {
    use crate::storage::immutable::Blob;
    use bytes::Bytes;
    let provider = Provider::new();
    let clock = ManualClock::default();
    let mut old = active(&provider, &clock).await;
    let payload = Bytes::from(vec![19; 128 * 1024]);
    let blob = Blob::upload(provider.files.clone(), payload.clone())
        .await
        .unwrap();
    let mut files = Namespace::new(
        View::empty(Limits::new(8, 256).unwrap()),
        old.take_journal(QueueLimits::default()).unwrap(),
    )
    .unwrap();
    let upload = files
        .prepare_reference("compacted/one", &blob, Condition::Absent)
        .unwrap();
    let version = upload.versions()[0];
    files.complete(upload.send().await).unwrap();
    assert_eq!(
        files.view().unwrap().resident_bytes(),
        56 + "compacted/one".len()
    );
    assert!(
        files
            .view()
            .unwrap()
            .get("compacted/one")
            .unwrap()
            .bytes()
            .is_none()
    );
    let mut next = successor(&provider, &clock).await;
    let journal = scan(&mut next).await;
    seal(&mut next, journal).await;
    drop(files);
    drop(old);
    let view = owned_files::restore(next.replay().unwrap(), Limits::new(8, 256).unwrap())
        .await
        .unwrap();
    let file = view.get("compacted/one").unwrap();
    assert_eq!(file.size(), payload.len() as u64);
    assert_eq!(file.version(), version);
    let retained = *file.blob().unwrap();
    assert_eq!(
        retained
            .read_range(provider.files.as_ref(), 0..retained.size())
            .await
            .unwrap(),
        payload
    );
    let mut files =
        Namespace::new(view, next.take_journal(QueueLimits::default()).unwrap()).unwrap();
    let delete = files
        .prepare(&[Change {
            name: "compacted/one",
            value: None,
            condition: Condition::Matches(version),
        }])
        .unwrap();
    files.complete(delete.send().await).unwrap();
    assert!(files.view().unwrap().is_empty());
    assert_eq!(
        retained
            .read_range(provider.files.as_ref(), 1..4)
            .await
            .unwrap(),
        b"\x13\x13\x13"[..]
    );
    let mut last = successor(&provider, &clock).await;
    let journal = scan(&mut last).await;
    seal(&mut last, journal).await;
    let view = owned_files::restore(last.replay().unwrap(), Limits::new(8, 256).unwrap())
        .await
        .unwrap();
    assert!(view.is_empty());
    assert_eq!(view.revision(), 2);
}
