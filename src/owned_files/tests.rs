use super::*;
use crate::{
    journal::{QueueLimits, Writer},
    storage::Remote,
};
use std::sync::Arc;

fn namespace(limits: Limits) -> Namespace {
    Namespace::new(
        View::empty(limits),
        Writer::new(
            Arc::new(
                Remote::new(Arc::new(object_store::memory::InMemory::new()), "files").unwrap(),
            ),
            Uuid::new_v4(),
            QueueLimits::default(),
        )
        .unwrap(),
    )
    .unwrap()
}
fn change<'a>(name: &'a str, value: Option<&'a [u8]>, condition: Condition) -> Change<'a> {
    Change {
        name,
        value,
        condition,
    }
}
async fn put(ns: &mut Namespace, name: &str, value: &[u8]) -> Version {
    let upload = ns
        .prepare(&[change(name, Some(value), Condition::Any)])
        .unwrap();
    let version = upload.versions()[0];
    ns.complete(upload.send().await).unwrap();
    version
}

#[tokio::test]
async fn conditions_include_pending_predecessors_and_visibility_waits_for_every_gap() {
    let mut ns = namespace(Limits::default());
    let first = ns
        .prepare(&[change("manifest/1", Some(b"one"), Condition::Absent)])
        .unwrap();
    let first_version = first.versions()[0];
    assert!(
        ns.prepare(&[change("manifest/1", Some(b"conflict"), Condition::Absent)])
            .err()
            .unwrap()
            .downcast_ref::<Error>()
            .is_some()
    );
    let second = ns
        .prepare(&[
            change(
                "manifest/1",
                Some(b"two"),
                Condition::Matches(first_version),
            ),
            change("wal/2", Some(b""), Condition::Absent),
        ])
        .unwrap();
    assert!(
        ns.prepare(&[change(
            "manifest/1",
            None,
            Condition::Matches(first_version)
        )])
        .is_err()
    );
    ns.complete(second.send().await).unwrap();
    assert!(ns.view().unwrap().is_empty());
    assert_eq!(ns.view().unwrap().revision(), 0);
    ns.complete(first.send().await).unwrap();
    assert_eq!(
        ns.view()
            .unwrap()
            .get("manifest/1")
            .unwrap()
            .bytes()
            .unwrap(),
        b"two"
    );
    assert_eq!(
        ns.view().unwrap().get("wal/2").unwrap().bytes().unwrap(),
        b""
    );
    assert_eq!(ns.view().unwrap().revision(), 3);
    assert_eq!(ns.pending_batches(), 0);
    assert_eq!(ns.durable_prefix().position(), 2);
    assert_eq!(ns.view().unwrap().list("manifest/").count(), 1);
}

#[tokio::test]
async fn identical_overwrites_and_delete_recreate_never_accept_stale_versions() {
    let mut ns = namespace(Limits::default());
    let first = put(&mut ns, "m", b"same").await;
    let second = put(&mut ns, "m", b"same").await;
    assert_ne!(first, second);
    assert!(
        ns.prepare(&[change("m", Some(b"bad"), Condition::Matches(first))])
            .is_err()
    );
    let delete = ns
        .prepare(&[change("m", None, Condition::Matches(second))])
        .unwrap();
    let recreate = ns
        .prepare(&[change("m", Some(b"same"), Condition::Absent)])
        .unwrap();
    let third = recreate.versions()[0];
    assert_ne!(second, third);
    ns.complete(recreate.send().await).unwrap();
    assert_eq!(ns.view().unwrap().get("m").unwrap().version(), second);
    ns.complete(delete.send().await).unwrap();
    assert_eq!(ns.view().unwrap().get("m").unwrap().version(), third);
    assert!(
        ns.prepare(&[change("m", None, Condition::Matches(second))])
            .is_err()
    );
}

#[tokio::test]
async fn admission_errors_are_atomic_and_limits_cover_pending_allocations() {
    let mut ns = namespace(Limits::new(2, 10).unwrap());
    let first = ns
        .prepare(&[change("a", Some(b"1234"), Condition::Absent)])
        .unwrap();
    for changes in [
        vec![change("b", Some(b"12345"), Condition::Absent)],
        vec![
            change("b", Some(b"1"), Condition::Absent),
            change("c", Some(b"1"), Condition::Absent),
        ],
        vec![
            change("b", Some(b"1"), Condition::Absent),
            change("a", Some(b"bad"), Condition::Absent),
        ],
        vec![
            change("b", Some(b"1"), Condition::Absent),
            change("b", None, Condition::Any),
        ],
    ] {
        assert!(ns.prepare(&changes).is_err());
        assert_eq!(ns.pending_batches(), 1);
    }
    ns.complete(first.send().await).unwrap();
    let second = put(&mut ns, "b", b"1234").await;
    assert_eq!(second.revision, 2);
    assert_eq!(ns.view().unwrap().resident_bytes(), 10);
    for name in ["", "/a", "a/", "a//b", "a/../b", "a/./b", "a\0b"] {
        assert!(ns.prepare(&[change(name, None, Condition::Any)]).is_err());
    }
    assert!(
        ns.prepare(&[change(
            "a",
            Some(&vec![0; MAX_INLINE_BYTES + 1]),
            Condition::Any
        )])
        .is_err()
    );
    let delete = ns.prepare(&[change("a", None, Condition::Any)]).unwrap();
    let next = ns
        .prepare(&[change("c", Some(b"1234"), Condition::Absent)])
        .unwrap();
    ns.complete(next.send().await).unwrap();
    ns.complete(delete.send().await).unwrap();
    assert_eq!(ns.view().unwrap().resident_bytes(), 10);
    assert_eq!(ns.view().unwrap().len(), 2);
}

#[tokio::test]
async fn cancelled_upload_stays_pending_until_recovery_and_queue_limits_are_enforced() {
    let mut ns = namespace(Limits::default());
    let first = ns
        .prepare(&[change("a", Some(b"one"), Condition::Absent)])
        .unwrap();
    drop(first);
    assert_eq!(ns.resolve(1).await.unwrap(), journal::Resolution::Unknown);
    for i in 1..16 {
        let next = ns
            .prepare(&[change(&format!("f{i}"), Some(b"two"), Condition::Absent)])
            .unwrap();
        ns.complete(next.send().await).unwrap();
    }
    assert_eq!(ns.pending_batches(), 16);
    assert!(
        ns.prepare(&[change("last", Some(b"full"), Condition::Absent)])
            .is_err()
    );
    assert!(ns.view().unwrap().is_empty());
}

#[test]
fn codec_rejects_corruption_truncation_and_checksum_valid_invalid_fields() {
    let record = record::Record {
        name: "table/é".into(),
        value: Some(Content::Inline(Bytes::from_static(b""))),
        expected: None,
        version: Version {
            epoch: Uuid::new_v4(),
            revision: 1,
        },
    };
    let wire = record.encode().unwrap();
    assert_eq!(
        record::Record::decode(&wire)
            .unwrap()
            .value
            .unwrap()
            .inline()
            .unwrap(),
        &b""[..]
    );
    for end in 0..wire.len() {
        assert!(record::Record::decode(&wire[..end]).is_err());
    }
    for i in 0..wire.len() {
        let mut bad = wire.to_vec();
        bad[i] ^= 1;
        assert!(record::Record::decode(&bad).is_err());
    }
    for (start, end) in [(0, 8), (8, 24), (24, 32), (56, 58)] {
        let mut bad = wire.to_vec();
        bad[start..end].fill(0);
        let end = bad.len() - 32;
        let digest = <sha2::Sha256 as sha2::Digest>::digest(&bad[..end]);
        bad[end..].copy_from_slice(&digest);
        assert!(record::Record::decode(&bad).is_err());
    }
}

#[tokio::test]
async fn definite_conflicts_must_wait_for_their_predecessor_and_partial_internal_failures_retire() {
    let mut ns = namespace(Limits::default());
    let upload = ns
        .prepare(&[
            change("a", Some(b"one"), Condition::Absent),
            change("b", Some(b"two"), Condition::Absent),
        ])
        .unwrap();
    let error = ns
        .prepare(&[change("a", None, Condition::Absent)])
        .err()
        .unwrap();
    let Error::Precondition { through } = error.downcast_ref::<Error>().unwrap();
    assert_eq!(*through, 1);
    assert!(ns.durable_prefix().position() < *through);
    let version = upload.versions()[0];
    assert_eq!(version.to_string().parse::<Version>().unwrap(), version);
    assert!(
        "00000000000000000000000000000000-0000000000000001"
            .parse::<Version>()
            .is_err()
    );
    // Simulate a broken in-process projection after the first mutation. The
    // namespace must become unusable, even though its journal is durable.
    ns.pending.front_mut().unwrap().records[1].version.revision += 1;
    assert!(ns.complete(upload.send().await).is_err());
    assert!(ns.view().is_err());
    assert!(
        ns.prepare(&[change("c", Some(b"no"), Condition::Any)])
            .is_err()
    );
}
