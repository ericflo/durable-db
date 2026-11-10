use super::*;

fn database() -> Database<'static> {
    Database {
        name: "web-app",
        generation: Uuid::from_u128(7),
    }
}
fn encoded() -> Bytes {
    Commit::encode(
        database(),
        2,
        0,
        0,
        "request",
        [9; 32],
        &[
            Mutation {
                key: "a",
                value: None,
            },
            Mutation {
                key: "b",
                value: Some(b""),
            },
            Mutation {
                key: "z",
                value: Some(b"payload"),
            },
        ],
    )
    .unwrap()
}
fn checksum(bytes: &mut Vec<u8>) {
    bytes.truncate(bytes.len() - CHECKSUM);
    let digest = Sha256::digest(&*bytes);
    bytes.extend_from_slice(&digest);
}

#[test]
fn canonical_record_borrows_values_and_distinguishes_delete_from_empty() {
    let bytes = encoded();
    let record = Commit::decode(&bytes).unwrap();
    assert!(record.database() == database());
    assert_eq!(record.revision(), 2);
    assert_eq!(record.snapshot_revision(), 0);
    assert_eq!(record.receipt_floor(), 0);
    assert_eq!(record.request_id(), "request");
    assert_eq!(record.fingerprint(), [9; 32]);
    let mutations: Vec<_> = record.mutations().collect();
    assert_eq!(mutations.len(), 3);
    assert!(mutations[0].value.is_none());
    assert_eq!(mutations[1].value, Some(b"".as_slice()));
    assert_eq!(mutations[2].value, Some(b"payload".as_slice()));
    let value = mutations[2].value.unwrap();
    assert!(
        (bytes.as_ptr() as usize..bytes.as_ptr() as usize + bytes.len())
            .contains(&(value.as_ptr() as usize))
    );
    assert_eq!(
        Commit::encode(
            record.database(),
            record.revision(),
            record.snapshot_revision(),
            record.receipt_floor(),
            record.request_id(),
            record.fingerprint(),
            &mutations
        )
        .unwrap(),
        bytes
    );
    let empty = Commit::encode(
        database(),
        u64::MAX,
        u64::MAX - 1,
        u64::MAX - 1,
        "receipt-only",
        [0; 32],
        &[],
    )
    .unwrap();
    assert_eq!(Commit::decode(&empty).unwrap().mutations().len(), 0);
}

#[test]
fn all_single_byte_corruptions_and_truncations_fail() {
    let bytes = encoded();
    for i in 0..bytes.len() {
        let mut bad = bytes.to_vec();
        bad[i] ^= 1;
        assert!(Commit::decode(&bad).is_err(), "corruption at {i}");
        assert!(Commit::decode(&bytes[..i]).is_err(), "truncation at {i}");
    }
    assert!(Commit::decode(&vec![0; crate::journal::MAX_RECORD_BYTES + 1]).is_err());
}

#[test]
fn valid_checksums_do_not_bypass_semantic_validation() {
    let original = encoded();
    let first = HEADER + database().name.len() + "request".len();
    let mut cases = Vec::new();
    for (offset, value) in [
        (0, b'X'),
        (80, 0),
        (81, 0),
        (83, 129),
        (HEADER, b'/'),
        (first + 6, 0),
        (first + 13, b'a'),
        (first + 13, 0xff),
    ] {
        let mut bad = original.to_vec();
        bad[offset] = value;
        cases.push(bad);
    }
    let mut nil = original.to_vec();
    nil[8..24].fill(0);
    cases.push(nil);
    let mut zero = original.to_vec();
    zero[24..32].fill(0);
    cases.push(zero);
    let mut future = original.to_vec();
    future[32..40].copy_from_slice(&2u64.to_be_bytes());
    cases.push(future);
    let mut floor = original.to_vec();
    floor[40..48].copy_from_slice(&2u64.to_be_bytes());
    cases.push(floor);
    let mut huge = original.to_vec();
    huge[first + 2..first + 6].copy_from_slice(&65_537u32.to_be_bytes());
    cases.push(huge);
    let mut trailing = original.to_vec();
    trailing.insert(trailing.len() - CHECKSUM, 0);
    cases.push(trailing);
    for (i, mut bad) in cases.into_iter().enumerate() {
        checksum(&mut bad);
        assert!(Commit::decode(&bad).is_err(), "semantic corruption {i}");
    }
}

#[test]
fn limits_are_checked_before_encoding_and_during_decode() {
    let key = "k".repeat(1024);
    let value = vec![7; MAX_VALUE];
    let request = "r".repeat(128);
    let max = Commit::encode(
        Database {
            name: &"d".repeat(64),
            ..database()
        },
        1,
        0,
        0,
        &request,
        [0; 32],
        &[Mutation {
            key: &key,
            value: Some(&value),
        }],
    )
    .unwrap();
    assert_eq!(
        Commit::decode(&max)
            .unwrap()
            .mutations()
            .next()
            .unwrap()
            .value
            .unwrap()
            .len(),
        MAX_VALUE
    );
    for (key, value) in [
        ("".to_string(), None),
        ("a\0".into(), None),
        ("a".repeat(1025), None),
        ("a".into(), Some(vec![0; MAX_VALUE + 1])),
    ] {
        assert!(
            Commit::encode(
                database(),
                1,
                0,
                0,
                "r",
                [0; 32],
                &[Mutation {
                    key: &key,
                    value: value.as_deref()
                }]
            )
            .is_err()
        );
    }
    for keys in [vec!["a", "a"], vec!["b", "a"], vec!["a"; 129]] {
        let mutations: Vec<_> = keys
            .iter()
            .map(|key| Mutation { key, value: None })
            .collect();
        assert!(Commit::encode(database(), 1, 0, 0, "r", [0; 32], &mutations).is_err());
    }
    let keys: Vec<_> = (0..128).map(|i| format!("key-{i:03}")).collect();
    let mutations: Vec<_> = keys
        .iter()
        .map(|key| Mutation { key, value: None })
        .collect();
    let max = Commit::encode(database(), 1, 0, 0, "r", [0; 32], &mutations).unwrap();
    assert_eq!(Commit::decode(&max).unwrap().mutations().len(), 128);
    let too_big: Vec<_> = keys
        .iter()
        .map(|key| Mutation {
            key,
            value: Some(value.as_slice()),
        })
        .collect();
    assert!(Commit::encode(database(), 1, 0, 0, "r", [0; 32], &too_big).is_err());
    for (revision, snapshot, request) in [(0, 0, "r"), (1, 1, "r"), (1, 0, "")] {
        assert!(Commit::encode(database(), revision, snapshot, 0, request, [0; 32], &[]).is_err());
    }
    assert!(Commit::encode(database(), MAX_RECEIPTS, 0, 0, "r", [0; 32], &[]).is_ok());
    assert!(Commit::encode(database(), MAX_RECEIPTS + 1, 0, 0, "r", [0; 32], &[]).is_err());
    assert!(Commit::encode(database(), MAX_RECEIPTS + 1, 0, 1, "r", [0; 32], &[]).is_ok());
}
