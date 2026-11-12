use super::*;
use tokio::task::JoinSet;

#[tokio::test(start_paused = true)]
async fn challenges_coalesce_without_authority_and_answers_invalidate_captured_claims() {
    let provider = Provider::new();
    let mut owner = provider
        .root()
        .advertise("127.0.0.1:7654".parse().unwrap())
        .unwrap();
    claim(&mut owner).await;
    // An activating/recovering owner can already answer. No serving permission
    // or recovery-field change follows from publishing or answering a hint.
    let before = provider.record();
    let mut first = provider.root();
    let observed = first.observe().await.unwrap();
    let nonce = first.challenge(observed).await.unwrap().unwrap();
    assert!(first.identity().unwrap().is_none());
    assert!(first.permission().await.unwrap().is_none());
    assert!(before.same_recovery(provider.record()));
    let mut second = provider.root();
    let calls = provider.calls.load(Ordering::SeqCst);
    assert_eq!(
        second
            .challenge(second.observe().await.unwrap())
            .await
            .unwrap(),
        Some(nonce)
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    // Generic control maintenance must not answer on the owner's behalf.
    let old = provider.read().await.unwrap();
    let mut info = old.info.clone();
    info.insert("durable-db-read-fence".into(), Uuid::new_v4().to_string());
    provider.replace(&old, info).await.unwrap();
    assert_eq!(first.observe().await.unwrap().challenge(), Some(nonce));
    let claim = second.claim(second.observe().await.unwrap()).unwrap();
    let mut replies = JoinSet::new();
    replies.spawn(owner.answer_challenges());
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(first.observe().await.unwrap().challenge().is_none());
    assert!(before.same_recovery(provider.record()));
    assert!(matches!(
        second.finish(claim.execute().await).unwrap(),
        Outcome::Conflict
    ));
    assert!(owner.identity().unwrap().is_none());
    validate(&mut owner).await;
    activate(&mut owner).await;
    assert!(owner.permission().await.unwrap().is_some());
}

#[tokio::test(start_paused = true)]
async fn uncertain_challenges_defer_and_stale_owner_markers_are_not_answers() {
    let provider = Provider::new();
    let mut owner = active(&provider).await;
    let mut candidate = provider.root();
    provider.lose_reply.store(true, Ordering::SeqCst);
    assert!(
        candidate
            .challenge(candidate.observe().await.unwrap())
            .await
            .unwrap()
            .is_none()
    );
    let observed = candidate.observe().await.unwrap();
    let nonce = observed.challenge().unwrap();
    assert_eq!(candidate.challenge(observed).await.unwrap(), Some(nonce));
    let old_marker = provider.read().await.unwrap().info["ddb-challenge"].clone();
    claim(&mut candidate).await;
    // An old binary preserves opaque fields when claiming. A challenge bound
    // to the predecessor must not be mistaken for the successor's response.
    let old = provider.read().await.unwrap();
    let mut info = old.info.clone();
    info.insert("ddb-challenge".into(), old_marker);
    provider.replace(&old, info).await.unwrap();
    assert!(candidate.observe().await.unwrap().challenge().is_none());
    let mut challenger = provider.root();
    let nonce2 = challenger
        .challenge(challenger.observe().await.unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(nonce, nonce2);
    let mut replies = JoinSet::new();
    replies.spawn(owner.answer_challenges());
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(
        challenger.observe().await.unwrap().challenge(),
        Some(nonce2)
    );
    assert!(owner.validate(owner.observe().await.unwrap()).is_err());
    // The matching owner's normal metadata publication answers as well.
    validate(&mut candidate).await;
    assert!(challenger.observe().await.unwrap().challenge().is_none());
    for malformed in [
        "bad".to_owned(),
        format!("{}:{}", Uuid::nil(), Uuid::new_v4()),
        format!("{}:{}", candidate.incarnation, Uuid::new_v4().simple()),
    ] {
        let mut info = provider.read().await.unwrap().info;
        info.insert("ddb-challenge".into(), malformed);
        assert!(metadata::decode(&info, "test", encoding(), false).is_err());
    }
}
