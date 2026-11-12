//! Durable proposals let replicas combine independent database publications.
//! After submission, an absent current pointer is never a definite abort.
use super::*;
use anyhow::ensure;
use futures::{StreamExt, TryStreamExt, stream};

const MAX_PROPOSAL_BYTES: usize = 2048;
const MAX_MANIFEST_BYTES: usize = 512 * 1024;
const MAX_SELECTION: usize = 64;

/// Best-effort cleanup of proven committed proposals. A full queue or process
/// loss merely leaves garbage for the durable fenced collector. No write waits
/// for this queue, and workers exit after the owner is dropped and queued work drains.
pub(super) struct Cleanup {
    queue: tokio::sync::mpsc::Sender<String>,
}
impl Cleanup {
    pub fn new(authority: Arc<dyn Authority>) -> Self {
        let (queue, pending) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            stream::unfold(pending, |mut pending| async move {
                pending.recv().await.map(|key| (key, pending))
            })
            .for_each_concurrent(8, |key: String| {
                let authority = &authority;
                async move {
                    if authority.delete_versions(&key).await.is_ok() {
                        metrics::REGISTRY.event(Event::ProposalDeleteAck, 1);
                    }
                }
            })
            .await;
        });
        Self { queue }
    }
    fn committed(&self, key: String) {
        let _ = self.queue.try_send(key);
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Proposal {
    database: String,
    expected: Option<Pointer>,
    next: Pointer,
    fence: Option<String>,
}
impl Proposal {
    fn key(&self) -> String {
        format!(
            "proposals/{}",
            self.next
                .key
                .strip_prefix("manifests/")
                .expect("validated manifest")
        )
    }
    fn validate(&self, key: &str) -> anyhow::Result<()> {
        ensure!(
            witness(key, "proposals/")? == self.database,
            "incorrect proposal namespace"
        );
        ensure!(
            self.next.key == format!("manifests/{}", key.strip_prefix("proposals/").unwrap()),
            "incorrect proposal manifest identity"
        );
        for pointer in std::iter::once(&self.next).chain(self.expected.iter()) {
            ensure!(
                witness(&pointer.key, "manifests/")? == self.database
                    && pointer.sha256.len() == 64
                    && pointer.sha256.bytes().all(|b| b.is_ascii_hexdigit()),
                "invalid proposal pointer"
            );
        }
        if let Some(fence) = &self.fence {
            Uuid::parse_str(fence).context("invalid proposal fence")?;
        }
        Ok(())
    }
}

impl Published {
    pub(super) async fn publish(
        &self,
        key: &str,
        old: Option<&Version>,
        bytes: Bytes,
    ) -> anyhow::Result<Option<Version>> {
        let started = std::time::Instant::now();
        let publication = Uuid::new_v4();
        let database = slot(key)?;
        ensure!(
            bytes.len() <= MAX_MANIFEST_BYTES,
            "manifest exceeds publication bound"
        );
        let initial = self.authority.read().await?;
        if self.global_collection
            && let Some(old) = old
        {
            let observation: DataObservation = serde_json::from_str(
                old.0
                    .version
                    .as_deref()
                    .context("missing data collection observation")?,
            )?;
            ensure!(
                observation.format == 5,
                "unsupported data collection observation"
            );
            // The engine captured this before preparing any pages. Adopting a
            // later fence here could publish pages already collected as orphans.
            if observation.fence.as_ref() != initial.info.get("durable-db-fence") {
                return Ok(None);
            }
        }
        let initial_root = root(&initial.info)?;
        let expected = match self.directory.get(initial_root.as_ref(), &database).await {
            Err(e) if e.is::<MissingNode>() => return Ok(None),
            other => other?,
        };
        let identity = expected.as_ref().map(serde_json::to_string).transpose()?;
        let matches = match old {
            None => expected.is_none(),
            Some(v) => identity.as_ref() == v.0.e_tag.as_ref(),
        };
        if !matches {
            return Ok(None);
        }
        budget(&initial.info)?;
        let proposal = Proposal {
            database,
            expected,
            next: Pointer {
                key: format!("manifests/{}.{}", slot(key)?, publication),
                sha256: hex::encode(Sha256::digest(&bytes)),
            },
            fence: initial.info.get("durable-db-fence").cloned(),
        };
        let name = proposal.key();
        proposal.validate(&name)?;
        let encoded = Bytes::from(serde_json::to_vec(&proposal)?);
        ensure!(
            encoded.len() <= MAX_PROPOSAL_BYTES,
            "proposal exceeds size bound"
        );
        self.files.put(&proposal.next.key, bytes.clone()).await?;
        if let Some(cache) = &self.cache {
            cache.insert(&proposal.next.key, bytes).await;
        }

        // Once this upload starts, another replica can publish it, including
        // after this task disappears. No path below may return Ok(None).
        self.files.put(&name, encoded).await?;
        metrics::REGISTRY.event(Event::ProposalSubmitted, 1);
        // Admission only: the protocol is safe with concurrent helpers. One
        // helper per process bounds staging memory and duplicate network work.
        let queued = std::time::Instant::now();
        let _helper = self
            .helpers
            .acquire()
            .await
            .context("publication helper stopped")?;
        let queue_ms = queued.elapsed().as_millis() as u64;
        // Fixed outcomes and a random invocation identity only; never log a
        // database name, key, manifest body or provider error.
        let progress = |outcome: &'static str, attempt: u64, prepare_ms: u64, compare_ms: u64| {
            tracing::info!(%publication, outcome, attempt, queue_ms, prepare_ms, compare_ms,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "shared publication progress");
        };
        progress("admitted", 0, 0, 0);
        let mut discovery = None;
        for attempt in 0..8 {
            let preparing = std::time::Instant::now();
            let before = self.authority.read().await?;
            if self.proposal_result(&proposal, &before).await? {
                self.proposal_cleanup.committed(name.clone());
                progress("helped", attempt, preparing.elapsed().as_millis() as u64, 0);
                return self.head_version(&proposal.next, &before).map(Some);
            }
            // One bounded discovery per invocation. A comparison conflict can
            // reuse immutable proposal metadata, but never target eligibility.
            // New arrivals can be helped by the next invocation.
            let observed = if discovery.is_none() {
                let names = self
                    .files
                    .list("proposals/", cursor(&before, HELP_CURSOR), MAX_SELECTION)
                    .await?;
                let next_cursor = if names.len() == MAX_SELECTION {
                    names.last().cloned().unwrap()
                } else {
                    String::new()
                };
                let pending: Vec<Option<Proposal>> = stream::iter(names)
                    .map(|candidate_name| {
                        let own_name = &name;
                        async move {
                            let name = candidate_name;
                            if &name == own_name {
                                return Ok::<Option<Proposal>, anyhow::Error>(None);
                            }
                            let Some(object) = self.files.get(&name, MAX_PROPOSAL_BYTES).await?
                            else {
                                return Ok(None);
                            };
                            let candidate = serde_json::from_slice::<Proposal>(&object.bytes)
                                .map_err(anyhow::Error::from)
                                .and_then(|candidate| {
                                    candidate.validate(&name)?;
                                    Ok(candidate)
                                });
                            match candidate {
                                Ok(candidate) => Ok(Some(candidate)),
                                Err(_) => {
                                    // An invalid uncommitted proposal cannot authorize
                                    // a write. The fenced collector can reclaim it.
                                    metrics::REGISTRY.event(Event::ProposalRejected, 1);
                                    Ok(None)
                                }
                            }
                        }
                    })
                    .buffer_unordered(8)
                    .try_collect::<Vec<_>>()
                    .await?;
                discovery = Some((
                    next_cursor,
                    pending.into_iter().flatten().collect::<Vec<_>>(),
                ));
                // Refresh after discovery I/O. Retries already checked a new
                // comparison above and need no duplicate read/target lookup.
                let observed = self.authority.read().await?;
                if self.proposal_result(&proposal, &observed).await? {
                    self.proposal_cleanup.committed(name.clone());
                    progress("helped", attempt, preparing.elapsed().as_millis() as u64, 0);
                    return self.head_version(&proposal.next, &observed).map(Some);
                }
                observed
            } else {
                before
            };
            let (next_cursor, pending) = discovery.as_ref().unwrap();
            // Raw observations may be stale; only the exact CAS can publish.
            let observed_root = root(&observed.info)?;
            let mut selected = BTreeMap::new();
            selected.insert(proposal.database.clone(), proposal.next.clone());
            if !self.manifest_available(&proposal.next).await? {
                bail!("submitted manifest unavailable; outcome is uncertain");
            }
            let eligible: Vec<Option<&Proposal>> = stream::iter(0..pending.len())
                .map(|index| {
                    let candidate = &pending[index];
                    let observed_root = &observed_root;
                    let eligible = candidate.database != proposal.database
                        && candidate.fence.as_ref() == observed.info.get("durable-db-fence");
                    async move {
                        if !eligible {
                            return Ok(None);
                        }
                        let current = match self
                            .directory
                            .get(observed_root.as_ref(), &candidate.database)
                            .await
                        {
                            Err(e) if e.is::<MissingNode>() => {
                                return Ok::<Option<&Proposal>, anyhow::Error>(None);
                            }
                            other => other?,
                        };
                        if current == candidate.expected
                            && self.manifest_available(&candidate.next).await?
                        {
                            Ok(Some(candidate))
                        } else {
                            Ok(None)
                        }
                    }
                })
                .buffer_unordered(8)
                .try_collect::<Vec<_>>()
                .await?;
            for candidate in eligible.into_iter().flatten() {
                if selected.len() == MAX_SELECTION {
                    break;
                }
                selected
                    .entry(candidate.database.clone())
                    .or_insert_with(|| candidate.next.clone());
            }
            let updated = match self
                .directory
                .insert_many(observed_root.as_ref(), &selected)
                .await
            {
                Err(e) if e.is::<MissingNode>() => continue,
                other => other?,
            };
            let mut info = observed.info.clone();
            write_root(&mut info, updated)?;
            info.insert(HELP_CURSOR.into(), next_cursor.clone());
            budget(&info)?;
            // Errors may have committed the whole group. Preserve uncertainty.
            let prepare_ms = preparing.elapsed().as_millis() as u64;
            let comparing = std::time::Instant::now();
            let result = self.authority.replace(&observed, info).await;
            progress(
                match &result {
                    Ok(Some(_)) => "confirmed",
                    Ok(None) => "conflict",
                    Err(_) => "uncertain",
                },
                attempt + 1,
                prepare_ms,
                comparing.elapsed().as_millis() as u64,
            );
            if let Some(published) = result? {
                metrics::REGISTRY.event(Event::PublicationGroups, 1);
                metrics::REGISTRY.event(Event::PublicationGroupMembers, selected.len());
                for pointer in selected.values() {
                    self.proposal_cleanup.committed(format!(
                        "proposals/{}",
                        pointer
                            .key
                            .strip_prefix("manifests/")
                            .expect("validated manifest")
                    ));
                }
                return self.head_version(&proposal.next, &published).map(Some);
            }
            metrics::REGISTRY.event(Event::PublicationRebase, 1);
            tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt + 1))).await;
        }
        bail!("publication helping budget exhausted; outcome is uncertain")
    }

    // True proves this fresh identity was published. False permits helping only
    // while the original target/fence remain equal. Every other case is unknown:
    // the proposal might already have committed and then been superseded.
    async fn proposal_result(
        &self,
        proposal: &Proposal,
        observed: &Bucket,
    ) -> anyhow::Result<bool> {
        let current = self
            .directory
            .get(root(&observed.info)?.as_ref(), &proposal.database)
            .await?;
        if current.as_ref() == Some(&proposal.next) {
            return Ok(true);
        }
        ensure!(
            current == proposal.expected
                && observed.info.get("durable-db-fence") == proposal.fence.as_ref(),
            "submitted publication changed or was fenced; outcome is uncertain"
        );
        Ok(false)
    }

    async fn manifest_available(&self, pointer: &Pointer) -> anyhow::Result<bool> {
        if let Some(cache) = &self.cache
            && let Some(bytes) = cache.get(&pointer.key, &pointer.sha256).await
        {
            ensure!(
                bytes.len() <= MAX_MANIFEST_BYTES,
                "manifest exceeds publication bound"
            );
            return Ok(true);
        }
        let Some(object) = self.files.get(&pointer.key, MAX_MANIFEST_BYTES).await? else {
            return Ok(false);
        };
        ensure!(
            hex::encode(Sha256::digest(&object.bytes)) == pointer.sha256,
            "proposal manifest checksum mismatch"
        );
        if let Some(cache) = &self.cache {
            cache.insert(&pointer.key, object.bytes).await;
        }
        Ok(true)
    }
}
