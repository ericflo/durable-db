//! Reuse the single-entry tree algorithm, staging intermediate paths locally.
//! Only new pages reachable from the final root are uploaded before returning.
use super::*;
use crate::storage::{Object, Version};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::UpdateVersion;
use std::collections::BTreeSet;
use tokio::sync::Mutex;

pub(crate) const MAX_UPDATES: usize = 64;
const MAX_STAGED_BYTES: usize = 64 * 1024 * 1024;
const MAX_STAGED_PAGES: usize = MAX_UPDATES * (2 * MAX_HEIGHT as usize + 1);

impl Directory {
    pub async fn insert_many(
        &self,
        root: Option<&Root>,
        updates: &BTreeMap<String, Pointer>,
    ) -> anyhow::Result<Root> {
        ensure!(
            (1..=MAX_UPDATES).contains(&updates.len()),
            "directory update batch must contain 1..64 entries"
        );
        // Reject the entire malformed request before creating any index objects.
        for (db, pointer) in updates {
            validate_db(db)?;
            ensure!(
                witness(&pointer.key, "manifests/")? == db && digest_valid(&pointer.sha256),
                "invalid directory update pointer"
            );
        }
        if updates.len() == 1 {
            let (db, pointer) = updates.first_key_value().unwrap();
            return self.insert_one(root, db, pointer.clone()).await;
        }
        let staged = Arc::new(Staging {
            backing: self.files.clone(),
            state: Mutex::new(Staged {
                objects: BTreeMap::new(),
                weight: 0,
            }),
        });
        let mut temporary = Directory::new(staged.clone());
        temporary.cache = self.cache.clone();
        // Use existing verified cache entries, but keep intermediate paths out
        // of both cache layers. They will never be published or uploaded.
        temporary.cache_writes = false;
        let mut next = root.cloned();
        for (db, pointer) in updates {
            next = Some(
                temporary
                    .insert_one(next.as_ref(), db, pointer.clone())
                    .await?,
            );
        }
        let root = next.unwrap();
        let objects = staged.take_reachable(&root).await?;
        drop(temporary);
        drop(staged); // Release intermediate bytes before starting network uploads.
        stream::iter(objects)
            .map(|(key, bytes)| async move {
                self.files.put(&key, bytes.clone()).await?;
                if let Some(cache) = &self.cache {
                    cache.insert(&key, bytes).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await?;
        // Publication still needs the caller's authority CAS and original fence.
        Ok(root)
    }
}

struct Staged {
    objects: BTreeMap<String, Bytes>,
    weight: usize,
}
struct Staging {
    backing: Arc<dyn Storage>,
    state: Mutex<Staged>,
}
impl Staging {
    async fn take_reachable(&self, root: &Root) -> anyhow::Result<Vec<(String, Bytes)>> {
        let mut staged = self.state.lock().await;
        let mut stack = vec![root.object.clone()];
        let mut visited = BTreeSet::new();
        let mut objects = Vec::new();
        while let Some(key) = stack.pop() {
            if !visited.insert(key.clone()) {
                continue;
            }
            let Some(bytes) = staged.objects.remove(&key) else {
                // An unchanged immutable subtree is already durable. Its
                // descendants cannot contain any of our newly allocated pages.
                continue;
            };
            if let Node::Branch(children) = serde_json::from_slice(&bytes)? {
                stack.extend(children.into_iter().map(|child| child.object));
            }
            objects.push((key, bytes));
        }
        Ok(objects)
    }
}
#[async_trait]
impl Storage for Staging {
    async fn get(&self, key: &str, max_bytes: usize) -> anyhow::Result<Option<Object>> {
        let staged = self.state.lock().await.objects.get(key).cloned();
        if let Some(bytes) = staged {
            ensure!(bytes.len() <= max_bytes, "staged index exceeds read bound");
            return Ok(Some(Object {
                bytes,
                version: Version(UpdateVersion {
                    e_tag: None,
                    version: None,
                }),
            }));
        }
        self.backing.get(key, max_bytes).await
    }
    async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        ensure!(
            key.starts_with("indexes/") && bytes.len() <= MAX_NODE_BYTES,
            "invalid staged directory page"
        );
        let mut staged = self.state.lock().await;
        ensure!(
            !staged.objects.contains_key(key),
            "directory page identity reused"
        );
        let weight = staged
            .weight
            .checked_add(bytes.len() + key.len() + 256)
            .context("directory staging overflow")?;
        ensure!(
            weight <= MAX_STAGED_BYTES && staged.objects.len() < MAX_STAGED_PAGES,
            "directory staging budget exceeded"
        );
        staged.objects.insert(key.to_owned(), bytes);
        staged.weight = weight;
        Ok(())
    }
    async fn cas(&self, _: &str, _: Option<&Version>, _: Bytes) -> anyhow::Result<bool> {
        bail!("directory staging cannot publish authority")
    }
    async fn list(&self, _: &str, _: Option<&str>, _: usize) -> anyhow::Result<Vec<String>> {
        bail!("directory staging cannot list storage")
    }
    async fn delete(&self, _: &str) -> anyhow::Result<()> {
        bail!("directory staging cannot delete storage")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Remote;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Counted {
        inner: Arc<dyn Storage>,
        puts: AtomicUsize,
    }
    #[async_trait]
    impl Storage for Counted {
        async fn get(&self, key: &str, max: usize) -> anyhow::Result<Option<Object>> {
            self.inner.get(key, max).await
        }
        async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.inner.put(key, bytes).await
        }
        async fn cas(
            &self,
            key: &str,
            old: Option<&Version>,
            bytes: Bytes,
        ) -> anyhow::Result<bool> {
            self.inner.cas(key, old, bytes).await
        }
        async fn list(
            &self,
            prefix: &str,
            after: Option<&str>,
            limit: usize,
        ) -> anyhow::Result<Vec<String>> {
            self.inner.list(prefix, after, limit).await
        }
        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            self.inner.delete(key).await
        }
    }
    fn pointer(db: &str) -> Pointer {
        Pointer {
            key: format!("manifests/{db}.{}", Uuid::new_v4()),
            sha256: "0".repeat(64),
        }
    }
    fn files() -> Arc<Counted> {
        Arc::new(Counted {
            inner: Arc::new(
                Remote::new(Arc::new(object_store::memory::InMemory::new()), "batch").unwrap(),
            ),
            puts: AtomicUsize::new(0),
        })
    }
    #[tokio::test]
    async fn full_leaf_batch_uploads_only_the_final_page_and_rejects_bad_input_before_io() {
        let files = files();
        let tree = Directory::new(files.clone());
        let mut updates: BTreeMap<_, _> = (0..64)
            .map(|i| {
                let db = format!("db-{i:03}");
                (db.clone(), pointer(&db))
            })
            .collect();
        let root = tree.insert_many(None, &updates).await.unwrap();
        assert_eq!(root.count, 64);
        assert_eq!(files.puts.load(Ordering::SeqCst), 1);
        for (db, expected) in &updates {
            assert_eq!(
                tree.get(Some(&root), db).await.unwrap().as_ref(),
                Some(expected)
            );
        }
        updates.insert("overflow".into(), pointer("overflow"));
        assert!(tree.insert_many(Some(&root), &updates).await.is_err());
        updates.remove("overflow");
        updates.insert("db-063".into(), pointer("foreign"));
        assert!(tree.insert_many(Some(&root), &updates).await.is_err());
        assert_eq!(files.puts.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn bulk_updates_split_reuse_and_collect_without_losing_unrelated_entries() {
        let files = files();
        let tree = Directory::new(files.clone())
            .with_cache(Arc::new(Cache::new(4 * 1024 * 1024, None).unwrap()));
        let mut expected = BTreeMap::new();
        let mut root = None;
        for chunk in 0..64 {
            let updates: BTreeMap<_, _> = (chunk * 64..(chunk + 1) * 64)
                .map(|i| {
                    let db = format!("site-{i:05}");
                    (db.clone(), pointer(&db))
                })
                .collect();
            root = Some(tree.insert_many(root.as_ref(), &updates).await.unwrap());
            expected.extend(updates);
        }
        let old = root.unwrap();
        assert!(old.height >= 3);
        let old_pointer = expected["site-00000"].clone();
        let mut updates: BTreeMap<_, _> = (0..59)
            .map(|i| {
                let db = format!("site-{:05}", i * 61);
                (db.clone(), pointer(&db))
            })
            .collect();
        for db in ["a", "a-", "a0", "a_", &"z".repeat(64)] {
            updates.insert(db.into(), pointer(db));
        }
        let before = files.puts.load(Ordering::SeqCst);
        let root = tree.insert_many(Some(&old), &updates).await.unwrap();
        assert!(
            files.puts.load(Ordering::SeqCst) - before < 128,
            "do not upload intermediate roots"
        );
        assert_eq!(
            tree.get(Some(&old), "site-00000").await.unwrap(),
            Some(old_pointer)
        );
        expected.extend(updates);
        assert_eq!(root.count, expected.len() as u64);
        let mut names = Vec::new();
        loop {
            let page = tree
                .list(
                    Some(&root),
                    names
                        .last()
                        .map(|s: &String| s.strip_prefix("heads/").unwrap()),
                    31,
                )
                .await
                .unwrap();
            if page.is_empty() {
                break;
            }
            names.extend(page);
        }
        let mut ordered: Vec<_> = expected
            .keys()
            .map(|db| format!("heads/{db}.json"))
            .collect();
        ordered.sort();
        assert_eq!(names, ordered);

        // Compare witness membership to an independent complete traversal.
        let mut live = BTreeSet::new();
        let mut stack = vec![root.clone()];
        while let Some(page) = stack.pop() {
            live.insert(page.object.clone());
            if let Node::Branch(children) = tree.load(&page).await.unwrap() {
                stack.extend(children);
            }
        }
        let mut after = None;
        loop {
            let page = files.list("indexes/", after.as_deref(), 128).await.unwrap();
            if page.is_empty() {
                break;
            }
            after = page.last().cloned();
            for key in page {
                let reachable = tree.reachable(Some(&root), &key).await.unwrap();
                assert_eq!(reachable, live.contains(&key));
                if !reachable {
                    files.delete(&key).await.unwrap();
                }
            }
        }
        let cold = Directory::new(files);
        for (db, pointer) in expected {
            assert_eq!(cold.get(Some(&root), &db).await.unwrap(), Some(pointer));
        }
    }
}
