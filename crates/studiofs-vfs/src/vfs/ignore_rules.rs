use super::*;
use crate::scratch_rules::{MAX_RULE_BYTES, RULE_FILE, ScratchClass, ScratchRules};
use std::collections::BTreeMap;
use tokio::sync::{Notify, broadcast, watch};

type RuleSources = BTreeMap<Vec<u8>, Vec<u8>>;

pub(super) struct IgnoreRules {
    snapshot: parking_lot::RwLock<Arc<ScratchRules>>,
    sources: RuleSources,
    pub(super) refresh: Notify,
    changed: broadcast::Sender<Vec<Vec<u8>>>,
    bootstrap: watch::Sender<bool>,
}

impl IgnoreRules {
    pub(super) fn load(meta: &dyn VfsMeta) -> Self {
        let mut restored = false;
        let sources: RuleSources = match meta.load_ignore_rules_cache() {
            Ok(Some(bytes)) => match postcard::from_bytes(&bytes) {
                Ok(sources) => {
                    restored = true;
                    sources
                }
                Err(error) => {
                    warn!(%error, "could not decode local ignore rules");
                    RuleSources::new()
                }
            },
            Ok(None) => RuleSources::new(),
            Err(error) => {
                warn!(%error, "could not load local ignore rules");
                RuleSources::new()
            }
        };
        let mut rules = ScratchRules::new(true);
        for (directory, payload) in &sources {
            if let Err(error) = rules.update(directory, payload) {
                warn!(%error, ?directory, "could not restore local ignore rules");
            }
        }
        Self {
            snapshot: parking_lot::RwLock::new(Arc::new(rules)),
            sources,
            refresh: Notify::new(),
            changed: broadcast::channel(32).0,
            bootstrap: watch::channel(restored).0,
        }
    }
}

impl StudioVfs {
    /// Classify a canonical volume-relative byte path using the current snapshot.
    pub fn classify_scratch_path(&self, path: &[u8], is_dir: bool) -> ScratchClass {
        let snapshot = Arc::clone(&self.ignore_rules.snapshot.read());
        snapshot.classify(path, is_dir)
    }

    /// Receive volume-relative directories whose rules were replaced or removed.
    pub fn subscribe_ignore_rules_changes(&self) -> broadcast::Receiver<Vec<Vec<u8>>> {
        self.ignore_rules.changed.subscribe()
    }

    /// Wait for the initial prefetch pass before exposing the mount.
    pub async fn wait_for_ignore_rules_bootstrap(&self) {
        if !self.read_only_mount {
            let mut bootstrap = self.ignore_rules.bootstrap.subscribe();
            if tokio::time::timeout(Duration::from_secs(5), bootstrap.wait_for(|ready| *ready))
                .await
                .is_err()
            {
                warn!("ignore rule discovery or persistence exceeded the mount bootstrap budget");
            }
        }
    }

    pub(super) async fn background_ignore_rules(self: Arc<Self>) {
        let mut sources = self.ignore_rules.sources.clone();
        let mut versions = BTreeMap::new();
        let mut pinned = HashSet::new();
        let cancellation = &self.state.shutdown.background_shutdown_token;
        loop {
            let retry = self
                .refresh_ignore_rules(&mut sources, &mut versions, &mut pinned)
                .await;
            self.ignore_rules.bootstrap.send_replace(true);
            tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = self.ignore_rules.refresh.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(15)), if retry => {}
            }
        }
        self.ignore_rules.bootstrap.send_replace(true);
        for object_id in pinned {
            self.local_payload_backend.unpin(object_id);
        }
    }

    async fn read_ignore_rule_payload(&self, ino: Ino, size: u64) -> VfsResult<Vec<u8>> {
        let mut payload = Vec::with_capacity(size as usize);
        for block_index in 0..size.div_ceil(u64::from(self.block_size)) as u32 {
            match self
                .read_metadata_block_from_slice_graph_async(ino, block_index, size)
                .await?
            {
                Some(bytes) => payload.extend_from_slice(&bytes),
                None => {
                    payload.resize(payload.len() + self.logical_block_len(size, block_index), 0)
                }
            }
        }
        Ok(payload)
    }

    async fn refresh_ignore_rules(
        &self,
        sources: &mut RuleSources,
        versions: &mut BTreeMap<Vec<u8>, (Ino, u64, u64)>,
        pinned: &mut HashSet<u64>,
    ) -> bool {
        let cancellation = self.state.shutdown.background_shutdown_token.clone();
        let scan = self
            .meta
            .run_maintenance(move |meta| {
                let mut directories = vec![(ROOT_INO, Vec::new())];
                let mut files = BTreeMap::new();
                let mut objects = HashSet::new();
                while let Some((ino, path)) = directories.pop() {
                    if cancellation.is_cancelled() {
                        break;
                    }
                    for entry in meta.readdir(ino)? {
                        if entry.kind == FileType::Directory {
                            let mut child = path.clone();
                            if !child.is_empty() {
                                child.push(b'/');
                            }
                            child.extend_from_slice(&entry.name);
                            directories.push((entry.ino, child));
                        }
                    }
                    let entry = match meta.lookup(ino, RULE_FILE) {
                        Ok(entry) => entry,
                        Err(studiofs_meta::MetaError::EntryNotFound { .. }) => continue,
                        Err(error) => return Err(error),
                    };
                    if entry.kind != FileType::RegularFile {
                        continue;
                    }
                    let attr = meta.get_attr(entry.ino)?;
                    objects.extend(
                        meta.get_regular_file_graph_object_usage(entry.ino)?
                            .into_iter()
                            .map(|(object_id, _)| object_id),
                    );
                    files.insert(path, (entry.ino, attr.content_epoch, attr.size));
                }
                Ok((files, objects))
            })
            .await;
        let (files, objects) = match scan {
            Ok(result) => result,
            Err(error) => {
                warn!(%error, "could not discover ignore rule files");
                return true;
            }
        };
        let cancellation = &self.state.shutdown.background_shutdown_token;
        if cancellation.is_cancelled() {
            return false;
        }

        // Upload ownership uses CachedStore's set. Hold a separate reference
        // on the backend so upload completion cannot drop the rule file's pin.
        for object_id in objects.difference(pinned) {
            self.local_payload_backend.pin(*object_id);
        }
        for object_id in pinned.difference(&objects) {
            self.local_payload_backend.unpin(*object_id);
        }
        *pinned = objects;

        let mut next_sources = sources.clone();
        let snapshot = Arc::clone(&self.ignore_rules.snapshot.read());
        let mut next_rules = (*snapshot).clone();
        let mut next_versions = versions.clone();
        let mut changed = Vec::new();
        for directory in sources.keys().filter(|path| !files.contains_key(*path)) {
            next_rules.remove(directory);
            next_sources.remove(directory);
            changed.push(directory.clone());
        }
        next_versions.retain(|path, _| files.contains_key(path));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut missing = Vec::new();
        for (directory, version @ (ino, _, size)) in files {
            if versions.get(&directory) == Some(&version) {
                continue;
            }
            if size > MAX_RULE_BYTES as u64 {
                warn!(?directory, "ignore rule file exceeds the size limit");
                next_versions.insert(directory, version);
                continue;
            }
            let read = tokio::select! {
                _ = cancellation.cancelled() => return false,
                read = tokio::time::timeout_at(deadline, self.read_ignore_rule_payload(ino, size)) => read,
            };
            let payload = match read {
                Ok(Ok(payload)) if payload.len() as u64 == size => payload,
                _ => {
                    missing.push(directory);
                    continue;
                }
            };
            match next_rules.update(&directory, &payload) {
                Ok(_) => {
                    if sources.get(&directory).is_none_or(|old| old != &payload) {
                        next_sources.insert(directory.clone(), payload);
                        changed.push(directory.clone());
                    }
                }
                Err(error) => warn!(%error, ?directory, "ignore rule update rejected"),
            }
            next_versions.insert(directory, version);
        }
        if !missing.is_empty() {
            warn!(?missing, "ignore rule payloads unavailable during refresh");
        }
        if !changed.is_empty() || !*self.ignore_rules.bootstrap.borrow() {
            let bytes = match postcard::to_allocvec(&next_sources) {
                Ok(bytes) => bytes,
                Err(error) => {
                    warn!(%error, "could not encode local ignore rules");
                    return true;
                }
            };
            if let Err(error) = self
                .meta
                .run_maintenance(move |meta| meta.store_ignore_rules_cache(&bytes))
                .await
            {
                warn!(%error, "could not persist local ignore rules");
                return true;
            }
            *self.ignore_rules.snapshot.write() = Arc::new(next_rules);
            *sources = next_sources;
            if !changed.is_empty() {
                let _ = self.ignore_rules.changed.send(changed);
            }
        }
        *versions = next_versions;
        !missing.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignore_rules_flush_publishes_snapshot_and_restores_after_reopen() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-");
        let (runtime, vfs, meta) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules");
        let mut changes = vfs.subscribe_ignore_rules_changes();
        let old = Arc::clone(&vfs.ignore_rules.snapshot.read());
        runtime.block_on(async {
            let worker = tokio::spawn(Arc::clone(&vfs).background_ignore_rules());
            tokio::time::timeout(
                Duration::from_secs(5),
                vfs.wait_for_ignore_rules_bootstrap(),
            )
            .await
            .expect("initial rule inventory");
            let attr = vfs.create(ROOT_INO, RULE_FILE, 0, 0, 0o644).unwrap();
            let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh(fh, attr.ino, 0, b"*.tmp\n").await.unwrap();
            vfs.flush_fh(fh).await.unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), changes.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                vec![Vec::<u8>::new()],
            );
            assert_eq!(old.classify(b"file.tmp", false), ScratchClass::Tracked);
            assert_eq!(
                vfs.classify_scratch_path(b"file.tmp", false),
                ScratchClass::Scratch
            );
            vfs.release_fh(fh).await.unwrap();
            vfs.state.shutdown.background_shutdown_token.cancel();
            tokio::time::timeout(Duration::from_secs(5), worker)
                .await
                .expect("rule worker joined")
                .unwrap();
        });
        drop(vfs);
        drop(meta);
        let (_, reopened, _) = super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules");
        assert_eq!(
            reopened.classify_scratch_path(b"file.tmp", false),
            ScratchClass::Scratch
        );
    }
}
