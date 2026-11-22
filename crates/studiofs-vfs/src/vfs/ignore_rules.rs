use super::*;
use crate::scratch_rules::{
    CompiledDirRules, MAX_RULE_BYTES, RULE_FILE, ScratchClass, ScratchRules,
};
use lru::LruCache;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use studiofs_meta::coherence::{InvalidationBatch, InvalidationIntent};
use tokio::sync::{Notify, broadcast, watch};

const DIR_RULE_CACHE_CAPACITY: usize = 4096;

type RuleSources = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Clone, PartialEq, Eq)]
struct RuleVersion {
    ino: Ino,
    content_epoch: u64,
    size: u64,
    objects: Vec<(u64, u64)>,
}

/// Mount bootstrap waits this long for the first discovery and prefetch pass.
const BOOTSTRAP_BUDGET: Duration = Duration::from_secs(5);
/// Shared payload read deadline for one refresh, inside the bootstrap budget.
const PAYLOAD_READ_BUDGET: Duration = Duration::from_secs(3);
/// Backoff before retrying a refresh that could not read every payload.
const RETRY_INTERVAL: Duration = Duration::from_secs(15);

/// Inodes whose changes can alter the compiled rules. Entries are added before
/// a scan reads their state, so a commit that a scan missed is still
/// recognized when its invalidation arrives. A completed scan drops the
/// inodes it no longer found unless a binding was noted meanwhile.
#[derive(Default)]
struct RuleInodes {
    files: HashSet<Ino>,
    /// Directories on the path to a rule file, including its own directory.
    directories: HashSet<Ino>,
    /// Rule files bound by a namespace change since the current scan began.
    bound_during_scan: HashSet<Ino>,
}

#[derive(Default)]
struct RulePins {
    objects: HashSet<u64>,
    revision: u64,
}

#[cfg(test)]
type PublicationBarrier = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);

struct CachedDirRules {
    rule_generation: u64,
    dir_rename_epoch: u64,
    compiled: Arc<CompiledDirRules>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RefreshOutcome {
    /// Every discovered rule file was loaded (or skipped as oversized) and any
    /// changes were persisted and published.
    Loaded,
    /// Metadata scan, payload read, or cache persistence failed; retry later.
    Retry,
    /// Background shutdown interrupted the refresh before it completed.
    Cancelled,
}

pub(super) struct IgnoreRules {
    backend: Arc<dyn LocalPayloadBackend>,
    pins: parking_lot::Mutex<RulePins>,
    #[cfg(test)]
    before_publish: parking_lot::Mutex<Option<PublicationBarrier>>,
    snapshot: parking_lot::RwLock<Arc<ScratchRules>>,
    rule_generation: AtomicU64,
    dir_rename_epoch: AtomicU64,
    write_activity_epoch: AtomicU64,
    opening_trunc_writers: parking_lot::Mutex<HashMap<Ino, u32>>,
    dir_rule_cache: parking_lot::Mutex<LruCache<Ino, CachedDirRules>>,
    sources: RuleSources,
    refresh: Notify,
    watched: Arc<parking_lot::Mutex<RuleInodes>>,
    changed: broadcast::Sender<Vec<Vec<u8>>>,
    bootstrap: watch::Sender<bool>,
    rules_loaded: watch::Sender<bool>,
    #[cfg(test)]
    refresh_passes: watch::Sender<u64>,
}

/// Keeps a watched `.studiofsignore` inode marked as open for writing across
/// `open_file(..., trunc=true)` between its `setattr(size=0)` and handle
/// registration in `HandleTable`.
pub(super) struct OpenTruncRuleGuard<'a> {
    rules: Option<&'a IgnoreRules>,
    ino: Ino,
    notify_on_drop: bool,
}

impl OpenTruncRuleGuard<'_> {
    /// Disarms the drop refresh notification once the write-capable handle is
    /// registered in `HandleTable` (its eventual close will notify instead).
    pub(super) fn disarm_notify(&mut self) {
        self.notify_on_drop = false;
    }
}

impl Drop for OpenTruncRuleGuard<'_> {
    fn drop(&mut self) {
        let Some(rules) = self.rules.take() else {
            return;
        };
        {
            let mut active = rules.opening_trunc_writers.lock();
            if let Some(count) = active.get_mut(&self.ino) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    active.remove(&self.ino);
                }
            }
        }
        rules.bump_write_activity_epoch();
        if self.notify_on_drop {
            rules.note_local_barrier(self.ino);
        }
    }
}

impl IgnoreRules {
    pub(super) fn load(meta: &dyn VfsMeta, backend: Arc<dyn LocalPayloadBackend>) -> Self {
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
        let state = Self {
            backend,
            pins: parking_lot::Mutex::default(),
            #[cfg(test)]
            before_publish: parking_lot::Mutex::default(),
            snapshot: parking_lot::RwLock::new(Arc::new(rules)),
            rule_generation: AtomicU64::new(0),
            dir_rename_epoch: AtomicU64::new(0),
            write_activity_epoch: AtomicU64::new(0),
            opening_trunc_writers: parking_lot::Mutex::default(),
            dir_rule_cache: parking_lot::Mutex::new(LruCache::new(
                NonZeroUsize::new(DIR_RULE_CACHE_CAPACITY).expect("non-zero dir rule cache cap"),
            )),
            sources,
            refresh: Notify::new(),
            watched: Arc::default(),
            changed: broadcast::channel(32).0,
            bootstrap: watch::channel(restored).0,
            rules_loaded: watch::channel(false).0,
            #[cfg(test)]
            refresh_passes: watch::channel(0).0,
        };
        'sources: for directory in state.sources.keys() {
            let mut parent = ROOT_INO;
            let mut watched = vec![parent];
            for component in directory
                .split(|byte| *byte == b'/')
                .filter(|part| !part.is_empty())
            {
                let Ok(entry) = meta.lookup(parent, component) else {
                    continue 'sources;
                };
                parent = entry.ino;
                watched.push(parent);
            }
            if let Ok(entry) = meta.lookup(parent, RULE_FILE)
                && entry.kind == FileType::RegularFile
            {
                let mut rule_inodes = state.watched.lock();
                rule_inodes.files.insert(entry.ino);
                rule_inodes.directories.extend(watched);
                drop(rule_inodes);
                state.pin_inode(meta, entry.ino);
            }
        }
        state
    }

    fn pin_inode(&self, meta: &dyn VfsMeta, ino: Ino) {
        match meta.get_regular_file_graph_object_usage(ino) {
            Ok(objects) => self.pin_objects(ino, objects.into_iter().map(|(id, _)| id)),
            Err(studiofs_meta::MetaError::InodeNotFound(_)) => {}
            Err(error) => warn!(%error, ino, "could not pin ignore rule payload"),
        }
    }

    pub(crate) fn pin_objects(&self, ino: Ino, objects: impl IntoIterator<Item = u64>) {
        if !self.watched.lock().files.contains(&ino) {
            return;
        }
        let mut pins = self.pins.lock();
        let mut inserted = false;
        for id in objects {
            if pins.objects.insert(id) {
                self.backend.pin(id);
                inserted = true;
            }
        }
        // Only a new pin can race a scan's inventory; a repeated pin must not
        // defer the scan's release of superseded payloads.
        if inserted {
            pins.revision = pins.revision.wrapping_add(1);
        }
    }

    /// Returns the currently published compiled rule snapshot.
    pub(super) fn current_rules(&self) -> Arc<ScratchRules> {
        self.snapshot.read().clone()
    }

    /// Request a refresh after a local flush or close of a known rule file.
    pub(super) fn note_local_barrier(&self, ino: Ino) {
        if self.watched.lock().files.contains(&ino) {
            self.refresh.notify_one();
        }
    }

    fn bump_write_activity_epoch(&self) {
        // Serialize writer activity with the refresh's final check and swap.
        let _snapshot = self.snapshot.read();
        self.write_activity_epoch.fetch_add(1, Ordering::AcqRel);
    }

    /// Record that a write-capable handle was opened on `ino`.
    pub(super) fn note_write_open(&self, ino: Ino) {
        if self.watched.lock().files.contains(&ino) {
            self.bump_write_activity_epoch();
        }
    }

    /// Record that the last write-capable handle on `ino` closed, requesting a
    /// refresh if `ino` is a watched rule file.
    pub(super) fn note_last_write_close(&self, ino: Ino) {
        if self.watched.lock().files.contains(&ino) {
            self.bump_write_activity_epoch();
            self.refresh.notify_one();
        }
    }

    /// Mark a watched rule file as open for writing while `open_file(..., trunc=true)`
    /// truncates it before inserting the handle into `HandleTable`.
    pub(super) fn begin_open_trunc(&self, ino: Ino) -> OpenTruncRuleGuard<'_> {
        if !self.watched.lock().files.contains(&ino) {
            return OpenTruncRuleGuard {
                rules: None,
                ino,
                notify_on_drop: false,
            };
        }
        self.bump_write_activity_epoch();
        *self.opening_trunc_writers.lock().entry(ino).or_default() += 1;
        OpenTruncRuleGuard {
            rules: Some(self),
            ino,
            notify_on_drop: true,
        }
    }

    fn has_opening_trunc_writer(&self, ino: Ino) -> bool {
        self.opening_trunc_writers.lock().contains_key(&ino)
    }

    /// Request a refresh after a local namespace change binds `name` to `ino`
    /// (`bound`) or removes `name`. `ino` is the inode moved or created, when
    /// known. Only a binding as a rule file watches and pins the inode.
    pub(super) fn note_local_entry(
        &self,
        meta: &dyn VfsMeta,
        name: &[u8],
        ino: Option<Ino>,
        bound: bool,
    ) {
        let relevant = self.watched.lock().note_entry(name, ino, bound);
        if relevant {
            self.bump_write_activity_epoch();
            if bound
                && let Some(ino) = ino
                && studiofs_meta::fold_dentry_name(name).as_ref() == RULE_FILE
            {
                self.pin_inode(meta, ino);
            }
            self.refresh.notify_one();
        }
    }

    /// Invalidate cached per-directory compiled rules after a local rename or
    /// a foreign entry change or reset. Removals need no bump: inode numbers
    /// are never reused, so a removed directory's cache slot is unreachable.
    pub(super) fn bump_dir_rename_epoch(&self) {
        self.dir_rename_epoch.fetch_add(1, Ordering::AcqRel);
    }

    /// Request a refresh when a committed batch names a rule file or moves a
    /// directory above one. Other namespace and content churn is ignored so
    /// ordinary commits do not rescan the volume.
    pub(super) fn note_invalidation(&self, meta: &dyn VfsMeta, batch: &InvalidationBatch) {
        let mut watched = self.watched.lock();
        let mut relevant = false;
        let mut bump_dir_epoch = false;
        let mut files = HashSet::new();
        for intent in &batch.intents {
            if matches!(
                intent,
                InvalidationIntent::Reset | InvalidationIntent::Entry { .. }
            ) {
                bump_dir_epoch = true;
            }
            relevant |= match intent {
                InvalidationIntent::Reset => true,
                // The pre-mutation inode of a foreign removal is also watched
                // until the refresh this requests finds it unbound.
                InvalidationIntent::Entry { name, ino, .. } => watched.note_entry(name, *ino, true),
                InvalidationIntent::Attributes { ino } | InvalidationIntent::Content { ino } => {
                    watched.files.contains(ino)
                }
                InvalidationIntent::Directory { .. } => false,
            };
            match intent {
                InvalidationIntent::Entry { ino: Some(ino), .. }
                | InvalidationIntent::Content { ino }
                | InvalidationIntent::Attributes { ino }
                    if watched.files.contains(ino) =>
                {
                    files.insert(*ino);
                }
                _ => {}
            }
        }
        drop(watched);
        if bump_dir_epoch {
            self.bump_dir_rename_epoch();
        }
        for ino in files {
            self.pin_inode(meta, ino);
        }
        if relevant {
            self.refresh.notify_one();
        }
    }
}

impl Drop for IgnoreRules {
    fn drop(&mut self) {
        for id in self.pins.get_mut().objects.drain() {
            self.backend.unpin(id);
        }
    }
}

impl RuleInodes {
    /// Whether a binding change of `name` to (`bound`) or from `ino` can alter
    /// the rules. Binding a rule file name starts watching its inode, and
    /// unbinding one stops; a scan rewatches an inode still bound elsewhere.
    fn note_entry(&mut self, name: &[u8], ino: Option<Ino>, bound: bool) -> bool {
        if studiofs_meta::fold_dentry_name(name).as_ref() == RULE_FILE {
            if let Some(ino) = ino {
                if bound {
                    self.files.insert(ino);
                    self.bound_during_scan.insert(ino);
                } else {
                    self.files.remove(&ino);
                    self.bound_during_scan.remove(&ino);
                }
            }
            return true;
        }
        ino.is_some_and(|ino| self.files.contains(&ino) || self.directories.contains(&ino))
    }

    /// Keep only what a completed scan found, plus rule files bound since it
    /// began.
    fn retain_scanned(&mut self, files: &HashSet<Ino>, directories: &HashSet<Ino>) {
        let bound = std::mem::take(&mut self.bound_during_scan);
        self.files
            .retain(|ino| files.contains(ino) || bound.contains(ino));
        self.directories.retain(|ino| directories.contains(ino));
    }
}

/// Resolve the canonical volume-relative byte path of a tracked directory.
///
/// Callers supply `deferred_step` when uncommitted deferred creates may still
/// sit ahead of committed metadata; offline callers pass `|_| None` and
/// `check_root_exists = true`. Returns `Ok(None)` when the directory is no
/// longer bound in metadata.
pub(super) fn resolve_tracked_directory_path_with<
    M: studiofs_meta::MetaInodes + studiofs_meta::MetaNamespace + ?Sized,
>(
    meta: &M,
    dir_ino: Ino,
    check_root_exists: bool,
    mut deferred_step: impl FnMut(Ino) -> Option<(Ino, Vec<u8>)>,
) -> VfsResult<Option<Vec<u8>>> {
    if dir_ino == ROOT_INO {
        if check_root_exists {
            return match meta.get_attr(ROOT_INO) {
                Ok(_) => Ok(Some(Vec::new())),
                Err(MetaError::InodeNotFound(_)) => Ok(None),
                Err(error) => Err(error.into()),
            };
        }
        return Ok(Some(Vec::new()));
    }

    let mut components: Vec<Vec<u8>> = Vec::new();
    let mut visited = HashSet::new();
    let mut current = dir_ino;
    while current != ROOT_INO {
        if current == 0 || components.len() >= 1024 || !visited.insert(current) {
            return Ok(None);
        }
        if let Some((parent, name)) = deferred_step(current) {
            if parent == current {
                return Ok(None);
            }
            components.push(name);
            current = parent;
            continue;
        }

        let attr = match meta.get_attr(current) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let parent = attr.parent;
        if parent == 0 || parent == current {
            return Ok(None);
        }
        let mut resolved_name = None;
        for (binding_parent, binding_name) in meta.entry_bindings_for_inode(current)? {
            if binding_parent != parent {
                continue;
            }
            match meta.lookup(parent, &binding_name) {
                Ok(entry) if entry.ino == current => {
                    resolved_name = Some(binding_name);
                    break;
                }
                Ok(_) | Err(MetaError::EntryNotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        let Some(name) = resolved_name else {
            return Ok(None);
        };
        components.push(name);
        current = parent;
    }

    components.reverse();
    let total_len: usize = components
        .iter()
        .map(Vec::len)
        .sum::<usize>()
        .saturating_add(components.len().saturating_sub(1));
    let mut path = Vec::with_capacity(total_len);
    for (idx, component) in components.into_iter().enumerate() {
        if idx > 0 {
            path.push(b'/');
        }
        path.extend_from_slice(&component);
    }
    Ok(Some(path))
}

impl StudioVfs {
    /// Returns the monotonically increasing ignore-rule snapshot generation.
    pub(super) fn ignore_rule_generation(&self) -> u64 {
        self.ignore_rules.rule_generation.load(Ordering::Acquire)
    }

    /// Classify a canonical volume-relative byte path using the current snapshot.
    pub fn classify_scratch_path(&self, path: &[u8], is_dir: bool) -> ScratchClass {
        let snapshot = Arc::clone(&self.ignore_rules.snapshot.read());
        snapshot.classify(path, is_dir)
    }

    /// Resolve the canonical volume-relative byte path of a tracked directory.
    ///
    /// Returns `Ok(None)` when the directory is no longer bound in metadata.
    pub(super) fn resolve_tracked_directory_path(
        &self,
        dir_ino: Ino,
    ) -> VfsResult<Option<Vec<u8>>> {
        let meta = self.sync_meta();
        resolve_tracked_directory_path_with(meta.as_ref(), dir_ino, false, |current| {
            // Check pending deferred namespace creates first so uncommitted directories
            // classify children with their real path before metadata settlement.
            let inner = self.state.namespace.deferred.inner.lock();
            inner.attrs.get(&current).and_then(|attr| {
                let parent = attr.parent;
                inner
                    .entries
                    .iter()
                    .find(|(key, ino)| **ino == current && key.parent == parent)
                    .map(|(key, _)| (parent, key.name.as_ref().to_vec()))
            })
        })
    }

    /// Classify a new entry being created under `parent`.
    ///
    /// Scratch directories always produce scratch descendants without rule
    /// lookup. Tracked parents use a per-directory compiled rule cache keyed by
    /// `(rule_generation, dir_rename_epoch)`.
    pub(super) fn classify_new_entry(
        &self,
        parent: Ino,
        name: &[u8],
        is_dir: bool,
    ) -> VfsResult<ScratchClass> {
        // Reserved trash roots keep the existing discard/retained namespace routing.
        if parent == ROOT_INO && super::is_probable_trash_component(name) {
            return Ok(ScratchClass::Tracked);
        }
        if self.is_scratch_inode(parent)? {
            return Ok(ScratchClass::Scratch);
        }
        if studiofs_meta::fold_dentry_name(name).as_ref() == RULE_FILE {
            return Ok(ScratchClass::Tracked);
        }

        let rule_gen = self.ignore_rules.rule_generation.load(Ordering::Acquire);
        let rename_epoch = self.ignore_rules.dir_rename_epoch.load(Ordering::Acquire);
        let cached = {
            let mut cache = self.ignore_rules.dir_rule_cache.lock();
            cache
                .get(&parent)
                .filter(|entry| {
                    entry.rule_generation == rule_gen && entry.dir_rename_epoch == rename_epoch
                })
                .map(|entry| Arc::clone(&entry.compiled))
        };
        if let Some(compiled) = cached {
            return Ok(compiled.classify_child(name, is_dir));
        }

        let observed_rename_epoch = self.ignore_rules.dir_rename_epoch.load(Ordering::Acquire);
        let dir_path = self.resolve_tracked_directory_path(parent)?;
        let (snapshot, observed_rule_gen) = {
            let guard = self.ignore_rules.snapshot.read();
            let current_rule_gen = self.ignore_rules.rule_generation.load(Ordering::Acquire);
            (Arc::clone(&guard), current_rule_gen)
        };
        let compiled = Arc::new(snapshot.compile_directory(dir_path.as_deref().unwrap_or(b"")));
        if dir_path.is_some()
            && self.ignore_rules.dir_rename_epoch.load(Ordering::Acquire) == observed_rename_epoch
        {
            self.ignore_rules.dir_rule_cache.lock().put(
                parent,
                CachedDirRules {
                    rule_generation: observed_rule_gen,
                    dir_rename_epoch: observed_rename_epoch,
                    compiled: Arc::clone(&compiled),
                },
            );
        }
        Ok(compiled.classify_child(name, is_dir))
    }

    /// Receive volume-relative directories whose rules were replaced or removed.
    pub fn subscribe_ignore_rules_changes(&self) -> broadcast::Receiver<Vec<Vec<u8>>> {
        self.ignore_rules.changed.subscribe()
    }

    /// Observe whether the background rule worker has completed a full rule load
    /// at least once since mount.
    pub(super) fn subscribe_ignore_rules_loaded(&self) -> watch::Receiver<bool> {
        self.ignore_rules.rules_loaded.subscribe()
    }

    #[cfg(test)]
    pub(super) fn trigger_ignore_rules_refresh_for_test(&self) {
        self.ignore_rules.refresh.notify_one();
    }

    #[cfg(test)]
    pub(super) fn subscribe_ignore_rules_refresh_passes_for_test(&self) -> watch::Receiver<u64> {
        self.ignore_rules.refresh_passes.subscribe()
    }

    /// Returns whether a rule file currently has a local write-capable open
    /// handle, an in-flight `open(..., trunc=true)`, or unflushed local dirty
    /// state from a handle that is still closing.
    fn is_ignore_rule_inode_open_for_writing(&self, ino: Ino) -> bool {
        self.handle_table.write_open_count(ino) > 0
            || self.ignore_rules.has_opening_trunc_writer(ino)
            || self.inode_has_pending_local_state(ino)
    }

    /// Wait for the initial prefetch pass before exposing the mount.
    pub async fn wait_for_ignore_rules_bootstrap(&self) {
        if !self.read_only_mount {
            let mut bootstrap = self.ignore_rules.bootstrap.subscribe();
            if tokio::time::timeout(BOOTSTRAP_BUDGET, bootstrap.wait_for(|ready| *ready))
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
        let mut cache_needs_persist = false;
        let cancellation = &self.state.shutdown.background_shutdown_token;
        loop {
            let outcome = self
                .refresh_ignore_rules(&mut sources, &mut versions, &mut cache_needs_persist)
                .await;
            if outcome == RefreshOutcome::Loaded {
                self.ignore_rules.rules_loaded.send_if_modified(|loaded| {
                    let changed = !*loaded;
                    *loaded = true;
                    changed
                });
            }
            self.ignore_rules.bootstrap.send_replace(true);
            #[cfg(test)]
            self.ignore_rules
                .refresh_passes
                .send_modify(|passes| *passes = passes.saturating_add(1));
            let retry = outcome == RefreshOutcome::Retry;
            tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = self.ignore_rules.refresh.notified() => {}
                _ = tokio::time::sleep(RETRY_INTERVAL), if retry => {}
            }
        }
        self.ignore_rules.bootstrap.send_replace(true);
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
        versions: &mut BTreeMap<Vec<u8>, RuleVersion>,
        cache_needs_persist: &mut bool,
    ) -> RefreshOutcome {
        let cancellation = self.state.shutdown.background_shutdown_token.clone();
        let watched = Arc::clone(&self.ignore_rules.watched);
        watched.lock().bound_during_scan.clear();
        let write_epoch_before = self
            .ignore_rules
            .write_activity_epoch
            .load(Ordering::Acquire);
        let pin_revision = self.ignore_rules.pins.lock().revision;
        let scan = self
            .meta
            .run_maintenance(move |meta| {
                use studiofs_meta::MetaError;
                let mut directories = vec![(ROOT_INO, Vec::new())];
                let mut parents = HashMap::new();
                let mut files = BTreeMap::new();
                let mut objects = HashSet::new();
                let mut scanned = (HashSet::new(), HashSet::new());
                while let Some((ino, path)) = directories.pop() {
                    if cancellation.is_cancelled() {
                        break;
                    }
                    // A directory removed after its parent was listed has no
                    // rules; its invalidation requests another pass if needed.
                    let entries = match meta.readdir(ino) {
                        Ok(entries) => entries,
                        Err(MetaError::InodeNotFound(_)) => continue,
                        Err(error) => return Err(error),
                    };
                    for entry in entries {
                        if entry.kind == FileType::Directory {
                            let mut child = path.clone();
                            if !child.is_empty() {
                                child.push(b'/');
                            }
                            child.extend_from_slice(&entry.name);
                            parents.insert(entry.ino, ino);
                            directories.push((entry.ino, child));
                        }
                    }
                    let entry = match meta.lookup(ino, RULE_FILE) {
                        Ok(entry) => entry,
                        Err(MetaError::EntryNotFound { .. } | MetaError::InodeNotFound(_)) => {
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    if entry.kind != FileType::RegularFile {
                        continue;
                    }
                    // Watch before reading so a later commit is either seen
                    // here or recognized by its invalidation.
                    {
                        let mut watched = watched.lock();
                        watched.files.insert(entry.ino);
                        scanned.0.insert(entry.ino);
                        let mut directory = Some(ino);
                        while let Some(current) = directory {
                            watched.directories.insert(current);
                            scanned.1.insert(current);
                            directory = parents.get(&current).copied();
                        }
                    }
                    let attr = match meta.get_attr(entry.ino) {
                        Ok(attr) => attr,
                        Err(MetaError::InodeNotFound(_)) => continue,
                        Err(error) => return Err(error),
                    };
                    let file_objects = meta.get_regular_file_graph_object_usage(entry.ino)?;
                    // An oversized file is never loaded, so it gets no eviction exemption.
                    if attr.size <= MAX_RULE_BYTES as u64 {
                        objects.extend(file_objects.iter().map(|(object_id, _)| *object_id));
                    }
                    files.insert(
                        path,
                        RuleVersion {
                            ino: entry.ino,
                            content_epoch: attr.content_epoch,
                            size: attr.size,
                            // A same-size rewrite can share an unpublished content epoch.
                            objects: file_objects,
                        },
                    );
                }
                Ok((files, objects, scanned))
            })
            .await;
        let (files, objects, (scanned_files, scanned_directories)) = match scan {
            Ok(result) => result,
            Err(error) => {
                warn!(%error, "could not discover ignore rule files");
                return RefreshOutcome::Retry;
            }
        };
        let cancellation = &self.state.shutdown.background_shutdown_token;
        if cancellation.is_cancelled() {
            return RefreshOutcome::Cancelled;
        }
        self.ignore_rules
            .watched
            .lock()
            .retain_scanned(&scanned_files, &scanned_directories);

        {
            let mut pins = self.ignore_rules.pins.lock();
            // A namespace or upload submission can pin newer objects during discovery.
            if pin_revision == pins.revision {
                for id in pins.objects.difference(&objects) {
                    self.ignore_rules.backend.unpin(*id);
                }
                pins.objects.retain(|id| objects.contains(id));
            }
            for id in objects {
                if pins.objects.insert(id) {
                    self.ignore_rules.backend.pin(id);
                }
            }
        }

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

        let deadline = tokio::time::Instant::now() + PAYLOAD_READ_BUDGET;
        let mut missing = Vec::new();
        let mut rescan_after_write_activity = false;
        for (directory, version @ RuleVersion { ino, size, .. }) in files {
            if versions.get(&directory) == Some(&version) {
                continue;
            }
            // Keep the last compiled rule set while a local write-capable handle
            // is open (or still draining its close barrier) so in-place rewrites
            // (truncate, then write) never publish an empty or partial rule set.
            if self.is_ignore_rule_inode_open_for_writing(ino) {
                continue;
            }
            if self
                .ignore_rules
                .write_activity_epoch
                .load(Ordering::Acquire)
                != write_epoch_before
            {
                rescan_after_write_activity = true;
                continue;
            }
            if size > MAX_RULE_BYTES as u64 {
                warn!(?directory, "ignore rule file exceeds the size limit");
                next_versions.insert(directory, version);
                continue;
            }
            let read = tokio::select! {
                _ = cancellation.cancelled() => return RefreshOutcome::Cancelled,
                read = tokio::time::timeout_at(deadline, self.read_ignore_rule_payload(ino, size)) => read,
            };
            let payload = match read {
                Ok(Ok(payload)) if payload.len() as u64 == size => payload,
                _ => {
                    missing.push(directory);
                    continue;
                }
            };
            if self.is_ignore_rule_inode_open_for_writing(ino) {
                continue;
            }
            if self
                .ignore_rules
                .write_activity_epoch
                .load(Ordering::Acquire)
                != write_epoch_before
            {
                rescan_after_write_activity = true;
                continue;
            }
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
        if rescan_after_write_activity {
            self.ignore_rules.refresh.notify_one();
        }
        if !missing.is_empty() {
            warn!(?missing, "ignore rule payloads unavailable during refresh");
        }
        if !changed.is_empty() || *cache_needs_persist || !*self.ignore_rules.bootstrap.borrow() {
            let bytes = match postcard::to_allocvec(&next_sources) {
                Ok(bytes) => bytes,
                Err(error) => {
                    warn!(%error, "could not encode local ignore rules");
                    return RefreshOutcome::Retry;
                }
            };
            // A discarded candidate may already have replaced the persisted
            // cache. Rewrite it even if the next pass has no live changes.
            *cache_needs_persist = true;
            if let Err(error) = self
                .meta
                .run_maintenance(move |meta| meta.store_ignore_rules_cache(&bytes))
                .await
            {
                warn!(%error, "could not persist local ignore rules");
                return RefreshOutcome::Retry;
            }
            #[cfg(test)]
            {
                let barrier = self.ignore_rules.before_publish.lock().take();
                if let Some((entered, resume)) = barrier {
                    let _ = entered.send(());
                    if !matches!(
                        tokio::time::timeout(BOOTSTRAP_BUDGET, resume).await,
                        Ok(Ok(()))
                    ) {
                        return RefreshOutcome::Retry;
                    }
                }
            }
            {
                let mut snapshot_guard = self.ignore_rules.snapshot.write();
                // A writer can reopen while the cache is being persisted. Keep
                // the live rules and versions so the next pass can reconsider.
                if self
                    .ignore_rules
                    .write_activity_epoch
                    .load(Ordering::Acquire)
                    != write_epoch_before
                {
                    drop(snapshot_guard);
                    self.ignore_rules.refresh.notify_one();
                    return RefreshOutcome::Retry;
                }
                *snapshot_guard = Arc::new(next_rules);
                self.ignore_rules
                    .rule_generation
                    .fetch_add(1, Ordering::Release);
            }
            *sources = next_sources;
            *cache_needs_persist = false;
            if !changed.is_empty() {
                let _ = self.ignore_rules.changed.send(changed);
            }
        }
        *versions = next_versions;
        if missing.is_empty() {
            RefreshOutcome::Loaded
        } else {
            RefreshOutcome::Retry
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::future::Future;
    use std::task::{Context, Waker};
    use studiofs_meta::{MetaEngineCore, MetaNamespace, MetaPayloadObjects};

    #[test]
    fn ignore_rules_preserve_reserved_trash_root_routing() {
        use studiofs_meta::MetaFsOps as _;
        use studiofs_meta::trash::MetaVolumeSettings as _;

        for trash_days in [0, 7] {
            let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-trash-root-");
            let (_runtime, vfs, meta) =
                super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules-trash-root");
            if trash_days != 0 {
                meta.compare_and_set_trash_days(0, trash_days)
                    .unwrap()
                    .unwrap();
            }
            let mut rules = ScratchRules::new(true);
            rules.update(b"", b"**/\n").unwrap();
            *vfs.ignore_rules.snapshot.write() = Arc::new(rules);

            let parent = meta.create_dir(ROOT_INO, b"work", 0, 0, 0o755).unwrap();
            for name in [
                b"$RECYCLE.BIN".as_slice(),
                b"$recycle.bin",
                b".Trash",
                b".Trashes",
                b".Trash-1000",
            ] {
                assert_eq!(
                    vfs.classify_new_entry(ROOT_INO, name, true).unwrap(),
                    ScratchClass::Tracked,
                    "reserved root {name:?}, retention {trash_days}"
                );
                assert_eq!(
                    vfs.classify_new_entry(parent.ino, name, true).unwrap(),
                    ScratchClass::Scratch,
                    "ordinary nested name {name:?}"
                );
            }
            let trash = vfs.mkdir(ROOT_INO, b"$RECYCLE.BIN", 0, 0, 0o755).unwrap();
            assert_eq!(
                meta.lookup(ROOT_INO, b"$RECYCLE.BIN").unwrap().ino,
                trash.ino
            );
            assert!(
                meta.lookup_scratch_entry(ROOT_INO, b"$RECYCLE.BIN")
                    .unwrap()
                    .is_none()
            );
        }
    }

    fn take_refresh_permit(rules: &IgnoreRules) -> bool {
        let mut notified = std::pin::pin!(rules.refresh.notified());
        notified
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    }

    async fn write_rule_file(vfs: &StudioVfs, parent: Ino, payload: &[u8]) -> Ino {
        let attr = vfs.create(parent, RULE_FILE, 0, 0, 0o644).unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        vfs.write_fh(fh, attr.ino, 0, payload).await.unwrap();
        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
        attr.ino
    }

    /// Wait on change notifications until `path` has `class`, and return the
    /// directories announced meanwhile. Each wait is bounded.
    async fn wait_for_class(
        vfs: &StudioVfs,
        changes: &mut broadcast::Receiver<Vec<Vec<u8>>>,
        path: &[u8],
        class: ScratchClass,
    ) -> BTreeSet<Vec<u8>> {
        let mut announced = BTreeSet::new();
        while vfs.classify_scratch_path(path, false) != class {
            let paths = tokio::time::timeout(Duration::from_secs(5), changes.recv())
                .await
                .expect("rules changed notification")
                .unwrap();
            announced.extend(paths);
        }
        announced
    }

    /// Waits for `path` to reach `class` and for notifications naming every
    /// `expected` directory. The snapshot is swapped before the notification is
    /// sent, so the class alone can change before its notification arrives.
    async fn wait_for_announced(
        vfs: &StudioVfs,
        changes: &mut broadcast::Receiver<Vec<Vec<u8>>>,
        path: &[u8],
        class: ScratchClass,
        expected: &BTreeSet<Vec<u8>>,
    ) -> BTreeSet<Vec<u8>> {
        let mut announced = wait_for_class(vfs, changes, path, class).await;
        while !announced.is_superset(expected) {
            let paths = tokio::time::timeout(Duration::from_secs(5), changes.recv())
                .await
                .expect("rules changed notification")
                .unwrap();
            announced.extend(paths);
        }
        announced
    }

    #[test]
    fn ignore_rules_refresh_only_for_rule_files_and_their_directories() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-");
        let (_runtime, vfs, meta) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules-filter");
        let rules = &vfs.ignore_rules;
        {
            let mut watched = rules.watched.lock();
            watched.files.insert(10);
            watched.directories.insert(2);
        }
        let batch = |intents| InvalidationBatch {
            generation: 1,
            intents,
        };
        let entry = |parent, name: &[u8], ino| InvalidationIntent::Entry {
            parent,
            name: name.to_vec(),
            ancestors: None,
            ino,
        };

        // Children created in a watched directory touch its attributes only.
        rules.note_invalidation(
            meta.as_ref(),
            &batch(vec![
                entry(2, b"render.exr", Some(11)),
                InvalidationIntent::Directory { ino: 2 },
                InvalidationIntent::Attributes { ino: 2 },
                InvalidationIntent::Content { ino: 11 },
            ]),
        );
        assert!(!take_refresh_permit(rules));
        rules.note_local_barrier(11);
        assert!(!take_refresh_permit(rules));

        rules.note_local_barrier(10);
        assert!(take_refresh_permit(rules));
        rules.note_invalidation(
            meta.as_ref(),
            &batch(vec![InvalidationIntent::Content { ino: 10 }]),
        );
        assert!(take_refresh_permit(rules));
        rules.note_invalidation(
            meta.as_ref(),
            &batch(vec![entry(ROOT_INO, b"shots", Some(2))]),
        );
        assert!(take_refresh_permit(rules));
        rules.note_invalidation(meta.as_ref(), &batch(vec![InvalidationIntent::Reset]));
        assert!(take_refresh_permit(rules));

        // A new rule file, in any case spelling, is watched from its first intent.
        rules.note_invalidation(
            meta.as_ref(),
            &batch(vec![entry(7, b".StudioFSIgnore", Some(12))]),
        );
        assert!(take_refresh_permit(rules));
        rules.note_local_barrier(12);
        assert!(take_refresh_permit(rules));
    }

    /// WinFSP writes a rule file through the volatile writeback flush and the deferred
    /// last-handle release; the new rules must load without a remount.
    #[test]
    fn ignore_rules_refresh_after_volatile_flush_and_deferred_release() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-winfsp-");
        let (runtime, vfs, _meta) = super::super::tests::make_unstarted_test_vfs_at(
            tmp.path(),
            "ignore-rules-winfsp",
            crate::VfsConfig::default(),
            None,
        );
        let _guard = runtime.enter();
        vfs.start().unwrap();
        let mut changes = vfs.subscribe_ignore_rules_changes();
        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(5),
                vfs.wait_for_ignore_rules_bootstrap(),
            )
            .await
            .expect("initial rule inventory");
            let attr = vfs
                .create_with_deferred_metadata_admission(
                    ROOT_INO,
                    RULE_FILE,
                    0,
                    0,
                    0o644,
                    &tokio::runtime::Handle::current(),
                )
                .unwrap();
            let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh(fh, attr.ino, 0, b"*.tmp\n").await.unwrap();
            vfs.flush_volatile_writeback_with_outcome(attr.ino, fh)
                .await
                .unwrap();
            vfs.release_fh_with_deferred_last_handle_barrier(
                fh,
                &tokio::runtime::Handle::current(),
            );
            wait_for_class(&vfs, &mut changes, b"file.tmp", ScratchClass::Scratch).await;
            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
    }

    /// A WinFSP write-through handle commits the rule file inside the write callback,
    /// but the rule loader keeps the previous rules until the write handle closes.
    #[test]
    fn ignore_rules_refresh_after_write_through_write() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-wt-");
        let (runtime, vfs, _meta) = super::super::tests::make_unstarted_test_vfs_at(
            tmp.path(),
            "ignore-rules-wt",
            crate::VfsConfig::default(),
            None,
        );
        let _guard = runtime.enter();
        vfs.start().unwrap();
        let mut changes = vfs.subscribe_ignore_rules_changes();
        let mut passes = vfs.subscribe_ignore_rules_refresh_passes_for_test();
        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(5),
                vfs.wait_for_ignore_rules_bootstrap(),
            )
            .await
            .expect("initial rule inventory");
            let attr = vfs
                .create_with_deferred_metadata_admission(
                    ROOT_INO,
                    RULE_FILE,
                    0,
                    0,
                    0o644,
                    &tokio::runtime::Handle::current(),
                )
                .unwrap();
            let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
            let pass_before = *passes.borrow();
            vfs.write_fh_with_deferred_materialize(
                fh,
                attr.ino,
                0,
                b"*.tmp\n",
                crate::LocalSyncPolicy::WriteThrough,
            )
            .await
            .unwrap();
            tokio::time::timeout(
                Duration::from_secs(5),
                passes.wait_for(|count| *count > pass_before),
            )
            .await
            .expect("refresh pass while write-through handle is open")
            .unwrap();
            assert_eq!(
                vfs.classify_scratch_path(b"file.tmp", false),
                ScratchClass::Tracked,
                "rules must not load while write-through handle is still open"
            );
            vfs.release_fh(fh).await.unwrap();
            wait_for_class(&vfs, &mut changes, b"file.tmp", ScratchClass::Scratch).await;
            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
    }

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
            write_rule_file(&vfs, ROOT_INO, b"*.tmp\n").await;
            let expected = BTreeSet::from([Vec::new()]);
            let announced = wait_for_announced(
                &vfs,
                &mut changes,
                b"file.tmp",
                ScratchClass::Scratch,
                &expected,
            )
            .await;
            assert_eq!(announced, expected);
            // A snapshot taken before the refresh is never mutated in place.
            assert_eq!(old.classify(b"file.tmp", false), ScratchClass::Tracked);
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

    #[test]
    fn ignore_rules_defer_publication_when_writer_reopens() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-");
        let (runtime, vfs, meta) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules-reopen");
        runtime.block_on(async {
            let ino = write_rule_file(&vfs, ROOT_INO, b"*.old\n").await;
            let mut changes = vfs.subscribe_ignore_rules_changes();
            let worker = tokio::spawn(Arc::clone(&vfs).background_ignore_rules());
            wait_for_class(&vfs, &mut changes, b"a.old", ScratchClass::Scratch).await;

            let (entered, reached) = tokio::sync::oneshot::channel();
            let (resume, paused) = tokio::sync::oneshot::channel();
            *vfs.ignore_rules.before_publish.lock() = Some((entered, paused));
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh(fh, ino, 0, b"*.new\n").await.unwrap();
            vfs.release_fh(fh).await.unwrap();
            tokio::time::timeout(BOOTSTRAP_BUDGET, reached)
                .await
                .unwrap()
                .unwrap();

            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            let mut passes = vfs.subscribe_ignore_rules_refresh_passes_for_test();
            let paused_pass = *passes.borrow_and_update();
            resume.send(()).unwrap();
            tokio::time::timeout(
                BOOTSTRAP_BUDGET,
                passes.wait_for(|count| *count > paused_pass),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                vfs.classify_scratch_path(b"a.old", false),
                ScratchClass::Scratch
            );
            assert_eq!(
                vfs.classify_scratch_path(b"a.new", false),
                ScratchClass::Tracked
            );
            tokio::time::timeout(
                BOOTSTRAP_BUDGET,
                passes.wait_for(|count| *count >= paused_pass + 2),
            )
            .await
            .unwrap()
            .unwrap();
            let persisted = meta.load_ignore_rules_cache().unwrap().unwrap();
            let persisted: RuleSources = postcard::from_bytes(&persisted).unwrap();
            assert_eq!(persisted[&Vec::new()], b"*.old\n");

            vfs.release_fh(fh).await.unwrap();
            wait_for_class(&vfs, &mut changes, b"a.new", ScratchClass::Scratch).await;
            vfs.state.shutdown.background_shutdown_token.cancel();
            tokio::time::timeout(BOOTSTRAP_BUDGET, worker)
                .await
                .unwrap()
                .unwrap();
        });
    }

    #[test]
    fn ignore_rules_classify_while_recompile_is_ready_to_publish() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-");
        let (runtime, vfs, _) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules-recompile");
        let classes = || {
            [b"a.tmp".as_slice(), b"shots/b.log"].map(|path| vfs.classify_scratch_path(path, false))
        };
        runtime.block_on(async {
            let ino = write_rule_file(&vfs, ROOT_INO, b"*.old\nshots/*.old\n").await;
            let mut changes = vfs.subscribe_ignore_rules_changes();
            let worker = tokio::spawn(Arc::clone(&vfs).background_ignore_rules());
            wait_for_class(&vfs, &mut changes, b"a.old", ScratchClass::Scratch).await;

            let (entered, reached) = tokio::sync::oneshot::channel();
            let (resume, paused) = tokio::sync::oneshot::channel();
            *vfs.ignore_rules.before_publish.lock() = Some((entered, paused));
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh(fh, ino, 0, b"*.tmp\nshots/*.log\n")
                .await
                .unwrap();
            vfs.flush_fh(fh).await.unwrap();
            vfs.release_fh(fh).await.unwrap();
            tokio::time::timeout(BOOTSTRAP_BUDGET, reached)
                .await
                .unwrap()
                .unwrap();

            assert_eq!(classes(), [ScratchClass::Tracked; 2]);
            assert_eq!(
                vfs.classify_scratch_path(b"a.old", false),
                ScratchClass::Scratch
            );
            assert_eq!(
                vfs.classify_scratch_path(b"shots/b.old", false),
                ScratchClass::Scratch
            );
            resume.send(()).unwrap();
            tokio::time::timeout(BOOTSTRAP_BUDGET, changes.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(classes(), [ScratchClass::Scratch; 2]);
            assert_eq!(
                vfs.classify_scratch_path(b"a.old", false),
                ScratchClass::Tracked
            );
            assert_eq!(
                vfs.classify_scratch_path(b"shots/b.old", false),
                ScratchClass::Tracked
            );

            vfs.state.shutdown.background_shutdown_token.cancel();
            tokio::time::timeout(BOOTSTRAP_BUDGET, worker)
                .await
                .unwrap()
                .unwrap();
        });
    }

    fn assert_rule_survives_eviction(vfs: &StudioVfs, object_id: u64) {
        let backend = &vfs.local_payload_backend;
        let payload = backend.get(object_id).expect("local rule payload");
        // Disk pressure only considers downloaded payloads.
        backend.put(object_id, &payload, true).unwrap();
        backend
            .put(u64::MAX - 4, b"downloaded control", true)
            .unwrap();
        backend
            .as_any()
            .downcast_ref::<studiofs_cache::PersistentLocalPayloadBackend>()
            .unwrap()
            .run_low_disk_space_for_test()
            .unwrap();
        assert!(
            !backend.contains(u64::MAX - 4),
            "disk-pressure eviction must run"
        );
        assert!(
            backend.contains(object_id),
            "disk-pressure eviction removed the rule"
        );

        let pressure = vec![0x77; 32 * 1024 * 1024];
        for id in [u64::MAX - 3, u64::MAX - 2, u64::MAX - 1] {
            backend.put(id, &pressure, true).unwrap();
        }
        assert!(!backend.contains(u64::MAX - 3), "budget eviction must run");
        assert!(
            backend.contains(object_id),
            "budget eviction removed the rule"
        );
    }

    #[test]
    fn ignore_rules_pins_and_classifies_after_process_crash() {
        use std::process::{Command, Stdio};

        const CHILD_ROOT: &str = "STUDIOFS_IGNORE_RULES_CRASH_TEST_ROOT";
        let open = |root: &std::path::Path| {
            super::super::tests::make_unstarted_test_vfs_at(
                root,
                "ignore-rules-crash",
                VfsConfig {
                    max_dirty_bytes: 8 * 1024 * 1024,
                    write_eager_flush_batch_bytes: 4 * 1024 * 1024,
                    ..VfsConfig::default()
                },
                None,
            )
        };
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            let root = PathBuf::from(root);
            let (runtime, vfs, meta) = open(&root);
            runtime.block_on(async {
                write_rule_file(&vfs, ROOT_INO, b"*.tmp\n").await;
                let ino = meta.lookup(ROOT_INO, RULE_FILE).unwrap().ino;
                let uploads = vfs.pending_uploads_snapshot_for_inode(ino);
                assert_eq!(uploads.len(), 1);
                let upload = &uploads[0];
                vfs.local_payload_backend
                    .mark_uploaded(upload.object_id)
                    .unwrap();
                vfs.cached_store.complete_upload(upload);
                assert_rule_survives_eviction(&vfs, upload.object_id);

                let mut changes = vfs.subscribe_ignore_rules_changes();
                let worker = tokio::spawn(Arc::clone(&vfs).background_ignore_rules());
                wait_for_class(&vfs, &mut changes, b"a.tmp", ScratchClass::Scratch).await;
                assert!(meta.load_ignore_rules_cache().unwrap().is_some());
                std::fs::write(root.join("ready"), []).unwrap();
                // The parent kills this process while its VFS and metadata owners are live.
                tokio::time::sleep(Duration::from_secs(30)).await;
                worker.abort();
                let _ = worker.await;
            });
            panic!("parent did not terminate crash fixture");
        }

        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-crash-");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "vfs::ignore_rules::tests::ignore_rules_pins_and_classifies_after_process_crash",
                "--nocapture",
            ])
            .env(CHILD_ROOT, tmp.path())
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let ready = loop {
            if tmp.path().join("ready").is_file() {
                break true;
            }
            if child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                break false;
            }
            std::thread::park_timeout(Duration::from_millis(10));
        };
        let killed = child.kill();
        let status = child.wait().unwrap();
        assert!(ready, "crash fixture did not persist rules: {status}");
        killed.unwrap();
        assert!(!status.success());

        let (_runtime, vfs, meta) = open(tmp.path());
        assert_eq!(
            vfs.classify_scratch_path(b"a.tmp", false),
            ScratchClass::Scratch
        );
        let ino = meta.lookup(ROOT_INO, RULE_FILE).unwrap().ino;
        let objects = meta.get_regular_file_graph_object_usage(ino).unwrap();
        assert_eq!(objects.len(), 1);
        assert_rule_survives_eviction(&vfs, objects[0].0);
    }

    /// Local rename and unlink of a rule file, and a move of its directory,
    /// trigger a refresh without any flush. The worker is then abandoned
    /// without shutdown and the metadata cache reopened.
    #[test]
    fn ignore_rules_follow_rule_file_rename_unlink_and_survive_abandoned_worker() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-");
        let (runtime, vfs, meta) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules-moves");
        let mut changes = vfs.subscribe_ignore_rules_changes();
        runtime.block_on(async {
            let worker = tokio::spawn(Arc::clone(&vfs).background_ignore_rules());
            vfs.wait_for_ignore_rules_bootstrap().await;
            write_rule_file(&vfs, ROOT_INO, b"*.tmp\n").await;
            wait_for_class(&vfs, &mut changes, b"file.tmp", ScratchClass::Scratch).await;

            vfs.rename(ROOT_INO, RULE_FILE, ROOT_INO, b"rules.off", false)
                .unwrap();
            wait_for_class(&vfs, &mut changes, b"file.tmp", ScratchClass::Tracked).await;

            vfs.rename(ROOT_INO, b"rules.off", ROOT_INO, RULE_FILE, false)
                .unwrap();
            wait_for_class(&vfs, &mut changes, b"file.tmp", ScratchClass::Scratch).await;

            let shots = vfs.mkdir(ROOT_INO, b"shots", 0, 0, 0o755).unwrap();
            write_rule_file(&vfs, shots.ino, b"*.log\n").await;
            wait_for_announced(
                &vfs,
                &mut changes,
                b"shots/a.log",
                ScratchClass::Scratch,
                &BTreeSet::from([b"shots".to_vec()]),
            )
            .await;
            // Drop notifications left over from the earlier steps.
            while changes.try_recv().is_ok() {}

            // Moving the directory moves its rules with it.
            vfs.rename(ROOT_INO, b"shots", ROOT_INO, b"plates", false)
                .unwrap();
            let expected = BTreeSet::from([b"plates".to_vec(), b"shots".to_vec()]);
            let announced = wait_for_announced(
                &vfs,
                &mut changes,
                b"plates/a.log",
                ScratchClass::Scratch,
                &expected,
            )
            .await;
            assert_eq!(announced, expected);
            assert_eq!(
                vfs.classify_scratch_path(b"shots/a.log", false),
                ScratchClass::Tracked
            );

            vfs.unlink(ROOT_INO, RULE_FILE).unwrap();
            wait_for_class(&vfs, &mut changes, b"file.tmp", ScratchClass::Tracked).await;

            worker.abort();
            assert!(worker.await.unwrap_err().is_cancelled());
        });
        drop(vfs);
        drop(meta);
        drop(runtime);
        let (_, reopened, _) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules-moves");
        assert_eq!(
            reopened.classify_scratch_path(b"plates/a.log", false),
            ScratchClass::Scratch
        );
        assert_eq!(
            reopened.classify_scratch_path(b"file.tmp", false),
            ScratchClass::Tracked
        );
    }

    #[test]
    fn ignore_rules_pin_foreign_payload_before_cache_admission() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-");
        let (runtime, vfs, meta) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules-foreign-admission");
        runtime.block_on(async {
            let payload = b"*.tmp\n";
            let ino = vfs.create(ROOT_INO, b"rules.off", 0, 0, 0o644).unwrap().ino;
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh(fh, ino, 0, payload).await.unwrap();
            vfs.flush_fh(fh).await.unwrap();
            vfs.release_fh(fh).await.unwrap();
            let uploads = vfs.pending_uploads_snapshot_for_inode(ino);
            assert_eq!(uploads.len(), 1);
            let upload = &uploads[0];
            vfs.local_payload_backend
                .mark_uploaded(upload.object_id)
                .unwrap();
            vfs.cached_store.complete_upload(upload);
            vfs.local_payload_backend
                .remove_batch(&[upload.object_id])
                .unwrap();

            meta.apply_replica_serving_state(&[studiofs_meta::MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: RULE_FILE.to_vec(),
                entry: studiofs_meta::schema::DirEntryValue {
                    ino,
                    kind: FileType::RegularFile,
                },
            }])
            .unwrap();
            vfs.drain_semantic_invalidations();
            assert!(!vfs.local_payload_backend.contains(upload.object_id));
            vfs.local_payload_backend
                .put(upload.object_id, payload, true)
                .unwrap();
            assert!(!vfs.local_payload_backend.is_evictable(upload.object_id));
        });
    }

    /// Renaming a rule file away stops watching and pinning its inode, and a
    /// scan drops an unlinked rule file from the watched set.
    #[test]
    fn ignore_rules_stop_watching_rule_file_renamed_away_or_unlinked() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-");
        let (runtime, vfs, meta) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules-unwatch");
        let mut changes = vfs.subscribe_ignore_rules_changes();
        runtime.block_on(async {
            let worker = tokio::spawn(Arc::clone(&vfs).background_ignore_rules());
            vfs.wait_for_ignore_rules_bootstrap().await;
            let ino = write_rule_file(&vfs, ROOT_INO, b"*.tmp\n").await;
            wait_for_class(&vfs, &mut changes, b"file.tmp", ScratchClass::Scratch).await;
            assert!(vfs.ignore_rules.watched.lock().files.contains(&ino));

            vfs.rename(ROOT_INO, RULE_FILE, ROOT_INO, b"rules.off", false)
                .unwrap();
            wait_for_class(&vfs, &mut changes, b"file.tmp", ScratchClass::Tracked).await;
            assert!(!vfs.ignore_rules.watched.lock().files.contains(&ino));

            let other = write_rule_file(&vfs, ROOT_INO, b"*.log\n").await;
            wait_for_class(&vfs, &mut changes, b"a.log", ScratchClass::Scratch).await;
            vfs.unlink(ROOT_INO, RULE_FILE).unwrap();
            wait_for_class(&vfs, &mut changes, b"a.log", ScratchClass::Tracked).await;
            assert!(!vfs.ignore_rules.watched.lock().files.contains(&other));

            vfs.state.shutdown.background_shutdown_token.cancel();
            tokio::time::timeout(BOOTSTRAP_BUDGET, worker)
                .await
                .unwrap()
                .unwrap();

            // With the worker stopped, a rewrite of the ordinary file neither
            // requests a rescan nor gains a rule pin.
            let _ = take_refresh_permit(&vfs.ignore_rules);
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh(fh, ino, 0, b"plain bytes\n").await.unwrap();
            vfs.flush_fh(fh).await.unwrap();
            vfs.release_fh(fh).await.unwrap();
            assert!(!take_refresh_permit(&vfs.ignore_rules));
            let pins = vfs.ignore_rules.pins.lock();
            for (object_id, _) in meta.get_regular_file_graph_object_usage(ino).unwrap() {
                assert!(
                    !pins.objects.contains(&object_id),
                    "renamed-away payload {object_id} gained a rule pin"
                );
            }
        });
    }

    #[test]
    fn ignore_rules_pin_revision_changes_only_with_the_pin_set() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-");
        let (_runtime, vfs, _) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules-revision");
        let rules = &vfs.ignore_rules;
        rules.watched.lock().files.insert(10);
        rules.pin_objects(10, [u64::MAX - 1]);
        let revision = rules.pins.lock().revision;
        rules.pin_objects(10, [u64::MAX - 1]);
        rules.pin_objects(10, []);
        rules.pin_objects(11, [u64::MAX - 2]);
        assert_eq!(rules.pins.lock().revision, revision);
        rules.pin_objects(10, [u64::MAX - 1, u64::MAX - 2]);
        assert_ne!(rules.pins.lock().revision, revision);
    }

    /// A rule file over the size limit is never loaded, so its payload must
    /// not stay exempt from cache eviction.
    #[test]
    fn ignore_rules_release_oversized_rule_payload_pins() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-");
        let (runtime, vfs, _) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules-oversized");
        runtime.block_on(async {
            let ino = write_rule_file(&vfs, ROOT_INO, &vec![b'#'; MAX_RULE_BYTES + 1]).await;
            let uploads = vfs.pending_uploads_snapshot_for_inode(ino);
            assert!(!uploads.is_empty());
            for upload in &uploads {
                vfs.local_payload_backend
                    .mark_uploaded(upload.object_id)
                    .unwrap();
                vfs.cached_store.complete_upload(upload);
            }

            let worker = tokio::spawn(Arc::clone(&vfs).background_ignore_rules());
            vfs.wait_for_ignore_rules_bootstrap().await;
            for upload in &uploads {
                assert!(
                    vfs.local_payload_backend.is_evictable(upload.object_id),
                    "oversized rule payload {} stayed pinned",
                    upload.object_id
                );
            }
            vfs.state.shutdown.background_shutdown_token.cancel();
            tokio::time::timeout(BOOTSTRAP_BUDGET, worker)
                .await
                .unwrap()
                .unwrap();
        });
    }

    /// A peer's removal of a rule file reaches the worker only through the
    /// committed invalidation batch of foreign apply.
    #[test]
    fn ignore_rules_follow_foreign_rule_file_removal() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-");
        let (runtime, vfs, meta) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules-foreign");
        let mut changes = vfs.subscribe_ignore_rules_changes();
        runtime.block_on(async {
            let worker = tokio::spawn(Arc::clone(&vfs).background_ignore_rules());
            vfs.wait_for_ignore_rules_bootstrap().await;
            write_rule_file(&vfs, ROOT_INO, b"*.tmp\n").await;
            wait_for_class(&vfs, &mut changes, b"file.tmp", ScratchClass::Scratch).await;

            meta.apply_replica_serving_state(&[studiofs_meta::MetaEvent::RemoveEntry {
                parent: ROOT_INO,
                name: RULE_FILE.to_vec(),
            }])
            .expect("apply foreign removal");
            // Stand in for the running metadata invalidation worker.
            vfs.drain_semantic_invalidations();
            let announced =
                wait_for_class(&vfs, &mut changes, b"file.tmp", ScratchClass::Tracked).await;
            assert_eq!(announced, BTreeSet::from([Vec::new()]));

            vfs.state.shutdown.background_shutdown_token.cancel();
            worker.await.unwrap();
        });
    }

    /// While `.studiofsignore` has any write-capable open handle, the rule loader
    /// keeps the last compiled rule set across truncate, write, and flush, and
    /// loads the new content only after the last write handle closes (even if a
    /// read-only handle remains open).
    #[test]
    fn ignore_rules_keep_last_compiled_rules_while_open_for_writing() {
        let tmp = studiofs_test_support::workspace_target_tempdir("ignore-rules-open-write-");
        let (runtime, vfs, _) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "ignore-rules-open-write");
        let mut changes = vfs.subscribe_ignore_rules_changes();
        let mut passes = vfs.subscribe_ignore_rules_refresh_passes_for_test();
        runtime.block_on(async {
            let worker = tokio::spawn(Arc::clone(&vfs).background_ignore_rules());
            vfs.wait_for_ignore_rules_bootstrap().await;
            let ino = write_rule_file(&vfs, ROOT_INO, b"*.tmp\n").await;
            wait_for_class(&vfs, &mut changes, b"keep.tmp", ScratchClass::Scratch).await;
            while changes.try_recv().is_ok() {}
            let gen_initial = vfs.ignore_rule_generation();

            // Open for writing, truncate, and flush: previous rules must stay active.
            let fh1 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            let pass_before = *passes.borrow();
            vfs.setattr_with_handle(Some(fh1), ino, SetattrArgs::size_only(0))
                .unwrap();
            vfs.flush_fh(fh1).await.unwrap();
            tokio::time::timeout(
                Duration::from_secs(5),
                passes.wait_for(|count| *count > pass_before),
            )
            .await
            .expect("refresh pass after truncate+flush")
            .unwrap();
            assert_eq!(vfs.ignore_rule_generation(), gen_initial);
            assert_eq!(
                vfs.classify_scratch_path(b"keep.tmp", false),
                ScratchClass::Scratch
            );
            assert!(matches!(
                changes.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ));

            // A second write handle keeps rules deferred even after fh1 writes, flushes, and closes.
            let fh2 = vfs.open_file(ino, OpenFlags::WRONLY, false).unwrap();
            vfs.write_fh(fh1, ino, 0, b"*.bak\n").await.unwrap();
            vfs.flush_fh(fh1).await.unwrap();
            let pass_before = *passes.borrow();
            vfs.release_fh(fh1).await.unwrap();
            vfs.trigger_ignore_rules_refresh_for_test();
            tokio::time::timeout(
                Duration::from_secs(5),
                passes.wait_for(|count| *count > pass_before),
            )
            .await
            .expect("refresh pass while second write handle is open")
            .unwrap();
            assert_eq!(vfs.ignore_rule_generation(), gen_initial);
            assert_eq!(
                vfs.classify_scratch_path(b"keep.tmp", false),
                ScratchClass::Scratch
            );
            assert_eq!(
                vfs.classify_scratch_path(b"keep.bak", false),
                ScratchClass::Tracked
            );

            // Closing the last write handle loads the new rules even while a read-only handle is open.
            let ro_fh = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
            vfs.release_fh(fh2).await.unwrap();
            wait_for_class(&vfs, &mut changes, b"keep.bak", ScratchClass::Scratch).await;
            assert_eq!(
                vfs.classify_scratch_path(b"keep.tmp", false),
                ScratchClass::Tracked
            );
            vfs.release_fh(ro_fh).await.unwrap();
            while changes.try_recv().is_ok() {}
            let gen_bak = vfs.ignore_rule_generation();

            // Opening with O_TRUNC must also keep previous rules active until close.
            let pass_before = *passes.borrow();
            let trunc_fh = vfs.open_file(ino, OpenFlags::RDWR, true).unwrap();
            vfs.trigger_ignore_rules_refresh_for_test();
            tokio::time::timeout(
                Duration::from_secs(5),
                passes.wait_for(|count| *count > pass_before),
            )
            .await
            .expect("refresh pass while O_TRUNC handle is open")
            .unwrap();
            assert_eq!(vfs.ignore_rule_generation(), gen_bak);
            assert_eq!(
                vfs.classify_scratch_path(b"keep.bak", false),
                ScratchClass::Scratch
            );
            vfs.write_fh(trunc_fh, ino, 0, b"*.log\n").await.unwrap();
            vfs.release_fh(trunc_fh).await.unwrap();
            wait_for_class(&vfs, &mut changes, b"keep.log", ScratchClass::Scratch).await;
            assert_eq!(
                vfs.classify_scratch_path(b"keep.bak", false),
                ScratchClass::Tracked
            );

            vfs.state.shutdown.background_shutdown_token.cancel();
            worker.await.unwrap();
        });
    }

    /// A regular (non-`.studiofsignore`) file undergoing open -> truncate -> flush ->
    /// write -> flush -> close keeps its immediate visibility and durability behaviour.
    #[test]
    fn regular_file_truncate_and_flush_while_open_keeps_current_behaviour() {
        let tmp = studiofs_test_support::workspace_target_tempdir("regular-file-trunc-");
        let (runtime, vfs, _) =
            super::super::tests::make_unstarted_test_vfs(&tmp, "regular-file-trunc");
        runtime.block_on(async {
            let attr = vfs.create(ROOT_INO, b"notes.txt", 0, 0, 0o644).unwrap();
            let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh(fh, attr.ino, 0, b"before\n").await.unwrap();
            vfs.flush_fh(fh).await.unwrap();
            vfs.release_fh(fh).await.unwrap();

            let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
            vfs.setattr_with_handle(Some(fh), attr.ino, SetattrArgs::size_only(0))
                .unwrap();
            vfs.flush_fh(fh).await.unwrap();
            assert_eq!(vfs.getattr(attr.ino).unwrap().size, 0);
            assert!(
                vfs.read(attr.ino, 0, 64).await.unwrap().is_empty(),
                "truncated regular file must read empty immediately while still open"
            );

            vfs.write_fh(fh, attr.ino, 0, b"after\n").await.unwrap();
            vfs.flush_fh(fh).await.unwrap();
            assert_eq!(vfs.getattr(attr.ino).unwrap().size, 6);
            assert_eq!(
                vfs.read(attr.ino, 0, 64).await.unwrap().as_ref(),
                b"after\n",
                "flushed regular file writes must be visible immediately while still open"
            );

            vfs.release_fh(fh).await.unwrap();
            assert_eq!(vfs.getattr(attr.ino).unwrap().size, 6);
            assert_eq!(
                vfs.read(attr.ino, 0, 64).await.unwrap().as_ref(),
                b"after\n"
            );
        });
    }
}
