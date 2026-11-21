use super::file_session::FileSessionOperation;
use super::metrics::{ScratchPromoteOverLimitEntry, VfsScratchAutoPromoteSnapshot};
use super::namespace::DentryCacheValue;
use super::{StudioVfs, VfsError, VfsResult};
use crate::scratch_rules::{ScratchClass, ScratchRules};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use studiofs_meta::{FileType, Ino, MetaEngine, MetaError, ROOT_INO, ScratchAutoPromoteResult};
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const SCRATCH_PROMOTE_RATE_LIMIT_ENV: &str = "STUDIOFS_SCRATCH_PROMOTE_RATE_LIMIT";
const DEFAULT_AUTO_PROMOTE_RATE_PER_SEC: u64 = 64;
const DEFAULT_AUTO_PROMOTE_BURST: u64 = 16;
const MAX_CANDIDATE_RETRIES: usize = 3;
const FOLLOW_UP_PASS_MIN_DELAY: Duration = Duration::from_secs(1);
const FOLLOW_UP_PASS_MAX_DELAY: Duration = Duration::from_secs(60);

#[cfg(test)]
type RateOverride = Option<(u64, u64)>;

pub(super) struct ScratchAutoPromoteState {
    snapshot: Mutex<VfsScratchAutoPromoteSnapshot>,
    status_tx: watch::Sender<VfsScratchAutoPromoteSnapshot>,
    #[cfg(test)]
    rate_override: Mutex<RateOverride>,
}

impl Default for ScratchAutoPromoteState {
    fn default() -> Self {
        let rate_limit_per_sec = scratch_promote_rate_limit_from_env();
        let initial = VfsScratchAutoPromoteSnapshot {
            entry_limit: super::bootstrap::scratch_promote_entry_limit_from_env(),
            rate_limit_per_sec,
            ..Default::default()
        };
        let (status_tx, _) = watch::channel(initial.clone());
        Self {
            snapshot: Mutex::new(initial),
            status_tx,
            #[cfg(test)]
            rate_override: Mutex::new(None),
        }
    }
}

impl ScratchAutoPromoteState {
    fn effective_rate(&self) -> (u64, u64) {
        #[cfg(test)]
        if let Some(override_rate) = *self.rate_override.lock() {
            return override_rate;
        }
        let rate = self.snapshot.lock().rate_limit_per_sec;
        let burst = if rate == 0 {
            1
        } else {
            rate.clamp(1, DEFAULT_AUTO_PROMOTE_BURST)
        };
        (rate, burst)
    }
}

fn scratch_promote_rate_limit_from_env() -> u64 {
    std::env::var(SCRATCH_PROMOTE_RATE_LIMIT_ENV)
        .ok()
        .as_deref()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_AUTO_PROMOTE_RATE_PER_SEC)
}

enum CandidateOutcome {
    Promoted { conflict_renamed: bool },
    OverLimit(ScratchPromoteOverLimitEntry),
    Skipped,
    Cancelled,
}

struct AutoPromoteRateLimiter {
    rate_per_sec: u64,
    burst: u64,
    tokens: f64,
    last_refill: Instant,
}

impl AutoPromoteRateLimiter {
    fn new(rate_per_sec: u64, burst: u64) -> Self {
        let burst = burst.max(1);
        Self {
            rate_per_sec,
            burst,
            tokens: burst as f64,
            last_refill: Instant::now(),
        }
    }

    fn sync_config(&mut self, rate_per_sec: u64, burst: u64) {
        let burst = burst.max(1);
        if self.rate_per_sec != rate_per_sec || self.burst != burst {
            self.rate_per_sec = rate_per_sec;
            self.burst = burst;
            self.tokens = (burst as f64).min(self.tokens.max(1.0));
            self.last_refill = Instant::now();
        }
    }

    async fn acquire(&mut self, cancel: &CancellationToken) -> bool {
        if cancel.is_cancelled() {
            return false;
        }
        if self.rate_per_sec == 0 {
            return true;
        }
        loop {
            let now = Instant::now();
            let elapsed = now
                .saturating_duration_since(self.last_refill)
                .as_secs_f64();
            self.last_refill = now;
            self.tokens = (self.tokens + elapsed * self.rate_per_sec as f64).min(self.burst as f64);
            if self.tokens >= 1.0 {
                self.tokens -= 1.0;
                return true;
            }
            let wait_secs = (1.0 - self.tokens) / self.rate_per_sec as f64;
            let wait = Duration::from_secs_f64(wait_secs.max(0.001));
            tokio::select! {
                _ = cancel.cancelled() => return false,
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }
}

fn join_relative_path(parent_rel: &[u8], name: &[u8]) -> Vec<u8> {
    if parent_rel.is_empty() {
        return name.to_vec();
    }
    let mut path = Vec::with_capacity(parent_rel.len() + 1 + name.len());
    path.extend_from_slice(parent_rel);
    path.push(b'/');
    path.extend_from_slice(name);
    path
}

fn format_relative_path(rel_path: &[u8]) -> String {
    String::from_utf8_lossy(rel_path).into_owned()
}

fn load_compiled_scratch_rules_from_meta(meta: &dyn MetaEngine) -> VfsResult<ScratchRules> {
    let sources: BTreeMap<Vec<u8>, Vec<u8>> = match meta.load_ignore_rules_cache()? {
        Some(bytes) => match postcard::from_bytes(&bytes) {
            Ok(sources) => sources,
            Err(error) => {
                warn!(%error, "could not decode cached ignore rules while inspecting over-limit scratch directories");
                BTreeMap::new()
            }
        },
        None => BTreeMap::new(),
    };
    let mut rules = ScratchRules::new(true);
    for (directory, payload) in sources {
        if let Err(error) = rules.update(&directory, &payload) {
            warn!(%error, ?directory, "could not compile cached ignore rules");
        }
    }
    Ok(rules)
}

fn resolve_tracked_directory_path_from_meta(
    meta: &dyn MetaEngine,
    dir_ino: Ino,
) -> VfsResult<Option<Vec<u8>>> {
    if dir_ino == ROOT_INO {
        return match meta.get_attr(ROOT_INO) {
            Ok(_) => Ok(Some(Vec::new())),
            Err(MetaError::InodeNotFound(_)) => Ok(None),
            Err(error) => Err(error.into()),
        };
    }

    let mut components: Vec<Vec<u8>> = Vec::new();
    let mut visited = HashSet::new();
    let mut current = dir_ino;
    while current != ROOT_INO {
        if current == 0 || components.len() >= 1024 || !visited.insert(current) {
            return Ok(None);
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
    let total_len = components
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

/// Inspect scratch directories attached to tracked parents that would be
/// eligible for automatic promotion under cached `.studiofsignore` rules, and
/// return those whose subtree entry count (including the directory itself)
/// exceeds `entry_limit`.
pub fn find_over_limit_scratch_directories(
    meta: &dyn MetaEngine,
    entry_limit: usize,
) -> VfsResult<Vec<ScratchPromoteOverLimitEntry>> {
    let rules = load_compiled_scratch_rules_from_meta(meta)?;
    let candidates = meta.list_tracked_parent_scratch_entries()?;
    let mut parent_paths: BTreeMap<Ino, Option<Vec<u8>>> = BTreeMap::new();
    let mut over_limit = Vec::new();

    for (parent, name, attr) in candidates {
        if attr.kind != FileType::Directory {
            continue;
        }
        let parent_path = if let Some(cached) = parent_paths.get(&parent) {
            cached.clone()
        } else {
            let resolved = resolve_tracked_directory_path_from_meta(meta, parent)?;
            parent_paths.insert(parent, resolved.clone());
            resolved
        };
        let Some(parent_path) = parent_path else {
            continue;
        };
        let rel_path = join_relative_path(&parent_path, &name);
        if rules.classify(&rel_path, true) != ScratchClass::Tracked {
            continue;
        }

        let dir_ino = attr.ino;
        let mut visited = HashSet::new();
        let mut pending = vec![attr];
        let mut exceeded = false;
        while let Some(node) = pending.pop() {
            if !visited.insert(node.ino) {
                continue;
            }
            if visited.len() + pending.len() > entry_limit {
                exceeded = true;
                break;
            }
            if node.kind == FileType::Directory {
                pending.extend(
                    meta.readdir_scratch(node.ino)?
                        .into_iter()
                        .map(|(_, child)| child),
                );
                if visited.len() + pending.len() > entry_limit {
                    exceeded = true;
                    break;
                }
            }
        }
        if exceeded {
            over_limit.push(ScratchPromoteOverLimitEntry {
                dir_ino,
                relative_path: format_relative_path(&rel_path),
                entry_count: visited.len() + pending.len(),
                entry_limit,
            });
        }
    }

    over_limit.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(over_limit)
}

impl StudioVfs {
    /// Returns the latest observability snapshot of background scratch auto-promotion.
    pub fn scratch_auto_promote_snapshot(&self) -> VfsScratchAutoPromoteSnapshot {
        let mut snapshot = self.scratch_auto_promote.snapshot.lock().clone();
        snapshot.entry_limit = self.scratch_promote_entry_limit;
        snapshot
    }

    /// Subscribes to completed background scratch auto-promotion pass updates.
    pub fn subscribe_scratch_auto_promote_status(
        &self,
    ) -> watch::Receiver<VfsScratchAutoPromoteSnapshot> {
        self.scratch_auto_promote.status_tx.subscribe()
    }

    #[cfg(test)]
    pub(super) fn set_scratch_auto_promote_rate_limit_for_test(
        &self,
        rate_per_sec: u64,
        burst: u64,
    ) {
        *self.scratch_auto_promote.rate_override.lock() = Some((rate_per_sec, burst.max(1)));
        let mut snapshot = self.scratch_auto_promote.snapshot.lock();
        snapshot.rate_limit_per_sec = rate_per_sec;
        self.scratch_auto_promote
            .status_tx
            .send_replace(snapshot.clone());
    }

    pub(super) async fn background_scratch_auto_promote(
        &self,
        mut rules_rx: broadcast::Receiver<Vec<Vec<u8>>>,
    ) {
        let cancel = self.state.shutdown.background_shutdown_token.clone();
        let (rate_per_sec, burst) = self.scratch_auto_promote.effective_rate();
        let mut limiter = AutoPromoteRateLimiter::new(rate_per_sec, burst);
        // A pass that promoted or failed something gets a backed-off follow-up pass: it picks
        // up entries created as scratch under a directory promoted mid-pass and candidates
        // that kept changing under active writes.
        let mut follow_up: Option<Duration> = None;

        loop {
            let follow_up_timer = async {
                match follow_up {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = follow_up_timer => {}
                recv = rules_rx.recv() => {
                    match recv {
                        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                            while rules_rx.try_recv().is_ok() {}
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
            }

            if cancel.is_cancelled() {
                return;
            }
            let needs_follow_up = self
                .run_scratch_auto_promote_pass(&mut limiter, &cancel)
                .await;
            follow_up = needs_follow_up.then(|| {
                follow_up.map_or(FOLLOW_UP_PASS_MIN_DELAY, |delay| {
                    (delay * 2).min(FOLLOW_UP_PASS_MAX_DELAY)
                })
            });
        }
    }

    /// Runs one pass; returns whether a follow-up pass is warranted.
    async fn run_scratch_auto_promote_pass(
        &self,
        limiter: &mut AutoPromoteRateLimiter,
        cancel: &CancellationToken,
    ) -> bool {
        let (rate_per_sec, burst) = self.scratch_auto_promote.effective_rate();
        limiter.sync_config(rate_per_sec, burst);

        let observed_rule_generation = self.ignore_rule_generation();
        let meta = self.sync_meta();
        let entries = match meta.list_tracked_parent_scratch_entries() {
            Ok(entries) => entries,
            Err(error) => {
                warn!(%error, "failed to list tracked-parent scratch candidates");
                return true;
            }
        };

        let mut parent_paths: BTreeMap<Ino, Option<Vec<u8>>> = BTreeMap::new();
        let mut candidates: Vec<(bool, Ino, Vec<u8>)> = Vec::new();
        for (parent, name, attr) in entries {
            let parent_path = if let Some(cached) = parent_paths.get(&parent) {
                cached.clone()
            } else {
                let resolved = match self.resolve_tracked_directory_path(parent) {
                    Ok(path) => path,
                    Err(error) => {
                        debug!(parent, %error, "skipping scratch auto-promote candidate with unresolvable parent");
                        None
                    }
                };
                parent_paths.insert(parent, resolved.clone());
                resolved
            };
            let Some(parent_path) = parent_path else {
                continue;
            };
            let rel_path = join_relative_path(&parent_path, &name);
            let is_dir = attr.kind == FileType::Directory;
            if self.classify_scratch_path(&rel_path, is_dir) != ScratchClass::Tracked {
                continue;
            }
            let has_open_handle = self.handle_table.open_count(attr.ino) > 0;
            candidates.push((has_open_handle, parent, name));
        }

        // Open-handle entries promote before unopened background entries, then deterministically.
        candidates.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.2.cmp(&b.2))
        });

        let mut pass_promoted = 0u64;
        let mut pass_conflict_renames = 0u64;
        let mut pass_failed = 0u64;
        let mut over_limit = Vec::new();

        for (has_open_handle, parent, name) in candidates {
            if cancel.is_cancelled() {
                return false;
            }
            if !has_open_handle {
                tokio::select! {
                    _ = cancel.cancelled() => return false,
                    _ = self.qos.wait_foreground_local_io_idle() => {}
                }
                if !limiter.acquire(cancel).await {
                    return false;
                }
            }

            let mut attempt = 0usize;
            loop {
                match self.promote_one_auto_candidate(parent, &name).await {
                    Ok(CandidateOutcome::Promoted { conflict_renamed }) => {
                        pass_promoted += 1;
                        if conflict_renamed {
                            pass_conflict_renames += 1;
                        }
                        break;
                    }
                    Ok(CandidateOutcome::OverLimit(entry)) => {
                        over_limit.push(entry);
                        break;
                    }
                    Ok(CandidateOutcome::Skipped) => break,
                    Ok(CandidateOutcome::Cancelled) => return false,
                    Err(VfsError::Meta(MetaError::TargetChanged))
                        if attempt + 1 < MAX_CANDIDATE_RETRIES =>
                    {
                        attempt += 1;
                    }
                    Err(error) => {
                        pass_failed += 1;
                        warn!(
                            parent,
                            name = %String::from_utf8_lossy(&name),
                            %error,
                            "background scratch auto-promote failed for candidate"
                        );
                        break;
                    }
                }
            }
        }

        over_limit.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
        let next_snapshot = {
            let mut snapshot = self.scratch_auto_promote.snapshot.lock();
            snapshot.entry_limit = self.scratch_promote_entry_limit;
            snapshot.rate_limit_per_sec = rate_per_sec;
            snapshot.passes_completed = snapshot.passes_completed.saturating_add(1);
            snapshot.last_rule_generation =
                snapshot.last_rule_generation.max(observed_rule_generation);
            snapshot.promoted_entries = snapshot.promoted_entries.saturating_add(pass_promoted);
            snapshot.conflict_renames = snapshot
                .conflict_renames
                .saturating_add(pass_conflict_renames);
            snapshot.failed_entries = snapshot.failed_entries.saturating_add(pass_failed);
            snapshot.over_limit_directories = over_limit;
            snapshot.clone()
        };
        self.scratch_auto_promote
            .status_tx
            .send_replace(next_snapshot);
        pass_promoted > 0 || pass_failed > 0
    }

    async fn promote_one_auto_candidate(
        &self,
        parent: Ino,
        name: &[u8],
    ) -> VfsResult<CandidateOutcome> {
        let Ok(_op_guard) = self.state.shutdown.mutating_operation_gate.try_read() else {
            return Ok(CandidateOutcome::Cancelled);
        };

        let meta = self.sync_meta();
        self.wait_for_deferred_namespace_entry(parent, name).await?;

        let Some(current_ino) = meta.lookup_scratch_entry(parent, name)? else {
            return Ok(CandidateOutcome::Skipped);
        };
        let current_attr = match meta.get_scratch_attr(current_ino) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => return Ok(CandidateOutcome::Skipped),
            Err(error) => return Err(error.into()),
        };

        let Some(parent_path) = self.resolve_tracked_directory_path(parent)? else {
            return Ok(CandidateOutcome::Skipped);
        };
        let rel_path = join_relative_path(&parent_path, name);
        let root_is_dir = current_attr.kind == FileType::Directory;
        if self.classify_scratch_path(&rel_path, root_is_dir) != ScratchClass::Tracked {
            return Ok(CandidateOutcome::Skipped);
        }

        let mut frozen = HashSet::new();
        let mut kept_scratch = HashSet::new();
        let mut pending = vec![(current_attr.clone(), rel_path.clone())];
        let mut files = Vec::new();
        while let Some((attr, attr_path)) = pending.pop() {
            if !frozen.insert(attr.ino) {
                return Err(MetaError::TargetChanged.into());
            }
            if frozen.len() + pending.len() > self.scratch_promote_entry_limit {
                let relative_path = format_relative_path(&rel_path);
                let entry_count = frozen.len() + pending.len();
                warn!(
                    dir_ino = current_attr.ino,
                    %relative_path,
                    entry_count,
                    entry_limit = self.scratch_promote_entry_limit,
                    "Scratch directory `{}` exceeds promote limit; run `studiofs scratch promote`",
                    relative_path
                );
                return Ok(CandidateOutcome::OverLimit(ScratchPromoteOverLimitEntry {
                    dir_ino: current_attr.ino,
                    relative_path,
                    entry_count,
                    entry_limit: self.scratch_promote_entry_limit,
                }));
            }
            if attr.kind == FileType::Directory {
                let children = meta.readdir_scratch(attr.ino)?;
                if frozen.len() + pending.len() + children.len() > self.scratch_promote_entry_limit
                {
                    let relative_path = format_relative_path(&rel_path);
                    let entry_count = frozen.len() + pending.len() + children.len();
                    warn!(
                        dir_ino = current_attr.ino,
                        %relative_path,
                        entry_count,
                        entry_limit = self.scratch_promote_entry_limit,
                        "Scratch directory `{}` exceeds promote limit; run `studiofs scratch promote`",
                        relative_path
                    );
                    return Ok(CandidateOutcome::OverLimit(ScratchPromoteOverLimitEntry {
                        dir_ino: current_attr.ino,
                        relative_path,
                        entry_count,
                        entry_limit: self.scratch_promote_entry_limit,
                    }));
                }
                for (child_name, child) in children {
                    let child_path = join_relative_path(&attr_path, &child_name);
                    let child_is_dir = child.kind == FileType::Directory;
                    // A descendant that still matches a rule stays scratch with its subtree.
                    if self.classify_scratch_path(&child_path, child_is_dir)
                        != ScratchClass::Tracked
                    {
                        kept_scratch.insert(child.ino);
                    } else {
                        pending.push((child, child_path));
                    }
                }
            } else if attr.kind == FileType::RegularFile {
                files.push(attr.ino);
            }
        }

        files.sort_unstable();
        for &ino in &files {
            self.wait_for_deferred_release_inflight(ino).await;
            self.wait_for_deferred_materialization_inflight(ino).await;
        }

        // Outermost lock: serializes these multi-permit sessions with retained restore.
        let restore_gate = self.retained_restore_gate.lock().await;
        let mut sessions = Vec::with_capacity(files.len());
        for ino in files {
            // Shutdown waits for the mutating gate before cancelling background work.
            if self.is_shutting_down() {
                return Ok(CandidateOutcome::Cancelled);
            }
            sessions.push(
                self.file_session_cleanup(ino).with_permit(
                    self.state
                        .file_session
                        .enter_op(ino, FileSessionOperation::Restore)
                        .await,
                ),
            );
            self.flush_retained_source_ordered(ino).await?;
        }

        let conflict_client = meta
            .invalidation_sender_id()
            .unwrap_or_else(|| "local".to_string());
        let started_epoch = self.state.namespace.namespace_epoch.load(Ordering::Acquire);
        let promoted = meta.promote_scratch_auto(
            parent,
            name,
            current_attr.ino,
            &frozen,
            &kept_scratch,
            &conflict_client,
        )?;

        self.apply_auto_promote_side_effects(meta.as_ref(), started_epoch, root_is_dir, &promoted);

        drop(sessions);
        drop(restore_gate);
        meta.request_eager_flush();
        Ok(CandidateOutcome::Promoted {
            conflict_renamed: promoted.conflict_renamed,
        })
    }

    fn apply_auto_promote_side_effects(
        &self,
        meta: &dyn super::VfsMeta,
        started_epoch: u64,
        root_is_dir: bool,
        promoted: &ScratchAutoPromoteResult,
    ) {
        for attr in &promoted.attrs {
            self.handle_table
                .update_open_attrs_for_inode(attr.ino, attr);
        }
        self.invalidate_attr_read_cache(
            std::iter::once(promoted.parent).chain(promoted.attrs.iter().map(|attr| attr.ino)),
        );
        // Never insert DentryCacheValue::Negative for `original_name` when conflict-renamed:
        // the colliding tracked winner still occupies `original_name`.
        self.dentry_cache_invalidate(promoted.parent, &promoted.original_name);
        self.record_dentry_changes(
            started_epoch,
            &[promoted.parent],
            &[(
                promoted.parent,
                promoted.final_name.as_slice(),
                DentryCacheValue::Positive(promoted.promoted_ino),
            )],
        );
        if root_is_dir {
            self.ignore_rules.bump_dir_rename_epoch();
        }
        self.ignore_rules.note_local_entry(
            meta,
            &promoted.final_name,
            Some(promoted.promoted_ino),
            true,
        );
        self.bump_acl_namespace_epoch();

        let mut kernel_entries = vec![(promoted.parent, promoted.final_name.clone())];
        if promoted.conflict_renamed {
            kernel_entries.push((promoted.parent, promoted.original_name.clone()));
        }
        self.invalidate_kernel_entries(kernel_entries);
        self.invalidate_kernel_inode_attrs(
            std::iter::once(promoted.parent).chain(promoted.attrs.iter().map(|attr| attr.ino)),
        );

        for attr in &promoted.attrs {
            if !Self::needs_epoch_finalization(attr) {
                continue;
            }
            let objects = match self.regular_file_graph_objects(attr) {
                Ok(objects) => objects,
                Err(error) => {
                    warn!(ino = attr.ino, %error, "could not enumerate auto-promoted payloads");
                    continue;
                }
            };
            let extents: Vec<_> = objects
                .iter()
                .map(|object| object.live_extent(attr.ino, attr.content_epoch))
                .collect();
            let mut queued = HashSet::new();
            let ops: Vec<_> = objects
                .iter()
                .filter(|object| queued.insert(object.object_id))
                .map(|object| object.upload_op(attr.ino, attr.content_epoch))
                .collect();
            self.record_live_extents(&extents);
            self.cached_store.submit_uploads_batch(&ops);
            if ops.is_empty() {
                self.queue_epoch_finalization(attr.ino, attr.content_epoch);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::file_session::OpenFlags;
    use super::super::tests::make_unstarted_test_vfs_at;
    use super::*;
    use crate::VfsConfig;
    use crate::scratch_rules::RULE_FILE;
    use std::sync::Arc;
    use studiofs_meta::contract_harness::{
        project_local_events_for_replay, replay_ordered_batches_with_adr_0012,
    };
    use studiofs_meta::schema::{
        ChunkId, PayloadRef, PayloadRefState, SliceId, SliceOrder, SliceRange, SliceRecord,
        UploadState, counters,
    };
    use studiofs_meta::{
        InodeAttr, MetaEngineCore, MetaEvent, MetaFsOps, MetaInodes, MetaNamespace,
        MetaPayloadObjects, RedbMetaEngine,
    };

    fn make_started_vfs_with_limit(
        temp: &tempfile::TempDir,
        name: &str,
        entry_limit: Option<usize>,
        initial_next_inode: Option<u64>,
        initial_next_object_id: Option<u64>,
    ) -> (
        Arc<tokio::runtime::Runtime>,
        Arc<StudioVfs>,
        Arc<RedbMetaEngine>,
    ) {
        let (rt, mut vfs, meta) =
            make_unstarted_test_vfs_at(temp.path(), name, VfsConfig::default(), None);
        if let Some(limit) = entry_limit {
            Arc::get_mut(&mut vfs).unwrap().scratch_promote_entry_limit = limit;
        }
        if let Some(next_ino) = initial_next_inode {
            meta.set_counter(counters::NEXT_INODE, next_ino).unwrap();
        }
        if let Some(next_obj) = initial_next_object_id {
            meta.set_counter(counters::NEXT_OBJECT_ID, next_obj)
                .unwrap();
        }
        let _guard = rt.enter();
        vfs.start().unwrap();
        (rt, vfs, meta)
    }

    async fn install_ignore_rules(vfs: &Arc<StudioVfs>, parent: Ino, payload: &[u8]) {
        let staging = b"studiofsignore_stage";
        let attr = vfs.create(parent, staging, 0, 0, 0o644).unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, true).unwrap();
        if !payload.is_empty() {
            vfs.write_fh(fh, attr.ino, 0, payload).await.unwrap();
        }
        vfs.fsync(attr.ino, false).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
        let mut changed = vfs.subscribe_ignore_rules_changes();
        vfs.rename(parent, staging, parent, RULE_FILE, true)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), changed.recv())
            .await
            .expect("ignore rule refresh timed out")
            .expect("ignore rule channel closed");
    }

    async fn wait_for_auto_promote_generation(
        rx: &mut watch::Receiver<VfsScratchAutoPromoteSnapshot>,
        min_rule_generation: u64,
    ) -> VfsScratchAutoPromoteSnapshot {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if rx.borrow().last_rule_generation >= min_rule_generation {
                    return rx.borrow().clone();
                }
                rx.changed()
                    .await
                    .expect("scratch auto-promote watch closed");
            }
        })
        .await
        .expect("timed out waiting for scratch auto-promote rule generation")
    }

    async fn write_and_close_file(
        vfs: &Arc<StudioVfs>,
        parent: Ino,
        name: &[u8],
        payload: &[u8],
    ) -> InodeAttr {
        let attr = vfs.create(parent, name, 0, 0, 0o644).unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        if !payload.is_empty() {
            vfs.write_fh(fh, attr.ino, 0, payload).await.unwrap();
        }
        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
        attr
    }

    #[test]
    fn scratch_auto_promote_on_rule_removal_promotes_open_handle_without_copy() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-open-");
        let (rt, vfs, meta) =
            make_started_vfs_with_limit(&temp, "scratch-auto-open", None, None, None);
        let _guard = rt.enter();
        rt.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;

            // Open flushed scratch handle (pre-allocated scratch object ID).
            let file = vfs.create(ROOT_INO, b"active.tmp", 0, 0, 0o644).unwrap();
            assert!(vfs.is_scratch_inode(file.ino).unwrap());
            let fh = vfs.open_file(file.ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh(fh, file.ino, 0, b"first-part-").await.unwrap();
            vfs.flush_fh(fh).await.unwrap();

            // Second open scratch handle left dirty/unflushed before rule removal.
            let dirty = vfs
                .create(ROOT_INO, b"dirty_open.tmp", 0, 0, 0o644)
                .unwrap();
            assert!(vfs.is_scratch_inode(dirty.ino).unwrap());
            let dirty_fh = vfs.open_file(dirty.ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh(dirty_fh, dirty.ino, 0, b"dirty-unflushed-")
                .await
                .unwrap();

            let before_object_ids: Vec<u64> = meta
                .lookup_scratch_slices(file.ino, 0, 64)
                .unwrap()
                .into_iter()
                .filter_map(|span| span.payload.map(|p| p.object_id))
                .collect();
            assert!(
                !before_object_ids.is_empty(),
                "flushed scratch handle should have a scratch payload object"
            );
            let uploads_before = vfs
                .cached_store
                .cache_pipeline_metrics()
                .upload_queue
                .submitted;

            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();

            install_ignore_rules(&vfs, ROOT_INO, b"").await;
            let snap =
                wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation())
                    .await;
            assert!(snap.promoted_entries >= 2);
            assert!(!vfs.is_scratch_inode(file.ino).unwrap());
            assert!(!vfs.is_scratch_inode(dirty.ino).unwrap());
            assert_eq!(vfs.lookup(ROOT_INO, b"active.tmp").unwrap().ino, file.ino);
            assert_eq!(
                vfs.lookup(ROOT_INO, b"dirty_open.tmp").unwrap().ino,
                dirty.ino
            );

            let after_object_ids: Vec<u64> = meta
                .lookup_regular_file_stored_slices(file.ino, 0, 64)
                .unwrap()
                .into_iter()
                .filter_map(|span| span.payload.map(|p| p.object_id))
                .collect();
            assert_eq!(
                after_object_ids, before_object_ids,
                "auto-promotion must reuse scratch object IDs without copying"
            );
            assert!(
                vfs.cached_store
                    .cache_pipeline_metrics()
                    .upload_queue
                    .submitted
                    > uploads_before
            );

            // Both existing open handles continue writing and flushing seamlessly after promotion.
            vfs.write_fh(fh, file.ino, 11, b"second-part")
                .await
                .unwrap();
            vfs.fsync_fh(fh).await.unwrap();
            vfs.release_fh(fh).await.unwrap();

            vfs.write_fh(dirty_fh, dirty.ino, 16, b"tail")
                .await
                .unwrap();
            vfs.fsync_fh(dirty_fh).await.unwrap();
            vfs.release_fh(dirty_fh).await.unwrap();

            let content = vfs.read(file.ino, 0, 64).await.unwrap();
            assert_eq!(content.as_ref(), b"first-part-second-part");
            let dirty_content = vfs.read(dirty.ino, 0, 64).await.unwrap();
            assert_eq!(dirty_content.as_ref(), b"dirty-unflushed-tail");

            // Adding the rule back leaves the promoted tracked file tracked (sticky tracked).
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;
            wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation()).await;
            assert!(!vfs.is_scratch_inode(file.ino).unwrap());
            assert!(!vfs.is_scratch_inode(dirty.ino).unwrap());

            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
    }

    #[test]
    fn scratch_auto_promote_partial_rule_change_and_peer_edit() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-partial-");
        let (rt, vfs, meta) =
            make_started_vfs_with_limit(&temp, "scratch-auto-partial", None, None, None);
        let _guard = rt.enter();
        rt.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n*.bak\nscratch_dir/\n").await;

            let a_tmp = write_and_close_file(&vfs, ROOT_INO, b"a.tmp", b"alpha").await;
            let b_bak = write_and_close_file(&vfs, ROOT_INO, b"b.bak", b"beta").await;
            let sdir = vfs.mkdir(ROOT_INO, b"scratch_dir", 0, 0, 0o755).unwrap();
            let inside_tmp =
                write_and_close_file(&vfs, sdir.ino, b"inside.tmp", b"inside-tmp").await;
            let inside_txt =
                write_and_close_file(&vfs, sdir.ino, b"inside.txt", b"inside-txt").await;

            for ino in [
                a_tmp.ino,
                b_bak.ino,
                sdir.ino,
                inside_tmp.ino,
                inside_txt.ino,
            ] {
                assert!(vfs.is_scratch_inode(ino).unwrap());
            }

            // Local partial rule removal: drop `*.tmp`, keep `*.bak` and `scratch_dir/`.
            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();
            install_ignore_rules(&vfs, ROOT_INO, b"*.bak\nscratch_dir/\n").await;
            wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation()).await;

            assert!(!vfs.is_scratch_inode(a_tmp.ino).unwrap());
            for still_scratch in [b_bak.ino, sdir.ino, inside_tmp.ino, inside_txt.ino] {
                assert!(vfs.is_scratch_inode(still_scratch).unwrap());
            }

            // Peer rule edit via foreign apply: publish current `.studiofsignore` to base (modeling
            // prior cluster sync), then apply peer edit dropping `*.bak` while keeping `scratch_dir/`.
            let rule_entry = meta.lookup(ROOT_INO, RULE_FILE).unwrap();
            let mut rule_attr = meta.get_attr(rule_entry.ino).unwrap();
            if rule_attr.upload_state != UploadState::Ready
                || rule_attr.committed_content_epoch != rule_attr.content_epoch
            {
                let obj_ids = meta
                    .get_regular_file_overlay_object_ids(rule_entry.ino)
                    .unwrap();
                meta.mark_regular_file_overlay_objects_uploaded_if_match(
                    rule_entry.ino,
                    rule_attr.content_epoch,
                    &obj_ids,
                )
                .unwrap();
                let _ = meta
                    .publish_regular_file_overlay_to_base_if_matches(
                        rule_entry.ino,
                        rule_attr.content_epoch,
                    )
                    .unwrap();
                rule_attr = meta.get_attr(rule_entry.ino).unwrap();
            }
            let peer_rules = b"scratch_dir/\n";
            let peer_obj = meta.next_object_id().unwrap();
            vfs.local_payload_backend
                .put(peer_obj, peer_rules, true)
                .unwrap();
            rule_attr.content_epoch = rule_attr.content_epoch.saturating_add(1);
            rule_attr.committed_content_epoch = rule_attr.content_epoch;
            rule_attr.size = peer_rules.len() as u64;
            rule_attr.committed_size = rule_attr.size;
            rule_attr.upload_state = UploadState::Ready;
            let mut rec = SliceRecord::new(
                SliceId(1),
                SliceOrder(1),
                SliceRange::new(0, peer_rules.len() as u32).unwrap(),
                PayloadRef::new_partial(peer_obj, peer_rules.len() as u32),
            );
            rec.payload_state = PayloadRefState::Published;

            let mut rules_changed = vfs.subscribe_ignore_rules_changes();
            let peer_events = [
                MetaEvent::SetAttr { attr: rule_attr },
                MetaEvent::PublishRegularFileBaseSlices {
                    ino: rule_entry.ino,
                    chunk_slices: vec![(ChunkId(0), vec![rec])],
                },
            ];
            meta.apply_replica_events_to_base(&peer_events).unwrap();
            meta.apply_replica_serving_state(&peer_events).unwrap();
            vfs.drain_semantic_invalidations();
            tokio::time::timeout(Duration::from_secs(5), async {
                while vfs.classify_scratch_path(b"b.bak", false) != ScratchClass::Tracked {
                    rules_changed
                        .recv()
                        .await
                        .expect("peer ignore rule channel closed");
                }
            })
            .await
            .expect("peer ignore rule change timed out");
            wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation()).await;

            assert!(!vfs.is_scratch_inode(b_bak.ino).unwrap());
            // Entries inside a still-scratch directory remain scratch regardless of name.
            for still_scratch in [sdir.ino, inside_tmp.ino, inside_txt.ino] {
                assert!(vfs.is_scratch_inode(still_scratch).unwrap());
            }

            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
    }

    #[test]
    fn scratch_auto_promote_directory_keeps_descendants_that_still_match_a_rule() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-keep-");
        let (rt, vfs, meta) =
            make_started_vfs_with_limit(&temp, "scratch-auto-keep", None, None, None);
        let _guard = rt.enter();
        rt.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"cache/\n*.tmp\n").await;
            let cache = vfs.mkdir(ROOT_INO, b"cache", 0, 0, 0o755).unwrap();
            let kept_file = write_and_close_file(&vfs, cache.ino, b"a.tmp", b"tmp").await;
            let kept_dir = vfs.mkdir(cache.ino, b"sub.tmp", 0, 0, 0o755).unwrap();
            let kept_nested =
                write_and_close_file(&vfs, kept_dir.ino, b"nested.txt", b"nested").await;
            let moved_file = write_and_close_file(&vfs, cache.ino, b"b.txt", b"keep").await;

            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;
            let snap =
                wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation())
                    .await;
            assert_eq!(snap.failed_entries, 0);

            assert!(!vfs.is_scratch_inode(cache.ino).unwrap());
            assert!(!vfs.is_scratch_inode(moved_file.ino).unwrap());
            for still_scratch in [kept_file.ino, kept_dir.ino, kept_nested.ino] {
                assert!(vfs.is_scratch_inode(still_scratch).unwrap());
            }
            // Tracked directories do not count scratch subdirectories in nlink.
            assert_eq!(meta.get_attr(cache.ino).unwrap().nlink, 2);
            // The kept entries stay visible under the promoted directory.
            assert_eq!(vfs.lookup(cache.ino, b"a.tmp").unwrap().ino, kept_file.ino);
            assert_eq!(vfs.lookup(cache.ino, b"sub.tmp").unwrap().ino, kept_dir.ino);

            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
    }

    #[test]
    fn scratch_auto_promote_over_limit_directory_stays_scratch_and_updates_status() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-limit-");
        let (rt, vfs, meta) =
            make_started_vfs_with_limit(&temp, "scratch-auto-limit", Some(3), None, None);
        let _guard = rt.enter();
        rt.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"big/\n").await;

            let big = vfs.mkdir(ROOT_INO, b"big", 0, 0, 0o755).unwrap();
            let f1 = write_and_close_file(&vfs, big.ino, b"f1.txt", b"1").await;
            let f2 = write_and_close_file(&vfs, big.ino, b"f2.txt", b"2").await;
            let f3 = write_and_close_file(&vfs, big.ino, b"f3.txt", b"3").await;

            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();
            install_ignore_rules(&vfs, ROOT_INO, b"").await;
            let snap =
                wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation())
                    .await;

            for ino in [big.ino, f1.ino, f2.ino, f3.ino] {
                assert!(vfs.is_scratch_inode(ino).unwrap());
            }
            assert_eq!(snap.over_limit_directories.len(), 1);
            assert_eq!(snap.over_limit_directories[0].relative_path, "big");
            assert_eq!(snap.over_limit_directories[0].dir_ino, big.ino);
            assert_eq!(snap.over_limit_directories[0].entry_limit, 3);
            assert!(snap.over_limit_directories[0].entry_count > 3);

            // Offline status inspection sees the exact same over-limit directory.
            let offline = find_over_limit_scratch_directories(meta.as_ref(), 3).unwrap();
            assert_eq!(offline, snap.over_limit_directories);

            // Re-adding the rule clears the over-limit warning from live status.
            install_ignore_rules(&vfs, ROOT_INO, b"big/\n").await;
            let cleared =
                wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation())
                    .await;
            assert!(cleared.over_limit_directories.is_empty());

            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
    }

    #[test]
    fn scratch_auto_promote_taken_name_renames_with_client_suffix() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-conflict-");
        let (rt, vfs, meta) =
            make_started_vfs_with_limit(&temp, "scratch-auto-conflict", None, None, None);
        let _guard = rt.enter();
        rt.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;

            let scratch_root =
                write_and_close_file(&vfs, ROOT_INO, b"taken.tmp", b"scratch-root").await;
            let sub = vfs.mkdir(ROOT_INO, b"sub", 0, 0, 0o755).unwrap();
            meta.create_file(sub.ino, b"taken.tmp.scratch-conflict-local", 0, 0, 0o644)
                .unwrap();
            let scratch_sub =
                write_and_close_file(&vfs, sub.ino, b"taken.tmp", b"scratch-sub").await;

            // Populate colliding tracked winners directly in the tracked base and warm dentry cache.
            let tracked_root = meta
                .create_file(ROOT_INO, b"taken.tmp", 0, 0, 0o644)
                .unwrap();
            let tracked_sub = meta
                .create_file(sub.ino, b"taken.tmp", 0, 0, 0o644)
                .unwrap();
            assert_eq!(
                meta.lookup(ROOT_INO, b"taken.tmp").unwrap().ino,
                tracked_root.ino
            );
            assert_eq!(
                meta.lookup(sub.ino, b"taken.tmp").unwrap().ino,
                tracked_sub.ino
            );
            assert_eq!(
                vfs.lookup(ROOT_INO, b"taken.tmp").unwrap().ino,
                scratch_root.ino
            );
            assert_eq!(
                vfs.lookup(sub.ino, b"taken.tmp").unwrap().ino,
                scratch_sub.ino
            );

            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();
            install_ignore_rules(&vfs, ROOT_INO, b"").await;
            let snap =
                wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation())
                    .await;
            assert!(snap.conflict_renames >= 2);

            assert!(!vfs.is_scratch_inode(scratch_root.ino).unwrap());
            assert!(!vfs.is_scratch_inode(scratch_sub.ino).unwrap());
            assert_eq!(
                vfs.lookup(ROOT_INO, b"taken.tmp").unwrap().ino,
                tracked_root.ino
            );
            assert_eq!(
                vfs.lookup(ROOT_INO, b"taken.tmp.scratch-conflict-local")
                    .unwrap()
                    .ino,
                scratch_root.ino
            );
            assert_eq!(
                vfs.lookup(sub.ino, b"taken.tmp").unwrap().ino,
                tracked_sub.ino
            );
            assert_eq!(
                vfs.lookup(sub.ino, b"taken.tmp.scratch-conflict-local-2")
                    .unwrap()
                    .ino,
                scratch_sub.ino
            );
            assert_eq!(
                vfs.read(scratch_root.ino, 0, 64).await.unwrap().as_ref(),
                b"scratch-root"
            );
            assert_eq!(
                vfs.read(scratch_sub.ino, 0, 64).await.unwrap().as_ref(),
                b"scratch-sub"
            );

            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
    }

    #[test]
    fn scratch_auto_promote_two_clients_publish_under_adr_0012() {
        let temp_a = studiofs_test_support::workspace_target_tempdir("scratch-auto-c1-");
        let temp_b = studiofs_test_support::workspace_target_tempdir("scratch-auto-c2-");
        let (rt_a, vfs_a, meta_a) = make_started_vfs_with_limit(
            &temp_a,
            "scratch-auto-two",
            None,
            Some(1_000),
            Some(1_000),
        );
        let (rt_b, vfs_b, meta_b) = make_started_vfs_with_limit(
            &temp_b,
            "scratch-auto-two",
            None,
            Some(100_000),
            Some(100_000),
        );

        let initial_rules = b"work/\nshared.tmp\n";
        rt_a.block_on(install_ignore_rules(&vfs_a, ROOT_INO, initial_rules));
        rt_b.block_on(install_ignore_rules(&vfs_b, ROOT_INO, initial_rules));

        // Client A creates scratch `work/from_a.txt` and `shared.tmp`.
        let work_a = vfs_a.mkdir(ROOT_INO, b"work", 0, 0, 0o755).unwrap();
        let from_a = rt_a.block_on(write_and_close_file(
            &vfs_a,
            work_a.ino,
            b"from_a.txt",
            b"client-a",
        ));
        let shared_a = rt_a.block_on(write_and_close_file(
            &vfs_a,
            ROOT_INO,
            b"shared.tmp",
            b"shared-a",
        ));

        // Client B creates scratch `work/from_b.txt` and `shared.tmp` with disjoint inodes/objects.
        let work_b = vfs_b.mkdir(ROOT_INO, b"work", 0, 0, 0o755).unwrap();
        let from_b = rt_b.block_on(write_and_close_file(
            &vfs_b,
            work_b.ino,
            b"from_b.txt",
            b"client-b",
        ));
        let shared_b = rt_b.block_on(write_and_close_file(
            &vfs_b,
            ROOT_INO,
            b"shared.tmp",
            b"shared-b",
        ));

        let a_start = meta_a.get_counter(counters::NEXT_EVENT_SEQ).unwrap();
        let b_start = meta_b.get_counter(counters::NEXT_EVENT_SEQ).unwrap();

        // Both clients remove `.studiofsignore` and wait for their auto-promote passes.
        rt_a.block_on(async {
            let mut status_rx = vfs_a.subscribe_scratch_auto_promote_status();
            install_ignore_rules(&vfs_a, ROOT_INO, b"").await;
            wait_for_auto_promote_generation(&mut status_rx, vfs_a.ignore_rule_generation()).await;
        });
        rt_b.block_on(async {
            let mut status_rx = vfs_b.subscribe_scratch_auto_promote_status();
            install_ignore_rules(&vfs_b, ROOT_INO, b"").await;
            wait_for_auto_promote_generation(&mut status_rx, vfs_b.ignore_rule_generation()).await;
        });

        // Mark promoted regular-file payloads uploaded and publish overlays to base before replay.
        for (meta, inos) in [
            (&meta_a, [from_a.ino, shared_a.ino]),
            (&meta_b, [from_b.ino, shared_b.ino]),
        ] {
            for ino in inos {
                let attr = meta.get_attr(ino).unwrap();
                if attr.upload_state != UploadState::Ready
                    || attr.committed_content_epoch != attr.content_epoch
                {
                    let obj_ids = meta.get_regular_file_overlay_object_ids(ino).unwrap();
                    meta.mark_regular_file_overlay_objects_uploaded_if_match(
                        ino,
                        attr.content_epoch,
                        &obj_ids,
                    )
                    .unwrap();
                    let _ = meta
                        .publish_regular_file_overlay_to_base_if_matches(ino, attr.content_epoch)
                        .unwrap();
                }
                let final_attr = meta.get_attr(ino).unwrap();
                assert_eq!(final_attr.upload_state, UploadState::Ready);
                assert_eq!(final_attr.committed_content_epoch, final_attr.content_epoch);
            }
        }

        let a_end = meta_a.get_counter(counters::NEXT_EVENT_SEQ).unwrap();
        let b_end = meta_b.get_counter(counters::NEXT_EVENT_SEQ).unwrap();
        let batch_a =
            project_local_events_for_replay(&meta_a.read_events_range(a_start, a_end).unwrap());
        let batch_b =
            project_local_events_for_replay(&meta_b.read_events_range(b_start, b_end).unwrap());

        replay_ordered_batches_with_adr_0012(
            meta_a.as_ref(),
            vec![(1, batch_a.clone(), true), (2, batch_b.clone(), false)],
            0,
            a_start,
            a_end,
        )
        .unwrap();
        replay_ordered_batches_with_adr_0012(
            meta_b.as_ref(),
            vec![(1, batch_a, false), (2, batch_b, true)],
            0,
            b_start,
            b_end,
        )
        .unwrap();

        let merged_a = meta_a.lookup(ROOT_INO, b"work").unwrap();
        let merged_b = meta_b.lookup(ROOT_INO, b"work").unwrap();
        assert_eq!(merged_a.ino, work_a.ino);
        assert_eq!(merged_b.ino, work_a.ino);
        assert_eq!(
            meta_a.lookup(merged_a.ino, b"from_a.txt").unwrap().ino,
            from_a.ino
        );
        assert_eq!(
            meta_a.lookup(merged_a.ino, b"from_b.txt").unwrap().ino,
            from_b.ino
        );
        assert_eq!(
            meta_b.lookup(merged_b.ino, b"from_a.txt").unwrap().ino,
            from_a.ino
        );
        assert_eq!(
            meta_b.lookup(merged_b.ino, b"from_b.txt").unwrap().ino,
            from_b.ino
        );

        // Regular-file same-name auto-promotion resolves by last writer (`batch_b`).
        assert_eq!(
            meta_a.lookup(ROOT_INO, b"shared.tmp").unwrap().ino,
            shared_b.ino
        );
        assert_eq!(
            meta_b.lookup(ROOT_INO, b"shared.tmp").unwrap().ino,
            shared_b.ino
        );

        rt_a.block_on(vfs_a.shutdown(Duration::from_secs(5)))
            .unwrap();
        rt_b.block_on(vfs_b.shutdown(Duration::from_secs(5)))
            .unwrap();
    }

    #[test]
    fn scratch_auto_promote_rate_limiter_paces_and_shutdown_joins_worker() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-rate-");
        let (rt, vfs, meta) =
            make_started_vfs_with_limit(&temp, "scratch-auto-rate", None, None, None);
        let _guard = rt.enter();
        rt.block_on(async {
            vfs.set_scratch_auto_promote_rate_limit_for_test(4, 1);
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;

            let f1 = write_and_close_file(&vfs, ROOT_INO, b"p1.tmp", b"1").await;
            let f2 = write_and_close_file(&vfs, ROOT_INO, b"p2.tmp", b"2").await;
            let f3 = write_and_close_file(&vfs, ROOT_INO, b"p3.tmp", b"3").await;

            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();
            let started = Instant::now();
            install_ignore_rules(&vfs, ROOT_INO, b"").await;
            let snap =
                wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation()).await;
            let elapsed = started.elapsed();

            assert!(snap.promoted_entries >= 3);
            for ino in [f1.ino, f2.ino, f3.ino] {
                assert!(!vfs.is_scratch_inode(ino).unwrap());
            }
            assert!(
                elapsed >= Duration::from_millis(350),
                "rate-limited promotion of 3 entries at 4/s burst 1 should pace (elapsed={elapsed:?})"
            );

            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
        drop(_guard);
        drop(vfs);
        drop(meta);
        // If background worker did not join cleanly on shutdown, reopening the redb lock fails.
        RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
    }
}
