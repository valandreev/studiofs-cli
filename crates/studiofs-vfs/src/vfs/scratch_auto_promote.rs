use super::file_session::FileSessionOperation;
use super::metrics::{
    ScratchPromoteOverLimitEntry, ScratchPromotePathOutcome, VfsScratchAutoPromoteSnapshot,
};
use super::namespace::DentryCacheValue;
use super::{StudioVfs, VfsError, VfsResult};
use crate::scratch_rules::{ScratchClass, ScratchRules};
#[cfg(test)]
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use studiofs_meta::schema::ROOT_INO;
use studiofs_meta::{
    FileType, Ino, InodeAttr, MetaEngine, MetaEngineCore, MetaError, ScratchAutoPromoteResult,
};
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const SCRATCH_PROMOTE_RATE_LIMIT_ENV: &str = "STUDIOFS_SCRATCH_PROMOTE_RATE_LIMIT";
const DEFAULT_AUTO_PROMOTE_RATE_PER_SEC: u64 = 64;
const DEFAULT_AUTO_PROMOTE_BURST: u64 = 16;
const MAX_CANDIDATE_RETRIES: usize = 3;
const FOLLOW_UP_PASS_MIN_DELAY: Duration = Duration::from_secs(1);
const FOLLOW_UP_PASS_MAX_DELAY: Duration = Duration::from_secs(60);
/// Quiet period after a rule change before a pass. Local in-place rewrites of
/// `.studiofsignore` are already deferred until the last write handle closes,
/// but a peer's truncate and subsequent write can still land in separate
/// foreign-apply replay batches on clients with no local write handle.
const RULES_SETTLE_DELAY: Duration = Duration::from_secs(1);

#[cfg(test)]
type RateOverride = Option<(u64, u64)>;

pub(super) struct ScratchAutoPromoteState {
    status_tx: watch::Sender<VfsScratchAutoPromoteSnapshot>,
    /// Wakes the worker when replay merged scratch children into a tracked directory.
    replay_merge_wake: tokio::sync::Notify,
    seen_replay_merge_generation: std::sync::atomic::AtomicU64,
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
        let (status_tx, _) = watch::channel(initial);
        Self {
            status_tx,
            replay_merge_wake: tokio::sync::Notify::new(),
            seen_replay_merge_generation: std::sync::atomic::AtomicU64::new(0),
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
        let rate = self.status_tx.borrow().rate_limit_per_sec;
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
    Promoted {
        conflict_renamed: bool,
    },
    /// Merged into a same-named tracked directory; the moved children are still scratch.
    Merged {
        children: Vec<(Ino, Vec<u8>, InodeAttr)>,
        conflict_renamed: bool,
    },
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

pub(super) fn join_relative_path(parent_rel: &[u8], name: &[u8]) -> Vec<u8> {
    if parent_rel.is_empty() {
        return name.to_vec();
    }
    let mut path = Vec::with_capacity(parent_rel.len() + 1 + name.len());
    path.extend_from_slice(parent_rel);
    path.push(b'/');
    path.extend_from_slice(name);
    path
}

pub(super) fn format_relative_path(rel_path: &[u8]) -> String {
    String::from_utf8_lossy(rel_path).into_owned()
}

pub(super) fn parse_volume_relative_components(raw_path: &str) -> VfsResult<Vec<&str>> {
    if raw_path.is_empty() {
        return Err(VfsError::InvalidName(
            "scratch promote path must name a file or directory inside the volume".to_string(),
        ));
    }
    let mut components = Vec::new();
    for segment in raw_path.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(VfsError::InvalidName(format!(
                "invalid scratch promote path `{raw_path}`"
            )));
        }
        components.push(segment);
    }
    Ok(components)
}

pub(super) fn load_compiled_scratch_rules_from_meta(
    meta: &dyn MetaEngine,
) -> VfsResult<ScratchRules> {
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

pub(super) struct ScratchPromoteSubtree {
    pub(super) frozen: HashSet<Ino>,
    pub(super) kept_scratch: HashSet<Ino>,
    pub(super) files: Vec<Ino>,
}

pub(super) enum ScratchPromoteSubtreeWalk {
    Ready(ScratchPromoteSubtree),
    OverLimit { entry_count: usize },
}

/// How the subtree walk treats an inode it reaches a second time.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ScratchPromoteRevisit {
    /// Promotion freezes an exact set, so a repeated inode means the subtree
    /// changed under the walk.
    TargetChanged,
    /// Offline over-limit inspection only counts entries and skips repeats.
    Skip,
}

/// Walks the scratch subtree under `root_attr`. Every direct child counts
/// toward `entry_limit`, including children `keep_child_scratch` leaves in
/// scratch; their own descendants are not visited.
pub(super) fn collect_scratch_promote_subtree<M: MetaEngineCore + ?Sized>(
    meta: &M,
    root_attr: InodeAttr,
    root_rel_path: &[u8],
    entry_limit: usize,
    revisit: ScratchPromoteRevisit,
    mut keep_child_scratch: impl FnMut(&[u8], bool) -> bool,
) -> VfsResult<ScratchPromoteSubtreeWalk> {
    let mut frozen = HashSet::new();
    let mut kept_scratch = HashSet::new();
    let mut pending = vec![(root_attr, root_rel_path.to_vec())];
    let mut files = Vec::new();
    while let Some((attr, attr_path)) = pending.pop() {
        if !frozen.insert(attr.ino) {
            match revisit {
                ScratchPromoteRevisit::TargetChanged => {
                    return Err(MetaError::TargetChanged.into());
                }
                ScratchPromoteRevisit::Skip => continue,
            }
        }
        if frozen.len() + pending.len() > entry_limit {
            return Ok(ScratchPromoteSubtreeWalk::OverLimit {
                entry_count: frozen.len() + pending.len(),
            });
        }
        if attr.kind == FileType::Directory {
            let children = meta.readdir_scratch(attr.ino)?;
            let entry_count = frozen.len() + pending.len() + children.len();
            if entry_count > entry_limit {
                return Ok(ScratchPromoteSubtreeWalk::OverLimit { entry_count });
            }
            for (child_name, child) in children {
                let child_path = join_relative_path(&attr_path, &child_name);
                let child_is_dir = child.kind == FileType::Directory;
                // A descendant that still matches a rule stays scratch with its subtree.
                if keep_child_scratch(&child_path, child_is_dir) {
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
    Ok(ScratchPromoteSubtreeWalk::Ready(ScratchPromoteSubtree {
        frozen,
        kept_scratch,
        files,
    }))
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
            let resolved = super::ignore_rules::resolve_tracked_directory_path_with(
                meta,
                parent,
                true,
                |_| None,
            )?;
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
        if let ScratchPromoteSubtreeWalk::OverLimit { entry_count } =
            collect_scratch_promote_subtree(
                meta,
                attr,
                &rel_path,
                entry_limit,
                ScratchPromoteRevisit::Skip,
                // The status report counts the whole scratch subtree, including
                // descendants that rules would keep in scratch.
                |_, _| false,
            )?
        {
            over_limit.push(ScratchPromoteOverLimitEntry {
                dir_ino,
                relative_path: format_relative_path(&rel_path),
                path_is_lossy: std::str::from_utf8(&rel_path).is_err(),
                entry_count,
                entry_limit,
            });
        }
    }

    over_limit.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(over_limit)
}

pub(super) struct FrozenScratchSubtree<'a> {
    pub(super) frozen: HashSet<Ino>,
    pub(super) kept_scratch: HashSet<Ino>,
    // Drop order matters: release per-inode `Restore` permits before releasing `retained_restore_gate`.
    _sessions: Vec<super::file_session::FileSessionCleanup>,
    _restore_gate: tokio::sync::MutexGuard<'a, ()>,
}

pub(super) enum FreezeScratchSubtreeOutcome<'a> {
    Ready(FrozenScratchSubtree<'a>),
    OverLimit { entry_count: usize },
    Cancelled,
}

impl StudioVfs {
    /// Returns the latest observability snapshot of background scratch auto-promotion.
    ///
    /// Recorded `over_limit_directories` entries are re-checked against live metadata
    /// and ignore rules so mounted `status` stays accurate after promotions, removals,
    /// renames, or directories shrinking below `entry_limit` between passes.
    pub fn scratch_auto_promote_snapshot(&self) -> VfsScratchAutoPromoteSnapshot {
        let mut snapshot = self.scratch_auto_promote.status_tx.borrow().clone();
        snapshot.entry_limit = self.scratch_promote_entry_limit;
        if !snapshot.over_limit_directories.is_empty() {
            snapshot.over_limit_directories =
                self.recheck_over_limit_directories(snapshot.over_limit_directories);
        }
        snapshot
    }

    /// Drops over-limit entries whose directory left scratch; call after the local commit.
    pub(super) fn prune_over_limit_directories(&self, is_removed: impl Fn(Ino) -> bool) {
        self.scratch_auto_promote
            .status_tx
            .send_if_modified(|snapshot| {
                let before = snapshot.over_limit_directories.len();
                snapshot
                    .over_limit_directories
                    .retain(|entry| !is_removed(entry.dir_ino));
                snapshot.over_limit_directories.len() != before
            });
    }

    fn recheck_over_limit_directories(
        &self,
        entries: Vec<ScratchPromoteOverLimitEntry>,
    ) -> Vec<ScratchPromoteOverLimitEntry> {
        let meta = self.sync_meta();
        let entry_limit = self.scratch_promote_entry_limit;
        let mut live: Vec<ScratchPromoteOverLimitEntry> = entries
            .into_iter()
            .filter_map(|entry| self.recheck_over_limit_entry(meta.as_ref(), entry_limit, entry))
            .collect();
        live.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
        live
    }

    fn recheck_over_limit_entry(
        &self,
        meta: &dyn super::VfsMeta,
        entry_limit: usize,
        entry: ScratchPromoteOverLimitEntry,
    ) -> Option<ScratchPromoteOverLimitEntry> {
        let attr = match meta.get_scratch_attr(entry.dir_ino) {
            Ok(attr) if attr.kind == FileType::Directory => attr,
            Ok(_) | Err(MetaError::InodeNotFound(_)) => return None,
            Err(error) => {
                warn!(
                    dir_ino = entry.dir_ino,
                    %error,
                    "could not re-check over-limit scratch directory attr"
                );
                return Some(entry);
            }
        };
        match meta.get_scratch_attr(attr.parent) {
            // Moved inside another scratch directory: no longer a tracked-parent candidate.
            Ok(_) => return None,
            Err(MetaError::InodeNotFound(_)) => {}
            Err(error) => {
                warn!(
                    dir_ino = entry.dir_ino,
                    parent = attr.parent,
                    %error,
                    "could not re-check over-limit scratch directory parent"
                );
                return Some(entry);
            }
        }
        let name = match meta.readdir_scratch(attr.parent) {
            Ok(children) => children
                .into_iter()
                .find_map(|(name, child)| (child.ino == entry.dir_ino).then_some(name))?,
            Err(MetaError::InodeNotFound(_)) => return None,
            Err(error) => {
                warn!(
                    dir_ino = entry.dir_ino,
                    parent = attr.parent,
                    %error,
                    "could not re-check over-limit scratch directory binding"
                );
                return Some(entry);
            }
        };
        let parent_path = match self.resolve_tracked_directory_path(attr.parent) {
            Ok(Some(path)) => path,
            Ok(None) => return None,
            Err(error) => {
                warn!(
                    dir_ino = entry.dir_ino,
                    parent = attr.parent,
                    %error,
                    "could not resolve tracked parent path for over-limit scratch directory"
                );
                return Some(entry);
            }
        };
        let rel_path = join_relative_path(&parent_path, &name);
        if self.classify_scratch_path(&rel_path, true) != ScratchClass::Tracked {
            return None;
        }
        match collect_scratch_promote_subtree(
            meta,
            attr,
            &rel_path,
            entry_limit,
            ScratchPromoteRevisit::Skip,
            |child_path, child_is_dir| {
                self.classify_scratch_path(child_path, child_is_dir) != ScratchClass::Tracked
            },
        ) {
            Ok(ScratchPromoteSubtreeWalk::OverLimit { entry_count }) => {
                Some(ScratchPromoteOverLimitEntry {
                    dir_ino: entry.dir_ino,
                    relative_path: format_relative_path(&rel_path),
                    path_is_lossy: std::str::from_utf8(&rel_path).is_err(),
                    entry_count,
                    entry_limit,
                })
            }
            Ok(ScratchPromoteSubtreeWalk::Ready(_))
            | Err(VfsError::Meta(MetaError::InodeNotFound(_))) => None,
            Err(error) => {
                warn!(
                    dir_ino = entry.dir_ino,
                    %error,
                    "could not re-check over-limit scratch directory subtree"
                );
                Some(entry)
            }
        }
    }

    /// Requests a pass when replay merged a local scratch directory into a replicated
    /// tracked directory since the last check; its moved children are candidates.
    pub(super) fn note_scratch_replay_merges(&self) {
        let generation = self.sync_meta().scratch_replay_merge_generation();
        let seen = self
            .scratch_auto_promote
            .seen_replay_merge_generation
            .fetch_max(generation, Ordering::AcqRel);
        if generation > seen {
            self.scratch_auto_promote.replay_merge_wake.notify_one();
        }
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
        self.scratch_auto_promote
            .status_tx
            .send_modify(|snapshot| snapshot.rate_limit_per_sec = rate_per_sec);
    }

    /// Waits for `RULES_SETTLE_DELAY` of quiet on `rules_rx`; returns `false` on
    /// shutdown or channel closure.
    async fn wait_for_rules_to_settle(
        rules_rx: &mut broadcast::Receiver<Vec<Vec<u8>>>,
        cancel: &CancellationToken,
    ) -> bool {
        // Run on settled rules: restart the quiet period on every change.
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return false,
                _ = tokio::time::sleep(RULES_SETTLE_DELAY) => return true,
                recv = rules_rx.recv() => {
                    if matches!(recv, Err(broadcast::error::RecvError::Closed)) {
                        return false;
                    }
                }
            }
        }
    }

    pub(super) async fn background_scratch_auto_promote(
        &self,
        mut rules_rx: broadcast::Receiver<Vec<Vec<u8>>>,
        mut rules_loaded_rx: watch::Receiver<bool>,
    ) {
        let cancel = self.state.shutdown.background_shutdown_token.clone();
        // Never run a pass before the initial rule load succeeds: without loaded
        // rules every path classifies as tracked and all scratch would be promoted.
        tokio::select! {
            _ = cancel.cancelled() => return,
            loaded = rules_loaded_rx.wait_for(|loaded| *loaded) => {
                if loaded.is_err() {
                    return;
                }
            }
        }
        if cancel.is_cancelled() {
            return;
        }

        let (rate_per_sec, burst) = self.scratch_auto_promote.effective_rate();
        let mut limiter = AutoPromoteRateLimiter::new(rate_per_sec, burst);
        // A pass that promoted or failed something gets a backed-off follow-up pass: it picks
        // up entries created as scratch under a directory promoted mid-pass and candidates
        // that kept changing under active writes.
        let mut follow_up: Option<Duration> = None;
        let mut run_initial_pass = true;

        loop {
            if run_initial_pass {
                run_initial_pass = false;
                match rules_rx.try_recv() {
                    Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) => {
                        if !Self::wait_for_rules_to_settle(&mut rules_rx, &cancel).await {
                            return;
                        }
                    }
                    Err(broadcast::error::TryRecvError::Empty) => {}
                    Err(broadcast::error::TryRecvError::Closed) => return,
                }
            } else {
                let follow_up_timer = async {
                    match follow_up {
                        Some(delay) => tokio::time::sleep(delay).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = follow_up_timer => {}
                    _ = self.scratch_auto_promote.replay_merge_wake.notified() => {}
                    recv = rules_rx.recv() => {
                        if matches!(recv, Err(broadcast::error::RecvError::Closed)) {
                            return;
                        }
                        if !Self::wait_for_rules_to_settle(&mut rules_rx, &cancel).await {
                            return;
                        }
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
        let mut candidates: Vec<(bool, Ino, Vec<u8>)> = entries
            .into_iter()
            .filter_map(|(parent, name, attr)| {
                self.auto_promote_candidate(&mut parent_paths, parent, name, &attr)
            })
            .collect();

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

        let mut next = 0;
        while let Some((has_open_handle, parent, name)) = candidates.get(next).cloned() {
            next += 1;
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
                    Ok(CandidateOutcome::Merged {
                        children,
                        conflict_renamed,
                    }) => {
                        if conflict_renamed {
                            pass_conflict_renames += 1;
                        }
                        candidates.extend(children.into_iter().filter_map(
                            |(child_parent, child_name, attr)| {
                                self.auto_promote_candidate(
                                    &mut parent_paths,
                                    child_parent,
                                    child_name,
                                    &attr,
                                )
                            },
                        ));
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
        self.scratch_auto_promote.status_tx.send_modify(|snapshot| {
            // A manual promotion can commit after the worker measured a candidate.
            over_limit.retain(|entry| {
                !matches!(
                    meta.get_scratch_attr(entry.dir_ino),
                    Err(MetaError::InodeNotFound(_))
                )
            });
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
        });
        pass_promoted > 0 || pass_failed > 0
    }

    /// Returns `(has_open_handle, parent, name)` when a tracked-parent scratch entry no longer
    /// matches any rule.
    fn auto_promote_candidate(
        &self,
        parent_paths: &mut BTreeMap<Ino, Option<Vec<u8>>>,
        parent: Ino,
        name: Vec<u8>,
        attr: &InodeAttr,
    ) -> Option<(bool, Ino, Vec<u8>)> {
        let parent_path = parent_paths
            .entry(parent)
            .or_insert_with(|| match self.resolve_tracked_directory_path(parent) {
                Ok(path) => path,
                Err(error) => {
                    debug!(parent, %error, "skipping scratch auto-promote candidate with unresolvable parent");
                    None
                }
            })
            .as_ref()?;
        let rel_path = join_relative_path(parent_path, &name);
        let is_dir = attr.kind == FileType::Directory;
        if self.classify_scratch_path(&rel_path, is_dir) != ScratchClass::Tracked {
            return None;
        }
        let has_open_handle = self.handle_table.open_count(attr.ino) > 0;
        Some((has_open_handle, parent, name))
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

        // A same-named tracked directory means a merge: only the root is frozen and the
        // moved children are promoted afterwards as ordinary candidates.
        if root_is_dir
            && let Ok(existing) = meta.lookup(parent, name)
            && existing.kind == FileType::Directory
            && existing.ino != current_attr.ino
        {
            let conflict_client = meta
                .invalidation_sender_id()
                .unwrap_or_else(|| "local".to_string());
            let started_epoch = self.state.namespace.namespace_epoch.load(Ordering::Acquire);
            let merged = meta.promote_scratch_in_place(
                parent,
                name,
                current_attr.ino,
                &HashSet::from([current_attr.ino]),
                &HashSet::new(),
                Some(&conflict_client),
            )?;
            return Ok(self.finish_auto_promote(meta.as_ref(), started_epoch, true, merged));
        }

        let root_ino = current_attr.ino;
        let frozen = match self
            .freeze_scratch_promote_subtree(
                meta.as_ref(),
                current_attr,
                &rel_path,
                true,
                |child_path, child_is_dir| {
                    // A descendant that still matches a rule stays scratch with its subtree.
                    self.classify_scratch_path(child_path, child_is_dir) != ScratchClass::Tracked
                },
            )
            .await?
        {
            FreezeScratchSubtreeOutcome::Ready(frozen) => frozen,
            FreezeScratchSubtreeOutcome::OverLimit { entry_count } => {
                let relative_path = format_relative_path(&rel_path);
                warn!(
                    dir_ino = root_ino,
                    %relative_path,
                    entry_count,
                    entry_limit = self.scratch_promote_entry_limit,
                    "Scratch directory exceeds promote limit; see `studiofs status` for manual promotion"
                );
                return Ok(CandidateOutcome::OverLimit(ScratchPromoteOverLimitEntry {
                    dir_ino: root_ino,
                    relative_path,
                    path_is_lossy: std::str::from_utf8(&rel_path).is_err(),
                    entry_count,
                    entry_limit: self.scratch_promote_entry_limit,
                }));
            }
            FreezeScratchSubtreeOutcome::Cancelled => return Ok(CandidateOutcome::Cancelled),
        };

        let conflict_client = meta
            .invalidation_sender_id()
            .unwrap_or_else(|| "local".to_string());
        let started_epoch = self.state.namespace.namespace_epoch.load(Ordering::Acquire);
        let promoted = meta.promote_scratch_in_place(
            parent,
            name,
            root_ino,
            &frozen.frozen,
            &frozen.kept_scratch,
            Some(&conflict_client),
        )?;

        let outcome = self.finish_auto_promote(meta.as_ref(), started_epoch, root_is_dir, promoted);
        drop(frozen);
        Ok(outcome)
    }

    /// Walks the scratch subtree under `root_attr`, waits for in-flight deferred
    /// release/materialization work, and freezes every regular file in ascending
    /// inode order under `retained_restore_gate` -> `FileSessionOperation::Restore`.
    pub(super) async fn freeze_scratch_promote_subtree<'a, M: MetaEngineCore + ?Sized>(
        &'a self,
        meta: &M,
        root_attr: InodeAttr,
        root_rel_path: &[u8],
        abort_on_shutdown: bool,
        keep_child_scratch: impl FnMut(&[u8], bool) -> bool,
    ) -> VfsResult<FreezeScratchSubtreeOutcome<'a>> {
        self.freeze_scratch_promote_subtree_with_limit(
            meta,
            root_attr,
            root_rel_path,
            abort_on_shutdown,
            Some(self.scratch_promote_entry_limit),
            keep_child_scratch,
        )
        .await
    }

    async fn freeze_scratch_promote_subtree_with_limit<'a, M: MetaEngineCore + ?Sized>(
        &'a self,
        meta: &M,
        root_attr: InodeAttr,
        root_rel_path: &[u8],
        abort_on_shutdown: bool,
        entry_limit: Option<usize>,
        keep_child_scratch: impl FnMut(&[u8], bool) -> bool,
    ) -> VfsResult<FreezeScratchSubtreeOutcome<'a>> {
        let subtree = match collect_scratch_promote_subtree(
            meta,
            root_attr,
            root_rel_path,
            entry_limit.unwrap_or(usize::MAX),
            ScratchPromoteRevisit::TargetChanged,
            keep_child_scratch,
        )? {
            ScratchPromoteSubtreeWalk::Ready(subtree) => subtree,
            ScratchPromoteSubtreeWalk::OverLimit { entry_count } => {
                return Ok(FreezeScratchSubtreeOutcome::OverLimit { entry_count });
            }
        };

        for &ino in &subtree.files {
            self.wait_for_deferred_release_inflight(ino).await;
            self.wait_for_deferred_materialization_inflight(ino).await;
        }

        // Outermost lock: serializes these multi-permit sessions with retained restore.
        let restore_gate = self.retained_restore_gate.lock().await;
        let mut sessions = Vec::with_capacity(subtree.files.len());
        // Hold the write gates through the metadata commit and upload hand-off.
        for ino in subtree.files {
            if abort_on_shutdown && self.is_shutting_down() {
                return Ok(FreezeScratchSubtreeOutcome::Cancelled);
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

        Ok(FreezeScratchSubtreeOutcome::Ready(FrozenScratchSubtree {
            frozen: subtree.frozen,
            kept_scratch: subtree.kept_scratch,
            _sessions: sessions,
            _restore_gate: restore_gate,
        }))
    }

    /// Hands promoted regular-file payloads to the background uploader (or
    /// queues empty epoch finalization). Callers refresh caches and open-handle
    /// attrs themselves. Must run after the metadata commit while holding the
    /// frozen restore permits.
    pub(super) fn submit_promoted_subtree_uploads(&self, attrs: &[InodeAttr]) {
        for attr in attrs {
            if !Self::needs_epoch_finalization(attr) {
                continue;
            }
            let objects = match self.regular_file_graph_objects(attr) {
                Ok(objects) => objects,
                Err(error) => {
                    warn!(ino = attr.ino, %error, "could not enumerate promoted payloads");
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

    fn finish_auto_promote(
        &self,
        meta: &dyn super::VfsMeta,
        started_epoch: u64,
        root_is_dir: bool,
        promoted: ScratchAutoPromoteResult,
    ) -> CandidateOutcome {
        if promoted.merged_into.is_some() {
            self.apply_auto_merge_side_effects(&promoted);
            return CandidateOutcome::Merged {
                conflict_renamed: promoted.conflict_renamed,
                children: promoted.merged_children,
            };
        }
        self.apply_auto_promote_side_effects(meta, started_epoch, root_is_dir, &promoted);
        meta.request_eager_flush();
        CandidateOutcome::Promoted {
            conflict_renamed: promoted.conflict_renamed,
        }
    }

    /// A merge moves scratch rows only (no events): drop cached names and attrs for the
    /// removed scratch directories and the moved children.
    fn apply_auto_merge_side_effects(&self, merged: &ScratchAutoPromoteResult) {
        let mut entries = Vec::new();
        let mut inodes = Vec::new();
        for (old_parent, name, ino) in &merged.merged_away_dirs {
            entries.push((*old_parent, name.clone()));
            inodes.push(*ino);
        }
        for (new_parent, name, attr) in &merged.merged_children {
            self.handle_table
                .update_open_attrs_for_inode(attr.ino, attr);
            entries.push((*new_parent, name.clone()));
            inodes.push(attr.ino);
            inodes.push(*new_parent);
        }
        for (parent, name) in &entries {
            self.dentry_cache_invalidate(*parent, name);
        }
        self.invalidate_attr_read_cache(inodes.iter().copied());
        self.ignore_rules.bump_dir_rename_epoch();
        self.invalidate_kernel_entries(entries);
        self.invalidate_kernel_inode_attrs(inodes);
    }

    /// Applies post-commit VFS side effects for an in-place automatic promotion.
    ///
    /// Unlike explicit rename promotion (`promote_scratch_rename`), auto-promotion
    /// does not move across parents or insert a negative dentry on `original_name`
    /// (since a conflict-renamed winner still owns `original_name`).
    fn apply_auto_promote_side_effects(
        &self,
        meta: &dyn super::VfsMeta,
        started_epoch: u64,
        root_is_dir: bool,
        promoted: &ScratchAutoPromoteResult,
    ) {
        // Unlike explicit rename promotion, refresh open-handle attrs for the
        // promoted inodes before dropping their cached attributes.
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
        self.submit_promoted_subtree_uploads(&promoted.attrs);
    }

    /// Explicitly promotes a scratch file or directory at a volume-relative
    /// path through the control API without enforcing the background entry
    /// limit, and clears any matching over-limit status entries.
    pub async fn promote_scratch_path(
        &self,
        raw_path: &str,
    ) -> VfsResult<ScratchPromotePathOutcome> {
        let components = parse_volume_relative_components(raw_path)?;
        let Ok(_op_guard) = self.state.shutdown.mutating_operation_gate.try_read() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "volume is shutting down",
            )
            .into());
        };

        let meta = self.sync_meta();
        let mut parent = ROOT_INO;
        for (idx, segment) in components[..components.len() - 1].iter().enumerate() {
            self.wait_for_deferred_namespace_entry(parent, segment.as_bytes())
                .await?;
            match meta.lookup(parent, segment.as_bytes()) {
                Ok(attr) => {
                    if attr.kind != FileType::Directory {
                        return Err(MetaError::NotADirectory(attr.ino).into());
                    }
                    parent = attr.ino;
                }
                Err(MetaError::EntryNotFound { .. }) => {
                    if meta
                        .lookup_scratch_entry(parent, segment.as_bytes())?
                        .is_some()
                    {
                        let prefix = components[..=idx].join("/");
                        return Err(VfsError::InvalidName(format!(
                            "parent directory `{prefix}` is still in scratch; promote it first"
                        )));
                    }
                    return Err(MetaError::EntryNotFound {
                        parent,
                        name: (*segment).to_string(),
                    }
                    .into());
                }
                Err(error) => return Err(error.into()),
            }
        }

        let target_name = components[components.len() - 1].as_bytes();
        self.wait_for_deferred_namespace_entry(parent, target_name)
            .await?;
        let Some(root_ino) = meta.lookup_scratch_entry(parent, target_name)? else {
            return match meta.lookup(parent, target_name) {
                Ok(_) => Err(VfsError::InvalidName(format!(
                    "path `{raw_path}` is already tracked, not in scratch"
                ))),
                Err(error) => Err(error.into()),
            };
        };
        let root = meta.get_scratch_attr(root_ino)?;
        let root_is_dir = root.kind == FileType::Directory;
        let force_subtree =
            self.classify_scratch_path(raw_path.as_bytes(), root_is_dir) != ScratchClass::Tracked;
        let frozen = match self
            .freeze_scratch_promote_subtree_with_limit(
                meta.as_ref(),
                root,
                raw_path.as_bytes(),
                false,
                None,
                |child_path, child_is_dir| {
                    !force_subtree
                        && self.classify_scratch_path(child_path, child_is_dir)
                            != ScratchClass::Tracked
                },
            )
            .await?
        {
            FreezeScratchSubtreeOutcome::Ready(frozen) => frozen,
            FreezeScratchSubtreeOutcome::OverLimit { .. }
            | FreezeScratchSubtreeOutcome::Cancelled => return Err(VfsError::CrossDevice),
        };

        let started_epoch = self.state.namespace.namespace_epoch.load(Ordering::Acquire);
        let promoted = meta.promote_scratch_in_place(
            parent,
            target_name,
            root_ino,
            &frozen.frozen,
            &frozen.kept_scratch,
            None,
        )?;
        self.apply_auto_promote_side_effects(meta.as_ref(), started_epoch, root_is_dir, &promoted);
        meta.request_eager_flush();
        self.prune_over_limit_directories(|ino| frozen.frozen.contains(&ino));
        Ok(ScratchPromotePathOutcome {
            relative_path: raw_path.to_owned(),
            promoted_inodes: promoted.attrs.len(),
        })
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
        MetaPayloadObjects, ROOT_INO, RedbMetaEngine,
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

    async fn wait_for_auto_promote_status(
        rx: &mut watch::Receiver<VfsScratchAutoPromoteSnapshot>,
        what: &str,
        done: impl Fn(&VfsScratchAutoPromoteSnapshot) -> bool,
    ) -> VfsScratchAutoPromoteSnapshot {
        tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|snap| done(snap)))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for scratch auto-promote {what}"))
            .expect("scratch auto-promote watch closed")
            .clone()
    }

    async fn wait_for_auto_promote_generation(
        rx: &mut watch::Receiver<VfsScratchAutoPromoteSnapshot>,
        min_rule_generation: u64,
    ) -> VfsScratchAutoPromoteSnapshot {
        wait_for_auto_promote_status(rx, "rule generation", |snap| {
            snap.last_rule_generation >= min_rule_generation
        })
        .await
    }

    async fn wait_for_auto_promote_pass(
        rx: &mut watch::Receiver<VfsScratchAutoPromoteSnapshot>,
        min_passes: u64,
    ) -> VfsScratchAutoPromoteSnapshot {
        wait_for_auto_promote_status(rx, "pass", |snap| snap.passes_completed >= min_passes).await
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

    /// Rewriting `.studiofsignore` in place briefly loads an empty rule set (truncate,
    /// then write). The pass must run on the settled rules, not on the empty ones.
    #[test]
    fn scratch_auto_promote_waits_for_rules_to_settle_after_in_place_rewrite() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-settle-");
        let (rt, vfs, _meta) =
            make_started_vfs_with_limit(&temp, "scratch-auto-settle", None, None, None);
        let _guard = rt.enter();
        rt.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;
            let kept = write_and_close_file(&vfs, ROOT_INO, b"keep.tmp", b"k").await;
            assert!(vfs.is_scratch_inode(kept.ino).unwrap());

            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();
            install_ignore_rules(&vfs, ROOT_INO, b"").await;
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;
            let snap =
                wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation())
                    .await;
            assert_eq!(snap.promoted_entries, 0);
            assert!(vfs.is_scratch_inode(kept.ino).unwrap());

            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
    }

    /// Opening `.studiofsignore` for writing, truncating, flushing, and waiting
    /// longer than `RULES_SETTLE_DELAY` must keep the previous rules active and
    /// leave scratch entries unpromoted. Once new rules are written and the
    /// handle closes, the new rules take effect and trigger auto-promotion.
    #[test]
    fn scratch_auto_promote_does_not_run_on_open_for_writing_rule_file_past_settle_delay() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-open-write-");
        let (rt, vfs, meta) =
            make_started_vfs_with_limit(&temp, "scratch-auto-open-write", None, None, None);
        let _guard = rt.enter();
        rt.block_on(async {
            let mut rules_changed = vfs.subscribe_ignore_rules_changes();
            let mut refresh_passes = vfs.subscribe_ignore_rules_refresh_passes_for_test();
            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();

            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;
            while rules_changed.try_recv().is_ok() {}
            let initial_gen = vfs.ignore_rule_generation();
            let initial_snap = wait_for_auto_promote_generation(&mut status_rx, initial_gen).await;
            assert_eq!(initial_snap.promoted_entries, 0);

            let kept = write_and_close_file(&vfs, ROOT_INO, b"keep.tmp", b"k").await;
            assert!(vfs.is_scratch_inode(kept.ino).unwrap());

            // Open root `.studiofsignore` for writing, truncate it, and flush.
            let rule_ino = meta.lookup(ROOT_INO, RULE_FILE).unwrap().ino;
            let fh = vfs.open_file(rule_ino, OpenFlags::RDWR, false).unwrap();
            let pass_before = *refresh_passes.borrow();
            vfs.setattr_with_handle(Some(fh), rule_ino, crate::SetattrArgs::size_only(0))
                .unwrap();
            vfs.flush_fh(fh).await.unwrap();
            tokio::time::timeout(
                Duration::from_secs(5),
                refresh_passes.wait_for(|count| *count > pass_before),
            )
            .await
            .expect("rule refresh pass after truncate+flush")
            .unwrap();
            assert_eq!(vfs.ignore_rule_generation(), initial_gen);

            // Install an unrelated subdirectory rule file so `background_scratch_auto_promote`
            // waits out the full `RULES_SETTLE_DELAY` and runs a real pass while root
            // `.studiofsignore` is still open for writing (no fixed sleep needed).
            let other_dir = vfs.mkdir(ROOT_INO, b"other", 0, 0, 0o755).unwrap();
            install_ignore_rules(&vfs, other_dir.ino, b"*.other\n").await;
            let gen_after_other = vfs.ignore_rule_generation();
            assert!(gen_after_other > initial_gen);
            let snap_while_open =
                wait_for_auto_promote_generation(&mut status_rx, gen_after_other).await;
            assert_eq!(
                vfs.classify_scratch_path(b"keep.tmp", false),
                ScratchClass::Scratch,
                "previous root rules must remain active while root .studiofsignore is open for writing"
            );
            assert!(
                vfs.is_scratch_inode(kept.ino).unwrap(),
                "scratch entry must not be promoted while .studiofsignore is open for writing"
            );
            assert_eq!(snap_while_open.promoted_entries, 0);

            // Write replacement rules and close the write handle: new rules apply
            // and `keep.tmp` is promoted.
            while rules_changed.try_recv().is_ok() {}
            vfs.write_fh(fh, rule_ino, 0, b"*.bak\n").await.unwrap();
            vfs.flush_fh(fh).await.unwrap();
            vfs.release_fh(fh).await.unwrap();

            tokio::time::timeout(Duration::from_secs(5), rules_changed.recv())
                .await
                .expect("rules changed after close")
                .expect("rules channel open");
            assert_eq!(
                vfs.classify_scratch_path(b"keep.tmp", false),
                ScratchClass::Tracked
            );
            assert_eq!(
                vfs.classify_scratch_path(b"other.bak", false),
                ScratchClass::Scratch
            );

            let new_gen = vfs.ignore_rule_generation();
            assert!(new_gen > gen_after_other);
            let snap = wait_for_auto_promote_generation(&mut status_rx, new_gen).await;
            assert_eq!(snap.promoted_entries, 1);
            assert!(!vfs.is_scratch_inode(kept.ino).unwrap());

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
    fn scratch_auto_promote_limit_counts_children_that_stay_scratch() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-limit-kept-");
        let (rt, vfs, meta) =
            make_started_vfs_with_limit(&temp, "scratch-auto-limit-kept", Some(3), None, None);
        let _guard = rt.enter();
        rt.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"big/\n*.tmp\n").await;

            let big = vfs.mkdir(ROOT_INO, b"big", 0, 0, 0o755).unwrap();
            let moved = write_and_close_file(&vfs, big.ino, b"a.txt", b"a").await;
            let kept_b = write_and_close_file(&vfs, big.ino, b"b.tmp", b"b").await;
            let kept_c = write_and_close_file(&vfs, big.ino, b"c.tmp", b"c").await;

            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;
            let snap =
                wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation())
                    .await;

            // Children that still match a rule count toward the limit before
            // they are classified, so `big` (1 + 3 children) stays scratch.
            for ino in [big.ino, moved.ino, kept_b.ino, kept_c.ino] {
                assert!(vfs.is_scratch_inode(ino).unwrap());
            }
            assert_eq!(snap.over_limit_directories.len(), 1);
            assert_eq!(snap.over_limit_directories[0].dir_ino, big.ino);
            assert_eq!(snap.over_limit_directories[0].entry_count, 4);

            // Offline inspection counts the whole scratch subtree and agrees.
            let offline = find_over_limit_scratch_directories(meta.as_ref(), 3).unwrap();
            assert_eq!(offline, snap.over_limit_directories);

            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
    }

    #[test]
    fn scratch_explicit_promote_path_promotes_over_limit_directory_and_clears_status() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-explicit-promote-");
        let (rt, vfs, meta) =
            make_started_vfs_with_limit(&temp, "scratch-explicit-promote", Some(3), None, None);
        let _guard = rt.enter();
        rt.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"big/\n*.tmp\n").await;

            let big = vfs.mkdir(ROOT_INO, b"big", 0, 0, 0o755).unwrap();
            let f1 = write_and_close_file(&vfs, big.ino, b"a.txt", b"alpha").await;
            let f2 = write_and_close_file(&vfs, big.ino, b"b.txt", b"beta").await;
            let kept = write_and_close_file(&vfs, big.ino, b"c.tmp", b"temp").await;

            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;
            let snap =
                wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation())
                    .await;
            assert_eq!(snap.over_limit_directories.len(), 1);
            assert_eq!(snap.over_limit_directories[0].relative_path, "big");
            assert_eq!(snap.over_limit_directories[0].dir_ino, big.ino);

            // Explicit control-API promotion is not bound by the background entry limit.
            let outcome = vfs.promote_scratch_path("big").await.unwrap();
            assert_eq!(outcome.relative_path, "big");
            assert_eq!(outcome.promoted_inodes, 3);

            assert!(!vfs.is_scratch_inode(big.ino).unwrap());
            assert!(!vfs.is_scratch_inode(f1.ino).unwrap());
            assert!(!vfs.is_scratch_inode(f2.ino).unwrap());
            assert!(
                vfs.is_scratch_inode(kept.ino).unwrap(),
                "descendant still matching *.tmp must stay scratch"
            );
            assert_eq!(vfs.read(f1.ino, 0, 64).await.unwrap().as_ref(), b"alpha");
            assert_eq!(vfs.read(f2.ino, 0, 64).await.unwrap().as_ref(), b"beta");
            assert_eq!(vfs.read(kept.ino, 0, 64).await.unwrap().as_ref(), b"temp");

            // Both mounted snapshot and offline inspection no longer list `big`.
            assert!(
                vfs.scratch_auto_promote_snapshot()
                    .over_limit_directories
                    .is_empty()
            );
            assert!(
                find_over_limit_scratch_directories(meta.as_ref(), 3)
                    .unwrap()
                    .is_empty()
            );

            // Promoting an already-tracked path, missing path, or invalid path returns an error.
            let already = vfs.promote_scratch_path("big").await.unwrap_err();
            assert!(already.to_string().contains("already tracked"));
            assert!(matches!(
                vfs.promote_scratch_path("missing").await,
                Err(VfsError::Meta(MetaError::EntryNotFound { .. }))
            ));
            assert!(matches!(
                vfs.promote_scratch_path("../big").await,
                Err(VfsError::InvalidName(_))
            ));

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
    fn scratch_auto_promote_taken_directory_name_merges_instead_of_renaming() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-merge-");
        let (rt, vfs, meta) =
            make_started_vfs_with_limit(&temp, "scratch-auto-merge", None, None, None);
        let _guard = rt.enter();
        rt.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"work/\n*.tmp\n").await;

            let scratch_work = vfs.mkdir(ROOT_INO, b"work", 0, 0, 0o755).unwrap();
            let scratch_a = write_and_close_file(&vfs, scratch_work.ino, b"a.txt", b"sa").await;
            let scratch_b = write_and_close_file(&vfs, scratch_work.ino, b"b.txt", b"sb").await;
            let scratch_c = write_and_close_file(&vfs, scratch_work.ino, b"c.tmp", b"sc").await;
            let scratch_sub = vfs.mkdir(scratch_work.ino, b"sub", 0, 0, 0o755).unwrap();
            let scratch_n = write_and_close_file(&vfs, scratch_sub.ino, b"n.txt", b"sn").await;

            // Colliding tracked tree populated directly in the tracked base.
            let tracked_work = meta.create_dir(ROOT_INO, b"work", 0, 0, 0o755).unwrap();
            let tracked_a = meta
                .create_file(tracked_work.ino, b"a.txt", 0, 0, 0o644)
                .unwrap();
            let tracked_sub = meta
                .create_dir(tracked_work.ino, b"sub", 0, 0, 0o755)
                .unwrap();

            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;
            let snap =
                wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation())
                    .await;
            assert_eq!(snap.failed_entries, 0);

            // Directories merge: no conflict copy of `work` or `sub`.
            assert_eq!(vfs.lookup(ROOT_INO, b"work").unwrap().ino, tracked_work.ino);
            assert!(
                vfs.lookup(ROOT_INO, b"work.scratch-conflict-local")
                    .is_err()
            );
            assert_eq!(
                vfs.lookup(tracked_work.ino, b"sub").unwrap().ino,
                tracked_sub.ino
            );
            assert!(!vfs.is_scratch_inode(scratch_work.ino).unwrap());
            assert!(!vfs.is_scratch_inode(scratch_sub.ino).unwrap());

            // Files promote into the merged directories; a taken file name is renamed.
            assert_eq!(
                vfs.lookup(tracked_work.ino, b"b.txt").unwrap().ino,
                scratch_b.ino
            );
            assert!(!vfs.is_scratch_inode(scratch_b.ino).unwrap());
            assert_eq!(
                vfs.lookup(tracked_work.ino, b"a.txt").unwrap().ino,
                tracked_a.ino
            );
            assert_eq!(
                vfs.lookup(tracked_work.ino, b"a.txt.scratch-conflict-local")
                    .unwrap()
                    .ino,
                scratch_a.ino
            );
            assert!(!vfs.is_scratch_inode(scratch_a.ino).unwrap());
            assert_eq!(
                vfs.lookup(tracked_sub.ino, b"n.txt").unwrap().ino,
                scratch_n.ino
            );
            assert!(!vfs.is_scratch_inode(scratch_n.ino).unwrap());
            assert_eq!(vfs.read(scratch_n.ino, 0, 8).await.unwrap().as_ref(), b"sn");

            // A rule-matching child moves into the tracked directory but stays scratch.
            assert_eq!(
                vfs.lookup(tracked_work.ino, b"c.tmp").unwrap().ino,
                scratch_c.ino
            );
            assert!(vfs.is_scratch_inode(scratch_c.ino).unwrap());

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

    #[test]
    fn scratch_auto_promote_runs_pass_after_remount_with_unchanged_rules() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-remount-");

        // Mount 1: create scratch entries under `*.tmp` and `*.bak`.
        let (promote_me, dir_tmp, nested, keep_me) = {
            let (rt, vfs, meta) =
                make_started_vfs_with_limit(&temp, "scratch-auto-remount", None, None, None);
            let _guard = rt.enter();
            let created = rt.block_on(async {
                install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n*.bak\n").await;

                let promote_me =
                    write_and_close_file(&vfs, ROOT_INO, b"promote_me.tmp", b"after-remount").await;
                let dir_tmp = vfs.mkdir(ROOT_INO, b"dir.tmp", 0, 0, 0o755).unwrap();
                let nested =
                    write_and_close_file(&vfs, dir_tmp.ino, b"nested.txt", b"nested-remount").await;
                let keep_me =
                    write_and_close_file(&vfs, ROOT_INO, b"keep_me.bak", b"stay-scratch").await;

                for ino in [promote_me.ino, dir_tmp.ino, nested.ino, keep_me.ino] {
                    assert!(vfs.is_scratch_inode(ino).unwrap());
                }

                vfs.shutdown(Duration::from_secs(5)).await.unwrap();
                (promote_me, dir_tmp, nested, keep_me)
            });
            drop(_guard);
            drop(vfs);
            drop(meta);
            drop(rt);
            created
        };

        // Remove `*.tmp` (persisting the updated rules cache) while stopping before any
        // auto-promote pass runs, leaving the candidates in scratch across shutdown.
        {
            let (rt, vfs, meta) = make_unstarted_test_vfs_at(
                temp.path(),
                "scratch-auto-remount",
                VfsConfig::default(),
                None,
            );
            rt.block_on(async {
                let rules_worker = tokio::spawn(Arc::clone(&vfs).background_ignore_rules());
                vfs.wait_for_ignore_rules_bootstrap().await;
                install_ignore_rules(&vfs, ROOT_INO, b"*.bak\n").await;
                vfs.state.shutdown.background_shutdown_token.cancel();
                tokio::time::timeout(Duration::from_secs(5), rules_worker)
                    .await
                    .expect("ignore rules worker joined")
                    .unwrap();
                for ino in [promote_me.ino, dir_tmp.ino, nested.ino, keep_me.ino] {
                    assert!(
                        vfs.is_scratch_inode(ino).unwrap(),
                        "candidate inode {ino} must remain scratch before remount"
                    );
                }
            });
            drop(vfs);
            drop(meta);
            drop(rt);
        }

        // Mount 2: remount with the already-persisted unchanged rule set. No `rules_changed`
        // notification fires, yet the post-mount pass promotes the remaining candidates.
        let (rt, vfs, meta) = make_unstarted_test_vfs_at(
            temp.path(),
            "scratch-auto-remount",
            VfsConfig::default(),
            None,
        );
        let _guard = rt.enter();
        let mut status_rx = vfs.subscribe_scratch_auto_promote_status();
        let mut rules_changed = vfs.subscribe_ignore_rules_changes();
        vfs.start().unwrap();
        rt.block_on(async {
            let snap = wait_for_auto_promote_pass(&mut status_rx, 1).await;
            assert!(
                matches!(
                    rules_changed.try_recv(),
                    Err(broadcast::error::TryRecvError::Empty)
                ),
                "remount with unchanged rules must not depend on a rules-changed notification"
            );
            assert!(snap.passes_completed >= 1);
            assert!(snap.promoted_entries >= 2);
            assert_eq!(snap.failed_entries, 0);

            for promoted_ino in [promote_me.ino, dir_tmp.ino, nested.ino] {
                assert!(
                    !vfs.is_scratch_inode(promoted_ino).unwrap(),
                    "inode {promoted_ino} should be promoted after remount"
                );
            }
            assert!(
                vfs.is_scratch_inode(keep_me.ino).unwrap(),
                "entry still matching `*.bak` must stay scratch"
            );
            assert_eq!(
                vfs.lookup(ROOT_INO, b"promote_me.tmp").unwrap().ino,
                promote_me.ino
            );
            assert_eq!(
                vfs.read(promote_me.ino, 0, 64).await.unwrap().as_ref(),
                b"after-remount"
            );
            assert_eq!(
                vfs.lookup(dir_tmp.ino, b"nested.txt").unwrap().ino,
                nested.ino
            );
            assert_eq!(
                vfs.read(nested.ino, 0, 64).await.unwrap().as_ref(),
                b"nested-remount"
            );

            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
        drop(_guard);
        drop(vfs);
        drop(meta);
    }

    #[test]
    fn scratch_auto_promote_does_not_run_before_rules_are_loaded() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-unloaded-");
        let rule_payload = b"*.tmp\n";
        let (rt, vfs, meta) = make_unstarted_test_vfs_at(
            temp.path(),
            "scratch-auto-unloaded",
            VfsConfig::default(),
            None,
        );
        let _guard = rt.enter();
        rt.block_on(async {
            // Seed a published `.studiofsignore` (as if replicated from a peer) whose payload
            // object is absent from local storage and unreachable remotely, with no cached
            // rules in redb.
            let rule_file = meta.create_file(ROOT_INO, RULE_FILE, 0, 0, 0o644).unwrap();
            let mut rule_attr = meta.get_attr(rule_file.ino).unwrap();
            let rule_object_id = meta.next_object_id().unwrap();
            rule_attr.content_epoch = rule_attr.content_epoch.saturating_add(1);
            rule_attr.committed_content_epoch = rule_attr.content_epoch;
            rule_attr.size = rule_payload.len() as u64;
            rule_attr.committed_size = rule_attr.size;
            rule_attr.upload_state = UploadState::Ready;
            let mut rec = SliceRecord::new(
                SliceId(1),
                SliceOrder(1),
                SliceRange::new(0, rule_payload.len() as u32).unwrap(),
                PayloadRef::new_partial(rule_object_id, rule_payload.len() as u32),
            );
            rec.payload_state = PayloadRefState::Published;
            let events = [
                MetaEvent::SetAttr { attr: rule_attr },
                MetaEvent::PublishRegularFileBaseSlices {
                    ino: rule_file.ino,
                    chunk_slices: vec![(ChunkId(0), vec![rec])],
                },
            ];
            meta.apply_replica_events_to_base(&events).unwrap();
            meta.apply_replica_serving_state(&events).unwrap();
            assert!(!vfs.local_payload_backend.contains(rule_object_id));
            assert!(meta.load_ignore_rules_cache().unwrap().is_none());

            // Seed scratch entries: `keep.tmp` matches the unloaded rule and must not be
            // promoted; `promote.txt` does not match and should promote once rules load.
            let keep_tmp = vfs
                .store_scratch_entry(b"keep.tmp", InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
                .unwrap();
            vfs.write_scratch(0, keep_tmp.ino, 0, b"keep-local")
                .await
                .unwrap();
            vfs.flush(keep_tmp.ino).await.unwrap();

            let promote_txt = vfs
                .store_scratch_entry(
                    b"promote.txt",
                    InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644),
                )
                .unwrap();
            vfs.write_scratch(0, promote_txt.ino, 0, b"promote-me")
                .await
                .unwrap();
            vfs.flush(promote_txt.ino).await.unwrap();

            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();
            let rules_loaded_rx = vfs.subscribe_ignore_rules_loaded();
            vfs.start().unwrap();

            // Wait for the first refresh attempt to finish bootstrap with the missing payload.
            vfs.wait_for_ignore_rules_bootstrap().await;
            // A replay-merge wake before the load must not bypass the gate. The snapshot check
            // below is only a sanity check; the proof is that `keep.tmp` (matched solely by the
            // unloaded rule) is still scratch after the post-load pass, since any pass run
            // without rules would have promoted it.
            vfs.scratch_auto_promote.replay_merge_wake.notify_one();

            assert!(
                !*rules_loaded_rx.borrow(),
                "rules must not report loaded when a rule payload is missing"
            );
            assert_eq!(
                vfs.scratch_auto_promote_snapshot().passes_completed,
                0,
                "auto-promote must not run any pass before rules are loaded"
            );
            assert!(vfs.is_scratch_inode(keep_tmp.ino).unwrap());
            assert!(vfs.is_scratch_inode(promote_txt.ino).unwrap());

            // Once the rule payload becomes available, the first successful load runs one pass
            // using the loaded rules: `keep.tmp` stays scratch and `promote.txt` promotes.
            vfs.local_payload_backend
                .put(rule_object_id, rule_payload, true)
                .unwrap();
            vfs.trigger_ignore_rules_refresh_for_test();

            let snap = wait_for_auto_promote_pass(&mut status_rx, 1).await;
            assert!(*rules_loaded_rx.borrow());
            assert!(snap.passes_completed >= 1);
            assert_eq!(snap.failed_entries, 0);
            assert!(
                vfs.is_scratch_inode(keep_tmp.ino).unwrap(),
                "matching entry must stay scratch once rules load"
            );
            assert!(
                !vfs.is_scratch_inode(promote_txt.ino).unwrap(),
                "non-matching candidate must promote after the first successful rule load"
            );
            assert_eq!(
                vfs.read(promote_txt.ino, 0, 64).await.unwrap().as_ref(),
                b"promote-me"
            );

            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
        drop(_guard);
        drop(vfs);
        drop(meta);
    }

    #[test]
    fn scratch_over_limit_status_clears_between_passes_on_shrink_delete_rename_and_promote() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-over-limit-stale-");
        let (rt, vfs, _meta) =
            make_started_vfs_with_limit(&temp, "scratch-over-limit-stale", Some(3), None, None);
        let _guard = rt.enter();
        rt.block_on(async {
            install_ignore_rules(
                &vfs,
                ROOT_INO,
                b"shrink/\nremove_me/\nreignore/\npromote_me/\n*.tmp\n",
            )
            .await;

            // 1. Directory that will shrink below the limit, then rename into tracked.
            let shrink = vfs.mkdir(ROOT_INO, b"shrink", 0, 0, 0o755).unwrap();
            let _s1 = write_and_close_file(&vfs, shrink.ino, b"f1.txt", b"1").await;
            let _s2 = write_and_close_file(&vfs, shrink.ino, b"f2.txt", b"2").await;
            let _s3 = write_and_close_file(&vfs, shrink.ino, b"f3.txt", b"3").await;

            // 2. Directory that will be deleted (`rmdir`) between passes.
            let remove_me = vfs.mkdir(ROOT_INO, b"remove_me", 0, 0, 0o755).unwrap();
            let _r1 = write_and_close_file(&vfs, remove_me.ino, b"f1.txt", b"1").await;
            let _r2 = write_and_close_file(&vfs, remove_me.ino, b"f2.txt", b"2").await;
            let _r3 = write_and_close_file(&vfs, remove_me.ino, b"f3.txt", b"3").await;

            // 3. Directory that will be renamed within scratch to a rule-matching name.
            let reignore = vfs.mkdir(ROOT_INO, b"reignore", 0, 0, 0o755).unwrap();
            let _m1 = write_and_close_file(&vfs, reignore.ino, b"f1.txt", b"1").await;
            let _m2 = write_and_close_file(&vfs, reignore.ino, b"f2.txt", b"2").await;
            let _m3 = write_and_close_file(&vfs, reignore.ino, b"f3.txt", b"3").await;

            // 4. Directory that will be explicitly promoted via the control API.
            let promote_me = vfs.mkdir(ROOT_INO, b"promote_me", 0, 0, 0o755).unwrap();
            let _p1 = write_and_close_file(&vfs, promote_me.ino, b"f1.txt", b"1").await;
            let _p2 = write_and_close_file(&vfs, promote_me.ino, b"f2.txt", b"2").await;
            let _p3 = write_and_close_file(&vfs, promote_me.ino, b"f3.txt", b"3").await;

            let mut status_rx = vfs.subscribe_scratch_auto_promote_status();
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;
            let snap =
                wait_for_auto_promote_generation(&mut status_rx, vfs.ignore_rule_generation())
                    .await;
            let passes_after_discovery = snap.passes_completed;
            let listed_paths = || {
                vfs.scratch_auto_promote_snapshot()
                    .over_limit_directories
                    .into_iter()
                    .map(|entry| entry.relative_path)
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                listed_paths(),
                vec!["promote_me", "reignore", "remove_me", "shrink"]
            );

            // Shrink `shrink` below the limit (1 dir + 2 files == 3 <= 3): mounted status
            // immediately drops it without waiting for another pass.
            vfs.unlink(shrink.ino, b"f3.txt").unwrap();
            assert_eq!(
                vfs.scratch_auto_promote_snapshot().passes_completed,
                passes_after_discovery
            );
            assert_eq!(listed_paths(), vec!["promote_me", "reignore", "remove_me"]);

            // Rename `shrink` into tracked (`promote_scratch_rename`): also pruned from
            // the watch snapshot itself.
            vfs.rename(ROOT_INO, b"shrink", ROOT_INO, b"shrink_tracked", false)
                .unwrap();
            assert!(!vfs.is_scratch_inode(shrink.ino).unwrap());
            assert!(
                !status_rx
                    .borrow()
                    .over_limit_directories
                    .iter()
                    .any(|entry| entry.dir_ino == shrink.ino)
            );

            // Delete `remove_me` (`unlink` children + `rmdir`): pruned from both watch
            // snapshot and mounted status without another pass.
            vfs.unlink(remove_me.ino, b"f1.txt").unwrap();
            vfs.unlink(remove_me.ino, b"f2.txt").unwrap();
            vfs.unlink(remove_me.ino, b"f3.txt").unwrap();
            vfs.rmdir(ROOT_INO, b"remove_me").unwrap();
            assert!(
                !status_rx
                    .borrow()
                    .over_limit_directories
                    .iter()
                    .any(|entry| entry.dir_ino == remove_me.ino)
            );
            assert_eq!(listed_paths(), vec!["promote_me", "reignore"]);

            // Rename `reignore` within scratch to match `*.tmp`: mounted status drops it
            // immediately because its new path classifies as scratch.
            vfs.rename(ROOT_INO, b"reignore", ROOT_INO, b"reignore.tmp", false)
                .unwrap();
            assert!(vfs.is_scratch_inode(reignore.ino).unwrap());
            assert_eq!(listed_paths(), vec!["promote_me"]);

            // Explicitly promote `promote_me`: pruned from both watch snapshot and status.
            vfs.promote_scratch_path("promote_me").await.unwrap();
            assert!(!vfs.is_scratch_inode(promote_me.ino).unwrap());
            assert!(
                !status_rx
                    .borrow()
                    .over_limit_directories
                    .iter()
                    .any(|entry| entry.dir_ino == promote_me.ino)
            );
            assert!(listed_paths().is_empty());
            assert_eq!(
                vfs.scratch_auto_promote_snapshot().passes_completed,
                passes_after_discovery
            );

            vfs.shutdown(Duration::from_secs(5)).await.unwrap();
        });
    }

    #[test]
    fn scratch_auto_promote_skipped_candidate_clears_stale_over_limit_status() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-auto-skipped-");
        let (rt, mut vfs, meta) = make_unstarted_test_vfs_at(
            temp.path(),
            "scratch-auto-skipped",
            VfsConfig::default(),
            None,
        );
        Arc::get_mut(&mut vfs).unwrap().scratch_promote_entry_limit = 3;
        let _guard = rt.enter();
        rt.block_on(async {
            // Seed an over-limit scratch directory directly under root with empty rules
            // (so it is eligible for auto-promotion).
            let big = vfs
                .store_scratch_entry(b"big", InodeAttr::new_dir(0, ROOT_INO, 0, 0, 0o755))
                .unwrap();
            for name in [b"f1.txt".as_slice(), b"f2.txt", b"f3.txt"] {
                let child_ino = meta.next_inode().unwrap();
                let child = InodeAttr::new_file(child_ino, big.ino, 0, 0, 0o644);
                meta.create_scratch_entry(big.ino, name, &child).unwrap();
            }

            let cancel = CancellationToken::new();
            let mut limiter = AutoPromoteRateLimiter::new(0, 1);

            // Pass 1 records `big` as over-limit in the watch snapshot.
            let needs_follow_up = vfs
                .run_scratch_auto_promote_pass(&mut limiter, &cancel)
                .await;
            assert!(!needs_follow_up);
            let after_pass_1 = vfs.scratch_auto_promote.status_tx.borrow().clone();
            assert_eq!(after_pass_1.passes_completed, 1);
            assert_eq!(after_pass_1.over_limit_directories.len(), 1);
            assert_eq!(after_pass_1.over_limit_directories[0].dir_ino, big.ino);

            // Hold a foreground local IO permit so pass 2 collects `big` as a candidate
            // synchronously and pauses at `wait_foreground_local_io_idle()` on its first poll.
            let local_io = vfs.qos.acquire_foreground_local_io(1).await.unwrap();
            let mut pass_2 =
                std::pin::pin!(vfs.run_scratch_auto_promote_pass(&mut limiter, &cancel));
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            assert!(
                pass_2.as_mut().poll(&mut cx).is_pending(),
                "pass 2 must pause on foreground local IO after collecting candidates"
            );

            // Remove `big` directly in metadata (bypassing VFS `rmdir` pruning) so the
            // stored watch snapshot still lists `big` until pass 2 hits `CandidateOutcome::Skipped`.
            for name in [b"f1.txt".as_slice(), b"f2.txt", b"f3.txt"] {
                meta.unlink_scratch_entry(big.ino, name, false).unwrap();
            }
            meta.rmdir_scratch_entry(ROOT_INO, b"big").unwrap();
            assert_eq!(
                vfs.scratch_auto_promote
                    .status_tx
                    .borrow()
                    .over_limit_directories
                    .len(),
                1,
                "watch snapshot still has stale entry before pass 2 completes"
            );

            // Release foreground IO and let pass 2 finish: `promote_one_auto_candidate`
            // returns `CandidateOutcome::Skipped` and pass 2 clears the stale over-limit list.
            drop(local_io);
            let needs_follow_up = pass_2.await;
            assert!(!needs_follow_up);
            let after_pass_2 = vfs.scratch_auto_promote.status_tx.borrow().clone();
            assert_eq!(after_pass_2.passes_completed, 2);
            assert_eq!(after_pass_2.promoted_entries, 0);
            assert_eq!(after_pass_2.failed_entries, 0);
            assert!(
                after_pass_2.over_limit_directories.is_empty(),
                "skipped candidate must not leave stale over-limit status"
            );
            assert!(
                vfs.scratch_auto_promote_snapshot()
                    .over_limit_directories
                    .is_empty()
            );

            // Direct check of `CandidateOutcome::Skipped` for missing entry.
            let outcome = vfs
                .promote_one_auto_candidate(ROOT_INO, b"big")
                .await
                .unwrap();
            assert!(matches!(outcome, CandidateOutcome::Skipped));
        });
    }
}
