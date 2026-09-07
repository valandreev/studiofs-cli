use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;
use studiofs_meta::Ino;
use studiofs_vfs::DirectoryEnumerationObservation;
use tracing::trace;
use winfsp::U16CStr;
use winfsp::filesystem::{DirBufferLock, DirInfo};

use crate::context::MAX_WINDOWS_COMPONENT_UTF16_UNITS;

pub(super) const DEFAULT_ENTRY_BUDGET: usize = 64;
pub(super) const DEFAULT_BYTE_BUDGET: usize = 4 * 1024 * 1024;

type CachedDirInfo = DirInfo<MAX_WINDOWS_COMPONENT_UTF16_UNITS>;

#[derive(Debug, Clone, Copy, Default)]
#[cfg(test)]
pub(super) struct DirectorySnapshotCacheSnapshot {
    pub(super) hits: u64,
    pub(super) misses: u64,
    pub(super) evictions: u64,
    pub(super) invalidations: u64,
    pub(super) occupancy_entries: usize,
    pub(super) occupancy_bytes: usize,
    pub(super) entry_budget: usize,
    pub(super) byte_budget: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct DirectorySnapshotCacheKey {
    ino: Ino,
    observation: DirectoryEnumerationObservation,
    pattern: Option<Arc<[u16]>>,
}

impl DirectorySnapshotCacheKey {
    pub(super) fn new(
        ino: Ino,
        observation: DirectoryEnumerationObservation,
        pattern: Option<&U16CStr>,
    ) -> Self {
        Self {
            ino,
            observation,
            pattern: pattern.map(|pattern| Arc::from(pattern.as_slice())),
        }
    }

    fn dynamic_byte_size(&self) -> usize {
        self.pattern.as_ref().map_or(0, |pattern| {
            pattern.len().saturating_mul(std::mem::size_of::<u16>())
        })
    }

    #[cfg(test)]
    pub(super) fn for_test(ino: Ino, namespace_epoch: u64, directory_generation: u64) -> Self {
        Self {
            ino,
            observation: DirectoryEnumerationObservation {
                namespace_epoch,
                directory_generation,
                attribute_generation: 0,
                acl_epoch: 0,
                acl_namespace_epoch: 0,
                access_level: 2,
            },
            pattern: None,
        }
    }
}

struct DirectorySnapshotEntries {
    dot: CachedDirInfo,
    dotdot: Option<CachedDirInfo>,
    children: Vec<CachedDirInfo>,
}

pub(super) struct DirectorySnapshot {
    entries: Mutex<DirectorySnapshotEntries>,
    byte_size: usize,
}

impl DirectorySnapshot {
    pub(super) fn new(
        dot: CachedDirInfo,
        dotdot: Option<CachedDirInfo>,
        children: Vec<CachedDirInfo>,
    ) -> Self {
        let info_size = std::mem::size_of::<CachedDirInfo>();
        let byte_size =
            info_size.saturating_mul(1 + usize::from(dotdot.is_some()) + children.len());
        Self {
            entries: Mutex::new(DirectorySnapshotEntries {
                dot,
                dotdot,
                children,
            }),
            byte_size,
        }
    }

    pub(super) fn byte_size(&self) -> usize {
        self.byte_size
    }

    pub(super) fn children_len(&self) -> usize {
        self.entries.lock().children.len()
    }

    pub(super) fn write_to(&self, lock: &DirBufferLock<'_>) -> winfsp::Result<()> {
        let mut entries = self.entries.lock();
        let _ = lock.write(&mut entries.dot);
        if let Some(dotdot) = entries.dotdot.as_mut() {
            let _ = lock.write(dotdot);
        }
        for child in &mut entries.children {
            lock.write(child)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn empty_for_test(byte_size: usize) -> Arc<Self> {
        Arc::new(Self {
            entries: Mutex::new(DirectorySnapshotEntries {
                dot: CachedDirInfo::new(),
                dotdot: None,
                children: Vec::new(),
            }),
            byte_size,
        })
    }
}

struct DirectorySnapshotCacheState {
    entries: HashMap<DirectorySnapshotCacheKey, Arc<DirectorySnapshot>>,
    lru: VecDeque<DirectorySnapshotCacheKey>,
    last_namespace_epoch: Option<u64>,
    hits: u64,
    misses: u64,
    evictions: u64,
    invalidations: u64,
    occupancy_bytes: usize,
}

impl DirectorySnapshotCacheState {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            lru: VecDeque::new(),
            last_namespace_epoch: None,
            hits: 0,
            misses: 0,
            evictions: 0,
            invalidations: 0,
            occupancy_bytes: 0,
        }
    }
}

pub(super) struct DirectorySnapshotCache {
    state: Mutex<DirectorySnapshotCacheState>,
    entry_budget: usize,
    byte_budget: usize,
}

impl Default for DirectorySnapshotCache {
    fn default() -> Self {
        Self::new(DEFAULT_ENTRY_BUDGET, DEFAULT_BYTE_BUDGET)
    }
}

impl DirectorySnapshotCache {
    pub(super) fn new(entry_budget: usize, byte_budget: usize) -> Self {
        Self {
            state: Mutex::new(DirectorySnapshotCacheState::new()),
            entry_budget,
            byte_budget,
        }
    }

    pub(super) fn lookup(&self, key: &DirectorySnapshotCacheKey) -> Option<Arc<DirectorySnapshot>> {
        let mut state = self.state.lock();
        self.prepare_namespace_epoch(&mut state, key.observation.namespace_epoch);
        if let Some(snapshot) = state.entries.get(key).cloned() {
            state.hits = state.hits.saturating_add(1);
            Self::touch_lru(&mut state.lru, key);
            trace!(
                target: "studiofs_winfsp::metadata_baseline::directory_snapshot_cache_hit",
                ino = key.ino,
                "directory snapshot cache hit"
            );
            return Some(snapshot);
        }

        state.misses = state.misses.saturating_add(1);
        trace!(
            target: "studiofs_winfsp::metadata_baseline::directory_snapshot_cache_miss",
            ino = key.ino,
            "directory snapshot cache miss"
        );
        self.invalidate_stale_directory_entries(&mut state, key);
        None
    }

    pub(super) fn insert(
        &self,
        key: DirectorySnapshotCacheKey,
        snapshot: Arc<DirectorySnapshot>,
    ) -> bool {
        let mut state = self.state.lock();
        self.prepare_namespace_epoch(&mut state, key.observation.namespace_epoch);
        let byte_size = snapshot.byte_size().saturating_add(key.dynamic_byte_size());
        if self.entry_budget == 0 || byte_size > self.byte_budget {
            return false;
        }

        if let Some(previous) = state.entries.remove(&key) {
            state.occupancy_bytes = state
                .occupancy_bytes
                .saturating_sub(previous.byte_size().saturating_add(key.dynamic_byte_size()));
            Self::remove_lru(&mut state.lru, &key);
        }

        while state.entries.len() >= self.entry_budget
            || state.occupancy_bytes.saturating_add(byte_size) > self.byte_budget
        {
            let Some(oldest) = state.lru.pop_front() else {
                break;
            };
            if let Some(previous) = state.entries.remove(&oldest) {
                state.occupancy_bytes = state.occupancy_bytes.saturating_sub(
                    previous
                        .byte_size()
                        .saturating_add(oldest.dynamic_byte_size()),
                );
                state.evictions = state.evictions.saturating_add(1);
                trace!(
                    target: "studiofs_winfsp::metadata_baseline::directory_snapshot_cache_eviction",
                    ino = oldest.ino,
                    "directory snapshot cache eviction"
                );
            }
        }

        state.occupancy_bytes = state.occupancy_bytes.saturating_add(byte_size);
        state.lru.push_back(key.clone());
        state.entries.insert(key, snapshot);
        true
    }

    pub(super) fn note_stale_build(&self) {
        let mut state = self.state.lock();
        state.invalidations = state.invalidations.saturating_add(1);
        trace!(
            target: "studiofs_winfsp::metadata_baseline::directory_snapshot_cache_invalidation",
            "directory snapshot build crossed an invalidation"
        );
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> DirectorySnapshotCacheSnapshot {
        let state = self.state.lock();
        DirectorySnapshotCacheSnapshot {
            hits: state.hits,
            misses: state.misses,
            evictions: state.evictions,
            invalidations: state.invalidations,
            occupancy_entries: state.entries.len(),
            occupancy_bytes: state.occupancy_bytes,
            entry_budget: self.entry_budget,
            byte_budget: self.byte_budget,
        }
    }

    fn prepare_namespace_epoch(&self, state: &mut DirectorySnapshotCacheState, epoch: u64) {
        if state.last_namespace_epoch == Some(epoch) {
            return;
        }
        if state.last_namespace_epoch.is_some() {
            let invalidated = state.entries.len();
            state.invalidations = state.invalidations.saturating_add(invalidated as u64);
            state.entries.clear();
            state.lru.clear();
            state.occupancy_bytes = 0;
            if invalidated != 0 {
                trace!(
                    target: "studiofs_winfsp::metadata_baseline::directory_snapshot_cache_invalidation",
                    count = invalidated,
                    "directory snapshot cache invalidation"
                );
            }
        }
        state.last_namespace_epoch = Some(epoch);
    }

    fn invalidate_stale_directory_entries(
        &self,
        state: &mut DirectorySnapshotCacheState,
        key: &DirectorySnapshotCacheKey,
    ) {
        let stale_keys = state
            .entries
            .keys()
            .filter(|candidate| {
                candidate.ino == key.ino && candidate.observation != key.observation
            })
            .cloned()
            .collect::<Vec<_>>();
        for stale_key in stale_keys {
            if let Some(snapshot) = state.entries.remove(&stale_key) {
                state.occupancy_bytes = state.occupancy_bytes.saturating_sub(
                    snapshot
                        .byte_size()
                        .saturating_add(stale_key.dynamic_byte_size()),
                );
                Self::remove_lru(&mut state.lru, &stale_key);
                state.invalidations = state.invalidations.saturating_add(1);
                trace!(
                    target: "studiofs_winfsp::metadata_baseline::directory_snapshot_cache_invalidation",
                    ino = stale_key.ino,
                    "directory snapshot cache invalidation"
                );
            }
        }
    }

    fn touch_lru(lru: &mut VecDeque<DirectorySnapshotCacheKey>, key: &DirectorySnapshotCacheKey) {
        Self::remove_lru(lru, key);
        lru.push_back(key.clone());
    }

    fn remove_lru(lru: &mut VecDeque<DirectorySnapshotCacheKey>, key: &DirectorySnapshotCacheKey) {
        if let Some(index) = lru.iter().position(|candidate| candidate == key) {
            lru.remove(index);
        }
    }
}
