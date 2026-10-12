//! Session-wide directory snapshot cache, mirroring the WinFSP bridge.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use studiofs_meta::{DirEntry, Ino};
use studiofs_vfs::DirectoryEnumerationObservation;

/// Immutable names, inode numbers, and file types for one enumeration.
pub(crate) type DirectorySnapshot = Arc<[DirEntry]>;

const DEFAULT_ENTRY_BUDGET: usize = 64;
const DEFAULT_BYTE_BUDGET: usize = 4 * 1024 * 1024;

pub(crate) fn snapshot_byte_size(entries: &[DirEntry]) -> usize {
    entries
        .iter()
        .fold(std::mem::size_of_val(entries), |bytes, entry| {
            bytes.saturating_add(entry.name.capacity())
        })
}

/// Linux dirents carry only inode and file type, so attribute-only changes
/// cannot stale a snapshot; every other observation field still must match.
fn linux_observation(
    observation: DirectoryEnumerationObservation,
) -> DirectoryEnumerationObservation {
    DirectoryEnumerationObservation {
        attribute_generation: 0,
        ..observation
    }
}

struct CacheEntry {
    observation: DirectoryEnumerationObservation,
    snapshot: DirectorySnapshot,
    byte_size: usize,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<Ino, CacheEntry>,
    lru: VecDeque<Ino>,
    last_namespace_epoch: Option<u64>,
    occupancy_bytes: usize,
}

impl CacheState {
    /// Clears on a newer namespace epoch; rejects an older one.
    fn prepare_namespace_epoch(&mut self, epoch: u64) -> bool {
        match self.last_namespace_epoch {
            Some(last) if epoch < last => false,
            Some(last) if epoch == last => true,
            _ => {
                self.entries.clear();
                self.lru.clear();
                self.occupancy_bytes = 0;
                self.last_namespace_epoch = Some(epoch);
                true
            }
        }
    }

    fn remove(&mut self, ino: Ino) -> bool {
        let Some(entry) = self.entries.remove(&ino) else {
            return false;
        };
        self.occupancy_bytes = self.occupancy_bytes.saturating_sub(entry.byte_size);
        self.lru.retain(|key| *key != ino);
        true
    }
}

pub(crate) struct DirectorySnapshotCache {
    state: Mutex<CacheState>,
    entry_budget: usize,
    byte_budget: usize,
}

impl Default for DirectorySnapshotCache {
    fn default() -> Self {
        Self::new(DEFAULT_ENTRY_BUDGET, DEFAULT_BYTE_BUDGET)
    }
}

impl DirectorySnapshotCache {
    pub(crate) fn new(entry_budget: usize, byte_budget: usize) -> Self {
        Self {
            state: Mutex::new(CacheState::default()),
            entry_budget,
            byte_budget,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn lookup(
        &self,
        ino: Ino,
        observation: DirectoryEnumerationObservation,
    ) -> Option<DirectorySnapshot> {
        let observation = linux_observation(observation);
        let mut state = self.lock();
        if !state.prepare_namespace_epoch(observation.namespace_epoch) {
            return None;
        }
        match state.entries.get(&ino) {
            Some(entry) if entry.observation == observation => {
                let snapshot = Arc::clone(&entry.snapshot);
                state.lru.retain(|key| *key != ino);
                state.lru.push_back(ino);
                Some(snapshot)
            }
            Some(_) => {
                state.remove(ino);
                None
            }
            None => None,
        }
    }

    /// Admits a snapshot built entirely under `observation`; returns whether it was cached.
    pub(crate) fn insert(
        &self,
        ino: Ino,
        observation: DirectoryEnumerationObservation,
        snapshot: DirectorySnapshot,
    ) -> bool {
        let observation = linux_observation(observation);
        let byte_size = snapshot_byte_size(&snapshot);
        if self.entry_budget == 0 || byte_size > self.byte_budget {
            return false;
        }
        let mut state = self.lock();
        if !state.prepare_namespace_epoch(observation.namespace_epoch) {
            return false;
        }
        state.remove(ino);
        while state.entries.len() >= self.entry_budget
            || state.occupancy_bytes.saturating_add(byte_size) > self.byte_budget
        {
            let Some(oldest) = state.lru.front().copied() else {
                break;
            };
            state.remove(oldest);
        }
        state.occupancy_bytes = state.occupancy_bytes.saturating_add(byte_size);
        state.lru.push_back(ino);
        state.entries.insert(
            ino,
            CacheEntry {
                observation,
                snapshot,
                byte_size,
            },
        );
        true
    }

    #[cfg(test)]
    pub(crate) fn occupancy(&self) -> (usize, usize) {
        let state = self.lock();
        (state.entries.len(), state.occupancy_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_meta::FileType;

    fn observation(
        namespace_epoch: u64,
        directory_generation: u64,
    ) -> DirectoryEnumerationObservation {
        DirectoryEnumerationObservation {
            namespace_epoch,
            directory_generation,
            attribute_generation: 0,
            acl_epoch: 0,
            acl_namespace_epoch: 0,
            access_level: 0,
        }
    }

    fn snapshot(names: &[&[u8]]) -> DirectorySnapshot {
        names
            .iter()
            .enumerate()
            .map(|(index, name)| DirEntry {
                name: name.to_vec(),
                ino: index as Ino + 10,
                kind: FileType::RegularFile,
            })
            .collect()
    }

    #[test]
    fn reuses_matching_observation_and_ignores_attribute_generation() {
        let cache = DirectorySnapshotCache::default();
        let built = snapshot(&[b"a", b"b"]);
        assert!(cache.insert(7, observation(1, 1), Arc::clone(&built)));
        let mut attr_changed = observation(1, 1);
        attr_changed.attribute_generation = 99;
        let hit = cache
            .lookup(7, attr_changed)
            .expect("attribute change keeps dirents valid");
        assert!(Arc::ptr_eq(&hit, &built));
    }

    #[test]
    fn invalidates_on_directory_generation_and_namespace_epoch() {
        let cache = DirectorySnapshotCache::default();
        cache.insert(7, observation(1, 1), snapshot(&[b"a"]));
        cache.insert(8, observation(1, 1), snapshot(&[b"b"]));
        assert!(cache.lookup(7, observation(1, 2)).is_none());
        assert_eq!(cache.occupancy().0, 1, "stale entry is dropped on lookup");
        assert!(cache.lookup(8, observation(2, 1)).is_none());
        assert_eq!(cache.occupancy(), (0, 0), "newer epoch clears the cache");
        assert!(
            !cache.insert(8, observation(1, 1), snapshot(&[b"b"])),
            "a build from a regressed epoch is not admitted"
        );
    }

    #[test]
    fn enforces_entry_and_byte_budgets_with_lru_eviction() {
        let one = snapshot_byte_size(&snapshot(&[b"a"]));
        let cache = DirectorySnapshotCache::new(2, one * 2);
        cache.insert(1, observation(1, 1), snapshot(&[b"a"]));
        cache.insert(2, observation(1, 1), snapshot(&[b"b"]));
        assert!(cache.lookup(1, observation(1, 1)).is_some());
        cache.insert(3, observation(1, 1), snapshot(&[b"c"]));
        assert!(
            cache.lookup(2, observation(1, 1)).is_none(),
            "LRU entry evicted"
        );
        assert!(cache.lookup(1, observation(1, 1)).is_some());
        assert_eq!(cache.occupancy(), (2, one * 2));
        assert!(
            !cache.insert(4, observation(1, 1), snapshot(&[b"a", b"b", b"c"])),
            "oversized snapshots are never admitted"
        );
        assert!(!DirectorySnapshotCache::new(0, usize::MAX).insert(
            1,
            observation(1, 1),
            snapshot(&[b"a"])
        ));
    }
}
