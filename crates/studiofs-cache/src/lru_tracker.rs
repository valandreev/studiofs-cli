//! Unified LRU tracker for the two-level block cache.
//!
//! Tracks access order for all cached blocks across both L1 (RAM) and L2 (NVMe).
//! Uses sharded locks to minimise contention on the hot read path.
//!
//! Access-time updates are debounced: a `touch()` is a no-op if the entry was
//! accessed within the last `DEBOUNCE_SECS` seconds, avoiding repeated linked-list
//! mutations for hot blocks.

use parking_lot::Mutex;
use std::time::{Duration, Instant};

// =============================================================================
// Constants
// =============================================================================

/// Number of LRU shards.  Must be a power of two.
const SHARD_COUNT: usize = 64;

/// Minimum interval between access-time updates (debouncing).
const DEBOUNCE_SECS: u64 = 10;

// =============================================================================
// Types
// =============================================================================

/// Sequential block ID — unique across the volume.
pub type ObjectId = u64;

/// Where a cached block currently lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockLocation {
    /// Block is in L1 (RAM) **and** L2 (NVMe file).
    MemoryAndDisk,
    /// Block is on disk only (L2 NVMe).
    DiskOnly,
    /// Block is in L1 (RAM) only — background L2 write is pending.
    MemoryOnly,
}

/// A single LRU entry.
#[derive(Debug)]
struct LruEntry {
    object_id: ObjectId,
    /// Cached data size in bytes.
    size: u32,
    /// Where the block lives.
    location: BlockLocation,
    /// Whether eviction is blocked (pending upload).
    pinned: bool,
    /// Last access timestamp (for debounce).
    last_access: Instant,
}

/// One shard of the LRU.
///
/// Entries are kept in a `Vec` ordered from **oldest** (index 0) to **newest**
/// (last).  An auxiliary `HashMap` provides O(1) lookup by `ObjectId` →
/// position.  Promoting an entry moves it to the tail (newest position).
struct LruShard {
    /// Ordered entries: index 0 = oldest, last = newest.
    entries: Vec<LruEntry>,
    /// ObjectId → current index in `entries`.
    index: std::collections::HashMap<ObjectId, usize>,
}

/// Statistics snapshot returned by [`LruTracker::stats()`].
#[derive(Debug, Clone, Default)]
pub struct LruStats {
    /// Total tracked bytes across both levels.
    pub total_bytes: u64,
    /// Number of entries residing in L1 (RAM + disk).
    pub l1_entries: u64,
    /// Total bytes in L1.
    pub l1_bytes: u64,
    /// Number of entries residing on disk only.
    pub l2_entries: u64,
    /// Total bytes in L2-only.
    pub l2_bytes: u64,
    /// Number of pinned entries.
    pub pinned: u64,
}

// =============================================================================
// LruShard implementation
// =============================================================================

impl LruShard {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            index: std::collections::HashMap::new(),
        }
    }

    /// Insert or replace an entry, placing it at the tail (newest).
    fn insert(&mut self, entry: LruEntry) {
        let id = entry.object_id;
        if let Some(&old_pos) = self.index.get(&id) {
            // Update existing — remove old, push new to tail.
            self.remove_at(old_pos);
        }
        let pos = self.entries.len();
        self.entries.push(entry);
        self.index.insert(id, pos);
    }

    /// Touch (promote) an entry to the tail.  Returns `true` if the entry
    /// exists and was promoted.
    fn touch(&mut self, id: ObjectId, now: Instant) -> bool {
        let Some(&pos) = self.index.get(&id) else {
            return false;
        };
        let entry = &self.entries[pos];
        if now.duration_since(entry.last_access).as_secs() < DEBOUNCE_SECS {
            return true; // debounce — skip promotion
        }
        // Promote: remove from current position, push to tail.
        let mut entry = self.remove_at(pos);
        entry.last_access = now;
        let new_pos = self.entries.len();
        self.entries.push(entry);
        self.index.insert(id, new_pos);
        true
    }

    /// Remove an entry by ObjectId.  Returns it if it existed.
    fn remove(&mut self, id: ObjectId) -> Option<LruEntry> {
        let pos = self.index.remove(&id)?;
        Some(self.remove_at(pos))
    }

    /// Get a reference to an entry.
    fn get(&self, id: ObjectId) -> Option<&LruEntry> {
        self.index.get(&id).map(|&pos| &self.entries[pos])
    }

    /// Get a mutable reference to an entry.
    fn get_mut(&mut self, id: ObjectId) -> Option<&mut LruEntry> {
        self.index.get(&id).map(|&pos| &mut self.entries[pos])
    }

    /// Scan from oldest (index 0) forward to find the position.
    fn remove_at(&mut self, pos: usize) -> LruEntry {
        let entry = self.entries.swap_remove(pos);
        self.index.remove(&entry.object_id);
        // swap_remove moved the last element to `pos` — update its index.
        if pos < self.entries.len() {
            let moved_id = self.entries[pos].object_id;
            self.index.insert(moved_id, pos);
        }
        entry
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

// =============================================================================
// LruTracker
// =============================================================================

/// Global LRU tracker for the unified two-level cache.
pub struct LruTracker {
    shards: Vec<Mutex<LruShard>>,
}

impl LruTracker {
    /// Largest whole-second duration that can be safely subtracted from `now`.
    fn max_subtractable_secs(now: Instant) -> u64 {
        let mut lo = 0u64;
        let mut hi = 1u64;

        while hi < u64::MAX / 2 && now.checked_sub(Duration::from_secs(hi)).is_some() {
            lo = hi;
            hi = hi.saturating_mul(2);
        }
        if now.checked_sub(Duration::from_secs(hi)).is_some() {
            return hi;
        }

        while lo + 1 < hi {
            let mid = lo + (hi - lo) / 2;
            if now.checked_sub(Duration::from_secs(mid)).is_some() {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Create a new empty tracker.
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(Mutex::new(LruShard::new()));
        }
        Self { shards }
    }

    /// Insert or update an entry, placing it at the most-recently-used position.
    pub fn insert(&self, object_id: ObjectId, size: u32, location: BlockLocation, pinned: bool) {
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        self.shards[shard_idx].lock().insert(LruEntry {
            object_id,
            size,
            location,
            pinned,
            last_access: Instant::now(),
        });
    }

    /// Record an access (touch) for an existing entry.
    /// Returns `true` if the entry exists.
    pub fn touch(&self, object_id: ObjectId) -> bool {
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        self.shards[shard_idx]
            .lock()
            .touch(object_id, Instant::now())
    }

    /// Remove an entry entirely.  Returns `(size, location)` if it existed.
    pub fn remove(&self, object_id: ObjectId) -> Option<(u32, BlockLocation)> {
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        self.shards[shard_idx]
            .lock()
            .remove(object_id)
            .map(|e| (e.size, e.location))
    }

    /// Check if an entry exists.
    pub fn contains(&self, object_id: ObjectId) -> bool {
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        self.shards[shard_idx].lock().get(object_id).is_some()
    }

    /// Get the location of an entry.
    pub fn location(&self, object_id: ObjectId) -> Option<BlockLocation> {
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        self.shards[shard_idx]
            .lock()
            .get(object_id)
            .map(|e| e.location)
    }

    /// Pin an entry (prevent eviction).
    pub fn pin(&self, object_id: ObjectId) {
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        if let Some(e) = self.shards[shard_idx].lock().get_mut(object_id) {
            e.pinned = true;
        }
    }

    /// Check if an entry is pinned (pending upload).
    pub fn is_pinned(&self, object_id: ObjectId) -> bool {
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        self.shards[shard_idx]
            .lock()
            .get(object_id)
            .map_or(false, |e| e.pinned)
    }

    /// Unpin an entry (allow eviction).
    pub fn unpin(&self, object_id: ObjectId) {
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        if let Some(e) = self.shards[shard_idx].lock().get_mut(object_id) {
            e.pinned = false;
        }
    }

    /// Update the location of an entry (e.g. after demotion from L1 to L2-only).
    pub fn set_location(&self, object_id: ObjectId, location: BlockLocation) {
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        if let Some(e) = self.shards[shard_idx].lock().get_mut(object_id) {
            e.location = location;
        }
    }

    /// Evict the oldest non-pinned L1 entry across all shards (demote to disk-only).
    /// Returns `(object_id, size)` of the evicted entry, or `None`.
    pub fn evict_l1(&self) -> Option<(ObjectId, u32)> {
        // Find oldest L1 entry across all shards.
        let mut oldest: Option<(usize, ObjectId, Instant, u32)> = None;
        for (shard_idx, shard) in self.shards.iter().enumerate() {
            let s = shard.lock();
            for entry in &s.entries {
                // Pinned blocks must stay on disk for upload safety, but they
                // can still be demoted from L1 RAM once they are in L2.
                if entry.location != BlockLocation::MemoryAndDisk {
                    continue;
                }
                if oldest.is_none() || entry.last_access < oldest.as_ref().unwrap().2 {
                    oldest = Some((shard_idx, entry.object_id, entry.last_access, entry.size));
                }
            }
        }
        let (shard_idx, id, _, size) = oldest?;
        let mut s = self.shards[shard_idx].lock();
        if let Some(e) = s.get_mut(id) {
            e.location = BlockLocation::DiskOnly;
            Some((id, size))
        } else {
            None
        }
    }

    /// Evict the oldest non-pinned L2 entry across all shards (full removal).
    /// Returns `(object_id, size)` of the evicted entry, or `None`.
    pub fn evict_l2(&self) -> Option<(ObjectId, u32)> {
        // Find oldest DiskOnly entry across all shards.
        let mut oldest: Option<(usize, ObjectId, Instant, u32)> = None;
        for (shard_idx, shard) in self.shards.iter().enumerate() {
            let s = shard.lock();
            for entry in &s.entries {
                if entry.pinned {
                    continue;
                }
                if entry.location != BlockLocation::DiskOnly {
                    continue;
                }
                if oldest.is_none() || entry.last_access < oldest.as_ref().unwrap().2 {
                    oldest = Some((shard_idx, entry.object_id, entry.last_access, entry.size));
                }
            }
        }
        let (shard_idx, id, _, size) = oldest?;
        self.shards[shard_idx].lock().remove(id);
        Some((id, size))
    }

    /// Collect statistics.
    pub fn stats(&self) -> LruStats {
        let mut s = LruStats::default();
        for shard in &self.shards {
            let shard = shard.lock();
            for entry in &shard.entries {
                let sz = entry.size as u64;
                s.total_bytes += sz;
                if entry.pinned {
                    s.pinned += 1;
                }
                match entry.location {
                    BlockLocation::MemoryAndDisk | BlockLocation::MemoryOnly => {
                        s.l1_entries += 1;
                        s.l1_bytes += sz;
                    }
                    BlockLocation::DiskOnly => {
                        s.l2_entries += 1;
                        s.l2_bytes += sz;
                    }
                }
            }
        }
        s
    }

    /// Total number of tracked entries.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    /// Load LRU state from a persisted checkpoint.
    ///
    /// `entries` must be sorted oldest-first.
    pub fn load_checkpoint(&self, entries: Vec<CheckpointEntry>) {
        if entries.is_empty() {
            return;
        }

        let now = Instant::now();
        let max_back_secs = Self::max_subtractable_secs(now);
        let denom = (entries.len() - 1) as u128;

        for (i, ce) in entries.iter().enumerate() {
            let shard_idx = (ce.object_id as usize) % SHARD_COUNT;
            // Synthesize access times in the safe subtractable range:
            // oldest is furthest in the past, newest is closest to `now`.
            let offset_secs = if denom == 0 || max_back_secs == 0 {
                0
            } else {
                let oldest_rank = (entries.len() - 1 - i) as u128;
                ((oldest_rank * max_back_secs as u128) / denom) as u64
            };
            let access = now.checked_sub(Duration::from_secs(offset_secs)).unwrap_or(now);
            self.shards[shard_idx].lock().insert(LruEntry {
                object_id: ce.object_id,
                size: ce.size,
                location: ce.location,
                pinned: ce.pinned,
                last_access: access,
            });
        }
    }

    /// Snapshot all entries for checkpoint persistence (oldest-first).
    pub fn snapshot_for_checkpoint(&self) -> Vec<CheckpointEntry> {
        // Collect all entries with their access times, then sort globally.
        let mut all: Vec<(Instant, CheckpointEntry)> = Vec::new();
        for shard in &self.shards {
            let s = shard.lock();
            for entry in &s.entries {
                all.push((
                    entry.last_access,
                    CheckpointEntry {
                        object_id: entry.object_id,
                        size: entry.size,
                        location: entry.location,
                        pinned: entry.pinned,
                    },
                ));
            }
        }
        all.sort_by_key(|(t, _)| *t);
        all.into_iter().map(|(_, e)| e).collect()
    }
}

/// Entry format for checkpoint persistence.
#[derive(Debug, Clone)]
pub struct CheckpointEntry {
    pub object_id: ObjectId,
    pub size: u32,
    pub location: BlockLocation,
    pub pinned: bool,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_contains() {
        let lru = LruTracker::new();
        assert!(!lru.contains(1));
        lru.insert(1, 1024, BlockLocation::MemoryAndDisk, false);
        assert!(lru.contains(1));
        assert_eq!(lru.len(), 1);
    }

    #[test]
    fn touch_updates_position() {
        let lru = LruTracker::new();
        // Use IDs that map to the same shard so we can test ordering.
        let a = 0u64;
        let b = SHARD_COUNT as u64; // same shard as 0
        lru.insert(a, 100, BlockLocation::DiskOnly, false);
        lru.insert(b, 200, BlockLocation::DiskOnly, false);

        // Force debounce bypass by manipulating entry directly.
        {
            let shard_idx = 0;
            let mut s = lru.shards[shard_idx].lock();
            let e = s.get_mut(a).unwrap();
            e.last_access -= std::time::Duration::from_secs(DEBOUNCE_SECS + 1);
        }

        lru.touch(a);
        // Now `a` should be newer than `b` — evict should pick `b` first.
        let evicted = lru.evict_l2();
        assert_eq!(evicted.map(|(id, _)| id), Some(b));
    }

    #[test]
    fn evict_l1_demotes() {
        let lru = LruTracker::new();
        lru.insert(42, 1024, BlockLocation::MemoryAndDisk, false);
        let evicted = lru.evict_l1();
        assert_eq!(evicted, Some((42, 1024)));
        // Entry should still exist but as DiskOnly.
        assert_eq!(lru.location(42), Some(BlockLocation::DiskOnly));
    }

    #[test]
    fn pin_prevents_eviction() {
        let lru = LruTracker::new();
        lru.insert(1, 100, BlockLocation::DiskOnly, true);
        assert!(lru.evict_l2().is_none());
        lru.unpin(1);
        assert_eq!(lru.evict_l2(), Some((1, 100)));
    }

    #[test]
    fn remove_returns_info() {
        let lru = LruTracker::new();
        lru.insert(5, 500, BlockLocation::MemoryAndDisk, false);
        let r = lru.remove(5);
        assert_eq!(r, Some((500, BlockLocation::MemoryAndDisk)));
        assert!(!lru.contains(5));
    }

    #[test]
    fn checkpoint_roundtrip() {
        let lru = LruTracker::new();
        lru.insert(1, 100, BlockLocation::DiskOnly, false);
        lru.insert(2, 200, BlockLocation::MemoryAndDisk, true);
        lru.insert(3, 300, BlockLocation::DiskOnly, false);

        let snap = lru.snapshot_for_checkpoint();
        assert_eq!(snap.len(), 3);

        let lru2 = LruTracker::new();
        lru2.load_checkpoint(snap.clone());
        assert_eq!(lru2.len(), 3);
        assert!(lru2.contains(1));
        assert!(lru2.contains(2));
        assert!(lru2.contains(3));
    }

    #[test]
    fn load_checkpoint_many_entries_does_not_panic() {
        let lru = LruTracker::new();
        let entries: Vec<CheckpointEntry> = (0..250_000u64)
            .map(|id| CheckpointEntry {
                object_id: id,
                size: 1,
                location: BlockLocation::DiskOnly,
                pinned: false,
            })
            .collect();
        lru.load_checkpoint(entries);
        assert_eq!(lru.len(), 250_000);
    }

    #[test]
    fn stats_aggregation() {
        let lru = LruTracker::new();
        lru.insert(1, 100, BlockLocation::MemoryAndDisk, false);
        lru.insert(2, 200, BlockLocation::DiskOnly, false);
        lru.insert(3, 300, BlockLocation::MemoryAndDisk, true);
        let s = lru.stats();
        assert_eq!(s.total_bytes, 600);
        assert_eq!(s.l1_entries, 2);
        assert_eq!(s.l1_bytes, 400);
        assert_eq!(s.l2_entries, 1);
        assert_eq!(s.l2_bytes, 200);
        assert_eq!(s.pinned, 1);
    }
}
