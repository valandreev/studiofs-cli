//! L1 Memory Cache — in-memory block cache keyed by ObjectId.
//!
//! Sharded `HashMap<ObjectId, Bytes>` with no internal LRU — all eviction
//! decisions are driven by the global [`LruTracker`](super::lru_tracker::LruTracker).
//!
//! 64 shards, shard = `object_id % 64`.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

// =============================================================================
// Constants
// =============================================================================

const SHARD_COUNT: usize = 64;

// =============================================================================
// L1MemCache
// =============================================================================

/// In-memory (L1) block cache.
pub struct L1MemCache {
    shards: Vec<Mutex<HashMap<u64, Bytes>>>,
    /// Current total bytes stored.
    total_bytes: AtomicU64,
    /// Budget in bytes.
    max_bytes: u64,
}

impl L1MemCache {
    /// Create a new L1 cache with the given byte budget.
    pub fn new(max_bytes: u64) -> Self {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(Mutex::new(HashMap::new()));
        }
        Self {
            shards,
            total_bytes: AtomicU64::new(0),
            max_bytes,
        }
    }

    /// Look up a block.  Returns `None` on miss.
    pub fn get(&self, object_id: u64) -> Option<Bytes> {
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        self.shards[shard_idx].lock().get(&object_id).cloned()
    }

    /// Insert a block.  If the key already existed, replaces it and adjusts
    /// the byte counter.
    pub fn insert(&self, object_id: u64, data: Bytes) {
        let new_len = data.len() as u64;
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        let old = self.shards[shard_idx].lock().insert(object_id, data);
        if let Some(old_data) = old {
            // Replace: subtract old, add new.
            let old_len = old_data.len() as u64;
            if new_len >= old_len {
                self.total_bytes
                    .fetch_add(new_len - old_len, Ordering::Relaxed);
            } else {
                self.total_bytes
                    .fetch_sub(old_len - new_len, Ordering::Relaxed);
            }
        } else {
            self.total_bytes.fetch_add(new_len, Ordering::Relaxed);
        }
    }

    /// Remove a block.  Returns the data if it was present.
    pub fn remove(&self, object_id: u64) -> Option<Bytes> {
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        let removed = self.shards[shard_idx].lock().remove(&object_id);
        if let Some(ref data) = removed {
            self.total_bytes
                .fetch_sub(data.len() as u64, Ordering::Relaxed);
        }
        removed
    }

    /// Check if a block exists.
    pub fn contains(&self, object_id: u64) -> bool {
        let shard_idx = (object_id as usize) % SHARD_COUNT;
        self.shards[shard_idx].lock().contains_key(&object_id)
    }

    /// Current total bytes in memory.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Byte budget.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Whether the cache is over budget.
    pub fn over_budget(&self) -> bool {
        self.total_bytes() > self.max_bytes
    }

    /// Total number of entries across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all entries.
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.lock().clear();
        }
        self.total_bytes.store(0, Ordering::Relaxed);
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_remove() {
        let cache = L1MemCache::new(1024 * 1024);
        let data = Bytes::from_static(b"hello");
        cache.insert(1, data.clone());
        assert_eq!(cache.get(1).unwrap(), data);
        assert_eq!(cache.total_bytes(), 5);
        assert_eq!(cache.len(), 1);

        cache.remove(1);
        assert!(cache.get(1).is_none());
        assert_eq!(cache.total_bytes(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn replace_adjusts_bytes() {
        let cache = L1MemCache::new(1024 * 1024);
        cache.insert(1, Bytes::from_static(b"short"));
        assert_eq!(cache.total_bytes(), 5);
        cache.insert(1, Bytes::from_static(b"longer data"));
        assert_eq!(cache.total_bytes(), 11);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn over_budget() {
        let cache = L1MemCache::new(10);
        assert!(!cache.over_budget());
        cache.insert(1, Bytes::from(vec![0u8; 11]));
        assert!(cache.over_budget());
    }

    #[test]
    fn clear_resets() {
        let cache = L1MemCache::new(1024);
        cache.insert(1, Bytes::from_static(b"a"));
        cache.insert(2, Bytes::from_static(b"bb"));
        assert_eq!(cache.len(), 2);
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.total_bytes(), 0);
    }
}
