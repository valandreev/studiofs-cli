//! In-memory range cache using LRU eviction.
//!
//! Uses 64 independent shards keyed by `(ino XOR (offset >> 22)) % 64`.
//! Each shard has its own `Mutex<LruCache>`, so concurrent readers on
//! different inodes / offsets never contend.

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use studiofs_meta::Ino;
use tracing::debug;

/// Number of LRU shards in MemoryCache.
const SHARD_COUNT: usize = 64;

/// Key for the memory cache: (inode, block offset in file).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub ino: Ino,
    /// Absolute byte offset of the block start in the file.
    pub offset: u64,
}

impl CacheKey {
    pub fn new(ino: Ino, offset: u64) -> Self {
        Self { ino, offset }
    }
}

/// Map a key to a shard index.
#[inline]
fn shard_idx(key: &CacheKey) -> usize {
    ((key.ino ^ (key.offset >> 22)) as usize) % SHARD_COUNT
}

/// In-memory LRU cache for file data blocks.
///
/// Internally split into `SHARD_COUNT` independent shards to reduce
/// mutex contention under parallel reads.
pub struct MemoryCache {
    shards: Vec<Mutex<LruCache<CacheKey, Bytes>>>,
    /// Maximum total size across all shards in bytes.
    max_bytes: usize,
    /// Current total size (lock-free atomic).
    total_bytes: AtomicUsize,
}

impl MemoryCache {
    /// Create a new memory cache with the given max capacity in bytes.
    pub fn new(max_bytes: usize) -> Self {
        // Per-shard entry cap: generous so byte-level eviction triggers first.
        let per_shard_entries = ((max_bytes / SHARD_COUNT) / 64).max(256);
        let cap = NonZeroUsize::new(per_shard_entries).unwrap();
        let shards = (0..SHARD_COUNT)
            .map(|_| Mutex::new(LruCache::new(cap)))
            .collect();
        Self {
            shards,
            max_bytes,
            total_bytes: AtomicUsize::new(0),
        }
    }

    /// Get a cached block. Returns None on miss.
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        self.shards[shard_idx(key)].lock().get(key).cloned()
    }

    /// Insert a block into the cache, evicting LRU entries if needed.
    pub fn put(&self, key: CacheKey, data: Bytes) {
        let data_len = data.len();

        // Evict globally until we have room.
        self.evict_to_fit(data_len);

        let mut shard = self.shards[shard_idx(&key)].lock();
        if let Some(old) = shard.put(key, data) {
            // Replacing existing entry — credit back old size first.
            self.total_bytes.fetch_sub(old.len(), Ordering::Relaxed);
        }
        self.total_bytes.fetch_add(data_len, Ordering::Relaxed);
    }

    /// Remove a specific entry.
    pub fn remove(&self, key: &CacheKey) -> Option<Bytes> {
        let mut shard = self.shards[shard_idx(key)].lock();
        if let Some(data) = shard.pop(key) {
            self.total_bytes.fetch_sub(data.len(), Ordering::Relaxed);
            Some(data)
        } else {
            None
        }
    }

    /// Remove all entries for a given inode.
    pub fn invalidate_inode(&self, ino: Ino) {
        for shard in &self.shards {
            let mut s = shard.lock();
            let keys: Vec<CacheKey> = s
                .iter()
                .filter(|(k, _)| k.ino == ino)
                .map(|(k, _)| *k)
                .collect();
            for key in keys {
                if let Some(data) = s.pop(&key) {
                    self.total_bytes.fetch_sub(data.len(), Ordering::Relaxed);
                }
            }
        }
    }

    /// Clear all cached entries.
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.lock().clear();
        }
        self.total_bytes.store(0, Ordering::Relaxed);
    }

    /// Evict LRU entries globally until we have at least `needed` bytes free.
    fn evict_to_fit(&self, needed: usize) {
        // Round-robin across shards to find and evict LRU entries until enough
        // space is available.  We iterate shards in order; each pass may evict
        // at most one entry per shard so we loop until done or nothing left.
        let mut shard_cursor = 0usize;
        loop {
            if self.total_bytes.load(Ordering::Relaxed) + needed <= self.max_bytes {
                break;
            }

            // Try to pop LRU from current shard
            let mut s = self.shards[shard_cursor % SHARD_COUNT].lock();
            if let Some((key, data)) = s.pop_lru() {
                debug!(
                    ino = key.ino,
                    offset = key.offset,
                    bytes = data.len(),
                    "evicting from memory cache"
                );
                self.total_bytes.fetch_sub(data.len(), Ordering::Relaxed);
            } else {
                // Shard is empty — check if all shards exhausted
                drop(s);
                shard_cursor += 1;
                if shard_cursor >= SHARD_COUNT * 2 {
                    // Iterated every shard twice with nothing left
                    break;
                }
                continue;
            }

            shard_cursor = (shard_cursor + 1) % SHARD_COUNT;
        }
    }

    /// Current memory usage in bytes (lock-free).
    pub fn current_bytes(&self) -> usize {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Maximum capacity in bytes.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Number of entries in cache (sum across all shards).
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.lock().is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let cache = MemoryCache::new(1024 * 1024);
        let key = CacheKey::new(1, 0);
        let data = Bytes::from(vec![42u8; 1024]);

        cache.put(key, data.clone());
        let got = cache.get(&key).unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn test_eviction() {
        let cache = MemoryCache::new(2048); // tiny cache
        let k1 = CacheKey::new(1, 0);
        let k2 = CacheKey::new(1, 4096);
        let k3 = CacheKey::new(1, 8192);

        cache.put(k1, Bytes::from(vec![1u8; 1024]));
        cache.put(k2, Bytes::from(vec![2u8; 1024]));
        // Cache is full. Adding k3 should evict one of k1/k2 (LRU)
        cache.put(k3, Bytes::from(vec![3u8; 1024]));

        // Only 2 entries should remain (one was evicted to make room)
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&k3).is_some());
    }

    #[test]
    fn test_invalidate_inode() {
        let cache = MemoryCache::new(1024 * 1024);
        cache.put(CacheKey::new(1, 0), Bytes::from(vec![0u8; 100]));
        cache.put(CacheKey::new(1, 4096), Bytes::from(vec![0u8; 100]));
        cache.put(CacheKey::new(2, 0), Bytes::from(vec![0u8; 100]));

        cache.invalidate_inode(1);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(&CacheKey::new(2, 0)).is_some());
    }

    #[test]
    fn test_shard_distribution() {
        // Verify entries land in different shards.
        // Keys with different ino should spread across shards.
        let inos: Vec<u64> = (0..128).collect();
        let mut shard_set = std::collections::HashSet::new();
        for ino in &inos {
            shard_set.insert(shard_idx(&CacheKey::new(*ino, 0)));
        }
        // With 128 different inodes we should hit > 50% of shards
        assert!(shard_set.len() > SHARD_COUNT / 2);
    }

    #[test]
    fn test_current_bytes_atomic() {
        let cache = MemoryCache::new(1024 * 1024);
        assert_eq!(cache.current_bytes(), 0);
        cache.put(CacheKey::new(1, 0), Bytes::from(vec![0u8; 512]));
        assert_eq!(cache.current_bytes(), 512);
        cache.remove(&CacheKey::new(1, 0));
        assert_eq!(cache.current_bytes(), 0);
    }
}
