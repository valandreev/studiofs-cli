//! In-memory range cache using LRU eviction.

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;
use std::num::NonZeroUsize;
use studiofs_meta::Ino;
use tracing::debug;

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

/// In-memory LRU cache for file data blocks.
pub struct MemoryCache {
    inner: Mutex<LruCache<CacheKey, Bytes>>,
    /// Maximum total size in bytes.
    max_bytes: usize,
    /// Current total size in bytes.
    current_bytes: Mutex<usize>,
}

impl MemoryCache {
    /// Create a new memory cache with the given max capacity in bytes.
    pub fn new(max_bytes: usize) -> Self {
        // Entry count cap: generous enough that byte-level eviction always triggers first.
        let max_entries = NonZeroUsize::new((max_bytes / 64).max(1024)).unwrap();
        Self {
            inner: Mutex::new(LruCache::new(max_entries)),
            max_bytes,
            current_bytes: Mutex::new(0),
        }
    }

    /// Get a cached block. Returns None on miss.
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        self.inner.lock().get(key).cloned()
    }

    /// Insert a block into the cache, evicting LRU entries if needed.
    pub fn put(&self, key: CacheKey, data: Bytes) {
        let data_len = data.len();

        // Evict until we have space
        self.evict_to_fit(data_len);

        let mut cache = self.inner.lock();
        if let Some(old) = cache.put(key, data) {
            // Replace — adjust size for old entry
            let mut current = self.current_bytes.lock();
            *current = current.saturating_sub(old.len());
            *current += data_len;
        } else {
            *self.current_bytes.lock() += data_len;
        }
    }

    /// Remove a specific entry.
    pub fn remove(&self, key: &CacheKey) -> Option<Bytes> {
        let mut cache = self.inner.lock();
        if let Some(data) = cache.pop(key) {
            *self.current_bytes.lock() -= data.len();
            Some(data)
        } else {
            None
        }
    }

    /// Remove all entries for a given inode.
    pub fn invalidate_inode(&self, ino: Ino) {
        let mut cache = self.inner.lock();
        let mut current = self.current_bytes.lock();

        // Collect keys to remove
        let keys: Vec<CacheKey> = cache
            .iter()
            .filter(|(k, _)| k.ino == ino)
            .map(|(k, _)| *k)
            .collect();

        for key in keys {
            if let Some(data) = cache.pop(&key) {
                *current = current.saturating_sub(data.len());
            }
        }
    }

    /// Evict LRU entries until we have at least `needed` bytes free.
    fn evict_to_fit(&self, needed: usize) {
        let mut cache = self.inner.lock();
        let mut current = self.current_bytes.lock();

        while *current + needed > self.max_bytes {
            if let Some((key, data)) = cache.pop_lru() {
                debug!(
                    ino = key.ino,
                    offset = key.offset,
                    bytes = data.len(),
                    "evicting from memory cache"
                );
                *current = current.saturating_sub(data.len());
            } else {
                break;
            }
        }
    }

    /// Current memory usage in bytes.
    pub fn current_bytes(&self) -> usize {
        *self.current_bytes.lock()
    }

    /// Maximum capacity in bytes.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Number of entries in cache.
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
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
        // Cache is full. Adding k3 should evict k1 (LRU)
        cache.put(k3, Bytes::from(vec![3u8; 1024]));

        assert!(cache.get(&k1).is_none()); // evicted
        assert!(cache.get(&k2).is_some());
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
}
