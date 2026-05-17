//! Lightweight in-memory cache surface used by `studiofs test-cache`.
//!
//! The production VFS path uses the local payload backend directly. This module
//! retains the small L1 helper surface needed by direct benchmark workflows.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::metrics::{CachePipelineMetrics, CachePipelineMetricsSnapshot};

/// Stable identifier for cache payload entries in benchmark cache workflows.
pub type ObjectId = u64;

/// Default in-memory benchmark cache size: 2 GiB.
pub const DEFAULT_MEMORY_CACHE_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// Tuning configuration used when opening a benchmark cache.
#[derive(Debug, Clone)]
pub struct BenchmarkCacheConfig {
    /// In-memory L1 payload budget in bytes.
    pub memory_cache_size: u64,
    /// Root directory used for benchmark cache artifacts.
    pub cache_dir: PathBuf,
}

impl BenchmarkCacheConfig {
    /// Construct a benchmark cache configuration.
    pub fn new(cache_dir: impl Into<PathBuf>, memory_cache_size: u64) -> Self {
        Self {
            memory_cache_size,
            cache_dir: cache_dir.into(),
        }
    }
}

/// Logical read-path outcome reported for a cache lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheReadLevel {
    /// Object came from the in-memory L1 path.
    L1,
    /// Object was absent from cache and treated as a miss.
    Miss,
}

/// Per-lookup trace for cache reads.
#[derive(Debug, Clone)]
pub struct CacheReadTrace {
    /// Cache source used to fulfill the lookup.
    pub level: CacheReadLevel,
    /// Time spent resolving the object within the benchmark cache.
    pub l1_lookup_latency: Duration,
}

/// Snapshot of benchmark cache counters.
#[derive(Debug, Clone, Default)]
pub struct BenchmarkCacheStats {
    /// Bytes currently cached in the in-memory L1 layer.
    pub l1_bytes: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct BytesTracked(u64);

impl BytesTracked {
    fn add_len(&mut self, len: usize) {
        self.0 = self.0.saturating_add(bytes_len(len));
    }

    fn sub_len(&mut self, len: usize) {
        self.0 = self.0.saturating_sub(bytes_len(len));
    }

    fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Default)]
struct L1State {
    by_object: BTreeMap<ObjectId, Bytes>,
    insertion_order: VecDeque<ObjectId>,
    total_bytes: BytesTracked,
}

fn bytes_len(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

impl L1State {
    fn get_cloned(&self, object_id: ObjectId) -> Option<Bytes> {
        self.by_object.get(&object_id).cloned()
    }

    fn contains(&self, object_id: ObjectId) -> bool {
        self.by_object.contains_key(&object_id)
    }

    fn insert(&mut self, object_id: ObjectId, data: Bytes, max_bytes: u64) {
        let is_new = !self.by_object.contains_key(&object_id);
        if let Some(previous) = self.by_object.remove(&object_id) {
            self.total_bytes.sub_len(previous.len());
        }
        self.total_bytes.add_len(data.len());
        self.by_object.insert(object_id, data);
        if is_new {
            self.insertion_order.push_back(object_id);
        }
        self.evict_to_limit(max_bytes);
    }

    fn remove(&mut self, object_id: ObjectId) -> bool {
        let Some(removed) = self.by_object.remove(&object_id) else {
            return false;
        };
        self.total_bytes.sub_len(removed.len());
        true
    }

    fn evict_to_limit(&mut self, max_bytes: u64) {
        while self.total_bytes.get() > max_bytes {
            let Some(candidate) = self.insertion_order.pop_front() else {
                break;
            };
            if let Some(removed) = self.by_object.remove(&candidate) {
                self.total_bytes.sub_len(removed.len());
            }
        }
    }
}

/// Compact benchmark cache with staged in-memory payload cache and metrics snapshot hooks.
pub struct BenchmarkCache {
    /// Cache configuration used for current instance.
    config: BenchmarkCacheConfig,
    /// Exclusive in-memory state for L1 objects.
    l1: Mutex<L1State>,
    /// Current L1 byte limit used for evictions.
    l1_max_bytes: AtomicU64,
    /// Metrics collector used for benchmark observability.
    metrics: CachePipelineMetrics,
}

impl BenchmarkCache {
    /// Open a benchmark cache and initialize fresh in-memory state.
    pub fn open(config: BenchmarkCacheConfig) -> io::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            l1: Mutex::new(L1State::default()),
            l1_max_bytes: AtomicU64::new(config.memory_cache_size.max(1)),
            metrics: CachePipelineMetrics::default(),
            config,
        }))
    }

    /// Get a cached payload value, returning `None` on miss.
    pub async fn get(&self, object_id: ObjectId) -> Option<Bytes> {
        self.get_with_trace(object_id).await.0
    }

    /// Get a payload and detailed hit/miss read trace.
    pub async fn get_with_trace(&self, object_id: ObjectId) -> (Option<Bytes>, CacheReadTrace) {
        let started = Instant::now();
        let l1_hit = self.l1.lock().get_cloned(object_id);
        let l1_latency = started.elapsed();
        match l1_hit {
            Some(data) => (
                Some(data),
                CacheReadTrace {
                    level: CacheReadLevel::L1,
                    l1_lookup_latency: l1_latency,
                },
            ),
            None => (
                None,
                CacheReadTrace {
                    level: CacheReadLevel::Miss,
                    l1_lookup_latency: l1_latency,
                },
            ),
        }
    }

    /// Store bytes into L1 cache asynchronously.
    pub async fn put(&self, object_id: ObjectId, data: Bytes) -> io::Result<()> {
        self.stage_l1(object_id, data);
        Ok(())
    }

    /// Remove payload bytes asynchronously from cache.
    pub async fn remove(&self, object_id: ObjectId) {
        self.remove_sync(object_id);
    }

    /// Remove payload bytes synchronously from cache.
    pub fn remove_sync(&self, object_id: ObjectId) {
        let _ = self.l1.lock().remove(object_id);
    }

    /// Check whether payload exists in cache.
    pub fn contains(&self, object_id: ObjectId) -> bool {
        self.l1.lock().contains(object_id)
    }

    /// Insert payload into L1 cache state.
    pub fn stage_l1(&self, object_id: ObjectId, data: Bytes) {
        let max_bytes = self.l1_max_bytes.load(Ordering::Acquire).max(1);
        self.l1.lock().insert(object_id, data, max_bytes);
    }

    /// Read current cache statistics snapshot.
    pub fn stats(&self) -> BenchmarkCacheStats {
        BenchmarkCacheStats {
            l1_bytes: self.l1_bytes(),
        }
    }

    /// Current number of bytes retained in L1.
    pub fn l1_bytes(&self) -> u64 {
        self.l1.lock().total_bytes.get()
    }

    /// Return configured L1 byte budget.
    pub fn memory_cache_size(&self) -> u64 {
        self.l1_max_bytes.load(Ordering::Acquire)
    }

    /// Return benchmark cache directory for persistence side effects.
    pub fn cache_dir(&self) -> &Path {
        &self.config.cache_dir
    }

    /// Return latest pipeline metrics snapshot.
    pub fn cache_pipeline_metrics(&self) -> CachePipelineMetricsSnapshot {
        self.metrics.snapshot(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_with_trace_reports_hit_and_miss() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache =
            BenchmarkCache::open(BenchmarkCacheConfig::new(dir.path(), 1024)).expect("open");

        let (miss, miss_trace) = cache.get_with_trace(10).await;
        assert!(miss.is_none());
        assert_eq!(miss_trace.level, CacheReadLevel::Miss);

        cache
            .put(10, Bytes::from_static(b"payload"))
            .await
            .expect("put");
        let (hit, hit_trace) = cache.get_with_trace(10).await;
        assert_eq!(hit.expect("payload").as_ref(), b"payload");
        assert_eq!(hit_trace.level, CacheReadLevel::L1);
    }

    #[tokio::test]
    async fn evicts_oldest_entry_when_over_limit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = BenchmarkCache::open(BenchmarkCacheConfig::new(dir.path(), 4)).expect("open");

        cache
            .put(1, Bytes::from_static(b"aaaa"))
            .await
            .expect("put first");
        cache
            .put(2, Bytes::from_static(b"bb"))
            .await
            .expect("put second");

        assert!(!cache.contains(1));
        assert!(cache.contains(2));
    }

    #[tokio::test]
    async fn remove_clears_visibility() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache =
            BenchmarkCache::open(BenchmarkCacheConfig::new(dir.path(), 1024)).expect("open");

        cache
            .put(55, Bytes::from_static(b"abc"))
            .await
            .expect("put");
        cache.remove(55).await;

        assert!(!cache.contains(55));
        assert!(cache.get(55).await.is_none());
    }
}
