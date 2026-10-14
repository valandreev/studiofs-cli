//! Reusable reply storage for queued macFUSE reads.
//!
//! Pooled storage stays initialized across replies, so a recycled buffer still
//! holds bytes from earlier reads, possibly of other inodes. Reply safety
//! therefore depends on the `StudioVfs::read_into_fh` contract: on success,
//! every byte up to the returned length is overwritten, including zero-filled
//! holes. Callers must publish exactly that prefix through
//! [`PooledReadBuffer::limit_reply_len`] and must never reply with the buffer
//! after an error or cancellation.

use std::cmp::Reverse;
use std::fmt;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex as ParkingMutex;

const MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS: usize = 32;
pub(super) const MACFUSE_READ_BUFFER_POOL_MAX_RETAINED_BYTES: usize = 32 * 1024 * 1024;
pub(super) const MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY: usize = 8 * 1024 * 1024;
/// Debug and test builds overwrite lent storage with this byte so a VFS path
/// that under-fills its reported prefix fails visibly instead of replying with
/// stale bytes from an earlier read.
#[cfg(any(test, debug_assertions))]
pub(super) const MACFUSE_READ_BUFFER_POISON: u8 = 0xdb;

/// Bookkeeping snapshot of a [`ReadBufferPool`], reported through runtime stats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReadBufferPoolStats {
    pub(super) hits: u64,
    pub(super) misses: u64,
    pub(super) retained_bytes: usize,
}

#[derive(Default)]
struct PoolState {
    /// Idle buffers in recycle order: index 0 is the oldest and is evicted first.
    buffers: Vec<Vec<u8>>,
    retained_bytes: usize,
    hits: u64,
    misses: u64,
}

#[derive(Default)]
pub(super) struct ReadBufferPool {
    state: ParkingMutex<PoolState>,
    #[cfg(test)]
    freshly_zeroed_bytes: AtomicUsize,
}

impl fmt::Debug for ReadBufferPool {
    // Pooled storage holds stale file data, so Debug reports only bookkeeping.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadBufferPool")
            .field("available", &self.state.lock().buffers.len())
            .finish_non_exhaustive()
    }
}

impl ReadBufferPool {
    /// Lend `len` initialized bytes with unspecified, possibly stale contents.
    ///
    /// The whole storage is reply-visible until the caller limits it to the
    /// prefix that the VFS actually overwrote.
    pub(super) fn acquire(self: &Arc<Self>, len: usize) -> PooledReadBuffer {
        #[cfg_attr(not(any(test, debug_assertions)), expect(unused_mut))]
        let mut storage = self.take_storage(len);
        #[cfg(any(test, debug_assertions))]
        storage[..len].fill(MACFUSE_READ_BUFFER_POISON);
        PooledReadBuffer {
            pool: Arc::clone(self),
            storage,
            reply_len: len,
        }
    }

    /// Reuse initialized storage from the request's power-of-two size class, or
    /// allocate zeroed storage of that class. Requests above the pool capacity
    /// keep an exact-sized allocation and are never retained.
    fn take_storage(&self, len: usize) -> Vec<u8> {
        if len == 0 {
            return Vec::new();
        }
        let class_len = len
            .checked_next_power_of_two()
            .filter(|&class_len| class_len <= MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY);
        let mut state = self.state.lock();
        if let Some(class_len) = class_len
            && let Some(index) = state
                .buffers
                .iter()
                .enumerate()
                .filter(|(_, buffer)| {
                    buffer.len() >= len
                        && buffer.len().checked_next_power_of_two() == Some(class_len)
                })
                // Among equal capacities, reuse the most recently recycled
                // buffer: it is likely still cache-hot.
                .min_by_key(|(index, buffer)| (buffer.capacity(), Reverse(*index)))
                .map(|(index, _)| index)
        {
            let storage = state.buffers.remove(index);
            state.retained_bytes -= storage.capacity();
            state.hits += 1;
            return storage;
        }
        state.misses += 1;
        drop(state);
        let alloc_len = class_len.unwrap_or(len);
        #[cfg(test)]
        self.freshly_zeroed_bytes
            .fetch_add(alloc_len, Ordering::Relaxed);
        vec![0; alloc_len]
    }

    /// Retain `storage` as the newest idle buffer, evicting the oldest buffers
    /// while the count or byte budget is exceeded. The new buffer is last and
    /// can never alone exceed the budget, so it is never the one evicted.
    fn recycle(&self, storage: Vec<u8>) {
        // `mem::take` during `Drop` leaves a zero-capacity placeholder; recycling
        // it would only occupy a pool slot without avoiding a future allocation.
        if storage.capacity() == 0 || storage.capacity() > MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY {
            return;
        }
        let evicted = {
            let mut state = self.state.lock();
            state.retained_bytes += storage.capacity();
            state.buffers.push(storage);
            let mut evicted = Vec::new();
            while state.buffers.len() > MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS
                || state.retained_bytes > MACFUSE_READ_BUFFER_POOL_MAX_RETAINED_BYTES
            {
                if state.buffers.is_empty() {
                    break;
                }
                let oldest = state.buffers.remove(0);
                state.retained_bytes -= oldest.capacity();
                evicted.push(oldest);
            }
            evicted
        };
        // Freeing multi-MiB buffers can unmap memory; keep that work off the
        // shared lock so concurrent acquires and recycles are not serialized on it.
        drop(evicted);
    }

    pub(super) fn snapshot(&self) -> ReadBufferPoolStats {
        let state = self.state.lock();
        ReadBufferPoolStats {
            hits: state.hits,
            misses: state.misses,
            retained_bytes: state.retained_bytes,
        }
    }

    #[cfg(test)]
    pub(super) fn available_count(&self) -> usize {
        self.state.lock().buffers.len()
    }

    #[cfg(test)]
    pub(super) fn last_available_capacity(&self) -> Option<usize> {
        self.state.lock().buffers.last().map(Vec::capacity)
    }

    #[cfg(test)]
    pub(super) fn freshly_zeroed_bytes(&self) -> usize {
        self.freshly_zeroed_bytes.load(Ordering::Relaxed)
    }
}

/// Pooled read storage whose reply-visible prefix is `reply_len`.
///
/// Invariant: `reply_len <= storage.len()`. Bytes past `reply_len` stay
/// initialized so later reads can reuse them without zero-filling.
pub(super) struct PooledReadBuffer {
    pool: Arc<ReadBufferPool>,
    storage: Vec<u8>,
    reply_len: usize,
}

impl PooledReadBuffer {
    /// Shrink the reply-visible prefix to at most `len` bytes; never grows it.
    pub(super) fn limit_reply_len(&mut self, len: usize) {
        self.reply_len = self.reply_len.min(len);
    }
}

impl fmt::Debug for PooledReadBuffer {
    // Storage past `reply_len` may hold another inode's bytes; never print it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledReadBuffer")
            .field("reply_len", &self.reply_len)
            .field("initialized_len", &self.storage.len())
            .field("capacity", &self.storage.capacity())
            .finish_non_exhaustive()
    }
}

impl Deref for PooledReadBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        debug_assert!(self.reply_len <= self.storage.len());
        &self.storage[..self.reply_len]
    }
}

impl DerefMut for PooledReadBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        debug_assert!(self.reply_len <= self.storage.len());
        &mut self.storage[..self.reply_len]
    }
}

impl AsRef<[u8]> for PooledReadBuffer {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl Drop for PooledReadBuffer {
    fn drop(&mut self) {
        self.pool.recycle(mem::take(&mut self.storage));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_reply_len_exposes_only_the_overwritten_prefix() {
        let pool = Arc::new(ReadBufferPool::default());
        let mut buffer = pool.acquire(16);
        buffer[..5].copy_from_slice(b"short");

        buffer.limit_reply_len(5);

        assert_eq!(buffer.as_ref(), b"short");
    }

    #[test]
    fn limit_reply_len_never_grows_the_reply() {
        let pool = Arc::new(ReadBufferPool::default());
        let mut buffer = pool.acquire(8);
        buffer.limit_reply_len(3);

        buffer.limit_reply_len(8);

        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn acquire_reuses_initialized_storage_without_zero_fill() {
        let pool = Arc::new(ReadBufferPool::default());
        let mut first = pool.acquire(16);
        first.limit_reply_len(0);
        drop(first);
        let zeroed_after_warmup = pool.freshly_zeroed_bytes();

        drop(pool.acquire(12));
        drop(pool.acquire(16));

        assert_eq!(zeroed_after_warmup, 16);
        assert_eq!(pool.freshly_zeroed_bytes(), zeroed_after_warmup);
        assert_eq!(pool.available_count(), 1);
    }

    #[test]
    fn acquire_prefers_initialized_storage_large_enough_for_the_request() {
        let pool = Arc::new(ReadBufferPool::default());
        let large = pool.acquire(64);
        let small = pool.acquire(8);
        drop(large);
        drop(small);
        let zeroed_before = pool.freshly_zeroed_bytes();

        let reused = pool.acquire(64);

        assert_eq!(pool.freshly_zeroed_bytes(), zeroed_before);
        assert_eq!(reused.len(), 64);
        assert_eq!(pool.available_count(), 1);
    }

    #[test]
    fn acquire_reuses_the_most_recently_recycled_buffer_among_equal_capacities() {
        let pool = Arc::new(ReadBufferPool::default());
        let older = pool.acquire(16);
        let newer = pool.acquire(16);
        let newer_ptr = newer.storage.as_ptr();
        drop(older);
        drop(newer);

        let reused = pool.acquire(16);

        assert_eq!(reused.storage.as_ptr(), newer_ptr);
        assert_eq!(pool.available_count(), 1);
    }

    #[test]
    fn acquire_allocates_class_sized_storage_and_keeps_other_classes_idle() {
        let pool = Arc::new(ReadBufferPool::default());
        drop(pool.acquire(16));

        drop(pool.acquire(20));

        // 16 -> class 16; 20 -> class 32 (a miss, zeroed as 32 bytes).
        assert_eq!(pool.freshly_zeroed_bytes(), 16 + 32);
        assert_eq!(pool.available_count(), 2);
        assert_eq!(pool.last_available_capacity(), Some(32));
    }

    #[test]
    fn acquire_reuses_storage_across_a_power_of_two_size_class() {
        let pool = Arc::new(ReadBufferPool::default());
        drop(pool.acquire(65));
        let zeroed_before = pool.freshly_zeroed_bytes();

        let reused = pool.acquire(100);

        assert_eq!(reused.storage.len(), 128);
        assert_eq!(reused.len(), 100);
        assert_eq!(pool.freshly_zeroed_bytes(), zeroed_before);
        assert_eq!(
            pool.snapshot(),
            ReadBufferPoolStats {
                hits: 1,
                misses: 1,
                retained_bytes: 0,
            }
        );
    }

    #[test]
    fn small_request_does_not_reuse_a_large_buffer() {
        let pool = Arc::new(ReadBufferPool::default());
        drop(pool.acquire(8 * 1024 * 1024));

        let small = pool.acquire(128 * 1024);

        assert_eq!(small.storage.len(), 128 * 1024);
        assert_eq!(
            pool.snapshot(),
            ReadBufferPoolStats {
                hits: 0,
                misses: 2,
                retained_bytes: 8 * 1024 * 1024,
            }
        );
    }

    #[test]
    fn recycle_evicts_oldest_buffers_to_stay_within_the_retained_budget() {
        let pool = ReadBufferPool::default();
        let mut large_ptrs = Vec::new();
        for _ in 0..5 {
            let capacity = 8 * 1024 * 1024;
            let mut storage = Vec::with_capacity(capacity);
            storage.resize(1, 0);
            large_ptrs.push(storage.as_ptr());
            pool.recycle(storage);
        }

        assert_eq!(
            pool.snapshot().retained_bytes,
            MACFUSE_READ_BUFFER_POOL_MAX_RETAINED_BYTES
        );
        assert_eq!(pool.available_count(), 4);
        assert_eq!(pool.last_available_capacity(), Some(8 * 1024 * 1024));
        let state = pool.state.lock();
        assert!(
            state
                .buffers
                .iter()
                .all(|buffer| buffer.as_ptr() != large_ptrs[0])
        );
        assert!(
            large_ptrs[1..]
                .iter()
                .all(|ptr| state.buffers.iter().any(|buffer| buffer.as_ptr() == *ptr))
        );
    }

    #[test]
    fn recycle_under_count_pressure_keeps_the_newly_recycled_larger_buffer() {
        let pool = Arc::new(ReadBufferPool::default());
        let held: Vec<_> = (0..MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS)
            .map(|_| pool.acquire(4 * 1024))
            .collect();
        drop(held);
        assert_eq!(pool.available_count(), MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS);

        for _ in 0..10 {
            drop(pool.acquire(1024 * 1024));
        }

        // Only the first large read misses: its recycle evicts the oldest 4 KiB
        // buffer instead of itself, so the remaining nine reads reuse it.
        assert_eq!(
            pool.snapshot(),
            ReadBufferPoolStats {
                hits: 9,
                misses: MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS as u64 + 1,
                retained_bytes: (MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS - 1) * 4 * 1024 + 1024 * 1024,
            }
        );
    }

    #[test]
    fn recycle_under_byte_pressure_keeps_the_newly_recycled_buffer() {
        let pool = Arc::new(ReadBufferPool::default());
        let held: Vec<_> = (0..MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS)
            .map(|_| pool.acquire(1024 * 1024))
            .collect();
        drop(held);
        assert_eq!(
            pool.snapshot().retained_bytes,
            MACFUSE_READ_BUFFER_POOL_MAX_RETAINED_BYTES
        );

        for _ in 0..10 {
            drop(pool.acquire(4 * 1024 * 1024));
        }

        // The first 4 MiB recycle evicts the four oldest 1 MiB buffers to fit the
        // budget; the remaining nine reads reuse the retained 4 MiB buffer.
        assert_eq!(
            pool.snapshot(),
            ReadBufferPoolStats {
                hits: 9,
                misses: MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS as u64 + 1,
                retained_bytes: MACFUSE_READ_BUFFER_POOL_MAX_RETAINED_BYTES,
            }
        );
        assert_eq!(
            pool.available_count(),
            MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS - 4 + 1
        );
    }

    #[test]
    fn empty_and_oversized_requests_are_not_retained() {
        let pool = Arc::new(ReadBufferPool::default());
        assert!(pool.acquire(0).is_empty());
        drop(pool.acquire(MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY + 1));

        assert_eq!(
            pool.snapshot(),
            ReadBufferPoolStats {
                hits: 0,
                misses: 1,
                retained_bytes: 0,
            }
        );
        assert_eq!(pool.available_count(), 0);
    }

    #[test]
    fn recycle_caps_the_buffer_count() {
        let pool = ReadBufferPool::default();
        for _ in 0..=MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS {
            pool.recycle(vec![0]);
        }

        assert_eq!(pool.available_count(), MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS);
        assert_eq!(
            pool.snapshot().retained_bytes,
            MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS
        );
    }

    #[test]
    fn acquire_poisons_recycled_storage_in_test_builds() {
        let pool = Arc::new(ReadBufferPool::default());
        let mut buffer = pool.acquire(16);
        buffer.fill(0xa5);
        drop(buffer);

        let buffer = pool.acquire(16);

        assert_eq!(buffer.as_ref(), &[MACFUSE_READ_BUFFER_POISON; 16]);
    }

    #[test]
    fn debug_output_omits_buffer_contents() {
        let pool = Arc::new(ReadBufferPool::default());
        let mut buffer = pool.acquire(16);
        buffer.limit_reply_len(4);

        assert_eq!(
            format!("{buffer:?}"),
            "PooledReadBuffer { reply_len: 4, initialized_len: 16, capacity: 16, .. }"
        );
        drop(buffer);
        assert_eq!(format!("{pool:?}"), "ReadBufferPool { available: 1, .. }");
    }
}
