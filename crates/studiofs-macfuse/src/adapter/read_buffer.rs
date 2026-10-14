//! Reusable reply storage for queued macFUSE reads.
//!
//! Pooled storage stays initialized across replies, so a recycled buffer still
//! holds bytes from earlier reads, possibly of other inodes. Reply safety
//! therefore depends on the `StudioVfs::read_into_fh` contract: on success,
//! every byte up to the returned length is overwritten, including zero-filled
//! holes. Callers must publish exactly that prefix through
//! [`PooledReadBuffer::limit_reply_len`] and must never reply with the buffer
//! after an error or cancellation.

use std::fmt;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex as ParkingMutex;

const MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS: usize = 32;
pub(super) const MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY: usize = 8 * 1024 * 1024;
// Keep allocator-rounded buffers that grew slightly beyond the reusable read
// cap, while dropping genuinely oversized requests instead of pinning them.
pub(super) const MACFUSE_READ_BUFFER_POOL_RETAINED_CAPACITY: usize =
    MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY;
/// Debug and test builds overwrite lent storage with this byte so a VFS path
/// that under-fills its reported prefix fails visibly instead of replying with
/// stale bytes from an earlier read.
#[cfg(any(test, debug_assertions))]
pub(super) const MACFUSE_READ_BUFFER_POISON: u8 = 0xdb;

#[derive(Default)]
pub(super) struct ReadBufferPool {
    buffers: ParkingMutex<Vec<Vec<u8>>>,
    #[cfg(test)]
    freshly_zeroed_bytes: AtomicUsize,
}

impl fmt::Debug for ReadBufferPool {
    // Pooled storage holds stale file data, so Debug reports only bookkeeping.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadBufferPool")
            .field("available", &self.buffers.lock().len())
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

    /// Reuse already-initialized storage when possible. Otherwise allocate
    /// zeroed storage through `alloc_zeroed`, which lets the allocator skip the
    /// memset for pages it already knows are zero and never copies stale bytes
    /// the way growing an undersized recycled buffer would.
    fn take_storage(&self, len: usize) -> Vec<u8> {
        let mut buffers = self.buffers.lock();
        if let Some(index) = buffers.iter().rposition(|buffer| buffer.len() >= len) {
            return buffers.swap_remove(index);
        }
        // Evict one undersized buffer per miss so a pool of small buffers
        // converges toward the working-set size instead of starving large reads.
        let evicted = buffers.pop();
        drop(buffers);
        drop(evicted);
        #[cfg(test)]
        self.freshly_zeroed_bytes.fetch_add(len, Ordering::Relaxed);
        vec![0; len]
    }

    fn recycle(&self, storage: Vec<u8>) {
        // `mem::take` during `Drop` leaves a zero-capacity placeholder; recycling
        // it would only occupy a pool slot without avoiding a future allocation.
        if storage.capacity() == 0
            || storage.capacity() > MACFUSE_READ_BUFFER_POOL_RETAINED_CAPACITY
        {
            return;
        }
        let mut buffers = self.buffers.lock();
        if buffers.len() < MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS {
            buffers.push(storage);
        }
    }

    #[cfg(test)]
    pub(super) fn available_count(&self) -> usize {
        self.buffers.lock().len()
    }

    #[cfg(test)]
    pub(super) fn last_available_capacity(&self) -> Option<usize> {
        self.buffers.lock().last().map(Vec::capacity)
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
    fn acquire_replaces_undersized_storage_with_zeroed_allocation() {
        let pool = Arc::new(ReadBufferPool::default());
        drop(pool.acquire(16));

        drop(pool.acquire(20));

        assert_eq!(pool.freshly_zeroed_bytes(), 36);
        assert_eq!(pool.available_count(), 1);
        assert_eq!(pool.last_available_capacity(), Some(20));
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
