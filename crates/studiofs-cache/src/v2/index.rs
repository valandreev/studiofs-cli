//! In-memory block index combining HashMap lookup with intrusive LRU ordering.
//!
//! [`BlockIndex`] is the single in-memory index for the Cache v2 block store.
//! It wraps:
//! * A `HashMap<u64, Box<IndexEntry>>` for O(1) lookup by `object_id`.
//! * An intrusive [`LruList`] for O(1) eviction ordering.
//! * Aggregate counters (`block_count`, `dirty_count`).
//!
//! # Thread-safety
//!
//! `BlockIndex` is not `Sync`.  All operations are `&mut self` and callers
//! must serialise access — typically by wrapping the index in a
//! `parking_lot::Mutex`.
//!
//! # Safety note on raw pointers
//!
//! The `LruList` stores `NonNull<LruNode>` raw pointers into the
//! `Box<IndexEntry>` values owned by `entries`.  Stable heap addresses are
//! guaranteed by `Box`.  Every node is unlinked from the list before its
//! `Box` is dropped.  See [`super::lru`] for the full invariant list.

use std::collections::HashMap;
use std::ptr::NonNull;

use bitflags::bitflags;

use crate::v2::lru::{LruList, LruNode};

// ---------------------------------------------------------------------------
// BlockFlags
// ---------------------------------------------------------------------------

bitflags! {
    /// Per-block state flags that control eviction eligibility.
    ///
    /// A block is **evictable** only when *no* flag is set.
    ///
    /// | Flag | Meaning | Persisted on disk |
    /// |------|---------|-------------------|
    /// | `DIRTY` | Data not yet uploaded to S3 | Yes (bit 0x01 in block header) |
    /// | `PINNED` | Reserved for future user-controlled pinning | Yes (bit 0x02) |
    /// | `UPLOADING` | An S3 upload is currently in progress | No — transient |
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct BlockFlags: u8 {
        /// Data not yet uploaded to S3.  Set by `put(evictable=false)`;
        /// cleared by `mark_evictable` on successful upload.
        const DIRTY     = 0x01;
        /// Reserved for future user-controlled pinning (not wired yet).
        const PINNED    = 0x02;
        /// An S3 upload is currently in progress.  Set by `mark_uploading`,
        /// cleared by `mark_evictable` (success) or `mark_upload_failed`.
        /// Never written to the on-disk block header.
        const UPLOADING = 0x04;
    }
}

impl BlockFlags {
    /// Returns `true` when none of `DIRTY`, `PINNED`, or `UPLOADING` are set,
    /// meaning the block may be chosen as an eviction candidate.
    #[inline]
    pub fn is_evictable(self) -> bool {
        self.is_empty()
    }
}

// ---------------------------------------------------------------------------
// IndexEntry
// ---------------------------------------------------------------------------

/// In-memory metadata for a single cached block.
///
/// Heap-allocated via `Box` so its address is stable for the intrusive LRU.
/// The `lru_node` field **must not be moved** after the entry has been
/// inserted into a [`BlockIndex`].
pub struct IndexEntry {
    /// Byte offset of the block's header in `blocks.data`.
    pub offset: u64,
    /// Actual payload size (bytes).
    pub data_size: u32,
    /// Total on-disk allocation (page-aligned: header + data + padding).
    pub alloc_size: u64,
    /// CRC-32 of the payload (for read-time verification).
    pub checksum: u32,
    /// State flags controlling eviction eligibility.
    pub flags: BlockFlags,
    /// Intrusive LRU node embedded in this entry.
    ///
    /// Must not be moved while the entry is in a [`BlockIndex`].
    pub lru_node: LruNode,
}

// ---------------------------------------------------------------------------
// RemovedEntry
// ---------------------------------------------------------------------------

/// Minimal data returned when a block is removed or evicted.
///
/// Contains everything the caller needs to free space in the [`super::allocator::FreeList`]
/// and update external counters.
#[derive(Debug)]
pub struct RemovedEntry {
    /// Byte offset of the block in `blocks.data`.
    pub offset: u64,
    /// Page-aligned on-disk allocation size.
    pub alloc_size: u64,
    /// Flags at the time of removal (used by eviction to skip dirty accounting).
    pub flags: BlockFlags,
}

// ---------------------------------------------------------------------------
// BlockIndex
// ---------------------------------------------------------------------------

/// Combined block index and LRU eviction tracker.
///
/// All methods take `&mut self`; the caller serialises access via a
/// `parking_lot::Mutex<BlockIndex>`.
pub struct BlockIndex {
    entries: HashMap<u64, Box<IndexEntry>>,
    lru: LruList,
    dirty_count: u32,
    block_count: u64,
}

// SAFETY: BlockIndex wraps a HashMap and an LruList whose raw pointers all
// point into Boxed entries owned by the same HashMap.  All access is
// serialised by the surrounding Mutex, so transferring ownership of a
// BlockIndex across threads is safe.
unsafe impl Send for BlockIndex {}

impl BlockIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            lru: LruList::new(),
            dirty_count: 0,
            block_count: 0,
        }
    }

    /// Create an empty index with a pre-allocated HashMap bucket count.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            lru: LruList::new(),
            dirty_count: 0,
            block_count: 0,
        }
    }

    // -----------------------------------------------------------------------
    // Read-only accessors
    // -----------------------------------------------------------------------

    /// Look up an entry without altering the LRU order.
    ///
    /// Use this for `contains` checks and when copying entry metadata for I/O
    /// (before releasing the lock).  Call [`touch`] separately when the access
    /// should update recency.
    #[inline]
    pub fn get(&self, object_id: u64) -> Option<&IndexEntry> {
        self.entries.get(&object_id).map(|b| b.as_ref())
    }

    /// Returns `true` when `object_id` is present in the index.
    #[inline]
    pub fn contains(&self, object_id: u64) -> bool {
        self.entries.contains_key(&object_id)
    }

    /// Number of blocks currently tracked.
    #[inline]
    pub fn block_count(&self) -> u64 {
        self.block_count
    }

    /// Number of blocks with the `DIRTY` flag set.
    #[inline]
    pub fn dirty_count(&self) -> u32 {
        self.dirty_count
    }

    /// Number of nodes currently tracked by the intrusive LRU list.
    ///
    /// In a consistent state this equals `block_count`; useful in tests.
    #[inline]
    pub fn lru_len(&self) -> usize {
        self.lru.len()
    }

    // -----------------------------------------------------------------------
    // Mutating operations
    // -----------------------------------------------------------------------

    /// Insert a new entry for `object_id` as the MRU block.
    ///
    /// If an existing entry is present it is removed and its [`RemovedEntry`]
    /// returned (the caller must free the old allocation).  Otherwise returns
    /// `None`.
    pub fn insert(
        &mut self,
        object_id: u64,
        offset: u64,
        data_size: u32,
        alloc_size: u64,
        checksum: u32,
        flags: BlockFlags,
    ) -> Option<RemovedEntry> {
        // Remove any existing entry so counters stay consistent.
        let replaced = self.remove_unlinked(object_id);

        let mut entry = Box::new(IndexEntry {
            offset,
            data_size,
            alloc_size,
            checksum,
            flags,
            lru_node: LruNode::new(object_id),
        });

        // Push the embedded LruNode to the tail (MRU position).
        // SAFETY: entry is Box-allocated (stable address), lru_node is
        // freshly created (prev=None, next=None), and Box is alive for the
        // duration of its presence in self.entries.
        let node_ptr = NonNull::from(&mut entry.lru_node);
        unsafe { self.lru.push_back(node_ptr) };

        if flags.contains(BlockFlags::DIRTY) {
            self.dirty_count += 1;
        }
        self.block_count += 1;
        self.entries.insert(object_id, entry);

        replaced
    }

    /// Move `object_id` to the LRU tail (MRU position).
    ///
    /// Called by [`super::block_store::BlockStore::get`] after a successful
    /// cache read.  No-op if the entry does not exist.
    pub fn touch(&mut self, object_id: u64) {
        if let Some(entry) = self.entries.get_mut(&object_id) {
            let node_ptr = NonNull::from(&mut entry.lru_node);
            // SAFETY: node is linked in self.lru (it was pushed at insert time)
            // and the entry Box is still alive.
            unsafe { self.lru.touch(node_ptr) };
        }
    }

    /// Remove and return the entry for `object_id`.
    ///
    /// Returns `None` if the entry does not exist.
    pub fn remove(&mut self, object_id: u64) -> Option<RemovedEntry> {
        self.remove_unlinked(object_id)
    }

    /// Remove a batch of `object_ids`, returning all removed entries.
    ///
    /// Unknown IDs are silently skipped.
    pub fn remove_batch(&mut self, object_ids: &[u64]) -> Vec<RemovedEntry> {
        object_ids
            .iter()
            .filter_map(|&id| self.remove_unlinked(id))
            .collect()
    }

    /// Replace the flags for `object_id` wholesale and adjust `dirty_count`.
    ///
    /// Prefer [`modify_flags`] for callers that only know *which bits to
    /// change*, not the full desired flag set.
    ///
    /// Returns `false` if the entry does not exist.
    pub fn update_flags(&mut self, object_id: u64, new_flags: BlockFlags) -> bool {
        if let Some(entry) = self.entries.get_mut(&object_id) {
            let old_dirty = entry.flags.contains(BlockFlags::DIRTY);
            let new_dirty = new_flags.contains(BlockFlags::DIRTY);
            match (old_dirty, new_dirty) {
                (false, true) => self.dirty_count += 1,
                (true, false) => self.dirty_count = self.dirty_count.saturating_sub(1),
                _ => {}
            }
            entry.flags = new_flags;
            true
        } else {
            false
        }
    }

    /// Atomically apply `(current & !clear) | set` to the flags for
    /// `object_id`, adjusting `dirty_count` for any DIRTY transitions.
    ///
    /// Unlike [`update_flags`] the caller does not need to read the current
    /// flags first: the operation is a single locked read-modify-write, so
    /// concurrent flag mutations by other threads (e.g. a concurrent
    /// `mark_uploading` call) cannot be silently clobbered.
    ///
    /// Returns `false` if the entry does not exist.
    pub fn modify_flags(&mut self, object_id: u64, clear: BlockFlags, set: BlockFlags) -> bool {
        if let Some(entry) = self.entries.get_mut(&object_id) {
            let old_flags = entry.flags;
            let new_flags = (old_flags & !clear) | set;
            if old_flags != new_flags {
                let old_dirty = old_flags.contains(BlockFlags::DIRTY);
                let new_dirty = new_flags.contains(BlockFlags::DIRTY);
                match (old_dirty, new_dirty) {
                    (false, true) => self.dirty_count += 1,
                    (true, false) => self.dirty_count = self.dirty_count.saturating_sub(1),
                    _ => {}
                }
                entry.flags = new_flags;
            }
            true
        } else {
            false
        }
    }

    /// Pop the oldest *evictable* entry from the LRU head.
    ///
    /// Returns `None` when every block has at least one of `DIRTY`, `PINNED`,
    /// or `UPLOADING` set.
    ///
    /// Time complexity: O(k) where k is the count of non-evictable blocks
    /// scanned before finding an evictable one.
    pub fn evict_one(&mut self) -> Option<RemovedEntry> {
        // Split borrows: `entries` is immutably borrowed by the closure;
        // `lru` is mutably borrowed by pop_front_evictable.
        let oid = {
            let entries = &self.entries;
            // SAFETY: all nodes in self.lru are backed by live Boxes in entries.
            unsafe {
                self.lru.pop_front_evictable(|oid| {
                    entries
                        .get(&oid)
                        .map(|e| e.flags.is_evictable())
                        .unwrap_or(false)
                })
            }
        }?;

        // The node has been removed from the LRU by pop_front_evictable.
        // Remove from the map — do NOT call lru.remove() again.
        let boxed = self.entries.remove(&oid)?;

        if boxed.flags.contains(BlockFlags::DIRTY) {
            self.dirty_count = self.dirty_count.saturating_sub(1);
        }
        self.block_count = self.block_count.saturating_sub(1);

        Some(RemovedEntry {
            offset: boxed.offset,
            alloc_size: boxed.alloc_size,
            flags: boxed.flags,
        })
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Remove `object_id` from both the map and the LRU list.
    ///
    /// The node is unlinked **while the Box is still alive** so the raw
    /// pointer remains valid throughout the unlink operation.
    fn remove_unlinked(&mut self, object_id: u64) -> Option<RemovedEntry> {
        let mut boxed = self.entries.remove(&object_id)?;

        // Unlink from LRU before the Box is dropped.
        // SAFETY: the node was linked at insert time and the Box is still alive.
        let node_ptr = NonNull::from(&mut boxed.lru_node);
        unsafe { self.lru.remove(node_ptr) };

        if boxed.flags.contains(BlockFlags::DIRTY) {
            self.dirty_count = self.dirty_count.saturating_sub(1);
        }
        self.block_count = self.block_count.saturating_sub(1);

        Some(RemovedEntry {
            offset: boxed.offset,
            alloc_size: boxed.alloc_size,
            flags: boxed.flags,
        })
    }
}

impl Drop for BlockIndex {
    fn drop(&mut self) {
        // Unlink every node before the Boxes (and their embedded LruNodes) are
        // freed, so LruList::drop's debug_assert passes. Drain the map instead
        // of collecting keys and doing N extra HashMap lookups.
        let lru = &mut self.lru;
        for (_, mut boxed) in self.entries.drain() {
            let node_ptr = NonNull::from(&mut boxed.lru_node);
            // SAFETY: every drained entry was linked at insert time and the
            // Box remains alive for the duration of the unlink.
            unsafe { lru.remove(node_ptr) };
        }
        self.dirty_count = 0;
        self.block_count = 0;
    }
}

impl Default for BlockIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(flags: BlockFlags, offset: u64) -> (u64, u32, u64, u32, BlockFlags) {
        // (data_size, alloc_size, checksum, flags)
        (offset, 64, 4096, 0xDEAD, flags)
    }

    #[test]
    fn insert_and_get() {
        let mut idx = BlockIndex::new();
        let (offset, data_size, alloc_size, checksum, flags) =
            make_entry(BlockFlags::empty(), 4096);
        idx.insert(1, offset, data_size, alloc_size, checksum, flags);

        assert_eq!(idx.block_count(), 1);
        assert_eq!(idx.dirty_count(), 0);
        let e = idx.get(1).unwrap();
        assert_eq!(e.offset, 4096);
        assert_eq!(e.flags, BlockFlags::empty());
    }

    #[test]
    fn insert_dirty_increments_dirty_count() {
        let mut idx = BlockIndex::new();
        idx.insert(1, 4096, 64, 4096, 0, BlockFlags::DIRTY);
        assert_eq!(idx.dirty_count(), 1);
        idx.insert(2, 8192, 64, 4096, 0, BlockFlags::empty());
        assert_eq!(idx.dirty_count(), 1);
        assert_eq!(idx.block_count(), 2);
    }

    #[test]
    fn replace_existing_returns_old() {
        let mut idx = BlockIndex::new();
        idx.insert(1, 4096, 64, 4096, 0, BlockFlags::empty());
        let replaced = idx.insert(1, 8192, 128, 8192, 0, BlockFlags::empty());
        assert!(replaced.is_some());
        assert_eq!(replaced.unwrap().offset, 4096);
        assert_eq!(idx.block_count(), 1);
        assert_eq!(idx.lru_len(), 1);
    }

    #[test]
    fn remove_decrements_counters() {
        let mut idx = BlockIndex::new();
        idx.insert(1, 4096, 64, 4096, 0, BlockFlags::DIRTY);
        let removed = idx.remove(1).unwrap();
        assert_eq!(removed.offset, 4096);
        assert_eq!(idx.block_count(), 0);
        assert_eq!(idx.dirty_count(), 0);
    }

    #[test]
    fn remove_missing_is_none() {
        let mut idx = BlockIndex::new();
        assert!(idx.remove(999).is_none());
    }

    #[test]
    fn remove_batch_partial() {
        let mut idx = BlockIndex::new();
        for i in 1u64..=5 {
            idx.insert(i, i * 4096, 64, 4096, 0, BlockFlags::empty());
        }
        let removed = idx.remove_batch(&[1, 3, 5, 999]);
        assert_eq!(removed.len(), 3);
        assert_eq!(idx.block_count(), 2);
        assert!(idx.contains(2));
        assert!(idx.contains(4));
    }

    #[test]
    fn update_flags_dirty_transitions() {
        let mut idx = BlockIndex::new();
        idx.insert(1, 4096, 64, 4096, 0, BlockFlags::DIRTY);
        assert_eq!(idx.dirty_count(), 1);

        idx.update_flags(1, BlockFlags::empty());
        assert_eq!(idx.dirty_count(), 0);

        idx.update_flags(1, BlockFlags::DIRTY);
        assert_eq!(idx.dirty_count(), 1);
    }

    #[test]
    fn evict_one_picks_oldest_evictable() {
        let mut idx = BlockIndex::new();
        // Insert in order: 1=dirty, 2=evictable, 3=evictable
        idx.insert(1, 4096, 64, 4096, 0, BlockFlags::DIRTY);
        idx.insert(2, 8192, 64, 4096, 0, BlockFlags::empty());
        idx.insert(3, 12288, 64, 4096, 0, BlockFlags::empty());

        // 1 is oldest but dirty; evict_one should pick 2.
        let evicted = idx.evict_one().unwrap();
        assert_eq!(evicted.offset, 8192);
        assert_eq!(idx.block_count(), 2);
        assert_eq!(idx.lru_len(), 2);
    }

    #[test]
    fn evict_one_none_when_all_dirty() {
        let mut idx = BlockIndex::new();
        idx.insert(1, 4096, 64, 4096, 0, BlockFlags::DIRTY);
        idx.insert(2, 8192, 64, 4096, 0, BlockFlags::UPLOADING);
        assert!(idx.evict_one().is_none());
    }

    #[test]
    fn touch_reorders_lru() {
        let mut idx = BlockIndex::new();
        idx.insert(1, 4096, 64, 4096, 0, BlockFlags::empty());
        idx.insert(2, 8192, 64, 4096, 0, BlockFlags::empty());
        idx.insert(3, 12288, 64, 4096, 0, BlockFlags::empty());

        // Touch 1 (oldest) → it should no longer be the eviction candidate.
        idx.touch(1);

        // Now the oldest evictable should be 2.
        let evicted = idx.evict_one().unwrap();
        assert_eq!(evicted.offset, 8192);
    }

    #[test]
    fn uploading_flag_blocks_eviction() {
        let mut idx = BlockIndex::new();
        idx.insert(1, 4096, 64, 4096, 0, BlockFlags::UPLOADING);
        idx.insert(2, 8192, 64, 4096, 0, BlockFlags::empty());

        let evicted = idx.evict_one().unwrap();
        assert_eq!(evicted.offset, 8192); // 1 was skipped
    }

    #[test]
    fn lru_len_matches_block_count() {
        let mut idx = BlockIndex::new();
        for i in 0u64..10 {
            idx.insert(i, i * 4096 + 4096, 64, 4096, 0, BlockFlags::empty());
        }
        assert_eq!(idx.lru_len(), idx.block_count() as usize);

        idx.remove(5);
        assert_eq!(idx.lru_len(), idx.block_count() as usize);

        idx.evict_one();
        assert_eq!(idx.lru_len(), idx.block_count() as usize);
    }
}
