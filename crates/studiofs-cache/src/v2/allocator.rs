//! Free-space allocator for the `blocks.data` file.
//!
//! Best-fit allocation with coalescing of adjacent free regions,
//! tail truncation, and growth-chunk based file expansion.

use std::collections::BTreeMap;

use crate::v2::format::{GROWTH_CHUNK, PAGE_SIZE, SUPERBLOCK_SIZE};

/// Allocation failure.
#[derive(Debug, thiserror::Error)]
pub enum AllocError {
    #[error("cache full: need {needed} bytes, limit {max_size}")]
    CacheFull { needed: u64, max_size: u64 },
}

/// Free-space allocator tracking unused byte ranges in the data file.
///
/// Uses a dual-index BTreeMap approach:
/// - `by_offset` for O(log n) neighbour lookup (coalescing)
/// - `by_size` for O(log n) best-fit search
#[derive(Debug)]
pub struct FreeList {
    /// offset → size of each free region.
    by_offset: BTreeMap<u64, u64>,
    /// (size, offset) for best-fit lookup (smallest region ≥ requested).
    by_size: BTreeMap<(u64, u64), ()>,
    /// Current logical end of the data file.
    file_end: u64,
    /// User-configured maximum file size.
    max_size: u64,
    /// Sum of currently allocated bytes (not counting free regions or beyond-EOF space).
    allocated_bytes: u64,
}

impl FreeList {
    /// Create an allocator for a fresh data file (only superblock present).
    pub fn new(max_size: u64) -> Self {
        Self {
            by_offset: BTreeMap::new(),
            by_size: BTreeMap::new(),
            file_end: SUPERBLOCK_SIZE,
            max_size,
            allocated_bytes: 0,
        }
    }

    /// Rebuild allocator state from a known file length and a set of occupied regions.
    ///
    /// `occupied` is a slice of `(offset, size)` pairs for every live block.
    /// Gaps between occupied regions become free regions.
    pub fn recover(max_size: u64, file_end: u64, occupied: &[(u64, u64)]) -> Self {
        let mut fl = Self {
            by_offset: BTreeMap::new(),
            by_size: BTreeMap::new(),
            file_end,
            max_size,
            allocated_bytes: 0,
        };

        let mut sorted: Vec<(u64, u64)> = occupied.to_vec();
        sorted.sort_by_key(|&(off, _)| off);

        let mut cursor = SUPERBLOCK_SIZE;
        for &(off, size) in &sorted {
            if off > cursor {
                fl.insert_free(cursor, off - cursor);
            }
            fl.allocated_bytes += size;
            cursor = off + size;
        }
        if cursor < file_end {
            fl.insert_free(cursor, file_end - cursor);
        }
        fl
    }

    /// Allocate a contiguous region of exactly `size` bytes.
    ///
    /// `size` **must** be page-aligned (a multiple of [`PAGE_SIZE`]).
    ///
    /// Returns the byte offset within the data file on success.
    pub fn allocate(&mut self, size: u64) -> Result<u64, AllocError> {
        debug_assert!(
            size > 0 && size.is_multiple_of(PAGE_SIZE),
            "size must be page-aligned and > 0"
        );

        // 1. Best-fit: smallest free region ≥ size.
        if let Some(&(found_size, found_offset)) =
            self.by_size.range((size, 0)..).next().map(|(k, _)| k)
        {
            self.remove_free(found_offset, found_size);

            let remainder = found_size - size;
            if remainder > 0 {
                self.insert_free(found_offset + size, remainder);
            }
            self.allocated_bytes += size;
            return Ok(found_offset);
        }

        // 2. Grow the file.
        let grow = std::cmp::max(size, GROWTH_CHUNK);
        let new_end = self.file_end.saturating_add(grow);

        if new_end <= self.max_size {
            let offset = self.file_end;
            let leftover = grow - size;
            self.file_end = new_end;
            if leftover > 0 {
                self.insert_free(offset + size, leftover);
            }
            self.allocated_bytes += size;
            return Ok(offset);
        }

        // 3. Growth exceeds limit — try exact fit with remaining space.
        let remaining = self.max_size.saturating_sub(self.file_end);
        if remaining >= size {
            let offset = self.file_end;
            let leftover = remaining - size;
            self.file_end += remaining;
            if leftover > 0 {
                self.insert_free(offset + size, leftover);
            }
            self.allocated_bytes += size;
            return Ok(offset);
        }

        Err(AllocError::CacheFull {
            needed: size,
            max_size: self.max_size,
        })
    }

    /// Return a previously allocated region to the free list.
    ///
    /// Automatically coalesces with adjacent free regions and truncates
    /// the file tail when possible.
    pub fn free(&mut self, offset: u64, size: u64) {
        debug_assert!(size > 0);
        self.allocated_bytes = self.allocated_bytes.saturating_sub(size);

        let mut merged_offset = offset;
        let mut merged_size = size;

        // Coalesce with preceding free region.
        if let Some((&prev_off, &prev_size)) = self.by_offset.range(..offset).next_back() {
            if prev_off + prev_size == offset {
                self.remove_free(prev_off, prev_size);
                merged_offset = prev_off;
                merged_size += prev_size;
            }
        }

        // Coalesce with following free region.
        let following_start = merged_offset + merged_size;
        if let Some((&next_off, &next_size)) = self.by_offset.range(following_start..).next() {
            if next_off == following_start {
                self.remove_free(next_off, next_size);
                merged_size += next_size;
            }
        }

        // Tail truncate: if the merged region reaches file_end, shrink the logical end.
        if merged_offset + merged_size == self.file_end {
            self.file_end = merged_offset;
        } else {
            self.insert_free(merged_offset, merged_size);
        }
    }

    /// Current logical end of the data file (callers must ensure the physical
    /// file is at least this long before writing).
    pub fn file_end(&self) -> u64 {
        self.file_end
    }

    /// Total bytes currently allocated (occupied by live blocks).
    pub fn allocated_bytes(&self) -> u64 {
        self.allocated_bytes
    }

    /// Total bytes tracked in the free list (holes within the file).
    pub fn free_bytes(&self) -> u64 {
        self.by_offset.values().sum()
    }

    /// Number of distinct free regions.
    pub fn free_region_count(&self) -> usize {
        self.by_offset.len()
    }

    // -- internal helpers --

    fn insert_free(&mut self, offset: u64, size: u64) {
        self.by_offset.insert(offset, size);
        self.by_size.insert((size, offset), ());
    }

    fn remove_free(&mut self, offset: u64, size: u64) {
        self.by_offset.remove(&offset);
        self.by_size.remove(&(size, offset));
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    const SB: u64 = SUPERBLOCK_SIZE; // 4096

    /// Helper: create a FreeList with a small limit for testing.
    fn small_freelist(pages: u64) -> FreeList {
        FreeList::new(SB + pages * PAGE_SIZE)
    }

    #[test]
    fn basic_allocate_and_free() {
        let mut fl = small_freelist(10);
        let off = fl.allocate(PAGE_SIZE).unwrap();
        assert_eq!(off, SB);
        assert_eq!(fl.allocated_bytes(), PAGE_SIZE);

        fl.free(off, PAGE_SIZE);
        assert_eq!(fl.allocated_bytes(), 0);
    }

    #[test]
    fn best_fit_selection() {
        let mut fl = FreeList::new(u64::MAX / 2);
        // Manually inject two free regions: a big one and a small one.
        fl.file_end = SB + 20 * PAGE_SIZE;
        fl.insert_free(SB, 8 * PAGE_SIZE); // 8 pages
        fl.insert_free(SB + 10 * PAGE_SIZE, 2 * PAGE_SIZE); // 2 pages

        // Request 2 pages → should pick the 2-page region (best fit).
        let off = fl.allocate(2 * PAGE_SIZE).unwrap();
        assert_eq!(off, SB + 10 * PAGE_SIZE);

        // The 8-page region should still be there.
        assert_eq!(fl.free_region_count(), 1);
    }

    #[test]
    fn split_region() {
        let mut fl = FreeList::new(u64::MAX / 2);
        fl.file_end = SB + 8 * PAGE_SIZE;
        fl.insert_free(SB, 8 * PAGE_SIZE);

        let off = fl.allocate(2 * PAGE_SIZE).unwrap();
        assert_eq!(off, SB);
        // Remainder: 6 pages starting at SB + 2*PAGE.
        assert_eq!(fl.free_region_count(), 1);
        assert_eq!(
            *fl.by_offset.get(&(SB + 2 * PAGE_SIZE)).unwrap(),
            6 * PAGE_SIZE
        );
    }

    #[test]
    fn coalesce_left() {
        let mut fl = small_freelist(10);
        let a = fl.allocate(PAGE_SIZE).unwrap();
        let b = fl.allocate(PAGE_SIZE).unwrap();

        fl.free(a, PAGE_SIZE);
        // Now free b — should coalesce with the free region at a.
        fl.free(b, PAGE_SIZE);

        // Should be one merged free region, and file_end should have shrunk (tail truncate).
        assert_eq!(fl.free_region_count(), 0);
        assert_eq!(fl.file_end(), SB);
    }

    #[test]
    fn coalesce_right() {
        let mut fl = small_freelist(3); // exact fit for 3 blocks, no leftover
        let a = fl.allocate(PAGE_SIZE).unwrap();
        let b = fl.allocate(PAGE_SIZE).unwrap();
        let _c = fl.allocate(PAGE_SIZE).unwrap();

        // Free b first, then a — a should coalesce right with b.
        fl.free(b, PAGE_SIZE);
        fl.free(a, PAGE_SIZE);

        // a+b merged into one region.
        assert_eq!(fl.free_region_count(), 1);
        assert_eq!(*fl.by_offset.get(&SB).unwrap(), 2 * PAGE_SIZE);
    }

    #[test]
    fn coalesce_both_sides() {
        let mut fl = small_freelist(4); // exact fit for 4 blocks, no leftover
        let a = fl.allocate(PAGE_SIZE).unwrap();
        let b = fl.allocate(PAGE_SIZE).unwrap();
        let c = fl.allocate(PAGE_SIZE).unwrap();
        let _d = fl.allocate(PAGE_SIZE).unwrap();

        fl.free(a, PAGE_SIZE);
        fl.free(c, PAGE_SIZE);
        // Now free b — should coalesce left with a and right with c.
        fl.free(b, PAGE_SIZE);

        assert_eq!(fl.free_region_count(), 1);
        assert_eq!(*fl.by_offset.get(&SB).unwrap(), 3 * PAGE_SIZE);
    }

    #[test]
    fn tail_truncate() {
        let mut fl = small_freelist(10);
        let a = fl.allocate(PAGE_SIZE).unwrap();
        let b = fl.allocate(PAGE_SIZE).unwrap();

        // Free the tail block — file_end should shrink.
        fl.free(b, PAGE_SIZE);
        assert_eq!(fl.file_end(), SB + PAGE_SIZE);

        // Free the remaining block — file_end back to superblock.
        fl.free(a, PAGE_SIZE);
        assert_eq!(fl.file_end(), SB);
        assert_eq!(fl.free_region_count(), 0);
    }

    #[test]
    fn cache_full() {
        let mut fl = small_freelist(2); // max = SB + 8192
        fl.allocate(PAGE_SIZE).unwrap(); // uses first page
        fl.allocate(PAGE_SIZE).unwrap(); // uses second page
        let res = fl.allocate(PAGE_SIZE);
        assert!(res.is_err());
    }

    #[test]
    fn growth_chunk_applied() {
        // With a large max_size, the first allocation should grow by GROWTH_CHUNK.
        let mut fl = FreeList::new(GROWTH_CHUNK * 4);
        let off = fl.allocate(PAGE_SIZE).unwrap();
        assert_eq!(off, SB);
        // file_end should have jumped by GROWTH_CHUNK (not just PAGE_SIZE).
        assert_eq!(fl.file_end(), SB + GROWTH_CHUNK);
        // The leftover should be a single free region.
        assert_eq!(fl.free_region_count(), 1);
        assert_eq!(fl.free_bytes(), GROWTH_CHUNK - PAGE_SIZE);
    }

    #[test]
    fn allocate_after_free_reuses_space() {
        let mut fl = small_freelist(3);
        let a = fl.allocate(PAGE_SIZE).unwrap();
        let _b = fl.allocate(PAGE_SIZE).unwrap();
        let _c = fl.allocate(PAGE_SIZE).unwrap();

        // Cache is full. Free a, then allocate again — should reuse a's slot.
        fl.free(a, PAGE_SIZE);
        let d = fl.allocate(PAGE_SIZE).unwrap();
        assert_eq!(d, a);
    }

    #[test]
    fn recover_with_occupied_regions() {
        let occupied = vec![(SB, PAGE_SIZE), (SB + 2 * PAGE_SIZE, PAGE_SIZE)];
        let file_end = SB + 4 * PAGE_SIZE;
        let fl = FreeList::recover(u64::MAX / 2, file_end, &occupied);

        assert_eq!(fl.allocated_bytes(), 2 * PAGE_SIZE);
        // Gap at SB + PAGE_SIZE (size PAGE_SIZE) and trailing gap SB+3*PAGE..SB+4*PAGE.
        assert_eq!(fl.free_region_count(), 2);
        assert_eq!(fl.free_bytes(), 2 * PAGE_SIZE);
    }

    #[test]
    fn exact_fit_at_limit() {
        // max_size = SB + 1 page. Allocate exactly 1 page.
        let mut fl = small_freelist(1);
        let off = fl.allocate(PAGE_SIZE).unwrap();
        assert_eq!(off, SB);
        assert_eq!(fl.file_end(), SB + PAGE_SIZE);
    }
}
