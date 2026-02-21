//! Sequential read-ahead tracker.
//!
//! Detects sequential access patterns per inode and returns a list of upcoming
//! blocks that should be prefetched into cache before they are requested.
//!
//! ## Algorithm
//!
//! Each inode gets a [`ReadState`] that keeps the last block index read and a
//! *score* (0–[`MAX_SCORE`]).  On every recorded read:
//!
//! - If the new first block follows the previous last block consecutively
//!   the score increments (up to MAX_SCORE).
//! - Otherwise the score decrements by 2.
//!
//! The number of blocks to prefetch equals the current score (capped at
//! `width`).  So only truly sequential readers get aggressive prefetching while
//! random readers are quickly quieted.
//!
//! ## In-flight deduplication
//!
//! The `inflight` DashMap tracks which blocks are already being prefetched so
//! concurrent calls to `record_read()` never schedule duplicate work.

use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::HashSet;
use studiofs_meta::types::Ino;

/// Maximum sequential score.  Capped to prevent unbounded ramp-up.
const MAX_SCORE: u8 = 8;

// =============================================================================
// Internal state
// =============================================================================

struct ReadState {
    /// Last block index completed by the previous read.
    last_block: u32,
    /// Score representing how strongly sequential the access pattern is.
    score: u8,
}

// =============================================================================
// ReadAheadTracker
// =============================================================================

/// Tracks per-inode sequential read patterns and manages in-flight prefetch state.
pub struct ReadAheadTracker {
    /// Per-inode state.
    states: DashMap<Ino, ReadState>,
    /// Blocks that are already being prefetched — avoids duplicate work.
    inflight: DashMap<(Ino, u32), ()>,
    /// How many blocks ahead to fetch when fully ramped-up.
    pub width: u32,
}

impl ReadAheadTracker {
    /// Create a new tracker.
    ///
    /// `width` — maximum number of blocks to prefetch ahead (e.g. 8 = 32 MB
    /// look-ahead with 4 MB blocks).
    pub fn new(width: u32) -> Self {
        Self {
            states: DashMap::new(),
            inflight: DashMap::new(),
            width,
        }
    }

    /// Record that blocks `[first_block..=last_block]` were read for `ino`.
    ///
    /// Returns the list of *upcoming* block indices that the caller should
    /// schedule for prefetch.  Only blocks not already in-flight are returned.
    pub fn record_read(&self, ino: Ino, first_block: u32, last_block: u32) -> Vec<u32> {
        let mut state = self.states.entry(ino).or_insert(ReadState {
            last_block: u32::MAX,
            score: 0,
        });

        // Detect sequential continuation: the new read must start exactly at
        // the block immediately after where we left off.
        // Re-reads (same block) and backward seeks both decrease the score.
        let is_sequential =
            state.last_block != u32::MAX && first_block == state.last_block.saturating_add(1);

        if is_sequential {
            state.score = (state.score + 1).min(MAX_SCORE);
        } else {
            state.score = state.score.saturating_sub(2);
        }

        state.last_block = last_block;
        let score = state.score;
        drop(state);

        if score == 0 {
            return Vec::new();
        }

        // Return up to `min(score, width)` blocks beyond last_block.
        let prefetch_count = self.width.min(score as u32);
        (1..=prefetch_count)
            .filter_map(|i| {
                let block = last_block.checked_add(i)?;
                if self.inflight.contains_key(&(ino, block)) {
                    None // Already in-flight
                } else {
                    Some(block)
                }
            })
            .collect()
    }

    /// Mark a block as in-flight for prefetch.
    ///
    /// Returns `true` if the block was not previously in-flight (caller should
    /// proceed with the prefetch).  Returns `false` if the block is already
    /// being fetched by another task.
    pub fn start_prefetch(&self, ino: Ino, block: u32) -> bool {
        // insert returns the previous value; None means it wasn't present.
        self.inflight.insert((ino, block), ()).is_none()
    }

    /// Mark a prefetch as complete, allowing future re-scheduling if needed.
    pub fn finish_prefetch(&self, ino: Ino, block: u32) {
        self.inflight.remove(&(ino, block));
    }

    /// Remove all state for an inode (e.g. on file close or delete).
    pub fn evict(&self, ino: Ino) {
        self.states.remove(&ino);
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_prefetch_on_first_read() {
        let tracker = ReadAheadTracker::new(8);
        let blocks = tracker.record_read(1, 0, 0);
        // First read — no prior state, score starts at 0 → increments to 1.
        // Min(width=8, score=1) = 1 block prefetch.
        // BUT score was 0 before increment... let me trace:
        // initial score=0, last_block=u32::MAX
        // is_sequential: last_block=MAX → false → score = 0.saturating_sub(2) = 0
        // result: score=0 → no prefetch
        assert!(
            blocks.is_empty(),
            "first read of a brand-new inode should not trigger prefetch"
        );
    }

    #[test]
    fn test_sequential_ramp_up() {
        let tracker = ReadAheadTracker::new(8);

        // First read: block 0 (score stays 0 — no prior history)
        let p = tracker.record_read(1, 0, 0);
        assert!(p.is_empty());

        // Second read: block 1 (sequential after 0) → score becomes 1 → prefetch [2]
        let p = tracker.record_read(1, 1, 1);
        assert_eq!(p, vec![2], "score=1 → 1 block ahead");

        // Third read: block 2 → score=2 → prefetch [3, 4]
        let p = tracker.record_read(1, 2, 2);
        assert_eq!(p, vec![3, 4], "score=2 → 2 blocks ahead");

        // Keep going until score is capped at MAX_SCORE=8
        let mut last = 2u32;
        for _ in 0..10 {
            last += 1;
            let _ = tracker.record_read(1, last, last);
        }
        let blocks = tracker.record_read(1, last + 1, last + 1);
        // Score capped at 8 = MAX_SCORE, width=8 → at most 8 blocks prefetched
        assert!(blocks.len() <= 8, "prefetch depth should not exceed width");
    }

    #[test]
    fn test_random_read_kills_score() {
        let tracker = ReadAheadTracker::new(8);

        // Build up sequential score
        for i in 0..8u32 {
            tracker.record_read(1, i, i);
        }
        // After 7 sequential reads (0 is skipped — first read sees no prior state),
        // score is around 7.

        // Now do 5 random jumps, each to a completely different address.
        // Each decrements score by 2, so 4 random reads take score from 7 → 0.
        let jump_positions = [1000u32, 50000, 200000, 999999, 123456];
        for pos in jump_positions {
            let _ = tracker.record_read(1, pos, pos);
        }

        // After 5 random jumps from ~7, score should be 0 (7 - 2*4 = -1 → 0).
        let final_blocks = tracker.record_read(1, 9_000_000, 9_000_000);
        assert!(
            final_blocks.is_empty(),
            "after many random reads, prefetch should be disabled"
        );
    }

    #[test]
    fn test_inflight_deduplication() {
        let tracker = ReadAheadTracker::new(4);

        // First sequential read
        tracker.record_read(1, 0, 0);
        let p1 = tracker.record_read(1, 1, 1); // Returns blocks to prefetch

        // Mark them as in-flight
        for b in &p1 {
            tracker.start_prefetch(1, *b);
        }

        // Second call should NOT return already-in-flight blocks
        let p2 = tracker.record_read(1, 2, 2);
        for b in &p2 {
            assert!(
                !p1.contains(b),
                "in-flight block {} should not be re-returned",
                b
            );
        }
    }

    #[test]
    fn test_start_prefetch_idempotent() {
        let tracker = ReadAheadTracker::new(4);
        assert!(
            tracker.start_prefetch(1, 5),
            "first call should return true"
        );
        assert!(
            !tracker.start_prefetch(1, 5),
            "second call should return false (already in-flight)"
        );
        tracker.finish_prefetch(1, 5);
        assert!(
            tracker.start_prefetch(1, 5),
            "after finish, should return true again"
        );
    }

    #[test]
    fn test_multi_block_read() {
        let tracker = ReadAheadTracker::new(8);

        // Simulate a large read spanning blocks 0..=3
        tracker.record_read(1, 0, 3);
        // Next read: blocks 4..=7 (sequential)
        let p = tracker.record_read(1, 4, 7);
        // Should prefetch 8..=N
        assert!(
            !p.is_empty(),
            "should prefetch after sequential multi-block read"
        );
        assert!(p[0] == 8, "first prefetch block should follow last read");
    }

    #[test]
    fn test_evict_resets_state() {
        let tracker = ReadAheadTracker::new(8);

        for i in 0..5u32 {
            tracker.record_read(1, i, i);
        }

        tracker.evict(1);

        // After evict, state is gone — next read should not see sequential score
        let p = tracker.record_read(1, 6, 6);
        assert!(
            p.is_empty(),
            "evicted inode should have no sequential score"
        );
    }

    #[test]
    fn test_multiple_inodes_independent() {
        let tracker = ReadAheadTracker::new(8);

        // Build up score for inode 1
        for i in 0..5u32 {
            tracker.record_read(1, i, i);
        }

        // Inode 2 should start fresh
        let p = tracker.record_read(2, 0, 0);
        assert!(p.is_empty(), "separate inodes track state independently");
    }
}

// =============================================================================
// Directory-Level Prefetch Tracker
// =============================================================================

/// State for a directory whose children are being accessed.
struct DirPrefetchState {
    /// All regular-file child inodes from readdir.
    children: Vec<Ino>,
    /// Map from child ino back to its parent dir ino (for reverse lookup).
    /// How many children we've seen accessed (open/read).
    seen: usize,
    /// Which children have already been scheduled for prefetch.
    prefetched: HashSet<Ino>,
}

/// Tracks directory-level access patterns.
///
/// When a user browses a directory (readdir) and then starts opening/reading
/// files within it sequentially, this tracker detects the pattern and returns
/// a list of sibling inodes whose first block should be prefetched.
///
/// Trigger: after `>= 2` children from the same directory are read,
/// prefetch block 0 of the remaining siblings that haven't been accessed yet.
pub struct DirAccessTracker {
    /// dir_ino → prefetch state
    dirs: Mutex<std::collections::HashMap<Ino, DirPrefetchState>>,
    /// child_ino → parent_dir_ino (reverse lookup so on_read can find the dir)
    child_to_dir: DashMap<Ino, Ino>,
    /// Threshold: how many children must be read before prefetching triggers.
    threshold: usize,
}

impl DirAccessTracker {
    /// Create a new tracker.
    ///
    /// `threshold` — number of children that must be read before prefetching
    /// the rest of the directory (typically 2).
    pub fn new(threshold: usize) -> Self {
        Self {
            dirs: Mutex::new(std::collections::HashMap::new()),
            child_to_dir: DashMap::new(),
            threshold,
        }
    }

    /// Called after `readdir(dir_ino)` completes.
    ///
    /// `children` should contain the inodes of regular-file children only
    /// (exclude directories and symlinks — those have no data blocks).
    pub fn on_readdir(&self, dir_ino: Ino, children: Vec<Ino>) {
        // Register reverse mappings
        for &child in &children {
            self.child_to_dir.insert(child, dir_ino);
        }

        let mut dirs = self.dirs.lock();
        dirs.insert(
            dir_ino,
            DirPrefetchState {
                children,
                seen: 0,
                prefetched: HashSet::new(),
            },
        );
    }

    /// Called when a child inode is read.
    ///
    /// Returns a list of sibling inodes whose block 0 should be prefetched.
    /// Returns an empty vec if the threshold hasn't been met yet, or if the
    /// child's parent directory isn't tracked.
    pub fn on_child_read(&self, child_ino: Ino) -> Vec<Ino> {
        let dir_ino = match self.child_to_dir.get(&child_ino) {
            Some(v) => *v,
            None => return Vec::new(),
        };

        let mut dirs = self.dirs.lock();
        let state = match dirs.get_mut(&dir_ino) {
            Some(s) => s,
            None => return Vec::new(),
        };

        // Already counted this child? Skip.
        if state.prefetched.contains(&child_ino) {
            return Vec::new();
        }

        state.seen += 1;
        state.prefetched.insert(child_ino);

        // Not enough children read yet — don't prefetch.
        if state.seen < self.threshold {
            return Vec::new();
        }

        // Threshold met: return all un-prefetched children.
        let to_prefetch: Vec<Ino> = state
            .children
            .iter()
            .filter(|ino| !state.prefetched.contains(ino))
            .copied()
            .collect();

        // Mark them all as prefetched.
        for &ino in &to_prefetch {
            state.prefetched.insert(ino);
        }

        to_prefetch
    }

    /// Remove all state for a directory (e.g. when it is deleted).
    pub fn evict_dir(&self, dir_ino: Ino) {
        let mut dirs = self.dirs.lock();
        if let Some(state) = dirs.remove(&dir_ino) {
            for child in &state.children {
                self.child_to_dir.remove(child);
            }
        }
    }
}

#[cfg(test)]
mod dir_prefetch_tests {
    use super::*;

    #[test]
    fn test_no_prefetch_before_threshold() {
        let tracker = DirAccessTracker::new(2);
        tracker.on_readdir(100, vec![10, 11, 12, 13]);

        // First child read — below threshold
        let p = tracker.on_child_read(10);
        assert!(p.is_empty(), "below threshold: no prefetch");
    }

    #[test]
    fn test_prefetch_after_threshold() {
        let tracker = DirAccessTracker::new(2);
        tracker.on_readdir(100, vec![10, 11, 12, 13]);

        tracker.on_child_read(10);
        let p = tracker.on_child_read(11);
        // Threshold met: should return 12, 13 (the remaining children)
        assert_eq!(p.len(), 2);
        assert!(p.contains(&12));
        assert!(p.contains(&13));
    }

    #[test]
    fn test_no_double_prefetch() {
        let tracker = DirAccessTracker::new(2);
        tracker.on_readdir(100, vec![10, 11, 12]);

        tracker.on_child_read(10);
        let p1 = tracker.on_child_read(11);
        assert_eq!(p1.len(), 1); // [12]

        // Reading 12 again should not return anything
        let p2 = tracker.on_child_read(12);
        assert!(p2.is_empty());
    }

    #[test]
    fn test_untracked_child() {
        let tracker = DirAccessTracker::new(2);
        // child_ino 999 not in any tracked directory
        let p = tracker.on_child_read(999);
        assert!(p.is_empty());
    }

    #[test]
    fn test_evict_dir() {
        let tracker = DirAccessTracker::new(2);
        tracker.on_readdir(100, vec![10, 11, 12]);

        tracker.evict_dir(100);

        // After evict, children have no parent mapping
        let p = tracker.on_child_read(10);
        assert!(p.is_empty());
    }
}
