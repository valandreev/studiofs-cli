//! Sequential read-ahead tracker.
//!
//! Detects sequential access patterns per inode and returns a list of upcoming
//! blocks that should be prefetched into cache before they are requested.
//!
//! ## Algorithm
//!
//! Each inode gets a [`ReadState`] that keeps the last block index read and a
//! *score* (0–[`MAX_SCORE`]), plus a dynamic prefetch width.  On every recorded
//! read:
//!
//! - If the new first block follows the previous last block consecutively
//!   the score increments (up to MAX_SCORE), and after a confirmed streak the
//!   dynamic width ramps up (doubling each streak interval) up to a configured
//!   cap.
//! - If the reader is still inside the same block, the score is unchanged.
//! - Any other jump decrements the score by 2.
//!
//! The number of blocks to prefetch equals the current score (capped at
//! dynamic width). So only truly sequential readers get aggressive prefetching
//! while random readers are quickly quieted and reset toward baseline.
//!
//! ## In-flight deduplication
//!
//! The `inflight` DashMap tracks which blocks are already being prefetched so
//! concurrent calls to `record_read()` never schedule duplicate work.

use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use studiofs_meta::types::Ino;

/// Maximum sequential score.  Capped to prevent unbounded ramp-up.
const MAX_SCORE: u8 = 255;
const VELOCITY_SAMPLE_ALPHA: f64 = 0.25;
const VELOCITY_SAMPLE_MIN_AGE_US: u64 = 1_000;
const VELOCITY_SAMPLE_MAX_AGE_US: u64 = 2_000_000;
const LARGE_SEEK_RESET_BLOCK_DELTA: u32 = 256;

// =============================================================================
// Internal state
// =============================================================================

struct ReadState {
    /// Last block index completed by the previous read.
    last_block: u32,
    /// Score representing how strongly sequential the access pattern is.
    score: u8,
    /// Confirmed sequential read streak length.
    sequential_streak: u16,
    /// Dynamic width ramped up for sustained sequential readers.
    current_width: u32,
    /// Monotonic timestamp (microseconds) when this inode was last read.
    last_read_at_micros: u64,
    /// EWMA estimate of read velocity in blocks/sec.
    velocity_blocks_per_sec_ema: f64,
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
    /// Maximum blocks ahead to fetch when fully ramped-up.
    pub width: u32,
    /// Baseline prefetch width before ramp-up.
    base_width: u32,
    /// Lower bound for prefetch depth once sequential access is detected.
    min_prefetch_on_seq: u32,
    /// Score increase per sequential span block.
    sequential_step: u8,
    /// Score penalty for random jumps.
    random_penalty: u8,
    /// Number of consecutive sequential blocks required for a ramp-up step.
    /// Width doubling is conservative: bounded doublings per record_read call.
    sequential_ramp_trigger: u16,
    /// Maximum ramp-up doublings allowed per single record_read call.
    max_ramp_steps_per_call: u8,
}

impl ReadAheadTracker {
    fn monotonic_now_micros() -> u64 {
        static START: OnceLock<Instant> = OnceLock::new();
        START
            .get_or_init(Instant::now)
            .elapsed()
            .as_micros()
            .min(u64::MAX as u128) as u64
    }

    fn velocity_width_target(&self, blocks_per_sec: f64) -> u32 {
        if !blocks_per_sec.is_finite() || blocks_per_sec <= 0.0 {
            return self.base_width;
        }
        let multiplier: u32 = if blocks_per_sec >= 512.0 {
            8
        } else if blocks_per_sec >= 256.0 {
            6
        } else if blocks_per_sec >= 128.0 {
            4
        } else if blocks_per_sec >= 64.0 {
            3
        } else if blocks_per_sec >= 32.0 {
            2
        } else {
            1
        };
        self.base_width
            .saturating_mul(multiplier)
            .clamp(self.base_width, self.width)
    }

    fn record_read_with_stamp(
        &self,
        ino: Ino,
        first_block: u32,
        last_block: u32,
        now_micros: u64,
    ) -> Vec<u32> {
        let now_micros = now_micros.max(1);
        let mut state = self.states.entry(ino).or_insert(ReadState {
            last_block: u32::MAX,
            score: 0,
            sequential_streak: 0,
            current_width: self.base_width,
            last_read_at_micros: 0,
            velocity_blocks_per_sec_ema: 0.0,
        });

        // Detect sequential continuation: the new read must start exactly at
        // the block immediately after where we left off.
        //
        // Important for WinFSP/FUSE small reads: many sequential workloads
        // issue multiple requests within the same block before crossing the
        // boundary. Re-reads of the same block should not suppress prefetch.
        let is_same_block = state.last_block != u32::MAX && first_block == state.last_block;
        let is_sequential =
            state.last_block != u32::MAX && first_block == state.last_block.saturating_add(1);

        let span_blocks = last_block.saturating_sub(first_block).saturating_add(1);
        let seq_gain = (self.sequential_step as u32)
            .saturating_mul(span_blocks)
            .min(MAX_SCORE as u32) as u8;

        if is_sequential {
            state.score = state.score.saturating_add(seq_gain);
            let streak_add = span_blocks.min(u16::MAX as u32) as u16;
            state.sequential_streak = state.sequential_streak.saturating_add(streak_add);

            let prev_bucket = state
                .sequential_streak
                .saturating_sub(streak_add)
                .checked_div(self.sequential_ramp_trigger)
                .unwrap_or(0);
            let next_bucket = state
                .sequential_streak
                .checked_div(self.sequential_ramp_trigger)
                .unwrap_or(0);
            if next_bucket > prev_bucket {
                let ramp_steps = next_bucket
                    .saturating_sub(prev_bucket)
                    .min(self.max_ramp_steps_per_call as u16)
                    as usize;
                // Conservative ramp-up: even if a read span crosses multiple
                // thresholds, apply only bounded doublings per call.
                let mut next_width = state.current_width;
                for _ in 0..ramp_steps {
                    next_width = next_width.saturating_mul(2);
                }
                state.current_width = next_width.clamp(self.base_width, self.width);
            }
            if state.last_read_at_micros > 0 {
                let delta_micros = now_micros.saturating_sub(state.last_read_at_micros);
                if (VELOCITY_SAMPLE_MIN_AGE_US..=VELOCITY_SAMPLE_MAX_AGE_US).contains(&delta_micros)
                {
                    let inst_bps = (span_blocks as f64 * 1_000_000.0) / delta_micros as f64;
                    if inst_bps.is_finite() && inst_bps > 0.0 {
                        state.velocity_blocks_per_sec_ema =
                            if state.velocity_blocks_per_sec_ema > 0.0 {
                                state.velocity_blocks_per_sec_ema * (1.0 - VELOCITY_SAMPLE_ALPHA)
                                    + inst_bps * VELOCITY_SAMPLE_ALPHA
                            } else {
                                inst_bps
                            };
                        let velocity_width =
                            self.velocity_width_target(state.velocity_blocks_per_sec_ema);
                        state.current_width = state
                            .current_width
                            .max(velocity_width)
                            .clamp(self.base_width, self.width);
                    }
                }
            }
        } else if is_same_block {
            // Keep score as-is.
        } else {
            let large_seek_jump = state.last_block != u32::MAX
                && first_block.abs_diff(state.last_block) >= LARGE_SEEK_RESET_BLOCK_DELTA;
            if large_seek_jump {
                state.score = 0;
                state.velocity_blocks_per_sec_ema = 0.0;
            } else {
                state.score = state.score.saturating_sub(self.random_penalty);
                state.velocity_blocks_per_sec_ema *= 0.5;
            }
            state.sequential_streak = 0;
            state.current_width = self.base_width;
        }

        state.last_block = last_block;
        state.last_read_at_micros = now_micros;
        let score = state.score;
        let current_width = state.current_width.clamp(self.base_width, self.width);
        drop(state);

        if score == 0 {
            return Vec::new();
        }

        // Return up to `min(score, width)` blocks beyond last_block.
        let mut prefetch_count = current_width.min(score as u32);
        if (is_sequential || is_same_block) && score > 0 {
            // For small sequential reads over high-latency links, keep a
            // minimum look-ahead window to avoid throughput oscillation.
            prefetch_count = prefetch_count.max(self.min_prefetch_on_seq.min(current_width));
        }
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

    /// Create a new tracker.
    ///
    /// `width` — maximum number of blocks to prefetch ahead (e.g. 8 = 32 MB
    /// look-ahead with 4 MB blocks).
    pub fn new(width: u32) -> Self {
        Self::new_with_bounds(width, width, 1, 1, 2, 4)
    }

    /// Create a tracker with custom scoring and prefetch tuning.
    pub fn new_with_tuning(
        width: u32,
        min_prefetch_on_seq: u32,
        sequential_step: u8,
        random_penalty: u8,
    ) -> Self {
        Self::new_with_bounds(
            width,
            width,
            min_prefetch_on_seq,
            sequential_step,
            random_penalty,
            4,
        )
    }

    /// Create a tracker with dynamic width bounds and custom tuning.
    pub fn new_with_bounds(
        base_width: u32,
        max_width: u32,
        min_prefetch_on_seq: u32,
        sequential_step: u8,
        random_penalty: u8,
        sequential_ramp_trigger: u16,
    ) -> Self {
        let base_width = base_width.max(1);
        let width = max_width.max(base_width);
        let min_prefetch_on_seq = min_prefetch_on_seq.max(1).min(width);
        let sequential_step = sequential_step.max(1);
        let random_penalty = random_penalty.max(1);
        let sequential_ramp_trigger = sequential_ramp_trigger.max(1);
        Self {
            states: DashMap::new(),
            inflight: DashMap::new(),
            width,
            base_width,
            min_prefetch_on_seq,
            sequential_step,
            random_penalty,
            sequential_ramp_trigger,
            max_ramp_steps_per_call: 2,
        }
    }

    /// Record that blocks `[first_block..=last_block]` were read for `ino`.
    ///
    /// Returns the list of *upcoming* block indices that the caller should
    /// schedule for prefetch.  Only blocks not already in-flight are returned.
    pub fn record_read(&self, ino: Ino, first_block: u32, last_block: u32) -> Vec<u32> {
        self.record_read_with_stamp(ino, first_block, last_block, Self::monotonic_now_micros())
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

    /// Return whether a queued prefetch block is still relevant to the current
    /// read head for this inode.
    ///
    /// After a large seek jump, many queued prefetch tasks for the old region
    /// can still be in flight. Dropping those stale tasks improves seek
    /// responsiveness for interactive video playback.
    pub fn is_prefetch_relevant(&self, ino: Ino, block: u32) -> bool {
        let Some(state) = self.states.get(&ino) else {
            return true;
        };
        let head = state.last_block;
        let tail_slack = self.width.max(1);
        let min_valid = head.saturating_sub(tail_slack);
        block >= min_valid
    }

    /// Remove all state for an inode (e.g. on file close or delete).
    pub fn evict(&self, ino: Ino) {
        self.states.remove(&ino);
    }

    #[cfg(test)]
    fn debug_width(&self, ino: Ino) -> Option<u32> {
        self.states.get(&ino).map(|s| s.current_width)
    }

    #[cfg(test)]
    fn record_read_at_micros(
        &self,
        ino: Ino,
        first_block: u32,
        last_block: u32,
        at_micros: u64,
    ) -> Vec<u32> {
        self.record_read_with_stamp(ino, first_block, last_block, at_micros)
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
        let tracker = ReadAheadTracker::new_with_bounds(8, 64, 1, 8, 2, 2);
        let mut t = 1_000_000u64;

        // First read: block 0 (score stays 0 — no prior history)
        let p = tracker.record_read_at_micros(1, 0, 0, t);
        assert!(p.is_empty());

        // Second read: block 1 (sequential after 0)
        t = t.saturating_add(500);
        let p = tracker.record_read_at_micros(1, 1, 1, t);
        assert_eq!(p.len(), 8, "score=8, width=8 should prefetch 8 blocks");
        assert_eq!(p[0], 2, "prefetch should start immediately after read head");

        // Third read crosses ramp threshold (trigger=2) and doubles width.
        t = t.saturating_add(500);
        let p = tracker.record_read_at_micros(1, 2, 2, t);
        assert!(p.len() >= 8, "ramped width should allow deeper prefetch");
        assert_eq!(tracker.debug_width(1), Some(16));

        // Keep going; width should continue to ramp but stay capped at max width.
        let mut last = 2u32;
        for _ in 0..20 {
            last += 1;
            t = t.saturating_add(500);
            let _ = tracker.record_read_at_micros(1, last, last, t);
        }
        assert_eq!(tracker.debug_width(1), Some(64));
        t = t.saturating_add(500);
        let blocks = tracker.record_read_at_micros(1, last + 1, last + 1, t);
        assert!(
            blocks.len() <= 64,
            "prefetch depth should not exceed max width"
        );
    }

    #[test]
    fn test_random_seek_scales_back_to_base_width() {
        let tracker = ReadAheadTracker::new_with_bounds(8, 64, 1, 8, 2, 2);
        let mut t = 1_000_000u64;

        let _ = tracker.record_read_at_micros(1, 0, 0, t);
        t = t.saturating_add(500);
        let _ = tracker.record_read_at_micros(1, 1, 1, t);
        t = t.saturating_add(500);
        let _ = tracker.record_read_at_micros(1, 2, 2, t);
        assert_eq!(tracker.debug_width(1), Some(16));

        // Random jump resets width ramp.
        t = t.saturating_add(500);
        let _ = tracker.record_read_at_micros(1, 1000, 1000, t);
        assert_eq!(tracker.debug_width(1), Some(8));
    }

    #[test]
    fn test_large_span_ramps_up_by_two_steps_per_call() {
        let tracker = ReadAheadTracker::new_with_bounds(8, 128, 1, 8, 2, 4);
        let mut t = 1_000_000u64;

        // Establish baseline state.
        let _ = tracker.record_read_at_micros(1, 0, 0, t);
        assert_eq!(tracker.debug_width(1), Some(8));

        // span=8 crosses two ramp buckets (trigger=4). Width should 8 -> 32.
        t = t.saturating_add(500);
        let _ = tracker.record_read_at_micros(1, 1, 8, t);
        assert_eq!(tracker.debug_width(1), Some(32));
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
    fn test_same_block_reads_do_not_decay_score() {
        let tracker = ReadAheadTracker::new(8);

        // Establish initial sequential signal (score=1).
        let _ = tracker.record_read(1, 0, 0);
        let _ = tracker.record_read(1, 1, 1);

        // Multiple same-block reads should not decay prefetch score.
        let p1 = tracker.record_read(1, 1, 1);
        let p2 = tracker.record_read(1, 1, 1);

        assert!(
            !p1.is_empty(),
            "same-block reread should keep prefetch active"
        );
        assert!(
            !p2.is_empty(),
            "same-block reread should keep prefetch active"
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

    #[test]
    fn test_aggressive_min_prefetch_window() {
        let tracker = ReadAheadTracker::new_with_tuning(64, 16, 4, 2);

        // Initial read establishes baseline (no prefetch yet).
        let p0 = tracker.record_read(1, 0, 0);
        assert!(p0.is_empty());

        // First sequential boundary should immediately open a larger window.
        let p1 = tracker.record_read(1, 1, 1);
        assert!(
            p1.len() >= 16,
            "aggressive tuning should prefetch at least 16 blocks"
        );
        assert_eq!(p1[0], 2);
    }

    #[test]
    fn test_velocity_ramp_expands_window_without_streak_buckets() {
        let tracker = ReadAheadTracker::new_with_bounds(16, 256, 4, 8, 2, u16::MAX);
        let mut t = 1_000_000u64;
        let _ = tracker.record_read_at_micros(1, 0, 0, t);

        for block in 1..=32u32 {
            t = t.saturating_add(1_000); // ~1000 blocks/sec
            let _ = tracker.record_read_at_micros(1, block, block, t);
        }

        let widened = tracker.debug_width(1).unwrap_or(16);
        assert!(
            widened >= 64,
            "high read velocity should widen prefetch window, got {widened}"
        );
    }

    #[test]
    fn test_slow_velocity_keeps_base_window() {
        let tracker = ReadAheadTracker::new_with_bounds(16, 256, 4, 8, 2, u16::MAX);
        let mut t = 1_000_000u64;
        let _ = tracker.record_read_at_micros(1, 0, 0, t);

        for block in 1..=8u32 {
            t = t.saturating_add(1_000_000); // ~1 block/sec
            let _ = tracker.record_read_at_micros(1, block, block, t);
        }

        assert_eq!(tracker.debug_width(1), Some(16));
    }

    #[test]
    fn test_stale_velocity_sample_does_not_expand_window() {
        let tracker = ReadAheadTracker::new_with_bounds(16, 256, 4, 8, 2, u16::MAX);
        let mut t = 1_000_000u64;
        let _ = tracker.record_read_at_micros(1, 0, 0, t);

        // Gap exceeds VELOCITY_SAMPLE_MAX_AGE_US, so velocity sample is ignored.
        t = t.saturating_add(VELOCITY_SAMPLE_MAX_AGE_US + 1);
        let _ = tracker.record_read_at_micros(1, 1, 1, t);

        assert_eq!(tracker.debug_width(1), Some(16));
    }

    #[test]
    fn test_large_seek_hard_resets_velocity_and_score() {
        let tracker = ReadAheadTracker::new_with_bounds(16, 256, 4, 8, 2, u16::MAX);
        let mut t = 1_000_000u64;
        let _ = tracker.record_read_at_micros(1, 0, 0, t);

        for block in 1..=32u32 {
            t = t.saturating_add(1_000);
            let _ = tracker.record_read_at_micros(1, block, block, t);
        }
        assert!(tracker.debug_width(1).unwrap_or(16) > 16);

        // Jump far enough to trigger hard reset path.
        t = t.saturating_add(1_000);
        let prefetch = tracker.record_read_at_micros(
            1,
            32 + LARGE_SEEK_RESET_BLOCK_DELTA + 32,
            32 + LARGE_SEEK_RESET_BLOCK_DELTA + 32,
            t,
        );
        assert!(
            prefetch.is_empty(),
            "large seek should reset score and suppress immediate prefetch"
        );
        assert_eq!(tracker.debug_width(1), Some(16));
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
    /// Stable generation stamp for the current readdir child set.
    readdir_touch: u64,
    /// Monotonic access stamp used for bounded LRU-style eviction.
    last_touch: u64,
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
    dirs: DashMap<Ino, DirPrefetchState>,
    /// child_ino → (parent_dir_ino, generation touch stamp)
    /// so stale reverse mappings can be pruned safely.
    child_to_dir: DashMap<Ino, (Ino, u64)>,
    /// Monotonic stamp for per-directory LRU tracking.
    access_tick: AtomicU64,
    /// Threshold: how many children must be read before prefetching triggers.
    threshold: usize,
    /// Hard cap for tracked directories to prevent unbounded growth.
    max_tracked_dirs: usize,
}

impl DirAccessTracker {
    /// Create a new tracker.
    ///
    /// `threshold` — number of children that must be read before prefetching
    /// the rest of the directory (typically 2).
    pub fn new(threshold: usize) -> Self {
        Self::new_with_capacity(threshold, 4096)
    }

    /// Create a new tracker with an explicit tracked-directory cap.
    pub fn new_with_capacity(threshold: usize, max_tracked_dirs: usize) -> Self {
        Self {
            dirs: DashMap::new(),
            child_to_dir: DashMap::new(),
            access_tick: AtomicU64::new(0),
            threshold,
            max_tracked_dirs: max_tracked_dirs.max(1),
        }
    }

    fn next_touch(&self) -> u64 {
        self.access_tick
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    fn evict_lru_if_needed(&self) {
        while self.dirs.len() > self.max_tracked_dirs {
            let Some(victim_dir) = self
                .dirs
                .iter()
                .min_by_key(|entry| entry.last_touch)
                .map(|entry| *entry.key())
            else {
                break;
            };
            if let Some((_, victim)) = self.dirs.remove(&victim_dir) {
                for child in &victim.children {
                    self.child_to_dir.remove(child);
                }
            }
        }
    }

    fn remove_child_mapping_if_unchanged(&self, child_ino: Ino, expected: (Ino, u64)) {
        if let Some(current) = self.child_to_dir.get(&child_ino)
            && *current.value() == expected
        {
            drop(current);
            self.child_to_dir.remove(&child_ino);
        }
    }

    /// Called after `readdir(dir_ino)` completes.
    ///
    /// `children` should contain the inodes of regular-file children only
    /// (exclude directories and symlinks — those have no data blocks).
    pub fn on_readdir(&self, dir_ino: Ino, children: Vec<Ino>) {
        let touch = self.next_touch();
        if let Some((_, prev)) = self.dirs.remove(&dir_ino) {
            for child in &prev.children {
                self.child_to_dir.remove(child);
            }
        }
        self.dirs.insert(
            dir_ino,
            DirPrefetchState {
                children: children.clone(),
                seen: 0,
                prefetched: HashSet::new(),
                readdir_touch: touch,
                last_touch: touch,
            },
        );
        // Register reverse mappings for the latest child set.
        for &child in &children {
            self.child_to_dir.insert(child, (dir_ino, touch));
        }
        self.evict_lru_if_needed();
    }

    /// Called when a child inode is read.
    ///
    /// Returns a list of sibling inodes whose block 0 should be prefetched.
    /// Returns an empty vec if the threshold hasn't been met yet, or if the
    /// child's parent directory isn't tracked.
    pub fn on_child_read(&self, child_ino: Ino) -> Vec<Ino> {
        let (dir_ino, mapping_touch) = match self.child_to_dir.get(&child_ino) {
            Some(v) => *v,
            None => return Vec::new(),
        };

        let touch = self.next_touch();
        let mut state = match self.dirs.get_mut(&dir_ino) {
            Some(s) => s,
            None => {
                // Reverse map can become stale after bounded eviction.
                // Remove only if no newer readdir mapping replaced it.
                self.remove_child_mapping_if_unchanged(child_ino, (dir_ino, mapping_touch));
                return Vec::new();
            }
        };
        if state.readdir_touch != mapping_touch {
            // Stale mapping observed during concurrent readdir refresh.
            self.remove_child_mapping_if_unchanged(child_ino, (dir_ino, mapping_touch));
            return Vec::new();
        }
        state.last_touch = touch;

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
        if let Some((_, state)) = self.dirs.remove(&dir_ino) {
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

    #[test]
    fn test_bounded_dir_tracking_evicts_lru_mappings() {
        let tracker = DirAccessTracker::new_with_capacity(2, 2);
        tracker.on_readdir(100, vec![10, 11]);
        tracker.on_readdir(200, vec![20, 21]);
        // Access dir 200 to make dir 100 the LRU victim.
        let _ = tracker.on_child_read(20);
        tracker.on_readdir(300, vec![30, 31]);

        // Dir 100 should be evicted and its child mappings removed.
        let p = tracker.on_child_read(10);
        assert!(
            p.is_empty(),
            "evicted directory child mapping must be cleared"
        );

        // New directory remains active.
        tracker.on_child_read(30);
        let p_new = tracker.on_child_read(31);
        assert!(p_new.is_empty());
    }

    #[test]
    fn test_readdir_refresh_prunes_stale_child_mappings() {
        let tracker = DirAccessTracker::new_with_capacity(2, 8);
        tracker.on_readdir(100, vec![10, 11, 12]);
        tracker.on_readdir(100, vec![11, 13]);

        // Child 10 no longer belongs to directory 100 and should be pruned.
        let p_old = tracker.on_child_read(10);
        assert!(p_old.is_empty());
        let p_new = tracker.on_child_read(11);
        assert!(p_new.is_empty());
    }

    #[test]
    fn test_stale_child_mapping_does_not_remove_fresh_mapping() {
        let tracker = DirAccessTracker::new_with_capacity(2, 8);
        tracker.on_readdir(100, vec![10]);
        let stale = tracker.child_to_dir.get(&10).map(|v| *v.value()).unwrap();

        // Refresh directory; child 10 is still present, but with a newer touch.
        tracker.on_readdir(100, vec![10, 11]);
        let fresh = tracker.child_to_dir.get(&10).map(|v| *v.value()).unwrap();
        assert_ne!(stale.1, fresh.1, "touch stamp should advance on refresh");

        // Simulate stale cleanup path: must not delete the fresh mapping.
        tracker.remove_child_mapping_if_unchanged(10, stale);
        assert!(
            tracker.child_to_dir.get(&10).is_some(),
            "fresh mapping must survive stale compare-and-remove"
        );
    }
}
