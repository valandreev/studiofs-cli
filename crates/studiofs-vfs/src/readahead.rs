//! Sequential read-ahead tracker.
//!
//! Detects sequential access patterns per inode and returns a list of upcoming
//! blocks that should be prefetched into cache before they are requested.
//!
//! ## Algorithm
//!
//! Each inode gets a compact `ReadState` that tracks only the previous block
//! and whether the stream is currently sequential. Once a read crosses a block
//! boundary contiguously, the tracker opens a fixed prefetch window derived
//! from the configured base/minimum settings. Non-sequential jumps or long
//! idle gaps reset the stream back to "cold" so the next reader has to prove
//! sequential progress again.
//!
//! ## In-flight deduplication
//!
//! The `inflight` DashMap tracks which blocks are already being prefetched so
//! concurrent calls to `record_read()` never schedule duplicate work.

use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{BinaryHeap, HashMap, HashSet};
#[cfg(test)]
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use studiofs_meta::types::Ino;

/// Uniquely identifies a logical read stream for readahead tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadStreamKey {
    /// Inode that owns the stream.
    pub ino: Ino,
    /// Per-handle stream identifier.
    pub stream_id: u64,
}

#[derive(Debug, Clone, Copy)]
/// Tunable controls that are applied when deciding prefetch depth.
pub struct ReadAheadPolicy {
    /// Baseline width for prefetch scheduling.
    pub base_width: u32,
    /// Maximum width for prefetch scheduling.
    pub max_width: u32,
    /// Minimum width used once sequential access is detected.
    pub min_prefetch_on_seq: u32,
    /// Master switch to disable prefetch scheduling.
    pub prefetch_enabled: bool,
}

/// Maximum idle gap before the read state resets back to cold, in microseconds.
const READ_STATE_IDLE_RESET_US: u64 = 10_000_000; // 10 s
const READAHEAD_DASHMAP_SHARDS: usize = 256;

#[cfg(test)]
type ReadAheadTrackerTestHook = Arc<dyn Fn() + Send + Sync>;

// =============================================================================
// Internal state
// =============================================================================

struct ReadState {
    /// Last block index completed by the previous read.
    last_block: u32,
    /// Whether recent reads established sequential forward progress.
    sequential_ready: bool,
    /// Fixed prefetch width once the sequential window opens.
    current_width: u32,
    /// Monotonic timestamp (microseconds) when this inode was last read.
    last_read_at_micros: u64,
}

struct InodeReadAheadState {
    /// Serializes the small amount of state owned by one inode only.
    state: Mutex<InodeReadAheadStateInner>,
}

struct InodeReadAheadStateInner {
    /// Read-ahead normally tracks only the handles currently open on this inode.
    streams: Vec<(u64, ReadState)>,
    /// Most entries belong to the small active read-ahead window.
    inflight: Vec<u32>,
}

impl Default for InodeReadAheadState {
    fn default() -> Self {
        Self {
            state: Mutex::new(InodeReadAheadStateInner {
                streams: Vec::with_capacity(4),
                inflight: Vec::with_capacity(4),
            }),
        }
    }
}

// =============================================================================
// ReadAheadTracker
// =============================================================================

/// Tracks per-inode sequential read patterns and manages in-flight prefetch state.
pub struct ReadAheadTracker {
    /// Per-inode state grouped so full eviction can remove one entry.
    states: DashMap<Ino, InodeReadAheadState>,
    /// Maximum blocks ahead to fetch when fully ramped-up.
    pub width: u32,
    /// Baseline prefetch width before ramp-up.
    base_width: u32,
    /// Lower bound for prefetch depth once sequential access is detected.
    min_prefetch_on_seq: u32,
    #[cfg(test)]
    evict_hook: Option<ReadAheadTrackerTestHook>,
    #[cfg(test)]
    evict_stream_hook: Option<ReadAheadTrackerTestHook>,
}

impl ReadAheadTracker {
    fn default_policy(&self) -> ReadAheadPolicy {
        ReadAheadPolicy {
            base_width: self.base_width,
            max_width: self.width,
            min_prefetch_on_seq: self.min_prefetch_on_seq,
            prefetch_enabled: self.width > 0,
        }
    }

    fn monotonic_now_micros() -> u64 {
        static START: OnceLock<Instant> = OnceLock::new();
        START
            .get_or_init(Instant::now)
            .elapsed()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    fn with_inode_state<T, F>(&self, ino: Ino, operation: F) -> T
    where
        F: FnOnce(&InodeReadAheadState) -> T,
    {
        if let Some(inode_state) = self.states.get(&ino) {
            return operation(&inode_state);
        }

        let inode_state = self.states.entry(ino).or_default();
        operation(&inode_state)
    }

    fn with_existing_inode_state<T, F>(&self, ino: Ino, operation: F) -> Option<T>
    where
        F: FnOnce(&InodeReadAheadState) -> T,
    {
        let inode_state = self.states.get(&ino)?;
        Some(operation(&inode_state))
    }

    fn record_read_with_stamp(
        &self,
        key: ReadStreamKey,
        first_block: u32,
        last_block: u32,
        policy: ReadAheadPolicy,
        now_micros: u64,
    ) -> Vec<u32> {
        if !policy.prefetch_enabled || policy.base_width == 0 || policy.max_width == 0 {
            return Vec::new();
        }

        let now_micros = now_micros.max(1);
        let base_width = policy.base_width.max(1);
        let max_width = policy.max_width.max(base_width);
        let fixed_width = policy
            .min_prefetch_on_seq
            .max(base_width)
            .clamp(base_width, max_width);
        self.with_inode_state(key.ino, |inode_state| {
            let mut state = inode_state.state.lock();
            let read_state = if let Some(index) = state
                .streams
                .iter()
                .position(|(stream_id, _)| *stream_id == key.stream_id)
            {
                &mut state.streams[index].1
            } else {
                state.streams.push((
                    key.stream_id,
                    ReadState {
                        last_block: u32::MAX,
                        sequential_ready: false,
                        current_width: base_width,
                        last_read_at_micros: 0,
                    },
                ));
                &mut state
                    .streams
                    .last_mut()
                    .expect("stream was pushed before it was accessed")
                    .1
            };

            if read_state.last_read_at_micros > 0
                && now_micros.saturating_sub(read_state.last_read_at_micros)
                    > READ_STATE_IDLE_RESET_US
            {
                read_state.sequential_ready = false;
                read_state.current_width = base_width;
                read_state.last_block = u32::MAX;
            }

            // Detect sequential continuation: the new read must start exactly at
            // the block immediately after where we left off.
            //
            // Important for WinFSP/FUSE small reads: many sequential workloads
            // issue multiple requests within the same block before crossing the
            // boundary. Re-reads of the same block should not suppress prefetch.
            let is_same_block =
                read_state.last_block != u32::MAX && first_block == read_state.last_block;
            let is_sequential = read_state.last_block != u32::MAX
                && first_block == read_state.last_block.saturating_add(1);

            let was_ready = read_state.sequential_ready;
            if is_sequential {
                read_state.sequential_ready = true;
                read_state.current_width = fixed_width;
            } else if is_same_block {
                if !read_state.sequential_ready {
                    read_state.current_width = base_width;
                }
            } else {
                read_state.sequential_ready = false;
                read_state.current_width = base_width;
            }

            read_state.last_block = last_block;
            read_state.last_read_at_micros = now_micros;
            let current_width = read_state.current_width.clamp(base_width, max_width);
            let sequential_ready = read_state.sequential_ready;

            if !(is_sequential || (is_same_block && (was_ready || sequential_ready))) {
                return Vec::new();
            }
            (1..=current_width)
                .filter_map(|i| {
                    let block = last_block.checked_add(i)?;
                    if state.inflight.contains(&block) {
                        None // Already in-flight
                    } else {
                        Some(block)
                    }
                })
                .collect()
        })
    }

    /// Create a new tracker.
    ///
    /// `width` — maximum number of blocks to prefetch ahead (e.g. 8 = 32 MB
    /// look-ahead with 4 MB blocks).
    pub fn new(width: u32) -> Self {
        Self::new_with_bounds(width, width, 1)
    }

    /// Create a tracker with a custom sequential prefetch floor.
    pub fn new_with_tuning(width: u32, min_prefetch_on_seq: u32) -> Self {
        Self::new_with_bounds(width, width, min_prefetch_on_seq)
    }

    /// Create a tracker with dynamic width bounds.
    pub fn new_with_bounds(base_width: u32, max_width: u32, min_prefetch_on_seq: u32) -> Self {
        let (base_width, width, min_prefetch_on_seq) = if base_width == 0 {
            (0, 0, 0)
        } else {
            let base_width = base_width.max(1);
            let width = max_width.max(base_width);
            (base_width, width, min_prefetch_on_seq.max(1).min(width))
        };
        Self {
            states: DashMap::with_capacity_and_shard_amount(4_096, READAHEAD_DASHMAP_SHARDS),
            width,
            base_width,
            min_prefetch_on_seq,
            #[cfg(test)]
            evict_hook: None,
            #[cfg(test)]
            evict_stream_hook: None,
        }
    }

    /// Record that blocks `[first_block..=last_block]` were read for `ino`.
    ///
    /// Returns the list of *upcoming* block indices that the caller should
    /// schedule for prefetch.  Only blocks not already in-flight are returned.
    pub fn record_read(&self, ino: Ino, first_block: u32, last_block: u32) -> Vec<u32> {
        self.record_read_for_stream(
            ReadStreamKey { ino, stream_id: 0 },
            first_block,
            last_block,
            self.default_policy(),
        )
    }

    /// Record read coverage for a specific stream key under an explicit policy.
    ///
    /// Returns the same upcoming blocks as [`Self::record_read`], but allows multiple
    /// independent streams per inode.
    pub fn record_read_for_stream(
        &self,
        key: ReadStreamKey,
        first_block: u32,
        last_block: u32,
        policy: ReadAheadPolicy,
    ) -> Vec<u32> {
        self.record_read_with_stamp(
            key,
            first_block,
            last_block,
            policy,
            Self::monotonic_now_micros(),
        )
    }

    /// Mark a block as in-flight for prefetch.
    ///
    /// Returns `true` if the block was not previously in-flight (caller should
    /// proceed with the prefetch).  Returns `false` if the block is already
    /// being fetched by another task.
    pub fn start_prefetch(&self, ino: Ino, block: u32) -> bool {
        self.with_inode_state(ino, |inode_state| {
            let mut state = inode_state.state.lock();
            if state.inflight.contains(&block) {
                false
            } else {
                state.inflight.push(block);
                true
            }
        })
    }

    /// Mark a prefetch as complete, allowing future re-scheduling if needed.
    pub fn finish_prefetch(&self, ino: Ino, block: u32) {
        let Some(should_cleanup) = self.with_existing_inode_state(ino, |inode_state| {
            let mut state = inode_state.state.lock();
            if let Some(index) = state
                .inflight
                .iter()
                .position(|candidate| *candidate == block)
            {
                state.inflight.swap_remove(index);
            }
            state.streams.is_empty() && state.inflight.is_empty()
        }) else {
            return;
        };
        if should_cleanup {
            self.remove_empty_inode_state(ino);
        }
    }

    /// Return whether a queued prefetch block is still relevant to the current
    /// read head for this inode.
    ///
    /// After a large seek jump, many queued prefetch tasks for the old region
    /// can still be in flight. Dropping those stale tasks improves seek
    /// responsiveness for interactive video playback.
    pub fn is_prefetch_relevant(&self, ino: Ino, block: u32) -> bool {
        let Some(relevant) = self.with_existing_inode_state(ino, |inode_state| {
            let state = inode_state.state.lock();
            let mut found_stream = false;
            for (_, read_state) in &state.streams {
                found_stream = true;
                if Self::state_considers_block_relevant(read_state, self.width.max(1), block) {
                    return true;
                }
            }
            !found_stream
        }) else {
            return true;
        };
        relevant
    }

    /// Check whether a prefetch candidate is still relevant for a specific stream.
    ///
    /// Returns `true` if the block still sits near the current stream head, or
    /// if the stream is not currently tracked (conservative fallback).
    pub fn stream_prefetch_relevant(&self, key: ReadStreamKey, block: u32) -> bool {
        let Some(relevant) = self.with_existing_inode_state(key.ino, |inode_state| {
            let state = inode_state.state.lock();
            let Some((_, read_state)) = state
                .streams
                .iter()
                .find(|(stream_id, _)| *stream_id == key.stream_id)
            else {
                return true;
            };
            Self::state_considers_block_relevant(read_state, self.width.max(1), block)
        }) else {
            return true;
        };
        relevant
    }

    /// Remove cached tracking state for a specific stream key.
    pub fn evict_stream(&self, key: ReadStreamKey) {
        let state_after_removal = self.with_existing_inode_state(key.ino, |inode_state| {
            let mut state = inode_state.state.lock();
            if let Some(index) = state
                .streams
                .iter()
                .position(|(stream_id, _)| *stream_id == key.stream_id)
            {
                state.streams.swap_remove(index);
            }
            state.streams.is_empty() && state.inflight.is_empty()
        });
        #[cfg(test)]
        if let Some(hook) = &self.evict_stream_hook {
            hook();
        }
        if let Some(true) = state_after_removal {
            self.remove_empty_inode_state(key.ino);
        }
    }

    /// Remove all state for an inode (e.g. on file close or delete).
    pub fn evict(&self, ino: Ino) {
        #[cfg(test)]
        if let Some(hook) = &self.evict_hook {
            hook();
        }
        self.states.remove(&ino);
    }

    fn remove_empty_inode_state(&self, ino: Ino) {
        self.states.remove_if(&ino, |_, inode_state| {
            let state = inode_state.state.lock();
            state.streams.is_empty() && state.inflight.is_empty()
        });
    }

    fn state_considers_block_relevant(state: &ReadState, tail_slack: u32, block: u32) -> bool {
        let head = state.last_block;
        let min_valid = head.saturating_sub(tail_slack);
        block >= min_valid
    }

    #[cfg(test)]
    fn debug_width(&self, ino: Ino) -> Option<u32> {
        self.states.get(&ino).and_then(|inode_state| {
            inode_state
                .state
                .lock()
                .streams
                .iter()
                .find(|(stream_id, _)| *stream_id == 0)
                .map(|(_, stream_state)| stream_state.current_width)
        })
    }

    #[cfg(test)]
    pub(crate) fn debug_has_stream(&self, key: ReadStreamKey) -> bool {
        self.states.get(&key.ino).is_some_and(|inode_state| {
            inode_state
                .state
                .lock()
                .streams
                .iter()
                .any(|(stream_id, _)| *stream_id == key.stream_id)
        })
    }

    #[cfg(test)]
    fn record_read_at_micros(
        &self,
        ino: Ino,
        first_block: u32,
        last_block: u32,
        at_micros: u64,
    ) -> Vec<u32> {
        self.record_read_with_stamp(
            ReadStreamKey { ino, stream_id: 0 },
            first_block,
            last_block,
            self.default_policy(),
            at_micros,
        )
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn test_no_prefetch_on_first_read() {
        let tracker = ReadAheadTracker::new(8);
        let blocks = tracker.record_read(1, 0, 0);
        assert!(
            blocks.is_empty(),
            "first read of a brand-new inode should not trigger prefetch"
        );
    }

    #[test]
    fn zero_width_disables_prefetch_after_sequential_reads() {
        let tracker = ReadAheadTracker::new_with_bounds(0, 64, 1);

        assert_eq!(tracker.width, 0);
        assert!(tracker.record_read_at_micros(1, 0, 0, 1_000_000).is_empty());
        assert!(tracker.record_read_at_micros(1, 1, 1, 1_500_000).is_empty());
    }

    #[test]
    fn test_sequential_ramp_up() {
        let tracker = ReadAheadTracker::new_with_bounds(8, 64, 1);
        let mut t = 1_000_000u64;

        // First read: block 0 establishes baseline state only.
        let p = tracker.record_read_at_micros(1, 0, 0, t);
        assert!(p.is_empty());

        // Second read: block 1 opens the fixed sequential window.
        t = t.saturating_add(500_000);
        let p = tracker.record_read_at_micros(1, 1, 1, t);
        assert_eq!(
            p.len(),
            8,
            "sequential reads should use the configured window"
        );
        assert_eq!(p[0], 2, "prefetch should start immediately after read head");

        // Continued sequential reads keep the same stable width.
        t = t.saturating_add(500_000);
        let p = tracker.record_read_at_micros(1, 2, 2, t);
        assert_eq!(
            p.len(),
            8,
            "the simplified model should avoid widening surprises"
        );
        assert_eq!(tracker.debug_width(1), Some(8));

        let mut last = 2u32;
        for _ in 0..20 {
            last += 1;
            t = t.saturating_add(500_000);
            let _ = tracker.record_read_at_micros(1, last, last, t);
        }
        assert_eq!(tracker.debug_width(1), Some(8));
        t = t.saturating_add(500_000);
        let blocks = tracker.record_read_at_micros(1, last + 1, last + 1, t);
        assert!(
            blocks.len() <= 8,
            "prefetch depth should stay on the configured fixed window"
        );
    }

    #[test]
    fn test_random_seek_scales_back_to_base_width() {
        let tracker = ReadAheadTracker::new_with_bounds(8, 64, 1);
        let mut t = 1_000_000u64;

        let _ = tracker.record_read_at_micros(1, 0, 0, t);
        t = t.saturating_add(500_000);
        let _ = tracker.record_read_at_micros(1, 1, 1, t);
        t = t.saturating_add(500_000);
        let _ = tracker.record_read_at_micros(1, 2, 2, t);
        assert_eq!(tracker.debug_width(1), Some(8));

        t = t.saturating_add(500_000);
        let random = tracker.record_read_at_micros(1, 1000, 1000, t);
        assert!(
            random.is_empty(),
            "random jump should suppress immediate prefetch"
        );
        assert_eq!(tracker.debug_width(1), Some(8));
    }

    #[test]
    fn test_large_span_opens_the_same_fixed_window() {
        let tracker = ReadAheadTracker::new_with_bounds(8, 128, 1);
        let mut t = 1_000_000u64;

        // Establish baseline state.
        let _ = tracker.record_read_at_micros(1, 0, 0, t);
        assert_eq!(tracker.debug_width(1), Some(8));

        t = t.saturating_add(500_000);
        let p = tracker.record_read_at_micros(1, 1, 8, t);
        assert_eq!(tracker.debug_width(1), Some(8));
        assert_eq!(p.len(), 8);
    }

    #[test]
    fn test_random_jump_resets_prefetch_state() {
        let tracker = ReadAheadTracker::new(8);

        // Build up sequential state.
        for i in 0..8u32 {
            tracker.record_read(1, i, i);
        }

        let jump_positions = [1000u32, 50000, 200000, 999999, 123456];
        for pos in jump_positions {
            let _ = tracker.record_read(1, pos, pos);
        }

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
    fn test_inode_relevance_accepts_any_live_stream() {
        let tracker = ReadAheadTracker::new(8);
        let policy = tracker.default_policy();
        let stale = ReadStreamKey {
            ino: 99,
            stream_id: 1,
        };
        let live = ReadStreamKey {
            ino: 99,
            stream_id: 2,
        };

        let _ = tracker.record_read_for_stream(stale, 0, 0, policy);
        let _ = tracker.record_read_for_stream(stale, 1, 1, policy);
        let _ = tracker.record_read_for_stream(live, 0, 0, policy);
        let _ = tracker.record_read_for_stream(live, 1, 1, policy);
        let _ = tracker.record_read_for_stream(stale, 1024, 1024, policy);

        assert!(!tracker.stream_prefetch_relevant(stale, 2));
        assert!(tracker.stream_prefetch_relevant(live, 2));
        assert!(tracker.is_prefetch_relevant(99, 2));
    }

    #[test]
    fn test_evict_stream_removes_only_target_stream() {
        let tracker = ReadAheadTracker::new(8);
        let policy = tracker.default_policy();
        let keep = ReadStreamKey {
            ino: 77,
            stream_id: 1,
        };
        let drop_key = ReadStreamKey {
            ino: 77,
            stream_id: 2,
        };

        let _ = tracker.record_read_for_stream(keep, 0, 0, policy);
        let _ = tracker.record_read_for_stream(drop_key, 0, 0, policy);
        tracker.evict_stream(drop_key);

        assert!(tracker.debug_has_stream(keep));
        assert!(!tracker.debug_has_stream(drop_key));
    }

    #[test]
    fn test_evict_uses_one_inode_scoped_removal() {
        let mut tracker = ReadAheadTracker::new(8);
        let observed_passes = Arc::new(AtomicUsize::new(0));
        let observed_passes_for_hook = Arc::clone(&observed_passes);
        tracker.evict_hook = Some(Arc::new(move || {
            observed_passes_for_hook.fetch_add(1, Ordering::SeqCst);
        }));

        tracker.evict(1);

        assert_eq!(
            observed_passes.load(Ordering::SeqCst),
            1,
            "full eviction should remove one inode entry instead of scanning each global map"
        );
    }

    #[test]
    fn test_evict_clears_selected_inode_only() {
        let tracker = ReadAheadTracker::new(8);
        let policy = tracker.default_policy();
        let selected = ReadStreamKey {
            ino: 1,
            stream_id: 11,
        };
        let retained = ReadStreamKey {
            ino: 2,
            stream_id: 22,
        };

        let _ = tracker.record_read_for_stream(selected, 0, 0, policy);
        let _ = tracker.record_read_for_stream(retained, 0, 0, policy);
        assert!(tracker.start_prefetch(selected.ino, 7));
        assert!(tracker.start_prefetch(retained.ino, 7));

        tracker.evict(selected.ino);

        assert!(!tracker.debug_has_stream(selected));
        assert!(tracker.debug_has_stream(retained));
        assert!(
            tracker.start_prefetch(selected.ino, 7),
            "full eviction should clear the selected inode's in-flight blocks"
        );
        assert!(
            !tracker.start_prefetch(retained.ino, 7),
            "full eviction must not clear another inode's in-flight blocks"
        );
    }

    #[test]
    fn test_concurrent_evict_stream_preserves_new_stream() {
        let mut tracker = ReadAheadTracker::new(8);
        let policy = tracker.default_policy();
        let target = ReadStreamKey {
            ino: 1,
            stream_id: 11,
        };
        let new_stream = ReadStreamKey {
            ino: 1,
            stream_id: 22,
        };
        let _ = tracker.record_read_for_stream(target, 0, 0, policy);

        let cleanup_pause = Arc::new(Barrier::new(2));
        let cleanup_pause_for_hook = Arc::clone(&cleanup_pause);
        tracker.evict_stream_hook = Some(Arc::new(move || {
            cleanup_pause_for_hook.wait();
        }));
        let tracker = Arc::new(tracker);
        let recorder_tracker = Arc::clone(&tracker);
        let recorder_pause = Arc::clone(&cleanup_pause);
        let recorder = thread::spawn(move || {
            recorder_pause.wait();
            let _ = recorder_tracker.record_read_for_stream(new_stream, 0, 0, policy);
        });

        tracker.evict_stream(target);
        recorder.join().expect("stream recorder should finish");

        assert!(!tracker.debug_has_stream(target));
        assert!(
            tracker.debug_has_stream(new_stream),
            "a stream recorded during empty-group cleanup must not be removed"
        );
    }

    #[test]
    fn test_evict_stream_preserves_inflight_dedup_until_finish() {
        let tracker = ReadAheadTracker::new(8);
        let key = ReadStreamKey {
            ino: 1,
            stream_id: 11,
        };
        let _ = tracker.record_read_for_stream(key, 0, 0, tracker.default_policy());
        assert!(tracker.start_prefetch(key.ino, 7));

        tracker.evict_stream(key);

        assert!(!tracker.debug_has_stream(key));
        assert!(
            !tracker.start_prefetch(key.ino, 7),
            "stream eviction must not discard an in-flight inode block"
        );
        tracker.finish_prefetch(key.ino, 7);
        assert!(tracker.start_prefetch(key.ino, 7));
    }

    #[test]
    fn test_same_block_reads_keep_prefetch_active() {
        let tracker = ReadAheadTracker::new(8);

        // Establish initial sequential signal.
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

        // After evict, state is gone — next read should not see prior sequential state.
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
        let tracker = ReadAheadTracker::new_with_tuning(64, 16);

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
    fn test_far_jump_resets_window_and_prefetch_state() {
        let tracker = ReadAheadTracker::new_with_bounds(16, 256, 4);
        let mut t = 1_000_000u64;
        let _ = tracker.record_read_at_micros(1, 0, 0, t);

        for block in 1..=8u32 {
            t = t.saturating_add(1_000_000);
            let _ = tracker.record_read_at_micros(1, block, block, t);
        }
        assert_eq!(tracker.debug_width(1), Some(16));

        // A far non-sequential jump should clear the current sequential state.
        t = t.saturating_add(1_000);
        let prefetch = tracker.record_read_at_micros(1, 320, 320, t);
        assert!(
            prefetch.is_empty(),
            "far jumps should reset sequential state and suppress immediate prefetch"
        );
        assert_eq!(tracker.debug_width(1), Some(16));
    }

    #[test]
    fn test_idle_gap_resets_window_before_prefetch_resumes() {
        let tracker = ReadAheadTracker::new_with_bounds(16, 256, 4);
        let mut t = 1_000_000u64;
        let _ = tracker.record_read_at_micros(1, 0, 0, t);
        for block in 1..=8u32 {
            t = t.saturating_add(1_000_000);
            let _ = tracker.record_read_at_micros(1, block, block, t);
        }
        assert_eq!(tracker.debug_width(1), Some(16));

        t = t.saturating_add(15_000_000);
        let reset = tracker.record_read_at_micros(1, 9, 9, t);
        assert!(
            reset.is_empty(),
            "reads after an idle reset should rebuild the sequential signal first"
        );
        assert_eq!(tracker.debug_width(1), Some(16));

        t = t.saturating_add(1_000_000);
        let resumed = tracker.record_read_at_micros(1, 10, 10, t);
        assert!(
            !resumed.is_empty(),
            "the next contiguous read should re-open the fixed sequential prefetch window"
        );
    }

    #[test]
    fn test_predictable_window_stays_stable_across_sequential_reads() {
        let tracker = ReadAheadTracker::new_with_bounds(16, 256, 4);
        let mut t = 1_000_000u64;
        let _ = tracker.record_read_at_micros(1, 0, 0, t);

        t = t.saturating_add(1_000_000);
        let second = tracker.record_read_at_micros(1, 1, 1, t);
        assert!(
            second.len() >= 16,
            "first sequential continuation should open the configured minimum window"
        );

        t = t.saturating_add(1_000_000);
        let third = tracker.record_read_at_micros(1, 2, 2, t);
        assert_eq!(
            second.len(),
            third.len(),
            "steady sequential reads should keep a predictable window instead of velocity-driven swings"
        );
    }
}

// =============================================================================
// Directory-Level Prefetch Tracker
// =============================================================================

/// State for a directory whose children are being accessed.
struct DirPrefetchState {
    /// All regular-file child inodes from readdir.
    children: Vec<Ino>,
    /// How many children we've seen accessed (open/read).
    seen: usize,
    /// Which children have been observed through a real foreground access.
    accessed: HashSet<Ino>,
    /// Which children have already been scheduled for prefetch.
    prefetched: HashSet<Ino>,
    /// Contiguous frame-sequence runs in numeric order.
    sequences: Vec<Vec<Ino>>,
    /// child ino -> position inside `sequences`.
    sequence_positions: HashMap<Ino, FrameSequencePosition>,
    /// Stable generation stamp for the current readdir child set.
    readdir_touch: u64,
    /// Monotonic access stamp used for bounded LRU-style eviction.
    last_touch: u64,
}

#[cfg(test)]
type DirAccessTrackerTestHook = Arc<dyn Fn() + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
/// A directory entry being tracked for sequence-aware prefetching.
pub struct DirAccessEntry<'a> {
    /// Inode for a regular-file directory child.
    pub ino: Ino,
    /// Raw directory entry name as returned by metadata.
    pub name: &'a [u8],
}

impl<'a> DirAccessEntry<'a> {
    /// Create a tracked directory child entry from an inode and raw name bytes.
    pub fn new(ino: Ino, name: &'a [u8]) -> Self {
        Self { ino, name }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FrameSequenceKey<'a> {
    prefix: &'a [u8],
    width: usize,
    suffix: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
struct FrameSequenceMember {
    number: u64,
    ino: Ino,
}

#[derive(Debug, Clone, Copy)]
struct FrameSequencePosition {
    sequence_idx: usize,
    member_idx: usize,
}

#[derive(Debug, Default)]
struct FrameSequenceIndex {
    sequences: Vec<Vec<Ino>>,
    positions: HashMap<Ino, FrameSequencePosition>,
}

fn parse_frame_sequence_member<'a>(
    entry: &DirAccessEntry<'a>,
) -> Option<(FrameSequenceKey<'a>, u64)> {
    let name = entry.name;
    let stem_end = name.iter().rposition(|b| *b == b'.').unwrap_or(name.len());
    if stem_end == 0 {
        return None;
    }

    let mut digit_start = stem_end;
    while digit_start > 0 && name[digit_start - 1].is_ascii_digit() {
        digit_start -= 1;
    }
    if digit_start == stem_end {
        return None;
    }

    let mut number = 0u64;
    for digit in &name[digit_start..stem_end] {
        number = number
            .checked_mul(10)?
            .checked_add(u64::from(digit - b'0'))?;
    }

    Some((
        FrameSequenceKey {
            prefix: &name[..digit_start],
            width: stem_end - digit_start,
            suffix: &name[stem_end..],
        },
        number,
    ))
}

fn build_sequence_index<'a>(entries: &[DirAccessEntry<'a>]) -> FrameSequenceIndex {
    let mut groups: HashMap<FrameSequenceKey<'a>, Vec<FrameSequenceMember>> = HashMap::new();
    for entry in entries {
        if let Some((key, number)) = parse_frame_sequence_member(entry) {
            groups.entry(key).or_default().push(FrameSequenceMember {
                number,
                ino: entry.ino,
            });
        }
    }

    let mut index = FrameSequenceIndex::default();
    for members in groups.values_mut() {
        if members.len() < 2 {
            continue;
        }
        members.sort_unstable_by_key(|member| (member.number, member.ino));
        let mut run_start = 0;
        for run_end in 1..=members.len() {
            let run_continues = run_end < members.len()
                && members[run_end - 1].number.checked_add(1) == Some(members[run_end].number);
            if run_continues {
                continue;
            }

            if run_end - run_start >= 2 {
                let sequence_idx = index.sequences.len();
                let sequence: Vec<Ino> = members[run_start..run_end]
                    .iter()
                    .map(|member| member.ino)
                    .collect();
                for (member_idx, &ino) in sequence.iter().enumerate() {
                    index.positions.insert(
                        ino,
                        FrameSequencePosition {
                            sequence_idx,
                            member_idx,
                        },
                    );
                }
                index.sequences.push(sequence);
            }
            run_start = run_end;
        }
    }
    index
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
    /// Serializes slow-path LRU selection across concurrent registrations.
    eviction_lock: Mutex<()>,
    #[cfg(test)]
    test_hook: OnceLock<DirAccessTrackerTestHook>,
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
            eviction_lock: Mutex::new(()),
            #[cfg(test)]
            test_hook: OnceLock::new(),
        }
    }

    fn next_touch(&self) -> u64 {
        self.access_tick
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    fn evict_lru_if_needed(&self) {
        if self.dirs.len() <= self.max_tracked_dirs {
            return;
        }
        let _eviction_guard = self.eviction_lock.lock();
        loop {
            let excess = self.dirs.len().saturating_sub(self.max_tracked_dirs);
            if excess == 0 {
                break;
            }
            let candidates = self.select_lru_candidates(excess);
            if candidates.is_empty() {
                break;
            }
            #[cfg(test)]
            self.invoke_test_hook();

            // A candidate can become stale while another read touches the directory.
            // The next loop selects a replacement from the current map. The same
            // loop coalesces registrations that arrive during this batch.
            for (victim_dir, observed_last_touch) in candidates {
                if self.dirs.len() <= self.max_tracked_dirs {
                    break;
                }
                if let Some((_, victim)) = self.dirs.remove_if(&victim_dir, |_, state| {
                    state.last_touch == observed_last_touch
                }) {
                    for child in &victim.children {
                        self.remove_child_mapping_if_same(
                            *child,
                            (victim_dir, victim.readdir_touch),
                        );
                    }
                }
            }
        }
    }

    fn select_lru_candidates(&self, count: usize) -> Vec<(Ino, u64)> {
        // Keep the newest selected entry at the top so each replacement leaves
        // only the oldest `count` entries in the bounded heap.
        let mut candidates = BinaryHeap::with_capacity(count);
        for entry in &self.dirs {
            let candidate = (entry.last_touch, *entry.key());
            if candidates.len() < count {
                candidates.push(candidate);
            } else if candidates.peek().is_some_and(|newest| candidate < *newest) {
                candidates.pop();
                candidates.push(candidate);
            }
        }

        candidates
            .into_sorted_vec()
            .into_iter()
            .map(|(last_touch, dir_ino)| (dir_ino, last_touch))
            .collect()
    }

    fn remove_child_mapping_if_same(&self, child_ino: Ino, expected: (Ino, u64)) {
        // Keep the generation check and removal under the same shard lock.
        self.child_to_dir.remove_if(&child_ino, |_, current| {
            let matches_expected = *current == expected;
            #[cfg(test)]
            if matches_expected {
                self.invoke_test_hook();
            }
            matches_expected
        });
    }

    /// Called after `readdir(dir_ino)` completes.
    ///
    /// `children` should contain the inodes of regular-file children only
    /// (exclude directories and symlinks — those have no file payload).
    pub fn on_readdir(&self, dir_ino: Ino, children: Vec<Ino>) {
        let entries = children
            .into_iter()
            .map(|ino| DirAccessEntry { ino, name: &[] })
            .collect();
        self.on_readdir_entries(dir_ino, entries);
    }

    /// Called after `readdir(dir_ino)` completes with child names available.
    pub fn on_readdir_entries(&self, dir_ino: Ino, entries: Vec<DirAccessEntry<'_>>) {
        let touch = self.next_touch();
        if let Some((_, prev)) = self.dirs.remove(&dir_ino) {
            #[cfg(test)]
            self.invoke_test_hook();
            for child in &prev.children {
                self.remove_child_mapping_if_same(*child, (dir_ino, prev.readdir_touch));
            }
        }
        let children: Vec<Ino> = entries.iter().map(|entry| entry.ino).collect();
        let sequence_index = build_sequence_index(&entries);
        self.dirs.insert(
            dir_ino,
            DirPrefetchState {
                children: children.clone(),
                seen: 0,
                accessed: HashSet::new(),
                prefetched: HashSet::new(),
                sequences: sequence_index.sequences,
                sequence_positions: sequence_index.positions,
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
        self.on_child_read_limited(child_ino, usize::MAX)
    }

    /// Called when a child inode is read, with an upper bound on returned inodes.
    pub fn on_child_read_limited(&self, child_ino: Ino, limit: usize) -> Vec<Ino> {
        if limit == 0 {
            return Vec::new();
        }

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
                self.remove_child_mapping_if_same(child_ino, (dir_ino, mapping_touch));
                return Vec::new();
            }
        };
        if state.readdir_touch != mapping_touch {
            // Stale mapping observed during concurrent readdir refresh.
            self.remove_child_mapping_if_same(child_ino, (dir_ino, mapping_touch));
            return Vec::new();
        }
        state.last_touch = touch;

        // Already counted this foreground access? Skip.
        if !state.accessed.insert(child_ino) {
            return Vec::new();
        }

        if let Some(position) = state.sequence_positions.get(&child_ino).copied() {
            let Some(sequence) = state.sequences.get(position.sequence_idx) else {
                return Vec::new();
            };
            let Some(successors) = sequence.get(position.member_idx.saturating_add(1)..) else {
                return Vec::new();
            };
            // `accessed` contains prior foreground reads from this immutable
            // readdir snapshot; avoid warming files the caller already touched.
            let to_prefetch: Vec<Ino> = successors
                .iter()
                .copied()
                .filter(|ino| !state.accessed.contains(ino) && !state.prefetched.contains(ino))
                .take(limit)
                .collect();
            for &ino in &to_prefetch {
                state.prefetched.insert(ino);
            }
            return to_prefetch;
        }

        state.seen += 1;

        // Not enough children read yet — don't prefetch.
        if state.seen < self.threshold {
            return Vec::new();
        }

        // Threshold met: return all un-prefetched children.
        // `children` is the original readdir snapshot, so accessed children
        // remain present here even after foreground reads.
        let to_prefetch: Vec<Ino> = state
            .children
            .iter()
            .filter(|ino| !state.accessed.contains(ino) && !state.prefetched.contains(ino))
            .copied()
            .take(limit)
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
            #[cfg(test)]
            self.invoke_test_hook();
            for child in &state.children {
                self.remove_child_mapping_if_same(*child, (dir_ino, state.readdir_touch));
            }
        }
    }

    #[cfg(test)]
    fn set_test_hook(&self, hook: DirAccessTrackerTestHook) {
        assert!(
            self.test_hook.set(hook).is_ok(),
            "test hook must only be configured once"
        );
    }

    #[cfg(test)]
    fn debug_tracked_dir_count(&self) -> usize {
        self.dirs.len()
    }

    #[cfg(test)]
    fn invoke_test_hook(&self) {
        if let Some(hook) = self.test_hook.get().cloned() {
            hook();
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
    fn test_matching_child_mapping_is_removed() {
        let tracker = DirAccessTracker::new_with_capacity(2, 8);
        tracker.on_readdir(100, vec![10]);
        let current = tracker.child_to_dir.get(&10).map(|v| *v.value()).unwrap();

        tracker.remove_child_mapping_if_same(10, current);

        assert!(
            tracker.child_to_dir.get(&10).is_none(),
            "the exact current child mapping should be removed"
        );
    }

    #[test]
    fn test_mismatched_generation_does_not_remove_fresh_mapping() {
        let tracker = DirAccessTracker::new_with_capacity(2, 8);
        tracker.on_readdir(100, vec![10]);
        let stale = tracker.child_to_dir.get(&10).map(|v| *v.value()).unwrap();

        // Refresh directory; child 10 is still present, but with a newer touch.
        tracker.on_readdir(100, vec![10, 11]);
        let fresh = tracker.child_to_dir.get(&10).map(|v| *v.value()).unwrap();
        assert_ne!(stale.1, fresh.1, "touch stamp should advance on refresh");

        // Simulate stale cleanup path: must not delete the fresh mapping.
        tracker.remove_child_mapping_if_same(10, stale);
        assert!(
            tracker
                .child_to_dir
                .get(&10)
                .is_some_and(|current| *current.value() == fresh),
            "fresh mapping must survive stale compare-and-remove"
        );
    }

    #[test]
    fn test_readdir_cleanup_preserves_mapping_reassigned_to_another_directory() {
        let tracker = Arc::new(DirAccessTracker::new_with_capacity(2, 8));
        tracker.on_readdir(100, vec![10]);

        let cleanup_started = Arc::new(std::sync::Barrier::new(2));
        let cleanup_release = Arc::new(std::sync::Barrier::new(2));
        let started_for_hook = Arc::clone(&cleanup_started);
        let release_for_hook = Arc::clone(&cleanup_release);
        tracker.set_test_hook(Arc::new(move || {
            started_for_hook.wait();
            release_for_hook.wait();
        }));

        let refresh_tracker = Arc::clone(&tracker);
        let refresh = std::thread::spawn(move || {
            refresh_tracker.on_readdir(100, vec![11]);
        });
        cleanup_started.wait();

        tracker.on_readdir(200, vec![10]);
        let reassigned = tracker.child_to_dir.get(&10).map(|v| *v.value()).unwrap();
        cleanup_release.wait();

        refresh.join().unwrap();
        assert_eq!(
            tracker.child_to_dir.get(&10).map(|v| *v.value()),
            Some(reassigned),
            "refresh cleanup must not remove a mapping reassigned to another directory"
        );
    }

    #[test]
    fn test_evict_dir_cleanup_preserves_mapping_reassigned_to_another_directory() {
        let tracker = Arc::new(DirAccessTracker::new_with_capacity(2, 8));
        tracker.on_readdir(100, vec![10]);

        let cleanup_started = Arc::new(std::sync::Barrier::new(2));
        let cleanup_release = Arc::new(std::sync::Barrier::new(2));
        let started_for_hook = Arc::clone(&cleanup_started);
        let release_for_hook = Arc::clone(&cleanup_release);
        tracker.set_test_hook(Arc::new(move || {
            started_for_hook.wait();
            release_for_hook.wait();
        }));

        let eviction_tracker = Arc::clone(&tracker);
        let eviction = std::thread::spawn(move || {
            eviction_tracker.evict_dir(100);
        });
        cleanup_started.wait();

        tracker.on_readdir(200, vec![10]);
        let reassigned = tracker.child_to_dir.get(&10).map(|v| *v.value()).unwrap();
        cleanup_release.wait();

        eviction.join().unwrap();
        assert_eq!(
            tracker.child_to_dir.get(&10).map(|v| *v.value()),
            Some(reassigned),
            "directory eviction must not remove a mapping reassigned to another directory"
        );
    }

    #[test]
    fn test_stale_cleanup_preserves_fresh_mapping_during_concurrent_refresh() {
        let tracker = Arc::new(DirAccessTracker::new_with_capacity(2, 8));
        tracker.on_readdir(100, vec![10]);
        let stale = tracker.child_to_dir.get(&10).map(|v| *v.value()).unwrap();
        tracker.dirs.remove(&100);

        let cleanup_candidate = Arc::new(std::sync::Barrier::new(2));
        let cleanup_release = Arc::new(std::sync::Barrier::new(2));
        let candidate_for_hook = Arc::clone(&cleanup_candidate);
        let release_for_hook = Arc::clone(&cleanup_release);
        tracker.set_test_hook(Arc::new(move || {
            candidate_for_hook.wait();
            release_for_hook.wait();
        }));

        let cleanup_tracker = Arc::clone(&tracker);
        let cleanup = std::thread::spawn(move || {
            cleanup_tracker.on_child_read(10);
        });
        cleanup_candidate.wait();

        let refresh_started = Arc::new(std::sync::Barrier::new(2));
        let allow_refresh = Arc::new(std::sync::Barrier::new(2));
        let refresh_done = Arc::new(std::sync::Barrier::new(2));
        let refresh_tracker = Arc::clone(&tracker);
        let refresh_started_for_thread = Arc::clone(&refresh_started);
        let allow_refresh_for_thread = Arc::clone(&allow_refresh);
        let refresh_done_for_thread = Arc::clone(&refresh_done);
        let fresh = (stale.0, stale.1.saturating_add(1));
        let refresh = std::thread::spawn(move || {
            refresh_started_for_thread.wait();
            allow_refresh_for_thread.wait();
            refresh_tracker.child_to_dir.insert(10, fresh);
            refresh_done_for_thread.wait();
        });
        refresh_started.wait();

        let helper_holds_shard_lock = tracker.child_to_dir.try_get(&10).is_locked();
        if helper_holds_shard_lock {
            cleanup_release.wait();
            allow_refresh.wait();
            refresh_done.wait();
        } else {
            allow_refresh.wait();
            refresh_done.wait();
            cleanup_release.wait();
        }

        refresh.join().unwrap();
        cleanup.join().unwrap();
        assert_eq!(
            tracker.child_to_dir.get(&10).map(|v| *v.value()),
            Some(fresh),
            "concurrent refresh must survive stale cleanup"
        );
    }

    #[test]
    fn test_lru_cleanup_does_not_remove_recently_touched_directory() {
        let tracker = Arc::new(DirAccessTracker::new_with_capacity(2, 1));
        tracker.on_readdir(100, vec![10]);
        let original_mapping = tracker.child_to_dir.get(&10).map(|v| *v.value()).unwrap();

        let victim_selected = Arc::new(std::sync::Barrier::new(2));
        let eviction_release = Arc::new(std::sync::Barrier::new(2));
        let first_selection = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let selected_for_hook = Arc::clone(&victim_selected);
        let release_for_hook = Arc::clone(&eviction_release);
        let first_selection_for_hook = Arc::clone(&first_selection);
        tracker.set_test_hook(Arc::new(move || {
            if first_selection_for_hook.swap(false, std::sync::atomic::Ordering::AcqRel) {
                selected_for_hook.wait();
                release_for_hook.wait();
            }
        }));

        let eviction_tracker = Arc::clone(&tracker);
        let eviction = std::thread::spawn(move || {
            eviction_tracker.on_readdir(200, vec![20]);
        });
        victim_selected.wait();

        tracker.on_child_read(10);
        eviction_release.wait();
        eviction.join().unwrap();

        assert!(
            tracker.dirs.get(&100).is_some(),
            "an LRU snapshot must not remove a directory touched after selection"
        );
        assert_eq!(
            tracker.child_to_dir.get(&10).map(|v| *v.value()),
            Some(original_mapping),
            "a retained directory must keep its reverse mapping"
        );
    }

    #[test]
    fn test_batched_eviction_rechecks_capacity_after_concurrent_removal() {
        let mut tracker = DirAccessTracker::new_with_capacity(2, 4);
        for dir_ino in 100..104 {
            tracker.on_readdir(dir_ino, Vec::new());
        }
        tracker.max_tracked_dirs = 2;
        let tracker = Arc::new(tracker);

        let selection_started = Arc::new(std::sync::Barrier::new(2));
        let selection_release = Arc::new(std::sync::Barrier::new(2));
        let first_selection = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let started_for_hook = Arc::clone(&selection_started);
        let release_for_hook = Arc::clone(&selection_release);
        let first_selection_for_hook = Arc::clone(&first_selection);
        tracker.set_test_hook(Arc::new(move || {
            if first_selection_for_hook.swap(false, std::sync::atomic::Ordering::AcqRel) {
                started_for_hook.wait();
                release_for_hook.wait();
            }
        }));

        let eviction_tracker = Arc::clone(&tracker);
        let eviction = std::thread::spawn(move || {
            eviction_tracker.evict_lru_if_needed();
        });
        selection_started.wait();

        tracker.evict_dir(102);
        selection_release.wait();
        eviction.join().unwrap();

        assert_eq!(
            tracker.debug_tracked_dir_count(),
            2,
            "batched eviction must stop after concurrent removal restores capacity"
        );
    }

    #[test]
    fn test_oversubscribed_eviction_batches_lru_selection() {
        let first_dir = 100;
        let directory_count = 1_024;
        let mut tracker = DirAccessTracker::new_with_capacity(2, directory_count as usize);
        for offset in 0..directory_count {
            tracker.on_readdir(first_dir + offset, Vec::new());
        }
        tracker.max_tracked_dirs = 2;

        let selection_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let selection_count_for_hook = Arc::clone(&selection_count);
        tracker.set_test_hook(Arc::new(move || {
            selection_count_for_hook.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }));

        tracker.evict_lru_if_needed();

        assert_eq!(
            tracker.debug_tracked_dir_count(),
            2,
            "eviction must reduce the tracker to its configured capacity"
        );
        assert_eq!(
            selection_count.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "one oversubscribed eviction should select all victims in one map scan"
        );
        assert!(
            tracker
                .dirs
                .get(&(first_dir + directory_count - 2))
                .is_some()
        );
        assert!(
            tracker
                .dirs
                .get(&(first_dir + directory_count - 1))
                .is_some()
        );
    }

    #[test]
    fn test_batched_lru_eviction_cleans_all_reverse_mappings() {
        let mut tracker = DirAccessTracker::new_with_capacity(2, 4);
        for dir_ino in 100..104 {
            tracker.on_readdir(dir_ino, vec![dir_ino + 1_000]);
        }
        tracker.max_tracked_dirs = 2;

        tracker.evict_lru_if_needed();

        assert_eq!(tracker.debug_tracked_dir_count(), 2);
        for child_ino in 1_100..1_102 {
            assert!(
                tracker.child_to_dir.get(&child_ino).is_none(),
                "evicted directories must release every reverse mapping"
            );
        }
        for child_ino in 1_102..1_104 {
            assert!(
                tracker.child_to_dir.get(&child_ino).is_some(),
                "retained directories must keep their reverse mappings"
            );
        }
    }

    #[test]
    fn test_concurrent_registrations_respect_directory_capacity() {
        let tracker = Arc::new(DirAccessTracker::new_with_capacity(2, 2));
        let registration_count = 64;
        let start = Arc::new(std::sync::Barrier::new(registration_count + 1));
        let handles: Vec<_> = (0..registration_count)
            .map(|index| {
                let tracker = Arc::clone(&tracker);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    tracker.on_readdir(10_000 + index as Ino, Vec::new());
                })
            })
            .collect();

        start.wait();
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            tracker.debug_tracked_dir_count(),
            2,
            "concurrent registrations must leave exactly the configured capacity"
        );
    }

    #[test]
    fn test_frame_sequence_prefetches_next_numeric_siblings_first() {
        let tracker = DirAccessTracker::new_with_capacity(2, 8);
        tracker.on_readdir_entries(
            100,
            vec![
                DirAccessEntry::new(10, b"plate_0100.exr"),
                DirAccessEntry::new(11, b"plate_0101.exr"),
                DirAccessEntry::new(12, b"plate_0102.exr"),
                DirAccessEntry::new(99, b"notes.txt"),
            ],
        );

        let p = tracker.on_child_read_limited(10, 2);

        assert_eq!(
            p,
            vec![11, 12],
            "first frame read should prefer the tight numeric sequence window"
        );
    }

    #[test]
    fn test_frame_sequence_index_stores_long_runs_linearly() {
        let names: Vec<String> = (0..128)
            .map(|frame| format!("plate_{frame:04}.exr"))
            .collect();
        let entries: Vec<DirAccessEntry<'_>> = names
            .iter()
            .enumerate()
            .map(|(frame, name)| DirAccessEntry::new(10_000 + frame as Ino, name.as_bytes()))
            .collect();

        let index = build_sequence_index(&entries);
        let stored_successor_slots: usize =
            index.sequences.iter().map(|sequence| sequence.len()).sum();

        assert!(
            stored_successor_slots <= entries.len(),
            "sequence index should store each frame at most once"
        );
    }

    #[test]
    fn test_frame_sequence_requires_adjacent_frame_numbers() {
        let tracker = DirAccessTracker::new_with_capacity(2, 8);
        tracker.on_readdir_entries(
            100,
            vec![
                DirAccessEntry::new(10, b"plate_0100.exr"),
                DirAccessEntry::new(11, b"plate_0200.exr"),
                DirAccessEntry::new(12, b"plate_0201.exr"),
            ],
        );

        let first_gap = tracker.on_child_read_limited(10, 2);
        let second_run = tracker.on_child_read_limited(11, 2);

        assert!(
            first_gap.is_empty(),
            "a distant same-shape filename should not be treated as the next frame"
        );
        assert_eq!(
            second_run,
            vec![12],
            "adjacent frame numbers should still form a sequence"
        );
    }

    #[test]
    fn test_frame_sequence_rolls_window_forward_from_prefetched_member() {
        let tracker = DirAccessTracker::new_with_capacity(2, 8);
        tracker.on_readdir_entries(
            100,
            vec![
                DirAccessEntry::new(10, b"plate_0100.exr"),
                DirAccessEntry::new(11, b"plate_0101.exr"),
                DirAccessEntry::new(12, b"plate_0102.exr"),
                DirAccessEntry::new(13, b"plate_0103.exr"),
                DirAccessEntry::new(14, b"plate_0104.exr"),
            ],
        );

        let first = tracker.on_child_read_limited(10, 2);
        let second = tracker.on_child_read_limited(11, 2);
        let third = tracker.on_child_read_limited(12, 2);

        assert_eq!(first, vec![11, 12]);
        assert_eq!(
            second,
            vec![13, 14],
            "reading a frame that was already prefetched should keep the bounded window warm"
        );
        assert!(
            third.is_empty(),
            "no duplicate prefetch should be scheduled for already warm frames"
        );
    }

    #[test]
    fn test_frame_sequence_keeps_width_and_suffix_separate() {
        let tracker = DirAccessTracker::new_with_capacity(2, 8);
        tracker.on_readdir_entries(
            100,
            vec![
                DirAccessEntry::new(10, b"plate_0001.exr"),
                DirAccessEntry::new(11, b"plate_0002.exr"),
                DirAccessEntry::new(12, b"plate_0003.jpg"),
                DirAccessEntry::new(13, b"plate_003.exr"),
                DirAccessEntry::new(14, b"plate_0004.exr"),
            ],
        );

        let p = tracker.on_child_read_limited(10, 4);

        assert_eq!(
            p,
            vec![11],
            "sequence matching should require the same numeric width and suffix"
        );
    }

    #[test]
    fn test_frame_sequence_allows_empty_prefix_digit_only_names() {
        let tracker = DirAccessTracker::new_with_capacity(2, 8);
        tracker.on_readdir_entries(
            100,
            vec![
                DirAccessEntry::new(10, b"0001.exr"),
                DirAccessEntry::new(11, b"0002.exr"),
                DirAccessEntry::new(12, b"0003.exr"),
            ],
        );

        let p = tracker.on_child_read_limited(10, 2);

        assert_eq!(
            p,
            vec![11, 12],
            "digit-only frame names should be treated as a valid sequence"
        );
    }

    #[test]
    fn test_frame_sequence_readdir_refresh_replaces_stale_successors() {
        let tracker = DirAccessTracker::new_with_capacity(2, 8);
        tracker.on_readdir_entries(
            100,
            vec![
                DirAccessEntry::new(10, b"plate_0100.exr"),
                DirAccessEntry::new(11, b"plate_0101.exr"),
            ],
        );
        tracker.on_readdir_entries(
            100,
            vec![
                DirAccessEntry::new(10, b"plate_0100.exr"),
                DirAccessEntry::new(12, b"plate_0101.exr"),
            ],
        );

        let p = tracker.on_child_read_limited(10, 2);

        assert_eq!(
            p,
            vec![12],
            "fresh readdir sequence state should replace stale inode successors"
        );
        assert!(
            tracker.on_child_read_limited(11, 2).is_empty(),
            "stale child mapping should be removed during refresh"
        );
    }

    #[test]
    fn test_frame_sequence_read_does_not_arm_generic_sibling_prefetch() {
        let tracker = DirAccessTracker::new_with_capacity(2, 8);
        tracker.on_readdir_entries(
            100,
            vec![
                DirAccessEntry::new(10, b"plate_0100.exr"),
                DirAccessEntry::new(11, b"plate_0101.exr"),
                DirAccessEntry::new(12, b"plate_0102.exr"),
                DirAccessEntry::new(90, b"notes.txt"),
                DirAccessEntry::new(91, b"readme.txt"),
            ],
        );

        let sequence = tracker.on_child_read_limited(10, 1);
        let unrelated = tracker.on_child_read_limited(90, 4);

        assert_eq!(sequence, vec![11]);
        assert!(
            unrelated.is_empty(),
            "one sequence read plus one unrelated read should not trigger broad sibling prefetch"
        );
    }
}
