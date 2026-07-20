use crate::readahead::ReadAheadPolicy;
use crate::vfs::OpenFlags;

const ACCESS_SCORE_MAX: i16 = 12;
const READ_SEQUENTIAL_PROMOTION_THRESHOLD: i16 = 4;
const READ_RANDOM_PROMOTION_THRESHOLD: i16 = 3;
const READ_IMMEDIATE_SEQUENTIAL_BONUS: i16 = 3;
const WRITE_SEQUENTIAL_PROMOTION_THRESHOLD: i16 = 3;
const DEFAULT_READAHEAD_BASE_WIDTH: u32 = 4;
const DEFAULT_READAHEAD_MAX_WIDTH: u32 = 32;
const DEFAULT_READAHEAD_MIN_PREFETCH: u32 = 4;
/// A read or write spanning ≥ 4 blocks is treated as a "broad scan" — it earns
/// a triple sequential score bonus and can immediately promote to
/// `SequentialMediaRead` / `SequentialWriter` without waiting for the normal
/// contiguous-run threshold.  The value matches the eager-prefetch look-ahead
/// width (4 × block_size) so a single large I/O is indistinguishable from the
/// first steps of a genuine sequential stream.
const LARGE_SEQUENTIAL_SPAN_BLOCKS: u32 = 4;
const LARGE_RANDOM_JUMP_BLOCKS: u32 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub(crate) enum AccessClass {
    SequentialMediaRead,
    #[default]
    RandomInteractiveRead,
    SequentialWriter,
    RandomWriter,
}

impl AccessClass {
    pub(crate) const COUNT: usize = 4;

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::SequentialMediaRead => "sequential_media_read",
            Self::RandomInteractiveRead => "random_interactive_read",
            Self::SequentialWriter => "sequential_writer",
            Self::RandomWriter => "random_writer",
        }
    }

    pub(crate) fn index(self) -> usize {
        match self {
            Self::SequentialMediaRead => 0,
            Self::RandomInteractiveRead => 1,
            Self::SequentialWriter => 2,
            Self::RandomWriter => 3,
        }
    }

    pub(crate) fn from_index(index: usize) -> Option<Self> {
        match index {
            0 => Some(Self::SequentialMediaRead),
            1 => Some(Self::RandomInteractiveRead),
            2 => Some(Self::SequentialWriter),
            3 => Some(Self::RandomWriter),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccessTrackerConfig {
    pub(crate) block_size: u32,
    pub(crate) readahead_base_width: u32,
    pub(crate) readahead_max_width: u32,
    pub(crate) readahead_min_prefetch: u32,
}

impl AccessTrackerConfig {
    fn block_size_u64(self) -> u64 {
        u64::from(self.block_size.max(1))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccessPolicy {
    pub(crate) class: AccessClass,
    pub(crate) prefetch_enabled: bool,
    pub(crate) cache_promotion_on_read: bool,
    pub(crate) readahead_base_width: u32,
    pub(crate) readahead_max_width: u32,
    pub(crate) readahead_min_prefetch: u32,
    pub(crate) upload_pressure_boost: u8,
}

impl Default for AccessPolicy {
    fn default() -> Self {
        Self {
            class: AccessClass::RandomInteractiveRead,
            prefetch_enabled: false,
            cache_promotion_on_read: true,
            readahead_base_width: DEFAULT_READAHEAD_BASE_WIDTH,
            readahead_max_width: DEFAULT_READAHEAD_MAX_WIDTH,
            readahead_min_prefetch: DEFAULT_READAHEAD_MIN_PREFETCH,
            upload_pressure_boost: 1,
        }
    }
}

impl AccessPolicy {
    pub(crate) fn from_class(class: AccessClass, cfg: AccessTrackerConfig) -> Self {
        let prefetch_enabled = cfg.readahead_base_width > 0;
        let base_width = cfg.readahead_base_width;
        let (max_width, min_prefetch) = if prefetch_enabled {
            let max_width = cfg.readahead_max_width.max(base_width);
            (max_width, cfg.readahead_min_prefetch.max(1).min(max_width))
        } else {
            (0, 0)
        };
        match class {
            AccessClass::SequentialMediaRead => Self {
                class,
                prefetch_enabled,
                cache_promotion_on_read: false,
                readahead_base_width: base_width,
                readahead_max_width: max_width,
                readahead_min_prefetch: min_prefetch,
                upload_pressure_boost: 3,
            },
            AccessClass::RandomInteractiveRead => Self {
                class,
                prefetch_enabled,
                cache_promotion_on_read: true,
                readahead_base_width: base_width,
                readahead_max_width: max_width,
                readahead_min_prefetch: min_prefetch,
                upload_pressure_boost: 2,
            },
            AccessClass::SequentialWriter => Self {
                class,
                prefetch_enabled: false,
                cache_promotion_on_read: true,
                readahead_base_width: 1,
                readahead_max_width: 1,
                readahead_min_prefetch: 1,
                upload_pressure_boost: 1,
            },
            AccessClass::RandomWriter => Self {
                class,
                prefetch_enabled: false,
                cache_promotion_on_read: false,
                readahead_base_width: 1,
                readahead_max_width: 1,
                readahead_min_prefetch: 1,
                upload_pressure_boost: 1,
            },
        }
    }

    pub(crate) fn readahead_policy(self) -> ReadAheadPolicy {
        ReadAheadPolicy {
            base_width: self.readahead_base_width,
            max_width: self.readahead_max_width,
            min_prefetch_on_seq: self.readahead_min_prefetch,
            prefetch_enabled: self.prefetch_enabled,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccessDecision {
    pub(crate) stream_id: u64,
    pub(crate) previous_class: AccessClass,
    pub(crate) class: AccessClass,
    pub(crate) policy: AccessPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccessOperation {
    Read,
    Write,
}

#[derive(Debug)]
pub(crate) struct HandleAccessState {
    stream_id: u64,
    flags: OpenFlags,
    current_class: AccessClass,
    last_op: Option<AccessOperation>,
    last_first_block: Option<u32>,
    last_last_block: Option<u32>,
    sequential_score: i16,
    random_score: i16,
}

impl HandleAccessState {
    pub(crate) fn new(stream_id: u64, flags: OpenFlags) -> Self {
        Self {
            stream_id,
            flags,
            current_class: initial_class(flags),
            last_op: None,
            last_first_block: None,
            last_last_block: None,
            sequential_score: 0,
            random_score: 0,
        }
    }

    pub(crate) fn stream_id(&self) -> u64 {
        self.stream_id
    }

    pub(crate) fn current_class(&self) -> AccessClass {
        self.current_class
    }

    pub(crate) fn observe_read(
        &mut self,
        offset: u64,
        len: usize,
        cfg: AccessTrackerConfig,
    ) -> AccessDecision {
        let previous_class = self.current_class;
        let (first_block, last_block, span_blocks) = block_span(offset, len, cfg.block_size_u64());
        let previous_op = self.last_op;
        let previous_first_block = self.last_first_block;
        self.prepare_for_operation(AccessOperation::Read);

        let restarting_after_write = previous_op == Some(AccessOperation::Write)
            && previous_first_block.is_some_and(|prev_first| first_block < prev_first);
        if restarting_after_write {
            // Only clear the block-position history when the first read jumps
            // backwards relative to the preceding write stream. Forward or
            // same-position read handoffs are intentional mixed-workload
            // behavior and still benefit from preserved contiguous progress.
            self.last_first_block = None;
            self.last_last_block = None;
        }

        let mut next_class = match self.current_class {
            AccessClass::SequentialWriter | AccessClass::RandomWriter => {
                AccessClass::RandomInteractiveRead
            }
            other => other,
        };
        let broad_scan = span_blocks >= LARGE_SEQUENTIAL_SPAN_BLOCKS;
        let recovering_from_random =
            next_class == AccessClass::RandomInteractiveRead && self.random_score > 0;
        let contiguous_forward = self
            .last_last_block
            .is_some_and(|prev_last| first_block <= prev_last.saturating_add(1))
            && self
                .last_first_block
                .is_some_and(|prev_first| first_block >= prev_first);
        let overlapping_window = self
            .last_last_block
            .is_some_and(|prev_last| first_block <= prev_last)
            && self
                .last_first_block
                .is_some_and(|prev_first| last_block >= prev_first);
        let large_jump = self
            .last_last_block
            .is_some_and(|prev_last| first_block.abs_diff(prev_last) >= LARGE_RANDOM_JUMP_BLOCKS);
        let backward_seek = self
            .last_first_block
            .is_some_and(|prev_first| first_block < prev_first);

        if self.last_first_block.is_none() {
            if broad_scan
                || (restarting_after_write
                    && first_block == 0
                    && previous_class == AccessClass::SequentialWriter)
            {
                self.bump_sequential(READ_IMMEDIATE_SEQUENTIAL_BONUS);
                next_class = AccessClass::SequentialMediaRead;
            }
        } else if broad_scan {
            self.bump_sequential(READ_SEQUENTIAL_PROMOTION_THRESHOLD);
            self.random_score = self
                .random_score
                .saturating_sub(READ_RANDOM_PROMOTION_THRESHOLD);
            next_class = AccessClass::SequentialMediaRead;
        } else if contiguous_forward || overlapping_window {
            if recovering_from_random {
                self.sequential_score = self.sequential_score.max(0);
            }
            let gain = if recovering_from_random {
                if contiguous_forward {
                    READ_SEQUENTIAL_PROMOTION_THRESHOLD
                } else {
                    2
                }
            } else if contiguous_forward {
                2
            } else {
                1
            };
            self.bump_sequential(gain);
            self.random_score = self.random_score.saturating_sub(1);
            if self.sequential_score >= READ_SEQUENTIAL_PROMOTION_THRESHOLD {
                next_class = AccessClass::SequentialMediaRead;
            }
        } else {
            let penalty = if backward_seek || large_jump { 2 } else { 1 };
            self.bump_random(penalty);
            self.sequential_score = self.sequential_score.saturating_sub(penalty + 1);
            if self.random_score >= READ_RANDOM_PROMOTION_THRESHOLD || backward_seek || large_jump {
                next_class = AccessClass::RandomInteractiveRead;
            }
        }

        self.last_first_block = Some(first_block);
        self.last_last_block = Some(last_block);
        self.current_class = next_class;

        AccessDecision {
            stream_id: self.stream_id,
            previous_class,
            class: next_class,
            policy: AccessPolicy::from_class(next_class, cfg),
        }
    }

    pub(crate) fn observe_write(
        &mut self,
        offset: u64,
        len: usize,
        cfg: AccessTrackerConfig,
    ) -> AccessDecision {
        let previous_class = self.current_class;
        let (first_block, last_block, span_blocks) = block_span(offset, len, cfg.block_size_u64());
        self.prepare_for_operation(AccessOperation::Write);

        let append_like = self.flags.contains(OpenFlags::APPEND)
            || self
                .last_last_block
                .is_some_and(|prev_last| first_block <= prev_last.saturating_add(1))
            || span_blocks >= LARGE_SEQUENTIAL_SPAN_BLOCKS;

        let next_class = if append_like {
            self.bump_sequential(if span_blocks >= LARGE_SEQUENTIAL_SPAN_BLOCKS {
                3
            } else {
                2
            });
            self.random_score = self.random_score.saturating_sub(1);
            if self.sequential_score >= WRITE_SEQUENTIAL_PROMOTION_THRESHOLD {
                AccessClass::SequentialWriter
            } else {
                previous_class
            }
        } else {
            self.bump_random(2);
            self.sequential_score = self.sequential_score.saturating_sub(2);
            AccessClass::RandomWriter
        };

        self.last_first_block = Some(first_block);
        self.last_last_block = Some(last_block);
        self.current_class = next_class;

        AccessDecision {
            stream_id: self.stream_id,
            previous_class,
            class: next_class,
            policy: AccessPolicy::from_class(next_class, cfg),
        }
    }

    fn prepare_for_operation(&mut self, next_op: AccessOperation) {
        if self.last_op != Some(next_op) {
            // Soft reset: clear scores but preserve block position so that the
            // first observation after a Read↔Write switch can still detect
            // contiguous-forward progress instead of starting from scratch.
            // This matters for mixed workloads (e.g. append-then-read logs)
            // that would otherwise never reach the sequential promotion
            // threshold.
            self.sequential_score = 0;
            self.random_score = 0;
            self.last_op = Some(next_op);
        }
    }

    fn bump_sequential(&mut self, delta: i16) {
        self.sequential_score = (self.sequential_score + delta).clamp(0, ACCESS_SCORE_MAX);
    }

    fn bump_random(&mut self, delta: i16) {
        self.random_score = (self.random_score + delta).clamp(0, ACCESS_SCORE_MAX);
    }
}

fn initial_class(flags: OpenFlags) -> AccessClass {
    if flags.contains(OpenFlags::APPEND) {
        AccessClass::SequentialWriter
    } else if flags.contains(OpenFlags::WRONLY) {
        AccessClass::RandomWriter
    } else {
        AccessClass::RandomInteractiveRead
    }
}

fn block_span(offset: u64, len: usize, block_size: u64) -> (u32, u32, u32) {
    let block_size = block_size.max(1);
    let span_len = u64::try_from(len).unwrap_or(u64::MAX).max(1);
    let first_block = clamp_u32(offset / block_size);
    let last_offset = offset.saturating_add(span_len.saturating_sub(1));
    let last_block = clamp_u32(last_offset / block_size);
    let span_blocks = last_block.saturating_sub(first_block).saturating_add(1);
    (first_block, last_block, span_blocks)
}

fn clamp_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> AccessTrackerConfig {
        AccessTrackerConfig {
            block_size: 1024,
            readahead_base_width: 32,
            readahead_max_width: 256,
            readahead_min_prefetch: 8,
        }
    }

    #[test]
    fn read_classifier_promotes_sequential_media_after_contiguous_progress() {
        let mut state = HandleAccessState::new(7, OpenFlags::RDONLY);
        let cfg = config();
        let first = state.observe_read(0, 1024, cfg);
        let second = state.observe_read(1024, 1024, cfg);
        let third = state.observe_read(2048, 4096, cfg);
        assert_eq!(first.class, AccessClass::RandomInteractiveRead);
        assert_eq!(second.class, AccessClass::RandomInteractiveRead);
        assert_eq!(third.class, AccessClass::SequentialMediaRead);
        assert!(third.policy.prefetch_enabled);
        assert!(!third.policy.cache_promotion_on_read);
    }

    #[test]
    fn zero_readahead_width_disables_prefetch_for_read_classes() {
        let disabled = AccessTrackerConfig {
            readahead_base_width: 0,
            readahead_max_width: 0,
            ..config()
        };

        for class in [
            AccessClass::SequentialMediaRead,
            AccessClass::RandomInteractiveRead,
        ] {
            let policy = AccessPolicy::from_class(class, disabled);
            assert!(!policy.prefetch_enabled);
            assert_eq!(policy.readahead_base_width, 0);
            assert_eq!(policy.readahead_max_width, 0);
        }
    }

    #[test]
    fn read_classifier_demotes_after_large_seek() {
        let mut state = HandleAccessState::new(8, OpenFlags::RDONLY);
        let cfg = config();
        let _ = state.observe_read(0, 4096, cfg);
        let _ = state.observe_read(4096, 4096, cfg);
        let random = state.observe_read(64 * 1024, 1024, cfg);
        assert_eq!(random.class, AccessClass::RandomInteractiveRead);
        assert!(random.policy.prefetch_enabled);
        assert!(random.policy.cache_promotion_on_read);
        assert!(random.policy.readahead_max_width > random.policy.readahead_base_width);
    }

    #[test]
    fn write_classifier_detects_append_like_streams() {
        let mut state = HandleAccessState::new(9, OpenFlags::RDWR | OpenFlags::APPEND);
        let cfg = config();
        let first = state.observe_write(0, 1024, cfg);
        let second = state.observe_write(1024, 1024, cfg);
        assert_eq!(first.class, AccessClass::SequentialWriter);
        assert_eq!(second.class, AccessClass::SequentialWriter);
        assert!(second.policy.cache_promotion_on_read);
    }

    #[test]
    fn mixed_workload_preserves_block_position_across_operation_switch() {
        let mut state = HandleAccessState::new(10, OpenFlags::RDWR);
        let cfg = config();
        // Write blocks 0, 1, 2. The first write is non-append (random), so
        // sequential_score is still only +2 by block 2, which is
        // below WRITE_SEQUENTIAL_PROMOTION_THRESHOLD(3), leaving class as
        // RandomWriter and current_class = RandomWriter throughout.
        state.observe_write(0, 1024, cfg);
        state.observe_write(1024, 1024, cfg);
        state.observe_write(2048, 1024, cfg);
        assert_eq!(state.current_class(), AccessClass::RandomWriter);

        // Switch to read at the next contiguous position — block 3.
        // With the soft reset, last_last_block=Some(2) is preserved, so the
        // tracking detects contiguous-forward from the very first read.
        let r1 = state.observe_read(3072, 1024, cfg);
        // gain=2 → sequential_score=2, below threshold(4) → stays on the
        // read-side random class instead of inheriting the old writer label.
        assert_eq!(r1.class, AccessClass::RandomInteractiveRead);

        // Second contiguous read at block 4: gain=2 → seq=4 ≥ threshold(4).
        let r2 = state.observe_read(4096, 1024, cfg);
        assert_eq!(
            r2.class,
            AccessClass::SequentialMediaRead,
            "soft reset: promotion after 2 contiguous reads (vs 3 with full reset)"
        );
    }

    #[test]
    fn read_classifier_recovers_quickly_after_random_seek() {
        let mut state = HandleAccessState::new(11, OpenFlags::RDONLY);
        let cfg = config();

        let _ = state.observe_read(64 * 1024, 1024, cfg);
        let r1 = state.observe_read(0, 1024, cfg);
        let r2 = state.observe_read(1024, 1024, cfg);

        assert_eq!(r1.class, AccessClass::RandomInteractiveRead);
        assert_eq!(
            r2.class,
            AccessClass::SequentialMediaRead,
            "two contiguous reads after a restart should recover out of random mode"
        );
        assert!(r2.policy.prefetch_enabled);
    }

    #[test]
    fn write_then_read_from_start_avoids_sticking_in_random_read() {
        let mut state = HandleAccessState::new(12, OpenFlags::RDWR | OpenFlags::APPEND);
        let cfg = config();

        let _ = state.observe_write(0, 1024, cfg);
        let _ = state.observe_write(1024, 1024, cfg);
        let _ = state.observe_write(2048, 1024, cfg);
        assert_eq!(state.current_class(), AccessClass::SequentialWriter);

        let r1 = state.observe_read(0, 1024, cfg);
        let r2 = state.observe_read(1024, 1024, cfg);

        assert_ne!(
            r1.class,
            AccessClass::RandomInteractiveRead,
            "the first read after a sequential writer restarts from offset 0 should not be treated as a random seek"
        );
        assert_eq!(
            r2.class,
            AccessClass::SequentialMediaRead,
            "a restarted sequential read should recover by the second contiguous read"
        );
    }

    #[test]
    fn tail_read_after_write_preserves_contiguous_progress() {
        let mut state = HandleAccessState::new(13, OpenFlags::RDWR);
        let cfg = config();

        state.observe_write(0, 1024, cfg);
        state.observe_write(1024, 1024, cfg);
        state.observe_write(2048, 1024, cfg);

        let r1 = state.observe_read(2048, 1024, cfg);
        let r2 = state.observe_read(3072, 1024, cfg);

        assert_eq!(r1.class, AccessClass::RandomInteractiveRead);
        assert_eq!(
            r2.class,
            AccessClass::SequentialMediaRead,
            "tail reads after a write stream should keep contiguous progress instead of hard-resetting"
        );
    }

    #[test]
    fn overlap_after_random_seek_does_not_instantly_promote_to_sequential() {
        let mut state = HandleAccessState::new(14, OpenFlags::RDONLY);
        let cfg = config();

        let _ = state.observe_read(64 * 1024, 1024, cfg);
        let _ = state.observe_read(2048, 1024, cfg);
        let overlap = state.observe_read(1536, 1024, cfg);
        let contiguous = state.observe_read(3072, 1024, cfg);

        assert_eq!(
            overlap.class,
            AccessClass::RandomInteractiveRead,
            "an overlapping retry alone should not recover all the way out of random mode"
        );
        assert_eq!(contiguous.class, AccessClass::SequentialMediaRead);
    }

    #[test]
    fn read_policies_keep_the_configured_prefetch_window() {
        let cfg = config();
        let sequential = AccessPolicy::from_class(AccessClass::SequentialMediaRead, cfg);
        let random = AccessPolicy::from_class(AccessClass::RandomInteractiveRead, cfg);

        assert!(sequential.prefetch_enabled);
        assert!(random.prefetch_enabled);
        assert_eq!(sequential.readahead_base_width, cfg.readahead_base_width);
        assert_eq!(sequential.readahead_max_width, cfg.readahead_max_width);
        assert_eq!(
            sequential.readahead_min_prefetch,
            cfg.readahead_min_prefetch
        );
        assert_eq!(random.readahead_base_width, cfg.readahead_base_width);
        assert_eq!(random.readahead_max_width, cfg.readahead_max_width);
        assert_eq!(random.readahead_min_prefetch, cfg.readahead_min_prefetch);
        assert!(!sequential.cache_promotion_on_read);
        assert!(random.cache_promotion_on_read);
    }

    #[test]
    fn read_classifier_promotes_on_sub_block_forward_progress() {
        let mut state = HandleAccessState::new(15, OpenFlags::RDONLY);
        let cfg = AccessTrackerConfig {
            block_size: 4096,
            readahead_base_width: 32,
            readahead_max_width: 256,
            readahead_min_prefetch: 8,
        };

        let first = state.observe_read(0, 1024, cfg);
        let second = state.observe_read(1024, 1024, cfg);
        let third = state.observe_read(2048, 1024, cfg);

        assert_eq!(first.class, AccessClass::RandomInteractiveRead);
        assert_eq!(second.class, AccessClass::RandomInteractiveRead);
        assert_eq!(
            third.class,
            AccessClass::SequentialMediaRead,
            "steady forward progress inside a single cache block should still promote to sequential media read"
        );
    }

    #[test]
    fn read_classifier_recovers_when_overlap_keeps_extending_frontier() {
        let mut state = HandleAccessState::new(16, OpenFlags::RDONLY);
        let cfg = config();

        let first = state.observe_read(0, 3 * 1024, cfg);
        let second = state.observe_read(1024, 3 * 1024, cfg);
        let third = state.observe_read(0, 5 * 1024, cfg);

        assert_eq!(first.class, AccessClass::RandomInteractiveRead);
        assert_eq!(second.class, AccessClass::RandomInteractiveRead);
        assert_eq!(
            third.class,
            AccessClass::SequentialMediaRead,
            "overlapping retries that keep pushing the read frontier forward should still recover into sequential mode"
        );
    }
}
