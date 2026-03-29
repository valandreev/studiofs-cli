use crate::readahead::ReadAheadPolicy;
use crate::vfs::OpenFlags;

const ACCESS_SCORE_MAX: i16 = 12;
const READ_SEQUENTIAL_PROMOTION_THRESHOLD: i16 = 4;
const READ_RANDOM_PROMOTION_THRESHOLD: i16 = 3;
const WRITE_SEQUENTIAL_PROMOTION_THRESHOLD: i16 = 3;
const LARGE_SEQUENTIAL_SPAN_BLOCKS: u32 = 4;
const LARGE_RANDOM_JUMP_BLOCKS: u32 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub(crate) enum AccessClass {
    SequentialMediaRead,
    #[default]
    RandomInteractiveRead,
    SequentialWriter,
    RandomWriter,
    BackgroundUploader,
}

impl AccessClass {
    pub(crate) const COUNT: usize = 5;

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::SequentialMediaRead => "sequential_media_read",
            Self::RandomInteractiveRead => "random_interactive_read",
            Self::SequentialWriter => "sequential_writer",
            Self::RandomWriter => "random_writer",
            Self::BackgroundUploader => "background_uploader",
        }
    }

    pub(crate) fn index(self) -> usize {
        match self {
            Self::SequentialMediaRead => 0,
            Self::RandomInteractiveRead => 1,
            Self::SequentialWriter => 2,
            Self::RandomWriter => 3,
            Self::BackgroundUploader => 4,
        }
    }

    pub(crate) fn from_index(index: usize) -> Option<Self> {
        match index {
            0 => Some(Self::SequentialMediaRead),
            1 => Some(Self::RandomInteractiveRead),
            2 => Some(Self::SequentialWriter),
            3 => Some(Self::RandomWriter),
            4 => Some(Self::BackgroundUploader),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccessClassifierConfig {
    pub(crate) block_size: u32,
    pub(crate) readahead_base_width: u32,
    pub(crate) readahead_max_width: u32,
    pub(crate) readahead_min_prefetch: u32,
}

impl AccessClassifierConfig {
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
            cache_promotion_on_read: false,
            readahead_base_width: 1,
            readahead_max_width: 1,
            readahead_min_prefetch: 1,
            upload_pressure_boost: 1,
        }
    }
}

impl AccessPolicy {
    pub(crate) fn from_class(class: AccessClass, cfg: AccessClassifierConfig) -> Self {
        let base_width = cfg.readahead_base_width.max(1);
        let max_width = cfg.readahead_max_width.max(base_width);
        let min_prefetch = cfg.readahead_min_prefetch.max(1).min(max_width);
        match class {
            AccessClass::SequentialMediaRead => Self {
                class,
                prefetch_enabled: true,
                cache_promotion_on_read: true,
                readahead_base_width: base_width,
                readahead_max_width: max_width,
                readahead_min_prefetch: min_prefetch,
                upload_pressure_boost: 2,
            },
            AccessClass::RandomInteractiveRead => Self {
                class,
                prefetch_enabled: false,
                cache_promotion_on_read: false,
                readahead_base_width: 1,
                readahead_max_width: base_width.clamp(1, 4),
                readahead_min_prefetch: 1,
                upload_pressure_boost: 1,
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
            AccessClass::BackgroundUploader => Self {
                class,
                prefetch_enabled: false,
                cache_promotion_on_read: false,
                readahead_base_width: 1,
                readahead_max_width: 1,
                readahead_min_prefetch: 1,
                upload_pressure_boost: 0,
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
        cfg: AccessClassifierConfig,
    ) -> AccessDecision {
        let previous_class = self.current_class;
        let (first_block, last_block, span_blocks) = block_span(offset, len, cfg.block_size_u64());
        self.prepare_for_operation(AccessOperation::Read);

        let mut next_class = self.current_class;
        let broad_scan = span_blocks >= LARGE_SEQUENTIAL_SPAN_BLOCKS;
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
            if broad_scan {
                self.bump_sequential(3);
                next_class = AccessClass::SequentialMediaRead;
            }
        } else if contiguous_forward || overlapping_window || broad_scan {
            let gain = if broad_scan {
                3
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
        cfg: AccessClassifierConfig,
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
    let span_len = len.max(1) as u64;
    let first_block = (offset / block_size).min(u32::MAX as u64) as u32;
    let last_offset = offset.saturating_add(span_len.saturating_sub(1));
    let last_block = (last_offset / block_size).min(u32::MAX as u64) as u32;
    let span_blocks = last_block.saturating_sub(first_block).saturating_add(1);
    (first_block, last_block, span_blocks)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> AccessClassifierConfig {
        AccessClassifierConfig {
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
        assert!(third.policy.cache_promotion_on_read);
    }

    #[test]
    fn read_classifier_demotes_after_large_seek() {
        let mut state = HandleAccessState::new(8, OpenFlags::RDONLY);
        let cfg = config();
        let _ = state.observe_read(0, 4096, cfg);
        let _ = state.observe_read(4096, 4096, cfg);
        let random = state.observe_read(64 * 1024, 1024, cfg);
        assert_eq!(random.class, AccessClass::RandomInteractiveRead);
        assert!(!random.policy.prefetch_enabled);
        assert!(!random.policy.cache_promotion_on_read);
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
        // sequential_score starts negative; by block 2 it reaches +2 which is
        // below WRITE_SEQUENTIAL_PROMOTION_THRESHOLD(3), leaving class as
        // RandomWriter and current_class = RandomWriter throughout.
        state.observe_write(0, 1024, cfg);
        state.observe_write(1024, 1024, cfg);
        state.observe_write(2048, 1024, cfg);
        assert_eq!(state.current_class(), AccessClass::RandomWriter);

        // Switch to read at the next contiguous position — block 3.
        // With the soft reset, last_last_block=Some(2) is preserved, so the
        // classifier detects contiguous-forward from the very first read.
        let r1 = state.observe_read(3072, 1024, cfg);
        // gain=2 → sequential_score=2, below threshold(4) → inherits RandomWriter.
        assert_eq!(r1.class, AccessClass::RandomWriter);

        // Second contiguous read at block 4: gain=2 → seq=4 ≥ threshold(4).
        let r2 = state.observe_read(4096, 1024, cfg);
        assert_eq!(
            r2.class,
            AccessClass::SequentialMediaRead,
            "soft reset: promotion after 2 contiguous reads (vs 3 with full reset)"
        );
    }
}
