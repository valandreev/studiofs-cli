use lru::LruCache;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use studiofs_meta::schema::{
    ChunkId, ResolvedSliceSource, ResolvedSliceSpan, SliceId, SliceOrder, SliceRange,
};
use studiofs_meta::types::{Ino, PayloadRef};

pub(crate) const OS_CACHE_GENERATION_CAPACITY: usize = 8192;

/// Whether an open handle may participate in the operating-system page cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OsCacheParticipation {
    /// Bypass the operating-system page cache for this handle.
    Disabled,
    /// Allow this handle to use the operating-system page cache.
    Enabled,
}

/// Whether the kernel may retain and reuse pages from an earlier open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OsCacheDisposition {
    /// Retain and reuse pages cached for the same stable content generation.
    Reuse,
    /// Discard previously cached pages before serving this open.
    Replace,
}

/// Stable reason attached to an OS-cache open decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OsCacheReason {
    /// Page-cache participation was explicitly disabled for this handle.
    Disabled,
    /// The handle can modify file contents.
    WriteCapable,
    /// The VFS could not determine a safe cache state for the inode.
    UnknownState,
    /// The VFS could not resolve the supplied file handle.
    UnknownHandle,
    /// The inode is not a regular file.
    NonRegular,
    /// The inode has locally dirty or unpublished local content that is not safe to cache.
    LocalDirty,
    /// Another open handle can currently modify the inode.
    ActiveWriter,
    /// This is the first observed content generation for the inode.
    FirstObservation,
    /// The observed content generation differs from the retained generation.
    GenerationChanged,
    /// The observed content generation matches the retained generation.
    StableGeneration,
}

/// Generation decision returned to a platform bridge.
#[must_use = "the platform bridge must apply or explicitly reject this cache decision"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OsCacheOpenDecision {
    disposition: OsCacheDisposition,
    reason: OsCacheReason,
}

impl OsCacheOpenDecision {
    pub(crate) const fn reuse(reason: OsCacheReason) -> Self {
        Self {
            disposition: OsCacheDisposition::Reuse,
            reason,
        }
    }

    pub(crate) const fn replace(reason: OsCacheReason) -> Self {
        Self {
            disposition: OsCacheDisposition::Replace,
            reason,
        }
    }

    /// Return whether retained OS-cache pages may be reused.
    pub const fn disposition(self) -> OsCacheDisposition {
        self.disposition
    }

    /// Return the safety reason behind this decision.
    pub const fn reason(self) -> OsCacheReason {
        self.reason
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OsCacheSliceSignature {
    chunk_id: ChunkId,
    range: SliceRange,
    source: ResolvedSliceSource,
    slice_id: Option<SliceId>,
    slice_order: Option<SliceOrder>,
    payload: Option<PayloadRef>,
}

impl From<&ResolvedSliceSpan> for OsCacheSliceSignature {
    fn from(span: &ResolvedSliceSpan) -> Self {
        Self {
            chunk_id: span.chunk_id,
            range: span.range,
            source: span.source,
            slice_id: span.slice_id,
            slice_order: span.slice_order,
            payload: span.payload,
        }
    }
}

/// Content identity used to decide whether retained page-cache data is reusable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OsCacheGeneration {
    content_epoch: u64,
    logical_size: u64,
    spans: Vec<OsCacheSliceSignature>,
}

impl OsCacheGeneration {
    pub(crate) fn new(content_epoch: u64, logical_size: u64, spans: &[ResolvedSliceSpan]) -> Self {
        Self {
            content_epoch,
            logical_size,
            spans: spans.iter().map(OsCacheSliceSignature::from).collect(),
        }
    }
}

/// Bounded generation and writer state shared by platform cache policies.
///
/// A caller must serialize [`Self::prepare_read`] for one inode through
/// acceptance of the corresponding OS open reply. If that reply is rejected,
/// it must call [`Self::clear_generation`] before admitting the next open for
/// the inode.
pub(crate) struct OsCacheState {
    generations: LruCache<Ino, OsCacheGeneration>,
    active_writers: HashMap<Ino, usize>,
    active_foreground_handles: usize,
    idle_notify: Arc<tokio::sync::Notify>,
}

/// Exactly-once writer participation owned by one VFS file handle.
///
/// Final-handle release moves this lease through the local durability barrier,
/// so a concurrently opened reader cannot reuse stale pages while close is
/// still materializing local state.
pub(crate) struct OsCacheWriterLease {
    ino: Ino,
    state: Arc<Mutex<OsCacheState>>,
}

impl std::fmt::Debug for OsCacheWriterLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OsCacheWriterLease")
            .field("ino", &self.ino)
            .finish_non_exhaustive()
    }
}

impl OsCacheWriterLease {
    pub(crate) fn begin(ino: Ino, state: Arc<Mutex<OsCacheState>>) -> Self {
        state.lock().begin_writer(ino);
        Self { ino, state }
    }
}

impl Drop for OsCacheWriterLease {
    fn drop(&mut self) {
        let notify = {
            let mut state = self.state.lock();
            state.end_writer(self.ino);
            state.idle_notifier()
        };
        notify.notify_waiters();
    }
}

impl Default for OsCacheState {
    fn default() -> Self {
        Self::with_capacity(OS_CACHE_GENERATION_CAPACITY)
    }
}

impl OsCacheState {
    fn with_capacity(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity).expect("OS cache capacity must be non-zero");
        Self {
            generations: LruCache::new(capacity),
            active_writers: HashMap::new(),
            active_foreground_handles: 0,
            idle_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub(crate) fn prepare_read(
        &mut self,
        ino: Ino,
        participation: OsCacheParticipation,
        generation: Option<OsCacheGeneration>,
    ) -> OsCacheOpenDecision {
        if participation == OsCacheParticipation::Disabled {
            self.generations.pop(&ino);
            return OsCacheOpenDecision::replace(OsCacheReason::Disabled);
        }
        if self.active_writers.contains_key(&ino) {
            self.generations.pop(&ino);
            return OsCacheOpenDecision::replace(OsCacheReason::ActiveWriter);
        }
        let Some(generation) = generation else {
            self.generations.pop(&ino);
            return OsCacheOpenDecision::replace(OsCacheReason::UnknownState);
        };

        let reason = match self.generations.get(&ino) {
            Some(previous) if previous == &generation => {
                return OsCacheOpenDecision::reuse(OsCacheReason::StableGeneration);
            }
            Some(_) => OsCacheReason::GenerationChanged,
            None => OsCacheReason::FirstObservation,
        };
        self.generations.put(ino, generation);
        OsCacheOpenDecision::replace(reason)
    }

    pub(crate) fn begin_writer(&mut self, ino: Ino) {
        self.generations.pop(&ino);
        let writer_count = self.active_writers.entry(ino).or_default();
        *writer_count = writer_count.saturating_add(1);
    }

    pub(crate) fn has_active_writer(&self, ino: Ino) -> bool {
        self.active_writers.contains_key(&ino)
    }

    pub(crate) fn begin_foreground_handle(&mut self) {
        self.active_foreground_handles = self.active_foreground_handles.saturating_add(1);
    }

    pub(crate) fn end_foreground_handle(&mut self) {
        debug_assert!(
            self.active_foreground_handles > 0,
            "OS cache foreground handle count underflow"
        );
        self.active_foreground_handles = self.active_foreground_handles.saturating_sub(1);
    }

    pub(crate) fn active_foreground_handle_count(&self) -> usize {
        self.active_foreground_handles
    }

    pub(crate) fn idle_notifier(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.idle_notify)
    }

    #[cfg(test)]
    pub(crate) fn has_generation(&self, ino: Ino) -> bool {
        self.generations.contains(&ino)
    }

    pub(crate) fn end_writer(&mut self, ino: Ino) {
        let Some(writer_count) = self.active_writers.get_mut(&ino) else {
            debug_assert!(false, "OS cache writer count underflow for inode {ino}");
            return;
        };
        *writer_count -= 1;
        if *writer_count == 0 {
            self.active_writers.remove(&ino);
        }
    }

    /// Roll back an unaccepted open decision or retire an invalid generation.
    pub(crate) fn clear_generation(&mut self, ino: Ino) {
        self.generations.pop(&ino);
    }
}

/// Exactly-once foreground participation owned by one read-only VFS handle.
pub(crate) struct OsCacheForegroundHandleLease {
    state: Arc<Mutex<OsCacheState>>,
}

impl OsCacheForegroundHandleLease {
    pub(crate) fn begin(state: Arc<Mutex<OsCacheState>>) -> Self {
        state.lock().begin_foreground_handle();
        Self { state }
    }
}

impl std::fmt::Debug for OsCacheForegroundHandleLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OsCacheForegroundHandleLease")
            .finish_non_exhaustive()
    }
}

impl Drop for OsCacheForegroundHandleLease {
    fn drop(&mut self) {
        let notify = {
            let mut state = self.state.lock();
            state.end_foreground_handle();
            state.idle_notifier()
        };
        notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_meta::schema::{
        ChunkId, ResolvedSliceSource, ResolvedSliceSpan, SliceId, SliceOrder, SliceRange,
    };
    use studiofs_meta::types::{BlockFlags, PayloadRef};

    const INO: u64 = 41;

    fn span(source: ResolvedSliceSource, object_id: u64) -> ResolvedSliceSpan {
        ResolvedSliceSpan {
            chunk_id: ChunkId(3),
            range: SliceRange::new(17, 53).expect("valid range"),
            source,
            slice_id: Some(SliceId(7)),
            slice_order: Some(SliceOrder(11)),
            payload: Some(PayloadRef {
                object_id,
                offset: 5,
                length: 36,
                flags: BlockFlags::empty(),
            }),
        }
    }

    fn generation(
        content_epoch: u64,
        logical_size: u64,
        source: ResolvedSliceSource,
        object_id: u64,
    ) -> OsCacheGeneration {
        OsCacheGeneration::new(content_epoch, logical_size, &[span(source, object_id)])
    }

    #[test]
    fn first_observation_replaces_then_stable_generation_reuses() {
        let mut state = OsCacheState::with_capacity(8);
        let generation = generation(4, 4096, ResolvedSliceSource::Overlay, 101);

        assert_eq!(
            state.prepare_read(INO, OsCacheParticipation::Enabled, Some(generation.clone())),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
        assert_eq!(
            state.prepare_read(INO, OsCacheParticipation::Enabled, Some(generation)),
            OsCacheOpenDecision::reuse(OsCacheReason::StableGeneration)
        );
    }

    #[test]
    fn decision_exposes_read_only_disposition_and_reason() {
        let decision = OsCacheOpenDecision::replace(OsCacheReason::FirstObservation);

        assert_eq!(decision.disposition(), OsCacheDisposition::Replace);
        assert_eq!(decision.reason(), OsCacheReason::FirstObservation);
    }

    #[test]
    fn epoch_size_and_each_resolved_span_field_change_replace() {
        let baseline = generation(4, 4096, ResolvedSliceSource::Overlay, 101);
        let mut variants = vec![
            generation(5, 4096, ResolvedSliceSource::Overlay, 101),
            generation(4, 8192, ResolvedSliceSource::Overlay, 101),
            generation(4, 4096, ResolvedSliceSource::Overlay, 102),
        ];
        let mut changed_range = span(ResolvedSliceSource::Overlay, 101);
        changed_range.range = SliceRange::new(18, 53).expect("valid range");
        variants.push(OsCacheGeneration::new(4, 4096, &[changed_range]));
        let mut changed_chunk = span(ResolvedSliceSource::Overlay, 101);
        changed_chunk.chunk_id = ChunkId(4);
        variants.push(OsCacheGeneration::new(4, 4096, &[changed_chunk]));
        variants.push(generation(4, 4096, ResolvedSliceSource::Base, 101));
        let mut changed_source = span(ResolvedSliceSource::Hole, 101);
        changed_source.slice_id = None;
        changed_source.slice_order = None;
        changed_source.payload = None;
        variants.push(OsCacheGeneration::new(4, 4096, &[changed_source]));
        let mut changed_slice_id = span(ResolvedSliceSource::Overlay, 101);
        changed_slice_id.slice_id = Some(SliceId(8));
        variants.push(OsCacheGeneration::new(4, 4096, &[changed_slice_id]));
        let mut changed_slice_order = span(ResolvedSliceSource::Overlay, 101);
        changed_slice_order.slice_order = Some(SliceOrder(12));
        variants.push(OsCacheGeneration::new(4, 4096, &[changed_slice_order]));
        let mut changed_payload_offset = span(ResolvedSliceSource::Overlay, 101);
        changed_payload_offset
            .payload
            .as_mut()
            .expect("payload")
            .offset = 6;
        variants.push(OsCacheGeneration::new(4, 4096, &[changed_payload_offset]));
        let mut changed_payload_length = span(ResolvedSliceSource::Overlay, 101);
        changed_payload_length
            .payload
            .as_mut()
            .expect("payload")
            .length = 35;
        variants.push(OsCacheGeneration::new(4, 4096, &[changed_payload_length]));
        let mut changed_payload_flags = span(ResolvedSliceSource::Overlay, 101);
        changed_payload_flags
            .payload
            .as_mut()
            .expect("payload")
            .flags = BlockFlags::PARTIAL_BLOCK;
        variants.push(OsCacheGeneration::new(4, 4096, &[changed_payload_flags]));
        variants.push(OsCacheGeneration::new(
            4,
            4096,
            &[
                span(ResolvedSliceSource::Overlay, 101),
                span(ResolvedSliceSource::Overlay, 103),
            ],
        ));
        variants.push(OsCacheGeneration::new(
            4,
            4096,
            &[
                span(ResolvedSliceSource::Overlay, 103),
                span(ResolvedSliceSource::Overlay, 101),
            ],
        ));

        for changed in variants {
            let mut state = OsCacheState::with_capacity(8);
            let _ = state.prepare_read(INO, OsCacheParticipation::Enabled, Some(baseline.clone()));
            assert_eq!(
                state.prepare_read(INO, OsCacheParticipation::Enabled, Some(changed)),
                OsCacheOpenDecision::replace(OsCacheReason::GenerationChanged)
            );
        }
    }

    #[test]
    fn publication_only_transition_reuses() {
        let mut state = OsCacheState::with_capacity(8);
        // Publication state is intentionally not accepted by the generation
        // constructor. Identical resolved content before and after publication
        // therefore produces an identical generation.
        let _ = state.prepare_read(
            INO,
            OsCacheParticipation::Enabled,
            Some(generation(4, 4096, ResolvedSliceSource::Overlay, 101)),
        );

        assert_eq!(
            state.prepare_read(
                INO,
                OsCacheParticipation::Enabled,
                Some(generation(4, 4096, ResolvedSliceSource::Overlay, 101)),
            ),
            OsCacheOpenDecision::reuse(OsCacheReason::StableGeneration)
        );
    }

    #[test]
    fn disabled_and_unknown_state_evict_prior_snapshot() {
        let generation = generation(4, 4096, ResolvedSliceSource::Overlay, 101);
        for unsafe_observation in [
            (OsCacheParticipation::Disabled, Some(generation.clone())),
            (OsCacheParticipation::Enabled, None),
        ] {
            let mut state = OsCacheState::with_capacity(8);
            let _ =
                state.prepare_read(INO, OsCacheParticipation::Enabled, Some(generation.clone()));
            let _ = state.prepare_read(INO, unsafe_observation.0, unsafe_observation.1);

            assert_eq!(
                state.prepare_read(INO, OsCacheParticipation::Enabled, Some(generation.clone()),),
                OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
            );
        }
    }

    #[test]
    fn rejected_open_rollback_clears_generation() {
        let mut state = OsCacheState::with_capacity(8);
        let generation = generation(4, 4096, ResolvedSliceSource::Overlay, 101);
        let _ = state.prepare_read(INO, OsCacheParticipation::Enabled, Some(generation.clone()));

        state.clear_generation(INO);

        assert_eq!(
            state.prepare_read(INO, OsCacheParticipation::Enabled, Some(generation)),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
    }

    #[test]
    fn resolved_span_order_is_part_of_generation() {
        let mut state = OsCacheState::with_capacity(8);
        let first = span(ResolvedSliceSource::Overlay, 101);
        let mut second = span(ResolvedSliceSource::Overlay, 103);
        second.chunk_id = ChunkId(4);
        let _ = state.prepare_read(
            INO,
            OsCacheParticipation::Enabled,
            Some(OsCacheGeneration::new(
                4,
                4096,
                &[first.clone(), second.clone()],
            )),
        );

        assert_eq!(
            state.prepare_read(
                INO,
                OsCacheParticipation::Enabled,
                Some(OsCacheGeneration::new(4, 4096, &[second, first])),
            ),
            OsCacheOpenDecision::replace(OsCacheReason::GenerationChanged)
        );
    }

    #[test]
    fn active_writer_clears_snapshot_and_prevents_reuse() {
        let mut state = OsCacheState::with_capacity(8);
        let generation = generation(4, 4096, ResolvedSliceSource::Overlay, 101);
        let _ = state.prepare_read(INO, OsCacheParticipation::Enabled, Some(generation.clone()));

        state.begin_writer(INO);
        state.begin_writer(INO);
        assert_eq!(
            state.prepare_read(INO, OsCacheParticipation::Enabled, Some(generation.clone())),
            OsCacheOpenDecision::replace(OsCacheReason::ActiveWriter)
        );
        state.end_writer(INO);
        assert_eq!(
            state.prepare_read(INO, OsCacheParticipation::Enabled, Some(generation.clone())),
            OsCacheOpenDecision::replace(OsCacheReason::ActiveWriter)
        );
        state.end_writer(INO);
        assert_eq!(
            state.prepare_read(INO, OsCacheParticipation::Enabled, Some(generation)),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
    }

    #[test]
    fn least_recently_used_generation_is_evicted() {
        let mut state = OsCacheState::with_capacity(2);
        let first = generation(1, 1, ResolvedSliceSource::Overlay, 1);
        let second = generation(2, 2, ResolvedSliceSource::Overlay, 2);
        let third = generation(3, 3, ResolvedSliceSource::Overlay, 3);
        let _ = state.prepare_read(1, OsCacheParticipation::Enabled, Some(first.clone()));
        let _ = state.prepare_read(2, OsCacheParticipation::Enabled, Some(second.clone()));
        assert_eq!(
            state.prepare_read(1, OsCacheParticipation::Enabled, Some(first)),
            OsCacheOpenDecision::reuse(OsCacheReason::StableGeneration)
        );
        let _ = state.prepare_read(3, OsCacheParticipation::Enabled, Some(third));

        assert_eq!(
            state.prepare_read(2, OsCacheParticipation::Enabled, Some(second)),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
    }

    #[test]
    fn foreground_handle_lease_tracks_lifetime_exactly_once() {
        let state = Arc::new(Mutex::new(OsCacheState::with_capacity(8)));
        assert_eq!(state.lock().active_foreground_handle_count(), 0);

        let lease = OsCacheForegroundHandleLease::begin(Arc::clone(&state));
        assert_eq!(state.lock().active_foreground_handle_count(), 1);
        drop(lease);

        assert_eq!(state.lock().active_foreground_handle_count(), 0);
    }
}
