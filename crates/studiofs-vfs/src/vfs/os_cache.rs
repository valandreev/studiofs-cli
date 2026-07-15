use lru::LruCache;
use parking_lot::Mutex;
use std::collections::{HashMap, hash_map::Entry};
use std::num::NonZeroUsize;
use studiofs_meta::schema::{ResolvedSliceSource, ResolvedSliceSpan};
use studiofs_meta::types::{Ino, InodeAttr};

const OS_CACHE_GENERATION_CAPACITY: usize = 8192;

/// Whether an adapter enables OS page-cache reuse for an open handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OsCacheParticipation {
    /// Track handle lifetime without allowing page-cache reuse.
    Disabled,
    /// Track handle lifetime and allow generation-aware page-cache reuse.
    Enabled,
}

/// Whether the adapter may reuse or must replace cached pages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OsCacheDisposition {
    /// Reuse cached pages for the observed generation.
    Reuse,
    /// Replace cached pages before serving the open handle.
    Replace,
}

/// Shared reason for an OS page-cache open decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OsCacheReason {
    PolicyDisabled,
    WriteCapable,
    UnknownHandle,
    UnknownLocalState,
    NotRegularFile,
    LocalDirtyState,
    ActiveWriter,
    FirstObservation,
    GenerationChanged,
    StableGeneration,
}

/// Consistent disposition and reason returned for an OS-cache open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OsCacheOpenDecision {
    disposition: OsCacheDisposition,
    reason: OsCacheReason,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "open preparation consumes constructors in Task 3")
)]
impl OsCacheOpenDecision {
    pub(super) const fn replace(reason: OsCacheReason) -> Self {
        Self {
            disposition: OsCacheDisposition::Replace,
            reason,
        }
    }

    pub(super) const fn reuse() -> Self {
        Self {
            disposition: OsCacheDisposition::Reuse,
            reason: OsCacheReason::StableGeneration,
        }
    }

    /// Return whether cached pages can be reused.
    pub const fn disposition(self) -> OsCacheDisposition {
        self.disposition
    }

    /// Return the fact that determined the disposition.
    pub const fn reason(self) -> OsCacheReason {
        self.reason
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OsCacheGeneration {
    content_epoch: u64,
    size: u64,
    graph: Vec<OsCacheGraphSpan>,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "open preparation observes generations in Task 3")
)]
impl OsCacheGeneration {
    fn from_metadata(attr: &InodeAttr, spans: Vec<ResolvedSliceSpan>) -> Self {
        Self {
            content_epoch: attr.content_epoch,
            size: attr.size,
            graph: spans.into_iter().map(OsCacheGraphSpan::from).collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OsCacheGraphSpan {
    chunk_id: u64,
    range_start: u32,
    range_end: u32,
    source: u8,
    slice_id: Option<u64>,
    slice_order: Option<u64>,
    payload: Option<OsCachePayload>,
}

impl From<ResolvedSliceSpan> for OsCacheGraphSpan {
    fn from(span: ResolvedSliceSpan) -> Self {
        Self {
            chunk_id: span.chunk_id.0,
            range_start: span.range.start,
            range_end: span.range.end,
            source: source_tag(span.source),
            slice_id: span.slice_id.map(|slice_id| slice_id.0),
            slice_order: span.slice_order.map(|slice_order| slice_order.0),
            payload: span.payload.map(|payload| OsCachePayload {
                object_id: payload.object_id,
                offset: payload.offset,
                length: payload.length,
                flags: payload.flags.bits(),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OsCachePayload {
    object_id: u64,
    offset: u32,
    length: u32,
    flags: u8,
}

fn source_tag(source: ResolvedSliceSource) -> u8 {
    match source {
        ResolvedSliceSource::Overlay => 1,
        ResolvedSliceSource::Base => 2,
        ResolvedSliceSource::OverlayTombstone => 3,
        ResolvedSliceSource::BaseTombstone => 4,
        ResolvedSliceSource::Hole => 5,
    }
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "open preparation consumes owned state in Task 3")
)]
pub(super) struct OsCacheState {
    generations: Mutex<LruCache<Ino, OsCacheGeneration>>,
    active_writers: Mutex<HashMap<Ino, usize>>,
}

impl Default for OsCacheState {
    fn default() -> Self {
        Self::with_capacity(OS_CACHE_GENERATION_CAPACITY)
    }
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "handle lifecycle consumes state methods in Task 3"
    )
)]
impl OsCacheState {
    fn with_capacity(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN);
        Self {
            generations: Mutex::new(LruCache::new(capacity)),
            active_writers: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn observe(
        &self,
        attr: &InodeAttr,
        spans: Vec<ResolvedSliceSpan>,
    ) -> OsCacheOpenDecision {
        let generation = OsCacheGeneration::from_metadata(attr, spans);
        let mut generations = self.generations.lock();
        let reason = match generations.get(&attr.ino) {
            Some(previous) if previous == &generation => return OsCacheOpenDecision::reuse(),
            Some(_) => OsCacheReason::GenerationChanged,
            None => OsCacheReason::FirstObservation,
        };
        generations.put(attr.ino, generation);
        OsCacheOpenDecision::replace(reason)
    }

    pub(super) fn clear(&self, ino: Ino) {
        self.generations.lock().pop(&ino);
    }

    pub(super) fn begin_writer(&self, ino: Ino) {
        *self.active_writers.lock().entry(ino).or_default() += 1;
    }

    /// Return true only when this call removes the inode's final writer.
    pub(super) fn end_writer(&self, ino: Ino) -> bool {
        match self.active_writers.lock().entry(ino) {
            Entry::Occupied(mut writer) if *writer.get() > 1 => {
                *writer.get_mut() -= 1;
                false
            }
            Entry::Occupied(writer) => {
                writer.remove();
                true
            }
            Entry::Vacant(_) => false,
        }
    }

    pub(super) fn has_active_writer(&self, ino: Ino) -> bool {
        self.active_writers.lock().contains_key(&ino)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_meta::schema::{
        BlockFlags, ChunkId, PayloadRef, ResolvedSliceSource, ResolvedSliceSpan, SliceId,
        SliceOrder, SliceRange,
    };
    use studiofs_meta::types::{InodeAttr, UploadState};

    fn attr() -> InodeAttr {
        let mut attr = InodeAttr::new_file(42, 1, 501, 20, 0o644);
        attr.size = 128;
        attr.content_epoch = 7;
        attr
    }

    fn spans() -> Vec<ResolvedSliceSpan> {
        vec![
            ResolvedSliceSpan {
                chunk_id: ChunkId(1),
                range: SliceRange::new(0, 64).unwrap(),
                source: ResolvedSliceSource::Overlay,
                slice_id: Some(SliceId(10)),
                slice_order: Some(SliceOrder(20)),
                payload: Some(PayloadRef {
                    object_id: 30,
                    offset: 4,
                    length: 64,
                    flags: BlockFlags::PARTIAL_BLOCK,
                }),
            },
            ResolvedSliceSpan {
                chunk_id: ChunkId(1),
                range: SliceRange::new(64, 128).unwrap(),
                source: ResolvedSliceSource::Hole,
                slice_id: None,
                slice_order: None,
                payload: None,
            },
        ]
    }

    fn assert_changed(mutator: impl FnOnce(&mut InodeAttr, &mut Vec<ResolvedSliceSpan>)) {
        let state = OsCacheState::with_capacity(8);
        let original_attr = attr();
        let original_spans = spans();
        assert_eq!(
            state.observe(&original_attr, original_spans),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
        let mut changed_attr = attr();
        let mut changed_spans = spans();
        mutator(&mut changed_attr, &mut changed_spans);
        assert_eq!(
            state.observe(&changed_attr, changed_spans),
            OsCacheOpenDecision::replace(OsCacheReason::GenerationChanged)
        );
    }

    #[test]
    fn first_observation_replaces_and_identical_second_observation_reuses() {
        let state = OsCacheState::with_capacity(8);
        let attr = attr();

        assert_eq!(
            state.observe(&attr, spans()),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
        assert_eq!(state.observe(&attr, spans()), OsCacheOpenDecision::reuse());
    }

    #[test]
    fn content_epoch_change_replaces() {
        assert_changed(|attr, _| attr.content_epoch += 1);
    }

    #[test]
    fn logical_size_change_replaces() {
        assert_changed(|attr, _| attr.size += 1);
    }

    #[test]
    fn span_order_change_replaces() {
        assert_changed(|_, spans| spans.swap(0, 1));
    }

    #[test]
    fn span_range_or_chunk_change_replaces() {
        assert_changed(|_, spans| spans[0].range = SliceRange::new(1, 64).unwrap());
        assert_changed(|_, spans| spans[0].chunk_id = ChunkId(2));
    }

    #[test]
    fn slice_identity_or_order_change_replaces() {
        assert_changed(|_, spans| spans[0].slice_id = Some(SliceId(11)));
        assert_changed(|_, spans| spans[0].slice_order = Some(SliceOrder(21)));
    }

    #[test]
    fn source_change_replaces() {
        assert_changed(|_, spans| spans[0].source = ResolvedSliceSource::Base);
    }

    #[test]
    fn payload_object_offset_length_or_flags_change_replaces() {
        assert_changed(|_, spans| spans[0].payload.as_mut().unwrap().object_id += 1);
        assert_changed(|_, spans| spans[0].payload.as_mut().unwrap().offset += 1);
        assert_changed(|_, spans| spans[0].payload.as_mut().unwrap().length -= 1);
        assert_changed(|_, spans| spans[0].payload.as_mut().unwrap().flags = BlockFlags::empty());
    }

    #[test]
    fn publication_only_transition_reuses() {
        let state = OsCacheState::with_capacity(8);
        let mut local = attr();
        local.committed_size = 0;
        local.committed_content_epoch = 6;
        local.upload_state = UploadState::Updating;
        assert_eq!(
            state.observe(&local, spans()),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );

        local.committed_size = local.size;
        local.committed_content_epoch = local.content_epoch;
        local.upload_state = UploadState::Ready;
        assert_eq!(state.observe(&local, spans()), OsCacheOpenDecision::reuse());
    }

    #[test]
    fn generation_store_evicts_to_its_fixed_capacity() {
        let state = OsCacheState::with_capacity(8);
        for ino in 1..=9 {
            let mut attr = attr();
            attr.ino = ino;
            assert_eq!(
                state.observe(&attr, spans()),
                OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
            );
        }
        let mut evicted = attr();
        evicted.ino = 1;
        assert_eq!(
            state.observe(&evicted, spans()),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
    }

    #[test]
    fn active_writers_are_counted_per_inode() {
        let state = OsCacheState::with_capacity(8);
        state.begin_writer(42);
        state.begin_writer(42);

        assert!(state.has_active_writer(42));
        assert!(!state.end_writer(42));
        assert!(state.end_writer(42));
        assert!(!state.has_active_writer(42));
    }

    #[test]
    fn clearing_generation_makes_next_observation_first() {
        let state = OsCacheState::with_capacity(8);
        let attr = attr();
        assert_eq!(
            state.observe(&attr, spans()),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );

        state.clear(attr.ino);

        assert_eq!(
            state.observe(&attr, spans()),
            OsCacheOpenDecision::replace(OsCacheReason::FirstObservation)
        );
    }

    #[test]
    fn decision_accessors_expose_a_consistent_pair() {
        let decision = OsCacheOpenDecision::reuse();

        assert_eq!(decision.disposition(), OsCacheDisposition::Reuse);
        assert_eq!(decision.reason(), OsCacheReason::StableGeneration);
    }
}
