use super::{ResolvedSliceSource, StudioVfs, VfsResult};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use studiofs_cache::upload_queue::PendingUpload;
use studiofs_meta::types::Ino;

use crate::VfsMeta;
use crate::runtime::run_meta;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct ExtentRecordId {
    pub(super) object_id: u64,
    pub(super) content_epoch: u64,
}

#[derive(Debug, Clone)]
pub(super) enum PendingOverlayBlockSource {
    Pending(Bytes),
    Extent(u64),
}

#[derive(Debug, Clone, Default)]
pub(super) struct PendingUploadCatalog {
    by_block: HashMap<(Ino, u64), Vec<PendingUpload>>,
}

impl PendingUploadCatalog {
    pub(super) fn from_ops(ops: impl IntoIterator<Item = PendingUpload>) -> Self {
        let mut catalog = Self::default();
        for op in ops {
            catalog.insert(op);
        }
        catalog
    }

    fn insert(&mut self, op: PendingUpload) {
        self.by_block
            .entry((op.ino, op.offset))
            .or_default()
            .push(op);
    }

    pub(super) fn latest_extent_record(
        &self,
        ino: Ino,
        offset: u64,
        pending_content_epoch: Option<u64>,
    ) -> Option<ExtentRecordId> {
        self.by_block
            .get(&(ino, offset))
            .and_then(|ops| Self::latest_matching_op(ops, pending_content_epoch))
            .map(|op| ExtentRecordId {
                object_id: op.object_id,
                content_epoch: op.content_epoch,
            })
    }

    pub(super) fn live_extents(
        &self,
        block_size: u32,
        quarantined_inodes: &HashSet<Ino>,
    ) -> Vec<(Ino, u32, u64, u64)> {
        let mut live_extents = Vec::new();
        for (&(ino, offset), ops) in &self.by_block {
            if quarantined_inodes.contains(&ino) || offset % block_size as u64 != 0 {
                continue;
            }
            let Some(op) = Self::latest_matching_op(ops, None) else {
                continue;
            };
            let block_index = (offset / block_size as u64) as u32;
            live_extents.push((ino, block_index, op.object_id, op.content_epoch));
        }
        live_extents
    }

    fn latest_matching_op(
        ops: &[PendingUpload],
        pending_content_epoch: Option<u64>,
    ) -> Option<&PendingUpload> {
        ops.iter()
            .filter(|op| pending_content_epoch.is_none_or(|epoch| op.content_epoch == epoch))
            .max_by_key(|op| (op.content_epoch, op.created_unix_ms, op.object_id))
    }
}

impl StudioVfs {
    pub(super) async fn current_content_epoch_async(&self, ino: Ino) -> VfsResult<u64> {
        if let Some(attr) = self.state.namespace.dirty_attrs.lock().get(&ino) {
            return Ok(attr.content_epoch);
        }
        let meta = self.meta.clone();
        run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
            Ok(engine.get_attr(ino)?.content_epoch)
        })
        .await
    }

    pub(super) fn pending_content_epoch_for_inode(&self, ino: Ino) -> Option<u64> {
        self.state
            .incarnation
            .inode_incarnations
            .lock()
            .get(&ino)
            .and_then(|state| {
                state
                    .pending_incarnation
                    .as_ref()
                    .map(|pending| pending.content_epoch)
            })
    }

    pub(super) fn snapshot_pending_upload_index(&self) -> PendingUploadCatalog {
        PendingUploadCatalog::from_ops(self.pending_uploads_snapshot())
    }

    pub(super) fn pending_live_extent_record(
        &self,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
        pending_upload_index: Option<&PendingUploadCatalog>,
    ) -> Option<ExtentRecordId> {
        if let Some(extent) = self
            .state
            .incarnation
            .live_extent_records
            .lock()
            .get(&(ino, block_index))
            .copied()
        {
            if pending_content_epoch.is_none_or(|epoch| extent.content_epoch == epoch) {
                return Some(extent);
            }
        }

        let block_offset = (block_index as u64).saturating_mul(self.block_size as u64);
        let recovered =
            pending_upload_index?.latest_extent_record(ino, block_offset, pending_content_epoch)?;

        self.state
            .incarnation
            .live_extent_records
            .lock()
            .insert((ino, block_index), recovered);
        Some(recovered)
    }

    pub(super) fn pending_overlay_block_source(
        &self,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
        pending_upload_index: Option<&PendingUploadCatalog>,
    ) -> Option<PendingOverlayBlockSource> {
        // Stage 1 — local dirty state: newest local bytes not yet represented
        // by committed local-backend metadata.
        if let Some(pending_payload) = self
            .state
            .local_dirty
            .pending_payloads
            .lock()
            .get(&(ino, block_index))
        {
            if let Some(object_id) = pending_payload.local_extent_id() {
                return Some(PendingOverlayBlockSource::Extent(object_id));
            }
            return Some(PendingOverlayBlockSource::Pending(
                pending_payload.data.clone().freeze(),
            ));
        }
        if self.has_pending_dirty_slices_for_block(ino, block_index) {
            return None;
        }
        // Stage 2 — committed local cache state for the pending epoch.
        let extent = self.pending_live_extent_record(
            ino,
            block_index,
            pending_content_epoch,
            pending_upload_index,
        )?;
        if !self.pending_live_extent_matches_current_slice_graph(ino, block_index, extent) {
            return None;
        }
        Some(PendingOverlayBlockSource::Extent(extent.object_id))
    }

    fn pending_live_extent_matches_current_slice_graph(
        &self,
        ino: Ino,
        block_index: u32,
        extent: ExtentRecordId,
    ) -> bool {
        let visible_size = if let Some(attr) = self.state.namespace.dirty_attrs.lock().get(&ino) {
            attr.size
        } else {
            match self.sync_meta().get_attr(ino) {
                Ok(attr) => attr.size,
                Err(_) => return false,
            }
        };
        let logical_len = self.logical_block_len(visible_size, block_index);
        if logical_len == 0 {
            return false;
        }

        let block_start = u64::from(block_index).saturating_mul(u64::from(self.block_size));
        let Ok(spans) =
            self.sync_meta()
                .lookup_regular_file_slices(ino, block_start, logical_len as u64)
        else {
            return false;
        };
        if spans.len() != 1 {
            return false;
        }
        let span = &spans[0];
        if !matches!(
            span.source,
            ResolvedSliceSource::Overlay | ResolvedSliceSource::Base
        ) {
            return false;
        }
        let Some(block) = span.payload.as_ref() else {
            return false;
        };
        block.object_id == extent.object_id
            && block.offset == 0
            && block.length as usize == logical_len
    }

    pub(super) async fn read_pending_overlay_block_async(
        &self,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
        pending_upload_index: Option<&PendingUploadCatalog>,
    ) -> VfsResult<Option<Bytes>> {
        let Some(source) = self.pending_overlay_block_source(
            ino,
            block_index,
            pending_content_epoch,
            pending_upload_index,
        ) else {
            return Ok(None);
        };
        match source {
            PendingOverlayBlockSource::Pending(data) => Ok(Some(data)),
            PendingOverlayBlockSource::Extent(object_id) => {
                self.payload_store_get_async(object_id).await
            }
        }
    }

    pub(super) fn inode_has_local_overlay(&self, ino: Ino) -> bool {
        if self
            .state
            .local_dirty
            .pending_payloads
            .lock()
            .keys()
            .any(|(candidate, _)| *candidate == ino)
        {
            return true;
        }
        if self
            .state
            .incarnation
            .live_extent_records
            .lock()
            .keys()
            .any(|(candidate, _)| *candidate == ino)
        {
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{ExtentRecordId, PendingUploadCatalog};
    use studiofs_cache::upload_queue::PendingUpload;

    #[test]
    fn pending_upload_catalog_prefers_latest_pending_upload_for_block() {
        let ino = 41;
        let block_offset = 0;
        let pending_upload_index = PendingUploadCatalog::from_ops([
            PendingUpload {
                ino,
                offset: block_offset,
                object_id: 101,
                content_epoch: 3,
                created_unix_ms: 100,
                not_before_unix_ms: 0,
                retry_count: 0,
            },
            PendingUpload {
                ino,
                offset: block_offset,
                object_id: 202,
                content_epoch: 5,
                created_unix_ms: 200,
                not_before_unix_ms: 0,
                retry_count: 0,
            },
        ]);

        let recovered = pending_upload_index
            .latest_extent_record(ino, block_offset, None)
            .expect("recover pending live extent");

        assert_eq!(
            recovered,
            ExtentRecordId {
                object_id: 202,
                content_epoch: 5,
            }
        );
    }
}
