use super::PgMetaEngine;
use crate::engine::{
    DeferredDeleteRecord, LocalStorageCompactionResult, MetaAcl, MetaEngine, MetaError, MetaFormat,
    MetaFsOps, MetaGc, MetaInodes, MetaNamespace, MetaPayloadObjects, MetaRecovery,
    MetaReplication, MetaResult, MetaSnapshots, MetaSymlink, MetaXattr, PendingCreateRecord,
    UndoLogKind, UndoLogRecord,
};
use crate::types::*;
use std::collections::HashSet;

impl MetaFormat for PgMetaEngine {
    fn set_format_info(&self, info: &FormatInfo) -> MetaResult<()> {
        self.local.set_format_info(info)
    }

    fn get_format_info(&self) -> MetaResult<FormatInfo> {
        self.local.get_format_info()
    }
}

impl MetaInodes for PgMetaEngine {
    fn get_counter(&self, name: &str) -> MetaResult<u64> {
        self.local.get_counter(name)
    }

    fn incr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        self.mutate(|local| local.incr_counter(name, delta))
    }

    fn decr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        self.mutate(|local| local.decr_counter(name, delta))
    }

    fn set_counter(&self, name: &str, value: u64) -> MetaResult<()> {
        self.mutate(|local| local.set_counter(name, value))
    }

    fn next_inode(&self) -> MetaResult<Ino> {
        self.allocate_inode_object_ids(1)?
            .into_iter()
            .next()
            .ok_or_else(|| MetaError::Storage("missing allocated inode".to_string()))
    }

    fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr> {
        self.local.get_attr(ino)
    }

    fn get_inode_lifecycle(&self, ino: Ino) -> MetaResult<Option<InodeLifecycleRecord>> {
        self.local.get_inode_lifecycle(ino)
    }

    fn list_inode_lifecycles(&self) -> MetaResult<Vec<InodeLifecycleRecord>> {
        self.local.list_inode_lifecycles()
    }

    fn get_attrs_batch(&self, inos: &[Ino]) -> MetaResult<Vec<InodeAttr>> {
        self.local.get_attrs_batch(inos)
    }

    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()> {
        self.mutate(|local| local.set_attr(attr))
    }

    fn remove_attr(&self, ino: Ino) -> MetaResult<()> {
        self.mutate(|local| local.remove_attr(ino))
    }

    fn list_inode_attrs(&self) -> MetaResult<Vec<InodeAttr>> {
        self.local.list_inode_attrs()
    }
}

impl MetaNamespace for PgMetaEngine {
    fn lookup(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        self.local.lookup(parent, name)
    }

    fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>> {
        self.local.readdir(parent)
    }

    fn add_entry(&self, parent: Ino, name: &[u8], entry: &DirEntryValue) -> MetaResult<()> {
        self.mutate(|local| local.add_entry(parent, name, entry))
    }

    fn remove_entry(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        self.mutate(|local| local.remove_entry(parent, name))
    }
}

impl MetaFsOps for PgMetaEngine {
    fn create_file(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let ino = self.next_inode()?;
        self.mutate(|local| local.create_file_with_ino(ino, parent, name, uid, gid, mode))
    }

    fn create_file_with_ino(
        &self,
        ino: Ino,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.create_file_with_ino(ino, parent, name, uid, gid, mode))
    }

    fn create_dir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let ino = self.next_inode()?;
        self.mutate(|local| local.create_dir_with_ino(ino, parent, name, uid, gid, mode))
    }

    fn create_dir_with_ino(
        &self,
        ino: Ino,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.create_dir_with_ino(ino, parent, name, uid, gid, mode))
    }

    fn unlink(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.unlink(parent, name))
    }

    fn unlink_open_handle(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.unlink_open_handle(parent, name))
    }

    fn rmdir(&self, parent: Ino, name: &[u8]) -> MetaResult<()> {
        self.mutate(|local| local.rmdir(parent, name))
    }

    fn rename_atomic(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> MetaResult<RenameAtomicResult> {
        self.mutate(|local| local.rename_atomic(parent, name, new_parent, new_name))
    }

    fn rename(&self, parent: Ino, name: &[u8], new_parent: Ino, new_name: &[u8]) -> MetaResult<()> {
        self.mutate(|local| local.rename(parent, name, new_parent, new_name))
    }

    fn link_atomic(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.link_atomic(ino, new_parent, new_name))
    }

    fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.link(ino, new_parent, new_name))
    }

    fn create_symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> MetaResult<InodeAttr> {
        let ino = self.next_inode()?;
        self.mutate(|local| local.create_symlink_with_ino(ino, parent, name, target, uid, gid))
    }
}

impl MetaPayloadObjects for PgMetaEngine {
    fn lookup_regular_file_chunk_slices(
        &self,
        ino: Ino,
        chunk_id: ChunkId,
        chunk_range: SliceRange,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        self.local
            .lookup_regular_file_chunk_slices(ino, chunk_id, chunk_range)
    }

    fn lookup_regular_file_slices(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        self.local.lookup_regular_file_slices(ino, offset, length)
    }

    fn remove_regular_file_graph(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        self.mutate(|local| local.remove_regular_file_graph(ino))
    }

    fn next_object_id(&self) -> MetaResult<u64> {
        self.next_object_ids(1)?
            .into_iter()
            .next()
            .ok_or_else(|| MetaError::Storage("missing allocated object ID".to_string()))
    }

    fn next_object_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        self.allocate_inode_object_ids(count)
    }

    fn flush_regular_file_overlay_slices(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<crate::schema::SliceRecord>)],
    ) -> MetaResult<()> {
        self.mutate(|local| local.flush_regular_file_overlay_slices(ino, chunk_slices))
    }

    fn has_local_regular_file_overlay(&self, ino: Ino) -> MetaResult<bool> {
        self.local.has_local_regular_file_overlay(ino)
    }

    fn get_regular_file_overlay_object_ids(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        self.local.get_regular_file_overlay_object_ids(ino)
    }

    fn get_regular_file_graph_object_usage(&self, ino: Ino) -> MetaResult<Vec<(u64, u64)>> {
        self.local.get_regular_file_graph_object_usage(ino)
    }

    fn has_regular_file_overlay_object_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> MetaResult<bool> {
        self.local
            .has_regular_file_overlay_object_if_matches(ino, expected_epoch, object_id)
    }

    fn mark_regular_file_overlay_object_uploaded_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> MetaResult<bool> {
        self.mark_regular_file_overlay_objects_uploaded_if_match(ino, expected_epoch, &[object_id])
            .map(|marked| marked.contains(&object_id))
    }

    fn mark_regular_file_overlay_objects_uploaded_if_match(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_ids: &[u64],
    ) -> MetaResult<Vec<u64>> {
        let result = self.mutate(|local| {
            local.mark_regular_file_overlay_objects_uploaded_if_match(
                ino,
                expected_epoch,
                object_ids,
            )
        });
        if matches!(result, Ok(ref marked) if !marked.is_empty()) {
            self.request_pending_flush();
        }
        result
    }

    fn publish_regular_file_overlay_to_base_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
    ) -> MetaResult<bool> {
        let result = self.mutate(|local| {
            local.publish_regular_file_overlay_to_base_if_matches(ino, expected_epoch)
        });
        if matches!(result, Ok(true)) {
            self.request_eager_flush();
        }
        result
    }

    fn compact_fragmented_regular_file_chunks(&self, max_chunks: usize) -> MetaResult<usize> {
        let result = self.mutate(|local| local.compact_fragmented_regular_file_chunks(max_chunks));
        if matches!(result, Ok(count) if count > 0) {
            self.request_pending_flush();
        }
        result
    }
}

impl MetaGc for PgMetaEngine {
    fn compact_local_storage(&self) -> MetaResult<LocalStorageCompactionResult> {
        self.local.compact_local_storage()
    }

    fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> MetaResult<()> {
        self.mutate(|local| local.add_pending_deletion(object_id, timestamp))
    }

    fn remove_pending_deletion(&self, object_id: u64) -> MetaResult<()> {
        self.mutate(|local| local.remove_pending_deletion(object_id))
    }

    fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>> {
        self.local.get_pending_deletions()
    }

    fn get_referenced_object_ids(&self) -> MetaResult<HashSet<u64>> {
        self.local.get_referenced_object_ids()
    }
}

impl MetaRecovery for PgMetaEngine {
    fn add_deferred_delete(&self, record: &DeferredDeleteRecord) -> MetaResult<()> {
        self.mutate(|local| local.add_deferred_delete(record))
    }

    fn remove_deferred_delete(&self, ino: Ino) -> MetaResult<()> {
        self.mutate(|local| local.remove_deferred_delete(ino))
    }

    fn list_deferred_deletes(&self) -> MetaResult<Vec<DeferredDeleteRecord>> {
        self.local.list_deferred_deletes()
    }

    fn recover_pending_creates(&self) -> MetaResult<Vec<Ino>> {
        self.mutate(|local| local.recover_pending_creates())
    }

    fn add_pending_create(&self, record: &PendingCreateRecord) -> MetaResult<()> {
        self.mutate(|local| local.add_pending_create(record))
    }

    fn remove_pending_create(&self, ino: Ino) -> MetaResult<()> {
        self.mutate(|local| local.remove_pending_create(ino))
    }

    fn list_pending_creates(&self) -> MetaResult<Vec<PendingCreateRecord>> {
        self.local.list_pending_creates()
    }

    fn recover_undo_logs(&self) -> MetaResult<Vec<Ino>> {
        self.mutate(|local| local.recover_undo_logs())
    }

    fn append_undo_log(&self, record: &UndoLogRecord) -> MetaResult<()> {
        self.mutate(|local| local.append_undo_log(record))
    }

    fn remove_undo_logs_for(&self, kind: UndoLogKind, ino: Ino) -> MetaResult<()> {
        self.mutate(|local| local.remove_undo_logs_for(kind, ino))
    }

    fn list_undo_logs(&self) -> MetaResult<Vec<UndoLogRecord>> {
        self.local.list_undo_logs()
    }
}

impl MetaSymlink for PgMetaEngine {
    fn set_symlink_target(&self, ino: Ino, target: &[u8]) -> MetaResult<()> {
        self.mutate(|local| local.set_symlink_target(ino, target))
    }

    fn get_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>> {
        self.local.get_symlink_target(ino)
    }
}

impl MetaXattr for PgMetaEngine {
    fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>> {
        self.local.get_xattr(ino, name)
    }

    fn get_xattr_size(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<usize>> {
        self.local.get_xattr_size(ino, name)
    }

    fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()> {
        self.mutate(|local| local.set_xattr(ino, name, value, flags))
    }

    fn remove_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()> {
        self.mutate(|local| local.remove_xattr(ino, name))
    }

    fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>> {
        self.local.list_xattrs(ino)
    }
}

impl MetaAcl for PgMetaEngine {
    fn set_acl_rule(&self, ino: Ino, principal: &str, access_level: AccessLevel) -> MetaResult<()> {
        self.mutate(|local| local.set_acl_rule(ino, principal, access_level))
    }

    fn remove_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<()> {
        self.mutate(|local| local.remove_acl_rule(ino, principal))
    }

    fn get_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<Option<AccessLevel>> {
        self.local.get_acl_rule(ino, principal)
    }
}

impl MetaSnapshots for PgMetaEngine {
    fn create_snapshot(&self, root_ino: Ino) -> MetaResult<MetadataSnapshot> {
        self.mutate(|local| local.create_snapshot(root_ino))
    }

    fn list_snapshots(&self) -> MetaResult<Vec<MetadataSnapshot>> {
        let _ = self.sync_metadata();
        self.local.list_snapshots()
    }

    fn get_snapshot(&self, snapshot_id: &str) -> MetaResult<Option<MetadataSnapshot>> {
        let _ = self.sync_metadata();
        self.local.get_snapshot(snapshot_id)
    }
}

impl MetaEngine for PgMetaEngine {
    fn flush_regular_file_overlay_slices_with_attr(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<crate::schema::SliceRecord>)],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Option<InodeAttr>> {
        self.mutate(|local| {
            local.flush_regular_file_overlay_slices_with_attr(ino, chunk_slices, attr)
        })
    }
}
