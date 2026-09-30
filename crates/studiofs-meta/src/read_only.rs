//! Read-only metadata adapter used by immutable snapshot mounts.
//!
//! The adapter deliberately keeps the complete metadata contract available to
//! the VFS while turning every mutation capability into the existing
//! `AccessDenied` error.  Snapshot mounts can therefore reuse the normal read
//! path without exposing a writable metadata engine to platform bridges.

use crate::engine::*;
use crate::snapshot::VolumeId;
use crate::snapshot_ports::*;
use crate::types::*;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// Metadata engine facade that exposes reads and rejects all mutations.
pub struct ReadOnlyMetaEngine<M: ?Sized> {
    inner: Arc<M>,
}

impl<M: ?Sized> ReadOnlyMetaEngine<M> {
    /// Wraps an already-open metadata engine with a read-only contract.
    pub fn new(inner: Arc<M>) -> Self {
        Self { inner }
    }
}

fn read_only_error(ino: Ino) -> MetaError {
    MetaError::AccessDenied {
        ino,
        principal: "snapshot-read-only".to_string(),
        required: AccessLevel::ReadWrite,
        actual: AccessLevel::ReadOnly,
    }
}

impl<M> MetaEngineCore for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn metadata_write_observability(&self) -> MetadataWriteObservability {
        self.inner.metadata_write_observability()
    }

    fn try_acquire_global_janitor_lock(&self) -> MetaResult<bool> {
        Ok(false)
    }

    fn release_global_janitor_lock(&self) -> MetaResult<()> {
        Ok(())
    }

    fn get_global_snapshot_pins(&self) -> MetaResult<HashSet<u64>> {
        self.inner.get_global_snapshot_pins()
    }

    fn acquire_file_lock_lease(
        &self,
        ino: Ino,
        _kind: HardLockKind,
        _lease_ttl: Duration,
    ) -> MetaResult<FileLockLeaseAcquireOutcome> {
        Err(read_only_error(ino))
    }

    fn release_file_lock_lease(&self, ino: Ino) -> MetaResult<FileLockLeaseReleaseOutcome> {
        Err(read_only_error(ino))
    }

    fn renew_file_lock_lease(
        &self,
        ino: Ino,
        _lease_ttl: Duration,
    ) -> MetaResult<FileLockLeaseRenewOutcome> {
        Err(read_only_error(ino))
    }

    fn current_file_lock_lease(&self, ino: Ino) -> MetaResult<Option<FileLockLease>> {
        self.inner.current_file_lock_lease(ino)
    }

    fn ensure_known_file_lock_monitoring(&self) -> MetaResult<()> {
        Ok(())
    }

    fn known_file_lock_lease(&self, ino: Ino) -> Option<FileLockLease> {
        self.inner.known_file_lock_lease(ino)
    }
}

impl<M> MetaFormat for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn set_format_info(&self, _info: &FormatInfo) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn get_format_info(&self) -> MetaResult<FormatInfo> {
        self.inner.get_format_info()
    }
}

impl<M> MetaInodes for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn get_counter(&self, name: &str) -> MetaResult<u64> {
        self.inner.get_counter(name)
    }

    fn incr_counter(&self, _name: &str, _delta: u64) -> MetaResult<u64> {
        Err(read_only_error(ROOT_INO))
    }

    fn decr_counter(&self, _name: &str, _delta: u64) -> MetaResult<u64> {
        Err(read_only_error(ROOT_INO))
    }

    fn set_counter(&self, _name: &str, _value: u64) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn next_inode(&self) -> MetaResult<Ino> {
        Err(read_only_error(ROOT_INO))
    }

    fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr> {
        self.inner.get_attr(ino)
    }

    fn get_inode_lifecycle(&self, ino: Ino) -> MetaResult<Option<InodeLifecycleRecord>> {
        self.inner.get_inode_lifecycle(ino)
    }

    fn list_inode_lifecycles(&self) -> MetaResult<Vec<InodeLifecycleRecord>> {
        self.inner.list_inode_lifecycles()
    }

    fn set_attr(&self, _attr: &InodeAttr) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn remove_attr(&self, ino: Ino) -> MetaResult<()> {
        Err(read_only_error(ino))
    }
}

impl<M> MetaNamespace for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn lookup(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        self.inner.lookup(parent, name)
    }

    fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>> {
        self.inner.readdir(parent)
    }

    fn add_entry(&self, _parent: Ino, _name: &[u8], _entry: &DirEntryValue) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn remove_entry(&self, parent: Ino, _name: &[u8]) -> MetaResult<DirEntryValue> {
        Err(read_only_error(parent))
    }

    fn remove_entry_if_matches(
        &self,
        parent: Ino,
        _name: &[u8],
        _expected_ino: Ino,
    ) -> MetaResult<Option<DirEntryValue>> {
        Err(read_only_error(parent))
    }
}

impl<M> MetaFsOps for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn apply_namespace_mutations(
        &self,
        _mutations: &[NamespaceMutation],
    ) -> MetaResult<Vec<InodeAttr>> {
        Err(read_only_error(ROOT_INO))
    }

    fn create_file(
        &self,
        parent: Ino,
        _name: &[u8],
        _uid: u32,
        _gid: u32,
        _mode: u16,
    ) -> MetaResult<InodeAttr> {
        Err(read_only_error(parent))
    }

    fn create_file_with_ino(
        &self,
        _ino: Ino,
        parent: Ino,
        _name: &[u8],
        _uid: u32,
        _gid: u32,
        _mode: u16,
    ) -> MetaResult<InodeAttr> {
        Err(read_only_error(parent))
    }

    fn create_dir(
        &self,
        parent: Ino,
        _name: &[u8],
        _uid: u32,
        _gid: u32,
        _mode: u16,
    ) -> MetaResult<InodeAttr> {
        Err(read_only_error(parent))
    }

    fn create_dir_with_ino(
        &self,
        _ino: Ino,
        parent: Ino,
        _name: &[u8],
        _uid: u32,
        _gid: u32,
        _mode: u16,
    ) -> MetaResult<InodeAttr> {
        Err(read_only_error(parent))
    }

    fn unlink(&self, parent: Ino, _name: &[u8]) -> MetaResult<InodeAttr> {
        Err(read_only_error(parent))
    }

    fn unlink_open_handle(&self, parent: Ino, _name: &[u8]) -> MetaResult<InodeAttr> {
        Err(read_only_error(parent))
    }

    fn rmdir(&self, parent: Ino, _name: &[u8]) -> MetaResult<()> {
        Err(read_only_error(parent))
    }

    fn rename_atomic(
        &self,
        parent: Ino,
        _name: &[u8],
        _new_parent: Ino,
        _new_name: &[u8],
    ) -> MetaResult<RenameAtomicResult> {
        Err(read_only_error(parent))
    }

    fn link_atomic(&self, ino: Ino, _new_parent: Ino, _new_name: &[u8]) -> MetaResult<InodeAttr> {
        Err(read_only_error(ino))
    }

    fn create_symlink(
        &self,
        parent: Ino,
        _name: &[u8],
        _target: &[u8],
        _uid: u32,
        _gid: u32,
    ) -> MetaResult<InodeAttr> {
        Err(read_only_error(parent))
    }
}

impl<M> MetaPayloadObjects for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn lookup_regular_file_chunk_slices(
        &self,
        ino: Ino,
        chunk_id: ChunkId,
        chunk_range: SliceRange,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        self.inner
            .lookup_regular_file_chunk_slices(ino, chunk_id, chunk_range)
    }

    fn lookup_regular_file_slices(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        self.inner.lookup_regular_file_slices(ino, offset, length)
    }

    fn remove_regular_file_graph(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        Err(read_only_error(ino))
    }

    fn next_object_id(&self) -> MetaResult<u64> {
        Err(read_only_error(ROOT_INO))
    }

    fn next_object_ids(&self, _count: u32) -> MetaResult<Vec<u64>> {
        Err(read_only_error(ROOT_INO))
    }

    fn flush_regular_file_overlay_slices(
        &self,
        ino: Ino,
        _chunk_slices: &[(ChunkId, Vec<SliceRecord>)],
    ) -> MetaResult<()> {
        Err(read_only_error(ino))
    }

    fn has_local_regular_file_overlay(&self, ino: Ino) -> MetaResult<bool> {
        self.inner.has_local_regular_file_overlay(ino)
    }

    fn get_regular_file_overlay_object_ids(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        self.inner.get_regular_file_overlay_object_ids(ino)
    }

    fn get_regular_file_graph_object_usage(&self, ino: Ino) -> MetaResult<Vec<(u64, u64)>> {
        self.inner.get_regular_file_graph_object_usage(ino)
    }

    fn has_regular_file_overlay_object_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> MetaResult<bool> {
        self.inner
            .has_regular_file_overlay_object_if_matches(ino, expected_epoch, object_id)
    }

    fn mark_regular_file_overlay_object_uploaded_if_matches(
        &self,
        ino: Ino,
        _expected_epoch: u64,
        _object_id: u64,
    ) -> MetaResult<bool> {
        Err(read_only_error(ino))
    }

    fn publish_regular_file_overlay_to_base_if_matches_batch(
        &self,
        _requests: &[(Ino, u64)],
    ) -> MetaResult<Vec<bool>> {
        Err(read_only_error(ROOT_INO))
    }
}

impl<M> MetaGc for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn local_storage_diagnostics(&self) -> MetaResult<Option<LocalStorageDiagnostics>> {
        self.inner.local_storage_diagnostics()
    }

    fn add_pending_deletion(&self, _object_id: u64, _timestamp: i64) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn add_pending_deletions(&self, _entries: &[(u64, i64)]) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn remove_pending_deletion(&self, _object_id: u64) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>> {
        self.inner.get_pending_deletions()
    }

    fn get_referenced_object_ids(&self) -> MetaResult<HashSet<u64>> {
        self.inner.get_referenced_object_ids()
    }
}

impl<M> MetaRecovery for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn recover_pending_creates(&self) -> MetaResult<Vec<Ino>> {
        Err(read_only_error(ROOT_INO))
    }

    fn add_pending_create(&self, _record: &PendingCreateRecord) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn remove_pending_create(&self, ino: Ino) -> MetaResult<()> {
        Err(read_only_error(ino))
    }

    fn list_pending_creates(&self) -> MetaResult<Vec<PendingCreateRecord>> {
        self.inner.list_pending_creates()
    }

    fn append_undo_log(&self, _record: &UndoLogRecord) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn remove_undo_logs_for(&self, _kind: UndoLogKind, ino: Ino) -> MetaResult<()> {
        Err(read_only_error(ino))
    }

    fn list_undo_logs(&self) -> MetaResult<Vec<UndoLogRecord>> {
        self.inner.list_undo_logs()
    }

    fn recover_undo_logs(&self) -> MetaResult<Vec<Ino>> {
        Err(read_only_error(ROOT_INO))
    }

    fn add_deferred_delete(&self, _record: &DeferredDeleteRecord) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn remove_deferred_delete(&self, ino: Ino) -> MetaResult<()> {
        Err(read_only_error(ino))
    }

    fn list_deferred_deletes(&self) -> MetaResult<Vec<DeferredDeleteRecord>> {
        self.inner.list_deferred_deletes()
    }
}

impl<M> MetaSymlink for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn set_symlink_target(&self, ino: Ino, _target: &[u8]) -> MetaResult<()> {
        Err(read_only_error(ino))
    }

    fn get_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>> {
        self.inner.get_symlink_target(ino)
    }
}

impl<M> MetaXattr for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>> {
        self.inner.get_xattr(ino, name)
    }

    fn get_xattr_size(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<usize>> {
        self.inner.get_xattr_size(ino, name)
    }

    fn set_xattr(&self, ino: Ino, _name: &[u8], _value: &[u8], _flags: u32) -> MetaResult<()> {
        Err(read_only_error(ino))
    }

    fn remove_xattr(&self, ino: Ino, _name: &[u8]) -> MetaResult<()> {
        Err(read_only_error(ino))
    }

    fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>> {
        self.inner.list_xattrs(ino)
    }
}

impl<M> MetaAcl for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn set_acl_rule(
        &self,
        ino: Ino,
        _principal: &str,
        _access_level: AccessLevel,
    ) -> MetaResult<()> {
        Err(read_only_error(ino))
    }

    fn remove_acl_rule(&self, ino: Ino, _principal: &str) -> MetaResult<()> {
        Err(read_only_error(ino))
    }

    fn get_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<Option<AccessLevel>> {
        self.inner.get_acl_rule(ino, principal)
    }
}

impl<M> MetaReplication for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn check_consistency(&self) -> MetaResult<()> {
        self.inner.check_consistency()
    }

    fn flush_metadata(&self) -> MetaResult<()> {
        Ok(())
    }

    fn flush_local_metadata(&self) -> MetaResult<()> {
        Ok(())
    }

    fn sync_metadata(&self) -> MetaResult<()> {
        Ok(())
    }

    fn sync_local_metadata(&self) -> MetaResult<()> {
        Ok(())
    }

    fn note_active_open_inode(&self, _ino: Ino) {}

    fn note_inactive_open_inode(&self, _ino: Ino) {}

    fn sync_metadata_for_invalidation_poll(&self) -> MetaResult<()> {
        Ok(())
    }

    fn metadata_coherence_progress(&self) -> crate::engine::MetadataCoherenceProgress {
        self.inner.metadata_coherence_progress()
    }

    fn invalidation_sender_id(&self) -> Option<String> {
        None
    }

    fn request_eager_flush(&self) {}

    fn metadata_version_token(&self) -> Option<String> {
        self.inner.metadata_version_token()
    }

    fn namespace_version_token(&self) -> Option<String> {
        self.inner.namespace_version_token()
    }

    fn replication_backlog(&self) -> MetadataReplicationBacklog {
        MetadataReplicationBacklog::default()
    }

    fn metadata_authority_status(&self) -> MetadataAuthorityStatus {
        MetadataAuthorityStatus::read_only()
    }

    fn pg_sync_status(&self) -> PgSyncStatus {
        PgSyncStatus::new(
            self.inner.pg_sync_status().lifecycle,
            MetadataAuthorityStatus::read_only(),
        )
    }

    fn pg_sync_diagnostics(&self) -> PgSyncDiagnostics {
        let mut diagnostics = self.inner.pg_sync_diagnostics();
        diagnostics.authority = MetadataAuthorityStatus::read_only();
        diagnostics.write_authority = false;
        diagnostics.write_authority_reason = Some(PgSyncWriteAuthorityReason::ReadOnlyMount);
        diagnostics
    }

    fn remote_metadata_size(&self) -> MetaResult<Option<u64>> {
        self.inner.remote_metadata_size()
    }

    fn prepare_for_close(&self) -> MetaResult<()> {
        self.inner.prepare_for_close()
    }
}

impl<M> UploadedFragmentLedger for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn fragment_ledger_volume_id(&self) -> MetaResult<VolumeId> {
        self.inner.fragment_ledger_volume_id()
    }

    fn claim_uploaded_fragment_reconciliation(
        &self,
        _owner: &str,
    ) -> MetaResult<Vec<UploadedFragmentReconciliationClaim>> {
        Err(read_only_error(ROOT_INO))
    }

    fn finish_uploaded_fragment_reconciliation(
        &self,
        _owner: &str,
        _claim: &UploadedFragmentReconciliationClaim,
        _outcome: UploadedFragmentReconciliationOutcome,
    ) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn claim_uploaded_object_deletion(
        &self,
        _object_id: u64,
        _owner: &str,
    ) -> MetaResult<UploadedObjectDeletionClaim> {
        Err(read_only_error(ROOT_INO))
    }

    fn finish_uploaded_object_deletion(
        &self,
        _object_id: u64,
        _owner: &str,
        _fencing_token: i64,
        _outcome: UploadedObjectDeletionOutcome,
    ) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn begin_fragment_upload(&self, _entry: &UploadedFragmentLedgerEntry) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }

    fn confirm_fragment_uploaded(&self, _entry: &UploadedFragmentLedgerEntry) -> MetaResult<()> {
        Err(read_only_error(ROOT_INO))
    }
}

impl<M> MetaEngine for ReadOnlyMetaEngine<M>
where
    M: ?Sized + MetaEngine,
{
    fn apply_regular_file_overlay_batch(
        &self,
        _mutations: &[RegularFileOverlayMutation],
    ) -> MetaResult<Vec<Option<InodeAttr>>> {
        Err(read_only_error(ROOT_INO))
    }

    fn flush_regular_file_overlay_slices_with_attr(
        &self,
        ino: Ino,
        _chunk_slices: &[(ChunkId, Vec<SliceRecord>)],
        _attr: Option<&InodeAttr>,
    ) -> MetaResult<Option<InodeAttr>> {
        Err(read_only_error(ino))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redb_engine::RedbMetaEngine;

    #[test]
    fn read_only_meta_rejects_conditional_namespace_removal() {
        let dir = studiofs_test_support::workspace_target_tempdir("read-only-namespace-");
        let inner = Arc::new(RedbMetaEngine::open(dir.path().join("meta.redb")).unwrap());
        let entry = DirEntryValue {
            ino: 42,
            kind: FileType::RegularFile,
        };
        inner.add_entry(ROOT_INO, b"entry", &entry).unwrap();
        let read_only = ReadOnlyMetaEngine::new(Arc::clone(&inner));

        assert!(matches!(
            read_only.remove_entry_if_matches(ROOT_INO, b"entry", 42),
            Err(MetaError::AccessDenied { .. })
        ));
        assert_eq!(inner.lookup(ROOT_INO, b"entry").unwrap(), entry);
    }

    #[test]
    fn pg_sync_status_preserves_lifecycle_and_forces_read_only_authority() {
        let dir = studiofs_test_support::workspace_target_tempdir("read-only-pg-sync-");
        let path = dir.path().join("meta.redb");
        let inner = Arc::new(RedbMetaEngine::open(&path).expect("open local metadata"));
        let expected_lifecycle = inner.pg_sync_status().lifecycle;

        let read_only = ReadOnlyMetaEngine::new(Arc::clone(&inner));
        let status = read_only.pg_sync_status();

        assert_eq!(status.lifecycle, expected_lifecycle);
        assert_eq!(status.authority, MetadataAuthorityStatus::read_only());
        assert!(!status.allows_new_mutations());
    }

    #[test]
    fn read_only_meta_forwards_local_storage_diagnostics() {
        let dir = studiofs_test_support::workspace_target_tempdir("read-only-storage-stats-");
        let path = dir.path().join("meta.redb");
        let writable = RedbMetaEngine::open(&path).expect("open local metadata");
        writable
            .add_pending_deletion(7, 11)
            .expect("write one local event");
        drop(writable);

        let inner = Arc::new(RedbMetaEngine::open_read_only(&path).expect("open read-only redb"));
        let expected = inner
            .local_storage_diagnostics()
            .expect("read-only inner diagnostics")
            .expect("redb supports local diagnostics");
        let read_only = ReadOnlyMetaEngine::new(inner);

        assert_eq!(
            read_only
                .local_storage_diagnostics()
                .expect("forward local diagnostics"),
            Some(expected)
        );
    }
}
