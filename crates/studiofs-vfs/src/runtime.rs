use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use studiofs_meta::engine::{
    DeferredDeleteRecord, MetadataAuthorityStatus, MetadataCheckpointFreshnessDiagnostics,
    MetadataCoherenceProgress, MetadataInvalidationDiagnostics, MetadataReplicationBacklog,
    MetadataWriteObservability, PendingCreateRecord, PgSyncStatus, UndoLogKind, UndoLogRecord,
};
use studiofs_meta::snapshot_ports::{
    SnapshotCleanupStore, SnapshotJobStore, SnapshotPolicyStore, SnapshotSourceReader,
    UploadedFragmentLedger,
};
use studiofs_meta::{
    AccessLevel, ChunkId, DirEntry, DirEntryValue, FormatInfo, Ino, InodeAttr,
    InodeLifecycleRecord, MetaAcl, MetaEngine, MetaEngineCore, MetaFormat, MetaFsOps, MetaGc,
    MetaInodes, MetaNamespace, MetaPayloadObjects, MetaRecovery, MetaReplication, MetaResult,
    MetaSymlink, MetaXattr, NamespaceMutation, RenameAtomicResult, ResolvedSliceSpan, SliceRange,
    SliceRecord, SyncMetaEngineAdapter, execute_sync_meta,
};
use tokio::task;
use tokio_util::sync::CancellationToken;

use super::{VfsError, VfsResult};

/// Runtime-facing wrapper over metadata engine methods used when committing file
/// graph updates and optional attribute changes.
pub trait VfsCommitMeta: Send + Sync {
    /// Flush overlay slice data for regular files and optionally update the inode
    /// attributes in one metadata operation.
    fn flush_regular_file_overlay_slices_with_attr(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<SliceRecord>)],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Option<InodeAttr>>;

    /// Apply ordered regular-file overlay updates in one metadata transaction.
    fn apply_regular_file_overlay_batch(
        &self,
        mutations: &[studiofs_meta::RegularFileOverlayMutation],
    ) -> MetaResult<Vec<Option<InodeAttr>>>;
}

/// Optional Snapshot v2 ports exposed through the runtime metadata facade.
pub trait VfsSnapshotProvider: Send + Sync {
    /// Returns the durable snapshot job store, when the backend supports it.
    fn snapshot_job_store(&self) -> Option<Arc<dyn SnapshotJobStore>>;

    /// Returns the immutable snapshot source reader, when the backend supports it.
    fn snapshot_source_reader(&self) -> Option<Arc<dyn SnapshotSourceReader>>;

    /// Returns the durable Snapshot v2 cleanup store, when supported.
    fn snapshot_cleanup_store(&self) -> Option<Arc<dyn SnapshotCleanupStore>>;

    /// Returns the durable Snapshot v2 daily-policy store, when supported.
    fn snapshot_policy_store(&self) -> Option<Arc<dyn SnapshotPolicyStore>>;
}

impl<T> VfsSnapshotProvider for T
where
    T: ?Sized + MetaEngine,
{
    fn snapshot_job_store(&self) -> Option<Arc<dyn SnapshotJobStore>> {
        MetaEngine::snapshot_job_store(self)
    }

    fn snapshot_source_reader(&self) -> Option<Arc<dyn SnapshotSourceReader>> {
        MetaEngine::snapshot_source_reader(self)
    }

    fn snapshot_cleanup_store(&self) -> Option<Arc<dyn SnapshotCleanupStore>> {
        MetaEngine::snapshot_cleanup_store(self)
    }

    fn snapshot_policy_store(&self) -> Option<Arc<dyn SnapshotPolicyStore>> {
        MetaEngine::snapshot_policy_store(self)
    }
}

impl<T> VfsCommitMeta for T
where
    T: ?Sized + MetaEngine,
{
    fn flush_regular_file_overlay_slices_with_attr(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<SliceRecord>)],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Option<InodeAttr>> {
        <T as MetaEngine>::flush_regular_file_overlay_slices_with_attr(
            self,
            ino,
            chunk_slices,
            attr,
        )
    }

    fn apply_regular_file_overlay_batch(
        &self,
        mutations: &[studiofs_meta::RegularFileOverlayMutation],
    ) -> MetaResult<Vec<Option<InodeAttr>>> {
        <T as MetaEngine>::apply_regular_file_overlay_batch(self, mutations)
    }
}

/// Combined metadata surface required by the VFS runtime.
pub trait VfsMeta:
    Send
    + Sync
    + VfsCommitMeta
    + VfsSnapshotProvider
    + MetaEngineCore
    + MetaFormat
    + MetaInodes
    + MetaNamespace
    + MetaFsOps
    + MetaPayloadObjects
    + MetaGc
    + MetaRecovery
    + MetaSymlink
    + MetaXattr
    + MetaAcl
    + MetaReplication
    + UploadedFragmentLedger
{
    /// Return local metadata write timing through the runtime metadata facade.
    ///
    /// This explicit forwarding method preserves the concrete backend override
    /// when the VFS stores metadata as `dyn VfsMeta`.
    fn vfs_metadata_write_observability(&self) -> MetadataWriteObservability;
}

impl<T> VfsMeta for T
where
    T: ?Sized
        + Send
        + Sync
        + VfsCommitMeta
        + VfsSnapshotProvider
        + MetaEngineCore
        + MetaFormat
        + MetaInodes
        + MetaNamespace
        + MetaFsOps
        + MetaPayloadObjects
        + MetaGc
        + MetaRecovery
        + MetaSymlink
        + MetaXattr
        + MetaAcl
        + MetaReplication
        + UploadedFragmentLedger,
{
    fn vfs_metadata_write_observability(&self) -> MetadataWriteObservability {
        MetaEngineCore::metadata_write_observability(self)
    }
}

/// Read path runtime metadata facade used by VFS read-path operations.
pub trait ReadPathMeta: Send + Sync + MetaInodes + MetaPayloadObjects {}
impl<T> ReadPathMeta for T where T: ?Sized + Send + Sync + MetaInodes + MetaPayloadObjects {}

/// ACL-specific metadata facade for VFS callers.
pub trait AclMeta: Send + Sync + MetaInodes + MetaAcl {}
impl<T> AclMeta for T where T: ?Sized + Send + Sync + MetaInodes + MetaAcl {}

/// Upload path metadata facade used while staging and publishing file payloads.
pub trait UploadMeta: Send + Sync + MetaInodes + MetaPayloadObjects + MetaGc {}
impl<T> UploadMeta for T where T: ?Sized + Send + Sync + MetaInodes + MetaPayloadObjects + MetaGc {}

/// Metadata facade required by janitor maintenance tasks.
pub trait JanitorMeta:
    Send
    + Sync
    + MetaEngineCore
    + MetaInodes
    + MetaNamespace
    + MetaPayloadObjects
    + MetaGc
    + MetaRecovery
    + MetaReplication
{
}
impl<T> JanitorMeta for T where
    T: ?Sized
        + Send
        + Sync
        + MetaEngineCore
        + MetaInodes
        + MetaNamespace
        + MetaPayloadObjects
        + MetaGc
        + MetaRecovery
        + MetaReplication
{
}

macro_rules! forward_method {
    (fn $name:ident(&self $(, $arg:ident : $argty:ty)* $(,)?) -> $ret:ty) => {
        fn $name(&self $(, $arg: $argty)*) -> $ret {
            self.inner.$name($($arg),*)
        }
    };
    (fn $name:ident(&self $(, $arg:ident : $argty:ty)* $(,)?)) => {
        fn $name(&self $(, $arg: $argty)*) {
            self.inner.$name($($arg),*)
        }
    };
}

struct RuntimeMetaHandle<M: ?Sized> {
    inner: Arc<M>,
}

impl<M: ?Sized> RuntimeMetaHandle<M> {
    fn new(inner: Arc<M>) -> Self {
        Self { inner }
    }
}

impl<M: ?Sized + VfsCommitMeta> VfsCommitMeta for RuntimeMetaHandle<M> {
    forward_method!(
        fn apply_regular_file_overlay_batch(
            &self,
            mutations: &[studiofs_meta::RegularFileOverlayMutation],
        ) -> MetaResult<Vec<Option<InodeAttr>>>
    );
    forward_method!(
        fn flush_regular_file_overlay_slices_with_attr(
            &self,
            ino: Ino,
            chunk_slices: &[(ChunkId, Vec<SliceRecord>)],
            attr: Option<&InodeAttr>,
        ) -> MetaResult<Option<InodeAttr>>
    );
}

impl<M: ?Sized + VfsSnapshotProvider> VfsSnapshotProvider for RuntimeMetaHandle<M> {
    forward_method!(fn snapshot_job_store(&self) -> Option<Arc<dyn SnapshotJobStore>>);
    forward_method!(fn snapshot_source_reader(&self) -> Option<Arc<dyn SnapshotSourceReader>>);
    forward_method!(fn snapshot_cleanup_store(&self) -> Option<Arc<dyn SnapshotCleanupStore>>);
    forward_method!(fn snapshot_policy_store(&self) -> Option<Arc<dyn SnapshotPolicyStore>>);
}

impl<M: ?Sized + UploadedFragmentLedger> UploadedFragmentLedger for RuntimeMetaHandle<M> {
    forward_method!(fn fragment_ledger_volume_id(
        &self,
    ) -> MetaResult<studiofs_meta::snapshot::VolumeId>);
    forward_method!(fn claim_uploaded_fragment_reconciliation(
        &self,
        owner: &str,
    ) -> MetaResult<Vec<studiofs_meta::snapshot_ports::UploadedFragmentReconciliationClaim>>);
    forward_method!(fn finish_uploaded_fragment_reconciliation(
        &self,
        owner: &str,
        claim: &studiofs_meta::snapshot_ports::UploadedFragmentReconciliationClaim,
        outcome: studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome,
    ) -> MetaResult<()>);
    forward_method!(fn claim_uploaded_object_deletion(
        &self,
        object_id: u64,
        owner: &str,
    ) -> MetaResult<studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim>);
    forward_method!(fn finish_uploaded_object_deletion(
        &self,
        object_id: u64,
        owner: &str,
        fencing_token: i64,
        outcome: studiofs_meta::snapshot_ports::UploadedObjectDeletionOutcome,
    ) -> MetaResult<()>);
    forward_method!(fn begin_fragment_upload(
        &self,
        entry: &studiofs_meta::snapshot_ports::UploadedFragmentLedgerEntry,
    ) -> MetaResult<()>);
    forward_method!(fn begin_fragment_uploads(
        &self,
        entries: &[studiofs_meta::snapshot_ports::UploadedFragmentLedgerEntry],
    ) -> MetaResult<()>);
    forward_method!(fn confirm_fragment_uploaded(
        &self,
        entry: &studiofs_meta::snapshot_ports::UploadedFragmentLedgerEntry,
    ) -> MetaResult<()>);
    forward_method!(fn confirm_fragment_uploads(
        &self,
        entries: &[studiofs_meta::snapshot_ports::UploadedFragmentLedgerEntry],
    ) -> MetaResult<()>);
}

impl<M: ?Sized + MetaEngineCore> MetaEngineCore for RuntimeMetaHandle<M> {
    forward_method!(fn metadata_write_observability(&self) -> MetadataWriteObservability);
    forward_method!(fn try_acquire_global_janitor_lock(&self) -> MetaResult<bool>);
    forward_method!(fn release_global_janitor_lock(&self) -> MetaResult<()>);
    forward_method!(fn get_global_snapshot_pins(&self) -> MetaResult<HashSet<u64>>);
    forward_method!(
        fn acquire_file_lock_lease(
            &self,
            ino: Ino,
            kind: studiofs_meta::HardLockKind,
            lease_ttl: Duration,
        ) -> MetaResult<studiofs_meta::FileLockLeaseAcquireOutcome>
    );
    forward_method!(
        fn release_file_lock_lease(
            &self,
            ino: Ino,
        ) -> MetaResult<studiofs_meta::FileLockLeaseReleaseOutcome>
    );
    forward_method!(
        fn renew_file_lock_lease(
            &self,
            ino: Ino,
            lease_ttl: Duration,
        ) -> MetaResult<studiofs_meta::FileLockLeaseRenewOutcome>
    );
    forward_method!(
        fn current_file_lock_lease(
            &self,
            ino: Ino,
        ) -> MetaResult<Option<studiofs_meta::FileLockLease>>
    );
    forward_method!(fn ensure_known_file_lock_monitoring(&self) -> MetaResult<()>);
    forward_method!(fn known_file_lock_lease(&self, ino: Ino) -> Option<studiofs_meta::FileLockLease>);
}

impl<M: ?Sized + MetaFormat> MetaFormat for RuntimeMetaHandle<M> {
    forward_method!(fn set_format_info(&self, info: &FormatInfo) -> MetaResult<()>);
    forward_method!(fn get_format_info(&self) -> MetaResult<FormatInfo>);
}

impl<M: ?Sized + MetaInodes> MetaInodes for RuntimeMetaHandle<M> {
    forward_method!(fn get_counter(&self, name: &str) -> MetaResult<u64>);
    forward_method!(fn incr_counter(&self, name: &str, delta: u64) -> MetaResult<u64>);
    forward_method!(fn decr_counter(&self, name: &str, delta: u64) -> MetaResult<u64>);
    forward_method!(fn set_counter(&self, name: &str, value: u64) -> MetaResult<()>);
    forward_method!(fn next_inode(&self) -> MetaResult<Ino>);
    forward_method!(fn next_inode_ids(&self, count: u32) -> MetaResult<Vec<Ino>>);
    forward_method!(fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr>);
    forward_method!(fn get_inode_lifecycle(&self, ino: Ino) -> MetaResult<Option<InodeLifecycleRecord>>);
    forward_method!(fn list_inode_lifecycles(&self) -> MetaResult<Vec<InodeLifecycleRecord>>);
    forward_method!(fn get_attrs_batch(&self, inos: &[Ino]) -> MetaResult<Vec<InodeAttr>>);
    forward_method!(fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()>);
    forward_method!(fn remove_attr(&self, ino: Ino) -> MetaResult<()>);
    forward_method!(fn list_inode_attrs(&self) -> MetaResult<Vec<InodeAttr>>);
}

impl<M: ?Sized + MetaNamespace> MetaNamespace for RuntimeMetaHandle<M> {
    forward_method!(fn lookup(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue>);
    forward_method!(fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>>);
    forward_method!(
        fn add_entry(
            &self,
            parent: Ino,
            name: &[u8],
            entry: &DirEntryValue,
        ) -> MetaResult<()>
    );
    forward_method!(fn remove_entry(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue>);
}

impl<M: ?Sized + MetaFsOps> MetaFsOps for RuntimeMetaHandle<M> {
    forward_method!(
        fn apply_namespace_mutations(
            &self,
            mutations: &[NamespaceMutation],
        ) -> MetaResult<Vec<InodeAttr>>
    );
    forward_method!(
        fn create_file(
            &self,
            parent: Ino,
            name: &[u8],
            uid: u32,
            gid: u32,
            mode: u16,
        ) -> MetaResult<InodeAttr>
    );
    forward_method!(
        fn create_file_with_ino(
            &self,
            ino: Ino,
            parent: Ino,
            name: &[u8],
            uid: u32,
            gid: u32,
            mode: u16,
        ) -> MetaResult<InodeAttr>
    );
    forward_method!(
        fn create_dir(
            &self,
            parent: Ino,
            name: &[u8],
            uid: u32,
            gid: u32,
            mode: u16,
        ) -> MetaResult<InodeAttr>
    );
    forward_method!(
        fn create_dir_with_ino(
            &self,
            ino: Ino,
            parent: Ino,
            name: &[u8],
            uid: u32,
            gid: u32,
            mode: u16,
        ) -> MetaResult<InodeAttr>
    );
    forward_method!(fn unlink(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr>);
    forward_method!(
        fn unlink_open_handle(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr>
    );
    forward_method!(fn rmdir(&self, parent: Ino, name: &[u8]) -> MetaResult<()>);
    forward_method!(
        fn rename_atomic(
            &self,
            parent: Ino,
            name: &[u8],
            new_parent: Ino,
            new_name: &[u8],
        ) -> MetaResult<RenameAtomicResult>
    );
    forward_method!(
        fn create_symlink(
            &self,
            parent: Ino,
            name: &[u8],
            target: &[u8],
            uid: u32,
            gid: u32,
        ) -> MetaResult<InodeAttr>
    );
    forward_method!(
        fn link_atomic(
            &self,
            ino: Ino,
            new_parent: Ino,
            new_name: &[u8],
        ) -> MetaResult<InodeAttr>
    );
}

impl<M: ?Sized + MetaPayloadObjects> MetaPayloadObjects for RuntimeMetaHandle<M> {
    forward_method!(
        fn lookup_regular_file_chunk_slices(
            &self,
            ino: Ino,
            chunk_id: ChunkId,
            chunk_range: SliceRange,
        ) -> MetaResult<Vec<ResolvedSliceSpan>>
    );
    forward_method!(
        fn lookup_regular_file_slices(
            &self,
            ino: Ino,
            offset: u64,
            length: u64,
        ) -> MetaResult<Vec<ResolvedSliceSpan>>
    );
    forward_method!(fn remove_regular_file_graph(&self, ino: Ino) -> MetaResult<Vec<u64>>);
    forward_method!(fn next_object_id(&self) -> MetaResult<u64>);
    forward_method!(fn next_object_ids(&self, count: u32) -> MetaResult<Vec<u64>>);
    forward_method!(
        fn flush_regular_file_overlay_slices(
            &self,
            ino: Ino,
            chunk_slices: &[(ChunkId, Vec<SliceRecord>)],
        ) -> MetaResult<()>
    );
    forward_method!(fn has_local_regular_file_overlay(&self, ino: Ino) -> MetaResult<bool>);
    forward_method!(
        fn get_regular_file_overlay_object_ids(&self, ino: Ino) -> MetaResult<Vec<u64>>
    );
    forward_method!(
        fn get_regular_file_graph_object_usage(&self, ino: Ino) -> MetaResult<Vec<(u64, u64)>>
    );
    forward_method!(
        fn has_regular_file_overlay_object_if_matches(
            &self,
            ino: Ino,
            expected_epoch: u64,
            object_id: u64,
        ) -> MetaResult<bool>
    );
    forward_method!(
        fn mark_regular_file_overlay_object_uploaded_if_matches(
            &self,
            ino: Ino,
            expected_epoch: u64,
            object_id: u64,
        ) -> MetaResult<bool>
    );
    forward_method!(
        fn publish_regular_file_overlay_to_base_if_matches_batch(
            &self,
            requests: &[(Ino, u64)],
        ) -> MetaResult<Vec<bool>>
    );
    forward_method!(
        fn compact_fragmented_regular_file_chunks(&self, max_chunks: usize) -> MetaResult<usize>
    );
}

impl<M: ?Sized + MetaGc> MetaGc for RuntimeMetaHandle<M> {
    forward_method!(
        fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> MetaResult<()>
    );
    forward_method!(
        fn add_pending_deletions(&self, entries: &[(u64, i64)]) -> MetaResult<()>
    );
    forward_method!(fn remove_pending_deletion(&self, object_id: u64) -> MetaResult<()>);
    forward_method!(fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>>);
    forward_method!(fn get_pending_deletions_limited(&self, limit: usize) -> MetaResult<Vec<(u64, i64)>>);
    forward_method!(fn get_referenced_object_ids(&self) -> MetaResult<HashSet<u64>>);
}

impl<M: ?Sized + MetaRecovery> MetaRecovery for RuntimeMetaHandle<M> {
    forward_method!(fn recover_pending_creates(&self) -> MetaResult<Vec<Ino>>);
    forward_method!(
        fn add_pending_create(&self, record: &PendingCreateRecord) -> MetaResult<()>
    );
    forward_method!(fn remove_pending_create(&self, ino: Ino) -> MetaResult<()>);
    forward_method!(fn list_pending_creates(&self) -> MetaResult<Vec<PendingCreateRecord>>);
    forward_method!(fn append_undo_log(&self, record: &UndoLogRecord) -> MetaResult<()>);
    forward_method!(
        fn remove_undo_logs_for(&self, kind: UndoLogKind, ino: Ino) -> MetaResult<()>
    );
    forward_method!(fn list_undo_logs(&self) -> MetaResult<Vec<UndoLogRecord>>);
    forward_method!(fn recover_undo_logs(&self) -> MetaResult<Vec<Ino>>);
    forward_method!(
        fn add_deferred_delete(&self, record: &DeferredDeleteRecord) -> MetaResult<()>
    );
    forward_method!(fn remove_deferred_delete(&self, ino: Ino) -> MetaResult<()>);
    forward_method!(fn list_deferred_deletes(&self) -> MetaResult<Vec<DeferredDeleteRecord>>);
}

impl<M: ?Sized + MetaSymlink> MetaSymlink for RuntimeMetaHandle<M> {
    forward_method!(fn set_symlink_target(&self, ino: Ino, target: &[u8]) -> MetaResult<()>);
    forward_method!(fn get_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>>);
}

impl<M: ?Sized + MetaXattr> MetaXattr for RuntimeMetaHandle<M> {
    forward_method!(
        fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>>
    );
    forward_method!(
        fn get_xattr_size(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<usize>>
    );
    forward_method!(
        fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()>
    );
    forward_method!(fn remove_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()>);
    forward_method!(fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>>);
}

impl<M: ?Sized + MetaAcl> MetaAcl for RuntimeMetaHandle<M> {
    forward_method!(
        fn set_acl_rule(
            &self,
            ino: Ino,
            principal: &str,
            access_level: AccessLevel,
        ) -> MetaResult<()>
    );
    forward_method!(fn remove_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<()>);
    forward_method!(
        fn get_acl_rule(
            &self,
            ino: Ino,
            principal: &str,
        ) -> MetaResult<Option<AccessLevel>>
    );
}

impl<M: ?Sized + MetaReplication> MetaReplication for RuntimeMetaHandle<M> {
    forward_method!(fn take_invalidation_batches(&self) -> Vec<studiofs_meta::coherence::InvalidationBatch>);
    forward_method!(fn check_consistency(&self) -> MetaResult<()>);
    forward_method!(fn flush_metadata(&self) -> MetaResult<()>);
    forward_method!(fn flush_local_metadata(&self) -> MetaResult<()>);
    forward_method!(fn sync_metadata(&self) -> MetaResult<()>);
    forward_method!(fn sync_local_metadata(&self) -> MetaResult<()>);
    forward_method!(fn note_active_open_inode(&self, ino: Ino));
    forward_method!(fn note_inactive_open_inode(&self, ino: Ino));
    forward_method!(fn sync_metadata_for_invalidation_poll(&self) -> MetaResult<()>);
    forward_method!(fn invalidation_sender_id(&self) -> Option<String>);
    forward_method!(
        fn metadata_invalidation_notifier(&self) -> Option<Arc<tokio::sync::Notify>>
    );
    forward_method!(
        fn metadata_invalidation_diagnostics(&self) -> MetadataInvalidationDiagnostics
    );
    forward_method!(
        fn metadata_checkpoint_freshness_diagnostics(
            &self,
        ) -> MetadataCheckpointFreshnessDiagnostics
    );
    forward_method!(fn metadata_coherence_progress(&self) -> MetadataCoherenceProgress);
    forward_method!(fn request_eager_flush(&self));
    forward_method!(fn metadata_version_token(&self) -> Option<String>);
    forward_method!(fn namespace_version_token(&self) -> Option<String>);
    forward_method!(fn replication_backlog(&self) -> MetadataReplicationBacklog);
    forward_method!(fn metadata_authority_status(&self) -> MetadataAuthorityStatus);
    forward_method!(fn pg_sync_status(&self) -> PgSyncStatus);
    forward_method!(fn remote_metadata_size(&self) -> MetaResult<Option<u64>>);
    forward_method!(fn prepare_for_close(&self) -> MetaResult<()>);
}

/// Wrap a metadata principal in a local runtime handle that exposes the
/// `VfsMeta` bundle as a trait object.
///
/// This keeps `StudioVfs` internals on a narrowed bundle even when callers
/// still construct VFS from `Arc<dyn MetaEngine>`, and works around the lack of
/// trait-object upcasting from one dyn supertrait composition to another.
pub fn into_vfs_meta<M>(meta: Arc<M>) -> Arc<dyn VfsMeta>
where
    M: ?Sized + VfsMeta + 'static,
{
    Arc::new(RuntimeMetaHandle::new(meta))
}

/// Runtime adapter around sync metadata engines with async/sync execution helpers.
pub struct MetaExecutor<M: ?Sized> {
    backend: SyncMetaEngineAdapter<M>,
}

impl<M: ?Sized> Clone for MetaExecutor<M> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
        }
    }
}

impl<M: ?Sized + Send + Sync + 'static> MetaExecutor<M> {
    /// Build a metadata executor backed by the provided engine reference.
    pub fn new(engine: Arc<M>) -> Self {
        Self {
            backend: SyncMetaEngineAdapter::new(engine),
        }
    }

    /// Return the synchronous metadata view from the adapter.
    pub fn sync_view(&self) -> Arc<M> {
        self.backend.sync_engine()
    }

    /// Execute a metadata operation synchronously on the current thread.
    pub fn run_sync<T>(&self, operation: impl FnOnce(Arc<M>) -> MetaResult<T>) -> VfsResult<T> {
        operation(self.sync_view()).map_err(VfsError::Meta)
    }

    /// Execute a metadata operation on the sync adapter from async context.
    pub async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Arc<M>) -> MetaResult<T> + Send + 'static,
    ) -> VfsResult<T> {
        execute_sync_meta(self.backend.clone(), operation)
            .await
            .map_err(VfsError::Join)?
            .map_err(VfsError::Meta)
    }
}

/// Result state for operations raced against timeout and cancellation.
pub enum TimeoutOrCancel<T> {
    /// Operation completed successfully with a value.
    Completed(T),
    /// Operation exceeded the configured timeout.
    TimedOut,
    /// Operation was cancelled before completion.
    Cancelled,
}

/// Single execution model for local cache I/O.
///
/// Local cache operations run inline on the active runtime worker after the
/// bridge boundary (WinFSP `spawn_wait` / FUSE `block_on`) has entered Tokio.
/// This avoids per-operation task handoffs for hot cache calls, while
/// heavyweight blocking calls (for example `sync_data`) stay isolated on the
/// blocking pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalCacheIoExecutionModel {
    /// Execute inline on the runtime worker, isolating blocking I/O where needed.
    InlineRuntimeWorkerWithBlockingIsolation,
}

impl LocalCacheIoExecutionModel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InlineRuntimeWorkerWithBlockingIsolation => {
                "inline-runtime-worker-with-blocking-isolation"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LocalCacheIoTiming {
    /// Nanoseconds spent in handoff/dispatch before backend IO starts.
    pub handoff_ns: u64,
    /// Nanoseconds spent performing backend cache IO itself.
    pub backend_io_ns: u64,
}

pub const fn cache_io_execution_model() -> LocalCacheIoExecutionModel {
    LocalCacheIoExecutionModel::InlineRuntimeWorkerWithBlockingIsolation
}

/// The closure must not be called from Tokio's `spawn_blocking` pool. Such
/// callers must use [`run_local_cache_io_blocking`] instead; Tokio does not
/// expose a reliable runtime API for distinguishing a blocking-pool thread
/// from a worker thread before calling `block_in_place`.
pub fn run_local_cache_io<T>(operation: impl FnOnce() -> T) -> (T, LocalCacheIoTiming) {
    let io_started = std::time::Instant::now();
    let result = if tokio::runtime::Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
    {
        // Foreground payload reads borrow the caller's output buffer and cannot
        // move to spawn_blocking. Mark the synchronous pread as blocking so
        // Tokio can replace this worker instead of stalling unrelated FUSE reads.
        task::block_in_place(operation)
    } else {
        operation()
    };
    let backend_io_ns = duration_to_nanos_u64(io_started.elapsed());
    (
        result,
        LocalCacheIoTiming {
            handoff_ns: 0,
            backend_io_ns,
        },
    )
}

/// Run a potentially blocking closure on Tokio's dedicated blocking pool and
/// propagate `JoinError` as `VfsError::Join`.
pub async fn run_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> VfsResult<T> + Send + 'static,
) -> VfsResult<T> {
    task::spawn_blocking(operation)
        .await
        .map_err(VfsError::Join)?
}

/// Run local cache I/O on the blocking pool when the operation is
/// unavoidably blocking (for example fsync/maintenance), while still exposing
/// handoff-vs-IO timing split for observability.
pub async fn run_local_cache_io_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> VfsResult<T> + Send + 'static,
) -> VfsResult<(T, LocalCacheIoTiming)> {
    let submitted_at = std::time::Instant::now();
    run_blocking(move || {
        let io_started_at = std::time::Instant::now();
        let handoff_ns = io_started_at
            .checked_duration_since(submitted_at)
            .unwrap_or_default()
            .into_nanos_saturating_u64();
        let result = operation();
        let backend_io_ns = io_started_at.elapsed().into_nanos_saturating_u64();
        result.map(|value| {
            (
                value,
                LocalCacheIoTiming {
                    handoff_ns,
                    backend_io_ns,
                },
            )
        })
    })
    .await
}

trait DurationNanosExt {
    fn into_nanos_saturating_u64(self) -> u64;
}

impl DurationNanosExt for std::time::Duration {
    fn into_nanos_saturating_u64(self) -> u64 {
        u64::try_from(self.as_nanos()).unwrap_or(u64::MAX)
    }
}

fn duration_to_nanos_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Run a metadata closure on the blocking pool.
pub async fn run_meta<M: ?Sized + Send + Sync + 'static, T: Send + 'static>(
    engine: MetaExecutor<M>,
    operation: impl FnOnce(Arc<M>) -> MetaResult<T> + Send + 'static,
) -> VfsResult<T> {
    engine.run(operation).await
}

/// Race an async operation against timeout and cancellation.
pub async fn run_with_timeout_or_cancel<T>(
    cancel_token: &CancellationToken,
    timeout: Duration,
    operation: impl std::future::Future<Output = T>,
) -> TimeoutOrCancel<T> {
    tokio::select! {
        _ = cancel_token.cancelled() => TimeoutOrCancel::Cancelled,
        result = tokio::time::timeout(timeout, operation) => {
            match result {
                Ok(value) => TimeoutOrCancel::Completed(value),
                Err(_) => TimeoutOrCancel::TimedOut,
            }
        }
    }
}

/// Sleep unless cancellation is requested first.
pub async fn sleep_or_cancel(cancel_token: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        _ = cancel_token.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::{MetaExecutor, cache_io_execution_model, into_vfs_meta, run_local_cache_io};
    use std::sync::Arc;
    use std::time::Duration;
    use studiofs_meta::{FormatInfo, MetaError, MetaFormat, RedbMetaEngine};

    #[test]
    fn cache_io_execution_model_is_inline_runtime_worker() {
        assert_eq!(
            cache_io_execution_model().as_str(),
            "inline-runtime-worker-with-blocking-isolation"
        );
    }

    #[test]
    fn run_local_cache_io_reports_zero_handoff() {
        let (value, timing) = run_local_cache_io(|| 42usize);
        assert_eq!(value, 42);
        assert_eq!(timing.handoff_ns, 0);
    }

    #[test]
    fn run_local_cache_io_does_not_starve_single_runtime_worker() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .build()
            .expect("build single-worker runtime");

        runtime.block_on(async {
            let slow_read = tokio::spawn(async {
                run_local_cache_io(|| std::thread::sleep(Duration::from_millis(150)));
            });
            let (timer_tx, timer_rx) = std::sync::mpsc::channel();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                timer_tx.send(()).expect("test receiver remains live");
            });

            timer_rx
                .recv_timeout(Duration::from_millis(75))
                .expect("local cache I/O must not starve unrelated runtime work");
            slow_read.await.expect("slow local-cache task should join");
        });
    }

    #[test]
    fn meta_executor_run_sync_reads_from_narrow_sync_engine() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let path = temp.path().join("meta.redb");
        let engine = Arc::new(RedbMetaEngine::open(&path).expect("open redb"));
        let info = FormatInfo::new(
            "runtime-meta".to_string(),
            "runtime-meta-uuid".to_string(),
            studiofs_meta::FormatInfoDataStore {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "runtime-meta-bucket".to_string(),
                access_key: "runtime-access-key".to_string(),
                secret_key: "runtime-secret-key".to_string(),
                region: String::new(),
            },
            16,
        )
        .expect("generate format info");
        engine.set_format_info(&info).expect("set format info");

        let narrow: Arc<dyn MetaFormat> = engine;
        let executor = MetaExecutor::new(narrow);
        let observed = executor
            .run_sync(|meta| meta.get_format_info().map(|format| format.name))
            .expect("sync metadata call");

        assert_eq!(observed, "runtime-meta");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn meta_executor_run_maps_meta_errors() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let path = temp.path().join("meta.redb");
        let engine = Arc::new(RedbMetaEngine::open(&path).expect("open redb"));
        let info = FormatInfo::new(
            "runtime-meta".to_string(),
            "runtime-meta-uuid".to_string(),
            studiofs_meta::FormatInfoDataStore {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "runtime-meta-bucket".to_string(),
                access_key: "runtime-access-key".to_string(),
                secret_key: "runtime-secret-key".to_string(),
                region: String::new(),
            },
            16,
        )
        .expect("generate format info");
        engine.set_format_info(&info).expect("set format info");

        let executor = MetaExecutor::new(into_vfs_meta(engine));
        let err = executor
            .run::<()>(|_| Err(MetaError::Storage("boom".to_string())))
            .await
            .expect_err("metadata error must bubble");

        match err {
            super::super::VfsError::Meta(MetaError::Storage(message)) => {
                assert_eq!(message, "boom")
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
