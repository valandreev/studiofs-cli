use super::*;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use studiofs_meta::InodeLifecycleState;

use crate::runtime::{MetaExecutor, run_meta};
use crate::{VfsError, VfsMeta, VfsResult};

pub(super) struct CommitCoordinator;

pub(super) enum EpochFinalizeStatus {
    Published,
    WaitingForUploads,
    Deferred,
    Skipped,
}

struct FinalizeMetaSnapshot {
    lifecycle: Option<studiofs_meta::InodeLifecycleRecord>,
    has_persisted_overlay: bool,
    object_ids: Vec<u64>,
}

const EPOCH_PUBLICATION_BATCH_MAX_OPERATIONS: usize = 64;
const EPOCH_PUBLICATION_COALESCE_DELAY: Duration = Duration::from_millis(10);

type EpochPublicationCompletion = tokio::sync::oneshot::Sender<Result<bool, Arc<VfsError>>>;

struct EpochPublicationRequest {
    ino: studiofs_meta::Ino,
    content_epoch: u64,
    completion: EpochPublicationCompletion,
}

#[derive(Default)]
pub(super) struct EpochPublicationBatcher {
    inner: parking_lot::Mutex<EpochPublicationBatchInner>,
}

#[derive(Default)]
struct EpochPublicationBatchInner {
    queue: VecDeque<EpochPublicationRequest>,
    draining: bool,
}

impl EpochPublicationBatcher {
    async fn publish(
        self: &Arc<Self>,
        meta: MetaExecutor<dyn VfsMeta>,
        ino: studiofs_meta::Ino,
        content_epoch: u64,
    ) -> VfsResult<bool> {
        let (completion, receiver) = tokio::sync::oneshot::channel();
        let should_spawn = {
            let mut inner = self.inner.lock();
            inner.queue.push_back(EpochPublicationRequest {
                ino,
                content_epoch,
                completion,
            });
            if inner.draining {
                false
            } else {
                inner.draining = true;
                true
            }
        };
        if should_spawn {
            let batcher = Arc::clone(self);
            tokio::spawn(async move {
                batcher.drain(meta).await;
            });
        }

        match receiver.await {
            Ok(Ok(published)) => Ok(published),
            Ok(Err(error)) => Err(VfsError::Other(error.to_string())),
            Err(_) => Err(VfsError::Other(
                "epoch publication batcher stopped".to_string(),
            )),
        }
    }

    async fn drain(self: Arc<Self>, meta: MetaExecutor<dyn VfsMeta>) {
        tokio::time::sleep(EPOCH_PUBLICATION_COALESCE_DELAY).await;
        loop {
            let requests = {
                let mut inner = self.inner.lock();
                let take = EPOCH_PUBLICATION_BATCH_MAX_OPERATIONS.min(inner.queue.len());
                let requests: Vec<_> = inner.queue.drain(..take).collect();
                if requests.is_empty() {
                    inner.draining = false;
                    return;
                }
                requests
            };
            let keys: Vec<_> = requests
                .iter()
                .map(|request| (request.ino, request.content_epoch))
                .collect();
            let result = run_meta(meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                engine.publish_regular_file_overlay_to_base_if_matches_batch(&keys)
            })
            .await;
            match result {
                Ok(published) if published.len() == requests.len() => {
                    for (request, published) in requests.into_iter().zip(published) {
                        let _ = request.completion.send(Ok(published));
                    }
                }
                Ok(_) => {
                    let error = Arc::new(VfsError::Other(
                        "epoch publication batch returned an unexpected result count".to_string(),
                    ));
                    for request in requests {
                        let _ = request.completion.send(Err(Arc::clone(&error)));
                    }
                }
                Err(error) => {
                    let error = Arc::new(error);
                    for request in requests {
                        let _ = request.completion.send(Err(Arc::clone(&error)));
                    }
                }
            }
        }
    }
}

impl CommitCoordinator {
    #[cfg(test)]
    pub(super) async fn finalize_inode_epoch(
        vfs: &StudioVfs,
        ino: Ino,
        content_epoch: u64,
    ) -> VfsResult<EpochFinalizeStatus> {
        Self::finalize_inode_epoch_inner(vfs, ino, content_epoch, None, false).await
    }

    pub(super) async fn finalize_inode_epoch_with_publisher(
        vfs: &StudioVfs,
        ino: studiofs_meta::Ino,
        content_epoch: u64,
        publisher: Arc<EpochPublicationBatcher>,
        remote_visibility_confirmed: bool,
    ) -> VfsResult<EpochFinalizeStatus> {
        Self::finalize_inode_epoch_inner(
            vfs,
            ino,
            content_epoch,
            Some(publisher),
            remote_visibility_confirmed,
        )
        .await
    }

    async fn finalize_inode_epoch_inner(
        vfs: &StudioVfs,
        ino: studiofs_meta::Ino,
        content_epoch: u64,
        publisher: Option<Arc<EpochPublicationBatcher>>,
        remote_visibility_confirmed: bool,
    ) -> VfsResult<EpochFinalizeStatus> {
        let _commit_qos = vfs
            .qos
            .acquire_commit()
            .await
            .map_err(|e| VfsError::Other(format!("commit qos permit failed: {e}")))?;

        let attr = match run_meta(vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.get_attr(ino)
        })
        .await
        {
            Ok(attr) => attr,
            Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                vfs.clear_inode_incarnation(ino);
                return Ok(EpochFinalizeStatus::Skipped);
            }
            Err(err) => return Err(err),
        };
        if attr.content_epoch != content_epoch
            || attr.upload_state == MetaUploadState::Ready
            || !StudioVfs::needs_epoch_finalization(&attr)
        {
            return Ok(EpochFinalizeStatus::Skipped);
        }
        if attr.size == 0 && attr.committed_size > 0 && vfs.handle_table.open_count(ino) > 0 {
            return Ok(EpochFinalizeStatus::Deferred);
        }

        if vfs.inode_has_pending_uploads(ino) {
            tracing::debug!(
                ino,
                content_epoch,
                "waiting for pending uploads before epoch publication"
            );
            return Ok(EpochFinalizeStatus::WaitingForUploads);
        }
        if vfs.inode_has_unmaterialized_overlay_for_finalize(ino) {
            return Ok(EpochFinalizeStatus::Deferred);
        }
        let meta = run_meta(vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            let has_persisted_overlay = engine.has_local_regular_file_overlay(ino)?;
            let lifecycle = engine.get_inode_lifecycle(ino)?;
            let object_ids = if has_persisted_overlay {
                engine.get_regular_file_overlay_object_ids(ino)?
            } else {
                Vec::new()
            };
            Ok(FinalizeMetaSnapshot {
                lifecycle,
                has_persisted_overlay,
                object_ids,
            })
        })
        .await?;
        let Some(lifecycle) = meta.lifecycle else {
            tracing::debug!(
                ino,
                content_epoch,
                "deferring epoch publication until durable lifecycle bookkeeping is available"
            );
            return Ok(EpochFinalizeStatus::Deferred);
        };
        if lifecycle.local_content_epoch != content_epoch {
            tracing::debug!(
                ino,
                queued_content_epoch = content_epoch,
                lifecycle_content_epoch = lifecycle.local_content_epoch,
                lifecycle_state = ?lifecycle.state,
                "skipping stale epoch publication attempt after lifecycle refresh"
            );
            return Ok(EpochFinalizeStatus::Skipped);
        }
        if lifecycle.state != InodeLifecycleState::PublishReady {
            tracing::debug!(
                ino,
                content_epoch,
                lifecycle_state = ?lifecycle.state,
                "deferring epoch publication until durable lifecycle reaches publish-ready"
            );
            return Ok(EpochFinalizeStatus::Deferred);
        }

        if !remote_visibility_confirmed
            && let Err(err) = Self::wait_until_remote_objects_visible(vfs, &meta.object_ids).await
        {
            tracing::debug!(
                ino,
                content_epoch,
                error = %err,
                "deferring overlay publication until uploaded objects become remotely visible"
            );
            return Ok(EpochFinalizeStatus::Deferred);
        }
        let finalize_session = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Finalize)
            .await;
        let latest_attr = match run_meta(vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.get_attr(ino)
        })
        .await
        {
            Ok(attr) => attr,
            Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                vfs.clear_inode_incarnation(ino);
                drop(finalize_session);
                vfs.state.file_session.gc_if_idle(ino);
                return Ok(EpochFinalizeStatus::Skipped);
            }
            Err(err) => {
                drop(finalize_session);
                vfs.state.file_session.gc_if_idle(ino);
                return Err(err);
            }
        };
        if latest_attr.content_epoch != content_epoch
            || latest_attr.upload_state == MetaUploadState::Ready
            || !StudioVfs::needs_epoch_finalization(&latest_attr)
        {
            tracing::debug!(
                ino,
                queued_content_epoch = content_epoch,
                latest_content_epoch = latest_attr.content_epoch,
                upload_state = ?latest_attr.upload_state,
                "skipping stale epoch publication attempt after finalization gate"
            );
            drop(finalize_session);
            vfs.state.file_session.gc_if_idle(ino);
            return Ok(EpochFinalizeStatus::Skipped);
        }
        if latest_attr.size == 0
            && latest_attr.committed_size > 0
            && vfs.handle_table.open_count(ino) > 0
        {
            drop(finalize_session);
            vfs.state.file_session.gc_if_idle(ino);
            return Ok(EpochFinalizeStatus::Deferred);
        }
        if vfs.inode_has_pending_uploads(ino) {
            drop(finalize_session);
            vfs.state.file_session.gc_if_idle(ino);
            tracing::debug!(
                ino,
                content_epoch,
                "waiting for pending uploads before epoch publication after finalization gate"
            );
            return Ok(EpochFinalizeStatus::WaitingForUploads);
        }
        if vfs.inode_has_unmaterialized_overlay_for_finalize(ino) {
            drop(finalize_session);
            vfs.state.file_session.gc_if_idle(ino);
            return Ok(EpochFinalizeStatus::Deferred);
        }
        let publication_changes_visible_size = latest_attr.committed_size != latest_attr.size;
        let published = match if let Some(publisher) = publisher {
            publisher
                .publish(vfs.meta.clone(), ino, content_epoch)
                .await
        } else {
            run_meta(vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                engine.publish_regular_file_overlay_to_base_if_matches(ino, content_epoch)
            })
            .await
        } {
            Ok(published) => published,
            Err(err) => {
                drop(finalize_session);
                vfs.state.file_session.gc_if_idle(ino);
                return Err(err);
            }
        };
        if !published {
            drop(finalize_session);
            vfs.state.file_session.gc_if_idle(ino);
            let latest_attr = match run_meta(vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                engine.get_attr(ino)
            })
            .await
            {
                Ok(attr) => attr,
                Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                    vfs.clear_inode_incarnation(ino);
                    return Ok(EpochFinalizeStatus::Skipped);
                }
                Err(err) => return Err(err),
            };
            if latest_attr.content_epoch != content_epoch
                || latest_attr.upload_state == MetaUploadState::Ready
                || !StudioVfs::needs_epoch_finalization(&latest_attr)
            {
                tracing::debug!(
                    ino,
                    queued_content_epoch = content_epoch,
                    latest_content_epoch = latest_attr.content_epoch,
                    upload_state = ?latest_attr.upload_state,
                    "skipping stale epoch publication attempt"
                );
                return Ok(EpochFinalizeStatus::Skipped);
            }
            return Ok(EpochFinalizeStatus::Deferred);
        }

        let had_live_extents_for_epoch = vfs
            .live_extent_records_for_inode(ino)
            .iter()
            .any(|(_, _, epoch)| *epoch == content_epoch);
        vfs.remove_live_extents_for_inode_epoch(ino, content_epoch);

        // Clear any stale dirty_attr that was snapshotted before this epoch was
        // published. dirty_attrs entries captured at write time carry
        // committed_content_epoch=0 / committed_size=0 because the file had not
        // been published yet. After publication those values are stale: if a
        // subsequent flush_local_dirty_state finds this dirty_attr and the local
        // dirty state is empty it will call engine.set_attr with
        // committed_epoch=0, reverting INODES_TABLE back to {Uploading}. That
        // triggers a second spurious finalize_inode_epoch and emits a garbage
        // published-base event to PG, corrupting remote nodes' regular-file base
        // graph.
        //
        // Only remove the entry when content_epoch matches to avoid dropping a
        // dirty_attr that was written by a concurrent new write or setattr after
        // this publication completed (those will carry a different epoch). The
        // finalize session serializes publish+cleanup for this inode so a stale
        // older finalizer cannot restore a dirty_attr after a newer finalizer has
        // already published and removed it.
        let (was_dirty_visible, removed_matching_dirty_attr, newer_dirty_attr) = {
            let mut dirty_attrs = vfs.state.namespace.dirty_attrs.lock();
            let present = dirty_attrs.contains_key(&ino);
            let newer = dirty_attrs
                .get(&ino)
                .filter(|attr| attr.content_epoch > content_epoch)
                .cloned();
            let removed_matching = dirty_attrs
                .get(&ino)
                .is_some_and(|attr| attr.content_epoch == content_epoch);
            if removed_matching {
                dirty_attrs.remove(&ino);
            }
            (present, removed_matching, newer)
        };

        let published_attr = if let Ok(attr) =
            run_meta(vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                engine.get_attr(ino)
            })
            .await
        {
            vfs.refresh_inode_incarnation_state_from_attr_inner(ino, &attr, false);
            Some(attr)
        } else {
            vfs.clear_inode_incarnation(ino);
            None
        };
        let restored_newer_dirty_attr = newer_dirty_attr.is_some();
        if let Some(newer_dirty_attr) = newer_dirty_attr {
            Self::restore_newer_dirty_attr_and_refresh_open_handles(vfs, ino, newer_dirty_attr);
        } else if let Some(published_attr) = published_attr.as_ref() {
            vfs.handle_table
                .update_open_attrs_for_inode(ino, published_attr);
        }
        let removed_cached_attr = !vfs.invalidate_attr_read_cache([ino]).is_empty();
        // `getattr` can expose pending attrs from dirty_attrs without populating
        // attr_read_cache. A newer dirty attr must be preserved, but its presence
        // still means the kernel may hold stale attrs for the epoch just published.
        let local_matching_dirty_publication = (removed_matching_dirty_attr
            || had_live_extents_for_epoch)
            && !restored_newer_dirty_attr;
        let publication_shrinks_visible_size = latest_attr.committed_size > latest_attr.size;
        let should_invalidate_kernel_data = if local_matching_dirty_publication {
            publication_shrinks_visible_size || meta.has_persisted_overlay
        } else {
            was_dirty_visible
                || removed_cached_attr
                || publication_changes_visible_size
                || meta.has_persisted_overlay
        };
        let should_invalidate_kernel_attrs = was_dirty_visible
            || removed_cached_attr
            || publication_changes_visible_size
            || meta.has_persisted_overlay;
        drop(finalize_session);
        vfs.state.file_session.gc_if_idle(ino);
        if should_invalidate_kernel_data {
            vfs.invalidate_kernel_inodes([ino]);
        } else if should_invalidate_kernel_attrs {
            vfs.invalidate_kernel_inode_attrs([ino]);
        }

        Ok(EpochFinalizeStatus::Published)
    }

    pub(super) fn restore_newer_dirty_attr_and_refresh_open_handles(
        vfs: &StudioVfs,
        ino: Ino,
        newer_dirty_attr: InodeAttr,
    ) {
        let mut dirty_attrs = vfs.state.namespace.dirty_attrs.lock();
        // EOF is monotonic within one unpublished content epoch; a smaller
        // same-epoch snapshot is stale and must not become canonical again.
        let should_restore = dirty_attrs.get(&ino).is_none_or(|current| {
            current.content_epoch < newer_dirty_attr.content_epoch
                || (current.content_epoch == newer_dirty_attr.content_epoch
                    && current.size < newer_dirty_attr.size)
        });
        let open_attr = if should_restore {
            dirty_attrs.insert(ino, newer_dirty_attr.clone());
            newer_dirty_attr
        } else {
            dirty_attrs.get(&ino).cloned().unwrap_or(newer_dirty_attr)
        };
        drop(dirty_attrs);
        vfs.handle_table
            .update_open_attrs_for_inode(ino, &open_attr);
    }

    async fn wait_until_remote_objects_visible(
        vfs: &StudioVfs,
        object_ids: &[u64],
    ) -> VfsResult<()> {
        if object_ids.is_empty() {
            return Ok(());
        }
        let visibility_timeout = vfs.publish_visibility_timeout;
        let deadline = tokio::time::Instant::now() + visibility_timeout;
        loop {
            let mut all_visible = true;
            for object_id in object_ids {
                let checks = format_object_fragment_keys(&vfs.data_prefix, *object_id)
                    .into_iter()
                    .map(|key| async {
                        let result = vfs.fetch_scheduler.data_store().exists(&key).await;
                        (key, result)
                    });
                let results = futures::future::join_all(checks).await;
                for (key, result) in results {
                    match result {
                        Ok(true) => {}
                        Ok(false) => {
                            all_visible = false;
                            break;
                        }
                        Err(err) => {
                            all_visible = false;
                            tracing::debug!(
                                object_id = *object_id,
                                key,
                                error = %err,
                                "publish visibility check failed, will retry"
                            );
                            break;
                        }
                    }
                }
                if !all_visible {
                    break;
                }
            }

            if all_visible {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(VfsError::Other(format!(
                    "timed out after {:?} waiting for uploaded extents to become remotely visible",
                    visibility_timeout
                )));
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
