use super::*;
use studiofs_cache::local::LocalBlockLifecycleState;
use studiofs_data::object::format_object_fragment_keys;
use studiofs_meta::InodeLifecycleState;

pub(super) struct CommitCoordinator;

pub(super) enum EpochFinalizeStatus {
    Published,
    Deferred,
    Skipped,
}

enum FinalizeExtentPlan {
    Overlay { object_ids: Vec<u64> },
    Manifest { blocks: Vec<(u32, BlockRef)> },
}

struct FinalizeMetaSnapshot {
    lifecycle: Option<studiofs_meta::InodeLifecycleRecord>,
    extent_plan: FinalizeExtentPlan,
}

impl CommitCoordinator {
    pub(super) async fn finalize_inode_epoch(
        vfs: &StudioVfs,
        ino: Ino,
        content_epoch: u64,
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

        if vfs.inode_has_unmaterialized_overlay_for_finalize(ino) {
            return Ok(EpochFinalizeStatus::Deferred);
        }

        let meta = run_meta(vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            let has_persisted_overlay = engine.has_local_regular_file_overlay(ino)?;
            let lifecycle = engine.get_inode_lifecycle(ino)?;
            let extent_plan = if has_persisted_overlay {
                FinalizeExtentPlan::Overlay {
                    object_ids: engine.get_regular_file_overlay_object_ids(ino)?,
                }
            } else {
                FinalizeExtentPlan::Manifest {
                    blocks: engine.get_file_blocks(ino)?,
                }
            };
            Ok(FinalizeMetaSnapshot {
                lifecycle,
                extent_plan,
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
            let legacy_publishable = Self::legacy_epoch_can_publish_without_publish_ready(
                vfs,
                ino,
                content_epoch,
                &meta.extent_plan,
            )
            .await?;
            if legacy_publishable {
                tracing::debug!(
                    ino,
                    content_epoch,
                    lifecycle_state = ?lifecycle.state,
                    "finalizing legacy epoch without durable publish-ready marker"
                );
            } else {
                tracing::debug!(
                    ino,
                    content_epoch,
                    lifecycle_state = ?lifecycle.state,
                    "deferring epoch publication until durable lifecycle reaches publish-ready"
                );
                return Ok(EpochFinalizeStatus::Deferred);
            }
        }

        let published = if let FinalizeExtentPlan::Overlay {
            object_ids: overlay_object_ids,
        } = &meta.extent_plan
        {
            if !overlay_object_ids.is_empty() {
                let extents_to_verify: Vec<(u32, ExtentRecordId)> = overlay_object_ids
                    .iter()
                    .enumerate()
                    .map(|(idx, object_id)| {
                        (
                            u32::try_from(idx).unwrap_or(u32::MAX),
                            ExtentRecordId {
                                object_id: *object_id,
                                content_epoch,
                            },
                        )
                    })
                    .collect();
                Self::wait_until_remote_objects_visible(vfs, &extents_to_verify).await?;
            }
            run_meta(vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                engine.publish_regular_file_overlay_to_base_if_matches(ino, content_epoch)
            })
            .await?
        } else {
            let required_blocks = match &meta.extent_plan {
                FinalizeExtentPlan::Manifest { blocks } => blocks.clone(),
                FinalizeExtentPlan::Overlay { .. } => unreachable!("overlay path handled above"),
            };
            if !required_blocks.is_empty() {
                let extents_to_verify: Vec<(u32, ExtentRecordId)> = required_blocks
                    .iter()
                    .map(|(block_index, block_ref)| {
                        (
                            *block_index,
                            ExtentRecordId {
                                object_id: block_ref.object_id,
                                content_epoch,
                            },
                        )
                    })
                    .collect();
                if let Err(err) =
                    Self::wait_until_remote_objects_visible(vfs, &extents_to_verify).await
                {
                    requeue_missing_manifest_uploads(vfs, ino, content_epoch, &required_blocks)
                        .await;
                    return Err(err);
                }
            }

            let expected_manifest = required_blocks.clone();
            run_meta(vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                engine.publish_file_manifest_if_matches(
                    ino,
                    content_epoch,
                    expected_manifest.as_slice(),
                )
            })
            .await?
        };
        if !published {
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

        vfs.remove_live_extents_for_inode_epoch(ino, content_epoch);

        // Clear any stale dirty_attr that was snapshotted before this epoch was
        // published. dirty_attrs entries captured at write time carry
        // committed_content_epoch=0 / committed_size=0 because the file had not
        // been published yet. After publication those values are stale: if a
        // subsequent flush_ordered finds this dirty_attr and the write buffer is
        // empty it will call engine.set_attr with committed_epoch=0, reverting
        // INODES_TABLE back to {Uploading}. That triggers a second spurious
        // finalize_inode_epoch which (a) emits a garbage
        // PublishFileManifest{blocks:[]} event to PG — corrupting remote nodes'
        // COMMITTED_FILE_BLOCKS_TABLE — and (b) fires invalidate_kernel_inodes a
        // second time, sending a file-change notification that can cause readers
        // (e.g. media players) to abort an in-progress read.
        //
        // Only remove the entry when content_epoch matches to avoid dropping a
        // dirty_attr that was written by a concurrent new write or setattr after
        // this publication completed (those will carry a different epoch).
        {
            let mut dirty_attrs = vfs.state.namespace.dirty_attrs.lock();
            if dirty_attrs
                .get(&ino)
                .is_some_and(|a| a.content_epoch == content_epoch)
            {
                dirty_attrs.remove(&ino);
            }
        }

        if let Ok(attr) = run_meta(vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.get_attr(ino)
        })
        .await
        {
            vfs.refresh_inode_incarnation_state_from_attr(ino, &attr);
        } else {
            vfs.clear_inode_incarnation(ino);
        }
        vfs.invalidate_attr_read_cache([ino]);
        vfs.invalidate_kernel_inodes([ino]);

        Ok(EpochFinalizeStatus::Published)
    }

    async fn wait_until_remote_objects_visible(
        vfs: &StudioVfs,
        live_extents: &[(u32, ExtentRecordId)],
    ) -> VfsResult<()> {
        let visibility_timeout = vfs.publish_visibility_timeout;
        let deadline = tokio::time::Instant::now() + visibility_timeout;
        loop {
            let mut all_visible = true;
            for (_, extent) in live_extents {
                let checks = format_object_fragment_keys(&vfs.data_prefix, extent.object_id)
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
                                object_id = extent.object_id,
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

    async fn legacy_epoch_can_publish_without_publish_ready(
        vfs: &StudioVfs,
        ino: Ino,
        content_epoch: u64,
        extent_plan: &FinalizeExtentPlan,
    ) -> VfsResult<bool> {
        let pending_same_epoch = vfs
            .cached_store()
            .snapshot_pending_uploads()
            .into_iter()
            .any(|pending| pending.ino == ino && pending.content_epoch == content_epoch);
        if pending_same_epoch {
            return Ok(false);
        }

        let required_object_ids: Vec<u64> = match extent_plan {
            FinalizeExtentPlan::Overlay { object_ids } => object_ids.clone(),
            FinalizeExtentPlan::Manifest { blocks } => blocks
                .iter()
                .map(|(_, block_ref)| block_ref.object_id)
                .collect(),
        };

        Ok(required_object_ids.into_iter().all(|object_id| {
            !matches!(
                vfs.local_block_backend().lifecycle_state(object_id),
                LocalBlockLifecycleState::Dirty | LocalBlockLifecycleState::Uploading
            )
        }))
    }
}

async fn requeue_missing_manifest_uploads(
    vfs: &StudioVfs,
    ino: Ino,
    content_epoch: u64,
    required_blocks: &[(u32, BlockRef)],
) {
    let pending = vfs.cached_store().snapshot_pending_uploads();
    let pending_keys: std::collections::HashSet<(u64, u64)> = pending
        .into_iter()
        .filter(|op| op.ino == ino && op.content_epoch == content_epoch)
        .map(|op| (op.offset, op.object_id))
        .collect();

    for (block_index, block_ref) in required_blocks {
        let offset = u64::from(*block_index).saturating_mul(u64::from(vfs.block_size));
        if pending_keys.contains(&(offset, block_ref.object_id)) {
            continue;
        }

        let checks = format_object_fragment_keys(&vfs.data_prefix, block_ref.object_id)
            .into_iter()
            .map(|key| async move {
                tokio::time::timeout(
                    Duration::from_secs(3),
                    vfs.fetch_scheduler.data_store().exists(&key),
                )
                .await
            });
        let visible = futures::future::join_all(checks)
            .await
            .into_iter()
            .all(|result| matches!(result, Ok(Ok(true))));
        if visible {
            continue;
        }

        tracing::warn!(
            ino,
            block_index,
            object_id = block_ref.object_id,
            content_epoch,
            "commit reconcile: requeueing missing manifest upload"
        );
        vfs.cached_store()
            .submit_upload(ino, offset, block_ref.object_id, content_epoch);
    }
}
