use super::*;

pub(super) struct CommitCoordinator;

pub(super) enum EpochFinalizeStatus {
    Published,
    Deferred,
    Skipped,
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

        let attr = match run_meta(
            vfs.meta_async.clone(),
            move |engine: Arc<dyn MetaEngine>| engine.get_attr(ino),
        )
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

        let pending_same_epoch = vfs
            .block_cache
            .snapshot_pending_uploads()
            .into_iter()
            .any(|pending| pending.ino == ino && pending.content_epoch == content_epoch);
        if pending_same_epoch {
            return Ok(EpochFinalizeStatus::Deferred);
        }

        let has_persisted_overlay = run_meta(
            vfs.meta_async.clone(),
            move |engine: Arc<dyn MetaEngine>| engine.has_local_regular_file_overlay(ino),
        )
        .await?;
        let published = if has_persisted_overlay {
            let overlay_object_ids = run_meta(
                vfs.meta_async.clone(),
                move |engine: Arc<dyn MetaEngine>| engine.get_regular_file_overlay_object_ids(ino),
            )
            .await?;
            if !overlay_object_ids.is_empty() {
                let extents_to_verify: Vec<(u32, ExtentRecordId)> = overlay_object_ids
                    .iter()
                    .enumerate()
                    .map(|(idx, object_id)| {
                        (
                            idx as u32,
                            ExtentRecordId {
                                object_id: *object_id,
                                content_epoch,
                            },
                        )
                    })
                    .collect();
                Self::wait_until_remote_objects_visible(vfs, &extents_to_verify).await?;
            }
            run_meta(
                vfs.meta_async.clone(),
                move |engine: Arc<dyn MetaEngine>| {
                    engine.publish_regular_file_overlay_to_base_if_matches(ino, content_epoch)
                },
            )
            .await?
        } else {
            let required_blocks = run_meta(
                vfs.meta_async.clone(),
                move |engine: Arc<dyn MetaEngine>| engine.get_file_blocks(ino),
            )
            .await?;
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
            run_meta(
                vfs.meta_async.clone(),
                move |engine: Arc<dyn MetaEngine>| {
                    engine.publish_file_manifest_if_matches(
                        ino,
                        content_epoch,
                        expected_manifest.as_slice(),
                    )
                },
            )
            .await?
        };
        if !published {
            let latest_attr = match run_meta(
                vfs.meta_async.clone(),
                move |engine: Arc<dyn MetaEngine>| engine.get_attr(ino),
            )
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
        if let Ok(attr) = run_meta(
            vfs.meta_async.clone(),
            move |engine: Arc<dyn MetaEngine>| engine.get_attr(ino),
        )
        .await
        {
            vfs.refresh_inode_incarnation_state_from_attr(ino, &attr);
        } else {
            vfs.clear_inode_incarnation(ino);
        }

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
}

async fn requeue_missing_manifest_uploads(
    vfs: &StudioVfs,
    ino: Ino,
    content_epoch: u64,
    required_blocks: &[(u32, BlockRef)],
) {
    let pending = vfs.block_cache.snapshot_pending_uploads();
    let pending_keys: std::collections::HashSet<(u64, u64)> = pending
        .into_iter()
        .filter(|op| op.ino == ino && op.content_epoch == content_epoch)
        .map(|op| (op.offset, op.object_id))
        .collect();

    for (block_index, block_ref) in required_blocks {
        let offset = (*block_index as u64).saturating_mul(vfs.block_size as u64);
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
        vfs.block_cache
            .submit_upload(ino, offset, block_ref.object_id, content_epoch);
    }
}
