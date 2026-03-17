use super::*;

pub(super) struct CommitCoordinator;

impl CommitCoordinator {
    pub(super) async fn finish_extent_upload(
        vfs: &StudioVfs,
        op: &PendingUpload,
        block_index: u32,
    ) -> VfsResult<()> {
        let _commit_qos = vfs
            .qos
            .acquire_commit()
            .await
            .map_err(|e| VfsError::Other(format!("commit qos permit failed: {e}")))?;
        vfs.record_uploaded_extent(op.ino, block_index, op.object_id, op.content_epoch);

        let pending_same_epoch =
            vfs.block_cache
                .snapshot_pending_uploads()
                .into_iter()
                .any(|pending| {
                    pending.ino == op.ino
                        && pending.content_epoch == op.content_epoch
                        && pending.object_id != op.object_id
                });
        if pending_same_epoch {
            return Ok(());
        }

        let ino = op.ino;
        let required_blocks = run_meta(vfs.meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.get_file_blocks(ino)
        })
        .await?;
        if required_blocks.is_empty() {
            return Ok(());
        }

        let extents_to_verify: Vec<(u32, ExtentRecordId)> = required_blocks
            .iter()
            .map(|(block_index, block_ref)| {
                (
                    *block_index,
                    ExtentRecordId {
                        object_id: block_ref.object_id,
                        content_epoch: op.content_epoch,
                    },
                )
            })
            .collect();
        if let Err(err) = Self::wait_until_remote_objects_visible(vfs, &extents_to_verify).await {
            requeue_missing_manifest_uploads(vfs, op.ino, op.content_epoch, &required_blocks).await;
            return Err(err);
        }

        let attr = run_meta(vfs.meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.get_attr(ino)
        })
        .await?;
        if attr.content_epoch != op.content_epoch {
            return Ok(());
        }

        let content_epoch = op.content_epoch;
        let published = run_meta(vfs.meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.publish_file_manifest(ino, content_epoch)
        })
        .await?;
        if !published {
            return Ok(());
        }

        vfs.remove_live_extents_for_inode_epoch(op.ino, op.content_epoch);
        if let Ok(attr) = run_meta(vfs.meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.get_attr(ino)
        })
        .await
        {
            vfs.refresh_inode_incarnation_state_from_attr(ino, &attr);
        } else {
            vfs.clear_inode_incarnation(ino);
        }

        Ok(())
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
                let key = format_object_key(&vfs.data_prefix, extent.object_id);
                match vfs.fetch_scheduler.data_store().exists(&key).await {
                    Ok(true) => {}
                    Ok(false) => {
                        all_visible = false;
                        break;
                    }
                    Err(err) => {
                        all_visible = false;
                        tracing::debug!(
                            object_id = extent.object_id,
                            error = %err,
                            "publish visibility check failed, will retry"
                        );
                        break;
                    }
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

        let key = format_object_key(&vfs.data_prefix, block_ref.object_id);
        let visible = match tokio::time::timeout(
            Duration::from_secs(3),
            vfs.fetch_scheduler.data_store().exists(&key),
        )
        .await
        {
            Ok(Ok(true)) => true,
            Ok(Ok(false)) => false,
            Ok(Err(_)) => false,
            Err(_) => false,
        };
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
