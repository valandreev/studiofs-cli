use super::*;

pub(super) struct CommitCoordinator;

impl CommitCoordinator {
    pub(super) async fn finish_extent_upload(
        vfs: &StudioVfs,
        op: &PendingUpload,
        block_index: u32,
    ) -> VfsResult<()> {
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

        let live_extents: Vec<(u32, ExtentRecordId)> = vfs
            .state
            .live_extent_records
            .lock()
            .iter()
            .filter_map(|((candidate_ino, block_index), extent)| {
                (*candidate_ino == op.ino && extent.content_epoch == op.content_epoch)
                    .then_some((*block_index, *extent))
            })
            .collect();
        if live_extents.is_empty() {
            return Ok(());
        }

        let uploaded = vfs.uploaded_extent_records_for_inode_epoch(op.ino, op.content_epoch);
        let uploaded_map: std::collections::HashMap<u32, ExtentRecordId> =
            uploaded.into_iter().collect();
        let all_live_uploaded = live_extents.iter().all(|(block_index, extent)| {
            uploaded_map
                .get(block_index)
                .is_some_and(|current| current == extent)
        });
        if !all_live_uploaded {
            return Ok(());
        }

        Self::wait_until_remote_objects_visible(vfs, &live_extents).await?;

        let ino = op.ino;
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

        vfs.remove_live_extents_for_inode(op.ino);
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
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
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
                        return Err(VfsError::Other(format!(
                            "publish visibility check failed for object {}: {err}",
                            extent.object_id
                        )));
                    }
                }
            }

            if all_visible {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(VfsError::Other(
                    "timed out waiting for uploaded extents to become remotely visible".into(),
                ));
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
