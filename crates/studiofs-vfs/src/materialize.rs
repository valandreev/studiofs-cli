use super::*;
use std::collections::{BTreeSet, HashMap};

struct MaterializeInflightGuard<'a>(&'a studiofs_cache::metrics::MaterializeBatchStats);

impl<'a> MaterializeInflightGuard<'a> {
    fn new(stats: &'a studiofs_cache::metrics::MaterializeBatchStats) -> Self {
        stats.enter_inflight();
        Self(stats)
    }
}

impl Drop for MaterializeInflightGuard<'_> {
    fn drop(&mut self) {
        self.0.leave_inflight();
    }
}

#[derive(Debug, Clone)]
struct MaterializedExtent {
    ino: Ino,
    block_index: u32,
    data: Bytes,
    from_spill: bool,
    object_id: u64,
    content_epoch: u64,
}

#[derive(Debug, Clone)]
struct PendingManifestSummary {
    ino: Ino,
    content_epoch: u64,
    visible_block_count: usize,
    touched_block_count: usize,
    reused_visible_block_count: usize,
}

pub(super) struct Materializer;

impl Materializer {
    pub(super) async fn background_loop(
        vfs: Arc<StudioVfs>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        info!("background materializer started");
        let mut timer = tokio::time::interval(vfs.slab_flush_interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                msg = rx.recv() => {
                    if msg.is_none() {
                        break;
                    }
                }
                _ = timer.tick() => {}
            }
            if vfs.state.slab_flusher_stopped.load(Ordering::Acquire) {
                break;
            }
            Self::drain(vfs.as_ref()).await;
        }
        info!("background materializer stopped");
    }

    pub(super) async fn drain(vfs: &StudioVfs) {
        let _guard = vfs.state.dirty_drain_lock.lock().await;
        let _qos = match vfs.qos.acquire_materialize().await {
            Ok(permit) => permit,
            Err(e) => {
                error!("materializer: qos acquire failed: {}", e);
                return;
            }
        };
        let _inflight_guard = MaterializeInflightGuard::new(&vfs.state.materialize_batch_stats);
        Self::drain_inner(vfs).await;
    }

    async fn drain_inner(vfs: &StudioVfs) {
        let max_drain_batch = vfs.max_drain_batch.max(1);

        loop {
            let batch_started = Instant::now();
            let Some((extents, results)) = Self::collect_batch(vfs, max_drain_batch).await else {
                return;
            };
            let batch_blocks = extents.len() as u64;
            let batch_bytes = extents.iter().map(|extent| extent.data.len() as u64).sum();

            let manifests = match Self::build_pending_manifest_summaries(vfs, &extents).await {
                Ok(manifests) => manifests,
                Err(e) => {
                    error!("materializer: manifest build failed: {}", e);
                    return;
                }
            };

            let batch_len = Self::apply_batch(vfs, extents, results, manifests).await;
            vfs.state.materialize_batch_stats.record_batch(
                batch_blocks,
                batch_bytes,
                batch_started.elapsed(),
            );
            if batch_len < max_drain_batch {
                return;
            }
        }
    }

    async fn collect_batch(
        vfs: &StudioVfs,
        max_drain_batch: usize,
    ) -> Option<(Vec<MaterializedExtent>, Vec<BatchFlushResult>)> {
        let keys: Vec<(Ino, u32)> = {
            let dirty = vfs.state.dirty_mem.lock();
            let mut fair = FairInodeScheduler::default();
            let mut seen = std::collections::HashSet::new();
            for key in dirty.keys() {
                if seen.insert(*key) {
                    fair.push(key.0, *key);
                }
            }
            if fair.len() < max_drain_batch {
                let spill = vfs.state.ingress_live.lock();
                for key in spill.keys() {
                    if seen.insert(*key) {
                        fair.push(key.0, *key);
                    }
                }
            }
            let mut keys = Vec::with_capacity(max_drain_batch);
            while keys.len() < max_drain_batch {
                let Some(key) = fair.pop() else {
                    break;
                };
                keys.push(key);
            }
            keys
        };
        if keys.is_empty() {
            return None;
        }

        let mut work: Vec<(Ino, u32, Bytes, bool)> = Vec::with_capacity(keys.len());
        for (ino, block_index) in keys {
            if vfs.state.deleted_inodes.lock().contains(&ino) {
                // `dirty_mem` is `DirtyMemIndex`: remove() updates both payload map
                // and per-inode counters atomically.
                let removed = vfs.state.dirty_mem.lock().remove(&(ino, block_index));
                if let Some(data) = removed {
                    vfs.state
                        .dirty_bytes
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                            Some(v.saturating_sub(data.len()))
                        })
                        .ok();
                    vfs.state.dirty_drained_notify.notify_waiters();
                }
                let _ = vfs.remove_spill_entry((ino, block_index));
                debug!(
                    "materializer: skipping deleted inode {} block {}",
                    ino, block_index
                );
                continue;
            }

            let (data, from_spill) = {
                let dirty = vfs.state.dirty_mem.lock();
                match dirty.get(&(ino, block_index)) {
                    Some(d) => (d.clone(), false),
                    None => match vfs.load_spill_block((ino, block_index)) {
                        Ok(Some(d)) => (d, true),
                        Ok(None) => continue,
                        Err(e) => {
                            error!(
                                ino,
                                block_index,
                                error = %e,
                                "materializer: failed to load spill block"
                            );
                            return None;
                        }
                    },
                }
            };
            work.push((ino, block_index, data, from_spill));
        }

        if work.is_empty() {
            return None;
        }

        let keys_len = work.len();
        let ids = match run_meta(vfs.meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.next_block_ids(keys_len as u32)
        })
        .await
        {
            Ok(ids) => ids,
            Err(e) => {
                error!("materializer: next_block_ids failed: {}", e);
                return None;
            }
        };

        let entries: Vec<BatchFlushEntry> = work
            .iter()
            .zip(ids.iter())
            .map(|((ino, block_index, data, _from_spill), &object_id)| {
                let length = data.len() as u32;
                let flags = if length < vfs.block_size {
                    BlockFlags::PARTIAL_BLOCK
                } else {
                    BlockFlags::empty()
                };
                BatchFlushEntry {
                    ino: *ino,
                    block_index: *block_index,
                    object_id,
                    length,
                    flags,
                }
            })
            .collect();

        let results = match run_meta(vfs.meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.flush_blocks_batch(&entries)
        })
        .await
        {
            Ok(results) => results,
            Err(e) => {
                error!("materializer: flush_blocks_batch failed: {}", e);
                return None;
            }
        };

        let mut epoch_cache = HashMap::<Ino, u64>::new();
        let mut extents = Vec::with_capacity(work.len());
        for (idx, (ino, block_index, data, from_spill)) in work.into_iter().enumerate() {
            let content_epoch = if let Some(epoch) = epoch_cache.get(&ino) {
                *epoch
            } else {
                let epoch = match vfs.current_content_epoch_async(ino).await {
                    Ok(epoch) => epoch,
                    Err(e) => {
                        error!(ino, error = %e, "materializer: read content_epoch failed");
                        return None;
                    }
                };
                epoch_cache.insert(ino, epoch);
                epoch
            };
            extents.push(MaterializedExtent {
                ino,
                block_index,
                data,
                from_spill,
                object_id: ids[idx],
                content_epoch,
            });
        }

        Some((extents, results))
    }

    async fn build_pending_manifest_summaries(
        vfs: &StudioVfs,
        extents: &[MaterializedExtent],
    ) -> VfsResult<Vec<PendingManifestSummary>> {
        let mut touched_by_inode = HashMap::<Ino, (u64, BTreeSet<u32>)>::new();
        for extent in extents {
            let entry = touched_by_inode
                .entry(extent.ino)
                .or_insert_with(|| (extent.content_epoch, BTreeSet::new()));
            entry.0 = extent.content_epoch;
            entry.1.insert(extent.block_index);
        }

        let mut manifests = Vec::with_capacity(touched_by_inode.len());
        for (ino, (content_epoch, touched)) in touched_by_inode {
            let visible_blocks = run_meta(vfs.meta.clone(), move |engine: Arc<dyn MetaEngine>| {
                engine.get_file_blocks(ino)
            })
            .await?;
            let reused_visible_block_count = visible_blocks
                .iter()
                .filter(|(block_index, _)| !touched.contains(block_index))
                .count();
            manifests.push(PendingManifestSummary {
                ino,
                content_epoch,
                visible_block_count: visible_blocks.len(),
                touched_block_count: touched.len(),
                reused_visible_block_count,
            });
        }
        Ok(manifests)
    }

    async fn apply_batch(
        vfs: &StudioVfs,
        extents: Vec<MaterializedExtent>,
        results: Vec<BatchFlushResult>,
        manifests: Vec<PendingManifestSummary>,
    ) -> usize {
        let mut l2_work: Vec<(u64, Bytes)> = Vec::with_capacity(extents.len());
        let mut upload_ops: Vec<(u64, u64, u64, u64)> = Vec::with_capacity(extents.len());
        let mut freed_bytes = 0usize;
        let mut drained_mem_blocks = 0u64;
        let mut drained_mem_bytes = 0u64;
        let mut drained_spill_blocks = 0u64;
        let mut drained_spill_bytes = 0u64;

        for (idx, extent) in extents.iter().enumerate() {
            vfs.block_cache
                .stage_l1(extent.object_id, extent.data.clone());
            l2_work.push((extent.object_id, extent.data.clone()));

            let file_offset = (extent.block_index as u64) * vfs.block_size as u64;
            upload_ops.push((
                extent.ino,
                file_offset,
                extent.object_id,
                extent.content_epoch,
            ));
            vfs.record_live_extent(
                extent.ino,
                extent.block_index,
                extent.object_id,
                extent.content_epoch,
            );

            {
                let mut dirty = vfs.state.dirty_mem.lock();
                let key = (extent.ino, extent.block_index);
                let remove_match = dirty
                    .get(&key)
                    .is_some_and(|current| current.as_ref() == extent.data.as_ref());
                // `dirty_mem` is `DirtyMemIndex`: remove() updates both payload map
                // and per-inode counters atomically.
                if remove_match && let Some(removed) = dirty.remove(&key) {
                    freed_bytes += removed.len();
                }
            }

            let spill_entry = vfs
                .state
                .ingress_live
                .lock()
                .get_cloned(&(extent.ino, extent.block_index));
            let should_remove_spill = spill_entry
                .as_ref()
                .and_then(|entry| read_ingress_entry_bytes(entry).ok())
                .is_some_and(|current| current.as_ref() == extent.data.as_ref());
            if should_remove_spill {
                let _ = vfs.remove_spill_entry((extent.ino, extent.block_index));
            }

            if extent.from_spill {
                drained_spill_blocks = drained_spill_blocks.saturating_add(1);
                drained_spill_bytes = drained_spill_bytes.saturating_add(extent.data.len() as u64);
            } else {
                drained_mem_blocks = drained_mem_blocks.saturating_add(1);
                drained_mem_bytes = drained_mem_bytes.saturating_add(extent.data.len() as u64);
            }

            if let Some(ref old_block) = results[idx].old_block {
                debug!(
                    "marking old block {} for deletion (overwrite)",
                    old_block.object_id
                );
            }
        }

        for manifest in manifests {
            debug!(
                ino = manifest.ino,
                content_epoch = manifest.content_epoch,
                visible_blocks = manifest.visible_block_count,
                touched_blocks = manifest.touched_block_count,
                reused_visible_blocks = manifest.reused_visible_block_count,
                "materializer pending manifest updated"
            );
        }

        vfs.block_cache.submit_uploads_batch(&upload_ops);

        vfs.state
            .dirty_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(freed_bytes))
            })
            .ok();
        vfs.state
            .drained_mem_blocks
            .fetch_add(drained_mem_blocks, Ordering::Relaxed);
        vfs.state
            .drained_mem_bytes
            .fetch_add(drained_mem_bytes, Ordering::Relaxed);
        vfs.state
            .drained_spill_blocks
            .fetch_add(drained_spill_blocks, Ordering::Relaxed);
        vfs.state
            .drained_spill_bytes
            .fetch_add(drained_spill_bytes, Ordering::Relaxed);

        vfs.state.dirty_drained_notify.notify_waiters();

        let batch_len = l2_work.len();
        debug!(
            "materializer: drained {} blocks ({} bytes), dirty_bytes={}, mem_blocks={}, mem_bytes={}, spill_blocks={}, spill_bytes={}",
            batch_len,
            freed_bytes,
            vfs.state.dirty_bytes.load(Ordering::Relaxed),
            drained_mem_blocks,
            drained_mem_bytes,
            drained_spill_blocks,
            drained_spill_bytes,
        );

        if let Err(e) = vfs.block_cache.commit_l2_batch(&l2_work).await {
            warn!(error = %e, blocks = batch_len, "materializer: failed to enqueue L2 batch");
        }

        batch_len
    }
}
