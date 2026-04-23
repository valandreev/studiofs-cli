use futures::StreamExt;
use tokio::time::MissedTickBehavior;

use super::*;
use crate::vfs::upload_lifecycle::UploadLifecycleWorker;
use crate::{JanitorMeta, VfsMeta};

impl StudioVfs {
    pub(super) async fn background_aggressive_materialization(self: Arc<Self>) {
        let mut interval = tokio::time::interval(AGGRESSIVE_MATERIALIZATION_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = interval.tick() => {}
            }
            if self.shutdown_requested() {
                break;
            }
            if !self.aggressive_materialization_should_run() {
                continue;
            }

            let mut dirty_inodes = self.state.handle_writers.snapshot_dirty_inodes_by_bytes();
            // #region debug log
            self.debug_write_probe(
                "H2",
                "crates/studiofs-vfs/src/vfs/workers.rs:17",
                "aggressive materialization tick",
                serde_json::json!({
                    "total_dirty_bytes": self.state.handle_writers.total_dirty_bytes(),
                    "threshold_bytes": super::aggressive_materialization_threshold_bytes(self.max_dirty_bytes()),
                    "hard_threshold_bytes": super::hard_backpressure_threshold_bytes(self.max_dirty_bytes()),
                    "buffered_write_count": self.state.handle_writers.total_buffered_write_count(),
                    "dirty_inode_count": dirty_inodes.len(),
                }),
            );
            // #endregion debug log
            dirty_inodes.sort_by_key(|(_, dirty_bytes)| std::cmp::Reverse(*dirty_bytes));
            for (ino, dirty_bytes) in dirty_inodes {
                if dirty_bytes == 0 || self.shutdown_requested() {
                    break;
                }
                self.spawn_deferred_materialization_if_needed(ino).await;
            }
        }
    }

    pub(super) async fn background_local_dirty_flush(self: Arc<Self>) {
        let mut interval = tokio::time::interval(LOCAL_DIRTY_FLUSH_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = interval.tick() => {}
            }
            if self.shutdown_requested() {
                break;
            }
            let dirty_inodes = self.snapshot_pending_local_dirty_inodes();
            if dirty_inodes.is_empty() {
                continue;
            }
            for ino in dirty_inodes {
                if self.shutdown_requested() {
                    break;
                }
                if !self.should_background_flush_pending_local_dirty_inode(ino) {
                    continue;
                }
                if let Err(error) = self
                    .flush_local_dirty_state(ino, LocalSyncPolicy::Opportunistic)
                    .await
                {
                    warn!(
                        ino,
                        error = %error,
                        "background local-dirty flush failed; will retry"
                    );
                }
            }
        }
    }

    pub(super) async fn background_epoch_finalizer(self: Arc<Self>) {
        loop {
            let Some((ino, content_epoch)) = self.pop_epoch_finalization() else {
                tokio::select! {
                    _ = self.state.shutdown.shutdown_notify.notified() => break,
                    _ = self.state.upload.pending_epoch_finalizations_notify.notified() => {}
                }
                continue;
            };

            match commit::CommitCoordinator::finalize_inode_epoch(&self, ino, content_epoch).await {
                Ok(commit::EpochFinalizeStatus::Published) => {
                    self.state.file_session.note_background_transition(
                        ino,
                        FileSessionOperation::Finalize,
                        "published",
                    );
                }
                Ok(commit::EpochFinalizeStatus::Skipped) => {
                    self.state.file_session.note_background_transition(
                        ino,
                        FileSessionOperation::Finalize,
                        "skipped",
                    );
                }
                Ok(commit::EpochFinalizeStatus::Deferred) => {
                    self.state.file_session.note_background_transition(
                        ino,
                        FileSessionOperation::Finalize,
                        "deferred",
                    );
                    let retry_delay = self.upload_requeue_base.max(Duration::from_millis(25));
                    self.schedule_epoch_finalization_retry(ino, content_epoch, retry_delay);
                }
                Err(err) => {
                    self.state
                        .metrics
                        .zeroio_publish_failures
                        .fetch_add(1, Ordering::Relaxed);
                    self.record_runtime_event(
                        "zeroio",
                        "publish_failed",
                        "zeroio publish failed",
                        serde_json::json!({
                            "ino": ino,
                            "content_epoch": content_epoch,
                            "path": self.best_effort_path_for_inode(ino),
                            "error": err.to_string(),
                        }),
                    );
                    if Self::is_invalid_regular_file_graph_error(&err) {
                        let attr_size =
                            run_meta(self.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                                engine.get_attr(ino)
                            })
                            .await
                            .ok()
                            .map(|attr| attr.size)
                            .unwrap_or(0);
                        warn!(
                            ino,
                            content_epoch,
                            error = %err,
                            "epoch finalization failed due to invalid regular-file graph, quarantining inode"
                        );
                        self.quarantine_invalid_regular_file_graph(ino, attr_size, &err);
                        self.state.file_session.note_background_transition(
                            ino,
                            FileSessionOperation::Finalize,
                            "failed",
                        );
                        continue;
                    }
                    if self.shutdown_requested() {
                        break;
                    }
                    self.state.file_session.note_background_transition(
                        ino,
                        FileSessionOperation::Finalize,
                        "failed",
                    );
                    warn!(
                        ino,
                        content_epoch,
                        error = %err,
                        "epoch finalization failed, will retry"
                    );
                    self.schedule_epoch_finalization_retry(
                        ino,
                        content_epoch,
                        self.upload_requeue_base,
                    );
                }
            }
        }
    }

    fn schedule_epoch_finalization_retry(
        self: &Arc<Self>,
        ino: Ino,
        content_epoch: u64,
        delay: Duration,
    ) {
        if self.shutdown_requested() {
            return;
        }
        let vfs = self.clone();
        tokio::spawn(async move {
            if vfs.shutdown_requested() {
                return;
            }
            tokio::select! {
                _ = vfs.state.shutdown.shutdown_notify.notified() => {}
                _ = tokio::time::sleep(delay) => {
                    if !vfs.shutdown_requested() {
                        vfs.queue_epoch_finalization(ino, content_epoch);
                    }
                }
            }
        });
    }

    fn kernel_invalidator(&self) -> Option<Arc<dyn KernelInvalidator>> {
        self.kernel_invalidator.read().clone()
    }

    fn collect_metadata_inodes<M: ?Sized + JanitorMeta>(engine: &M) -> MetaResult<HashSet<Ino>> {
        let mut seen = HashSet::from([ROOT_INO]);
        let mut stack = vec![ROOT_INO];

        while let Some(ino) = stack.pop() {
            let attr = match engine.get_attr(ino) {
                Ok(attr) => attr,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            if attr.kind != FileType::Directory {
                continue;
            }
            let entries = match engine.readdir(ino) {
                Ok(entries) => entries,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            for entry in entries {
                if seen.insert(entry.ino) {
                    stack.push(entry.ino);
                }
            }
        }

        Ok(seen)
    }

    pub(super) fn invalidate_kernel_inodes(&self, inodes: impl IntoIterator<Item = Ino>) {
        let Some(invalidator) = self.kernel_invalidator() else {
            return;
        };
        let mut unique = HashSet::new();
        for ino in inodes {
            if unique.insert(ino) {
                invalidator.inval_inode(ino);
            }
        }
    }

    pub(super) fn invalidate_attr_read_cache(&self, inodes: impl IntoIterator<Item = Ino>) {
        let mut cache = self.state.namespace.attr_read_cache.lock();
        for ino in inodes {
            cache.remove(ino);
        }
    }

    pub(super) async fn background_metadata_invalidation(self: Arc<Self>, interval: Duration) {
        let meta = self.sync_meta();
        let mut last_namespace_token = meta.namespace_version_token();
        let mut last_metadata_token = meta.metadata_version_token();
        let mut last_snapshot: HashSet<Ino> = HashSet::new();

        if last_namespace_token.is_some() || last_metadata_token.is_some() {
            let meta = self.meta.clone();
            if let Ok(snapshot) = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                Self::collect_metadata_inodes(engine.as_ref())
            })
            .await
            {
                last_snapshot = snapshot;
            }
        }

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }
            if self.kernel_invalidator().is_none() {
                continue;
            }

            let meta = self.meta.clone();
            let sync = run_meta(meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                let before_namespace = engine.namespace_version_token();
                let before_metadata = engine.metadata_version_token();
                engine.sync_metadata()?;
                let after_namespace = engine.namespace_version_token();
                let after_metadata = engine.metadata_version_token();
                Ok((
                    before_namespace,
                    after_namespace,
                    before_metadata,
                    after_metadata,
                ))
            })
            .await;
            let Ok((
                _before_namespace,
                after_namespace_token,
                _before_metadata,
                after_metadata_token,
            )) = sync
            else {
                continue;
            };

            let namespace_changed = last_namespace_token != after_namespace_token;
            let metadata_changed = last_metadata_token != after_metadata_token;
            last_namespace_token = after_namespace_token.clone();
            last_metadata_token = after_metadata_token.clone();

            if !namespace_changed && !metadata_changed {
                continue;
            }

            let meta = self.meta.clone();
            let Ok(current_snapshot) = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                Self::collect_metadata_inodes(engine.as_ref())
            })
            .await
            else {
                continue;
            };

            let mut to_invalidate = last_snapshot.clone();
            to_invalidate.extend(current_snapshot.iter().copied());
            self.invalidate_attr_read_cache(to_invalidate.iter().copied());
            self.invalidate_kernel_inodes(to_invalidate.iter().copied());
            if namespace_changed {
                self.bump_namespace_epoch();
            }
            if namespace_changed || metadata_changed {
                self.bump_acl_namespace_epoch();
            }
            last_snapshot = current_snapshot;
        }
    }

    pub(super) async fn background_uploader(
        self: Arc<Self>,
        rx: tokio::sync::mpsc::UnboundedReceiver<PendingUpload>,
    ) {
        UploadLifecycleWorker::new(self).run(rx).await;
    }

    pub(super) async fn background_upload_stats(self: Arc<Self>) {
        let mut upload_pipeline_summary_was_active = false;
        let mut ticks_since_last_log = 0u64;
        let mut zero_log_counter = 0u8;
        const ZERO_LOG_TARGET: u8 = 3;

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(self.upload_stats_interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }
            if let Err(error) = self.block_store_run_pressure_maintenance_async().await {
                warn!(error = %error, "background local backend pressure maintenance failed");
            }
            let pending_uploads = self.pending_uploads_snapshot();
            let pending = pending_uploads.len();
            let pending_upload_bytes = pending_uploads
                .iter()
                .map(|op| {
                    self.local_block_backend
                        .object_stats(op.object_id)
                        .map(|stats| stats.payload_len)
                        .unwrap_or(u64::from(self.block_size))
                })
                .sum::<u64>();
            let metadata_backlog = self.sync_meta().replication_backlog();
            let pending_metadata_ops = metadata_backlog.total_ops();

            let is_active = Self::upload_pipeline_summary_is_active(pending, pending_metadata_ops);
            let activity_started = is_active && !upload_pipeline_summary_was_active;

            let should_log = if activity_started {
                ticks_since_last_log = 0;
                zero_log_counter = 0;
                true
            } else if is_active {
                ticks_since_last_log += 1;
                ticks_since_last_log >= 10
            } else if upload_pipeline_summary_was_active {
                ticks_since_last_log = 0;
                zero_log_counter = 1;
                true
            } else if zero_log_counter > 0 && zero_log_counter < ZERO_LOG_TARGET {
                ticks_since_last_log += 1;
                if ticks_since_last_log >= 10 {
                    zero_log_counter += 1;
                    true
                } else {
                    false
                }
            } else {
                false
            };

            let (p95_ms, max_ms) = self.pending_age_stats_ms(&pending_uploads);
            let ingress = self.ingress_stats();
            let materialize_batches = self.state.metrics.materialize_batch_stats.snapshot();
            let upload_queue = self.cached_store.cache_pipeline_metrics().upload_queue;
            let foreground_read_protection = self.foreground_read_protection_stats();
            self.adjust_upload_backpressure_from_metrics(
                foreground_read_protection.p90_latency_ns,
                foreground_read_protection.dominant_upload_pressure_boost,
            );
            let dentry_cache = self.dentry_cache_metrics();
            let path_shortcut_cache = self.path_shortcut_cache_metrics();
            let attr_read_cache = self.attr_read_cache_metrics();
            let local_backend_dirty_count = self.local_block_backend.dirty_object_count();
            let transit_memory = self.transit_memory_budget.snapshot();
            let file_sessions = self.file_session_stats();

            if should_log {
                ticks_since_last_log = 0;
                let pending_uploads =
                    Self::format_upload_pipeline_pending_uploads(pending_upload_bytes);
                let became_idle = !is_active && upload_pipeline_summary_was_active;
                if let Some((event_kind, event_title)) =
                    Self::upload_pipeline_runtime_event(activity_started, became_idle, is_active)
                {
                    self.record_runtime_event(
                        "upload_pipeline",
                        event_kind,
                        event_title,
                        serde_json::json!({
                            "pending_uploads": pending,
                            "pending_upload_bytes": pending_upload_bytes,
                            "pending_uploads_human": pending_uploads,
                            "pending_metadata_ops": pending_metadata_ops,
                            "inflight_metadata_ops": metadata_backlog.inflight_ops,
                            "dirty_mem_blocks": ingress.dirty_mem_blocks,
                            "dirty_mem_bytes": ingress.dirty_mem_bytes,
                            "write_backpressure_events": ingress.write_backpressure_events,
                            "write_backpressure_wait_ms": ingress.write_backpressure_wait_ms,
                            "foreground_read_p90_latency_ns": foreground_read_protection.p90_latency_ns,
                        }),
                    );
                }
                // #region debug log
                self.debug_write_probe(
                    "H3",
                    "crates/studiofs-vfs/src/vfs/workers.rs:389",
                    "upload pipeline summary tick",
                    serde_json::json!({
                        "pending_uploads": pending,
                        "pending_upload_bytes": pending_upload_bytes,
                        "pending_metadata_ops": pending_metadata_ops,
                        "inflight_metadata_ops": metadata_backlog.inflight_ops,
                        "local_backend_dirty_count": local_backend_dirty_count,
                        "handle_writer_total_dirty_bytes": self.state.handle_writers.total_dirty_bytes(),
                        "handle_writer_buffered_write_count": self.state.handle_writers.total_buffered_write_count(),
                        "pending_epoch_finalizations": self.state.upload.pending_epoch_finalizations.lock().len(),
                        "upload_scheduler_penalty_ms": self.upload_scheduler.adaptive_penalty_ms(),
                        "dirty_mem_bytes": ingress.dirty_mem_bytes,
                        "write_backpressure_events": ingress.write_backpressure_events,
                        "write_backpressure_wait_ms": ingress.write_backpressure_wait_ms,
                        "transit_used_bytes": transit_memory.used_bytes,
                        "transit_budget_bytes": transit_memory.budget_bytes,
                    }),
                );
                // #endregion debug log
                info!(
                    pending_uploads = %pending_uploads,
                    pending_metadata_ops,
                    "upload pipeline"
                );
            }

            upload_pipeline_summary_was_active = is_active;
            debug!(
                dentry_cache_entries = dentry_cache.occupancy_entries,
                dentry_cache_bytes = dentry_cache.occupancy_bytes,
                path_shortcut_cache_entries = path_shortcut_cache.occupancy_entries,
                path_shortcut_cache_bytes = path_shortcut_cache.occupancy_bytes,
                attr_read_cache_entries = attr_read_cache.occupancy_entries,
                attr_read_cache_bytes = attr_read_cache.occupancy_bytes,
                "upload pipeline caches"
            );
            if !self.upload_pipeline_is_idle(pending, &ingress) {
                debug!(
                    requeues = self.state.upload.upload_requeues.load(Ordering::Relaxed),
                    canceled_deleted = self.state.upload.upload_canceled_deleted.load(Ordering::Relaxed),
                    canceled_stale = self.state.upload.upload_canceled_stale.load(Ordering::Relaxed),
                    foreground_reads_inflight =
                        self.state.read.foreground_reads_inflight.load(Ordering::Relaxed),
                    dirty_mem_blocks = ingress.dirty_mem_blocks,
                    dirty_mem_bytes = ingress.dirty_mem_bytes,
                    write_backpressure_wait_ms = ingress.write_backpressure_wait_ms,
                    write_backpressure_events = ingress.write_backpressure_events,
                    fast_backpressure_wait_ms = ingress.fast_backpressure_wait_ms,
                    fast_backpressure_events = ingress.fast_backpressure_events,
                    fast_backpressure_stall_max_ms = ingress.fast_backpressure_stall_max_ms,
                    materialize_batches_total = materialize_batches.batches,
                    materialize_batches_blocks = materialize_batches.blocks,
                    materialize_batches_bytes = materialize_batches.bytes,
                    materialize_batches_inflight = materialize_batches.inflight_batches,
                    materialize_batches_inflight_peak = materialize_batches.inflight_batches_peak,
                    materialize_batch_duration_histogram = ?materialize_batches.duration_histogram.bucket_counts,
                    upload_queue_depth = upload_queue.pending_depth,
                    upload_queue_depth_peak = upload_queue.pending_depth_peak,
                    upload_queue_submitted = upload_queue.submitted,
                    upload_queue_completed = upload_queue.completed,
                    upload_queue_requeued = upload_queue.requeued,
                    upload_queue_canceled = upload_queue.canceled,
                    upload_io_slots_available = self.upload_cache_io_slots.available_permits(),
                    qos_accept_available = self.qos.accept_available(),
                    qos_materialize_available = self.qos.materialize_available(),
                    qos_commit_available = self.qos.commit_available(),
                    qos_fetch_available = self.qos.fetch_available(),
                    pending_metadata_ops = metadata_backlog.pending_ops,
                    inflight_metadata_ops = metadata_backlog.inflight_ops,
                    dentry_cache_hits = dentry_cache.hits,
                    dentry_cache_hot_hits = dentry_cache.protected_hits,
                    dentry_cache_misses = dentry_cache.misses,
                    dentry_cache_evictions = dentry_cache.evictions,
                    dentry_cache_negative_hits = dentry_cache.negative_hits,
                    dentry_cache_negative_hit_ratio = dentry_cache.negative_hit_ratio(),
                    dentry_cache_hot_hit_ratio = dentry_cache.hot_hit_ratio(),
                    dentry_cache_entries = dentry_cache.occupancy_entries,
                    dentry_cache_bytes = dentry_cache.occupancy_bytes,
                    dentry_cache_hot_entries = dentry_cache.protected_occupancy_entries,
                    dentry_cache_hot_bytes = dentry_cache.protected_occupancy_bytes,
                    dentry_cache_entry_budget = dentry_cache.entry_budget,
                    dentry_cache_byte_budget = dentry_cache.byte_budget,
                    path_shortcut_cache_hits = path_shortcut_cache.hits,
                    path_shortcut_cache_hot_hits = path_shortcut_cache.protected_hits,
                    path_shortcut_cache_misses = path_shortcut_cache.misses,
                    path_shortcut_cache_evictions = path_shortcut_cache.evictions,
                    path_shortcut_cache_negative_hits = path_shortcut_cache.negative_hits,
                    path_shortcut_cache_negative_hit_ratio =
                        path_shortcut_cache.negative_hit_ratio(),
                    path_shortcut_cache_hot_hit_ratio = path_shortcut_cache.hot_hit_ratio(),
                    path_shortcut_cache_entries = path_shortcut_cache.occupancy_entries,
                    path_shortcut_cache_bytes = path_shortcut_cache.occupancy_bytes,
                    path_shortcut_cache_hot_entries =
                        path_shortcut_cache.protected_occupancy_entries,
                    path_shortcut_cache_hot_bytes = path_shortcut_cache.protected_occupancy_bytes,
                    path_shortcut_cache_entry_budget = path_shortcut_cache.entry_budget,
                    path_shortcut_cache_byte_budget = path_shortcut_cache.byte_budget,
                    attr_read_cache_hits = attr_read_cache.hits,
                    attr_read_cache_hot_hits = attr_read_cache.protected_hits,
                    attr_read_cache_misses = attr_read_cache.misses,
                    attr_read_cache_evictions = attr_read_cache.evictions,
                    attr_read_cache_hot_hit_ratio = attr_read_cache.hot_hit_ratio(),
                    attr_read_cache_entries = attr_read_cache.occupancy_entries,
                    attr_read_cache_bytes = attr_read_cache.occupancy_bytes,
                    attr_read_cache_hot_entries = attr_read_cache.protected_occupancy_entries,
                    attr_read_cache_hot_bytes = attr_read_cache.protected_occupancy_bytes,
                    attr_read_cache_entry_budget = attr_read_cache.entry_budget,
                    attr_read_cache_byte_budget = attr_read_cache.byte_budget,
                    "upload pipeline counters"
                );
                debug!(
                    file_session_active = file_sessions.active_sessions,
                    file_session_open_handles = file_sessions.open_handles,
                    file_session_queued_ops = file_sessions.queued_ops,
                    file_session_max_queue_depth = file_sessions.max_queue_depth,
                    file_session_wait_events = file_sessions.wait_events,
                    file_session_wait_ms = file_sessions.wait_ms,
                    file_session_transitions = file_sessions.transitions,
                    "file session counters"
                );
                debug!(
                    file_session_wait_reasons = ?file_sessions.wait_reasons,
                    "file session queue wait reasons"
                );
                debug!(
                    upload_fallback_deferrals =
                        foreground_read_protection.upload_fallback_deferrals,
                    upload_fallback_rereads = foreground_read_protection.upload_fallback_rereads,
                    upload_fallback_reread_bytes =
                        foreground_read_protection.upload_fallback_reread_bytes,
                    upload_fallback_reread_cache_misses =
                        foreground_read_protection.upload_fallback_reread_cache_misses,
                    upload_fallback_wait_ms =
                        foreground_read_protection.upload_fallback_wait_ns / 1_000_000,
                    upload_fallback_wait_max_ms =
                        foreground_read_protection.upload_fallback_wait_max_ns / 1_000_000,
                    upload_fallback_read_ms =
                        foreground_read_protection.upload_fallback_read_ns / 1_000_000,
                    upload_fallback_read_max_ms =
                        foreground_read_protection.upload_fallback_read_max_ns / 1_000_000,
                    "upload pipeline local reread path"
                );
                debug!(
                    transit_memory_budget_bytes = transit_memory.budget_bytes,
                    transit_memory_used_bytes = transit_memory.used_bytes,
                    transit_memory_available_bytes = transit_memory.available_bytes,
                    transit_memory_waiting_foreground = transit_memory.waiting_foreground,
                    transit_memory_waiting_background = transit_memory.waiting_background,
                    "upload pipeline transit memory"
                );
                debug!(
                    local_backend_dirty_count,
                    prefetch_skips = self
                        .fetch_scheduler
                        .metrics
                        .prefetch_skips
                        .load(Ordering::Relaxed),
                    "local backend and fetch counters"
                );
                debug!(
                    upload_io_foreground_cost_base = self.upload_cache_io_foreground_cost_base,
                    upload_io_foreground_cost_step = self.upload_cache_io_foreground_cost_step,
                    upload_io_foreground_cost_dynamic =
                        foreground_read_protection.upload_cache_io_foreground_cost,
                    foreground_read_p90_present =
                        foreground_read_protection.p90_latency_ns.is_some(),
                    foreground_read_p90_ms = foreground_read_protection
                        .p90_latency_ns
                        .map(|ns| ns.saturating_div(1_000_000))
                        .unwrap_or(0),
                    foreground_read_speedometer_samples =
                        foreground_read_protection.speedometer_samples,
                    foreground_read_window_bytes = foreground_read_protection.window_bytes,
                    foreground_read_window_coverage_ms =
                        foreground_read_protection.window_coverage_ms,
                    foreground_read_throughput_bytes_per_sec =
                        foreground_read_protection.throughput_bytes_per_sec,
                    foreground_read_target_ms = self
                        .upload_fg_read_latency_target_ns
                        .saturating_div(1_000_000),
                    upload_scheduler_penalty_ms =
                        foreground_read_protection.upload_scheduler_penalty_ms,
                    "upload pipeline adaptive throttle"
                );
            }
            self.upload_scheduler.log_stats(pending, p95_ms, max_ms);
        }
    }

    pub(super) async fn background_gc(self: Arc<Self>) {
        info!("background GC started");

        let interval = tokio::time::Duration::from_secs(600);
        let grace_period = 3600i64;

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }

            let meta = self.meta.clone();
            let lock_acquired = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.try_acquire_global_janitor_lock()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "background GC skipped: janitor PG lock check failed");
                    continue;
                }
            };
            if !lock_acquired {
                debug!("background GC skipped: janitor PG lock not acquired");
                continue;
            }

            const CHUNK_COMPACTION_BATCH_LIMIT: usize = 32;
            let meta = self.meta.clone();
            match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.compact_fragmented_regular_file_chunks(CHUNK_COMPACTION_BATCH_LIMIT)
            })
            .await
            {
                Ok(compacted_chunks) if compacted_chunks > 0 => {
                    info!(
                        compacted_chunks,
                        "background GC compacted fragmented regular-file chunk rows"
                    );
                }
                Ok(_) => {}
                Err(e) => warn!(error = %e, "background GC chunk compaction failed"),
            }

            let meta = self.meta.clone();
            let referenced_object_ids = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_referenced_object_ids()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(
                        error = %e,
                        "background GC skipped: metadata reachability query failed"
                    );
                    continue;
                }
            };

            let now = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            let meta = self.meta.clone();
            match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_pending_deletions()
            })
            .await
            {
                Ok(pending) => {
                    let mut deleted_count = 0;
                    for (object_id, timestamp) in pending {
                        if referenced_object_ids.contains(&object_id) {
                            debug!(object_id, "skipping object - still reachable in metadata");
                            continue;
                        }

                        if now - timestamp < grace_period {
                            debug!("skipping object {} - still in grace period", object_id);
                            continue;
                        }

                        let mut delete_failed = false;
                        for key in format_object_fragment_keys(&self.data_prefix, object_id) {
                            if let Err(e) = self.data_write.delete_by_key(&key).await {
                                if matches!(&e, DataError::OpenDal(oe) if oe.kind() == ErrorKind::NotFound)
                                {
                                    continue;
                                }
                                delete_failed = true;
                                warn!(
                                    object_id,
                                    key,
                                    error = %e,
                                    "failed to delete object fragment from S3"
                                );
                            }
                        }
                        if !delete_failed {
                            let meta = self.meta.clone();
                            if let Err(e) = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                                engine.remove_pending_deletion(object_id)?;
                                Ok(())
                            })
                            .await
                            {
                                warn!("failed to remove pending deletion for {}: {}", object_id, e);
                            }
                            deleted_count += 1;
                            debug!("GC deleted object {}", object_id);
                        }
                    }
                    if deleted_count > 0 {
                        info!("GC deleted {} objects", deleted_count);
                    }
                }
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    error!("failed to get pending deletions: {}", e);
                }
            }

            let meta = self.meta.clone();
            if let Err(e) = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.release_global_janitor_lock()
            })
            .await
            {
                warn!(error = %e, "background GC failed to release janitor PG lock");
            }
        }
    }

    pub(super) async fn background_janitor(self: Arc<Self>) {
        info!(
            interval_secs = self.janitor_interval.as_secs(),
            grace_secs = self.janitor_skip_newer_than.as_secs(),
            max_deletes = self.janitor_max_deletes_per_run,
            "background janitor started"
        );
        let mut cycle: u64 = 0;

        loop {
            cycle = cycle.saturating_add(1);
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(self.janitor_interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }

            if let Some(lease) = &self.janitor_lease
                && !self.acquire_janitor_lease(lease).await
            {
                debug!(
                    cycle,
                    lock_path = lease.lock_path,
                    "janitor cycle skipped: lease not acquired"
                );
                continue;
            }

            let meta = self.meta.clone();
            let lock_acquired = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.try_acquire_global_janitor_lock()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(cycle, error = %e, "janitor cycle skipped: PG lock check failed");
                    continue;
                }
            };
            if !lock_acquired {
                debug!(cycle, "janitor cycle skipped: PG lock not acquired");
                continue;
            }

            let meta = self.meta.clone();
            let global_snapshot_pins = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_global_snapshot_pins()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(cycle, error = %e, "janitor cycle skipped: snapshot pin query failed");
                    continue;
                }
            };

            let meta = self.meta.clone();
            let referenced = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_referenced_object_ids()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(cycle, error = %e, "janitor cycle failed: get_referenced_object_ids");
                    continue;
                }
            };
            let meta = self.meta.clone();
            let pending_map: HashMap<u64, i64> =
                match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.get_pending_deletions()
                })
                .await
                {
                    Ok(v) => v.into_iter().collect(),
                    Err(e) => {
                        warn!(cycle, error = %e, "janitor: failed to read pending deletions");
                        HashMap::new()
                    }
                };

            let meta = self.meta.clone();
            let recovered_undo_parents = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.recover_pending_creates()
            })
            .await
            {
                Ok(parents) => parents,
                Err(e) => {
                    warn!(cycle, error = %e, "janitor: pending-create replay failed");
                    Vec::new()
                }
            };
            if !recovered_undo_parents.is_empty() {
                for parent in recovered_undo_parents.iter().copied() {
                    self.dentry_cache_invalidate_parent(parent);
                }
                self.bump_namespace_for_parents(recovered_undo_parents.iter().copied());
            }
            let meta = self.meta.clone();
            let (undo_total, undo_started, undo_replaying, undo_completed) =
                match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.list_undo_logs()
                })
                .await
                {
                    Ok(records) => {
                        let mut started = 0usize;
                        let mut replaying = 0usize;
                        let mut completed = 0usize;
                        for record in records.iter() {
                            match record.phase {
                                studiofs_meta::engine::UndoLogPhase::Started => {
                                    started = started.saturating_add(1)
                                }
                                studiofs_meta::engine::UndoLogPhase::Replaying => {
                                    replaying = replaying.saturating_add(1)
                                }
                                studiofs_meta::engine::UndoLogPhase::Completed => {
                                    completed = completed.saturating_add(1)
                                }
                            }
                        }
                        (records.len(), started, replaying, completed)
                    }
                    Err(e) => {
                        warn!(cycle, error = %e, "janitor: failed to read undo-log metrics");
                        (0usize, 0usize, 0usize, 0usize)
                    }
                };

            let mut lister = match self
                .data_write
                .operator()
                .lister_with(&self.data_prefix)
                .recursive(true)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(
                        cycle,
                        prefix = self.data_prefix,
                        error = %e,
                        "janitor cycle failed: list data prefix"
                    );
                    continue;
                }
            };

            let now = SystemTime::now();
            let now_unix = now
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let mut scanned: u64 = 0;
            let mut orphan_candidates: u64 = 0;
            let mut deleted: u64 = 0;
            let mut skipped_young: u64 = 0;
            let mut skipped_unknown_age: u64 = 0;
            let mut skipped_cap: u64 = 0;
            let mut skipped_non_object_key: u64 = 0;
            let mut delete_errors: u64 = 0;
            let mut seen_objects: HashSet<u64> = HashSet::new();

            let mut list_error = None;
            while let Some(entry_result) = lister.next().await {
                let entry = match entry_result {
                    Ok(entry) => entry,
                    Err(err) => {
                        list_error = Some(err);
                        break;
                    }
                };
                if entry.metadata().mode() != EntryMode::FILE {
                    continue;
                }
                scanned = scanned.saturating_add(1);
                let path = entry.path();
                let object_id = match parse_object_id_from_key(path) {
                    Ok(id) => id,
                    Err(_) => {
                        skipped_non_object_key = skipped_non_object_key.saturating_add(1);
                        continue;
                    }
                };
                if !seen_objects.insert(object_id) {
                    continue;
                }
                if referenced.contains(&object_id) {
                    continue;
                }
                if global_snapshot_pins.contains(&object_id) {
                    continue;
                }
                orphan_candidates = orphan_candidates.saturating_add(1);
                if deleted >= self.janitor_max_deletes_per_run {
                    skipped_cap = skipped_cap.saturating_add(1);
                    continue;
                }

                let mut is_old_enough = false;
                if let Some(ts) = pending_map.get(&object_id) {
                    let age = now_unix.saturating_sub(*ts) as u64;
                    if age >= self.janitor_skip_newer_than.as_secs() {
                        is_old_enough = true;
                    }
                } else if let Some(last_modified) = entry.metadata().last_modified() {
                    let last_modified_st: SystemTime = last_modified.into();
                    let age = now
                        .duration_since(last_modified_st)
                        .unwrap_or_else(|_| Duration::from_secs(0));
                    if age >= self.janitor_skip_newer_than {
                        is_old_enough = true;
                    }
                } else {
                    skipped_unknown_age = skipped_unknown_age.saturating_add(1);
                }

                if !is_old_enough {
                    skipped_young = skipped_young.saturating_add(1);
                    continue;
                }

                let mut delete_failed = false;
                for key in format_object_fragment_keys(&self.data_prefix, object_id) {
                    if let Err(e) = self.data_write.operator().delete(&key).await {
                        if e.kind() == ErrorKind::NotFound {
                            continue;
                        }
                        delete_failed = true;
                        warn!(cycle, key, error = %e, "janitor: fragment delete failed");
                    }
                }
                if delete_failed {
                    delete_errors = delete_errors.saturating_add(1);
                    continue;
                }
                deleted = deleted.saturating_add(1);
            }

            if let Some(err) = list_error {
                let meta = self.meta.clone();
                let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.release_global_janitor_lock()
                })
                .await;
                warn!(
                    cycle,
                    prefix = self.data_prefix,
                    error = %err,
                    "janitor cycle failed while streaming data prefix"
                );
                continue;
            }

            debug!(
                cycle,
                scanned,
                referenced = referenced.len(),
                snapshot_pins = global_snapshot_pins.len(),
                orphan_candidates,
                recovered_undo_parents = recovered_undo_parents.len(),
                undo_total,
                undo_started,
                undo_replaying,
                undo_completed,
                deleted,
                skipped_young,
                skipped_unknown_age,
                skipped_cap,
                skipped_non_object_key,
                delete_errors,
                "janitor cycle complete"
            );

            let meta = self.meta.clone();
            if let Err(e) = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.release_global_janitor_lock()
            })
            .await
            {
                warn!(cycle, error = %e, "janitor cycle failed to release PG lock");
            }
        }
    }
}
