use super::*;
use crate::snapshot_worker::{
    DataStoreSnapshotArtifactStore, DataStoreSnapshotPayloadReader, SnapshotWorker,
    SnapshotWorkerConfig, SystemSnapshotClock,
};
use studiofs_cache::{LocalPayloadSyncMode, LocalPayloadWriteMode};
use studiofs_meta::snapshot::SnapshotVolumeContext;
use studiofs_meta::snapshot_ports::{
    SnapshotArtifactStore, SnapshotPayloadReader, SnapshotWorkerId,
};

const UPLOAD_HOT_BUFFER_CAP_BYTES: usize = 2 * 1024 * 1024 * 1024;

pub(super) fn derive_upload_hot_buffer_bytes(
    max_dirty_bytes: usize,
    transit_memory_budget_bytes: usize,
    block_size: u32,
) -> usize {
    max_dirty_bytes
        .max(transit_memory_budget_bytes)
        .max(block_size.max(1) as usize)
        .min(UPLOAD_HOT_BUFFER_CAP_BYTES)
}

#[derive(Debug, Clone)]
pub(super) struct VfsBootstrapPlan {
    format_info: FormatInfo,
    memory_cache_budget_bytes: u64,
    pub(super) data_prefix: String,
    cache_runtime_tuning: CacheRuntimeTuning,
    pub(super) minimum_local_backend_budget_bytes: u64,
    pub(super) local_backend_budget_bytes: u64,
    payload_sync_mode: LocalPayloadSyncMode,
    payload_write_mode: LocalPayloadWriteMode,
    pub(super) access_tracker: AccessTrackerConfig,
    readahead_base_width: u32,
    pub(super) readahead_max_width: u32,
    readahead_min_prefetch: u32,
    fetch_aimd_config: crate::aimd::AimdConfig,
    upload_aimd_config: crate::aimd::AimdConfig,
    max_dirty_bytes: usize,
    write_eager_flush_batch_bytes: usize,
    write_eager_flush_drain_chunk_bytes: usize,
    payload_put_concurrency: usize,
    local_sync_group_min_bytes: u64,
    upload_cache_io_foreground_cost_base: u32,
    upload_cache_io_foreground_cost_step: u32,
    upload_fg_read_latency_target_ns: u64,
    upload_fg_read_latency_window: Duration,
    upload_fg_read_latency_sticky_window: Duration,
    janitor_interval: Duration,
    janitor_skip_newer_than: Duration,
    janitor_max_deletes_per_run: u64,
    metadata_invalidation_sync_interval: Duration,
    max_xattr_bytes: usize,
    effective_acl_cache_capacity: usize,
    acl_principal: String,
    upload_delay: Duration,
    upload_requeue_base: Duration,
    upload_requeue_max: Duration,
    upload_stats_interval: Duration,
    shutdown_metadata_flush_timeout: Duration,
    upload_user_idle_grace: Duration,
    upload_worker_count: usize,
    disable_background_slab_flush: bool,
    publish_visibility_timeout: Duration,
    prefetch_concurrency: usize,
    dir_prefetch_max_batch: usize,
    readdir_attr_batch_chunk: usize,
    readdir_attr_batch_max_inodes: usize,
    prefetch_foreground_reserve: usize,
    prefetch_pause_on_neterr: Duration,
}

impl VfsBootstrapPlan {
    pub(super) fn build(
        format_info: FormatInfo,
        cache_size: u64,
        memory_cache_budget_bytes: u64,
        config: &VfsConfigValidated,
    ) -> VfsResult<Self> {
        let data_prefix = normalize_data_prefix(&format_info);
        let block_size = format_info.block_size;
        let cache_runtime_tuning = resolve_cache_runtime_tuning_overrides(
            derive_cache_runtime_tuning(cache_size, block_size, memory_cache_budget_bytes),
            config,
            config.cache_runtime_overrides(),
        );
        let minimum_local_backend_budget_bytes =
            minimum_local_backend_budget_bytes(block_size, cache_runtime_tuning);
        let local_backend_budget_bytes =
            derive_local_backend_budget_bytes(cache_size, block_size, cache_runtime_tuning);
        if local_backend_budget_bytes > cache_size {
            warn!(
                configured_cache_size_bytes = cache_size,
                minimum_required_local_backend_budget_bytes = minimum_local_backend_budget_bytes,
                effective_local_backend_budget_bytes = local_backend_budget_bytes,
                block_size,
                max_dirty_bytes = cache_runtime_tuning.max_dirty_bytes,
                write_eager_flush_batch_bytes = cache_runtime_tuning.write_eager_flush_batch_bytes,
                "configured cache_size is below current local-cache durability requirements; clamping local backend budget upward"
            );
        }
        validate_volume_block_size(block_size)?;
        warn_small_block_addressability(block_size);

        let readahead_base_width = config.readahead_width;
        let readahead_max_width =
            derive_readahead_max_width(config.read_buffer_mb, block_size, readahead_base_width);
        let readahead_min_prefetch = config.readahead_min_prefetch;
        let access_tracker = AccessTrackerConfig {
            block_size,
            readahead_base_width,
            readahead_max_width,
            readahead_min_prefetch,
        };

        let max_dirty_bytes = cache_runtime_tuning.max_dirty_bytes;
        let write_eager_flush_batch_bytes = cache_runtime_tuning.write_eager_flush_batch_bytes;
        let write_eager_flush_drain_chunk_bytes =
            derive_write_eager_flush_drain_chunk_bytes(write_eager_flush_batch_bytes, block_size);
        let payload_put_concurrency =
            derive_payload_put_concurrency(block_size, write_eager_flush_drain_chunk_bytes);
        let local_sync_group_min_bytes = derive_fast_mode_sync_group_min_bytes(
            write_eager_flush_batch_bytes,
            write_eager_flush_drain_chunk_bytes,
        );
        let upload_cache_io_foreground_cost_base =
            derive_upload_cache_io_foreground_cost_base(cache_runtime_tuning.upload_cache_io_slots);
        let upload_cache_io_foreground_cost_step =
            derive_upload_cache_io_foreground_cost_step(cache_runtime_tuning.upload_cache_io_slots);
        let upload_fg_read_latency_window =
            Duration::from_millis(config.upload_fg_read_latency_window_ms);
        let upload_fg_read_latency_sticky_window = Duration::from_millis(
            config
                .upload_fg_read_latency_window_ms
                .saturating_mul(config.upload_fg_read_latency_sticky_windows),
        );

        Ok(Self {
            format_info,
            memory_cache_budget_bytes,
            data_prefix,
            cache_runtime_tuning,
            minimum_local_backend_budget_bytes,
            local_backend_budget_bytes,
            payload_sync_mode: LocalPayloadSyncMode::Fast,
            payload_write_mode: LocalPayloadWriteMode::default_for_mount_path(),
            access_tracker,
            readahead_base_width,
            readahead_max_width,
            readahead_min_prefetch,
            fetch_aimd_config: build_fetch_aimd_config(readahead_max_width, config),
            upload_aimd_config: build_upload_aimd_config(config),
            max_dirty_bytes,
            write_eager_flush_batch_bytes,
            write_eager_flush_drain_chunk_bytes,
            payload_put_concurrency,
            local_sync_group_min_bytes,
            upload_cache_io_foreground_cost_base,
            upload_cache_io_foreground_cost_step,
            upload_fg_read_latency_target_ns: Duration::from_millis(
                config.upload_fg_read_latency_target_ms,
            )
            .as_nanos()
            .clamp(1, u64::MAX as u128) as u64,
            upload_fg_read_latency_window,
            upload_fg_read_latency_sticky_window,
            janitor_interval: Duration::from_secs(config.janitor_interval_secs),
            janitor_skip_newer_than: Duration::from_secs(config.janitor_skip_newer_secs),
            janitor_max_deletes_per_run: config.janitor_max_deletes,
            metadata_invalidation_sync_interval: Duration::from_millis(
                config.metadata_invalidation_sync_interval_ms,
            ),
            max_xattr_bytes: config.max_xattr_bytes,
            effective_acl_cache_capacity: config.effective_acl_cache_capacity,
            acl_principal: config.principal.clone(),
            upload_delay: Duration::from_millis(config.upload_delay_ms),
            upload_requeue_base: Duration::from_millis(config.upload_requeue_base_ms),
            upload_requeue_max: Duration::from_millis(config.upload_requeue_max_ms),
            upload_stats_interval: Duration::from_secs(config.upload_stats_interval_secs),
            shutdown_metadata_flush_timeout: Duration::from_secs(
                config.shutdown_metadata_flush_timeout_secs,
            ),
            upload_user_idle_grace: Duration::from_millis(config.upload_user_idle_grace_ms),
            upload_worker_count: config.upload_worker_count,
            disable_background_slab_flush: config.disable_background_slab_flush,
            publish_visibility_timeout: Duration::from_secs(config.publish_visibility_timeout_secs),
            prefetch_concurrency: config.prefetch_concurrency,
            dir_prefetch_max_batch: config.dir_prefetch_max_batch,
            readdir_attr_batch_chunk: config.readdir_attr_batch_chunk,
            readdir_attr_batch_max_inodes: config.readdir_attr_batch_max_inodes,
            prefetch_foreground_reserve: config.prefetch_foreground_reserve,
            prefetch_pause_on_neterr: Duration::from_millis(config.prefetch_pause_on_neterr_ms),
        })
    }

    pub(super) fn block_size(&self) -> u32 {
        self.format_info.block_size
    }

    fn log_summary(&self, cache_size: u64, config: &VfsConfigValidated) {
        debug!(
            writeback = true,
            cache_size_bytes = cache_size,
            memory_cache_budget_bytes = self.memory_cache_budget_bytes,
            minimum_local_backend_budget_bytes = self.minimum_local_backend_budget_bytes,
            local_backend_budget_bytes = self.local_backend_budget_bytes,
            upload_delay_ms = self.upload_delay.as_millis() as u64,
            requeue_base_ms = self.upload_requeue_base.as_millis() as u64,
            requeue_max_ms = self.upload_requeue_max.as_millis() as u64,
            upload_stats_interval_secs = self.upload_stats_interval.as_secs(),
            upload_cache_io_slots = self.cache_runtime_tuning.upload_cache_io_slots,
            upload_cache_io_foreground_cost_base = self.upload_cache_io_foreground_cost_base,
            upload_cache_io_foreground_cost_step = self.upload_cache_io_foreground_cost_step,
            transit_memory_budget_bytes = self.cache_runtime_tuning.transit_memory_budget_bytes,
            upload_user_idle_grace_ms = self.upload_user_idle_grace.as_millis() as u64,
            upload_fg_read_latency_target_ms = config.upload_fg_read_latency_target_ms,
            upload_fg_read_latency_window_ms =
                self.upload_fg_read_latency_window.as_millis() as u64,
            upload_fg_read_latency_sticky_windows = config.upload_fg_read_latency_sticky_windows,
            max_dirty_bytes = self.max_dirty_bytes,
            write_eager_flush_batch_bytes = self.write_eager_flush_batch_bytes,
            write_eager_flush_drain_chunk_bytes = self.write_eager_flush_drain_chunk_bytes,
            payload_put_concurrency = self.payload_put_concurrency,
            payload_write_mode = ?self.payload_write_mode,
            local_sync_group_min_bytes = self.local_sync_group_min_bytes,
            max_xattr_bytes = self.max_xattr_bytes,
            dentry_cache_capacity = config.dentry_cache_capacity,
            dentry_cache_bytes = config.dentry_cache_bytes,
            path_shortcut_cache_capacity = config.path_shortcut_cache_capacity,
            path_shortcut_cache_bytes = config.path_shortcut_cache_bytes,
            attr_read_cache_capacity = config.attr_read_cache_capacity,
            attr_read_cache_bytes = config.attr_read_cache_bytes,
            readdir_attr_batch_chunk = self.readdir_attr_batch_chunk,
            readdir_attr_batch_max_inodes = self.readdir_attr_batch_max_inodes,
            "upload pipeline enabled"
        );
    }
}

fn remove_retired_ingress_spill(cache_root: &Path) -> VfsResult<()> {
    let retired_dir = cache_root.join("ingress_spill");
    let metadata = match std::fs::symlink_metadata(&retired_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(VfsError::Io(error)),
    };
    let file_type = metadata.file_type();

    if file_type.is_dir() && !file_type.is_symlink() {
        std::fs::remove_dir_all(&retired_dir).map_err(VfsError::Io)?;
    } else {
        std::fs::remove_file(&retired_dir).map_err(VfsError::Io)?;
    }

    warn!(
        retired_path = %retired_dir.display(),
        "startup removed retired ingress_spill state from the old cache layout"
    );
    Ok(())
}

struct VfsBootstrapResources {
    zeroio_runtime_state_dir: PathBuf,
    landing_zone: LandingZone,
    local_payload_backend: Arc<dyn LocalPayloadBackend>,
    cached_store: Arc<CachedStore>,
    readahead: Arc<ReadAheadTracker>,
    dir_prefetch: Arc<DirAccessTracker>,
    prefetch_tx: tokio::sync::mpsc::Sender<PrefetchWorkItem>,
    startup_channels: startup::StartupChannels,
    data_write: Arc<DataStore>,
    qos: Arc<QosController>,
    transit_memory_budget: Arc<TransitMemoryBudget>,
    fetch_scheduler: Arc<crate::fetch::FetchScheduler>,
    upload_scheduler: Arc<crate::upload::UploadScheduler>,
}

impl VfsBootstrapResources {
    fn open(
        data_config: &DataStoreConfig,
        cache_dir: &str,
        plan: &VfsBootstrapPlan,
        config: &VfsConfigValidated,
        cache_runtime: Option<PreparedLocalPayloadRuntime>,
    ) -> VfsResult<Self> {
        remove_retired_ingress_spill(Path::new(cache_dir))?;

        let zeroio_runtime_state_dir =
            PathBuf::from(cache_dir).join(super::ZEROIO_RUNTIME_STATE_DIR);
        std::fs::create_dir_all(&zeroio_runtime_state_dir).map_err(VfsError::Io)?;
        let landing_zone = LandingZone::open(zeroio_runtime_state_dir.join("landing-zone"))
            .map_err(VfsError::Io)?;

        let opened_local_backend = match cache_runtime {
            Some(cache_runtime) => {
                open_prepared_local_payload_backend_with_budget_sync_and_write_mode(
                    cache_runtime,
                    &plan.format_info.uuid,
                    plan.local_backend_budget_bytes,
                    plan.payload_sync_mode,
                    plan.payload_write_mode,
                )
            }
            None => open_local_payload_backend_with_budget_sync_and_write_mode(
                Path::new(cache_dir),
                &plan.format_info.uuid,
                plan.local_backend_budget_bytes,
                plan.payload_sync_mode,
                plan.payload_write_mode,
            ),
        }
        .map_err(|source| map_open_local_payload_backend_error(cache_dir, source))?;
        if opened_local_backend.detection.has_runtime_layout() {
            debug!(
                cache_root = %opened_local_backend.detection.layout.cache_root.display(),
                runtime_root = %opened_local_backend.detection.layout.runtime_root.display(),
                "using current file-per-payload-object local backend under the volume cache root"
            );
        }
        let local_payload_backend = opened_local_backend.backend;
        let upload_hot_bytes = derive_upload_hot_buffer_bytes(
            plan.max_dirty_bytes,
            plan.cache_runtime_tuning.transit_memory_budget_bytes,
            plan.block_size(),
        );

        let (cached_store, upload_rx) = CachedStore::open(
            CachedStoreConfig::new(cache_dir).with_upload_hot_bytes(upload_hot_bytes),
            local_payload_backend.clone(),
        )
        .map_err(VfsError::Io)?;

        let readahead = Arc::new(ReadAheadTracker::new_with_bounds(
            plan.readahead_base_width,
            plan.readahead_max_width,
            plan.readahead_min_prefetch,
        ));
        let dir_prefetch = Arc::new(DirAccessTracker::new(2));
        let (prefetch_tx, prefetch_rx) = tokio::sync::mpsc::channel::<PrefetchWorkItem>(
            super::prefetch::PREFETCH_INGRESS_CAPACITY,
        );

        let data_read = Arc::new(
            DataStore::with_profile(data_config, IoProfile::Read)
                .map_err(|e| VfsError::Other(format!("failed to create read data store: {e}")))?,
        );
        let data_write = Arc::new(
            DataStore::with_profile(data_config, IoProfile::Write)
                .map_err(|e| VfsError::Other(format!("failed to create write data store: {e}")))?,
        );

        let qos = Arc::new(QosController::from_config(config));
        let transit_memory_budget =
            TransitMemoryBudget::new(plan.cache_runtime_tuning.transit_memory_budget_bytes);
        let fetch_scheduler = Arc::new(crate::fetch::FetchScheduler::new(
            data_read,
            cached_store.clone(),
            plan.fetch_aimd_config.clone(),
            plan.prefetch_foreground_reserve,
            qos.clone(),
            transit_memory_budget.clone(),
            config,
        ));
        let upload_scheduler = Arc::new(crate::upload::UploadScheduler::new(
            data_write.clone(),
            plan.upload_aimd_config.clone(),
            config,
        ));

        Ok(Self {
            zeroio_runtime_state_dir,
            landing_zone,
            local_payload_backend,
            cached_store,
            readahead,
            dir_prefetch,
            prefetch_tx,
            startup_channels: startup::StartupChannels {
                prefetch_rx,
                upload_rx,
            },
            data_write,
            qos,
            transit_memory_budget,
            fetch_scheduler,
            upload_scheduler,
        })
    }
}

struct VfsBootstrap {
    meta: Arc<dyn VfsMeta>,
    config: VfsConfigValidated,
    plan: VfsBootstrapPlan,
    resources: VfsBootstrapResources,
    cache_dir: PathBuf,
    read_only_mount: bool,
    snapshot_mount_fence: Option<SnapshotMountFence>,
}

impl VfsBootstrap {
    fn prepare<M>(
        meta: Arc<M>,
        data_config: &DataStoreConfig,
        cache_dir: &str,
        cache_size: u64,
        memory_cache_budget_bytes: u64,
        config: VfsConfigValidated,
        cache_runtime: Option<PreparedLocalPayloadRuntime>,
        read_only_mount: bool,
        snapshot_mount_fence: Option<SnapshotMountFence>,
    ) -> VfsResult<Self>
    where
        M: ?Sized + VfsMeta + 'static,
    {
        let meta = into_vfs_meta(meta);
        let plan = VfsBootstrapPlan::build(
            meta.get_format_info()?,
            cache_size,
            memory_cache_budget_bytes,
            &config,
        )?;
        plan.log_summary(cache_size, &config);
        let resources =
            VfsBootstrapResources::open(data_config, cache_dir, &plan, &config, cache_runtime)?;
        Ok(Self {
            meta,
            config,
            plan,
            resources,
            cache_dir: PathBuf::from(cache_dir),
            read_only_mount,
            snapshot_mount_fence,
        })
    }

    fn assemble(self) -> Arc<StudioVfs> {
        let Self {
            meta,
            config,
            plan,
            resources,
            cache_dir,
            read_only_mount,
            snapshot_mount_fence,
        } = self;
        let block_size = plan.block_size();
        let upload_cache_io_slots = plan.cache_runtime_tuning.upload_cache_io_slots;
        let distributed_flock = (!read_only_mount)
            .then(|| {
                meta.invalidation_sender_id().map(|_| DistributedFlock {
                    lease_ttl: Duration::from_secs(config.lock_ttl_secs),
                    retry_base_ms: config.lock_retry_base_ms,
                    retry_max_ms: config.lock_retry_max_ms,
                })
            })
            .flatten();
        let snapshot_worker = if read_only_mount {
            None
        } else {
            build_snapshot_worker(
                meta.as_ref(),
                resources.data_write.clone(),
                snapshot_materializer_root(&cache_dir),
            )
        };

        Arc::new(StudioVfs {
            meta: MetaExecutor::new(meta),
            read_only_mount,
            snapshot_mount_fence,
            data_write: resources.data_write,
            snapshot_worker,
            local_payload_backend: resources.local_payload_backend,
            cached_store: resources.cached_store.clone(),
            block_size,
            data_prefix: plan.data_prefix,
            acl_principal: plan.acl_principal,
            effective_acl_cache_capacity: plan.effective_acl_cache_capacity,
            state: Arc::new(VfsState::new(
                resources.cached_store,
                block_size,
                &config,
                plan.upload_cache_io_foreground_cost_base,
            )),
            upload_scheduler: resources.upload_scheduler,
            upload_delay: plan.upload_delay,
            upload_requeue_base: plan.upload_requeue_base,
            upload_requeue_max: plan.upload_requeue_max,
            upload_stats_interval: plan.upload_stats_interval,
            shutdown_metadata_flush_timeout: plan.shutdown_metadata_flush_timeout,
            upload_cache_io_slots: Arc::new(Semaphore::new(upload_cache_io_slots)),
            upload_cache_io_slots_max: {
                debug_assert!(upload_cache_io_slots <= u32::MAX as usize);
                upload_cache_io_slots as u32
            },
            transit_memory_budget: resources.transit_memory_budget,
            qos: resources.qos,
            upload_cache_io_foreground_cost_base: plan.upload_cache_io_foreground_cost_base,
            upload_cache_io_foreground_cost_step: plan.upload_cache_io_foreground_cost_step,
            upload_fg_read_latency_target_ns: plan.upload_fg_read_latency_target_ns,
            upload_fg_read_latency_window: plan.upload_fg_read_latency_window,
            upload_fg_read_latency_sticky_window: plan.upload_fg_read_latency_sticky_window,
            access_tracker: plan.access_tracker,
            upload_user_idle_grace: plan.upload_user_idle_grace,
            upload_worker_count: plan.upload_worker_count,
            disable_background_slab_flush: plan.disable_background_slab_flush,
            publish_visibility_timeout: plan.publish_visibility_timeout,
            readahead: resources.readahead,
            dir_prefetch: resources.dir_prefetch,
            prefetch_tx: resources.prefetch_tx,
            startup: startup::StartupState::new(
                resources.startup_channels.prefetch_rx,
                resources.startup_channels.upload_rx,
                plan.metadata_invalidation_sync_interval,
            ),
            max_dirty_bytes: AtomicUsize::new(plan.max_dirty_bytes),
            write_eager_flush_batch_bytes: plan.write_eager_flush_batch_bytes,
            write_buffer_pool: BufferPool::new(
                block_size as usize,
                plan.payload_put_concurrency
                    .max(WRITE_BUFFER_POOL_MIN_BUFFERS)
                    .saturating_mul(WRITE_BUFFER_POOL_BUFFER_MULTIPLIER),
            ),
            write_eager_flush_drain_chunk_bytes: plan.write_eager_flush_drain_chunk_bytes,
            payload_put_concurrency: plan.payload_put_concurrency,
            local_sync_group_min_bytes: plan.local_sync_group_min_bytes,
            max_xattr_bytes: plan.max_xattr_bytes,
            zeroio_runtime_state_dir: resources.zeroio_runtime_state_dir,
            landing_zone: resources.landing_zone,
            handle_table: HandleTable::new(),
            os_cache: Arc::new(Mutex::new(OsCacheState::default())),
            janitor_interval: plan.janitor_interval,
            janitor_skip_newer_than: plan.janitor_skip_newer_than,
            janitor_max_deletes_per_run: plan.janitor_max_deletes_per_run,
            prefetch_concurrency: plan.prefetch_concurrency,
            dir_prefetch_max_batch: plan.dir_prefetch_max_batch,
            readdir_attr_batch_chunk: plan.readdir_attr_batch_chunk,
            readdir_attr_batch_max_inodes: plan.readdir_attr_batch_max_inodes,
            prefetch_pause_on_neterr: plan.prefetch_pause_on_neterr,
            fetch_scheduler: resources.fetch_scheduler,
            distributed_flock,
            kernel_invalidator: RwLock::new(None),
            deferred_kernel_invalidations: DashSet::new(),
            deferred_kernel_inode_attrs: DashSet::new(),
            write_path_backend_tag: "local-payload-runtime".to_string(),
            write_path_started_at: Instant::now(),
            runtime_stats_file_time_nanos: InodeAttr::now_nanos(),
            known_file_lock_overrides: Mutex::new(HashMap::new()),
            #[cfg(test)]
            metadata_authority_override: Mutex::new(None),
            #[cfg(test)]
            fail_next_inline_dirty_attr_persist: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fail_next_size_change_setattr_before_flush: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fail_next_size_change_truncate_after_first_removed_block:
                std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fail_next_deferred_materialization_prefetch: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn finish(self) -> VfsResult<Arc<StudioVfs>> {
        let vfs = self.assemble();
        vfs.finalize_constructor_bootstrap()?;
        Ok(vfs)
    }
}

fn build_snapshot_worker(
    meta: &dyn VfsMeta,
    data_store: Arc<DataStore>,
    materializer_root: PathBuf,
) -> Option<Arc<SnapshotWorker>> {
    let jobs = meta.snapshot_job_store()?;
    let sources = meta.snapshot_source_reader()?;
    let format_info = meta.get_format_info().ok()?;
    let postgres = format_info.metadata.postgres?;
    let volume =
        SnapshotVolumeContext::new(postgres.volume_id, format_info.name, format_info.uuid).ok()?;
    let payloads: Arc<dyn SnapshotPayloadReader> =
        Arc::new(DataStoreSnapshotPayloadReader::new(data_store.clone(), volume.clone()).ok()?);
    let artifacts: Arc<dyn SnapshotArtifactStore> =
        Arc::new(DataStoreSnapshotArtifactStore::new(data_store, volume.clone()).ok()?);
    let worker_id = SnapshotWorkerId::new(format!(
        "vfs-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ))
    .ok()?;
    SnapshotWorker::new(
        volume,
        worker_id,
        jobs,
        sources,
        payloads,
        artifacts,
        Arc::new(SystemSnapshotClock),
        SnapshotWorkerConfig::new(materializer_root),
    )
    .ok()
    .map(Arc::new)
}

fn snapshot_materializer_root(cache_dir: &Path) -> PathBuf {
    cache_dir.join("snapshot-v2").join("materializer")
}

impl StudioVfs {
    fn finalize_constructor_bootstrap(&self) -> VfsResult<()> {
        if self.read_only_mount {
            return Ok(());
        }
        let recovered_after_unclean_shutdown = self.begin_zeroio_session()?;
        let landing_recovery_inodes = if recovered_after_unclean_shutdown {
            match self.landing_zone.recoverable_inodes() {
                Ok(inodes) => Some(inodes),
                Err(error) => {
                    warn!(error = %error, "startup recovery could not inspect durable landing records");
                    None
                }
            }
        } else {
            None
        };
        let pending_uploads = self.cached_store.snapshot_pending_uploads();
        let attrs = self.sync_meta().list_inode_attrs()?;
        self.replay_dirty_uploads_after_unclean_shutdown(
            pending_uploads.as_slice(),
            attrs.as_slice(),
            recovered_after_unclean_shutdown,
            landing_recovery_inodes,
        );
        Ok(())
    }

    /// Create a new VFS instance using the current local payload backend contract.
    pub fn new<M>(
        meta: Arc<M>,
        data_config: &DataStoreConfig,
        cache_dir: &str,
        cache_size: u64,
        memory_cache_budget_bytes: u64,
        config: VfsConfigValidated,
    ) -> VfsResult<Arc<Self>>
    where
        M: ?Sized + VfsMeta + 'static,
    {
        Self::new_impl(
            meta,
            data_config,
            cache_dir,
            cache_size,
            memory_cache_budget_bytes,
            config,
            None,
            false,
            None,
        )
    }

    /// Create a new VFS instance reusing a prepared local-cache runtime.
    pub fn new_with_prepared_cache_runtime<M>(
        meta: Arc<M>,
        data_config: &DataStoreConfig,
        cache_dir: &str,
        cache_size: u64,
        memory_cache_budget_bytes: u64,
        config: VfsConfigValidated,
        cache_runtime: PreparedLocalPayloadRuntime,
    ) -> VfsResult<Arc<Self>>
    where
        M: ?Sized + VfsMeta + 'static,
    {
        Self::new_impl(
            meta,
            data_config,
            cache_dir,
            cache_size,
            memory_cache_budget_bytes,
            config,
            Some(cache_runtime),
            false,
            None,
        )
    }

    /// Create a read-only VFS fenced by the durable lease of its snapshot
    /// mount. The fence is checked before every metadata admission decision.
    pub(crate) fn new_read_only_with_snapshot_mount_fence<M>(
        meta: Arc<M>,
        data_config: &DataStoreConfig,
        cache_dir: &str,
        cache_size: u64,
        memory_cache_budget_bytes: u64,
        config: VfsConfigValidated,
        snapshot_mount_fence: SnapshotMountFence,
    ) -> VfsResult<Arc<Self>>
    where
        M: ?Sized + VfsMeta + 'static,
    {
        Self::new_impl(
            meta,
            data_config,
            cache_dir,
            cache_size,
            memory_cache_budget_bytes,
            config,
            None,
            true,
            Some(snapshot_mount_fence),
        )
    }

    fn new_impl<M>(
        meta: Arc<M>,
        data_config: &DataStoreConfig,
        cache_dir: &str,
        cache_size: u64,
        memory_cache_budget_bytes: u64,
        config: VfsConfigValidated,
        cache_runtime: Option<PreparedLocalPayloadRuntime>,
        read_only_mount: bool,
        snapshot_mount_fence: Option<SnapshotMountFence>,
    ) -> VfsResult<Arc<Self>>
    where
        M: ?Sized + VfsMeta + 'static,
    {
        VfsBootstrap::prepare(
            meta,
            data_config,
            cache_dir,
            cache_size,
            memory_cache_budget_bytes,
            config,
            cache_runtime,
            read_only_mount,
            snapshot_mount_fence,
        )?
        .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::{AccessClass, AccessPolicy};
    use std::path::Path;
    use std::sync::Arc;
    #[cfg(unix)]
    use std::{fs, os::unix::fs::symlink};
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, MetadataConfig};
    use studiofs_meta::{MetaEngine, MetaFormat, RedbMetaEngine};
    use tokio::runtime::Runtime;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("namespace-resolver-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("namespace-resolver-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            encryption: studiofs_meta::EncryptionConfig::new(
                [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
            ),
            block_size: 4 * 1024 * 1024,
        }
    }

    #[test]
    fn snapshot_materializer_root_is_below_configured_cache_directory() {
        assert_eq!(
            snapshot_materializer_root(Path::new("/cache/studiofs")),
            Path::new("/cache/studiofs/snapshot-v2/materializer")
        );
    }

    #[test]
    fn open_local_payload_backend_detection_errors_map_to_structured_vfs_error() {
        let source = OpenLocalPayloadBackendError::DetectCacheBackend {
            cache_root: "ignored".to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated permission error",
            ),
        };
        let mapped = map_open_local_payload_backend_error("E:\\cache-root", source);
        match mapped {
            VfsError::LocalPayloadRuntimeDetection { cache_root, source } => {
                assert_eq!(cache_root, "E:\\cache-root");
                assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }

    #[test]
    fn open_local_payload_backend_provision_errors_surface_clear_runtime_message() {
        let source = OpenLocalPayloadBackendError::ProvisionLocalPayloadLayout {
            cache_root: "E:\\cache-root".to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated permission error",
            ),
        };
        let mapped = map_open_local_payload_backend_error("E:\\cache-root", source);
        match mapped {
            VfsError::Other(message) => {
                assert!(message.contains("failed to provision local payload runtime layout"));
                assert!(message.contains("E:\\cache-root"));
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }

    #[test]
    fn studio_vfs_new_selects_local_payload_runtime_for_runtime_root_layout() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info("local-payload-runtime-bootstrap");
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);
        let cache_dir = tmp.path().join("cache");
        let layout = studiofs_cache::runtime::LocalPayloadLayout::from_cache_root(&cache_dir);
        layout.provision().expect("provision runtime layout");

        let rt = Runtime::new().expect("runtime");
        let _guard = rt.enter();
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");

        assert!(
            vfs.local_payload_backend()
                .as_any()
                .is::<studiofs_cache::runtime::LocalPayloadBackendRuntime>(),
            "layout-only runtime root should select the current local payload runtime backend"
        );
    }

    #[test]
    fn studio_vfs_new_does_not_spawn_background_workers() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info("new-no-start");
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);
        let cache_dir = tmp.path().join("cache");
        let rt = Runtime::new().expect("runtime");
        let _guard = rt.enter();

        let vfs = StudioVfs::new(
            meta,
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");

        let startup = vfs.startup.lifecycle.lock();
        assert!(
            matches!(startup.status, startup::StartupStatus::Uninitialized),
            "constructor should keep startup lifecycle uninitialized"
        );
        assert!(
            vfs.state.shutdown.background_tasks.lock().is_empty(),
            "constructor should not spawn background tasks"
        );
        assert!(
            vfs.state.shutdown.uploader_task.lock().is_none(),
            "constructor should not spawn uploader task"
        );
        drop(startup);
    }

    #[test]
    fn studio_vfs_new_accepts_metaengine_compatibility_shim() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info("new-metaengine-shim");
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);
        let cache_dir = tmp.path().join("cache");
        let meta: Arc<dyn MetaEngine> = meta;
        let rt = Runtime::new().expect("runtime");
        let _guard = rt.enter();

        let vfs = StudioVfs::new(
            meta,
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs from MetaEngine compatibility shim");

        assert_eq!(
            vfs.sync_meta().get_format_info().expect("format info").uuid,
            format_info.uuid
        );
    }

    #[test]
    fn studio_vfs_new_applies_bootstrap_plan_fields_to_constructed_vfs() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let mut format_info = test_format_info("constructor-bootstrap-fields");
        format_info.data_prefix = "tenant/demo".to_string();
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);
        let cache_dir = tmp.path().join("cache");
        let rt = Runtime::new().expect("runtime");
        let _guard = rt.enter();

        let vfs = StudioVfs::new(
            meta,
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig {
                principal: "dev-user".to_string(),
                ..VfsConfig::default()
            }
            .try_into()
            .expect("custom VFS config should validate"),
        )
        .expect("create vfs");

        assert_eq!(
            vfs.data_prefix, "tenant/demo/",
            "constructor should materialize normalized data_prefix from the bootstrap plan"
        );
        assert_eq!(
            vfs.acl_principal, "dev-user",
            "constructor should carry bootstrap ACL principal into the assembled VFS"
        );
        assert_eq!(
            vfs.write_path_backend_tag, "local-payload-runtime",
            "constructor should preserve the local-payload-runtime write-path backend tag in the assembled VFS"
        );
    }

    #[test]
    fn build_vfs_bootstrap_plan_normalizes_data_prefix_and_derives_tuning() {
        let config: VfsConfigValidated = VfsConfig::default()
            .try_into()
            .expect("default VFS config should validate");

        let mut empty_prefix = test_format_info("bootstrap-empty-prefix");
        empty_prefix.data_prefix.clear();
        let empty_plan = VfsBootstrapPlan::build(
            empty_prefix.clone(),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            &config,
        )
        .expect("build bootstrap plan with empty prefix");
        assert_eq!(
            empty_plan.data_prefix,
            format!("{}/objects/", empty_prefix.name),
            "empty data prefix should normalize to <volume>/objects/"
        );

        let mut missing_slash = test_format_info("bootstrap-missing-slash");
        missing_slash.data_prefix = "tenant/demo".to_string();
        let missing_slash_plan =
            VfsBootstrapPlan::build(missing_slash, 64 * 1024 * 1024, 32 * 1024 * 1024, &config)
                .expect("build bootstrap plan with missing trailing slash");
        assert_eq!(
            missing_slash_plan.data_prefix, "tenant/demo/",
            "non-empty data prefix should end with a single trailing slash"
        );
        assert_eq!(
            missing_slash_plan.access_tracker.block_size,
            missing_slash_plan.block_size(),
            "bootstrap plan should thread normalized block size into access classification"
        );
        assert!(
            missing_slash_plan.local_backend_budget_bytes
                >= missing_slash_plan.minimum_local_backend_budget_bytes,
            "bootstrap plan should never under-provision local backend budget"
        );
        assert!(
            missing_slash_plan.readahead_max_width >= missing_slash_plan.readahead_base_width,
            "bootstrap plan should preserve at least the configured readahead width"
        );
        assert_eq!(
            missing_slash_plan.payload_write_mode,
            LocalPayloadWriteMode::default_for_mount_path(),
            "mount bootstrap should use the platform default payload write path"
        );
    }

    #[test]
    fn build_vfs_bootstrap_plan_disables_prefetch_for_zero_readahead_width() {
        let config: VfsConfigValidated = VfsConfig {
            readahead_width: 0,
            ..VfsConfig::default()
        }
        .try_into()
        .expect("zero readahead width should validate");
        let plan = VfsBootstrapPlan::build(
            test_format_info("bootstrap-readahead-disabled"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            &config,
        )
        .expect("build bootstrap plan with readahead disabled");

        assert_eq!(plan.access_tracker.readahead_base_width, 0);
        assert_eq!(plan.readahead_max_width, 0);
        assert!(
            !AccessPolicy::from_class(AccessClass::SequentialMediaRead, plan.access_tracker)
                .prefetch_enabled
        );
    }

    #[test]
    fn build_vfs_bootstrap_plan_rejects_oversized_block_size() {
        let config: VfsConfigValidated = VfsConfig::default()
            .try_into()
            .expect("default VFS config should validate");
        let mut format_info = test_format_info("bootstrap-invalid-block-size");
        format_info.block_size = (CLOUD_FRAGMENT_SIZE * CLOUD_FRAGMENT_COUNT + 1)
            .try_into()
            .expect("fragment capacity should fit in u32 for tests");

        let error =
            VfsBootstrapPlan::build(format_info, 64 * 1024 * 1024, 32 * 1024 * 1024, &config)
                .expect_err("oversized block size should fail before bootstrap IO starts");

        assert!(
            error
                .to_string()
                .contains("exceeds 4x4 cloud fragment capacity"),
            "bootstrap plan should reject volumes whose block size cannot fit in the cloud fragment layout"
        );
    }

    #[test]
    fn build_vfs_bootstrap_plan_rejects_zero_block_size() {
        let config: VfsConfigValidated = VfsConfig::default()
            .try_into()
            .expect("default VFS config should validate");
        let mut format_info = test_format_info("bootstrap-zero-block-size");
        format_info.block_size = 0;

        let error =
            VfsBootstrapPlan::build(format_info, 64 * 1024 * 1024, 32 * 1024 * 1024, &config)
                .expect_err("zero block size should fail before bootstrap IO starts");

        assert!(
            error.to_string().contains("block_size must be non-zero"),
            "bootstrap plan should reject volumes whose block size is zero"
        );
    }

    #[test]
    fn build_vfs_bootstrap_plan_clamps_local_backend_budget_for_tiny_cache() {
        let config: VfsConfigValidated = VfsConfig::default()
            .try_into()
            .expect("default VFS config should validate");
        let tiny_cache_size = 4 * 1024;
        let plan = VfsBootstrapPlan::build(
            test_format_info("bootstrap-tiny-cache"),
            tiny_cache_size,
            32 * 1024 * 1024,
            &config,
        )
        .expect("build bootstrap plan for tiny cache budget");

        assert!(
            plan.local_backend_budget_bytes > tiny_cache_size,
            "bootstrap plan should clamp tiny cache budgets upward to protect local durability"
        );
        assert_eq!(
            plan.local_backend_budget_bytes, plan.minimum_local_backend_budget_bytes,
            "when clamped, bootstrap plan should use the computed minimum local backend budget"
        );
    }

    #[cfg(unix)]
    #[test]
    fn remove_retired_ingress_spill_removes_broken_symlink() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let cache_root = tmp.path().join("cache");
        fs::create_dir_all(&cache_root).expect("create cache root");
        let retired_path = cache_root.join("ingress_spill");
        let missing_target = tmp.path().join("missing-ingress-target");
        symlink(&missing_target, &retired_path).expect("create broken retired symlink");

        assert!(
            fs::symlink_metadata(&retired_path).is_ok(),
            "test setup should create a broken symlink entry"
        );

        remove_retired_ingress_spill(&cache_root)
            .expect("broken retired ingress symlink should be removed");

        assert!(
            fs::symlink_metadata(&retired_path).is_err(),
            "cleanup should remove the broken ingress_spill symlink itself"
        );
    }

    #[cfg(unix)]
    #[test]
    fn remove_retired_ingress_spill_removes_symlink_without_touching_target() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let cache_root = tmp.path().join("cache");
        fs::create_dir_all(&cache_root).expect("create cache root");
        let target_dir = tmp.path().join("retired-target");
        fs::create_dir_all(&target_dir).expect("create symlink target");
        let sentinel = target_dir.join("sentinel.txt");
        fs::write(&sentinel, b"keep").expect("write target sentinel");

        let retired_path = cache_root.join("ingress_spill");
        symlink(&target_dir, &retired_path).expect("create retired symlink");

        remove_retired_ingress_spill(&cache_root)
            .expect("retired ingress symlink should be removed");

        assert!(
            fs::symlink_metadata(&retired_path).is_err(),
            "cleanup should remove the cache-root symlink entry"
        );
        assert!(
            target_dir.exists(),
            "cleanup should not remove the symlink target"
        );
        assert!(
            sentinel.exists(),
            "cleanup should leave target contents intact"
        );
    }
}
