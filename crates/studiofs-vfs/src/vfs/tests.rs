use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use studiofs_meta::engine::{MetaBlocks, MetaInodes};
use studiofs_meta::schema::{
    CURRENT_FORMAT_VERSION, FormatInfo, InodeLifecycleState, MetadataConfig, PayloadRetentionClass,
    ROOT_INO,
};
use studiofs_meta::{MetaEngine, MetaFormat, MetaFsOps, RedbMetaEngine};
use tempfile::TempDir;
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
        block_size: 4 * 1024 * 1024,
    }
}

fn make_unstarted_test_vfs(
    tmp: &TempDir,
    volume_suffix: &str,
) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
    let db_path = tmp.path().join("meta.redb");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
    meta.init_root(0, 0).expect("init root");

    let format_info = test_format_info(volume_suffix);
    meta.set_format_info(&format_info).expect("set format info");
    let data_config = DataStoreConfig::from(&format_info);

    let cache_dir = tmp.path().join("cache");
    let rt = Arc::new(Runtime::new().expect("runtime"));
    let _guard = rt.enter();
    let vfs = StudioVfs::new(
        meta.clone(),
        &data_config,
        cache_dir.to_str().expect("cache dir str"),
        64 * 1024 * 1024,
        32 * 1024 * 1024,
        VfsConfig::default()
            .try_into()
            .expect("default VFS config should validate"),
    )
    .expect("create vfs");
    (rt, vfs, meta)
}

fn make_test_vfs(
    tmp: &TempDir,
    volume_suffix: &str,
) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
    let (rt, vfs, meta) = make_unstarted_test_vfs(tmp, volume_suffix);
    let _guard = rt.enter();
    vfs.start().expect("start vfs background workers");
    (rt, vfs, meta)
}

fn reopen_existing_test_vfs(tmp: &TempDir) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
    let db_path = tmp.path().join("meta.redb");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("reopen RedbMetaEngine"));
    let format_info = meta.get_format_info().expect("load format info");
    let data_config = DataStoreConfig::from(&format_info);
    let cache_dir = tmp.path().join("cache");
    let rt = Arc::new(Runtime::new().expect("runtime"));
    let _guard = rt.enter();
    let vfs = StudioVfs::new(
        meta.clone(),
        &data_config,
        cache_dir.to_str().expect("cache dir str"),
        64 * 1024 * 1024,
        32 * 1024 * 1024,
        VfsConfig::default()
            .try_into()
            .expect("default VFS config should validate"),
    )
    .expect("reopen vfs");
    (rt, vfs, meta)
}

fn mark_all_file_blocks_uploaded(
    vfs: &Arc<StudioVfs>,
    meta: &Arc<RedbMetaEngine>,
    ino: Ino,
) -> (u64, Vec<(u32, BlockRef)>) {
    let attr = meta.get_attr(ino).expect("load inode attrs");
    let blocks = meta.get_file_blocks(ino).expect("load file blocks");
    for (block_index, block) in &blocks {
        vfs.local_block_backend().mark_uploaded(block.object_id);
        assert!(
            meta.mark_file_block_uploaded_if_matches(ino, *block_index, attr.content_epoch, block)
                .expect("mark block uploaded"),
            "expected metadata upload mark to apply for every block"
        );
    }
    (attr.content_epoch, blocks)
}

fn publish_current_manifest(
    meta: &Arc<RedbMetaEngine>,
    ino: Ino,
    content_epoch: u64,
    blocks: &[(u32, BlockRef)],
) {
    assert!(
        meta.publish_file_manifest_if_matches(ino, content_epoch, blocks)
            .expect("publish current manifest"),
        "expected manifest publish to apply"
    );
}

#[test]
fn startup_replay_does_not_override_current_epoch_live_extent_with_stale_pending_upload() {
    let tmp = TempDir::new().expect("tempdir");
    let (_rt, vfs, meta) = make_test_vfs(&tmp, "startup-replay-stale-pending");

    let created = vfs
        .create(1, b"startup-stale-pending.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = 4096;
    attr.committed_size = 4096;
    attr.content_epoch = 5;
    attr.committed_content_epoch = 4;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.set_file_block(ino, 0, &BlockRef::new_partial(9001, 4096))
        .expect("persist current file block");

    let pending_uploads = vec![PendingUpload {
        ino,
        offset: 0,
        object_id: 7001,
        content_epoch: 4,
        created_unix_ms: 100,
        not_before_unix_ms: 0,
        retry_count: 0,
    }];

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr]);

    assert_eq!(
        vfs.live_extent_records_for_inode(ino),
        vec![(0, 9001, 5)],
        "startup replay must preserve the current metadata epoch instead of overwriting it with stale pending uploads"
    );
}

#[test]
fn upload_pipeline_runtime_events_only_emit_active_samples_and_transitions() {
    assert_eq!(
        StudioVfs::upload_pipeline_runtime_event(true, false, true),
        Some(("became_active", "upload pipeline active"))
    );
    assert_eq!(
        StudioVfs::upload_pipeline_runtime_event(false, true, false),
        Some(("became_idle", "upload pipeline idle"))
    );
    assert_eq!(
        StudioVfs::upload_pipeline_runtime_event(false, false, true),
        Some(("sample", "upload pipeline sample"))
    );
    assert_eq!(
        StudioVfs::upload_pipeline_runtime_event(false, false, false),
        None
    );
}

#[test]
fn startup_replay_keeps_metadata_live_extent_when_pending_upload_has_same_epoch() {
    let tmp = TempDir::new().expect("tempdir");
    let (_rt, vfs, meta) = make_test_vfs(&tmp, "startup-replay-same-epoch");

    let created = vfs
        .create(1, b"startup-same-epoch.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = 4096;
    attr.committed_size = 4096;
    attr.content_epoch = 5;
    attr.committed_content_epoch = 4;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.set_file_block(ino, 0, &BlockRef::new_partial(9001, 4096))
        .expect("persist current file block");

    let pending_uploads = vec![PendingUpload {
        ino,
        offset: 0,
        object_id: 7001,
        content_epoch: 5,
        created_unix_ms: 100,
        not_before_unix_ms: 0,
        retry_count: 0,
    }];

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr]);

    assert_eq!(
        vfs.live_extent_records_for_inode(ino),
        vec![(0, 9001, 5)],
        "startup replay must keep the metadata-derived block when pending upload only matches the same epoch"
    );
}

#[test]
fn startup_replay_prefers_overlay_metadata_live_extent_over_base_for_same_epoch() {
    let tmp = TempDir::new().expect("tempdir");
    let (_rt, vfs, meta) = make_test_vfs(&tmp, "startup-replay-overlay-same-epoch");

    let created = vfs
        .create(1, b"startup-overlay-same-epoch.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = 4096;
    attr.committed_size = 4096;
    attr.content_epoch = 5;
    attr.committed_content_epoch = 4;
    attr.upload_state = MetaUploadState::Updating;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.set_file_block(ino, 0, &BlockRef::new_partial(9001, 4096))
        .expect("persist base file block");
    meta.flush_regular_file_overlay_slices_with_attr(
        ino,
        &[(
            studiofs_meta::schema::ChunkId(0),
            vec![studiofs_meta::schema::SliceRecord {
                slice_id: studiofs_meta::schema::SliceId::default(),
                order: studiofs_meta::schema::SliceOrder::default(),
                range: studiofs_meta::schema::SliceRange::new(0, 4096)
                    .expect("overlay slice range"),
                block: Some(studiofs_meta::schema::BlockRef {
                    object_id: 9002,
                    offset: 0,
                    length: 4096,
                    flags: studiofs_meta::schema::BlockFlags::PARTIAL_BLOCK,
                }),
                block_state: studiofs_meta::schema::BlockRefState::LocalOnly,
            }],
        )],
        Some(&attr),
    )
    .expect("persist overlay slice state");

    let pending_uploads = vec![PendingUpload {
        ino,
        offset: 0,
        object_id: 9002,
        content_epoch: 5,
        created_unix_ms: 100,
        not_before_unix_ms: 0,
        retry_count: 0,
    }];

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr]);

    assert_eq!(
        vfs.live_extent_records_for_inode(ino),
        vec![(0, 9002, 5)],
        "startup replay must prefer overlay metadata over the published base when rebuilding same-epoch live extents"
    );
}

#[test]
fn startup_replay_removes_orphan_dirty_blocks_and_clears_pending_evictions() {
    let tmp = TempDir::new().expect("tempdir");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "startup-replay-orphan-cleanup");
    let object_id = 991_337;

    vfs.local_block_backend()
        .put(object_id, b"orphan-dirty-block", false)
        .expect("persist orphan dirty block");
    vfs.local_block_backend()
        .sync_data()
        .expect("sync orphan dirty block");
    vfs.state.upload.pending_evictions.lock().insert(object_id);

    assert!(
        vfs.local_block_backend()
            .dirty_object_ids()
            .contains(&object_id),
        "test setup should expose orphan as dirty before replay"
    );
    assert!(
        vfs.state
            .upload
            .pending_evictions
            .lock()
            .contains(&object_id),
        "test setup should seed pending eviction before replay"
    );

    vfs.replay_dirty_uploads_from_local_backend(&[], &[]);

    assert!(
        !vfs.local_block_backend().contains(object_id),
        "startup replay should remove orphan dirty block without metadata reference"
    );
    assert!(
        !vfs.local_block_backend()
            .dirty_object_ids()
            .contains(&object_id),
        "startup replay should clear orphan dirty block from dirty index"
    );
    assert!(
        !vfs.state
            .upload
            .pending_evictions
            .lock()
            .contains(&object_id),
        "startup replay should clear pending eviction bookkeeping for removed orphan"
    );
    assert_eq!(
        vfs.state
            .upload
            .upload_canceled_stale
            .load(Ordering::Relaxed),
        1,
        "startup replay should attribute removed orphan to stale-upload cleanup"
    );
}

#[test]
fn zeroio_snapshot_distinguishes_single_store_payload_classes_and_publish_backlog() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "zeroio-snapshot-classes");
    let _guard = rt.enter();

    rt.block_on(async {
        let pinned = vfs.create(1, b"pinned.bin", 0, 0, 0o644).unwrap();
        vfs.write(pinned.ino, 0, b"pinned-unpublished-payload")
            .await
            .unwrap();

        let publish_ready = vfs.create(1, b"publish-ready.bin", 0, 0, 0o644).unwrap();
        vfs.write(publish_ready.ino, 0, b"publish-ready-payload")
            .await
            .unwrap();

        let published = vfs.create(1, b"published.bin", 0, 0, 0o644).unwrap();
        vfs.write(published.ino, 0, b"published-warm-payload")
            .await
            .unwrap();

        let (publish_ready_epoch, publish_ready_blocks) =
            mark_all_file_blocks_uploaded(&vfs, &meta, publish_ready.ino);
        let (published_epoch, published_blocks) =
            mark_all_file_blocks_uploaded(&vfs, &meta, published.ino);
        publish_current_manifest(&meta, published.ino, published_epoch, &published_blocks);

        let snapshot = vfs.zeroio_runtime_snapshot();

        assert!(
            snapshot.unpublished_files >= 2,
            "expected pinned and publish-ready epochs to remain unpublished"
        );
        assert!(
            snapshot.cache_bytes.pinned_unpublished_bytes > 0,
            "single-store snapshot must report correctness-critical pinned unpublished bytes"
        );
        assert!(
            snapshot.cache_bytes.publish_ready_retained_bytes > 0,
            "single-store snapshot must report uploaded-but-unpublished retained bytes"
        );
        assert!(
            snapshot.cache_bytes.published_warm_bytes > 0,
            "single-store snapshot must report published evictable warm-cache bytes"
        );
        assert!(
            snapshot.publish_ready_backlog_epochs >= 1,
            "publish-ready backlog should include fully uploaded unpublished epochs"
        );

        let publish_ready_entry = snapshot
            .drilldown
            .iter()
            .find(|entry| entry.ino == publish_ready.ino)
            .expect("publish-ready inode should appear in zeroio drilldown");
        assert_eq!(
            publish_ready_entry.lifecycle_state,
            InodeLifecycleState::PublishReady
        );
        assert_eq!(
            publish_ready_entry.retention,
            PayloadRetentionClass::PublishReadyRetained
        );
        assert_eq!(publish_ready_entry.local_content_epoch, publish_ready_epoch);
        assert!(publish_ready_entry.publish_ready_queued);
        assert_eq!(publish_ready_entry.status, "publish_ready_queued");

        assert!(
            !snapshot
                .drilldown
                .iter()
                .any(|entry| entry.ino == published.ino),
            "published warm-cache files should not force per-file drilldown work"
        );

        assert!(
            !publish_ready_blocks.is_empty(),
            "test setup should persist current unpublished blocks"
        );
    });
}

#[test]
fn zeroio_snapshot_reports_delete_before_publish_cleanup_visibility() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "zeroio-delete-before-publish");
    let _guard = rt.enter();

    let attr = vfs
        .create(1, b"delete-before-publish.bin", 0, 0, 0o644)
        .unwrap();
    let mut staged_attr = meta
        .get_attr(attr.ino)
        .expect("load delete-before-publish attr");
    staged_attr.size = 8192;
    staged_attr.content_epoch = 1;
    meta.set_attr(&staged_attr).expect("persist staged attr");
    vfs.local_block_backend()
        .put(950, b"local-only", false)
        .expect("persist local-only object");
    vfs.local_block_backend()
        .put(951, b"uploaded", true)
        .expect("persist uploaded object");
    vfs.local_block_backend().mark_uploaded(951);
    meta.set_file_block(attr.ino, 0, &BlockRef::new_partial(950, 4096))
        .expect("persist local-only block");
    let uploaded = BlockRef::new_partial(951, 4096);
    meta.set_file_block(attr.ino, 1, &uploaded)
        .expect("persist uploaded block");
    assert!(
        meta.mark_file_block_uploaded_if_matches(attr.ino, 1, 1, &uploaded)
            .expect("mark uploaded block"),
        "expected uploaded block transition to apply"
    );

    meta.unlink(ROOT_INO, b"delete-before-publish.bin")
        .expect("persist delete-before-publish lifecycle");

    let lifecycle = meta
        .get_inode_lifecycle(attr.ino)
        .expect("load lifecycle tombstone")
        .expect("deleted-before-publish lifecycle should persist");
    assert_eq!(lifecycle.state, InodeLifecycleState::DeletedBeforePublish);
    assert!(
        !lifecycle.deferred_cleanup_object_ids.is_empty(),
        "test setup should preserve uploaded object ids for deferred cleanup"
    );

    let snapshot = vfs.zeroio_runtime_snapshot();
    assert_eq!(snapshot.deferred_delete_backlog_epochs, 1);
    assert!(
        snapshot.deferred_delete_backlog_objects > 0,
        "deleted-before-publish epochs should retain uploaded objects for deferred cleanup"
    );

    let entry = snapshot
        .drilldown
        .iter()
        .find(|entry| entry.ino == attr.ino)
        .expect("deleted-before-publish inode should remain drill-down visible");
    assert_eq!(
        entry.lifecycle_state,
        InodeLifecycleState::DeletedBeforePublish
    );
    assert_eq!(entry.status, "awaiting_deferred_cleanup");
    assert!(entry.deferred_cleanup_object_count > 0);
}

#[test]
fn epoch_finalizer_publishes_manifest_backed_zero_length_epoch() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "manifest-finalizer-zero-length");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"manifest-finalizer-zero-length.bin", 0, 0, 0o644)
            .expect("create test file");
        let payload = b"manifest-backed-finalization";
        vfs.write(attr.ino, 0, payload)
            .await
            .expect("write test payload");

        let (published_epoch, published_blocks) =
            mark_all_file_blocks_uploaded(&vfs, &meta, attr.ino);
        publish_current_manifest(&meta, attr.ino, published_epoch, &published_blocks);

        let mut truncated_attr = meta.get_attr(attr.ino).expect("load published attr");
        truncated_attr.size = 0;
        truncated_attr.content_epoch = truncated_attr.content_epoch.saturating_add(1);
        truncated_attr.upload_state = MetaUploadState::Updating;
        meta.set_attr(&truncated_attr)
            .expect("stage manifest-backed zero-length epoch");
        meta.remove_file_blocks(attr.ino)
            .expect("remove unpublished blocks for zero-length truncate");

        assert!(
            !meta
                .has_local_regular_file_overlay(attr.ino)
                .expect("query overlay state"),
            "regression setup should exercise the legacy manifest-backed path"
        );

        let unpublished_attr = meta.get_attr(attr.ino).expect("load unpublished attr");
        let lifecycle = meta
            .get_inode_lifecycle(attr.ino)
            .expect("load lifecycle")
            .expect("manifest-backed epoch should keep lifecycle state");
        assert_eq!(unpublished_attr.size, 0);
        assert_ne!(
            unpublished_attr.upload_state,
            MetaUploadState::Ready,
            "regression setup should keep the staged epoch on a non-ready upload path"
        );
        assert_ne!(
            unpublished_attr.committed_content_epoch, unpublished_attr.content_epoch,
            "regression setup should leave the zero-length epoch unpublished"
        );
        assert_ne!(
            lifecycle.state,
            InodeLifecycleState::PublishReady,
            "regression setup should continue exercising the legacy non-publish-ready path"
        );
        assert_eq!(
            meta.get_file_blocks(attr.ino)
                .expect("load unpublished manifest")
                .len(),
            0,
            "regression setup should not require remote object visibility checks"
        );

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            attr.ino,
            unpublished_attr.content_epoch,
        )
        .await
        .expect("finalize zero-length manifest-backed epoch");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Published),
            "manifest-backed zero-length epoch should still publish"
        );

        let published_attr = meta.get_attr(attr.ino).expect("load published attr");
        assert_eq!(published_attr.size, 0);
        assert_eq!(published_attr.committed_size, 0);
        assert_eq!(published_attr.upload_state, MetaUploadState::Ready);
        assert_eq!(
            published_attr.committed_content_epoch,
            published_attr.content_epoch
        );
    });
}

#[test]
fn zeroio_snapshot_reports_repeated_restart_replay_as_health_signal() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "zeroio-repeated-replay");
    let _guard = rt.enter();

    let created = vfs
        .create(1, b"replay-health.bin", 0, 0, 0o644)
        .expect("create replay test file");
    let ino = created.ino;
    rt.block_on(async {
        vfs.write(ino, 0, b"replay-health-payload").await.unwrap();
    });
    let attr = meta.get_attr(ino).expect("load replay attr");
    let block = meta
        .get_file_blocks(ino)
        .expect("load replay blocks")
        .into_iter()
        .next()
        .expect("at least one persisted block")
        .1;
    let pending_uploads = vec![PendingUpload {
        ino,
        offset: 0,
        object_id: block.object_id,
        content_epoch: attr.content_epoch,
        created_unix_ms: 100,
        not_before_unix_ms: 0,
        retry_count: 0,
    }];

    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, std::slice::from_ref(&attr));
    vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr]);

    let snapshot = vfs.zeroio_runtime_snapshot();
    assert!(
        snapshot
            .startup_replay
            .as_ref()
            .map(|summary| summary.replayed_blocks)
            .unwrap_or(0)
            >= 1,
        "startup replay summary should be surfaced in zeroio diagnostics"
    );
    assert!(
        snapshot
            .health_signals
            .iter()
            .any(|signal| signal.code == "repeated_replay" && signal.ino == Some(ino)),
        "repeated restart replay should surface as an explicit zeroio health signal"
    );
    let entry = snapshot
        .drilldown
        .iter()
        .find(|entry| entry.ino == ino)
        .expect("replayed inode should appear in zeroio drilldown");
    assert!(entry.replayed_after_restart);
    assert!(entry.repeated_replay_count >= 2);
}

#[test]
fn zeroio_snapshot_loads_last_graceful_shutdown_after_restart() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "zeroio-shutdown-persisted");
    let _guard = rt.enter();

    rt.block_on(async {
        let created = vfs
            .create(1, b"shutdown-persisted.bin", 0, 0, 0o644)
            .expect("create shutdown test file");
        vfs.write(created.ino, 0, b"shutdown-payload")
            .await
            .expect("write shutdown payload");
        vfs.shutdown(std::time::Duration::ZERO).await;
    });
    drop(vfs);
    drop(meta);
    drop(rt);

    let (_reopen_rt, reopened, _reopened_meta) = reopen_existing_test_vfs(&tmp);
    assert!(
        reopened
            .recent_runtime_events(8)
            .into_iter()
            .all(|event| event.kind != "shutdown_local_state"),
        "freshly reopened VFS should not have inherited runtime events in memory"
    );

    let snapshot = reopened.zeroio_runtime_snapshot();
    let shutdown = snapshot
        .last_graceful_shutdown
        .expect("shutdown evidence should survive restart");
    assert!(shutdown.generated_unix_ms > 0);
    assert_eq!(shutdown.metadata_flush_outcome, "success");
    assert!(shutdown.local_payload_sync_ok);
    assert!(shutdown.recoverable_on_restart);
}

#[test]
fn zeroio_snapshot_reports_replay_backlog_beyond_recent_event_sample_limit() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "zeroio-replay-sample-limit");
    let _guard = rt.enter();

    let mut replayed_epochs = Vec::new();
    rt.block_on(async {
        for idx in 0..17 {
            let name = format!("replay-{idx}.bin");
            let created = vfs.create(1, name.as_bytes(), 0, 0, 0o644).unwrap();
            vfs.write(created.ino, 0, b"replay-payload").await.unwrap();
            let attr = meta.get_attr(created.ino).expect("load replay attr");
            replayed_epochs.push((created.ino, attr.content_epoch));
        }
    });

    {
        let mut replay_counts = vfs.state.upload.startup_replay_counts.lock();
        for (ino, content_epoch) in &replayed_epochs {
            replay_counts.insert((*ino, *content_epoch), 1);
        }
    }

    let snapshot = vfs.zeroio_runtime_snapshot();
    assert_eq!(
        snapshot.replayed_after_restart_epochs,
        replayed_epochs.len() as u64
    );
    assert_eq!(snapshot.replay_backlog_epochs, replayed_epochs.len());
    let last_ino = replayed_epochs.last().map(|(ino, _)| *ino).unwrap();
    let entry = snapshot
        .drilldown
        .iter()
        .find(|entry| entry.ino == last_ino)
        .expect("epochs beyond the sampled runtime-event limit must still be marked replayed");
    assert!(entry.replayed_after_restart);
}

#[test]
fn cache_backend_detection_errors_are_structured() {
    let error = std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "simulated permission error",
    );
    let mapped = map_cache_backend_detection_error("E:\\cache-root", error);
    match mapped {
        VfsError::CacheBackendDetection { cache_root, source } => {
            assert_eq!(cache_root, "E:\\cache-root");
            assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
        }
        other => panic!("unexpected error variant: {other}"),
    }
}

#[test]
fn test_block_index_calculation() {
    let block_size = 4 * 1024 * 1024; // 4MB

    assert_eq!(block_index_from_offset(0, block_size), 0);
    assert_eq!(block_index_from_offset(1, block_size), 0);
    assert_eq!(block_index_from_offset(4 * 1024 * 1024, block_size), 1);
    assert_eq!(block_index_from_offset(4 * 1024 * 1024 + 1, block_size), 1);
}

#[test]
fn cache_runtime_tuning_enforces_bounds() {
    let tiny =
        derive_cache_runtime_tuning(2 * 1024 * 1024 * 1024, 16 * 1024 * 1024, 64 * 1024 * 1024);
    let huge = derive_cache_runtime_tuning(
        16 * 1024 * 1024 * 1024 * 1024,
        16 * 1024 * 1024,
        8 * 1024 * 1024 * 1024,
    );

    assert!(tiny.max_dirty_bytes > 0);
    assert!(tiny.upload_cache_io_slots > 0);
    assert_eq!(tiny.max_dirty_bytes, CACHE_RUNTIME_MIN_DIRTY_BYTES);
    assert_eq!(huge.max_dirty_bytes, CACHE_RUNTIME_MAX_DIRTY_BYTES_HUGE);
    assert!(huge.max_dirty_bytes >= tiny.max_dirty_bytes);
    assert!(huge.upload_cache_io_slots >= tiny.upload_cache_io_slots);
}

#[test]
fn local_backend_budget_honors_runtime_dirty_window() {
    let runtime_tuning = CacheRuntimeTuning {
        max_dirty_bytes: 1024 * 1024,
        write_eager_flush_batch_bytes: 512 * 1024,
        upload_cache_io_slots: 16,
        transit_memory_budget_bytes: 1024 * 1024,
    };

    assert_eq!(
        derive_local_backend_budget_bytes(4096, 512 * 1024, runtime_tuning),
        1024 * 1024
    );
}

#[test]
fn local_backend_budget_never_drops_below_block_size() {
    let runtime_tuning = CacheRuntimeTuning {
        max_dirty_bytes: 128 * 1024,
        write_eager_flush_batch_bytes: 128 * 1024,
        upload_cache_io_slots: 16,
        transit_memory_budget_bytes: 1024 * 1024,
    };

    assert_eq!(
        derive_local_backend_budget_bytes(64 * 1024, 512 * 1024, runtime_tuning),
        512 * 1024
    );
}

#[test]
fn cache_runtime_tuning_uses_internal_config_overrides_when_non_default() {
    let derived = derive_cache_runtime_tuning(16 * 1024 * 1024, 512 * 1024, 16 * 1024 * 1024);
    let defaults = VfsConfig::default();
    let mut config = defaults.clone();
    config.max_dirty_bytes = 1024 * 1024;
    config.write_eager_flush_batch_bytes = 1024 * 1024;
    config.upload_cache_io_slots = 7;
    let overrides = CacheRuntimeTuningOverrides {
        max_dirty_bytes: true,
        write_eager_flush_batch_bytes: true,
        upload_cache_io_slots: true,
        ..Default::default()
    };

    let resolved = resolve_cache_runtime_tuning_overrides(derived, &config, overrides);

    assert_eq!(resolved.max_dirty_bytes, 1024 * 1024);
    assert_eq!(resolved.write_eager_flush_batch_bytes, 1024 * 1024);
    assert_eq!(resolved.upload_cache_io_slots, 7);
}

#[test]
fn cache_runtime_tuning_respects_explicit_default_values_from_raw_config() {
    let validated = VfsConfigRaw {
        max_dirty_bytes: Some(DEFAULT_MAX_DIRTY_BYTES),
        write_eager_flush_batch_bytes: Some(DEFAULT_WRITE_EAGER_FLUSH_BATCH_BYTES),
        ..Default::default()
    }
    .validate()
    .expect("validate explicit default runtime tuning values");
    let derived =
        derive_cache_runtime_tuning(16 * 1024 * 1024 * 1024, 512 * 1024, 16 * 1024 * 1024);
    let resolved = resolve_cache_runtime_tuning_overrides(
        derived,
        &validated,
        validated.cache_runtime_overrides(),
    );

    assert_ne!(derived.max_dirty_bytes, DEFAULT_MAX_DIRTY_BYTES);
    assert_eq!(resolved.max_dirty_bytes, DEFAULT_MAX_DIRTY_BYTES);
    assert_eq!(
        resolved.write_eager_flush_batch_bytes,
        DEFAULT_WRITE_EAGER_FLUSH_BATCH_BYTES
    );
}

#[test]
fn vfs_config_raw_validate_clamps_and_normalizes() {
    let validated = VfsConfigRaw {
        janitor_interval_secs: Some(0),
        prefetch_concurrency: Some(0),
        slab_flush_interval_ms: Some(0),
        upload_worker_count: Some(0),
        principal: Some("   ".to_string()),
        ..Default::default()
    }
    .validate()
    .expect("validate clamping");

    assert_eq!(validated.janitor_interval_secs, 1);
    assert_eq!(
        validated.prefetch_concurrency,
        DEFAULT_PREFETCH_CONCURRENCY_MIN
    );
    assert_eq!(validated.slab_flush_interval_ms, 1);
    assert_eq!(validated.upload_worker_count, 128);
    assert_eq!(validated.principal, "*");
}

#[test]
fn upload_pipeline_summary_logs_only_when_pending_work_exists() {
    assert!(!StudioVfs::upload_pipeline_summary_is_active(0, 0));
    assert!(StudioVfs::upload_pipeline_summary_is_active(1, 0));
    assert!(!StudioVfs::upload_pipeline_summary_is_active(0, 1)); // < 10 threshold
    assert!(StudioVfs::upload_pipeline_summary_is_active(0, 10));
}

#[test]
fn upload_pipeline_summary_is_active_checks() {
    assert!(StudioVfs::upload_pipeline_summary_is_active(256, 0));
    assert!(!StudioVfs::upload_pipeline_summary_is_active(0, 9));
    assert!(StudioVfs::upload_pipeline_summary_is_active(0, 11));
}

#[test]
fn upload_pipeline_summary_formats_pending_uploads() {
    assert_eq!(StudioVfs::format_upload_pipeline_pending_uploads(0), "0");
    assert_eq!(
        StudioVfs::format_upload_pipeline_pending_uploads(512),
        "512B"
    );
    assert_eq!(
        StudioVfs::format_upload_pipeline_pending_uploads(12 * 1024),
        "12KB"
    );
    assert_eq!(
        StudioVfs::format_upload_pipeline_pending_uploads(1904 * 1024 * 1024),
        "1.9GB"
    );
}

#[test]
fn foreground_speedometer_p90_uses_recent_tail() {
    let p90 = StudioVfs::p90_ns(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(p90, Some(9));
}

#[test]
fn foreground_speedometer_p90_handles_empty_input() {
    assert_eq!(StudioVfs::p90_ns(vec![]), None);
}

#[test]
fn foreground_speedometer_trim_drops_expired_samples() {
    let mut samples = std::collections::VecDeque::from([
        ForegroundReadSample {
            timestamp_ms: 100,
            latency_ns: 10,
            bytes_read: 4_096,
            access_class: AccessClass::RandomInteractiveRead,
            policy: AccessPolicy::default(),
        },
        ForegroundReadSample {
            timestamp_ms: 200,
            latency_ns: 20,
            bytes_read: 4_096,
            access_class: AccessClass::RandomInteractiveRead,
            policy: AccessPolicy::default(),
        },
        ForegroundReadSample {
            timestamp_ms: 350,
            latency_ns: 30,
            bytes_read: 4_096,
            access_class: AccessClass::SequentialMediaRead,
            policy: AccessPolicy::from_class(
                AccessClass::SequentialMediaRead,
                AccessClassifierConfig {
                    block_size: 4096,
                    readahead_base_width: 8,
                    readahead_max_width: 32,
                    readahead_min_prefetch: 2,
                },
            ),
        },
    ]);
    ReadState::trim_foreground_latency_samples(&mut samples, 400, Duration::from_millis(150));
    let kept: Vec<(u64, u64, u64)> = samples
        .into_iter()
        .map(|sample| (sample.timestamp_ms, sample.latency_ns, sample.bytes_read))
        .collect();
    assert_eq!(kept, vec![(350, 30, 4_096)]);
}

#[test]
fn foreground_read_active_respects_idle_grace_after_last_read() {
    let state = ReadState::new(1);
    let grace = Duration::from_millis(50);

    state.mark_foreground_read(100);
    assert!(state.foreground_read_active(100, grace));

    state.unmark_foreground_read(120);
    assert!(
        state.foreground_read_active(169, grace),
        "idle grace should keep reads active shortly after scope exit"
    );
    assert!(
        !state.foreground_read_active(171, grace),
        "foreground read activity should expire after the configured idle grace"
    );
}

#[test]
fn foreground_read_scope_records_sample_and_clears_inflight() {
    let state = ReadState::new(1);

    {
        let mut scope = ForegroundReadScope::new(
            &state,
            Duration::from_secs(1),
            AccessPolicy::from_class(
                AccessClass::SequentialMediaRead,
                AccessClassifierConfig {
                    block_size: 4096,
                    readahead_base_width: 8,
                    readahead_max_width: 32,
                    readahead_min_prefetch: 2,
                },
            ),
        );
        scope.record_bytes(4096);
        assert_eq!(
            state.foreground_reads_inflight.load(Ordering::Relaxed),
            1,
            "creating a scope should mark one active foreground read"
        );
    }

    assert_eq!(
        state.foreground_reads_inflight.load(Ordering::Relaxed),
        0,
        "dropping the scope should clear the inflight foreground read"
    );

    let (p90_ns, total_samples, window_bytes, window_coverage_ms) =
        state.foreground_speedometer_snapshot(now_unix_millis(), Duration::from_secs(1));
    assert!(
        p90_ns.is_some(),
        "dropping the scope should record latency telemetry"
    );
    assert_eq!(total_samples, 1);
    assert_eq!(window_bytes, 4096);
    assert!(window_coverage_ms >= 1);
}

#[test]
fn release_fh_clears_per_stream_read_tracking() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-clears-stream-state");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs.create(1, b"stream-state.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
        let stream_id = vfs.get_open_file(fh).unwrap().stream_id();
        let key = ReadStreamKey { ino, stream_id };
        let policy =
            AccessPolicy::from_class(AccessClass::SequentialMediaRead, vfs.access_classifier)
                .readahead_policy();

        let _ = vfs.readahead.record_read_for_stream(key, 0, 0, policy);
        vfs.state
            .read
            .prefetch_last_hint
            .lock()
            .insert((ino, stream_id), (0, 1));

        assert!(vfs.readahead.debug_has_stream(key));
        assert!(
            vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .contains_key(&(ino, stream_id))
        );

        vfs.release_fh(fh).await.unwrap();

        assert!(!vfs.readahead.debug_has_stream(key));
        assert!(
            !vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .contains_key(&(ino, stream_id))
        );
    });
}

#[test]
fn fallback_read_access_tracking_does_not_increment_active_handle_counts() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "fallback-access-tracking");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"fallback-access-tracking.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let block = vfs.access_classifier.block_size as usize;

        let mut last = AccessClass::RandomInteractiveRead;
        for step in 0..4u64 {
            last = vfs
                .state
                .read
                .observe_read_access(None, ino, step * block as u64, block, vfs.access_classifier)
                .class;
        }

        assert_eq!(
            last,
            AccessClass::SequentialMediaRead,
            "contiguous fallback reads should still classify as sequential"
        );

        let stats = vfs.foreground_read_protection_stats();
        assert_eq!(
            stats.active_sequential_media_reads, 0,
            "fallback tracking must not pretend there is an active handle"
        );
        assert_eq!(
            stats.active_random_interactive_reads, 0,
            "fallback tracking must not perturb active read-handle counts"
        );
        assert!(
            vfs.state.read.access_class_transitions[AccessClass::SequentialMediaRead.index()]
                .load(Ordering::Relaxed)
                >= 1,
            "fallback tracking should still contribute access-class transition telemetry"
        );
    });
}

#[test]
fn handle_read_access_tracking_promotes_active_class_counts() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "handle-access-tracking");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"handle-access-tracking.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
        let block = vfs.access_classifier.block_size as usize;

        assert_eq!(
            vfs.state
                .read
                .active_access_class_count(AccessClass::RandomInteractiveRead),
            1,
            "new read handle should start in the random-interactive class"
        );
        assert_eq!(
            vfs.state
                .read
                .active_access_class_count(AccessClass::SequentialMediaRead),
            0
        );

        let mut last = AccessClass::RandomInteractiveRead;
        for step in 0..4u64 {
            let open_file = vfs.get_open_file(fh).expect("open file");
            last = vfs
                .state
                .read
                .observe_read_access(
                    Some(&open_file),
                    ino,
                    step * block as u64,
                    block,
                    vfs.access_classifier,
                )
                .class;
        }

        assert_eq!(
            last,
            AccessClass::SequentialMediaRead,
            "contiguous handle reads should promote the handle into sequential classification"
        );
        assert_eq!(
            vfs.state
                .read
                .active_access_class_count(AccessClass::RandomInteractiveRead),
            0,
            "promotion should decrement the previous active class"
        );
        assert_eq!(
            vfs.state
                .read
                .active_access_class_count(AccessClass::SequentialMediaRead),
            1,
            "promotion should increment the new active class"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn clear_access_tracking_clears_fallback_states_and_hints() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "clear-access-tracking");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"clear-access-tracking.bin", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let block = vfs.access_classifier.block_size as usize;

        let _ = vfs
            .state
            .read
            .observe_read_access(None, ino, 0, block, vfs.access_classifier);
        let _ = vfs
            .state
            .read
            .observe_write_access(None, ino, 0, block, vfs.access_classifier);
        vfs.state
            .read
            .prefetch_last_hint
            .lock()
            .insert((ino, 77), (4, 2));

        assert!(
            vfs.state
                .read
                .read_access_fallback
                .lock()
                .contains_key(&ino)
        );
        assert!(
            vfs.state
                .read
                .write_access_fallback
                .lock()
                .contains_key(&ino)
        );
        assert!(
            vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .contains_key(&(ino, 77))
        );

        vfs.clear_access_tracking(ino);

        assert!(
            !vfs.state
                .read
                .read_access_fallback
                .lock()
                .contains_key(&ino)
        );
        assert!(
            !vfs.state
                .read
                .write_access_fallback
                .lock()
                .contains_key(&ino)
        );
        assert!(
            !vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .contains_key(&(ino, 77))
        );
    });
}

#[test]
fn foreground_throughput_uses_minimum_coverage_floor_for_short_windows() {
    let throughput = StudioVfs::foreground_throughput_bytes_per_sec(1, 1_048_576, 10);
    assert_eq!(throughput, 10_485_760.0);
}

#[test]
fn foreground_throughput_is_zero_without_samples() {
    let throughput = StudioVfs::foreground_throughput_bytes_per_sec(0, 1_048_576, 10);
    assert_eq!(throughput, 0.0);
}

#[test]
fn test_run_sync_with_timeout_returns_result_before_deadline() {
    let result = run_sync_with_timeout(Duration::from_millis(100), || Ok::<_, VfsError>(7))
        .expect("operation should succeed");
    assert_eq!(result, Some(7));
}

#[test]
fn test_run_sync_with_timeout_returns_none_on_timeout() {
    let finished = Arc::new(AtomicBool::new(false));
    let finished_clone = finished.clone();
    let t0 = std::time::Instant::now();
    let result = run_sync_with_timeout(Duration::from_millis(10), move || {
        std::thread::sleep(Duration::from_millis(100));
        finished_clone.store(true, AtomicOrdering::Release);
        Ok::<_, VfsError>(())
    })
    .expect("timeout helper should not fail");

    // The primary timeout should report None (timed out).
    assert_eq!(result, None);
    // The thread is joined to ensure clean Arc release, so the operation
    // finishes by the time we return.
    assert!(
        finished.load(AtomicOrdering::Acquire),
        "thread should have been joined after timeout"
    );
    assert!(
        t0.elapsed() >= Duration::from_millis(100),
        "run_sync_with_timeout should wait for thread join"
    );
}

#[test]
fn test_offset_within_block() {
    let block_size = 4 * 1024 * 1024; // 4MB

    assert_eq!(offset_within_block(0, block_size), 0);
    assert_eq!(offset_within_block(100, block_size), 100);
    assert_eq!(offset_within_block(4 * 1024 * 1024, block_size), 0);
    assert_eq!(offset_within_block(4 * 1024 * 1024 + 100, block_size), 100);
}

#[test]
fn test_sticky_upload_error_is_deterministic_across_sync_points() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sticky-upload-error");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"sticky.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));

        let fsync_err = vfs.fsync(ino, false).await.unwrap_err();
        assert!(
            fsync_err.to_string().contains("sticky upload error"),
            "fsync should surface sticky upload errors"
        );
        let fsync_err_again = vfs.fsync(ino, false).await.unwrap_err();
        assert!(
            fsync_err_again.to_string().contains("sticky upload error"),
            "sticky upload errors should be deterministic across fsync calls"
        );
        vfs.clear_upload_sticky_error(ino);
        vfs.write(ino, 0, b"payload").await.unwrap();
        vfs.fsync(ino, false).await.unwrap();

        vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));
        let flush_err = vfs.flush_fh(fh).await.unwrap_err();
        assert!(
            flush_err.to_string().contains("sticky upload error"),
            "flush_fh should surface sticky upload errors"
        );
        let flush_err_again = vfs.flush_fh(fh).await.unwrap_err();
        assert!(
            flush_err_again.to_string().contains("sticky upload error"),
            "sticky upload errors should be deterministic across flush_fh calls"
        );
        vfs.clear_upload_sticky_error(ino);
        vfs.write(ino, 0, b"payload").await.unwrap();
        vfs.flush_fh(fh).await.unwrap();

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn test_fsync_reports_sticky_error_after_persisting_current_changes() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sticky-fsync-order");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"sticky-fsync.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let payload = b"persist-before-sticky-report";

        vfs.write(ino, 0, payload).await.unwrap();
        vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));

        let fsync_err = vfs.fsync(ino, false).await.unwrap_err();
        assert!(
            fsync_err.to_string().contains("sticky upload error"),
            "fsync should still report sticky upload errors"
        );

        let after = vfs.getattr(ino).unwrap();
        assert_eq!(
            after.size,
            payload.len() as u64,
            "fsync must persist current dirty attrs before reporting sticky EIO"
        );

        let fsync_err_again = vfs.fsync(ino, false).await.unwrap_err();
        assert!(
            fsync_err_again.to_string().contains("sticky upload error"),
            "sticky upload errors should remain visible until recovery"
        );
        vfs.clear_upload_sticky_error(ino);
        vfs.fsync(ino, false).await.unwrap();
    });
}

#[test]
fn test_release_fh_reports_sticky_error_after_persisting_current_changes() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sticky-release-order");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"sticky-release.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"persist-before-release-sticky-report";

        vfs.write(ino, 0, payload).await.unwrap();
        vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));

        let release_err = vfs.release_fh(fh).await.unwrap_err();
        assert!(
            release_err.to_string().contains("sticky upload error"),
            "release_fh should surface sticky upload errors"
        );
        assert_eq!(
            vfs.open_count(ino),
            0,
            "release_fh should still close the handle"
        );

        let after = vfs.getattr(ino).unwrap();
        assert_eq!(
            after.size,
            payload.len() as u64,
            "release_fh must persist local dirty state before reporting sticky EIO"
        );
    });
}

#[test]
fn test_release_fh_with_outcome_keeps_write_committed_inode_clean_on_close() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-outcome-close-ownership");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"release-outcome.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let fh1 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let fh2 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write(ino, 0, b"payload").await.unwrap();
        assert!(
            !vfs.inode_has_local_dirty_state(ino),
            "write should already cross the local durable barrier"
        );

        let first = vfs.release_fh_with_outcome(fh1).await.unwrap();
        assert!(
            !first.closed_last_handle,
            "closing one of multiple handles must not report last-handle ownership"
        );
        assert!(
            !first.flushed_local_dirty_state,
            "non-final close should not attribute a local dirty-state drain"
        );
        assert_eq!(vfs.open_count(ino), 1, "one handle should remain open");

        let second = vfs.release_fh_with_outcome(fh2).await.unwrap();
        assert!(
            second.closed_last_handle,
            "the final close should report last-handle ownership"
        );
        assert!(
            !second.flushed_local_dirty_state,
            "the final close should stay clean once write already persisted local state"
        );
        assert_eq!(vfs.open_count(ino), 0, "all handles should be closed");
    });
}

#[test]
fn test_release_fh_with_outcome_reports_clean_skip_after_explicit_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-outcome-clean-skip");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"release-outcome-clean-skip.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write(ino, 0, b"payload").await.unwrap();
        vfs.flush_fh(fh).await.unwrap();

        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
        assert!(
            outcome.closed_last_handle,
            "the only handle should still be reported as the last-handle close"
        );
        assert!(
            !outcome.flushed_local_dirty_state,
            "release should report a clean skip after explicit flush already drained local state"
        );
        assert_eq!(
            vfs.open_count(ino),
            0,
            "release should still close the handle"
        );
    });
}

#[test]
fn flush_then_release_keeps_written_bytes_visible_to_reopened_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-release-reopen-read");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"flush-release-read.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"flush-close-contract";

        vfs.write(ino, 0, payload).await.unwrap();
        vfs.flush_fh(fh).await.unwrap();
        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
        assert!(outcome.closed_last_handle);
        assert!(
            !outcome.flushed_local_dirty_state,
            "explicit flush should leave release as a clean close"
        );

        let reopened = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
        let bytes = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(bytes.as_ref(), payload);
        vfs.release_fh(reopened).await.unwrap();
    });
}

#[test]
fn same_mount_read_keeps_opportunistic_handle_write_visible_after_write_buffer_clear() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-245-opportunistic-handle-read");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-opportunistic-handle-read.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"slice-writer-visible";

        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            0,
            payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        let overlay_spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            overlay_spans.iter().all(|span| span.block.is_none()),
            "opportunistic handle writes should not materialize block-backed metadata before a barrier: {overlay_spans:?}"
        );

        vfs.clear_pending_blocks_for_inode(ino);

        let bytes = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(bytes.as_ref(), payload);

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn same_mount_read_prefers_newer_opportunistic_handle_writer_spans() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-245-overlapping-handle-writers");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-overlapping-handle-writers.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh1 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let fh2 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh_with_local_sync_policy(fh1, ino, 0, b"hello", LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        vfs.write_fh_with_local_sync_policy(fh2, ino, 1, b"XYZ", LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();

        vfs.clear_pending_blocks_for_inode(ino);

        let bytes = vfs.read(ino, 0, 5).await.unwrap();
        assert_eq!(bytes.as_ref(), b"hXYZo");

        vfs.release_fh(fh1).await.unwrap();
        vfs.release_fh(fh2).await.unwrap();
    });
}

#[test]
fn inode_write_materializes_directly_without_populating_legacy_write_buffer() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-245-inode-write-direct");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-inode-write-direct.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let payload = b"inode-direct";

        vfs.write_with_local_sync_policy(ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();

        assert!(
            !vfs.has_pending_write_buffer_blocks_for_inode(ino),
            "inode write should bypass the legacy write buffer"
        );
        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().any(|span| span.block.is_some()),
            "inode write should materialize block-backed unpublished slices: {spans:?}"
        );

        let bytes = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(bytes.as_ref(), payload);
    });
}

#[test]
fn inode_write_overwrite_updates_current_block_graph_over_published_base() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-245-inode-write-overwrite");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-inode-write-overwrite.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let baseline = b"published-baseline";
        let replacement = b"replacement-bytes";

        vfs.write(ino, 0, baseline).await.unwrap();
        vfs.flush(ino).await.unwrap();
        let (published_epoch, published_blocks) = mark_all_file_blocks_uploaded(&vfs, &meta, ino);
        publish_current_manifest(&meta, ino, published_epoch, &published_blocks);

        vfs.write(ino, 0, replacement).await.unwrap();
        vfs.flush(ino).await.unwrap();

        let current_blocks = meta.get_file_blocks(ino).expect("load current file graph");
        assert_eq!(current_blocks.len(), 1, "overwrite test should stay single-block");
        assert_ne!(
            current_blocks[0].1.object_id,
            published_blocks[0].1.object_id,
            "current file graph must advance to the replacement block instead of reusing the published base"
        );

        let spans = meta
            .lookup_regular_file_slices(ino, 0, replacement.len() as u64)
            .expect("load current regular-file slices");
        assert_eq!(
            spans.iter().filter_map(|span| span.block.as_ref()).count(),
            1,
            "overwrite should resolve to one block-backed span"
        );
        assert_eq!(
            spans
                .iter()
                .filter_map(|span| span.block.as_ref())
                .next()
                .expect("block-backed overwrite span")
                .object_id,
            current_blocks[0].1.object_id,
            "lookup_regular_file_slices and get_file_blocks must agree on the current unpublished block"
        );
    });
}

#[test]
fn test_release_fh_with_outcome_orphan_cleanup_does_not_report_flush_barrier() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-outcome-orphan-cleanup");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"release-outcome-orphan.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write(ino, 0, b"payload").await.unwrap();
        vfs.unlink(1, b"release-outcome-orphan.txt").unwrap();

        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
        assert!(
            outcome.closed_last_handle,
            "orphan cleanup should still be owned by the last-handle release"
        );
        assert!(
            !outcome.flushed_local_dirty_state,
            "orphan cleanup should not be reported as a normal release flush barrier"
        );
        assert!(
            vfs.getattr(ino).is_err(),
            "orphan cleanup should remove the inode"
        );
    });
}

#[test]
fn test_release_fh_orphan_cleanup_clears_live_extents() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-orphan-live-extents");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"release-orphan-live-extents.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write(ino, 0, b"payload").await.unwrap();
        vfs.flush(ino).await.unwrap();
        assert!(
            !vfs.live_extent_records_for_inode(ino).is_empty(),
            "flush should materialize live extents before orphan cleanup"
        );

        vfs.unlink(1, b"release-orphan-live-extents.txt").unwrap();
        assert!(
            !vfs.live_extent_records_for_inode(ino).is_empty(),
            "open-unlink should keep live extents available until the last handle closes"
        );

        vfs.release_fh(fh).await.unwrap();
        assert!(
            vfs.live_extent_records_for_inode(ino).is_empty(),
            "orphan cleanup should clear live extent identities on final close"
        );
    });
}

#[test]
fn test_release_fh_orphan_cleanup_waits_for_final_handle_before_clearing_live_extents() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-orphan-final-handle-live-extents");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"release-orphan-final-handle.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh1 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let fh2 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write(ino, 0, b"payload").await.unwrap();
        vfs.flush(ino).await.unwrap();
        assert!(
            !vfs.live_extent_records_for_inode(ino).is_empty(),
            "flush should materialize live extents before orphan cleanup"
        );

        vfs.unlink(1, b"release-orphan-final-handle.txt").unwrap();
        vfs.release_fh(fh1).await.unwrap();
        assert!(
            !vfs.live_extent_records_for_inode(ino).is_empty(),
            "non-final close must not clear orphan live extents"
        );

        vfs.release_fh(fh2).await.unwrap();
        assert!(
            vfs.live_extent_records_for_inode(ino).is_empty(),
            "final orphan close must clear live extents"
        );
    });
}

#[test]
fn test_unlink_immediate_cleanup_keeps_write_committed_inode_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "unlink-immediate-cleanup");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"unlink-immediate-cleanup.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        vfs.write(ino, 0, b"payload").await.unwrap();
        assert!(
            !vfs.inode_has_local_dirty_state(ino),
            "write should already leave the inode locally durable before unlink"
        );

        vfs.unlink(1, b"unlink-immediate-cleanup.txt").unwrap();

        assert!(
            vfs.getattr(ino).is_err(),
            "unlink should remove the inode immediately"
        );
        assert!(
            !vfs.inode_has_local_dirty_state(ino),
            "immediate unlink should clear local dirty state for the deleted inode"
        );
    });
}

#[test]
fn test_rename_over_existing_target_discards_clean_persisted_local_state() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "rename-displaced-cleanup");
    let _guard = rt.enter();
    rt.block_on(async {
        let source = vfs.create(1, b"rename-source.txt", 0, 0, 0o644).unwrap();
        let displaced = vfs.create(1, b"rename-target.txt", 0, 0, 0o644).unwrap();

        vfs.write(source.ino, 0, b"source").await.unwrap();
        vfs.write(displaced.ino, 0, b"target").await.unwrap();
        assert!(
            !vfs.inode_has_local_dirty_state(displaced.ino),
            "overwrite target should already be locally durable before it is displaced"
        );

        vfs.rename(1, b"rename-source.txt", 1, b"rename-target.txt")
            .unwrap();

        assert!(
            vfs.getattr(displaced.ino).is_err(),
            "displaced inode should be removed after rename overwrite"
        );
        assert!(
            !vfs.inode_has_local_dirty_state(displaced.ino),
            "rename overwrite should clear local dirty state for the displaced inode"
        );
    });
}

#[test]
fn test_flush_fh_with_outcome_treats_write_committed_inode_as_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-handle-outcome");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"flush-handle-outcome.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        let clean = vfs.flush_fh_with_outcome(fh).await.unwrap();
        assert!(
            !clean.flushed_local_dirty_state,
            "clean handle flush should report a clean skip"
        );

        let payload = b"payload";
        vfs.write(ino, 0, payload).await.unwrap();
        assert!(
            !vfs.inode_has_local_dirty_state(ino),
            "write should already persist local dirty state before flush_fh"
        );

        let after_write = vfs.flush_fh_with_outcome(fh).await.unwrap();
        assert!(
            !after_write.flushed_local_dirty_state,
            "flush_fh should report a clean skip once write already crossed the barrier"
        );

        let after = vfs.getattr(ino).unwrap();
        assert_eq!(
            after.size,
            payload.len() as u64,
            "flush_fh_with_outcome should preserve the write-committed file size"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn test_flush_with_outcome_treats_write_committed_inode_as_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-inode-outcome");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"flush-inode-outcome.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let clean = vfs.flush_with_outcome(ino).await.unwrap();
        assert!(
            !clean.flushed_local_dirty_state,
            "clean inode flush should report a clean skip"
        );

        let payload = b"payload";
        vfs.write(ino, 0, payload).await.unwrap();
        assert!(
            !vfs.inode_has_local_dirty_state(ino),
            "write should already persist local dirty state before inode flush"
        );

        let after_write = vfs.flush_with_outcome(ino).await.unwrap();
        assert!(
            !after_write.flushed_local_dirty_state,
            "flush_with_outcome should report a clean skip once write already crossed the barrier"
        );

        let after = vfs.getattr(ino).unwrap();
        assert_eq!(
            after.size,
            payload.len() as u64,
            "flush_with_outcome should preserve the write-committed file size"
        );
    });
}

#[test]
fn test_fsync_with_outcome_treats_write_committed_inode_as_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "fsync-outcome-clean-data");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"fsync-outcome-clean-data.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let clean = vfs.fsync_with_outcome(ino, false).await.unwrap();
        assert!(
            !clean.flushed_file_data,
            "clean fsync should not report a file-data barrier"
        );
        assert!(
            !clean.persisted_dirty_attrs,
            "clean fsync should not report metadata persistence work"
        );

        let payload = b"payload";
        vfs.write(ino, 0, payload).await.unwrap();
        assert!(
            !vfs.inode_has_local_dirty_state(ino),
            "write should already persist local dirty state before fsync"
        );

        let after_write = vfs.fsync_with_outcome(ino, false).await.unwrap();
        assert!(
            !after_write.flushed_file_data,
            "fsync should report clean data once write already crossed the local barrier"
        );
        assert!(
            !after_write.persisted_dirty_attrs,
            "fsync should not invent metadata work after write already committed it"
        );
    });
}

#[test]
fn test_datasync_with_outcome_treats_already_persisted_metadata_as_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "datasync-outcome-persisted-metadata");
    let _guard = rt.enter();
    rt.block_on(async {
            let attr = vfs
                .create(1, b"datasync-outcome-persisted-metadata.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;

            vfs.setattr(
                ino,
                SetattrArgs {
                    mtime: studiofs_meta::types::TimeSpec::Now,
                    ..Default::default()
                },
            )
            .unwrap();

            let datasync = vfs.fsync_with_outcome(ino, true).await.unwrap();
            assert!(
                !datasync.flushed_file_data,
                "metadata-only datasync should not report a file-data barrier"
            );
            assert!(
                !datasync.persisted_dirty_attrs,
                "datasync should not report metadata persistence when setattr already committed it"
            );

            let full = vfs.fsync_with_outcome(ino, false).await.unwrap();
            assert!(
                !full.flushed_file_data,
                "full fsync after already-persisted metadata should stay data-clean"
            );
            assert!(
                !full.persisted_dirty_attrs,
                "full fsync should not invent metadata persistence work after setattr already committed it"
            );
        });
}

#[test]
fn test_file_session_wait_reasons_and_transitions_are_observable() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-observability");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-observability.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let _pin = vfs.state.file_session.session(ino);

        let session_guard = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Write)
            .await;
        let vfs_clone = vfs.clone();
        let waiter = tokio::spawn(async move {
            let _queued = vfs_clone
                .state
                .file_session
                .enter_op(ino, FileSessionOperation::Read)
                .await;
        });

        let mid = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let stats = vfs.file_session_stats();
                if stats.queued_ops >= 1 {
                    break stats;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("expected at least one queued file-session operation");
        assert!(
            mid.queued_ops >= 1,
            "expected at least one queued file-session operation"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;

        drop(session_guard);
        waiter.await.unwrap();

        let final_stats = vfs.file_session_stats();
        assert!(
            final_stats.wait_events >= 1,
            "expected queued operation wait to be tracked"
        );
        assert!(
            final_stats.transitions >= 2,
            "expected start/finish transitions to be tracked"
        );
        assert!(
            final_stats
                .wait_reasons
                .iter()
                .any(|(reason, count)| reason == "read" && *count >= 1),
            "expected wait reason for read operation"
        );
    });
}

#[test]
fn test_size_changing_setattr_waits_on_file_session_gate() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-setattr-size-gate");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-setattr-size-gate.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let session_guard = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Write)
            .await;
        let vfs_clone = vfs.clone();
        let setter = std::thread::spawn(move || vfs_clone.setattr(ino, SetattrArgs::size_only(1)));

        let mid = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let stats = vfs.file_session_stats();
                if stats.queued_ops >= 1 {
                    break stats;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("expected size-changing setattr to wait behind the file-session gate");
        assert!(
            mid.queued_ops >= 1,
            "expected size-changing setattr to queue behind the file-session gate"
        );

        drop(session_guard);
        let updated = setter.join().unwrap().unwrap();
        assert_eq!(updated.size, 1);
    });
}

#[test]
fn test_size_changing_setattr_rollback_restores_old_local_state_on_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_test_vfs(&tmp, "setattr-size-rollback");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"setattr-size-rollback.txt", 0, 0, 0o644)
            .unwrap();
        let payload = b"setattr rollback payload";
        vfs.write(attr.ino, 0, payload).await.unwrap();

        vfs.arm_fail_next_size_change_setattr_before_flush_for_test();
        let error = vfs
            .setattr(attr.ino, SetattrArgs::size_only(8))
            .expect_err("simulated local persist failure should fail setattr");
        assert!(
            error
                .to_string()
                .contains("simulated size-changing setattr local persist failure"),
            "expected the injected size-changing setattr failure to surface"
        );

        let persisted_attr = meta.get_attr(attr.ino).unwrap();
        assert_eq!(
            persisted_attr.size,
            payload.len() as u64,
            "failed size-changing setattr must restore the previous durable size"
        );
        assert!(
            !vfs.inode_has_local_dirty_state(attr.ino),
            "failed size-changing setattr must not leave dirty local state behind"
        );
        let read = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            payload,
            "failed size-changing setattr must preserve the last durable local contents"
        );

        let blocks = meta.get_file_blocks(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            1,
            "failed rollback should restore the original block map"
        );
        assert_eq!(
            blocks[0].1.length as usize,
            payload.len(),
            "failed size-changing setattr must restore the previous block length"
        );
    });
}

#[test]
fn test_size_changing_setattr_rolls_back_partial_truncate_mutation_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_test_vfs(&tmp, "setattr-truncate-mutation-rollback");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"setattr-truncate-mutation-rollback.bin", 0, 0, 0o644)
            .unwrap();
        let block_size = vfs.block_size as usize;
        let payload = vec![0x5Au8; block_size * 2];
        vfs.write(attr.ino, 0, &payload).await.unwrap();

        vfs.arm_fail_next_size_change_truncate_after_first_removed_block_for_test();
        let error = vfs
            .setattr(attr.ino, SetattrArgs::size_only((block_size / 2) as u64))
            .expect_err("simulated truncate mutation failure should fail setattr");
        assert!(
            error
                .to_string()
                .contains("simulated truncate mutation failure after first removed block"),
            "expected the injected truncate mutation failure to surface"
        );

        let persisted_attr = meta.get_attr(attr.ino).unwrap();
        assert_eq!(
            persisted_attr.size,
            payload.len() as u64,
            "failed truncate mutation must restore the previous durable size"
        );
        assert!(
            !vfs.inode_has_local_dirty_state(attr.ino),
            "failed truncate mutation must not leave dirty local state behind"
        );

        let blocks = meta.get_file_blocks(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            2,
            "rollback should restore both originally committed blocks"
        );
        let read = vfs.read(attr.ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(
            read.as_ref(),
            payload.as_slice(),
            "failed truncate mutation must preserve the last durable local contents"
        );
    });
}

#[test]
fn test_file_session_read_lock_allows_parallel_readers() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-read-parallel");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-read-parallel.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let read_guard = vfs
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Read)
            .await;
        let vfs_clone = vfs.clone();
        let second_reader = tokio::spawn(async move {
            let _second = vfs_clone
                .state
                .file_session
                .enter_op(ino, FileSessionOperation::Read)
                .await;
        });

        tokio::time::timeout(Duration::from_millis(150), second_reader)
            .await
            .expect("parallel read lock acquisition should not block")
            .unwrap();
        drop(read_guard);
    });
}

#[test]
fn test_file_session_open_handle_stats_track_release_lifecycle() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-open-handle-stats");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-open-handle-stats.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let fh1 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let after_first = vfs.file_session_stats();
        assert_eq!(
            after_first.active_sessions, 1,
            "opening the first handle should materialize one file session"
        );
        assert_eq!(
            after_first.open_handles, 1,
            "opening the first handle should increment open-handle tracking"
        );

        let fh2 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let after_second = vfs.file_session_stats();
        assert_eq!(
            after_second.open_handles, 2,
            "opening a second handle should keep both handles observable"
        );

        vfs.release_fh(fh1).await.unwrap();
        let after_first_release = vfs.file_session_stats();
        assert_eq!(
            after_first_release.active_sessions, 1,
            "the session should stay alive until the last handle closes"
        );
        assert_eq!(
            after_first_release.open_handles, 1,
            "releasing one of two handles should decrement the handle count only once"
        );

        vfs.release_fh(fh2).await.unwrap();
        let final_stats = vfs.file_session_stats();
        assert_eq!(
            final_stats.open_handles, 0,
            "last-handle release should clear file-session open-handle tracking"
        );
        assert_eq!(
            final_stats.active_sessions, 0,
            "last-handle release should collect the idle file session"
        );
    });
}

#[test]
fn test_file_session_background_complete_gc_removes_idle_session() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-background-gc");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"session-background-gc.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;

        let _pin = vfs.state.file_session.session(ino);
        assert_eq!(
            vfs.file_session_stats().active_sessions,
            1,
            "pinning a session should materialize it for the test"
        );

        drop(_pin);
        vfs.state.file_session.note_background_transition(
            ino,
            FileSessionOperation::Upload,
            "complete",
        );

        assert_eq!(
            vfs.file_session_stats().active_sessions,
            0,
            "background completion should collect an otherwise idle session"
        );
    });
}

#[test]
fn test_file_session_gc_removes_idle_read_only_session() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-gc");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs.create(1, b"session-gc.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;
        let _ = vfs.read(ino, 0, 1).await.unwrap();
        let stats = vfs.file_session_stats();
        assert_eq!(
            stats.active_sessions, 0,
            "idle read-only session should be collected"
        );
    });
}

#[test]
fn test_validate_name_valid() {
    assert!(validate_name(b"hello.txt").is_ok());
    assert!(validate_name(b"a").is_ok());
    assert!(validate_name(b"...").is_ok());
    assert!(validate_name(&vec![b'x'; 255]).is_ok());
}

#[test]
fn test_validate_name_empty() {
    assert!(matches!(validate_name(b""), Err(VfsError::InvalidName(_))));
}

#[test]
fn test_validate_name_too_long() {
    let long = vec![b'a'; 256];
    assert!(matches!(
        validate_name(&long),
        Err(VfsError::InvalidName(_))
    ));
}

#[test]
fn test_validate_name_slash() {
    assert!(matches!(
        validate_name(b"a/b"),
        Err(VfsError::InvalidName(_))
    ));
}

#[test]
fn test_validate_name_dot() {
    assert!(matches!(validate_name(b"."), Err(VfsError::InvalidName(_))));
    assert!(matches!(
        validate_name(b".."),
        Err(VfsError::InvalidName(_))
    ));
}

#[test]
fn test_validate_name_windows_special_chars() {
    // Test each Windows-invalid character: < > : " \ | ? *
    assert!(matches!(
        validate_name(b"file<name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file>name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file:name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file\"name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file\\name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file|name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file?name"),
        Err(VfsError::InvalidName(_))
    ));
    assert!(matches!(
        validate_name(b"file*name"),
        Err(VfsError::InvalidName(_))
    ));
}

#[test]
fn attr_read_cache_is_bounded_and_tracks_hotset_hits() {
    let mut cache = AttrReadCache::new(2, AttrReadCache::estimate_entry_charge() * 4);
    let now = Instant::now();
    cache.put(
        1,
        InodeAttr::new_file(1, ROOT_INO, 1000, 1000, 0o644),
        ATTR_CACHE_TTL,
        now,
    );
    cache.put(
        2,
        InodeAttr::new_file(2, ROOT_INO, 1000, 1000, 0o644),
        ATTR_CACHE_TTL,
        now,
    );

    assert!(cache.get(1, now).is_some());
    assert!(cache.get(1, now).is_some());

    cache.put(
        3,
        InodeAttr::new_file(3, ROOT_INO, 1000, 1000, 0o644),
        ATTR_CACHE_TTL,
        now,
    );
    let metrics = cache.metrics();

    assert!(metrics.occupancy_entries <= metrics.entry_budget);
    assert!(metrics.occupancy_bytes <= metrics.byte_budget);
    assert!(metrics.protected_occupancy_entries >= 1);
    assert!(metrics.protected_hits >= 1);
    assert!(metrics.hot_hit_ratio() > 0.0);
}

#[test]
fn attr_read_cache_expired_entries_do_not_increment_evictions() {
    let mut cache = AttrReadCache::new(8, AttrReadCache::estimate_entry_charge() * 8);
    let now = Instant::now();
    cache.put(
        1,
        InodeAttr::new_file(1, ROOT_INO, 1000, 1000, 0o644),
        Duration::from_millis(10),
        now,
    );

    assert!(
        cache.get(1, now + Duration::from_secs(1)).is_none(),
        "expired entry should be treated as stale miss"
    );
    let metrics = cache.metrics();
    assert_eq!(metrics.evictions, 0);
    assert_eq!(metrics.misses, 1);
}

#[test]
fn test_probable_trash_component_names() {
    assert!(is_probable_trash_component(b".Trash"));
    assert!(is_probable_trash_component(b".Trashes"));
    assert!(is_probable_trash_component(b".Trash-501"));
    assert!(!is_probable_trash_component(b"Trash"));
    assert!(!is_probable_trash_component(b".Trashcan"));
}

#[test]
fn strict_local_sync_flushes_pending_group_commit_bytes() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "strict-local-sync-group");
    rt.block_on(async {
        let mut group = vfs.state.write_buffer.local_sync_group.lock().await;
        group.pending_bytes = 4096;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .block_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .block_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.maybe_block_store_sync_data_async(0, LocalSyncPolicy::Strict)
            .await
            .expect("strict local sync");
    });

    let calls_after = vfs
        .state
        .metrics
        .block_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .block_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "strict sync should force backend sync_data"
    );
    assert!(
        synced_after >= synced_before + 4096,
        "strict sync should account for pending group-commit bytes"
    );
    rt.block_on(async {
        let group = vfs.state.write_buffer.local_sync_group.lock().await;
        assert_eq!(group.pending_bytes, 0);
    });
}

#[test]
fn opportunistic_local_sync_defers_when_threshold_not_reached() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "opportunistic-local-sync-group");
    rt.block_on(async {
        let mut group = vfs.state.write_buffer.local_sync_group.lock().await;
        group.pending_bytes = 1024;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .block_store_sync_data_calls
        .load(Ordering::Relaxed);
    let deferred_before = vfs
        .state
        .metrics
        .block_store_sync_data_deferred_calls
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.maybe_block_store_sync_data_async(0, LocalSyncPolicy::Opportunistic)
            .await
            .expect("opportunistic local sync");
    });

    let calls_after = vfs
        .state
        .metrics
        .block_store_sync_data_calls
        .load(Ordering::Relaxed);
    let deferred_after = vfs
        .state
        .metrics
        .block_store_sync_data_deferred_calls
        .load(Ordering::Relaxed);
    assert_eq!(
        calls_after, calls_before,
        "opportunistic sync should not force sync_data under threshold/window"
    );
    assert!(
        deferred_after > deferred_before,
        "opportunistic defer should be tracked"
    );
}

#[test]
fn flush_fh_forces_strict_local_sync_when_dirty_state_remains() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-fh-strict-local-sync");
    let attr = vfs
        .create(1, b"flush-fh-strict-local-sync.txt", 0, 0, 0o644)
        .expect("create test file");
    let ino = attr.ino;
    let fh = vfs
        .open_file(ino, OpenFlags::RDWR, false)
        .expect("open writable handle");

    let mut dirty_attr = vfs.getattr(ino).expect("load attr");
    dirty_attr.size = 4096;
    dirty_attr.touch_mtime();
    dirty_attr.touch_ctime();
    vfs.state
        .namespace
        .dirty_attrs
        .lock()
        .insert(ino, dirty_attr.clone());

    rt.block_on(async {
        let mut group = vfs.state.write_buffer.local_sync_group.lock().await;
        group.pending_bytes = 4096;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .block_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .block_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.flush_fh(fh).await.expect("flush_fh barrier");
    });

    let calls_after = vfs
        .state
        .metrics
        .block_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .block_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "explicit flush must force local sync_data for dirty unpublished state"
    );
    assert!(
        synced_after >= synced_before + 4096,
        "explicit flush should account for the pending local durable sync bytes"
    );
    rt.block_on(async {
        let group = vfs.state.write_buffer.local_sync_group.lock().await;
        assert_eq!(
            group.pending_bytes, 0,
            "explicit flush should drain pending local sync bytes"
        );
    });
    assert!(
        !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
        "explicit flush should commit the dirty local attr state"
    );

    rt.block_on(async {
        vfs.release_fh(fh).await.expect("release handle");
    });
}

#[test]
fn flush_fh_materializes_opportunistic_handle_writer_state_into_overlay_slices() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-245-flush-handle-writer");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-flush-handle-writer.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"flush-materialize";

        vfs.write_fh_with_local_sync_policy(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert!(vfs.state.handle_writers.has_dirty_writers(ino));

        vfs.flush_fh(fh).await.unwrap();

        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().any(|span| span.block.is_some()),
            "flush_fh should materialize block-backed local unpublished slices: {spans:?}"
        );
        assert!(
            !vfs.state.handle_writers.has_dirty_writers(ino),
            "flush_fh should drain handle-writer dirty state"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn flush_fh_materializes_handle_writer_state_without_legacy_ordered_flush() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-245-flush-without-ordered-flush");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-flush-without-ordered-flush.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            0,
            b"direct-materialize",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        let ordered_flushes_before = vfs
            .state
            .metrics
            .flush_ordered_calls
            .load(Ordering::Relaxed);

        vfs.flush_fh(fh).await.unwrap();

        let ordered_flushes_after = vfs
            .state
            .metrics
            .flush_ordered_calls
            .load(Ordering::Relaxed);
        assert_eq!(
            ordered_flushes_after, ordered_flushes_before,
            "handle writer flush should materialize directly without routing through flush_ordered"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn release_fh_forces_strict_local_sync_when_last_handle_still_owns_dirty_state() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-fh-strict-local-sync");
    let attr = vfs
        .create(1, b"release-fh-strict-local-sync.txt", 0, 0, 0o644)
        .expect("create test file");
    let ino = attr.ino;
    let fh = vfs
        .open_file(ino, OpenFlags::RDWR, false)
        .expect("open writable handle");

    let mut dirty_attr = vfs.getattr(ino).expect("load attr");
    dirty_attr.size = 8192;
    dirty_attr.touch_mtime();
    dirty_attr.touch_ctime();
    vfs.state
        .namespace
        .dirty_attrs
        .lock()
        .insert(ino, dirty_attr.clone());

    rt.block_on(async {
        let mut group = vfs.state.write_buffer.local_sync_group.lock().await;
        group.pending_bytes = 8192;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .block_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .block_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);

    rt.block_on(async {
        let outcome = vfs
            .release_fh_with_outcome(fh)
            .await
            .expect("release_fh barrier");
        assert!(
            outcome.closed_last_handle,
            "the only handle should own the last-handle release path"
        );
        assert!(
            outcome.flushed_local_dirty_state,
            "last-handle release should report draining dirty local state"
        );
    });

    let calls_after = vfs
        .state
        .metrics
        .block_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .block_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "last-handle release must force local sync_data when dirty state remains"
    );
    assert!(
        synced_after >= synced_before + 8192,
        "last-handle release should account for the pending local sync bytes"
    );
    rt.block_on(async {
        let group = vfs.state.write_buffer.local_sync_group.lock().await;
        assert_eq!(
            group.pending_bytes, 0,
            "last-handle release should drain pending local sync bytes"
        );
    });
    assert!(
        !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
        "last-handle release should preserve the committed local attr state"
    );
}

#[test]
fn release_fh_materializes_opportunistic_handle_writer_state() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-245-release-handle-writer");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-release-handle-writer.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"release-materialize";

        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            0,
            payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        assert!(vfs.state.handle_writers.has_dirty_writers(ino));

        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
        assert!(outcome.closed_last_handle);
        assert!(outcome.flushed_local_dirty_state);

        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().any(|span| span.block.is_some()),
            "last-handle release should materialize block-backed local unpublished slices: {spans:?}"
        );
        assert!(
            !vfs.state.handle_writers.has_dirty_writers(ino),
            "last-handle release should drain handle-writer dirty state"
        );
    });
}

#[test]
fn write_path_observability_tracks_burst_shape_and_metadata_commit_stalls() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "write-path-observability-bursts");
    let block_size = meta.get_format_info().expect("get format info").block_size as usize;

    rt.block_on(async {
        let attr = vfs
            .create(1, b"write-path-burst.bin", 0, 0, 0o644)
            .expect("create");
        let first = vec![0x5Au8; block_size];
        let second = vec![0xA5u8; block_size];

        vfs.write(attr.ino, 0, &first).await.expect("first write");
        vfs.flush(attr.ino).await.expect("first flush");

        tokio::time::sleep(Duration::from_millis(2)).await;

        vfs.write(attr.ino, block_size as u64, &second)
            .await
            .expect("second write");
        vfs.flush(attr.ino).await.expect("second flush");

        // Force a deterministic durability barrier so the sync metric is stable.
        vfs.fsync(attr.ino, false).await.expect("fsync barrier");
    });

    let snapshot = vfs.write_path_observability_snapshot();
    assert!(
        snapshot.payload_burst.bursts >= 2,
        "two write+flush rounds should yield at least two payload bursts"
    );
    assert!(
        snapshot.payload_burst.total_blocks >= 2,
        "burst accounting should include persisted full blocks"
    );
    assert!(
        snapshot.payload_burst.total_bytes >= (block_size as u64) * 2,
        "burst accounting should include persisted payload bytes"
    );
    assert!(
        snapshot.payload_persist.calls >= 2,
        "payload stage timing should be captured per persisted burst"
    );
    assert!(
        snapshot.metadata_commit.calls >= 2,
        "metadata commit timing should be captured per persisted burst"
    );
    assert!(
        snapshot.metadata_commit_cadence.events >= 2,
        "metadata commit cadence should be captured per completed commit"
    );
    assert!(
        snapshot
            .metadata_commit_cadence
            .interval_histogram
            .bucket_counts
            .iter()
            .copied()
            .sum::<u64>()
            >= 1,
        "metadata commit cadence should record at least one interval sample after multiple commits"
    );
    assert!(
        snapshot.metadata_commit_cadence.max_interval_ms > 0.0,
        "metadata commit cadence should expose a positive interval after multiple commits"
    );
    assert!(
        snapshot.block_store_sync_data.calls >= 1,
        "sync barrier calls should be surfaced in write-path observability"
    );
    assert_eq!(
        snapshot.eager_flush.checks,
        snapshot.eager_flush.triggered
            + snapshot.eager_flush.skipped_empty
            + snapshot.eager_flush.skipped_below_threshold,
        "eager flush checks should be partitioned by outcome"
    );
}

#[test]
fn write_path_observability_reports_active_tuning_knobs() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_rt, vfs, meta) = make_test_vfs(&tmp, "write-path-observability-tuning");
    let block_size = meta.get_format_info().expect("get format info").block_size as usize;

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(
        snapshot.tuning.eager_flush_threshold_bytes, vfs.write_eager_flush_batch_bytes,
        "observability should report the active eager flush threshold"
    );
    assert!(
        snapshot.tuning.eager_flush_drain_chunk_bytes > 0,
        "observability should report a non-zero eager drain chunk"
    );
    assert!(
        snapshot.tuning.eager_flush_drain_chunk_bytes
            <= snapshot.tuning.eager_flush_threshold_bytes,
        "eager drain chunk should never exceed the trigger threshold"
    );
    assert!(
        snapshot.tuning.payload_put_concurrency >= 1,
        "payload put concurrency should be explicit in observability"
    );
    assert!(
        snapshot.tuning.payload_put_concurrency
            <= snapshot
                .tuning
                .eager_flush_drain_chunk_bytes
                .div_ceil(block_size)
                .max(1),
        "payload put concurrency should be bounded by the eager drain chunk"
    );
    assert_eq!(
        snapshot.tuning.sync_group_min_bytes, vfs.local_sync_group_min_bytes,
        "observability should report the active sync-data group minimum"
    );
    assert_eq!(
        snapshot.tuning.sync_group_max_delay_ms,
        vfs.local_sync_group_max_delay.as_millis() as u64,
        "observability should report the active sync-data delay cap"
    );
}

#[test]
fn winfsp_observability_snapshot_tracks_callback_and_barrier_paths() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "winfsp-observability-snapshot");

    vfs.note_winfsp_write_callback();
    vfs.note_winfsp_overwrite_callback();
    vfs.note_winfsp_cleanup_callback();
    vfs.note_winfsp_cleanup_flush_fh_callback();
    vfs.note_winfsp_flush_callback();
    vfs.note_winfsp_close_callback();
    vfs.note_winfsp_cleanup_barrier_outcome(true);
    vfs.note_winfsp_cleanup_barrier_outcome(false);
    vfs.note_winfsp_flush_barrier_outcome(true);
    vfs.note_winfsp_flush_barrier_outcome(false);
    vfs.note_winfsp_close_barrier_outcome(false);

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(snapshot.winfsp_callbacks.write, 1);
    assert_eq!(snapshot.winfsp_callbacks.overwrite, 1);
    assert_eq!(snapshot.winfsp_callbacks.cleanup, 1);
    assert_eq!(snapshot.winfsp_callbacks.cleanup_flush_fh, 1);
    assert_eq!(snapshot.winfsp_callbacks.flush, 1);
    assert_eq!(snapshot.winfsp_callbacks.close, 1);
    assert_eq!(snapshot.winfsp_foreground_flushes.cleanup, 1);
    assert_eq!(snapshot.winfsp_foreground_flushes.cleanup_skipped_clean, 1);
    assert_eq!(snapshot.winfsp_foreground_flushes.flush, 1);
    assert_eq!(snapshot.winfsp_foreground_flushes.flush_skipped_clean, 1);
    assert_eq!(snapshot.winfsp_foreground_flushes.close, 0);
    assert_eq!(snapshot.winfsp_foreground_flushes.close_skipped_clean, 1);
}

#[test]
fn failed_payload_persist_attempts_do_not_count_as_completed_bursts() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "write-path-observability-failed-burst");

    vfs.record_payload_persist_observability(4, 16 * 1024, Duration::from_millis(3), false);

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(
        snapshot.payload_persist.calls, 1,
        "payload stage latency should still include failed attempts"
    );
    assert_eq!(
        snapshot.payload_burst.bursts, 0,
        "failed payload persists must not count as completed bursts"
    );
    assert_eq!(
        snapshot.payload_burst.total_blocks, 0,
        "failed payload persists must not add completed burst block counts"
    );
    assert_eq!(
        snapshot.payload_burst.total_bytes, 0,
        "failed payload persists must not add completed burst bytes"
    );
}

#[test]
fn local_cache_io_stats_track_handoff_and_backend_time_separately() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-io-stats");
    let calls_before = vfs
        .state
        .metrics
        .local_cache_io_calls
        .load(Ordering::Relaxed);
    let handoff_before = vfs
        .state
        .metrics
        .local_cache_io_handoff_total_ns
        .load(Ordering::Relaxed);
    let backend_before = vfs
        .state
        .metrics
        .local_cache_io_backend_total_ns
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.block_store_put_async(0xAA55, Bytes::from_static(b"payload"), false)
            .await
            .expect("block store put");
        let payload = vfs
            .block_store_get_async(0xAA55)
            .await
            .expect("block store get")
            .expect("cached payload");
        assert_eq!(payload.as_ref(), b"payload");
    });

    let calls_after = vfs
        .state
        .metrics
        .local_cache_io_calls
        .load(Ordering::Relaxed);
    let handoff_after = vfs
        .state
        .metrics
        .local_cache_io_handoff_total_ns
        .load(Ordering::Relaxed);
    let backend_after = vfs
        .state
        .metrics
        .local_cache_io_backend_total_ns
        .load(Ordering::Relaxed);

    assert!(
        calls_after >= calls_before + 2,
        "put + get should record at least two local-cache I/O calls"
    );
    assert!(
        handoff_after >= handoff_before,
        "blocking-isolated local-cache operations may accrue scheduler handoff time"
    );
    assert!(
        backend_after >= backend_before,
        "backend execution time should be tracked independently"
    );

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(
        snapshot.local_cache_io.execution_model,
        "inline-runtime-worker-with-blocking-isolation"
    );
}

#[test]
fn local_cache_io_stats_track_direct_partial_read_into() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-read-into-stats");
    let calls_before = vfs
        .state
        .metrics
        .local_cache_io_calls
        .load(Ordering::Relaxed);
    let backend_before = vfs
        .state
        .metrics
        .local_cache_io_backend_total_ns
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.block_store_put_async(0xBB66, Bytes::from_static(b"payload"), false)
            .await
            .expect("block store put");
        let mut out = [0_u8; 3];
        let read = vfs
            .block_store_read_into_async(0xBB66, 2, &mut out)
            .await
            .expect("block store read_into");
        assert_eq!(read, Some(3));
        assert_eq!(&out, b"ylo");
    });

    let calls_after = vfs
        .state
        .metrics
        .local_cache_io_calls
        .load(Ordering::Relaxed);
    let backend_after = vfs
        .state
        .metrics
        .local_cache_io_backend_total_ns
        .load(Ordering::Relaxed);

    assert!(
        calls_after >= calls_before + 2,
        "put + read_into should record local-cache I/O calls"
    );
    assert!(
        backend_after >= backend_before,
        "direct read_into should contribute backend execution timing"
    );
}

#[test]
fn local_cache_read_into_returns_none_for_missing_block() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-read-into-missing");
    rt.block_on(async {
        let mut out = [0_u8; 8];
        let read = vfs
            .block_store_read_into_async(0xDEADBEEF, 0, &mut out)
            .await
            .expect("read_into should not error on cache miss");
        assert_eq!(read, None);
    });
}

#[test]
fn committed_copy_rejects_short_payload() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "committed-copy-short-payload");
    let block = BlockRef::new_full(42, vfs.block_size);
    let mut dst = [0_u8; 8];
    let err = vfs
        .copy_committed_block_payload_into_range(b"abcd", &block, 0, &mut dst)
        .expect_err("short committed payload should error");
    let message = match err {
        VfsError::Other(message) => message,
        other => panic!("expected VfsError::Other, got {other}"),
    };
    assert!(
        message.contains("committed block payload too short"),
        "error message should highlight short committed payload"
    );
}

#[test]
fn histogram_percentile_marks_overflow_bucket_as_saturated() {
    let snapshot = studiofs_cache::metrics::LatencyHistogramSnapshot {
        bucket_upper_bounds_ns: vec![1_000_000, 2_000_000],
        bucket_counts: vec![1, 1, 3],
    };
    let (value_ns, saturated) = StudioVfs::histogram_percentile_ns(&snapshot, 0.99);
    assert_eq!(value_ns, 2_000_000);
    assert!(saturated);
}

#[test]
fn histogram_percentile_non_overflow_bucket_is_not_saturated() {
    let snapshot = studiofs_cache::metrics::LatencyHistogramSnapshot {
        bucket_upper_bounds_ns: vec![1_000_000, 2_000_000],
        bucket_counts: vec![1, 2, 0],
    };
    let (value_ns, saturated) = StudioVfs::histogram_percentile_ns(&snapshot, 0.95);
    assert_eq!(value_ns, 2_000_000);
    assert!(!saturated);
}

#[test]
fn build_cadence_stats_averages_over_completed_intervals() {
    let histogram = studiofs_cache::metrics::LatencyHistogramSnapshot {
        bucket_upper_bounds_ns: vec![10_000_000, 20_000_000],
        bucket_counts: vec![1, 0, 0],
    };

    let stats = StudioVfs::build_cadence_stats(
        3,
        10_000_000,
        10_000_000,
        histogram,
        Duration::from_secs(1),
    );

    assert_eq!(
        stats.avg_interval_ms, 5.0,
        "cadence averages should divide by completed intervals (events - 1), not raw histogram sample count"
    );
}
