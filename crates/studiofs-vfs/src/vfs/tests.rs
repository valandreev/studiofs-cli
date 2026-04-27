use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use studiofs_meta::engine::MetadataAuthorityStatus;
use studiofs_meta::engine::{MetaInodes, MetaPayloadObjects};
use studiofs_meta::schema::{
    CURRENT_FORMAT_VERSION, ChunkId, DEFAULT_BLOCK_SIZE, FormatInfo, InodeLifecycleState,
    MetadataConfig, PayloadRefState, PayloadRetentionClass, ROOT_INO, SliceId, SliceOrder,
    SliceRange, SliceRecord,
};
use studiofs_meta::{
    FileLockLease, HardLockKind, MetaEngine, MetaFormat, MetaFsOps, RedbMetaEngine,
};
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

fn make_unstarted_test_vfs_with_config(
    tmp: &TempDir,
    volume_suffix: &str,
    cfg: VfsConfig,
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
        cfg.try_into().expect("test VFS config should validate"),
    )
    .expect("create vfs");
    (rt, vfs, meta)
}

fn make_unstarted_test_vfs(
    tmp: &TempDir,
    volume_suffix: &str,
) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
    make_unstarted_test_vfs_with_config(tmp, volume_suffix, VfsConfig::default())
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

fn make_test_vfs_with_config(
    tmp: &TempDir,
    volume_suffix: &str,
    cfg: VfsConfig,
) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
    let (rt, vfs, meta) = make_unstarted_test_vfs_with_config(tmp, volume_suffix, cfg);
    let _guard = rt.enter();
    vfs.start().expect("start vfs background workers");
    (rt, vfs, meta)
}

trait FileGraphMetaFixture {
    fn stage_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
        payload: &PayloadRef,
    ) -> MetaResult<Option<PayloadRef>>;
    fn lookup_payload_slices(&self, ino: Ino) -> MetaResult<Vec<(u32, PayloadRef)>>;
}

fn fixture_payload_slices(
    block_index: u32,
    payload: Option<&PayloadRef>,
) -> MetaResult<Vec<(ChunkId, Vec<SliceRecord>)>> {
    let block_start = u64::from(block_index)
        .checked_mul(u64::from(DEFAULT_BLOCK_SIZE))
        .ok_or_else(|| MetaError::Storage("fixture block offset overflow".to_string()))?;
    let data_end = block_start
        .checked_add(u64::from(payload.map_or(0, |payload| payload.length)))
        .ok_or_else(|| MetaError::Storage("fixture block data overflow".to_string()))?;
    let block_end = block_start
        .checked_add(u64::from(DEFAULT_BLOCK_SIZE))
        .ok_or_else(|| MetaError::Storage("fixture block range overflow".to_string()))?;
    let mut chunks = std::collections::BTreeMap::<ChunkId, Vec<SliceRecord>>::new();
    let mut push = |start: u64, end: u64, source: Option<&PayloadRef>| -> MetaResult<()> {
        let mut cursor = start;
        while cursor < end {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let piece_end = end.min(chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64));
            let range = SliceRange::new(
                (cursor - chunk_start) as u32,
                (piece_end - chunk_start) as u32,
            )
            .ok_or_else(|| MetaError::Storage("fixture invalid slice range".to_string()))?;
            let mut slice = if let Some(source) = source {
                let delta = u32::try_from(cursor.saturating_sub(block_start))
                    .map_err(|_| MetaError::Storage("fixture slice delta overflow".to_string()))?;
                SliceRecord::new(
                    SliceId(0),
                    SliceOrder(0),
                    range,
                    PayloadRef {
                        object_id: source.object_id,
                        offset: source.offset.saturating_add(delta),
                        length: u32::try_from(piece_end.saturating_sub(cursor)).map_err(|_| {
                            MetaError::Storage("fixture slice length overflow".to_string())
                        })?,
                        flags: source.flags,
                    },
                )
            } else {
                SliceRecord::tombstone(SliceId(0), SliceOrder(0), range)
            };
            slice.payload_state = PayloadRefState::LocalOnly;
            chunks.entry(chunk_id).or_default().push(slice);
            cursor = piece_end;
        }
        Ok(())
    };
    push(block_start, data_end, payload)?;
    if data_end < block_end {
        push(data_end, block_end, None)?;
    }
    Ok(chunks.into_iter().collect())
}

impl FileGraphMetaFixture for Arc<RedbMetaEngine> {
    fn stage_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
        payload: &PayloadRef,
    ) -> MetaResult<Option<PayloadRef>> {
        self.flush_regular_file_overlay_slices(
            ino,
            fixture_payload_slices(block_index, Some(payload))?.as_slice(),
        )?;
        Ok(None)
    }

    fn lookup_payload_slices(&self, ino: Ino) -> MetaResult<Vec<(u32, PayloadRef)>> {
        let attr = self.get_attr(ino)?;
        let mut blocks = BTreeMap::<u32, PayloadRef>::new();
        for span in self.lookup_regular_file_slices(ino, 0, attr.size)? {
            let Some(block) = span.payload else {
                continue;
            };
            let chunk_start = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
            let span_start = chunk_start.saturating_add(u64::from(span.range.start));
            let span_end = chunk_start.saturating_add(u64::from(span.range.end));
            let mut cursor = span_start;
            while cursor < span_end {
                let block_index = u32::try_from(cursor / u64::from(DEFAULT_BLOCK_SIZE))
                    .map_err(|_| MetaError::Storage("fixture block index overflow".to_string()))?;
                let logical_block_end =
                    (u64::from(block_index) + 1).saturating_mul(u64::from(DEFAULT_BLOCK_SIZE));
                let piece_end = span_end.min(logical_block_end);
                let piece_len = u32::try_from(piece_end.saturating_sub(cursor))
                    .map_err(|_| MetaError::Storage("fixture block length overflow".to_string()))?;
                let delta = u32::try_from(cursor.saturating_sub(span_start))
                    .map_err(|_| MetaError::Storage("fixture block offset overflow".to_string()))?;
                blocks
                    .entry(block_index)
                    .and_modify(|existing| {
                        existing.length = existing.length.saturating_add(piece_len);
                        existing.flags |= block.flags;
                    })
                    .or_insert_with(|| PayloadRef {
                        object_id: block.object_id,
                        offset: block.offset.saturating_add(delta),
                        length: piece_len,
                        flags: block.flags,
                    });
                cursor = piece_end;
            }
        }
        Ok(blocks.into_iter().collect())
    }
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

fn mark_all_overlay_objects_uploaded(
    vfs: &Arc<StudioVfs>,
    meta: &Arc<RedbMetaEngine>,
    ino: Ino,
) -> (u64, Vec<u64>) {
    let attr = meta.get_attr(ino).expect("load inode attrs");
    let object_ids = meta
        .get_regular_file_overlay_object_ids(ino)
        .expect("load overlay object ids");
    for object_id in &object_ids {
        vfs.local_payload_backend().mark_uploaded(*object_id);
        assert!(
            meta.mark_regular_file_overlay_object_uploaded_if_matches(
                ino,
                attr.content_epoch,
                *object_id
            )
            .expect("mark overlay object uploaded"),
            "expected metadata upload mark to apply for every overlay object"
        );
    }
    (attr.content_epoch, object_ids)
}

fn publish_current_file_graph(
    meta: &Arc<RedbMetaEngine>,
    ino: Ino,
    content_epoch: u64,
    _object_ids: &[u64],
) {
    assert!(
        meta.publish_regular_file_overlay_to_base_if_matches(ino, content_epoch)
            .expect("publish current file graph"),
        "expected file graph publish to apply"
    );
}

fn install_fragmented_overlay_block(
    vfs: &Arc<StudioVfs>,
    meta: &Arc<RedbMetaEngine>,
    ino: Ino,
    block_size: usize,
    left_fill: u8,
    right_fill: u8,
) -> Vec<u8> {
    let half = block_size / 2;
    let left = vec![left_fill; half];
    let right = vec![right_fill; block_size - half];
    let object_ids = meta
        .next_object_ids(2)
        .expect("allocate fragmented object ids");

    vfs.local_payload_backend()
        .put(object_ids[0], &left, false)
        .expect("persist left fragmented object");
    vfs.local_payload_backend()
        .put(object_ids[1], &right, false)
        .expect("persist right fragmented object");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync fragmented overlay objects");

    meta.flush_regular_file_overlay_slices_with_attr(
        ino,
        &[(
            ChunkId(0),
            vec![
                SliceRecord::new(
                    SliceId(0),
                    SliceOrder(0),
                    SliceRange::new(0, half as u32).expect("left slice range"),
                    PayloadRef::new_partial(object_ids[0], half as u32),
                ),
                SliceRecord::new(
                    SliceId(1),
                    SliceOrder(1),
                    SliceRange::new(half as u32, block_size as u32).expect("right slice range"),
                    PayloadRef::new_partial(object_ids[1], (block_size - half) as u32),
                ),
            ],
        )],
        None,
    )
    .expect("persist fragmented overlay slices");

    vfs.remove_live_extents_for_inode(ino);
    vfs.cancel_pending_uploads_for_inode(ino);

    let mut expected = left;
    expected.extend_from_slice(&right);
    expected
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
    meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(9001, 4096))
        .expect("persist current payload slice");

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
    meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(9001, 4096))
        .expect("persist current payload slice");

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
    meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(9001, 4096))
        .expect("persist base payload slice");
    meta.flush_regular_file_overlay_slices_with_attr(
        ino,
        &[(
            studiofs_meta::schema::ChunkId(0),
            vec![studiofs_meta::schema::SliceRecord {
                slice_id: studiofs_meta::schema::SliceId::default(),
                order: studiofs_meta::schema::SliceOrder::default(),
                range: studiofs_meta::schema::SliceRange::new(0, 4096)
                    .expect("overlay slice range"),
                payload: Some(studiofs_meta::schema::PayloadRef {
                    object_id: 9002,
                    offset: 0,
                    length: 4096,
                    flags: studiofs_meta::schema::BlockFlags::PARTIAL_BLOCK,
                }),
                payload_state: studiofs_meta::schema::PayloadRefState::LocalOnly,
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
fn pending_upload_views_follow_runtime_queue_after_startup_replay() {
    let tmp = TempDir::new().expect("tempdir");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "pending-views-runtime-queue");

    let created = vfs
        .create(1, b"persistent-pending.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let object_id = 7_001;
    let payload = vec![0xAB; 4096];

    vfs.local_payload_backend()
        .put(object_id, &payload, false)
        .expect("persist local payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync local payload");

    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = payload.len() as u64;
    attr.committed_size = 0;
    attr.content_epoch = 5;
    attr.committed_content_epoch = 4;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.stage_payload_slice(
        ino,
        0,
        &PayloadRef::new_partial(object_id, payload.len() as u32),
    )
    .expect("persist current payload slice");
    assert!(
        vfs.cached_store().snapshot_pending_uploads().is_empty(),
        "test setup requires startup replay to seed the runtime queue"
    );

    vfs.replay_dirty_uploads_from_local_backend(&[], &[attr.clone()]);

    assert_eq!(
        vfs.pending_uploads_for_inode(ino),
        vec![(0, object_id, attr.content_epoch)],
        "pending upload helpers must follow the runtime queue rebuilt during startup replay"
    );

    let pending_upload_index = vfs.snapshot_pending_upload_index();
    assert_eq!(
        pending_upload_index.latest_extent_record(ino, 0, Some(attr.content_epoch)),
        Some(ExtentRecordId {
            object_id,
            content_epoch: attr.content_epoch,
        }),
        "pending upload catalog must derive live extents from the replayed runtime queue"
    );
}

#[test]
fn startup_replay_rebuilds_runtime_pending_upload_index_from_persisted_state() {
    let tmp = TempDir::new().expect("tempdir");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-replay-runtime-uploads");

    let created = vfs
        .create(1, b"startup-replay.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let object_id = 7_011;
    let payload = vec![0xDA; 4096];

    vfs.local_payload_backend()
        .put(object_id, &payload, false)
        .expect("persist local payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync local payload");

    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = payload.len() as u64;
    attr.committed_size = 0;
    attr.content_epoch = 9;
    attr.committed_content_epoch = 8;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.stage_payload_slice(
        ino,
        0,
        &PayloadRef::new_partial(object_id, payload.len() as u32),
    )
    .expect("persist current payload slice");

    assert!(
        vfs.cached_store().snapshot_pending_uploads().is_empty(),
        "test setup requires the runtime queue to start empty"
    );
    vfs.replay_dirty_uploads_from_local_backend(&[], &[attr.clone()]);

    let runtime_pending = vfs.cached_store().snapshot_pending_uploads();
    assert_eq!(runtime_pending.len(), 1);
    assert_eq!(runtime_pending[0].ino, ino);
    assert_eq!(runtime_pending[0].object_id, object_id);
    assert!(
        vfs.cached_store().is_pinned(object_id),
        "requeued runtime upload should repin the durable local payload"
    );
    assert!(
        vfs.cached_store().has_staged_upload(object_id),
        "startup replay should restore upload links so recovered uploads do not depend on cold rereads"
    );
}

#[test]
fn startup_replay_summary_reports_recovered_bytes_and_queued_uploads() {
    let tmp = TempDir::new().expect("tempdir");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-replay-summary-bytes");

    let created = vfs
        .create(1, b"startup-summary.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let object_id = 7_021;
    let payload = vec![0xE1; 8192];

    vfs.local_payload_backend()
        .put(object_id, &payload, false)
        .expect("persist local payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync local payload");

    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = payload.len() as u64;
    attr.committed_size = 0;
    attr.content_epoch = 11;
    attr.committed_content_epoch = 10;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.stage_payload_slice(
        ino,
        0,
        &PayloadRef::new_partial(object_id, payload.len() as u32),
    )
    .expect("persist current payload slice");

    vfs.replay_dirty_uploads_from_local_backend(&[], &[attr]);

    let summary = vfs
        .zeroio_runtime_snapshot()
        .startup_replay
        .expect("startup replay summary should be visible");
    assert_eq!(summary.replayed_blocks, 1);
    assert_eq!(summary.replayed_bytes, payload.len() as u64);
    assert_eq!(summary.queued_uploads, 1);
    assert_eq!(summary.discarded_memory_only_bytes, 0);
}

#[test]
fn startup_replay_restores_live_extents_for_duplicate_dirty_object_references() {
    let tmp = TempDir::new().expect("tempdir");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-replay-duplicate-object");

    let first = vfs
        .create(1, b"duplicate-object-a.bin", 0, 0, 0o644)
        .expect("create first file");
    let second = vfs
        .create(1, b"duplicate-object-b.bin", 0, 0, 0o644)
        .expect("create second file");
    let object_id = 7_031;
    let payload = vec![0xC7; 4096];

    vfs.local_payload_backend()
        .put(object_id, &payload, false)
        .expect("persist shared dirty payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync shared dirty payload");

    let mut attrs = Vec::new();
    for ino in [first.ino, second.ino] {
        let mut attr = meta.get_attr(ino).expect("load attr");
        attr.size = payload.len() as u64;
        attr.committed_size = 0;
        attr.content_epoch = 12;
        attr.committed_content_epoch = 11;
        attr.upload_state = MetaUploadState::Uploading;
        meta.set_attr(&attr).expect("persist staged attr");
        meta.stage_payload_slice(
            ino,
            0,
            &PayloadRef::new_partial(object_id, payload.len() as u32),
        )
        .expect("persist duplicate object reference");
        attrs.push(attr);
    }

    vfs.replay_dirty_uploads_from_local_backend(&[], &attrs);

    assert_eq!(
        vfs.live_extent_records_for_inode(first.ino),
        vec![(0, object_id, 12)],
        "first file should recover a live extent for the shared dirty object"
    );
    assert_eq!(
        vfs.live_extent_records_for_inode(second.ino),
        vec![(0, object_id, 12)],
        "second file should also recover a live extent even though upload replay is object-deduped"
    );
    assert_eq!(
        vfs.cached_store().snapshot_pending_uploads().len(),
        1,
        "startup replay should still submit only one upload for one dirty object"
    );
}

#[test]
fn startup_replay_restores_all_block_extents_when_one_file_reuses_dirty_object_id() {
    let tmp = TempDir::new().expect("tempdir");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "startup-replay-duplicate-blocks");

    let created = vfs
        .create(1, b"duplicate-blocks.bin", 0, 0, 0o644)
        .expect("create test file");
    let object_id = 7_032;
    let block_len = vfs.block_size;

    vfs.local_payload_backend()
        .put(object_id, &vec![0xD4; block_len as usize], false)
        .expect("persist shared dirty payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync shared dirty payload");

    let mut attr = meta.get_attr(created.ino).expect("load attr");
    attr.size = u64::from(block_len) * 2;
    attr.committed_size = 0;
    attr.content_epoch = 13;
    attr.committed_content_epoch = 12;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist staged attr");
    meta.stage_payload_slice(
        created.ino,
        0,
        &PayloadRef::new_partial(object_id, block_len),
    )
    .expect("persist first duplicate object reference");
    meta.stage_payload_slice(
        created.ino,
        1,
        &PayloadRef::new_partial(object_id, block_len),
    )
    .expect("persist second duplicate object reference");

    vfs.replay_dirty_uploads_from_local_backend(&[], &[attr]);

    assert_eq!(
        vfs.live_extent_records_for_inode(created.ino),
        vec![(0, object_id, 13), (1, object_id, 13)],
        "startup replay should restore every logical block covered by the reused dirty object"
    );
    assert_eq!(
        vfs.cached_store().snapshot_pending_uploads().len(),
        1,
        "one physical dirty object should still map to one upload submission"
    );
    let summary = vfs
        .zeroio_runtime_snapshot()
        .startup_replay
        .expect("startup replay summary should be visible");
    assert_eq!(
        summary.replayed_blocks, 2,
        "replayed_blocks should count restored logical extents, not deduped upload ops"
    );
    assert_eq!(
        summary.queued_uploads, 1,
        "queued_uploads should stay deduped to one physical upload op"
    );
}

#[test]
fn flush_queues_runtime_upload_immediately_after_local_durability() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "flush-queues-runtime-upload");
    let _guard = rt.enter();

    let created = vfs
        .create(1, b"flush-truth.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let payload = vec![0x5Au8; vfs.block_size as usize];

    rt.block_on(async {
        vfs.write(ino, 0, &payload)
            .await
            .expect("write test payload");
        vfs.flush(ino).await.expect("flush durable local payload");
    });

    let pending_uploads = vfs.pending_uploads_for_inode(ino);
    assert_eq!(
        pending_uploads.len(),
        1,
        "local durability should enqueue the unpublished payload object into the runtime upload queue"
    );

    let runtime_pending = vfs.cached_store().snapshot_pending_uploads();
    assert_eq!(runtime_pending.len(), 1);
    assert_eq!(runtime_pending[0].ino, ino);
    assert_eq!(runtime_pending[0].object_id, pending_uploads[0].1);
}

#[test]
fn buffered_partial_write_uses_fragmented_overlay_slice_graph_as_base() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "fragmented-overlay-buffered-base");
    let _guard = rt.enter();
    rt.block_on(async {
        let created = vfs
            .create(1, b"fragmented-buffered-base.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = created.ino;
        let block_size = vfs.block_size as usize;
        let baseline = vec![0x33u8; block_size];

        vfs.write(ino, 0, &baseline).await.expect("write baseline");
        vfs.flush(ino).await.expect("flush baseline");

        let mut expected =
            install_fragmented_overlay_block(&vfs, &meta, ino, block_size, 0x11, 0x22);
        let patch_offset = 128 * 1024usize;
        let patch = vec![0x7Au8; 8192];
        expected[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);

        vfs.write(ino, patch_offset as u64, &patch)
            .await
            .expect("write patch over fragmented overlay");
        vfs.flush(ino)
            .await
            .expect("flush patch over fragmented overlay");

        let read = vfs
            .read(ino, 0, block_size as u32)
            .await
            .expect("read patched block");
        assert_eq!(
            read.as_ref(),
            expected.as_slice(),
            "buffered write path should reuse fragmented overlay slice graph as its base payload"
        );
    });
}

#[test]
fn release_fh_materializes_fragmented_overlay_slice_graph_base() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_test_vfs(&tmp, "fragmented-overlay-handle-base");
    let _guard = rt.enter();
    rt.block_on(async {
        let created = vfs
            .create(1, b"fragmented-handle-base.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = created.ino;
        let block_size = vfs.block_size as usize;
        let baseline = vec![0x44u8; block_size];

        vfs.write(ino, 0, &baseline).await.expect("write baseline");
        vfs.flush(ino).await.expect("flush baseline");

        let mut expected =
            install_fragmented_overlay_block(&vfs, &meta, ino, block_size, 0x21, 0x42);
        let patch_offset = 64 * 1024usize;
        let patch = vec![0x6Bu8; 4096];
        expected[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);

        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open test handle");
        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            patch_offset as u64,
            &patch,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("write handle patch over fragmented overlay");

        let outcome = vfs
            .release_fh_with_outcome(fh)
            .await
            .expect("release should materialize fragmented overlay base");
        assert!(outcome.closed_last_handle);
        assert!(outcome.flushed_local_dirty_state);

        let read = vfs
            .read(ino, 0, block_size as u32)
            .await
            .expect("read released handle block");
        assert_eq!(
            read.as_ref(),
            expected.as_slice(),
            "handle-writer materialization should reuse fragmented overlay slice graph as its base payload"
        );
    });
}

#[test]
fn wait_for_uploads_idle_observes_runtime_upload_queue() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "wait-for-runtime-upload-idle");
    let _guard = rt.enter();

    let created = vfs
        .create(1, b"wait-for-truth-idle.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let payload = vec![0x33u8; vfs.block_size as usize];

    rt.block_on(async {
        vfs.write(ino, 0, &payload)
            .await
            .expect("write test payload");
        vfs.flush(ino).await.expect("flush durable local payload");
    });

    assert!(
        !rt.block_on(vfs.wait_for_uploads_idle(Duration::from_millis(25))),
        "wait_for_uploads_idle must not report idle while runtime uploads are still queued"
    );
}

#[test]
fn upload_stats_report_backlog_bytes_age_retries_active_and_completion_rate() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "upload-stats-backlog");
    let _guard = rt.enter();

    let created = vfs
        .create(1, b"upload-stats.bin", 0, 0, 0o644)
        .expect("create test file");
    let payload = vec![0x44u8; vfs.block_size as usize];

    rt.block_on(async {
        vfs.write(created.ino, 0, &payload)
            .await
            .expect("write test payload");
        vfs.flush(created.ino)
            .await
            .expect("flush durable local payload");
    });

    let pending = vfs.cached_store().snapshot_pending_uploads();
    assert_eq!(pending.len(), 1, "test should stage one upload object");

    let stats = vfs.upload_stats();
    assert_eq!(stats.pending_uploads, 1);
    assert_eq!(stats.pending_upload_bytes, payload.len() as u64);
    assert!(
        stats.oldest_pending_upload_age_ms < 5_000,
        "newly staged upload should report a fresh backlog age, got {} ms",
        stats.oldest_pending_upload_age_ms
    );
    assert_eq!(stats.active_uploads, 0);
    assert_eq!(stats.pending_retry_count, 0);
    assert_eq!(stats.max_pending_retry_count, 0);
    assert_eq!(stats.completed_uploads, 0);
    assert_eq!(stats.completed_upload_bytes, 0);
    assert_eq!(stats.completed_uploads_per_sec, 0.0);
    assert_eq!(stats.completed_upload_bytes_per_sec, 0.0);

    let mut retry = pending[0].clone();
    retry.retry_count = 3;
    retry.not_before_unix_ms = retry.created_unix_ms.saturating_add(1_000);
    assert!(vfs.cached_store().requeue_upload(retry));
    let retry_stats = vfs.upload_stats();
    assert_eq!(retry_stats.pending_uploads, 1);
    assert_eq!(retry_stats.pending_retry_count, 3);
    assert_eq!(retry_stats.max_pending_retry_count, 3);

    vfs.cached_store().complete_upload(&pending[0]);

    let completed = vfs.upload_stats();
    assert_eq!(completed.pending_uploads, 0);
    assert_eq!(completed.pending_upload_bytes, 0);
    assert_eq!(completed.completed_uploads, 1);
    assert_eq!(completed.completed_upload_bytes, payload.len() as u64);
    assert!(completed.completed_uploads_per_sec >= 0.0);
    assert!(completed.completed_upload_bytes_per_sec >= 0.0);
}

#[test]
fn upload_stats_deserializes_snapshots_without_new_backlog_fields() {
    let stats: VfsUploadStats = serde_json::from_value(serde_json::json!({
        "pending_uploads": 2,
        "upload_requeues": 1,
        "canceled_deleted": 0,
        "canceled_stale": 0
    }))
    .expect("old upload stats snapshots should remain readable");

    assert_eq!(stats.pending_uploads, 2);
    assert_eq!(stats.pending_upload_bytes, 0);
    assert_eq!(stats.oldest_pending_upload_age_ms, 0);
    assert_eq!(stats.active_uploads, 0);
    assert_eq!(stats.pending_retry_count, 0);
    assert_eq!(stats.max_pending_retry_count, 0);
    assert_eq!(stats.completed_uploads, 0);
    assert_eq!(stats.completed_upload_bytes, 0);
    assert_eq!(stats.completed_uploads_per_sec, 0.0);
    assert_eq!(stats.completed_upload_bytes_per_sec, 0.0);
    assert_eq!(stats.upload_requeues, 1);
}

#[test]
fn pending_upload_truth_ignores_deleted_inode_local_payloads() {
    let tmp = TempDir::new().expect("tempdir");
    let (_rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "pending-views-deleted-inode");

    let created = vfs
        .create(1, b"deleted-pending.bin", 0, 0, 0o644)
        .expect("create test file");
    let ino = created.ino;
    let object_id = 7_002;
    let payload = vec![0xCD; 4096];

    vfs.local_payload_backend()
        .put(object_id, &payload, false)
        .expect("persist local payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync local payload");

    let mut attr = meta.get_attr(ino).expect("load test attr");
    attr.size = payload.len() as u64;
    attr.committed_size = 0;
    attr.content_epoch = 6;
    attr.committed_content_epoch = 5;
    attr.nlink = 0;
    attr.upload_state = MetaUploadState::Uploading;
    meta.set_attr(&attr).expect("persist deleted attr");
    meta.stage_payload_slice(
        ino,
        0,
        &PayloadRef::new_partial(object_id, payload.len() as u32),
    )
    .expect("persist deleted payload slice");

    assert!(
        vfs.pending_uploads_for_inode(ino).is_empty(),
        "deleted inodes must keep local payloads readable without advertising them as pending uploads"
    );

    let pending_upload_index = vfs.snapshot_pending_upload_index();
    assert_eq!(
        pending_upload_index.latest_extent_record(ino, 0, Some(attr.content_epoch)),
        None,
        "pending upload catalog must not expose deleted inode blocks as uploadable extents"
    );
}

#[test]
fn startup_replay_removes_orphan_dirty_payloads_and_clears_pending_evictions() {
    let tmp = TempDir::new().expect("tempdir");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "startup-replay-orphan-cleanup");
    let object_id = 991_337;

    vfs.local_payload_backend()
        .put(object_id, b"orphan-dirty-payload", false)
        .expect("persist orphan dirty payload");
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync orphan dirty payload");
    vfs.state.upload.pending_evictions.lock().insert(object_id);

    assert!(
        vfs.local_payload_backend()
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
        !vfs.local_payload_backend().contains(object_id),
        "startup replay should remove orphan dirty payload without metadata reference"
    );
    assert!(
        !vfs.local_payload_backend()
            .dirty_object_ids()
            .contains(&object_id),
        "startup replay should clear orphan dirty payload from dirty index"
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
fn startup_replay_removes_unreferenced_warm_payloads_and_stage_links() {
    let tmp = TempDir::new().expect("tempdir");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "startup-replay-warm-cache-cleanup");
    let object_id = 991_338;

    vfs.local_payload_backend()
        .put(object_id, b"orphan-warm-payload", true)
        .expect("persist orphan warm payload");
    vfs.local_payload_backend().mark_uploaded(object_id);
    vfs.local_payload_backend()
        .sync_data()
        .expect("sync orphan warm payload");
    vfs.cached_store
        .link_upload_stage_batch(&[object_id])
        .expect("seed stale upload-stage hardlink");
    vfs.state.upload.pending_evictions.lock().insert(object_id);

    assert!(
        vfs.local_payload_backend().contains(object_id),
        "test setup should expose orphan warm payload before replay"
    );
    assert!(
        !vfs.local_payload_backend()
            .dirty_object_ids()
            .contains(&object_id),
        "warm uploaded payload should not be covered by dirty-only orphan cleanup"
    );

    vfs.replay_dirty_uploads_from_local_backend(&[], &[]);

    assert!(
        !vfs.local_payload_backend().contains(object_id),
        "startup replay should remove unreferenced warm payload cache entries"
    );
    assert!(
        !vfs.state
            .upload
            .pending_evictions
            .lock()
            .contains(&object_id),
        "startup replay should clear stale pending eviction bookkeeping"
    );
    assert_eq!(
        vfs.state
            .upload
            .upload_canceled_stale
            .load(Ordering::Relaxed),
        1,
        "startup replay should attribute warm orphan removal to stale-cache cleanup"
    );
    let summary = vfs
        .zeroio_runtime_snapshot()
        .startup_replay
        .expect("startup replay summary should be visible");
    assert_eq!(summary.unreferenced_local_payloads_removed, 1);
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
        vfs.flush(pinned.ino)
            .await
            .expect("materialize pinned unpublished payload");

        let publish_ready = vfs.create(1, b"publish-ready.bin", 0, 0, 0o644).unwrap();
        vfs.write(publish_ready.ino, 0, b"publish-ready-payload")
            .await
            .unwrap();
        vfs.flush(publish_ready.ino)
            .await
            .expect("materialize publish-ready payload");

        let published = vfs.create(1, b"published.bin", 0, 0, 0o644).unwrap();
        vfs.write(published.ino, 0, b"published-warm-payload")
            .await
            .unwrap();
        vfs.flush(published.ino)
            .await
            .expect("materialize published warm payload");

        let (publish_ready_epoch, publish_ready_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, publish_ready.ino);
        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, published.ino);
        publish_current_file_graph(&meta, published.ino, published_epoch, &published_blocks);

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
            "test setup should persist current unpublished slice payloads"
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
    vfs.local_payload_backend()
        .put(950, b"local-only", false)
        .expect("persist local-only object");
    vfs.local_payload_backend()
        .put(951, b"uploaded", true)
        .expect("persist uploaded object");
    vfs.local_payload_backend().mark_uploaded(951);
    meta.stage_payload_slice(attr.ino, 0, &PayloadRef::new_partial(950, 4096))
        .expect("persist local-only block");
    let uploaded = PayloadRef::new_partial(951, 4096);
    meta.stage_payload_slice(attr.ino, 1, &uploaded)
        .expect("persist uploaded payload object");
    assert!(
        meta.mark_regular_file_overlay_object_uploaded_if_matches(attr.ino, 1, uploaded.object_id)
            .expect("mark uploaded object"),
        "expected uploaded object transition to apply"
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
fn epoch_finalizer_publishes_slice_graph_zero_length_epoch() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-zero-length");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"slice-graph-finalizer-zero-length.bin", 0, 0, 0o644)
            .expect("create test file");
        let payload = b"slice-graph-finalization";
        vfs.write(attr.ino, 0, payload)
            .await
            .expect("write test payload");
        vfs.flush(attr.ino)
            .await
            .expect("materialize unpublished slice payloads before publish setup");

        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        publish_current_file_graph(&meta, attr.ino, published_epoch, &published_blocks);

        let mut truncated_attr = meta.get_attr(attr.ino).expect("load published attr");
        truncated_attr.size = 0;
        truncated_attr.content_epoch = truncated_attr.content_epoch.saturating_add(1);
        truncated_attr.upload_state = MetaUploadState::Updating;
        meta.set_attr(&truncated_attr)
            .expect("stage slice-graph zero-length epoch");
        meta.remove_regular_file_graph(attr.ino)
            .expect("remove unpublished slice graph for zero-length truncate");

        assert!(
            !meta
                .has_local_regular_file_overlay(attr.ino)
                .expect("query overlay state"),
            "regression setup should exercise a payload-free slice-graph epoch"
        );

        let unpublished_attr = meta.get_attr(attr.ino).expect("load unpublished attr");
        let lifecycle = meta
            .get_inode_lifecycle(attr.ino)
            .expect("load lifecycle")
            .expect("slice-graph epoch should keep lifecycle state");
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
        assert_eq!(
            lifecycle.state,
            InodeLifecycleState::PublishReady,
            "payload-free slice-graph zero-length epochs should become publish-ready immediately"
        );
        assert_eq!(
            meta.lookup_payload_slices(attr.ino)
                .expect("load unpublished graph")
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
        .expect("finalize zero-length slice-graph epoch");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Published),
            "slice-graph zero-length epoch should still publish"
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
fn size_change_after_unpublished_flush_advances_content_epoch() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "setattr-size-advances-epoch");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"setattr-size-advances-epoch.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let payload = b"unpublished payload";

        vfs.write(ino, 0, payload)
            .await
            .expect("write test payload");
        vfs.flush(ino).await.expect("flush first unpublished epoch");

        let flushed_attr = meta.get_attr(ino).expect("load flushed attr");
        let old_epoch = flushed_attr.content_epoch;
        let old_objects = meta
            .get_regular_file_overlay_object_ids(ino)
            .expect("load old overlay object ids");
        assert!(
            !old_objects.is_empty(),
            "setup should leave unpublished upload work in the old epoch"
        );

        vfs.setattr(ino, SetattrArgs::size_only(0))
            .expect("truncate flushed unpublished file to zero");

        let truncated_attr = meta.get_attr(ino).expect("load truncated attr");
        assert_eq!(truncated_attr.size, 0);
        assert!(
            truncated_attr.content_epoch > old_epoch,
            "truncate-to-zero must stale queued upload/finalize work from the prior epoch"
        );
        assert!(
            !meta
                .mark_regular_file_overlay_object_uploaded_if_matches(
                    ino,
                    old_epoch,
                    old_objects[0],
                )
                .expect("stale upload completion should be ignored"),
            "old upload completion must not match the newer size-changing epoch"
        );
        assert!(
            meta.lookup_payload_slices(ino)
                .expect("load truncated payload slices")
                .is_empty(),
            "truncate-to-zero should leave no visible payload slices"
        );
    });
}

#[test]
fn nonzero_truncate_requeues_retained_overlay_uploads_with_new_epoch() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "truncate-requeues-retained");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"truncate-requeues-retained.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let payload = vec![0x5Au8; 8192];

        vfs.write(ino, 0, &payload).await.expect("write payload");
        vfs.flush(ino).await.expect("flush first unpublished epoch");

        let flushed_attr = meta.get_attr(ino).expect("load flushed attr");
        let old_epoch = flushed_attr.content_epoch;
        let old_objects = meta
            .get_regular_file_overlay_object_ids(ino)
            .expect("load old overlay object ids");
        assert_eq!(
            vfs.pending_uploads_for_inode(ino)
                .iter()
                .map(|(_, object_id, content_epoch)| (*object_id, *content_epoch))
                .collect::<Vec<_>>(),
            old_objects
                .iter()
                .map(|object_id| (*object_id, old_epoch))
                .collect::<Vec<_>>(),
            "setup should queue uploads for the original unpublished epoch"
        );

        vfs.setattr(ino, SetattrArgs::size_only(4096))
            .expect("truncate while retaining the first overlay payload");

        let truncated_attr = meta.get_attr(ino).expect("load truncated attr");
        assert!(truncated_attr.content_epoch > old_epoch);
        let pending = vfs.pending_uploads_for_inode(ino);
        assert!(
            !pending.is_empty(),
            "retained overlay payloads must remain queued after the epoch advances"
        );
        assert!(
            pending
                .iter()
                .all(|(_, _, content_epoch)| *content_epoch == truncated_attr.content_epoch),
            "retained payload uploads must be queued under the new content epoch"
        );
    });
}

#[test]
fn epoch_finalizer_defers_while_handle_writer_tail_is_still_buffered() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-buffered-tail");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"graph-finalizer-buffered-tail.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writable handle");
        let block_size = vfs.block_size as usize;
        let full_block = vec![0x41u8; block_size];
        let tail = vec![0x42u8; block_size / 8];

        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            0,
            &full_block,
            LocalSyncPolicy::Opportunistic,
        )
            .await
            .expect("stage the initial full-block epoch");
        vfs.flush_fh(fh)
            .await
            .expect("flush the initial full block into durable unpublished metadata");

        assert_eq!(
            meta.lookup_payload_slices(ino)
                .expect("load flushed payload slices")
                .len(),
            1,
            "setup should create a publishable durable prefix before buffering a newer tail"
        );

        let (content_epoch, _blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);

        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            block_size as u64,
            &tail,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .expect("buffer a newer partial tail without flushing it");

        assert_eq!(
            meta.lookup_payload_slices(ino)
                .expect("load durable payload slices")
                .len(),
            1,
            "the newer tail should remain buffered and must not rewrite the durable prefix yet"
        );
        let unpublished_attr = meta.get_attr(ino).expect("load unpublished attr");
        assert_eq!(
            unpublished_attr.size,
            block_size as u64,
            "durable metadata should still reflect only the older flushed epoch while the newer tail is buffered"
        );
        let dirty_attr = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .expect("dirty attr should preserve the buffered tail");
        assert_eq!(
            dirty_attr.size,
            (block_size + tail.len()) as u64,
            "dirty attr should keep the full visible EOF while the tail is still buffered"
        );
        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "setup should retain the partial tail in writer-stack memory"
        );
        assert!(
            !vfs.has_pending_local_dirty_payloads_for_inode(ino),
            "setup should isolate the regression to a buffered handle-writer tail, not staged local_dirty payloads"
        );

        let status =
            super::commit::CommitCoordinator::finalize_inode_epoch(&vfs, ino, content_epoch)
                .await
                .expect("finalize should defer while handle-writer tail is buffered");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Deferred),
            "finalize must not publish or clear dirty attrs while a newer visible tail is still buffered"
        );

        let attr_after = meta.get_attr(ino).expect("load attr after deferred finalize");
        assert_ne!(
            attr_after.upload_state,
            MetaUploadState::Ready,
            "deferred finalize should leave the unpublished epoch unpublished"
        );
        let dirty_attr_after = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .expect("dirty attr should survive deferred finalize");
        assert_eq!(
            dirty_attr_after.size,
            (block_size + tail.len()) as u64,
            "deferred finalize must preserve the larger visible EOF staged in dirty attrs"
        );

        vfs.release_fh(fh).await.expect("release handle");
    });
}

#[test]
fn epoch_finalizer_guard_observes_writer_stack_tail() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-writer-stack-tail");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"graph-finalizer-writer-stack-tail.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, OpenFlags::RDWR, false)
            .expect("open writable handle");
        let payload = b"writer-stack-only-tail";

        vfs.write_fh_with_local_sync_policy(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .expect("buffer writer-stack tail");

        assert!(vfs.state.writer_stack.has_dirty_writers(ino));
        assert!(!vfs.has_pending_local_dirty_payloads_for_inode(ino));
        assert!(!vfs.inode_has_dirty_handle_writers(ino));
        assert!(
            vfs.inode_has_unmaterialized_overlay_for_finalize(ino),
            "epoch finalization must defer while a mounted write is only buffered in the writer stack"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn writer_stack_object_reservation_counts_only_current_write() {
    let tmp = tempfile::tempdir().unwrap();
    let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "writer-stack-object-reservation");
    let block_size = vfs.block_size as usize;
    let expected_dirty_limit_objects = vfs.max_dirty_bytes().div_ceil(block_size).saturating_add(1);

    assert!(
        expected_dirty_limit_objects > 1,
        "setup should distinguish exact write reservation from dirty-limit reservation"
    );
    assert_eq!(vfs.writer_stack_object_upper_bound(0, 1).unwrap(), 1);
    assert_eq!(
        vfs.writer_stack_object_upper_bound(0, block_size + 1)
            .unwrap(),
        2
    );
}

#[test]
fn epoch_finalizer_defers_while_deferred_materialization_is_inflight() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "graph-finalizer-inflight");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"graph-finalizer-inflight.bin", 0, 0, 0o644)
            .expect("create test file");
        let payload = b"slice-graph-finalization";
        vfs.write(attr.ino, 0, payload)
            .await
            .expect("write test payload");
        vfs.flush(attr.ino)
            .await
            .expect("materialize unpublished slice payloads before publish setup");

        let (published_epoch, published_blocks) =
            mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        publish_current_file_graph(&meta, attr.ino, published_epoch, &published_blocks);

        let mut truncated_attr = meta.get_attr(attr.ino).expect("load published attr");
        truncated_attr.size = 0;
        truncated_attr.content_epoch = truncated_attr.content_epoch.saturating_add(1);
        truncated_attr.upload_state = MetaUploadState::Updating;
        meta.set_attr(&truncated_attr)
            .expect("stage slice-graph zero-length epoch");
        meta.remove_regular_file_graph(attr.ino)
            .expect("remove unpublished slice graph for zero-length truncate");

        let unpublished_attr = meta.get_attr(attr.ino).expect("load unpublished attr");
        let lifecycle = meta
            .get_inode_lifecycle(attr.ino)
            .expect("load lifecycle")
            .expect("zero-length epoch should keep lifecycle state");
        assert!(
            !vfs.inode_has_unmaterialized_overlay_for_finalize(attr.ino),
            "setup should otherwise be ready to finalize once uploads are visible"
        );
        assert_eq!(
            lifecycle.state,
            InodeLifecycleState::PublishReady,
            "once zero-length slice-graph state is durable, only publication should remain"
        );

        let task_lock = Arc::new(tokio::sync::Mutex::new(()));
        vfs.state
            .deferred_materialize_inflight
            .lock()
            .insert(attr.ino, Arc::clone(&task_lock));

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            attr.ino,
            unpublished_attr.content_epoch,
        )
        .await
        .expect("finalize should defer while deferred materialization is in flight");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Deferred),
            "finalize must not race ahead of an in-flight deferred materialization task"
        );

        vfs.state
            .deferred_materialize_inflight
            .lock()
            .remove(&attr.ino);

        let status = super::commit::CommitCoordinator::finalize_inode_epoch(
            &vfs,
            attr.ino,
            unpublished_attr.content_epoch,
        )
        .await
        .expect("finalize should publish once deferred materialization completes");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Published),
            "once the in-flight task clears, finalize should publish the prepared epoch"
        );
    });
}

#[test]
fn graph_finalizer_visibility_timeout_does_not_requeue_uploaded_blocks() {
    let tmp = TempDir::new().expect("tempdir");
    let cfg = VfsConfig {
        publish_visibility_timeout_secs: 1,
        ..VfsConfig::default()
    };
    let (rt, vfs, meta) =
        make_unstarted_test_vfs_with_config(&tmp, "graph-finalizer-no-requeue", cfg);
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"graph-finalizer-no-requeue.bin", 0, 0, 0o644)
            .expect("create test file");
        let payload = vec![0x5Au8; vfs.block_size as usize];
        vfs.write(attr.ino, 0, &payload)
            .await
            .expect("write test payload");
        vfs.flush(attr.ino)
            .await
            .expect("flush slice-graph epoch");

        let (content_epoch, _blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, attr.ino);
        for pending in vfs.pending_uploads_snapshot() {
            if pending.ino == attr.ino && pending.content_epoch == content_epoch {
                vfs.cached_store().complete_upload(&pending);
            }
        }
        assert!(
            vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "setup should simulate uploaded payload objects that are no longer pending locally"
        );

        let status =
            super::commit::CommitCoordinator::finalize_inode_epoch(&vfs, attr.ino, content_epoch)
                .await
                .expect("visibility timeout should defer instead of failing");
        assert!(
            matches!(status, super::commit::EpochFinalizeStatus::Deferred),
            "visibility lag should defer publish instead of requeueing uploaded payload objects"
        );
        assert!(
            vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "uploaded payload objects awaiting remote visibility should not be requeued into the upload backlog"
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
        vfs.flush(ino)
            .await
            .expect("materialize replay health payload");
    });
    let attr = meta.get_attr(ino).expect("load replay attr");
    let block = meta
        .lookup_payload_slices(ino)
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
        VfsError::LocalPayloadRuntimeDetection { cache_root, source } => {
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
fn cache_runtime_tuning_defaults_match_juicefs_sized_windows() {
    let derived =
        derive_cache_runtime_tuning(100 * 1024 * 1024 * 1024, 16 * 1024 * 1024, 64 * 1024 * 1024);

    assert_eq!(derived.max_dirty_bytes, DEFAULT_MAX_DIRTY_BYTES);
    assert_eq!(derived.transit_memory_budget_bytes, 300 * 1024 * 1024);
}

#[test]
fn soft_dirty_budget_pacing_starts_before_hard_limit() {
    assert_eq!(soft_dirty_budget_pacing_delay_ms(0, 100, 0), None);
    assert_eq!(soft_dirty_budget_pacing_delay_ms(44, 100, 0), None);
    assert_eq!(soft_dirty_budget_pacing_delay_ms(45, 100, 0), Some(1));
    assert_eq!(soft_dirty_budget_pacing_delay_ms(60, 100, 0), Some(3));
    assert_eq!(soft_dirty_budget_pacing_delay_ms(75, 100, 0), Some(6));
    assert_eq!(soft_dirty_budget_pacing_delay_ms(85, 100, 0), Some(10));
    assert_eq!(soft_dirty_budget_pacing_delay_ms(0, 100, 256), Some(1));
    assert_eq!(soft_dirty_budget_pacing_delay_ms(0, 100, 512), Some(3));
    assert_eq!(soft_dirty_budget_pacing_delay_ms(0, 100, 1024), Some(6));
    assert_eq!(soft_dirty_budget_pacing_delay_ms(62, 100, 1024), Some(6));
}

#[test]
fn default_readahead_tracks_the_juicefs_sized_buffer_budget() {
    let buffer_blocks = derive_buffer_blocks_default(DEFAULT_READ_BUFFER_MB, 16 * 1024 * 1024);
    let readahead_max_width = derive_readahead_max_width(
        DEFAULT_READ_BUFFER_MB,
        16 * 1024 * 1024,
        DEFAULT_READAHEAD_WIDTH,
    );
    let fetch_aimd = build_fetch_aimd_config(readahead_max_width, &VfsConfig::default());

    assert_eq!(buffer_blocks, 18);
    assert_eq!(readahead_max_width, DEFAULT_READAHEAD_WIDTH);
    assert_eq!(fetch_aimd.max_window, 18.0);
    assert_eq!(fetch_aimd.initial_window, 4.5);
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

    assert_eq!(derived.max_dirty_bytes, DEFAULT_MAX_DIRTY_BYTES);
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
    assert_eq!(validated.upload_worker_count, 20);
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
                AccessTrackerConfig {
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

#[tokio::test]
async fn foreground_idle_notify_fires_when_last_read_finishes() {
    let state = ReadState::new(1);
    state.mark_foreground_read(100);
    let notified = state.foreground_idle_notify.notified();
    state.unmark_foreground_read(120);
    tokio::time::timeout(Duration::from_millis(20), notified)
        .await
        .expect("dropping the last read should notify idle waiters");
}

#[test]
fn wait_for_foreground_reads_idle_honors_idle_grace() {
    let tmp = TempDir::new().expect("tempdir");
    let cfg = VfsConfig {
        upload_user_idle_grace_ms: 20,
        ..VfsConfig::default()
    };
    let (rt, vfs, _meta) = make_test_vfs_with_config(&tmp, "foreground-read-idle-wait", cfg);
    let _guard = rt.enter();

    rt.block_on(async {
        vfs.state.read.mark_foreground_read(now_unix_millis());
        let waiter = {
            let vfs = vfs.clone();
            tokio::spawn(async move {
                vfs.wait_for_foreground_reads_idle(Duration::from_millis(80))
                    .await
            })
        };

        tokio::time::sleep(Duration::from_millis(5)).await;
        vfs.state.read.unmark_foreground_read(now_unix_millis());
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(
            !waiter.is_finished(),
            "helper should keep upload rereads paused until the idle grace expires"
        );

        assert!(waiter.await.expect("join foreground idle waiter"));
    });
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
                AccessTrackerConfig {
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
        let policy = AccessPolicy::from_class(AccessClass::SequentialMediaRead, vfs.access_tracker)
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
        let block = vfs.access_tracker.block_size as usize;

        let mut last = AccessClass::RandomInteractiveRead;
        for step in 0..4u64 {
            last = vfs
                .state
                .read
                .observe_read_access(None, ino, step * block as u64, block, vfs.access_tracker)
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
        let block = vfs.access_tracker.block_size as usize;

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
                    vfs.access_tracker,
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
        let block = vfs.access_tracker.block_size as usize;

        let _ = vfs
            .state
            .read
            .observe_read_access(None, ino, 0, block, vfs.access_tracker);
        let _ = vfs
            .state
            .read
            .observe_write_access(None, ino, 0, block, vfs.access_tracker);
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
        // flush_fh guarantees local durability but ignores background upload
        // errors — the data is safe in NVMe and uploads will be retried.
        vfs.flush_fh(fh).await.unwrap();
        // A second flush also succeeds (sticky error is harmless).
        vfs.flush_fh(fh).await.unwrap();
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
fn test_release_fh_with_outcome_materializes_pending_inode_dirty_state_on_last_close() {
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
            vfs.inode_has_local_dirty_state(ino),
            "partial inode writes should remain pending until an explicit barrier or last close"
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
            second.flushed_local_dirty_state,
            "the final close should report that it materialized the pending inode dirty state"
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
fn ignored_entry_writes_preserve_synthetic_attr_size() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "ignored-entry-write-size");
    let _guard = rt.enter();
    rt.block_on(async {
        let inode_attr = vfs.create(1, b".DS_Store", 0, 0, 0o644).unwrap();
        let inode_payload = b"synthetic-inode";
        vfs.write_with_local_sync_policy(
            inode_attr.ino,
            0,
            inode_payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        let inode_after = vfs.getattr(inode_attr.ino).unwrap();
        assert_eq!(
            inode_after.size,
            inode_payload.len() as u64,
            "successful ignored-entry inode writes must still advance the synthetic attr size"
        );
        let inode_read = vfs
            .read(inode_attr.ino, 0, inode_payload.len() as u32)
            .await
            .unwrap();
        assert_eq!(
            inode_read.len(),
            inode_payload.len(),
            "ignored-entry reads should observe the updated synthetic file length"
        );

        let handle_attr = vfs.create(1, b"._synthetic", 0, 0, 0o644).unwrap();
        let handle_payload = b"synthetic-handle";
        let fh = vfs
            .open_file(handle_attr.ino, OpenFlags::RDWR, false)
            .unwrap();
        vfs.write_fh_with_local_sync_policy(
            fh,
            handle_attr.ino,
            0,
            handle_payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        let handle_after = vfs.getattr(handle_attr.ino).unwrap();
        assert_eq!(
            handle_after.size,
            handle_payload.len() as u64,
            "successful ignored-entry handle writes must still advance the synthetic attr size"
        );
        let handle_read = vfs
            .read(handle_attr.ino, 0, handle_payload.len() as u32)
            .await
            .unwrap();
        assert_eq!(
            handle_read.len(),
            handle_payload.len(),
            "ignored-entry handle writes should remain readable at the new synthetic length"
        );
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn same_mount_read_keeps_opportunistic_handle_write_visible_after_local_dirty_clear() {
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
            overlay_spans.iter().all(|span| span.payload.is_none()),
            "opportunistic handle writes should not materialize block-backed metadata before a barrier: {overlay_spans:?}"
        );

        vfs.clear_pending_payloads_for_inode(ino);

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

        vfs.clear_pending_payloads_for_inode(ino);

        let bytes = vfs.read(ino, 0, 5).await.unwrap();
        assert_eq!(bytes.as_ref(), b"hXYZo");

        vfs.release_fh(fh1).await.unwrap();
        vfs.release_fh(fh2).await.unwrap();
    });
}

#[test]
fn handle_writer_global_dirty_cap_drains_older_inodes_after_limit_drop() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-245-handle-writer-global-cap");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr1 = vfs.create(1, b"cap-a.txt", 0, 0, 0o644).unwrap();
        let attr2 = vfs.create(1, b"cap-b.txt", 0, 0, 0o644).unwrap();
        let attr3 = vfs.create(1, b"cap-c.txt", 0, 0, 0o644).unwrap();
        let fh1 = vfs.open_file(attr1.ino, OpenFlags::RDWR, false).unwrap();
        let fh2 = vfs.open_file(attr2.ino, OpenFlags::RDWR, false).unwrap();
        let fh3 = vfs.open_file(attr3.ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh_with_local_sync_policy(
            fh1,
            attr1.ino,
            0,
            b"12345678",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        vfs.write_fh_with_local_sync_policy(
            fh2,
            attr2.ino,
            0,
            b"abcdefgh",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();
        assert_eq!(vfs.state.writer_stack.total_dirty_bytes(), 16);

        vfs.set_max_dirty_bytes(8);

        vfs.write_fh_with_local_sync_policy(
            fh3,
            attr3.ino,
            0,
            b"z",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        assert!(
            vfs.state.writer_stack.total_dirty_bytes() <= vfs.max_dirty_bytes(),
            "once the runtime dirty limit drops, the next opportunistic write should drain enough writer-stack state to get back under the global cap"
        );

        vfs.release_fh(fh1).await.unwrap();
        vfs.release_fh(fh2).await.unwrap();
        vfs.release_fh(fh3).await.unwrap();
    });
}

#[test]
fn soft_dirty_budget_wait_observes_inflight_materialization() {
    let tmp = TempDir::new().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-257-soft-dirty-budget-wait");
    let _guard = rt.enter();

    rt.block_on(async {
        let ino = 42;
        let fh = 7;
        vfs.set_max_dirty_bytes(1024);
        vfs.state.handle_writers.open_writer_for_test(fh, ino, 0);
        let payload = vec![0xAB; 2048];
        vfs.state
            .handle_writers
            .write(fh, ino, 0, vfs.block_size, 0, &payload);

        let task_lock = Arc::new(tokio::sync::Mutex::new(()));
        let task_guard = task_lock.clone().lock_owned().await;
        vfs.state
            .deferred_materialize_inflight
            .lock()
            .insert(ino, task_lock.clone());

        let waiter_vfs = vfs.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            waiter_vfs.state.handle_writers.clear_inode(ino);
            waiter_vfs
                .state
                .deferred_materialize_inflight
                .lock()
                .remove(&ino);
            drop(task_guard);
        });

        let started = Instant::now();
        vfs.wait_for_soft_dirty_budget_relief(ino).await;
        assert!(
            started.elapsed() >= Duration::from_millis(15),
            "soft dirty-budget wait should block until the inflight drain completes"
        );
        assert!(
            vfs.state.handle_writers.total_dirty_bytes() <= vfs.max_dirty_bytes(),
            "soft dirty-budget wait should return only after dirty bytes are back under budget"
        );
    });
}

#[test]
fn aggressive_materialization_threshold_is_half_of_dirty_budget() {
    assert_eq!(aggressive_materialization_threshold_bytes(0), 1);
    assert_eq!(aggressive_materialization_threshold_bytes(1), 1);
    assert_eq!(aggressive_materialization_threshold_bytes(8), 4);
    assert_eq!(hard_backpressure_threshold_bytes(8), 16);
}

#[test]
fn ingress_pressure_counts_buffered_handle_writer_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-257-ingress-pressure-handle-writers");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-ingress-pressure-handle-writers.txt", 0, 0, 0o644)
            .unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"buffer-me";

        vfs.set_max_dirty_bytes(payload.len());
        assert_eq!(
            vfs.ingress_pressure_for(payload.len()).lane,
            VfsIngressLane::DirtyMem,
            "clean ingress state should accept the first buffered handle write"
        );

        vfs.write_fh_with_local_sync_policy(
            fh,
            attr.ino,
            0,
            payload,
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        assert!(
            vfs.state.writer_stack.has_dirty_writers(attr.ino),
            "setup should keep the payload buffered in the writer stack"
        );
        let pressure = vfs.ingress_pressure_for(1);
        assert_eq!(
            pressure.dirty_mem_bytes,
            payload.len(),
            "ingress pressure should include buffered handle-writer bytes before they materialize into local_dirty"
        );
        assert_eq!(
            pressure.lane,
            VfsIngressLane::Backpressure,
            "once buffered handle-writer bytes consume the dirty budget, follow-up ingress should see backpressure"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn inode_write_keeps_partial_state_buffered_until_flush() {
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
            vfs.has_pending_local_dirty_payloads_for_inode(ino),
            "ordinary opportunistic inode writes should keep partial state buffered until the flush barrier"
        );
        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().all(|span| span.payload.is_none()),
            "ordinary opportunistic inode writes should not materialize block-backed unpublished slices before flush: {spans:?}"
        );

        let bytes = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(bytes.as_ref(), payload);

        vfs.flush(ino).await.unwrap();

        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().any(|span| span.payload.is_some()),
            "flush should materialize block-backed unpublished slices for the buffered inode write: {spans:?}"
        );
    });
}

#[test]
fn write_fh_default_buffers_partial_state_until_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-257-write-fh-default-buffered");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-write-fh-default-buffered.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"buffer-me";

        vfs.write_fh(fh, ino, 0, payload).await.unwrap();

        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "default mounted handle writes should stay in writer-stack state until an explicit barrier"
        );
        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().all(|span| span.payload.is_none()),
            "default buffered handle writes should not materialize block-backed unpublished slices before flush: {spans:?}"
        );

        vfs.flush_fh(fh).await.unwrap();

        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().any(|span| span.payload.is_some()),
            "flush_fh should materialize block-backed unpublished slices for the buffered handle write: {spans:?}"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_fh_buffers_partial_tail_after_full_block_boundary_until_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-257-write-fh-full-block-plus-tail");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-write-fh-full-block-plus-tail.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let mut payload = vec![0x41u8; block_size];
        payload.extend_from_slice(&vec![0x42u8; block_size / 8]);

        vfs.write_fh(fh, ino, 0, &payload).await.unwrap();

        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            0,
            "stage-first opportunistic writes should keep the completed full block in local staging until a background or explicit flush commits metadata"
        );
        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "the partial tail should remain in the writer stack after the full-block prefix is accepted"
        );
        let visible_attr = vfs.getattr_unchecked(ino).unwrap();
        assert_eq!(
            visible_attr.size,
            payload.len() as u64,
            "read-visible inode size should advance immediately even when the full-block prefix is only staged locally"
        );
        let buffered = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(buffered.as_ref(), payload.as_slice());

        vfs.flush_fh(fh).await.unwrap();

        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            2,
            "flush should materialize the buffered tail block"
        );
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_fh_routes_mounted_write_through_writer_stack() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-292-write-fh-writer-stack");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-292-write-fh-writer-stack.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"mounted-writer-stack";

        vfs.write_fh(fh, ino, 0, payload).await.unwrap();

        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "mounted write callbacks should land in the new writer stack"
        );
        assert!(
            !vfs.state.handle_writers.has_dirty_writers(ino),
            "mounted write callbacks should no longer use the legacy handle writer registry"
        );
        let read = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(read.as_ref(), payload);

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_fh_does_not_allocate_object_ids_before_materialization() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-292-write-fh-no-object-ids");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-292-write-fh-no-object-ids.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let before = meta.next_object_ids(1).unwrap()[0];

        vfs.write_fh(fh, ino, 0, b"buffered").await.unwrap();

        let after = meta.next_object_ids(1).unwrap()[0];
        assert_eq!(
            after,
            before + 1,
            "foreground write_fh should not allocate payload object ids before materialization"
        );

        vfs.flush_fh(fh).await.unwrap();
        let spans = meta.lookup_payload_slices(ino).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].1.object_id, before + 2);

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_fh_full_block_callbacks_persist_in_small_foreground_waves() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-257-write-fh-multi-block-burst");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-write-fh-multi-block-burst.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;

        let first = vec![0x11; block_size];
        let second = vec![0x22; block_size];
        let third = vec![0x33; block_size];

        vfs.write_fh(fh, ino, 0, &first).await.unwrap();
        vfs.write_fh(fh, ino, block_size as u64, &second)
            .await
            .unwrap();
        vfs.write_fh(fh, ino, (block_size * 2) as u64, &third)
            .await
            .unwrap();

        let read = vfs.read(ino, 0, (block_size * 3) as u32).await.unwrap();
        assert_eq!(&read[..block_size], first.as_slice());
        assert_eq!(&read[block_size..block_size * 2], second.as_slice());
        assert_eq!(&read[block_size * 2..], third.as_slice());

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });

    let snapshot = vfs.write_path_observability_snapshot();
    assert!(
        snapshot.payload_burst.max_payloads_per_burst <= 1,
        "slice-first foreground persistence should avoid multi-payload bursts for full-block callbacks: {:?}",
        snapshot.payload_burst
    );
}

#[test]
fn opportunistic_write_fh_lands_before_deferred_materialization() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-284-landing-zone");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-284-landing-zone.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"landing-zone-visible";

        vfs.write_fh_with_deferred_materialize(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();

        let read = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(read.as_ref(), payload);

        let snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(snapshot.landing_zone.accepted_writes, 1);
        assert_eq!(snapshot.landing_zone.accepted_bytes, payload.len() as u64);
        assert_eq!(
            snapshot.landing_zone.data_syncs, 0,
            "opportunistic writeback accept must not issue a foreground landing data sync"
        );
        assert_eq!(
            snapshot.landing_zone.manifest_syncs, 0,
            "opportunistic writeback accept must not issue a foreground landing manifest sync"
        );
        assert!(
            snapshot.landing_zone.accept_avg_ms >= 0.0,
            "landing accept latency should be exported for AJA evidence"
        );

        let recovered = vfs.recover_landing_zone_segments().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].ino, ino);
        assert_eq!(recovered[0].offset, 0);
        assert_eq!(recovered[0].len, payload.len() as u64);

        vfs.flush_fh(fh).await.unwrap();
        assert!(
            vfs.recover_landing_zone_segments().unwrap().is_empty(),
            "strict local drain should retire landed records for this inode"
        );
        let drained_snapshot = vfs.write_path_observability_snapshot();
        assert!(
            drained_snapshot.landing_zone.data_syncs > 0,
            "strict local boundary should group landing data sync with landed-record retirement"
        );
        assert!(
            drained_snapshot.landing_zone.manifest_syncs > 0,
            "strict local boundary should group landing manifest sync with landed-record retirement"
        );
        assert_eq!(drained_snapshot.landing_zone.manifest_rewrites, 1);
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn fsync_retires_landed_write_records_at_strict_local_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-285-landing-fsync");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-285-landing-fsync.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"landed-fsync-boundary";

        vfs.write_fh_with_deferred_materialize(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.recover_landing_zone_segments().unwrap().len(),
            1,
            "opportunistic write should create a recoverable landed record before fsync"
        );

        vfs.fsync_with_outcome(ino, false).await.unwrap();
        assert!(
            vfs.recover_landing_zone_segments().unwrap().is_empty(),
            "deferred materialization or fsync should retire landed records for this inode"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn last_handle_release_retires_landed_write_records_at_strict_local_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-285-landing-release");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-285-landing-release.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = b"landed-release-boundary";

        vfs.write_fh_with_deferred_materialize(fh, ino, 0, payload, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.recover_landing_zone_segments().unwrap().len(),
            1,
            "opportunistic write should create a recoverable landed record before last close"
        );

        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
        assert!(outcome.closed_last_handle);
        assert!(
            vfs.recover_landing_zone_segments().unwrap().is_empty(),
            "deferred materialization or last-handle release should retire landed records for this inode"
        );
    });
}

#[test]
fn startup_replays_landed_write_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let volume_suffix = "sfs-284-landing-restart";
    let payload = b"restart-visible-landed";

    let ino = {
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, volume_suffix);
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"sfs-284-landing-restart.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh_with_deferred_materialize(
                fh,
                ino,
                0,
                payload,
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .unwrap();
            ino
        })
    };

    let db_path = tmp.path().join("meta.redb");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("reopen RedbMetaEngine"));
    let format_info = meta.get_format_info().expect("get format info");
    let data_config = DataStoreConfig::from(&format_info);
    let cache_dir = tmp.path().join("cache");
    let rt = Arc::new(Runtime::new().expect("runtime"));
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
    .expect("reopen vfs");
    vfs.start().expect("start recovery workers");

    rt.block_on(async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let read = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
            if read.as_ref() == payload {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "landing-zone startup recovery did not make accepted bytes visible"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            vfs.recover_landing_zone_segments().unwrap().is_empty(),
            "startup recovery should retire replayed landed records"
        );
    });
}

#[test]
fn opportunistic_completed_handle_writer_materialization_caps_burst_to_drain_chunk() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = VfsConfig {
        write_eager_flush_batch_bytes: 16 * 1024 * 1024,
        ..VfsConfig::default()
    };
    let (rt, vfs, meta) =
        make_unstarted_test_vfs_with_config(&tmp, "sfs-257-write-fh-capped-burst", cfg);
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-write-fh-capped-burst.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let first = vec![0x11; block_size];
        let second = vec![0x22; block_size];
        let third = vec![0x33; block_size];

        vfs.record_handle_writer_state(fh, ino, 0, &first)
            .await
            .unwrap();
        vfs.record_handle_writer_state(fh, ino, block_size as u64, &second)
            .await
            .unwrap();
        vfs.record_handle_writer_state(fh, ino, (block_size * 2) as u64, &third)
            .await
            .unwrap();

        assert!(
            vfs.materialize_completed_handle_writer_blocks_for_inode(
                ino,
                LocalSyncPolicy::Opportunistic
            )
            .await
            .unwrap(),
            "opportunistic materialization should stage at least one capped burst"
        );
        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            0,
            "completed payload should stay in local staging until a later flush commits metadata"
        );
        assert_eq!(
            vfs.total_pending_local_dirty_bytes(),
            0,
            "completed full logical block should be spilled out of hot RAM once durably staged locally"
        );
        assert!(
            vfs.has_pending_local_dirty_payloads_for_inode(ino),
            "payload-staged slices should remain locally visible until metadata commit catches up"
        );
        assert_eq!(
            vfs.state.handle_writers.dirty_bytes_for_inode(ino),
            block_size,
            "the final completed block should stay buffered for the next opportunistic pass"
        );
        let read = vfs.read(ino, 0, (block_size * 3) as u32).await.unwrap();
        assert_eq!(&read[..block_size], first.as_slice());
        assert_eq!(&read[block_size..block_size * 2], second.as_slice());
        assert_eq!(&read[block_size * 2..], third.as_slice());

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn opportunistic_handle_writer_slice_spill_batches_adjacent_callbacks_into_single_object() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-257-write-fh-slice-batch");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-write-fh-slice-batch.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let slice_bytes = 2 * 1024 * 1024usize;

        let first = vec![0x11; slice_bytes];
        let second = vec![0x22; slice_bytes];
        let third = vec![0x33; slice_bytes];

        vfs.record_handle_writer_state(fh, ino, 0, &first)
            .await
            .unwrap();
        vfs.record_handle_writer_state(fh, ino, slice_bytes as u64, &second)
            .await
            .unwrap();
        vfs.record_handle_writer_state(fh, ino, (slice_bytes * 2) as u64, &third)
            .await
            .unwrap();

        assert!(
            vfs.materialize_handle_writers_for_limits(ino, LocalSyncPolicy::Opportunistic)
                .await
                .unwrap(),
            "three sequential 2 MiB callbacks should spill the first 4 MiB as one staged slice object"
        );

        let dirty_object_ids = vfs.local_payload_backend().dirty_object_ids();
        assert_eq!(
            dirty_object_ids.len(),
            1,
            "adjacent slice spill should batch the first two callbacks into one staged object"
        );
        let staged = vfs
            .local_payload_backend()
            .get(dirty_object_ids[0])
            .expect("staged slice payload should be readable locally");
        assert_eq!(
            staged.len(),
            slice_bytes * 2,
            "the staged slice object should carry a full 4 MiB batch instead of a single callback"
        );
        assert_eq!(
            vfs.state.handle_writers.dirty_bytes_for_inode(ino),
            slice_bytes,
            "the newest callback should remain buffered as the active tail slice"
        );

        let read = vfs.read(ino, 0, (slice_bytes * 3) as u32).await.unwrap();
        assert_eq!(&read[..slice_bytes], first.as_slice());
        assert_eq!(&read[slice_bytes..slice_bytes * 2], second.as_slice());
        assert_eq!(&read[slice_bytes * 2..], third.as_slice());

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn opportunistic_handle_writer_slice_spill_drains_multiple_4mib_batches_per_pass() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-257-write-fh-slice-multi-batch");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-write-fh-slice-multi-batch.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let slice_bytes = 2 * 1024 * 1024usize;

        for index in 0..9u64 {
            let payload = vec![0x40u8.saturating_add(index as u8); slice_bytes];
            vfs.record_handle_writer_state(fh, ino, index * slice_bytes as u64, &payload)
                .await
                .unwrap();
        }

        assert!(
            vfs.materialize_handle_writers_for_limits(ino, LocalSyncPolicy::Opportunistic)
                .await
                .unwrap(),
            "a single opportunistic drain pass should spill multiple 4 MiB batches when enough old callbacks are queued"
        );

        let dirty_object_ids = vfs.local_payload_backend().dirty_object_ids();
        assert_eq!(
            dirty_object_ids.len(),
            4,
            "eight older 2 MiB callbacks should become four staged 4 MiB slice objects in one pass"
        );
        for object_id in dirty_object_ids {
            let staged = vfs
                .local_payload_backend()
                .get(object_id)
                .expect("staged slice payload should be readable locally");
            assert_eq!(staged.len(), 4 * 1024 * 1024);
        }
        assert_eq!(
            vfs.state.handle_writers.dirty_bytes_for_inode(ino),
            slice_bytes,
            "the newest callback should remain buffered as the active tail slice"
        );

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn opportunistic_handle_writer_slice_spill_splits_large_current_callback_into_batched_slices() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) =
        make_unstarted_test_vfs(&tmp, "sfs-257-write-fh-slice-large-current-callback");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(
                1,
                b"sfs-257-write-fh-slice-large-current-callback.txt",
                0,
                0,
                0o644,
            )
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let slice_bytes = 2 * 1024 * 1024usize;
        let payload = vec![0x5Au8; slice_bytes * 5];

        vfs.record_handle_writer_state(fh, ino, 0, &payload)
            .await
            .unwrap();

        assert!(
            vfs.materialize_handle_writers_for_limits(ino, LocalSyncPolicy::Opportunistic)
                .await
                .unwrap(),
            "a single large callback should spill all but the active hot tail into staged 4 MiB slice batches"
        );

        let dirty_object_ids = vfs.local_payload_backend().dirty_object_ids();
        assert_eq!(
            dirty_object_ids.len(),
            2,
            "an opportunistic spill should split a 10 MiB callback into two staged 4 MiB slice objects"
        );
        for object_id in dirty_object_ids {
            let staged = vfs
                .local_payload_backend()
                .get(object_id)
                .expect("staged slice payload should be readable locally");
            assert_eq!(staged.len(), 4 * 1024 * 1024);
        }
        assert_eq!(
            vfs.state.handle_writers.dirty_bytes_for_inode(ino),
            slice_bytes,
            "the newest 2 MiB hot tail should remain buffered after the spill"
        );

        let read = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(read.as_ref(), payload.as_slice());

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn opportunistic_handle_writer_spill_does_not_split_single_write_and_drop_hot_tail() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-282-spill-keeps-hot-tail");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-282-spill-keeps-hot-tail.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let payload = vec![0x6Du8; 3 * 1024 * 1024];

        vfs.record_handle_writer_state(fh, ino, 0, &payload)
            .await
            .unwrap();
        vfs.set_max_dirty_bytes(1);

        assert!(
            vfs.materialize_handle_writers_for_limits(ino, LocalSyncPolicy::Opportunistic)
                .await
                .unwrap(),
            "limit pressure should still make progress without partially spilling a single write"
        );

        let read = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
        assert_eq!(read.as_ref(), payload.as_slice());

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn background_flush_defers_active_persisted_slice_batches_until_commit_window() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-257-background-flush-defers-slices");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-background-flush-defers-slices.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let slice_bytes = 2 * 1024 * 1024usize;

        for index in 0..3u64 {
            let payload = vec![0x70u8.saturating_add(index as u8); slice_bytes];
            vfs.record_handle_writer_state(fh, ino, index * slice_bytes as u64, &payload)
                .await
                .unwrap();
        }

        assert!(
            vfs.materialize_handle_writers_for_limits(ino, LocalSyncPolicy::Opportunistic)
                .await
                .unwrap(),
            "first opportunistic spill should stage the older 4 MiB batch"
        );

        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            4 * 1024 * 1024,
            "the first spilled slice batch should remain locally staged"
        );
        assert!(
            !vfs.should_background_flush_pending_local_dirty_inode(ino),
            "background local-dirty flush should not immediately commit staged slices while the file still has an active dirty tail"
        );

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn background_flush_commits_persisted_slice_waves_and_keeps_latest_hot_tail_staged() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-257-background-flush-slice-waves");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-background-flush-slice-waves.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let slice_bytes = 2 * 1024 * 1024usize;

        for index in 0..17u64 {
            let payload = vec![0x20u8.saturating_add(index as u8); slice_bytes];
            vfs.record_handle_writer_state(fh, ino, index * slice_bytes as u64, &payload)
                .await
                .unwrap();
        }

        assert!(
            vfs.materialize_handle_writers_for_limits(ino, LocalSyncPolicy::Opportunistic)
                .await
                .unwrap(),
            "first opportunistic pass should spill the oldest staged slice wave while retaining a small active hot tail"
        );
        assert!(
            vfs.materialize_handle_writers_for_limits(ino, LocalSyncPolicy::Opportunistic)
                .await
                .unwrap(),
            "second opportunistic pass should spill the next staged slice wave"
        );

        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            32 * 1024 * 1024,
            "two opportunistic spill passes should stage 32 MiB of persisted slice payloads"
        );
        assert_eq!(
            vfs.state.handle_writers.dirty_bytes_for_inode(ino),
            slice_bytes,
            "the newest callback should remain buffered as the active dirty tail"
        );

        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            28 * 1024 * 1024,
            "background flush should commit only one steady oldest slice quota first"
        );

        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            24 * 1024 * 1024,
            "subsequent background passes should continue draining old staged slices without touching the hot tail"
        );

        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            20 * 1024 * 1024,
            "background flush should keep shrinking the staged backlog in small waves"
        );

        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            16 * 1024 * 1024,
            "steady draining should continue one quota at a time while the active tail remains dirty"
        );

        tokio::time::sleep(Duration::from_millis(125)).await;
        assert!(
            vfs.should_background_flush_pending_local_dirty_inode(ino),
            "steady draining should continue while the staged backlog is above one quota"
        );
        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            12 * 1024 * 1024,
            "steady draining should keep committing one quota per pass"
        );

        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            8 * 1024 * 1024,
            "steady draining should not collapse the remaining backlog into a burst"
        );

        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            4 * 1024 * 1024,
            "the newest persisted slice should remain staged as the hot tail"
        );

        tokio::time::sleep(Duration::from_millis(125)).await;
        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            0,
            "an aged-out final staged slice should eventually drain"
        );

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn background_flush_commits_persisted_slices_at_steady_quota() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-286-steady-slice-quota");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-286-steady-slice-quota.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let slice_bytes = 2 * 1024 * 1024usize;

        for index in 0..9u64 {
            let payload = vec![0x40u8.saturating_add(index as u8); slice_bytes];
            vfs.record_handle_writer_state(fh, ino, index * slice_bytes as u64, &payload)
                .await
                .unwrap();
        }

        assert!(
            vfs.materialize_handle_writers_for_limits(ino, LocalSyncPolicy::Opportunistic)
                .await
                .unwrap(),
            "opportunistic pass should stage persisted landed slices"
        );
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            16 * 1024 * 1024
        );

        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            12 * 1024 * 1024,
            "steady draining should commit one 4 MiB quota instead of an 8 MiB burst"
        );

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn aged_slice_catchup_keeps_fresh_staged_tail_protected() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-286-aged-fresh-tail");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-286-aged-fresh-tail.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let slice_bytes = 2 * 1024 * 1024usize;

        let active_tail = vec![0x60u8; slice_bytes];
        vfs.record_handle_writer_state(fh, ino, 4 * 1024 * 1024, &active_tail)
            .await
            .unwrap();
        vfs.install_persisted_pending_local_dirty_slice(
            ino,
            local_dirty::PersistedPendingLocalDirtySliceSpec {
                block_index: 0,
                file_offset: 0,
                len: slice_bytes,
                seq: 900,
                object_id: 900,
                initial_object_offset: 0,
            },
        );
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            slice_bytes
        );

        tokio::time::sleep(Duration::from_millis(125)).await;

        vfs.install_persisted_pending_local_dirty_slice(
            ino,
            local_dirty::PersistedPendingLocalDirtySliceSpec {
                block_index: 0,
                file_offset: slice_bytes as u64,
                len: slice_bytes,
                seq: 901,
                object_id: 901,
                initial_object_offset: 0,
            },
        );
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            2 * slice_bytes
        );

        vfs.flush_local_dirty_state(ino, LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert_eq!(
            vfs.pending_persisted_local_dirty_slice_bytes_for_inode(ino),
            slice_bytes,
            "aged catch-up should commit old staged slices without draining the fresh staged tail"
        );

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn soft_pacing_reacts_to_handle_writer_drain_lag_before_dirty_budget_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-286-lag-soft-pacing");
    let _guard = rt.enter();
    rt.block_on(async {
        vfs.set_max_dirty_bytes(128 * 1024 * 1024);
        let attr = vfs
            .create(1, b"sfs-286-lag-soft-pacing.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let slice_bytes = 2 * 1024 * 1024usize;

        for index in 0..3u64 {
            let payload = vec![0x50u8.saturating_add(index as u8); slice_bytes];
            vfs.record_handle_writer_state(fh, ino, index * slice_bytes as u64, &payload)
                .await
                .unwrap();
        }

        assert_eq!(
            vfs.total_pending_local_dirty_bytes(),
            0,
            "the lag signal should come from landed handle-writer bytes, not old dirty-memory pressure"
        );
        assert!(
            vfs.apply_write_path_soft_pacing(ino).await,
            "soft pressure should begin once landed writes outpace the background drain"
        );

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn drain_lag_pacing_attribution_scales_drain_lag_share_to_actual_wait() {
    assert_eq!(
        super::drain_lag_pacing_attribution_ms(12, 12, Some(4)),
        Some(4)
    );
    assert_eq!(
        super::drain_lag_pacing_attribution_ms(18, 12, Some(4)),
        Some(6)
    );
    assert_eq!(
        super::drain_lag_pacing_attribution_ms(18, 12, Some(12)),
        Some(18)
    );
    assert_eq!(super::drain_lag_pacing_attribution_ms(12, 12, None), None);
}

#[test]
fn write_path_observability_reports_landing_drain_lag_without_foreground_pacing() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-287-drain-lag-observability");
    let _guard = rt.enter();
    rt.block_on(async {
        vfs.set_max_dirty_bytes(128 * 1024 * 1024);
        let attr = vfs
            .create(1, b"sfs-287-drain-lag-observability.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let slice_bytes = 2 * 1024 * 1024usize;

        for index in 0..3u64 {
            let payload = vec![0x60u8.saturating_add(index as u8); slice_bytes];
            vfs.write_fh_with_deferred_materialize(
                fh,
                ino,
                index * slice_bytes as u64,
                &payload,
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .unwrap();
        }

        let snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(snapshot.landing_zone.accepted_writes, 3);
        assert_eq!(
            snapshot.landing_zone.accepted_bytes,
            (3 * slice_bytes) as u64
        );
        assert_eq!(
            snapshot.drain_lag.current_bytes,
            vfs.total_landed_write_drain_lag_bytes(),
            "snapshot should expose the same lag signal used by soft pacing"
        );
        assert!(
            snapshot.drain_lag.current_bytes >= snapshot.drain_lag.steady_quota_bytes,
            "three landed slices should exceed one steady drain quota"
        );
        assert_eq!(
            snapshot.drain_lag.soft_pacing_events, 0,
            "opportunistic writeback must not sleep on foreground drain-lag pacing"
        );
        assert_eq!(
            snapshot.drain_lag.soft_pacing_total_ms, 0,
            "writeback callbacks should remain free of soft pacing wait time"
        );

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn reset_diagnostics_observability_clears_drain_lag_pacing_counters() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-287-reset-drain-lag-counters");
    let _guard = rt.enter();
    rt.block_on(async {
        vfs.set_max_dirty_bytes(128 * 1024 * 1024);
        let attr = vfs
            .create(1, b"sfs-287-reset-drain-lag-counters.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let slice_bytes = 2 * 1024 * 1024usize;

        for index in 0..3u64 {
            let payload = vec![0x70u8.saturating_add(index as u8); slice_bytes];
            vfs.write_fh_with_deferred_materialize(
                fh,
                ino,
                index * slice_bytes as u64,
                &payload,
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .unwrap();
        }

        assert!(
            vfs.apply_write_path_soft_pacing(ino).await,
            "manual soft pacing should still record diagnostics when invoked"
        );

        let before_reset = vfs.write_path_observability_snapshot();
        assert!(before_reset.drain_lag.soft_pacing_events > 0);
        assert!(before_reset.drain_lag.soft_pacing_total_ms > 0);

        vfs.reset_diagnostics_observability();

        let after_reset = vfs.write_path_observability_snapshot();
        assert_eq!(after_reset.drain_lag.soft_pacing_events, 0);
        assert_eq!(after_reset.drain_lag.soft_pacing_total_ms, 0);
        assert_eq!(after_reset.drain_lag.soft_pacing_max_ms, 0);

        vfs.flush_fh(fh).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_fh_aligned_full_block_callback_is_accepted_immediately() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "sfs-257-write-fh-aligned-full-block");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-write-fh-aligned-full-block.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let payload = vec![0x7Au8; block_size];

        vfs.write_fh(fh, ino, 0, &payload).await.unwrap();

        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            0,
            "aligned full-block callbacks should be staged immediately instead of forcing inline metadata commits"
        );

        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "aligned full-block callbacks should land in the writer stack before flush"
        );
        let visible_attr = vfs.getattr_unchecked(ino).unwrap();
        assert_eq!(
            visible_attr.size,
            block_size as u64,
            "read-visible size should advance with the staged full-block accept"
        );
        let read = vfs.read(ino, 0, block_size as u32).await.unwrap();
        assert_eq!(read.as_ref(), payload.as_slice());

        vfs.flush_fh(fh).await.unwrap();
        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            1,
            "flush should durably materialize the staged full block"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_fh_sub_block_callbacks_batch_completed_blocks_until_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_test_vfs(&tmp, "sfs-257-write-fh-sub-block-burst");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-257-write-fh-sub-block-burst.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let block_size = vfs.block_size as usize;
        let half_block = block_size / 2;

        for block_index in 0..3usize {
            let prefix = vec![0x40 + block_index as u8; half_block];
            let suffix = vec![0x50 + block_index as u8; half_block];
            let base_offset = block_index * block_size;
            vfs.write_fh(fh, ino, base_offset as u64, &prefix)
                .await
                .unwrap();
            vfs.write_fh(fh, ino, (base_offset + half_block) as u64, &suffix)
                .await
                .unwrap();
        }

        let buffered = vfs.read(ino, 0, (block_size * 3) as u32).await.unwrap();
        for block_index in 0..3usize {
            let base = block_index * block_size;
            assert_eq!(
                &buffered[base..base + half_block],
                vec![0x40 + block_index as u8; half_block].as_slice()
            );
            assert_eq!(
                &buffered[base + half_block..base + block_size],
                vec![0x50 + block_index as u8; half_block].as_slice()
            );
        }

        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            0,
            "sub-block sequential callbacks should keep completed blocks buffered until flush can drain a larger burst"
        );

        vfs.flush_fh(fh).await.unwrap();
        assert_eq!(
            meta.lookup_payload_slices(ino).unwrap().len(),
            3,
            "flush should materialize all completed sequential logical blocks"
        );
        assert!(
            !vfs.state.writer_stack.has_dirty_writers(ino),
            "flush should drain the mounted writer stack after batching sub-block callbacks"
        );
        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn inode_write_overwrite_updates_current_slice_graph_over_published_base() {
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
        let (published_epoch, published_blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
        publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);

        vfs.write(ino, 0, replacement).await.unwrap();
        vfs.flush(ino).await.unwrap();

        let current_blocks = meta
            .lookup_payload_slices(ino)
            .expect("load current file graph");
        assert_eq!(current_blocks.len(), 1, "overwrite test should stay single-block");
        assert_ne!(
            current_blocks[0].1.object_id,
            published_blocks[0],
            "current file graph must advance to the replacement payload slice instead of reusing the published base"
        );

        let spans = meta
            .lookup_regular_file_slices(ino, 0, replacement.len() as u64)
            .expect("load current regular-file slices");
        assert_eq!(
            spans.iter().filter_map(|span| span.payload.as_ref()).count(),
            1,
            "overwrite should resolve to one block-backed span"
        );
        assert_eq!(
            spans
                .iter()
                .filter_map(|span| span.payload.as_ref())
                .next()
                .expect("block-backed overwrite span")
                .object_id,
            current_blocks[0].1.object_id,
            "regular-file slice lookup and payload helper must agree on the current unpublished payload"
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
fn test_unlink_immediate_cleanup_clears_pending_inode_dirty_state() {
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
            vfs.inode_has_local_dirty_state(ino),
            "partial inode writes should remain pending until unlink removes the inode"
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
fn unlink_immediate_cleanup_removes_pending_upload_payload_artifacts() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "unlink-pending-upload-cleanup");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"unlink-pending-upload-cleanup.bin", 0, 0, 0o644)
            .unwrap();
        let payload = vec![0xA5; vfs.block_size as usize];

        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        let object_id = meta
            .lookup_payload_slices(attr.ino)
            .expect("load flushed payload slices")
            .into_iter()
            .next()
            .expect("flushed file should have one payload object")
            .1
            .object_id;

        assert!(
            vfs.local_payload_backend().contains(object_id),
            "setup should leave the flushed payload in the local backend"
        );
        assert!(
            vfs.cached_store().has_staged_upload(object_id),
            "setup should create the upload-stage hardlink"
        );
        assert!(
            vfs.cached_store().is_pinned(object_id),
            "pending upload should pin the local payload before unlink"
        );
        assert!(
            !vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "setup should queue a pending upload before unlink"
        );

        vfs.unlink(1, b"unlink-pending-upload-cleanup.bin").unwrap();

        assert!(
            vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "unlink should cancel queued uploads for the deleted inode"
        );
        assert!(
            !vfs.cached_store().is_pinned(object_id),
            "unlink should unpin canceled upload payloads so they can be removed immediately"
        );
        assert!(
            !vfs.cached_store().has_staged_upload(object_id),
            "unlink should remove upload-stage hardlinks for deleted payloads"
        );
        assert!(
            vfs.cached_store().get_hot_upload(object_id).is_none(),
            "unlink should clear any hot upload copy for deleted payloads"
        );
        assert!(
            !vfs.local_payload_backend().contains(object_id),
            "unlink should remove deleted file payloads from the local backend before returning"
        );
        assert!(
            !vfs.state
                .upload
                .pending_evictions
                .lock()
                .contains(&object_id),
            "immediate cleanup should not leave a deferred eviction for the canceled upload"
        );
    });
}

#[test]
fn foreign_delete_lazy_evict_removes_local_payload_artifacts() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "foreign-delete-lazy-evict");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"foreign-delete-lazy-evict.bin", 0, 0, 0o644)
            .unwrap();
        let payload = vec![0x5C; vfs.block_size as usize];

        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        let object_id = meta
            .lookup_payload_slices(attr.ino)
            .expect("load flushed payload slices")
            .into_iter()
            .next()
            .expect("flushed file should have one payload object")
            .1
            .object_id;

        assert!(vfs.local_payload_backend().contains(object_id));
        assert!(vfs.cached_store().has_staged_upload(object_id));
        assert!(vfs.cached_store().is_pinned(object_id));

        vfs.lazy_evict_removed_metadata_payloads(
            std::collections::HashMap::from([(attr.ino, vec![object_id])]),
            &std::collections::HashSet::new(),
        )
        .await;

        assert!(
            vfs.pending_uploads_for_inode(attr.ino).is_empty(),
            "lazy eviction should cancel queued uploads for a remotely deleted inode"
        );
        assert!(
            !vfs.cached_store().is_pinned(object_id),
            "lazy eviction should unpin remotely deleted payloads"
        );
        assert!(
            !vfs.cached_store().has_staged_upload(object_id),
            "lazy eviction should remove upload-stage hardlinks"
        );
        assert!(
            !vfs.local_payload_backend().contains(object_id),
            "lazy eviction should remove local cached payload bytes"
        );
    });
}

#[test]
fn retained_inode_payload_evict_does_not_mark_inode_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "retained-inode-payload-evict");
    let _guard = rt.enter();

    rt.block_on(async {
        let attr = vfs
            .create(1, b"retained-inode-payload-evict.bin", 0, 0, 0o644)
            .unwrap();
        let payload = vec![0x7A; vfs.block_size as usize];

        vfs.write(attr.ino, 0, &payload).await.unwrap();
        vfs.flush(attr.ino).await.unwrap();

        let object_id = vfs
            .local_payload_backend()
            .object_ids()
            .into_iter()
            .next()
            .expect("flushed file should seed one local payload object");

        vfs.lazy_evict_removed_metadata_payloads(
            std::collections::HashMap::from([(attr.ino, vec![object_id])]),
            &std::collections::HashSet::from([ROOT_INO, attr.ino]),
        )
        .await;

        assert!(
            !vfs.state.upload.deleted_inodes.lock().contains(&attr.ino),
            "removed payloads for a retained inode must not poison future uploads with a delete marker"
        );
        assert!(
            !vfs.local_payload_backend().contains(object_id),
            "retained-inode payload eviction should still reclaim removed local payload bytes"
        );
    });
}

#[test]
fn metadata_payload_diff_detects_removed_objects_for_retained_inode() {
    let previous = std::collections::HashMap::from([(42, vec![1001, 1002]), (77, vec![2001])]);
    let current = std::collections::HashMap::from([(42, vec![1002, 1003])]);

    let removed = StudioVfs::removed_metadata_payloads(&previous, &current);

    assert_eq!(
        removed.get(&42).cloned(),
        Some(vec![1001]),
        "payload diff should evict objects removed by a same-inode remote update"
    );
    assert_eq!(
        removed.get(&77).cloned(),
        Some(vec![2001]),
        "payload diff should still evict all objects when the inode disappears"
    );
    assert!(
        !removed
            .get(&42)
            .is_some_and(|object_ids| object_ids.contains(&1002)),
        "payload diff must retain objects still referenced by the current snapshot"
    );
}

#[test]
fn test_rename_over_existing_target_discards_pending_local_state() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "rename-displaced-cleanup");
    let _guard = rt.enter();
    rt.block_on(async {
        let source = vfs.create(1, b"rename-source.txt", 0, 0, 0o644).unwrap();
        let displaced = vfs.create(1, b"rename-target.txt", 0, 0, 0o644).unwrap();

        vfs.write(source.ino, 0, b"source").await.unwrap();
        vfs.write(displaced.ino, 0, b"target").await.unwrap();
        assert!(
            vfs.inode_has_local_dirty_state(displaced.ino),
            "a displaced overwrite target may still carry pending local state before rename"
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
fn orphan_cleanup_clears_dirty_handle_writer_state_for_deleted_inode() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "orphan-handle-writer-cleanup");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"orphan-handle-writer-cleanup.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh_with_local_sync_policy(fh, ino, 0, b"payload", LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();
        assert!(
            vfs.state.writer_stack.has_dirty_writers(ino),
            "orphan cleanup test requires buffered writer-stack state before unlink"
        );

        vfs.unlink(1, b"orphan-handle-writer-cleanup.txt").unwrap();
        vfs.release_fh(fh).await.unwrap();

        assert!(
            !vfs.state.writer_stack.has_dirty_writers(ino),
            "final orphan cleanup must clear stale writer-stack state for the deleted inode"
        );
        assert_eq!(
            vfs.state.writer_stack.total_dirty_bytes(),
            0,
            "deleted inode must not retain dirty writer-stack bytes after last-handle cleanup"
        );
    });
}

#[test]
fn test_flush_fh_with_outcome_materializes_pending_inode_dirty_state() {
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
            vfs.inode_has_local_dirty_state(ino),
            "partial inode writes should stay buffered until the explicit flush barrier"
        );

        let after_write = vfs.flush_fh_with_outcome(fh).await.unwrap();
        assert!(
            after_write.flushed_local_dirty_state,
            "flush_fh should report that it materialized the pending inode dirty state"
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
fn test_flush_with_outcome_materializes_pending_inode_dirty_state() {
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
            vfs.inode_has_local_dirty_state(ino),
            "partial inode writes should stay buffered until the explicit inode flush barrier"
        );

        let after_write = vfs.flush_with_outcome(ino).await.unwrap();
        assert!(
            after_write.flushed_local_dirty_state,
            "flush_with_outcome should report that it materialized the pending inode dirty state"
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
fn test_fsync_with_outcome_materializes_pending_inode_dirty_state() {
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
            vfs.inode_has_local_dirty_state(ino),
            "partial inode writes should remain pending until fsync forces the local barrier"
        );

        let after_write = vfs.fsync_with_outcome(ino, false).await.unwrap();
        assert!(
            after_write.flushed_file_data,
            "fsync should report that it flushed the pending file data"
        );
        assert!(
            !after_write.persisted_dirty_attrs,
            "fsync should not report extra dirty-attr work once the file-data barrier already committed the pending state"
        );
    });
}

#[test]
fn test_fsync_with_outcome_reports_handle_writer_file_data_barrier() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "fsync-outcome-handle-writer");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"fsync-outcome-handle-writer.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        vfs.write_fh_with_local_sync_policy(fh, ino, 0, b"payload", LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();

        let after_write = vfs.fsync_with_outcome(ino, false).await.unwrap();
        assert!(
            after_write.flushed_file_data,
            "fsync should report file-data work when it materializes dirty writer-stack state"
        );

        let clean = vfs.fsync_with_outcome(ino, false).await.unwrap();
        assert!(
            !clean.flushed_file_data,
            "once the writer-stack state is drained, the next fsync should report a clean data path"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn test_flush_fh_retries_inline_dirty_attr_persist_after_transient_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let (rt, vfs, meta) = make_test_vfs(&tmp, "flush-fh-inline-dirty-attr-retry");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"flush-fh-inline-dirty-attr-retry.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        let mut dirty_attr = meta.get_attr(ino).expect("load attr");
        dirty_attr.mtime = InodeAttr::now_nanos();
        dirty_attr.ctime = dirty_attr.mtime;
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, dirty_attr.clone());

        vfs.arm_fail_next_inline_dirty_attr_persist_for_test();
        let err = vfs
            .flush_fh_with_outcome(fh)
            .await
            .expect_err("simulated inline dirty attr persist failure should fail flush_fh");
        assert!(
            err.to_string()
                .contains("simulated inline dirty attr persist failure"),
            "expected the injected inline dirty attr persist failure to surface"
        );
        assert!(
            vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
            "failed inline dirty attr persist must leave dirty attrs queued for retry"
        );

        vfs.flush_fh_with_outcome(fh)
            .await
            .expect("retry flush_fh after transient dirty attr persist failure");
        assert!(
            !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
            "successful retry should clear the queued dirty attrs"
        );

        let persisted = meta.get_attr(ino).expect("load persisted attr");
        assert_eq!(
            persisted.mtime, dirty_attr.mtime,
            "retry flush should eventually persist the queued dirty attr update"
        );

        vfs.release_fh(fh).await.unwrap();
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

        let blocks = meta.lookup_payload_slices(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            1,
            "failed rollback should restore the original logical block summary"
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

        let blocks = meta.lookup_payload_slices(attr.ino).unwrap();
        assert_eq!(
            blocks.len(),
            2,
            "rollback should restore both originally committed payload slices"
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
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 4096;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.maybe_payload_store_sync_data_async(0, LocalSyncPolicy::Strict)
            .await
            .expect("strict local sync");
    });

    let calls_after = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
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
        let group = vfs.state.local_dirty.local_sync_group.lock().await;
        assert_eq!(group.pending_bytes, 0);
    });
}

#[test]
fn opportunistic_local_sync_defers_when_threshold_not_reached() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "opportunistic-local-sync-group");
    rt.block_on(async {
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 1024;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let deferred_before = vfs
        .state
        .metrics
        .payload_store_sync_data_deferred_calls
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.maybe_payload_store_sync_data_async(0, LocalSyncPolicy::Opportunistic)
            .await
            .expect("opportunistic local sync");
    });

    let calls_after = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let deferred_after = vfs
        .state
        .metrics
        .payload_store_sync_data_deferred_calls
        .load(Ordering::Relaxed);
    assert_eq!(
        calls_after, calls_before,
        "opportunistic sync should not force sync_data while below the byte threshold"
    );
    assert!(
        deferred_after > deferred_before,
        "opportunistic defer should be tracked"
    );
}

#[test]
fn flush_fh_enforces_strict_local_durability_in_single_mode() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-fh-fast-local-sync");
    let attr = vfs
        .create(1, b"flush-fh-fast-local-sync.txt", 0, 0, 0o644)
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
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 4096;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.flush_fh(fh).await.expect("flush_fh barrier");
    });

    let calls_after = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "explicit flush must force sync_data when dirty unpublished state remains"
    );
    assert!(
        synced_after >= synced_before + 4096,
        "explicit flush should account for the pending local durable sync bytes"
    );
    rt.block_on(async {
        let group = vfs.state.local_dirty.local_sync_group.lock().await;
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
fn flush_fh_default_config_enforces_strict_local_durability() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = VfsConfig::default();
    let (rt, vfs, _meta) = make_test_vfs_with_config(&tmp, "flush-fh-durable-local-sync", cfg);
    let attr = vfs
        .create(1, b"flush-fh-durable-local-sync.txt", 0, 0, 0o644)
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
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 4096;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.flush_fh(fh).await.expect("flush_fh barrier");
    });

    let calls_after = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "default-config explicit flush must force sync_data when dirty unpublished state remains"
    );
    assert!(
        synced_after >= synced_before + 4096,
        "default-config explicit flush should account for the pending local durable sync bytes"
    );
    rt.block_on(async {
        let group = vfs.state.local_dirty.local_sync_group.lock().await;
        assert_eq!(
            group.pending_bytes, 0,
            "default-config explicit flush should drain pending local sync bytes"
        );
    });
    assert!(
        !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
        "default-config explicit flush should still commit the dirty local attr state"
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
        assert!(vfs.state.writer_stack.has_dirty_writers(ino));

        vfs.flush_fh(fh).await.unwrap();

        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().any(|span| span.payload.is_some()),
            "flush_fh should materialize block-backed local unpublished slices: {spans:?}"
        );
        assert!(
            !vfs.pending_uploads_for_inode(ino).is_empty(),
            "flush_fh should enqueue writer-stack payloads for background upload after metadata commit"
        );
        assert!(
            !vfs.state.writer_stack.has_dirty_writers(ino),
            "flush_fh should drain writer-stack dirty state"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn flush_fh_materializes_handle_writer_state_through_strict_local_barrier() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "sfs-245-flush-without-ordered-flush");
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
            .flush_barrier_calls
            .load(Ordering::Relaxed);

        vfs.flush_fh(fh).await.unwrap();

        let ordered_flushes_after = vfs
            .state
            .metrics
            .flush_barrier_calls
            .load(Ordering::Relaxed);
        assert_eq!(
            ordered_flushes_after,
            ordered_flushes_before + 1,
            "slice-first handle-writer flush should route through one strict local barrier to commit staged slices"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn write_fh_empty_payload_is_a_noop_for_handle_writer_state() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-245-empty-handle-write");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-empty-handle-write.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

        let written = vfs
            .write_fh_with_local_sync_policy(fh, ino, 0, &[], LocalSyncPolicy::Opportunistic)
            .await
            .unwrap();

        assert_eq!(written, 0);
        assert!(
            !vfs.state.handle_writers.has_dirty_writers(ino),
            "empty handle writes should not create dirty writer state"
        );
        assert!(
            !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
            "empty handle writes should not stage dirty attrs"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn handle_writer_dirty_attrs_keep_nanos_timestamps() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "sfs-245-handle-write-nanos");
    let _guard = rt.enter();
    rt.block_on(async {
        let attr = vfs
            .create(1, b"sfs-245-handle-write-nanos.txt", 0, 0, 0o644)
            .unwrap();
        let ino = attr.ino;
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
        let before = vfs.getattr(ino).unwrap();

        vfs.write_fh_with_local_sync_policy(
            fh,
            ino,
            0,
            b"timestamp-check",
            LocalSyncPolicy::Opportunistic,
        )
        .await
        .unwrap();

        let dirty_attr = vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .expect("dirty attr");
        assert!(
            dirty_attr.mtime >= before.mtime,
            "handle-writer mtime should not go backwards"
        );
        assert!(
            dirty_attr.ctime >= before.ctime,
            "handle-writer ctime should not go backwards"
        );
        assert!(
            dirty_attr.mtime > 1_000_000_000_000,
            "handle-writer timestamps should remain in nanoseconds"
        );
        assert!(
            dirty_attr.ctime > 1_000_000_000_000,
            "handle-writer timestamps should remain in nanoseconds"
        );

        vfs.release_fh(fh).await.unwrap();
    });
}

#[test]
fn release_fh_enforces_strict_local_durability_on_last_handle() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-fh-fast-local-sync");
    let attr = vfs
        .create(1, b"release-fh-fast-local-sync.txt", 0, 0, 0o644)
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
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 8192;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
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
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "last-handle release must force sync_data when dirty unpublished state remains"
    );
    assert!(
        synced_after >= synced_before + 8192,
        "last-handle release should account for the pending local durable sync bytes"
    );
    rt.block_on(async {
        let group = vfs.state.local_dirty.local_sync_group.lock().await;
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
fn release_fh_default_config_enforces_strict_local_durability() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = VfsConfig::default();
    let (rt, vfs, _meta) = make_test_vfs_with_config(&tmp, "release-fh-durable-local-sync", cfg);
    let attr = vfs
        .create(1, b"release-fh-durable-local-sync.txt", 0, 0, 0o644)
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
        let mut group = vfs.state.local_dirty.local_sync_group.lock().await;
        group.pending_bytes = 8192;
        group.last_sync_at = Instant::now();
    });

    let calls_before = vfs
        .state
        .metrics
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_before = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
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
        .payload_store_sync_data_calls
        .load(Ordering::Relaxed);
    let synced_after = vfs
        .state
        .metrics
        .payload_store_sync_data_synced_bytes
        .load(Ordering::Relaxed);
    assert!(
        calls_after > calls_before,
        "default-config last-handle release must force sync_data when dirty unpublished state remains"
    );
    assert!(
        synced_after >= synced_before + 8192,
        "default-config last-handle release should account for the pending local durable sync bytes"
    );
    rt.block_on(async {
        let group = vfs.state.local_dirty.local_sync_group.lock().await;
        assert_eq!(
            group.pending_bytes, 0,
            "default-config last-handle release should drain pending local sync bytes"
        );
    });
    assert!(
        !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
        "default-config last-handle release should preserve the committed local attr state"
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
        assert!(vfs.state.writer_stack.has_dirty_writers(ino));

        let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
        assert!(outcome.closed_last_handle);
        assert!(outcome.flushed_local_dirty_state);

        let spans = meta
            .lookup_regular_file_slices(ino, 0, payload.len() as u64)
            .unwrap();
        assert!(
            spans.iter().any(|span| span.payload.is_some()),
            "last-handle release should materialize block-backed local unpublished slices: {spans:?}"
        );
        assert!(
            !vfs.pending_uploads_for_inode(ino).is_empty(),
            "last-handle release should enqueue writer-stack payloads for background upload after metadata commit"
        );
        assert!(
            !vfs.state.writer_stack.has_dirty_writers(ino),
            "last-handle release should drain writer-stack dirty state"
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
        snapshot.payload_burst.total_payloads >= 2,
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
        snapshot.payload_store_sync_data.calls >= 1,
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
}

#[test]
fn write_path_observability_reports_winfsp_write_callback_breakdown() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "write-path-observability-winfsp-breakdown");

    vfs.note_winfsp_write_callback();
    vfs.record_winfsp_write_callback_observability(
        256 * 1024,
        LocalSyncPolicy::Strict,
        Duration::from_millis(6),
        Duration::from_millis(5),
        Duration::from_millis(1),
    );

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(
        snapshot.winfsp_callbacks.write, 1,
        "callback count should still be reflected in the WinFSP callback breakdown"
    );
    assert_eq!(
        snapshot.winfsp_write_path.write_bytes,
        256 * 1024,
        "observability should surface cumulative write callback bytes"
    );
    assert!(
        snapshot.winfsp_write_path.avg_write_bytes >= 256.0 * 1024.0,
        "observability should expose average bytes per write callback"
    );
    assert_eq!(
        snapshot.winfsp_write_path.strict_calls, 1,
        "observability should attribute strict WinFSP callbacks"
    );
    assert_eq!(
        snapshot.winfsp_write_path.strict_bytes,
        256 * 1024,
        "observability should surface bytes written through the strict WinFSP path"
    );
    assert_eq!(
        snapshot.winfsp_write_path.opportunistic_calls, 0,
        "observability should keep opportunistic counts separate"
    );
    assert_eq!(
        snapshot.winfsp_write_path.callback.calls, 1,
        "callback latency stats should count WinFSP write callbacks"
    );
    assert_eq!(
        snapshot.winfsp_write_path.vfs_write.calls, 1,
        "VFS write latency stats should count WinFSP callback work"
    );
    assert_eq!(
        snapshot.winfsp_write_path.post_write_getattr.calls, 1,
        "post-write getattr latency stats should count the WinFSP tail work"
    );
    assert!(
        snapshot.winfsp_write_path.callback.total_ms
            >= snapshot.winfsp_write_path.vfs_write.total_ms,
        "full callback latency should be at least as large as the inner VFS write latency"
    );
}

#[test]
fn write_path_observability_reports_vfs_write_stage_breakdown() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "write-path-observability-vfs-breakdown");

    vfs.note_vfs_write_call(4 * 1024 * 1024);
    vfs.record_vfs_write_record_handle_writer_state(Duration::from_millis(5));
    vfs.record_vfs_write_record_writer_stack_state(Duration::from_millis(3));
    vfs.record_vfs_write_materialize_completed(Duration::from_millis(7));
    vfs.record_vfs_write_materialize_for_limits(Duration::from_millis(2));

    let snapshot = vfs.write_path_observability_snapshot();
    assert_eq!(snapshot.vfs_write_path.calls, 1);
    assert_eq!(snapshot.vfs_write_path.write_bytes, 4 * 1024 * 1024);
    assert_eq!(snapshot.vfs_write_path.record_handle_writer_state.calls, 1);
    assert_eq!(snapshot.vfs_write_path.record_writer_stack_state.calls, 1);
    assert_eq!(snapshot.vfs_write_path.materialize_completed.calls, 1);
    assert_eq!(snapshot.vfs_write_path.materialize_strict.calls, 0);
    assert_eq!(snapshot.vfs_write_path.materialize_for_limits.calls, 1);
    assert!(snapshot.vfs_write_path.avg_write_bytes >= 4.0 * 1024.0 * 1024.0);
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
        snapshot.payload_burst.total_payloads, 0,
        "failed payload persists must not add completed burst payload counts"
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
        vfs.payload_store_put_async(0xAA55, Bytes::from_static(b"payload"), false)
            .await
            .expect("payload store put");
        let payload = vfs
            .payload_store_get_async(0xAA55)
            .await
            .expect("payload store get")
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
    let handoff_before = vfs
        .state
        .metrics
        .local_cache_io_handoff_total_ns
        .load(Ordering::Relaxed);

    rt.block_on(async {
        vfs.payload_store_put_async(0xBB66, Bytes::from_static(b"payload"), false)
            .await
            .expect("payload store put");
        let handoff_before_read = vfs
            .state
            .metrics
            .local_cache_io_handoff_total_ns
            .load(Ordering::Relaxed);
        let mut out = [0_u8; 3];
        let read = vfs
            .payload_store_read_into_async(0xBB66, 2, &mut out)
            .await
            .expect("payload store read_into");
        assert_eq!(read, Some(3));
        assert_eq!(&out, b"ylo");
        let handoff_after_read = vfs
            .state
            .metrics
            .local_cache_io_handoff_total_ns
            .load(Ordering::Relaxed);
        assert_eq!(
            handoff_after_read, handoff_before_read,
            "direct read_into should stay on the runtime worker and avoid blocking-pool handoff"
        );
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
    let handoff_after = vfs
        .state
        .metrics
        .local_cache_io_handoff_total_ns
        .load(Ordering::Relaxed);

    assert!(
        calls_after >= calls_before + 2,
        "put + read_into should record local-cache I/O calls"
    );
    assert!(
        backend_after >= backend_before,
        "direct read_into should contribute backend execution timing"
    );
    assert!(
        handoff_after >= handoff_before,
        "overall local-cache handoff accounting must remain monotonic"
    );
}

#[test]
fn local_cache_read_into_returns_none_for_missing_block() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-read-into-missing");
    rt.block_on(async {
        let mut out = [0_u8; 8];
        let read = vfs
            .payload_store_read_into_async(0xDEADBEEF, 0, &mut out)
            .await
            .expect("read_into should not error on cache miss");
        assert_eq!(read, None);
    });
}

#[test]
fn payload_store_get_prefers_hot_upload_buffer_before_disk() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-get-hot-upload");
    rt.block_on(async {
        vfs.cache_upload_hot_batch(vec![studiofs_cache::local::LocalPayloadPut {
            object_id: 0xABCD,
            data: Bytes::from_static(b"hot-payload"),
            evictable: false,
        }]);

        let payload = vfs
            .payload_store_get_async(0xABCD)
            .await
            .expect("payload store get from hot upload buffer")
            .expect("hot upload payload");
        assert_eq!(payload.as_ref(), b"hot-payload");
    });
}

#[test]
fn payload_store_read_into_prefers_hot_upload_buffer_before_disk() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-read-into-hot-upload");
    rt.block_on(async {
        vfs.cache_upload_hot_batch(vec![studiofs_cache::local::LocalPayloadPut {
            object_id: 0xBCDE,
            data: Bytes::from_static(b"hot-payload"),
            evictable: false,
        }]);

        let mut out = [0_u8; 7];
        let read = vfs
            .payload_store_read_into_async(0xBCDE, 4, &mut out)
            .await
            .expect("payload store read_into from hot upload buffer");
        assert_eq!(read, Some(7));
        assert_eq!(&out, b"payload");
    });
}

#[test]
fn committed_copy_rejects_short_payload() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "committed-copy-short-payload");
    let block = PayloadRef::new_full(42, vfs.block_size);
    let mut dst = [0_u8; 8];
    let err = vfs
        .copy_committed_payload_into_range(b"abcd", &block, 0, &mut dst)
        .expect_err("short committed payload should error");
    let message = match err {
        VfsError::Other(message) => message,
        other => panic!("expected VfsError::Other, got {other}"),
    };
    assert!(
        message.contains("committed payload object too short"),
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

fn foreign_exclusive_lock(ino: Ino) -> FileLockLease {
    FileLockLease {
        ino,
        kind: HardLockKind::Exclusive,
        generation: 1,
        lease_expires_at_unix_ms: i64::MAX,
        exclusive_owner: Some("client-slot:foreign".to_string()),
        shared_owners: BTreeSet::new(),
    }
}

#[test]
fn foreign_hard_lock_blocks_new_write_open_but_allows_readonly_open() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "foreign-lock-open");
    let attr = vfs
        .create(ROOT_INO, b"foreign-lock-open.txt", 0, 0, 0o644)
        .expect("create file");
    vfs.set_known_file_lock_for_test(attr.ino, Some(foreign_exclusive_lock(attr.ino)));

    let blocked = vfs.open_file(attr.ino, OpenFlags::RDWR, false);
    assert!(
        matches!(blocked, Err(VfsError::WouldBlock)),
        "new write-open should block once a foreign hard lock is known"
    );

    let fh = vfs
        .open_file(attr.ino, OpenFlags::RDONLY, false)
        .expect("read-only open should still succeed under a foreign hard lock");
    let open_file = vfs.get_open_file(fh).expect("tracked open file");
    assert_eq!(open_file.flags.bits(), OpenFlags::RDONLY.bits());
}

#[test]
fn existing_writer_handle_remains_usable_after_foreign_hard_lock_becomes_known() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "foreign-lock-grandfather");
    let attr = vfs
        .create(ROOT_INO, b"foreign-lock-grandfather.txt", 0, 0, 0o644)
        .expect("create file");
    let fh = vfs
        .open_file(attr.ino, OpenFlags::RDWR, false)
        .expect("open writer before lock becomes known");

    vfs.set_known_file_lock_for_test(attr.ino, Some(foreign_exclusive_lock(attr.ino)));

    rt.block_on(async {
        vfs.write_fh(fh, attr.ino, 0, b"abc")
            .await
            .expect("pre-lock writer handle should be allowed to finish");
    });

    assert_eq!(vfs.getattr(attr.ino).expect("getattr").size, 3);
}

#[test]
fn foreign_hard_lock_blocks_size_change_setattr_without_handle() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "foreign-lock-setattr");
    let attr = vfs
        .create(ROOT_INO, b"foreign-lock-setattr.txt", 0, 0, 0o644)
        .expect("create file");
    vfs.set_known_file_lock_for_test(attr.ino, Some(foreign_exclusive_lock(attr.ino)));

    let result = vfs.setattr(attr.ino, SetattrArgs::size_only(1));
    assert!(
        matches!(result, Err(VfsError::WouldBlock)),
        "inode-scoped size changes should block when a foreign hard lock is known"
    );
}

#[test]
fn foreign_hard_lock_blocks_unlink_and_rename() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_rt, vfs, _meta) = make_test_vfs(&tmp, "foreign-lock-namespace");
    let attr = vfs
        .create(ROOT_INO, b"foreign-lock-namespace.txt", 0, 0, 0o644)
        .expect("create file");
    vfs.set_known_file_lock_for_test(attr.ino, Some(foreign_exclusive_lock(attr.ino)));

    let unlink = vfs.unlink(ROOT_INO, b"foreign-lock-namespace.txt");
    assert!(
        matches!(unlink, Err(VfsError::WouldBlock)),
        "unlink should block when the target inode is hard-locked by a peer"
    );

    let rename = vfs.rename(
        ROOT_INO,
        b"foreign-lock-namespace.txt",
        ROOT_INO,
        b"foreign-lock-namespace-moved.txt",
    );
    assert!(
        matches!(rename, Err(VfsError::WouldBlock)),
        "rename should block when the source inode is hard-locked by a peer"
    );
}

#[test]
fn read_only_metadata_authority_blocks_new_mutations_but_keeps_reads_open() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (rt, vfs, _meta) = make_test_vfs(&tmp, "offline-read-only-authority");
    let attr = vfs
        .create(ROOT_INO, b"offline-read-only-authority.txt", 0, 0, 0o644)
        .expect("create file");
    let fh = vfs
        .open_file(attr.ino, OpenFlags::RDWR, false)
        .expect("open writer before authority degrades");

    vfs.set_metadata_authority_status_for_test(Some(MetadataAuthorityStatus::read_only()));

    let blocked_open = vfs.open_file(attr.ino, OpenFlags::RDWR, false);
    assert!(
        matches!(
            blocked_open,
            Err(VfsError::Meta(MetaError::AccessDenied {
                actual: AccessLevel::ReadOnly,
                ..
            }))
        ),
        "new write-capable opens should stop once metadata authority degrades to read-only"
    );

    let ro_fh = vfs
        .open_file(attr.ino, OpenFlags::RDONLY, false)
        .expect("read-only open should remain available after authority downgrade");
    let ro_open = vfs.get_open_file(ro_fh).expect("tracked read-only open");
    assert_eq!(ro_open.flags.bits(), OpenFlags::RDONLY.bits());

    rt.block_on(async {
        let write_error = vfs
            .write_fh(fh, attr.ino, 0, b"abc")
            .await
            .expect_err("existing writer handles should also become read-only");
        assert!(
            matches!(
                write_error,
                VfsError::Meta(MetaError::AccessDenied {
                    actual: AccessLevel::ReadOnly,
                    ..
                })
            ),
            "unexpected write error after authority downgrade: {write_error}"
        );
    });

    let create_error = vfs.create(ROOT_INO, b"blocked-after-downgrade.txt", 0, 0, 0o644);
    assert!(
        matches!(
            create_error,
            Err(VfsError::Meta(MetaError::AccessDenied {
                actual: AccessLevel::ReadOnly,
                ..
            }))
        ),
        "new namespace mutations should stop once metadata authority degrades to read-only"
    );
}
