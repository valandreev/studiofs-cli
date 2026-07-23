#[cfg(test)]
mod tests {
    use super::super::core::*;
    use super::super::ports::*;
    use super::super::*;
    use futures::stream::{FuturesUnordered, StreamExt};

    #[cfg(test)]
    fn assign_reserved_object_ids(
        pending: &mut std::collections::HashMap<(Ino, u32), PendingPayload>,
        ino: Ino,
        blocks: &mut [PersistedPayloadEntry],
        fresh_object_ids: &[u64],
    ) {
        let mut fresh_ids = fresh_object_ids.iter().copied();
        for block in blocks {
            let key = (ino, block.block_index);
            if let Some(current) = pending.get_mut(&key)
                && !current.is_local_extent()
                && block
                    .payload_bytes()
                    .is_some_and(|payload| current.data.as_ref() == payload.as_ref())
            {
                if let Some(object_id) = current.reserved_object_id {
                    block.object_id = object_id;
                    continue;
                }

                let object_id = fresh_ids
                    .next()
                    .expect("missing fresh object id for local dirty payload entry");
                current.reserved_object_id = Some(object_id);
                block.object_id = object_id;
                continue;
            }

            block.object_id = fresh_ids
                .next()
                .expect("missing fresh object id for local dirty payload entry");
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    async fn run_bounded_tasks<I, F, Fut, T, E>(
        items: I,
        concurrency: usize,
        mut make_future: F,
    ) -> Result<(), E>
    where
        I: IntoIterator,
        F: FnMut(I::Item) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let mut items = items.into_iter();
        let mut inflight = FuturesUnordered::new();
        let max_inflight = concurrency.max(1);
        let mut first_error = None;

        while inflight.len() < max_inflight {
            let Some(item) = items.next() else {
                break;
            };
            inflight.push(make_future(item));
        }

        while let Some(result) = inflight.next().await {
            match result {
                Ok(_) => {
                    if first_error.is_none()
                        && let Some(item) = items.next()
                    {
                        inflight.push(make_future(item));
                    }
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{
        MetaEngineCore, MetaFormat, MetaInodes, MetaPayloadObjects, RedbMetaEngine,
    };
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    type UploadRecord = (u64, u64, u64, u64);
    type SharedUploads = Arc<StdMutex<Vec<UploadRecord>>>;
    type SharedCommittedPayloads = Arc<StdMutex<Vec<Vec<u64>>>>;
    type SharedInodes = Arc<StdMutex<Vec<Ino>>>;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("local-dirty-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("local-dirty-uuid-{volume_suffix}"),
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

    #[test]
    fn concurrent_overlay_commits_share_one_metadata_batch() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "overlay-batch");
        let first = vfs
            .create(1, b"first.bin", 0, 0, 0o644)
            .expect("create first file");
        let second = vfs
            .create(1, b"second.bin", 0, 0, 0o644)
            .expect("create second file");

        rt.block_on(async {
            let committer = VfsOverlayCommitter { vfs: &vfs };
            let (first_result, second_result) = tokio::join!(
                committer.commit_overlay(first.ino, Vec::new(), Some(first.clone())),
                committer.commit_overlay(second.ino, Vec::new(), Some(second)),
            );
            first_result.expect("commit first overlay");
            second_result.expect("commit second overlay");
        });

        let batch = meta.metadata_write_observability().batch;
        assert_eq!(batch.batches, 1);
        assert_eq!(batch.requested_ops, 2);

        vfs.state
            .local_dirty
            .mark_overlay_metadata_error("sticky overlay failure".to_string());
        rt.block_on(async {
            let committer = VfsOverlayCommitter { vfs: &vfs };
            committer
                .commit_overlay(first.ino, Vec::new(), Some(first))
                .await
                .expect("a later successful batch should still complete");
        });
        assert!(
            vfs.ensure_volatile_overlay_metadata_healthy().is_err(),
            "a successful volatile batch must not clear a previous metadata failure"
        );
    }

    fn single_block_fixture(
        ino: Ino,
        block_index: u32,
        data: &'static [u8],
        reserved_object_id: Option<u64>,
    ) -> (
        std::collections::HashMap<(Ino, u32), PendingPayload>,
        Vec<DirtyWriteSnapshot>,
    ) {
        (
            std::collections::HashMap::from([(
                (ino, block_index),
                PendingPayload::buffered(BytesMut::from(data), reserved_object_id),
            )]),
            vec![DirtyWriteSnapshot::from_mutable(
                block_index,
                BytesMut::from(data),
            )],
        )
    }

    #[test]
    fn pending_slice_commit_without_dirty_attr_extends_visible_size() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "pending-slice-attr-recovery");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"pending-slice-attr-recovery.bin", 0, 0, 0o644)
                .expect("create file");
            let ino = attr.ino;
            let block_size = vfs.block_size;
            let pending_slices = std::collections::BTreeMap::from([(
                0,
                vec![PendingDirtySlice::persisted(
                    studiofs_meta::schema::SliceRange::new(0, block_size)
                        .expect("pending slice range"),
                    0,
                    0,
                    41,
                    9001,
                    0,
                )],
            )]);

            vfs.local_dirty_flush_session(ino)
                .commit_pending_slice_snapshot(pending_slices, 1, None)
                .await
                .expect("commit pending slice without dirty attr");

            let persisted = meta.get_attr(ino).expect("persisted attr");
            assert_eq!(
                persisted.size,
                u64::from(block_size),
                "metadata size must cover locally committed overlay slices even if dirty_attr was already cleared"
            );
        });
    }

    #[test]
    fn stale_pending_slice_commit_without_dirty_attr_preserves_newer_size() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "pending-slice-stale-attr");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"pending-slice-stale-attr.bin", 0, 0, 0o644)
                .expect("create file");
            let ino = attr.ino;
            let block_size = vfs.block_size;
            let mut newer_attr = meta.get_attr(ino).expect("load attr");
            newer_attr.size = 0;
            newer_attr.content_epoch = 2;
            meta.set_attr(&newer_attr).expect("store newer attr");
            let pending_slices = std::collections::BTreeMap::from([(
                0,
                vec![PendingDirtySlice::persisted(
                    studiofs_meta::schema::SliceRange::new(0, block_size)
                        .expect("pending slice range"),
                    0,
                    0,
                    41,
                    9002,
                    0,
                )],
            )]);

            vfs.local_dirty_flush_session(ino)
                .commit_pending_slice_snapshot(pending_slices, 1, None)
                .await
                .expect("commit stale pending slice without dirty attr");

            let persisted = meta.get_attr(ino).expect("persisted attr");
            assert_eq!(
                persisted.size, 0,
                "stale pending slice commits must not grow a newer inode attr"
            );
            assert_eq!(
                persisted.content_epoch, 2,
                "stale pending slice commits must preserve the newer content epoch"
            );
        });
    }

    #[test]
    fn stale_pending_slice_commit_with_dirty_attr_preserves_newer_metadata() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "pending-slice-stale-dirty-attr");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"pending-slice-stale-dirty-attr.bin", 0, 0, 0o644)
                .expect("create file");
            let ino = attr.ino;
            let block_size = vfs.block_size;
            let stale_object_id = 9003;
            let mut stale_attr = meta.get_attr(ino).expect("load stale attr");
            stale_attr.size = u64::from(block_size);
            stale_attr.content_epoch = 1;

            let mut newer_attr = stale_attr.clone();
            newer_attr.size = 0;
            newer_attr.content_epoch = 2;
            meta.set_attr(&newer_attr).expect("store newer attr");
            vfs.local_payload_backend()
                .put(stale_object_id, b"stale payload", false)
                .expect("store stale local payload");

            let pending_slices = std::collections::BTreeMap::from([(
                0,
                vec![PendingDirtySlice::persisted(
                    studiofs_meta::schema::SliceRange::new(0, block_size)
                        .expect("pending slice range"),
                    0,
                    0,
                    41,
                    stale_object_id,
                    0,
                )],
            )]);

            vfs.local_dirty_flush_session(ino)
                .commit_pending_slice_snapshot(pending_slices, 1, Some(stale_attr))
                .await
                .expect("commit stale pending slice with dirty attr");

            let persisted = meta.get_attr(ino).expect("persisted attr");
            assert_eq!(
                persisted.size, 0,
                "stale pending slice commits must not restore an older EOF"
            );
            assert_eq!(
                persisted.content_epoch, 2,
                "stale pending slice commits must preserve the newer content epoch"
            );
            let spans = meta
                .lookup_regular_file_slices(ino, 0, u64::from(block_size))
                .expect("lookup slices after stale commit");
            assert!(
                spans.iter().all(|span| span.payload.is_none()),
                "stale pending slice commits must not install visible payload slices: {spans:?}"
            );
            assert!(
                vfs.pending_uploads_for_inode(ino).is_empty(),
                "stale pending slice commits must not enqueue uploads"
            );
            assert!(
                !vfs.local_payload_backend().contains(stale_object_id),
                "stale pending slice commits must remove uncommitted local payloads"
            );
        });
    }

    #[test]
    fn subtract_pending_local_dirty_bytes_saturates_without_panicking() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-bytes-underflow");
        let _guard = rt.enter();
        vfs.state
            .local_dirty
            .pending_bytes
            .store(1, Ordering::Relaxed);

        vfs.subtract_pending_local_dirty_bytes(2);

        assert_eq!(
            vfs.state.local_dirty.pending_bytes.load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn commit_pending_local_dirty_slices_trims_committed_prefix() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-slice-prefix");
        let ino = 77;
        let chunk_id = 0;
        let original = PendingDirtySlice::persisted(
            studiofs_meta::schema::SliceRange::new(0, 4096).expect("slice range"),
            3,
            0,
            11,
            7001,
            0,
        );
        vfs.state
            .local_dirty
            .pending_chunk_slices
            .lock()
            .entry(ino)
            .or_default()
            .insert(chunk_id, vec![original]);

        let committed_prefix = original.prefix(1024).expect("prefix slice");
        let committed = std::collections::BTreeMap::from([(chunk_id, vec![committed_prefix])]);

        vfs.commit_pending_local_dirty_slices(ino, &committed);

        {
            let pending = vfs.state.local_dirty.pending_chunk_slices.lock();
            let remaining = pending
                .get(&ino)
                .and_then(|chunks| chunks.get(&chunk_id))
                .expect("remaining suffix");
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].range.start, 1024);
            assert_eq!(remaining[0].range.end, 4096);
            assert_eq!(remaining[0].block_offset, 1024);
            assert_eq!(remaining[0].object_offset, 1024);
            assert_eq!(remaining[0].seq, original.seq);
            assert_eq!(remaining[0].object_id, original.object_id);
        }

        vfs.commit_pending_local_dirty_slices(ino, &committed);

        {
            let pending = vfs.state.local_dirty.pending_chunk_slices.lock();
            let remaining = pending
                .get(&ino)
                .and_then(|chunks| chunks.get(&chunk_id))
                .expect("prefix should not match suffix again");
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].range.start, 1024);
            assert_eq!(remaining[0].range.end, 4096);
        }
    }

    #[test]
    fn pending_persisted_slice_byte_counter_tracks_insert_trim_and_clear() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-slice-byte-counter");
        let ino = 80;
        let chunk_id = 0;
        vfs.install_persisted_pending_local_dirty_slice(
            ino,
            PersistedPendingLocalDirtySliceSpec {
                block_index: 3,
                file_offset: 0,
                len: 4096,
                seq: 11,
                object_id: 7001,
                initial_object_offset: 0,
            },
        );

        assert_eq!(vfs.total_pending_persisted_local_dirty_slice_bytes(), 4096);

        let committed_prefix = PendingDirtySlice::persisted(
            studiofs_meta::schema::SliceRange::new(0, 1024).expect("slice range"),
            3,
            0,
            11,
            7001,
            0,
        );
        let committed = std::collections::BTreeMap::from([(chunk_id, vec![committed_prefix])]);
        vfs.commit_pending_local_dirty_slices(ino, &committed);

        assert_eq!(vfs.total_pending_persisted_local_dirty_slice_bytes(), 3072);

        vfs.clear_pending_dirty_slices_for_inode(ino);

        assert_eq!(vfs.total_pending_persisted_local_dirty_slice_bytes(), 0);
    }

    #[test]
    fn pending_slice_counters_track_insert_commit_and_clear() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-slice-counters");
        let ino = 81;
        let chunk_id = 0;
        let persisted = PendingDirtySlice::persisted(
            studiofs_meta::schema::SliceRange::new(0, 4096).expect("slice range"),
            3,
            0,
            11,
            7001,
            0,
        );
        let buffered = PendingDirtySlice {
            range: studiofs_meta::schema::SliceRange::new(5000, 6000).expect("slice range"),
            block_index: 3,
            block_offset: 4096,
            seq: 12,
            object_id: None,
            object_offset: 0,
            created_at: Instant::now(),
        };

        vfs.insert_pending_local_dirty_slice_entries(
            ino,
            vec![(chunk_id, persisted), (chunk_id, buffered)],
        );

        assert_eq!(vfs.pending_local_dirty_slice_counts(), (2, 1));

        let committed_prefix = persisted.prefix(1024).expect("prefix slice");
        let committed = std::collections::BTreeMap::from([(chunk_id, vec![committed_prefix])]);
        vfs.commit_pending_local_dirty_slices(ino, &committed);

        assert_eq!(vfs.pending_local_dirty_slice_counts(), (2, 1));

        let committed_rest = PendingDirtySlice::persisted(
            studiofs_meta::schema::SliceRange::new(1024, 4096).expect("slice range"),
            3,
            1024,
            11,
            7001,
            1024,
        );
        let committed = std::collections::BTreeMap::from([(chunk_id, vec![committed_rest])]);
        vfs.commit_pending_local_dirty_slices(ino, &committed);

        assert_eq!(vfs.pending_local_dirty_slice_counts(), (1, 0));

        vfs.clear_pending_dirty_slices_for_inode(ino);

        assert_eq!(vfs.pending_local_dirty_slice_counts(), (0, 0));
    }

    #[test]
    fn opportunistic_pending_slice_commit_waits_for_chunk_sized_wave_while_tail_is_open() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-slice-chunk-wave");
        let _guard = rt.enter();
        let ino = rt.block_on(async {
            let attr = vfs
                .create(1, b"pending-slice-chunk-wave.txt", 0, 0, 0o644)
                .expect("create file");
            let fh = vfs
                .open_file(attr.ino, OpenFlags::RDWR, false)
                .expect("open file");
            vfs.write_fh(fh, attr.ino, 0, b"tail")
                .await
                .expect("seed open writer tail");
            attr.ino
        });

        let block_size = vfs.block_size as usize;
        let chunk_size = studiofs_meta::schema::DEFAULT_CHUNK_SIZE as usize;
        let blocks_per_chunk = chunk_size / block_size;
        let moderately_aged_created_at = Instant::now()
            .checked_sub(Duration::from_millis(250))
            .expect("old instant");
        let mut persisted = std::collections::BTreeMap::<u64, Vec<PendingDirtySlice>>::new();
        for block in 0..blocks_per_chunk.saturating_sub(1) {
            let start = u32::try_from(block * block_size).expect("slice start");
            let end = start.saturating_add(vfs.block_size);
            persisted.entry(0).or_default().push(PendingDirtySlice {
                range: studiofs_meta::schema::SliceRange::new(start, end).expect("slice range"),
                block_index: block as u32,
                block_offset: 0,
                seq: block as u64 + 1,
                object_id: Some(10_000 + block as u64),
                object_offset: 0,
                created_at: moderately_aged_created_at,
            });
        }

        let selected = vfs.select_pending_persisted_local_dirty_slice_commit_wave(
            ino,
            persisted.clone(),
            LocalSyncPolicy::Opportunistic,
        );
        assert!(
            selected.is_empty(),
            "sub-chunk persisted blocks should wait for a JuiceFS-style chunk-sized metadata wave while an open writer tail remains"
        );

        let start = u32::try_from((blocks_per_chunk - 1) * block_size).expect("slice start");
        persisted.entry(0).or_default().push(PendingDirtySlice {
            range: studiofs_meta::schema::SliceRange::new(
                start,
                start.saturating_add(vfs.block_size),
            )
            .expect("slice range"),
            block_index: (blocks_per_chunk - 1) as u32,
            block_offset: 0,
            seq: blocks_per_chunk as u64,
            object_id: Some(10_000 + blocks_per_chunk as u64),
            object_offset: 0,
            created_at: moderately_aged_created_at,
        });

        let selected = vfs.select_pending_persisted_local_dirty_slice_commit_wave(
            ino,
            persisted,
            LocalSyncPolicy::Opportunistic,
        );
        let selected_bytes: usize = selected
            .values()
            .flat_map(|slices| slices.iter())
            .map(|slice| slice.len())
            .sum();
        assert_eq!(
            selected_bytes, chunk_size,
            "a full 64MiB chunk worth of persisted 4MiB blocks should commit in one metadata wave"
        );
    }

    #[test]
    fn opportunistic_pending_slice_commit_never_splits_persisted_payload_objects() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-slice-whole-objects");
        let _guard = rt.enter();
        let ino = rt.block_on(async {
            let attr = vfs
                .create(1, b"pending-slice-whole-objects.bin", 0, 0, 0o644)
                .expect("create file");
            let fh = vfs
                .open_file(attr.ino, OpenFlags::RDWR, false)
                .expect("open file");
            vfs.write_fh(fh, attr.ino, 0, b"tail")
                .await
                .expect("seed open writer tail");
            attr.ino
        });

        let block_size = vfs.block_size as usize;
        let first_len = block_size / 4;
        let created_at = Instant::now()
            .checked_sub(Duration::from_millis(250))
            .expect("old instant");
        let mut persisted = std::collections::BTreeMap::<u64, Vec<PendingDirtySlice>>::new();
        persisted.entry(1).or_default().push(PendingDirtySlice {
            range: studiofs_meta::schema::SliceRange::new(
                0,
                u32::try_from(first_len).expect("first slice len"),
            )
            .expect("first slice range"),
            block_index: 16,
            block_offset: 0,
            seq: 1,
            object_id: Some(40_000),
            object_offset: 0,
            created_at,
        });
        for index in 0..16usize {
            let start = u32::try_from(index * block_size).expect("slice start");
            persisted.entry(0).or_default().push(PendingDirtySlice {
                range: studiofs_meta::schema::SliceRange::new(start, start + vfs.block_size)
                    .expect("slice range"),
                block_index: u32::try_from(index).expect("block index"),
                block_offset: 0,
                seq: index as u64 + 2,
                object_id: Some(40_001 + index as u64),
                object_offset: 0,
                created_at,
            });
        }

        let selected = vfs.select_pending_persisted_local_dirty_slice_commit_wave(
            ino,
            persisted,
            LocalSyncPolicy::Opportunistic,
        );
        let selected_lengths = selected
            .values()
            .flat_map(|slices| slices.iter())
            .map(|slice| slice.len())
            .collect::<Vec<_>>();

        assert!(selected_lengths.contains(&first_len));
        assert!(
            selected_lengths
                .iter()
                .all(|len| *len == first_len || *len == block_size),
            "commit waves must retain or commit an immutable payload object as a whole: {selected_lengths:?}"
        );
    }

    #[test]
    fn opportunistic_pending_slice_commit_keeps_cross_chunk_object_atomic() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-cross-chunk-object");
        let _guard = rt.enter();
        let ino = rt.block_on(async {
            let attr = vfs
                .create(1, b"pending-cross-chunk-object.bin", 0, 0, 0o644)
                .expect("create file");
            let fh = vfs
                .open_file(attr.ino, OpenFlags::RDWR, false)
                .expect("open file");
            vfs.write_fh(fh, attr.ino, 0, b"tail")
                .await
                .expect("seed open writer tail");
            attr.ino
        });

        let block_size = vfs.block_size;
        let chunk_size = studiofs_meta::schema::DEFAULT_CHUNK_SIZE;
        let mut persisted = std::collections::BTreeMap::<u64, Vec<PendingDirtySlice>>::new();
        for index in 0..15u32 {
            let start = index * block_size;
            persisted.entry(0).or_default().push(PendingDirtySlice {
                range: studiofs_meta::schema::SliceRange::new(start, start + block_size)
                    .expect("full block range"),
                block_index: index,
                block_offset: 0,
                seq: u64::from(index) + 1,
                object_id: Some(50_000 + u64::from(index)),
                object_offset: 0,
                created_at: Instant::now(),
            });
        }
        let three_mib = 3 * 1024 * 1024;
        let sixty_mib = 60 * 1024 * 1024;
        persisted.entry(0).or_default().push(PendingDirtySlice {
            range: studiofs_meta::schema::SliceRange::new(sixty_mib, sixty_mib + three_mib)
                .expect("63 MiB prefix tail"),
            block_index: 15,
            block_offset: 0,
            seq: 16,
            object_id: Some(50_015),
            object_offset: 0,
            created_at: Instant::now(),
        });

        let cross_chunk_object = 60_000;
        persisted.entry(0).or_default().push(PendingDirtySlice {
            range: studiofs_meta::schema::SliceRange::new(chunk_size - 1024 * 1024, chunk_size)
                .expect("cross-chunk first slice"),
            block_index: 16,
            block_offset: 0,
            seq: cross_chunk_object,
            object_id: Some(cross_chunk_object),
            object_offset: 0,
            created_at: Instant::now(),
        });
        persisted.entry(1).or_default().push(PendingDirtySlice {
            range: studiofs_meta::schema::SliceRange::new(0, 2 * 1024 * 1024)
                .expect("cross-chunk second slice"),
            block_index: 16,
            block_offset: 1024 * 1024,
            seq: cross_chunk_object,
            object_id: Some(cross_chunk_object),
            object_offset: 1024 * 1024,
            created_at: Instant::now(),
        });

        let selected = vfs.select_pending_persisted_local_dirty_slice_commit_wave(
            ino,
            persisted,
            LocalSyncPolicy::Opportunistic,
        );
        let selected_cross_chunk_slice_count = selected
            .values()
            .flat_map(|slices| slices.iter())
            .filter(|slice| slice.object_id == Some(cross_chunk_object))
            .count();

        assert!(
            selected_cross_chunk_slice_count == 0 || selected_cross_chunk_slice_count == 2,
            "commit waves must keep every slice of one immutable object together"
        );
    }

    #[test]
    fn opportunistic_pending_slice_commit_drains_aged_tail_when_upload_pipeline_is_idle() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-slice-idle-tail");
        let _guard = rt.enter();
        let ino = rt.block_on(async {
            let attr = vfs
                .create(1, b"pending-slice-idle-tail.txt", 0, 0, 0o644)
                .expect("create file");
            let fh = vfs
                .open_file(attr.ino, OpenFlags::RDWR, false)
                .expect("open file");
            vfs.write_fh(fh, attr.ino, 0, b"tail")
                .await
                .expect("seed open writer tail");
            attr.ino
        });

        let block_size = vfs.block_size as usize;
        let chunk_size = studiofs_meta::schema::DEFAULT_CHUNK_SIZE as usize;
        let aged_created_at = Instant::now()
            .checked_sub(PENDING_SLICE_COMMIT_MAX_AGE + Duration::from_secs(1))
            .expect("old instant");
        let mut persisted = std::collections::BTreeMap::<u64, Vec<PendingDirtySlice>>::new();
        let blocks = (chunk_size + (chunk_size / 2)) / block_size;
        for block in 0..blocks {
            let absolute_offset = block * block_size;
            let chunk_id = (absolute_offset / chunk_size) as u64;
            let start = u32::try_from(absolute_offset % chunk_size).expect("slice start");
            persisted
                .entry(chunk_id)
                .or_default()
                .push(PendingDirtySlice {
                    range: studiofs_meta::schema::SliceRange::new(
                        start,
                        start.saturating_add(vfs.block_size),
                    )
                    .expect("slice range"),
                    block_index: block as u32,
                    block_offset: 0,
                    seq: block as u64 + 1,
                    object_id: Some(20_000 + block as u64),
                    object_offset: 0,
                    created_at: aged_created_at,
                });
        }

        let selected = vfs.select_pending_persisted_local_dirty_slice_commit_wave(
            ino,
            persisted,
            LocalSyncPolicy::Opportunistic,
        );
        let selected_bytes: usize = selected
            .values()
            .flat_map(|slices| slices.iter())
            .map(|slice| slice.len())
            .sum();

        assert_eq!(
            selected_bytes,
            chunk_size + (chunk_size / 2),
            "aged persisted tail should drain in one wave once uploads are idle"
        );
    }

    #[test]
    fn opportunistic_pending_slice_commit_drains_time_aged_tail_before_five_seconds() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-slice-short-tail-age");
        let _guard = rt.enter();
        let ino = rt.block_on(async {
            let attr = vfs
                .create(1, b"pending-slice-short-tail-age.txt", 0, 0, 0o644)
                .expect("create file");
            let fh = vfs
                .open_file(attr.ino, OpenFlags::RDWR, false)
                .expect("open file");
            vfs.write_fh(fh, attr.ino, 0, b"tail")
                .await
                .expect("seed open writer tail");
            attr.ino
        });

        let aged_created_at = Instant::now()
            .checked_sub(Duration::from_millis(1500))
            .expect("old instant");
        let persisted = std::collections::BTreeMap::from([(
            0,
            vec![PendingDirtySlice {
                range: studiofs_meta::schema::SliceRange::new(0, vfs.block_size)
                    .expect("slice range"),
                block_index: 0,
                block_offset: 0,
                seq: 1,
                object_id: Some(30_000),
                object_offset: 0,
                created_at: aged_created_at,
            }],
        )]);

        let selected = vfs.select_pending_persisted_local_dirty_slice_commit_wave(
            ino,
            persisted,
            LocalSyncPolicy::Opportunistic,
        );

        assert!(
            !selected.is_empty(),
            "time-aged persisted tail should commit before a five-second burst builds up"
        );
    }

    #[test]
    fn buffered_write_spill_batches_respect_cloud_object_capacity() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "spill-object-boundaries");
        let block_size = vfs.block_size as usize;
        let blocks = vec![
            DirtyWriteSnapshot::from_shared(0, Bytes::from(vec![0xA1; block_size])),
            DirtyWriteSnapshot::from_shared(1, Bytes::from(vec![0xB2; block_size])),
        ];

        let batches = vfs.build_dirty_write_snapshot_slice_spill_batches(&blocks);

        assert_eq!(
            batches.len(),
            2,
            "default logical blocks already fill one cloud object each"
        );
        for (block_index, batch) in batches.iter().enumerate() {
            assert_eq!(batch.payload.len(), block_size);
            assert_eq!(batch.segments.len(), 1);
            assert_eq!(batch.segments[0].block_index, block_index as u32);
            assert_eq!(batch.segments[0].object_offset, 0);
        }
    }

    #[test]
    #[should_panic(expected = "pending_slice_count underflow")]
    fn pending_slice_counter_underflow_is_caught_in_debug_builds() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-slice-counter-underflow");

        vfs.subtract_pending_slice_counts(1, 0);
    }

    #[test]
    #[should_panic(expected = "pending_persisted_slice_count underflow")]
    fn pending_persisted_slice_counter_underflow_is_caught_in_debug_builds() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) =
            make_unstarted_test_vfs(&tmp, "pending-persisted-slice-counter-underflow");

        vfs.subtract_pending_slice_counts(0, 1);
    }

    #[test]
    fn trim_local_payload_staging_trims_and_drops_pending_slices() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "trim-pending-slices");
        let ino = 78;
        let block_index = 3;
        let chunk_id = 0;

        vfs.replace_pending_local_dirty_payload(
            ino,
            block_index,
            PendingPayload::buffered(BytesMut::from(&b"abcdefgh"[..]), None),
        );
        vfs.insert_pending_local_dirty_slice_entries(
            ino,
            vec![
                (
                    chunk_id,
                    PendingDirtySlice {
                        range: studiofs_meta::schema::SliceRange::new(100, 106)
                            .expect("slice range"),
                        block_index,
                        block_offset: 2,
                        seq: 10,
                        object_id: None,
                        object_offset: 0,
                        created_at: Instant::now(),
                    },
                ),
                (
                    chunk_id,
                    PendingDirtySlice {
                        range: studiofs_meta::schema::SliceRange::new(200, 202)
                            .expect("slice range"),
                        block_index,
                        block_offset: 5,
                        seq: 11,
                        object_id: None,
                        object_offset: 0,
                        created_at: Instant::now(),
                    },
                ),
            ],
        );

        let changed = vfs
            .trim_local_payload_staging(ino, block_index, 5)
            .expect("trim payload staging");
        assert!(changed, "payload bytes should be truncated");
        assert_eq!(
            vfs.state
                .local_dirty
                .pending_payloads
                .lock()
                .get(&(ino, block_index))
                .expect("pending payload")
                .data
                .len(),
            5
        );

        let pending = vfs.state.local_dirty.pending_chunk_slices.lock();
        let remaining = pending
            .get(&ino)
            .and_then(|chunks| chunks.get(&chunk_id))
            .expect("trimmed slice should remain");
        assert_eq!(remaining.len(), 1, "slice beyond new len should be dropped");
        assert_eq!(remaining[0].range.start, 100);
        assert_eq!(remaining[0].range.end, 103);
        assert_eq!(remaining[0].block_offset, 2);
    }

    #[test]
    fn snapshot_pending_dirty_slice_read_spans_handles_boundaries_and_offsets() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "snapshot-pending-slices");
        let ino = 79;
        let block_index = 4;
        let chunk_id = 0;

        vfs.state.local_dirty.pending_payloads.lock().insert(
            (ino, block_index),
            PendingPayload::buffered(BytesMut::from(&b"0123456789"[..]), None),
        );
        vfs.state
            .local_dirty
            .pending_chunk_slices
            .lock()
            .entry(ino)
            .or_default()
            .insert(
                chunk_id,
                vec![PendingDirtySlice {
                    range: studiofs_meta::schema::SliceRange::new(100, 106).expect("slice range"),
                    block_index,
                    block_offset: 2,
                    seq: 7,
                    object_id: None,
                    object_offset: 0,
                    created_at: Instant::now(),
                }],
            );

        assert!(
            vfs.snapshot_pending_dirty_slice_read_spans(ino, 94, 6)
                .is_empty(),
            "request ending at slice start should not overlap"
        );
        assert!(
            vfs.snapshot_pending_dirty_slice_read_spans(ino, 106, 4)
                .is_empty(),
            "request starting at slice end should not overlap"
        );

        let spans = vfs.snapshot_pending_dirty_slice_read_spans(ino, 102, 10);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].start, 102);
        assert_eq!(spans[0].end, 106);
        assert_eq!(spans[0].block_offset, 4);
        assert_eq!(spans[0].seq, 7);
        assert_eq!(spans[0].data.as_ref(), b"0123456789");
    }

    #[test]
    fn assign_reserved_object_ids_reuses_existing_ids_and_stores_new_ones() {
        let ino = 42;
        let mut pending = std::collections::HashMap::from([
            (
                (ino, 0),
                PendingPayload::buffered(BytesMut::from(&b"alpha"[..]), Some(111)),
            ),
            (
                (ino, 1),
                PendingPayload::buffered(BytesMut::from(&b"beta"[..]), None),
            ),
        ]);
        let mut blocks = vec![
            PersistedPayloadEntry::from_payload(0, 0, Bytes::from_static(b"alpha")),
            PersistedPayloadEntry::from_payload(1, 0, Bytes::from_static(b"beta")),
        ];

        assign_reserved_object_ids(&mut pending, ino, &mut blocks, &[222]);

        assert_eq!(blocks[0].object_id, 111);
        assert_eq!(blocks[1].object_id, 222);
        assert_eq!(
            pending.get(&(ino, 0)).unwrap().reserved_object_id,
            Some(111)
        );
        assert_eq!(
            pending.get(&(ino, 1)).unwrap().reserved_object_id,
            Some(222)
        );
    }

    #[test]
    fn commit_persisted_payload_state_clears_reserved_id_without_dropping_newer_bytes() {
        let ino = 7;
        let mut pending = std::collections::HashMap::from([(
            (ino, 3),
            PendingPayload::buffered(BytesMut::from(&b"newer"[..]), Some(333)),
        )]);
        let blocks = vec![PersistedPayloadEntry::from_payload(
            3,
            333,
            Bytes::from_static(b"older"),
        )];

        let (removed_bytes, removed_block_indexes, removed_buffers) =
            commit_persisted_payload_state(&mut pending, ino, &blocks);

        assert_eq!(removed_bytes, 0);
        assert!(removed_block_indexes.is_empty());
        assert!(removed_buffers.is_empty());
        let pending_payload = pending.get(&(ino, 3)).unwrap();
        assert_eq!(pending_payload.data.as_ref(), b"newer");
        assert_eq!(pending_payload.reserved_object_id, None);
    }

    #[test]
    fn commit_persisted_payload_state_removes_matching_local_extent_after_metadata_commit() {
        let ino = 8;
        let mut pending =
            std::collections::HashMap::from([((ino, 4), PendingPayload::local_extent(444, 4096))]);
        let blocks = vec![PersistedPayloadEntry::from_local_extent(4, 444, 4096)];

        let (removed_bytes, removed_block_indexes, removed_buffers) =
            commit_persisted_payload_state(&mut pending, ino, &blocks);

        assert_eq!(removed_bytes, 0);
        assert_eq!(removed_block_indexes, vec![4]);
        assert!(removed_buffers.is_empty());
        assert!(
            !pending.contains_key(&(ino, 4)),
            "matching local extents should disappear once metadata commit catches up"
        );
    }

    #[derive(Clone, Default)]
    struct EventLog {
        entries: Arc<StdMutex<Vec<String>>>,
    }

    impl EventLog {
        fn push(&self, entry: impl Into<String>) {
            self.entries
                .lock()
                .expect("event log lock")
                .push(entry.into());
        }

        fn snapshot(&self) -> Vec<String> {
            self.entries.lock().expect("event log lock").clone()
        }
    }

    struct PanicBlockIds;

    impl BlockIdAllocatorPort for PanicBlockIds {
        async fn next_object_ids(&self, _count: u32) -> VfsResult<Vec<u64>> {
            panic!("object ID allocator should not be used in this test")
        }
    }

    struct TestLocalPayloadWriter {
        events: EventLog,
        fail_persist: bool,
    }

    impl LocalPayloadWriterPort for TestLocalPayloadWriter {
        async fn persist_payload(
            &self,
            _persisted_blocks: &[PersistedPayloadEntry],
            _persist_config: PayloadPersistConfig,
        ) -> VfsResult<()> {
            self.events.push("payload");
            if self.fail_persist {
                Err(VfsError::Io(std::io::Error::other("payload failure")))
            } else {
                Ok(())
            }
        }

        async fn cleanup_uncommitted(
            &self,
            _ino: Ino,
            _blocks: &[PersistedPayloadEntry],
            stage: &'static str,
        ) {
            self.events.push(format!("cleanup:{stage}"));
        }
    }

    struct TestOverlayCommitter {
        events: EventLog,
        fail_commit: bool,
        committed_attr: Option<InodeAttr>,
        committed_payloads: SharedCommittedPayloads,
    }

    impl OverlayCommitPort for TestOverlayCommitter {
        async fn commit_overlay(
            &self,
            _ino: Ino,
            _chunk_slices: Vec<(
                studiofs_meta::schema::ChunkId,
                Vec<studiofs_meta::schema::SliceRecord>,
            )>,
            _attr: Option<InodeAttr>,
        ) -> VfsResult<Option<InodeAttr>> {
            self.events.push("metadata");
            self.committed_payloads.lock().expect("commit lock").push(
                _chunk_slices
                    .iter()
                    .flat_map(|(_, records)| records)
                    .filter_map(|record| record.payload.as_ref().map(|payload| payload.object_id))
                    .collect(),
            );
            if self.fail_commit {
                Err(VfsError::Io(std::io::Error::other("metadata failure")))
            } else {
                Ok(self.committed_attr.clone())
            }
        }
    }

    struct TestUploadSubmitter {
        events: EventLog,
        uploads: SharedUploads,
    }

    impl UploadSubmissionPort for TestUploadSubmitter {
        async fn submit_uploads(
            &self,
            ino: Ino,
            persisted_blocks: &[PersistedPayloadEntry],
            content_epoch: u64,
        ) {
            self.events.push("upload");
            let ops: Vec<_> = persisted_blocks
                .iter()
                .map(|block| {
                    (
                        ino,
                        block.block_index as u64,
                        block.object_id,
                        content_epoch,
                    )
                })
                .collect();
            self.uploads.lock().expect("uploads lock").extend(ops);
        }
    }

    struct TestDrainObserver {
        events: EventLog,
        committed_ino: SharedInodes,
    }

    impl DrainCommitObserverPort for TestDrainObserver {
        fn on_metadata_committed(&self, ino: Ino, _committed_attr: Option<&InodeAttr>) {
            self.events.push("observer");
            self.committed_ino.lock().expect("observer lock").push(ino);
        }
    }

    fn test_drain(
        events: EventLog,
        fail_persist: bool,
        fail_commit: bool,
        committed_attr: Option<InodeAttr>,
        uploads: SharedUploads,
        observed_inodes: SharedInodes,
    ) -> LocalDirtyDrainCore<
        TestLocalPayloadWriter,
        TestOverlayCommitter,
        PanicBlockIds,
        TestUploadSubmitter,
        TestDrainObserver,
    > {
        test_drain_recording_commits(
            events,
            fail_persist,
            fail_commit,
            committed_attr,
            uploads,
            observed_inodes,
            Arc::new(StdMutex::new(Vec::new())),
        )
    }

    fn test_drain_recording_commits(
        events: EventLog,
        fail_persist: bool,
        fail_commit: bool,
        committed_attr: Option<InodeAttr>,
        uploads: SharedUploads,
        observed_inodes: SharedInodes,
        committed_payloads: SharedCommittedPayloads,
    ) -> LocalDirtyDrainCore<
        TestLocalPayloadWriter,
        TestOverlayCommitter,
        PanicBlockIds,
        TestUploadSubmitter,
        TestDrainObserver,
    > {
        LocalDirtyDrainCore {
            local_payloads: TestLocalPayloadWriter {
                events: events.clone(),
                fail_persist,
            },
            commit_meta: TestOverlayCommitter {
                events: events.clone(),
                fail_commit,
                committed_attr,
                committed_payloads,
            },
            object_ids: PanicBlockIds,
            uploads: TestUploadSubmitter {
                events: events.clone(),
                uploads,
            },
            observer: TestDrainObserver {
                events,
                committed_ino: observed_inodes,
            },
            block_size: 4096,
            put_concurrency: 4,
        }
    }

    #[test]
    fn drain_core_orders_payload_then_metadata_then_upload() {
        let ino = 11;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(
            events.clone(),
            false,
            false,
            None,
            uploads.clone(),
            observed_inodes.clone(),
        );
        let (mut pending, blocks) = single_block_fixture(ino, 0, b"alpha", None);
        let mut fresh_object_ids = std::collections::VecDeque::from([111]);
        let persisted_blocks = drain
            .try_prepare_persisted_blocks(&mut pending, ino, blocks, &mut fresh_object_ids)
            .expect("prepare persisted payloads");

        futures::executor::block_on(drain.execute_drain(
            ino,
            persisted_blocks,
            std::collections::BTreeMap::new(),
            9,
            None,
            PayloadPersistConfig {
                put_concurrency: 4,
                local_sync_policy: LocalSyncPolicy::Opportunistic,
            },
        ))
        .expect("drain succeeds");

        assert_eq!(
            pending
                .get(&(ino, 0))
                .and_then(|block| block.reserved_object_id),
            Some(111)
        );
        assert_eq!(
            events.snapshot(),
            vec![
                "payload".to_string(),
                "metadata".to_string(),
                "observer".to_string(),
                "upload".to_string(),
            ]
        );
        assert_eq!(
            *uploads.lock().expect("uploads lock"),
            vec![(ino, 0, 111, 9)]
        );
        assert_eq!(*observed_inodes.lock().expect("observer lock"), vec![ino]);
    }

    #[test]
    fn drain_core_cleans_up_after_metadata_failure() {
        let ino = 12;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(
            events.clone(),
            false,
            true,
            None,
            uploads.clone(),
            observed_inodes.clone(),
        );
        let (mut pending, blocks) = single_block_fixture(ino, 1, b"beta", None);
        let mut fresh_object_ids = std::collections::VecDeque::from([222]);
        let persisted_blocks = drain
            .try_prepare_persisted_blocks(&mut pending, ino, blocks, &mut fresh_object_ids)
            .expect("prepare persisted payloads");

        let error = futures::executor::block_on(drain.execute_drain(
            ino,
            persisted_blocks,
            std::collections::BTreeMap::new(),
            5,
            None,
            PayloadPersistConfig {
                put_concurrency: 4,
                local_sync_policy: LocalSyncPolicy::Opportunistic,
            },
        ))
        .expect_err("drain should fail");

        assert!(
            matches!(error, VfsError::Io(_)),
            "expected metadata failure to bubble up"
        );
        assert_eq!(
            events.snapshot(),
            vec![
                "payload".to_string(),
                "metadata".to_string(),
                "cleanup:metadata-commit".to_string(),
            ]
        );
        assert!(uploads.lock().expect("uploads lock").is_empty());
        assert!(observed_inodes.lock().expect("observer lock").is_empty());
    }

    #[test]
    fn prepare_persisted_blocks_requests_more_ids_when_snapshot_goes_stale() {
        let ino = 21;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(events, false, false, None, uploads, observed_inodes);
        let (mut pending, blocks) = single_block_fixture(ino, 0, b"alpha", Some(700));
        let mut fresh_object_ids = std::collections::VecDeque::new();

        pending.insert(
            (ino, 0),
            PendingPayload::buffered(BytesMut::from(&b"newer"[..]), Some(700)),
        );

        let (blocks, additional_needed) = drain
            .try_prepare_persisted_blocks(&mut pending, ino, blocks, &mut fresh_object_ids)
            .expect_err("stale snapshot should request another object id");
        assert_eq!(additional_needed, 1);

        fresh_object_ids.push_back(701);
        let persisted_blocks = drain
            .try_prepare_persisted_blocks(&mut pending, ino, blocks, &mut fresh_object_ids)
            .expect("prepare stale snapshot");
        assert_eq!(persisted_blocks[0].object_id, 701);
        let pending_payload = pending.get(&(ino, 0)).expect("pending payload");
        assert_eq!(pending_payload.data.as_ref(), b"newer");
        assert_eq!(pending_payload.reserved_object_id, Some(700));
    }

    #[test]
    fn prepare_persisted_blocks_freezes_snapshot_without_copying_payload() {
        let ino = 22;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(events, false, false, None, uploads, observed_inodes);
        let (mut pending, blocks) = single_block_fixture(ino, 0, b"alpha", None);
        let mut fresh_object_ids = std::collections::VecDeque::from([702]);
        let snapshot_ptr = blocks[0].as_ref().as_ptr();

        let persisted_blocks = drain
            .try_prepare_persisted_blocks(&mut pending, ino, blocks, &mut fresh_object_ids)
            .expect("prepare persisted payloads");

        assert_eq!(
            persisted_blocks[0]
                .payload_bytes()
                .expect("prepared block keeps inline payload")
                .as_ptr(),
            snapshot_ptr
        );
    }

    #[test]
    fn drain_core_commits_attr_only_without_uploading() {
        let ino = 13;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(
            events.clone(),
            false,
            false,
            Some(InodeAttr::new_file(ino, 1, 1000, 1000, 0o644)),
            uploads.clone(),
            observed_inodes.clone(),
        );

        let outcome = futures::executor::block_on(drain.execute_drain(
            ino,
            Vec::new(),
            std::collections::BTreeMap::new(),
            4,
            Some(InodeAttr::new_file(ino, 1, 1000, 1000, 0o644)),
            PayloadPersistConfig {
                put_concurrency: 4,
                local_sync_policy: LocalSyncPolicy::Opportunistic,
            },
        ))
        .expect("attr-only drain succeeds");

        assert!(outcome.persisted_blocks.is_empty());
        assert_eq!(
            events.snapshot(),
            vec!["metadata".to_string(), "observer".to_string()]
        );
        assert!(uploads.lock().expect("uploads lock").is_empty());
        assert_eq!(*observed_inodes.lock().expect("observer lock"), vec![ino]);
    }

    #[test]
    fn drain_core_commits_persisted_pending_slices_without_new_payloads() {
        let ino = 14;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let committed_payloads = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain_recording_commits(
            events.clone(),
            false,
            false,
            None,
            uploads.clone(),
            observed_inodes.clone(),
            committed_payloads.clone(),
        );
        let mut dirty_chunk_slices = std::collections::BTreeMap::new();
        dirty_chunk_slices.insert(
            3,
            vec![PendingDirtySlice::persisted(
                studiofs_meta::schema::SliceRange::new(0, 4096).expect("slice range"),
                0,
                0,
                9,
                9014,
                0,
            )],
        );

        let outcome = futures::executor::block_on(drain.execute_drain(
            ino,
            Vec::new(),
            dirty_chunk_slices,
            4,
            None,
            PayloadPersistConfig {
                put_concurrency: 4,
                local_sync_policy: LocalSyncPolicy::Opportunistic,
            },
        ))
        .expect("persisted pending-slice drain succeeds");

        assert!(outcome.persisted_blocks.is_empty());
        assert_eq!(
            *committed_payloads.lock().expect("commit lock"),
            vec![vec![9014]]
        );
        assert_eq!(
            events.snapshot(),
            vec!["metadata".to_string(), "observer".to_string()]
        );
        assert!(uploads.lock().expect("uploads lock").is_empty());
        assert_eq!(*observed_inodes.lock().expect("observer lock"), vec![ino]);
    }

    #[test]
    fn build_overlay_slice_records_splits_blocks_across_chunk_boundaries() {
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let mut drain = test_drain(events, false, false, None, uploads, observed_inodes);
        drain.block_size = 5 * 1024 * 1024;

        let chunk_slices = drain
            .build_overlay_slice_records(
                41,
                &[PersistedPayloadEntry::from_payload(
                    12,
                    9001,
                    Bytes::from(vec![0xAB; drain.block_size as usize]),
                )],
                std::collections::BTreeMap::new(),
            )
            .expect("chunk-aware block split");

        assert_eq!(
            chunk_slices.len(),
            2,
            "cross-chunk payload should split in two"
        );
        let first = &chunk_slices[0].1;
        let second = &chunk_slices[1].1;
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(chunk_slices[0].0, studiofs_meta::schema::ChunkId(0));
        assert_eq!(chunk_slices[1].0, studiofs_meta::schema::ChunkId(1));
        assert_eq!(first[0].range.start, 60 * 1024 * 1024);
        assert_eq!(
            first[0].range.end,
            studiofs_meta::schema::DEFAULT_CHUNK_SIZE
        );
        assert_eq!(first[0].payload.as_ref().expect("payload ref").offset, 0);
        assert_eq!(
            first[0].payload.as_ref().expect("payload ref").length,
            4 * 1024 * 1024
        );
        assert_eq!(second[0].range.start, 0);
        assert_eq!(second[0].range.end, 1024 * 1024);
        assert_eq!(
            second[0].payload.as_ref().expect("payload ref").offset,
            4 * 1024 * 1024
        );
        assert_eq!(
            second[0].payload.as_ref().expect("payload ref").length,
            1024 * 1024
        );
    }

    #[test]
    fn build_overlay_slice_records_orders_pending_slices_within_chunk() {
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(events, false, false, None, uploads, observed_inodes);

        let chunk_id = 7;
        let mut dirty_chunk_slices = std::collections::BTreeMap::new();
        dirty_chunk_slices.insert(
            chunk_id,
            vec![
                PendingDirtySlice::persisted(
                    studiofs_meta::schema::SliceRange::new(4096, 8192).expect("second range"),
                    0,
                    4096,
                    42,
                    7002,
                    4096,
                ),
                PendingDirtySlice::persisted(
                    studiofs_meta::schema::SliceRange::new(0, 4096).expect("first range"),
                    0,
                    0,
                    42,
                    7001,
                    0,
                ),
            ],
        );

        let chunk_slices = drain
            .build_overlay_slice_records(41, &[], dirty_chunk_slices)
            .expect("dirty pending slices become overlay records");

        let records = &chunk_slices[0].1;
        assert_eq!(
            records
                .iter()
                .map(|record| record.range.start)
                .collect::<Vec<_>>(),
            vec![0, 4096],
            "equal-seq slices should commit in chunk-local offset order"
        );
        assert_eq!(
            records
                .iter()
                .map(|record| record.payload.as_ref().expect("payload").object_id)
                .collect::<Vec<_>>(),
            vec![7001, 7002],
            "payload refs should follow the sorted chunk-local slice order"
        );
    }

    #[test]
    fn pending_dirty_slice_order_key_uses_range_end_and_object_id_ties() {
        let first = PendingDirtySlice::persisted(
            studiofs_meta::schema::SliceRange::new(0, 4096).expect("short range"),
            0,
            0,
            42,
            7001,
            4096,
        );
        let second = PendingDirtySlice::persisted(
            studiofs_meta::schema::SliceRange::new(0, 8192).expect("long range"),
            0,
            0,
            42,
            7002,
            0,
        );
        let mut slices = [second, first];

        slices.sort_by_key(|slice| slice.order_key(7));

        assert_eq!(
            slices
                .iter()
                .map(|slice| (slice.range.start, slice.range.end, slice.object_id))
                .collect::<Vec<_>>(),
            vec![(0, 4096, Some(7001)), (0, 8192, Some(7002))],
            "identical seq/start slices should use range end and object id before object offset"
        );
    }

    #[test]
    fn run_bounded_tasks_stops_starting_new_work_after_first_error() {
        let third_started = Arc::new(AtomicBool::new(false));
        let third_started_flag = third_started.clone();
        let (release_tx, release_rx) = futures::channel::oneshot::channel::<()>();
        let release_rx = Arc::new(StdMutex::new(Some(release_rx)));
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            let _ = release_tx.send(());
        });

        let result = futures::executor::block_on(run_bounded_tasks([0u8, 1, 2], 2, move |item| {
            let third_started_flag = third_started_flag.clone();
            let release_rx = release_rx.clone();
            async move {
                match item {
                    0 => Err("first failure"),
                    1 => {
                        let rx = release_rx
                            .lock()
                            .expect("release receiver lock")
                            .take()
                            .expect("release receiver only used once");
                        let _ = rx.await;
                        Ok::<(), &'static str>(())
                    }
                    2 => {
                        third_started_flag.store(true, Ordering::Release);
                        Ok::<(), &'static str>(())
                    }
                    _ => Ok::<(), &'static str>(()),
                }
            }
        }));

        assert_eq!(result, Err("first failure"));
        assert!(
            !third_started.load(Ordering::Acquire),
            "the helper should drain already-started work after the first error without launching more tasks"
        );
    }
}
