use super::*;

impl StudioVfs {
    /// Allocates a normal namespace inode and persists a local scratch entry.
    ///
    /// The caller resolves the parent, checks access and name conflicts, and
    /// supplies the attributes for the new entry. The allocator sets its inode.
    pub fn store_scratch_entry(&self, name: &[u8], mut attr: InodeAttr) -> VfsResult<InodeAttr> {
        self.enforce_metadata_mutation_authority(attr.parent)?;
        attr.ino = self.reserve_namespace_ino()?;
        self.sync_meta()
            .create_scratch_entry(attr.parent, name, &attr)?;
        Ok(attr)
    }

    pub(crate) fn is_scratch_inode(&self, ino: Ino) -> VfsResult<bool> {
        match self.sync_meta().get_scratch_attr(ino) {
            Ok(_) => Ok(true),
            Err(MetaError::InodeNotFound(_)) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Accepts scratch bytes into the mounted writer stack.
    ///
    /// The caller owns namespace and access checks. Like an opportunistic
    /// tracked write, admission applies the dirty-buffer backpressure and the
    /// accepted bytes start background materialization; flush and release use
    /// the same entry points as tracked files.
    pub async fn write_scratch(
        self: &Arc<Self>,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: &[u8],
    ) -> VfsResult<u32> {
        self.enforce_metadata_mutation_authority(ino)?;
        self.maybe_throttle_write_admission(data.len()).await;
        let session = self.file_session_cleanup(ino).with_permit(
            self.state
                .file_session
                .enter_op(ino, FileSessionOperation::Write)
                .await,
        );
        if fh != 0 {
            self.enforce_mutating_handle_access(fh, ino)?;
        }
        let attr = self.sync_meta().get_scratch_attr(ino)?;
        if attr.kind != FileType::RegularFile {
            return Err(MetaError::TargetChanged.into());
        }
        if !data.is_empty() {
            self.local_payload_backend
                .admit_scratch_write()
                .map_err(VfsError::LocalPayload)?;
        }
        let accepted = self
            .state
            .writer_stack
            .write_bytes(
                super::writer_stack::WriterWriteSpec {
                    fh,
                    ino,
                    current_size: attr.size,
                    content_epoch: attr.content_epoch,
                    first_object_id: 0,
                    offset,
                },
                data,
            )
            .map_err(|error| error.into_vfs_error("scratch writer stack write failed"))?;
        drop(session);
        if accepted.accepted_bytes > 0 {
            self.spawn_deferred_materialization_if_needed(ino);
        }
        Ok(accepted.accepted_bytes)
    }

    /// Reads committed scratch bytes for a block. An uncovered block denotes a hole.
    pub async fn read_scratch_block(&self, ino: Ino, block: u32) -> VfsResult<Option<Bytes>> {
        use studiofs_meta::schema::{
            DEFAULT_CHUNK_SIZE, SliceRange, chunk_id_from_offset, chunk_start_offset,
            resolve_chunk_slice_spans,
        };

        // A background overlay commit can retire objects from the old mapping.
        let _session = self.file_session_cleanup(ino).with_permit(
            self.state
                .file_session
                .enter_op(ino, FileSessionOperation::Flush)
                .await,
        );
        let start = u64::from(block) * u64::from(self.block_size);
        let size = self.sync_meta().get_scratch_attr(ino)?.size;
        let end = (start + u64::from(self.block_size)).min(size);
        if start >= end {
            return Ok(None);
        }
        let mut data =
            if let Some(object) = self.sync_meta().get_legacy_scratch_block(ino, block)? {
                let bytes = self.payload_store_get_async(object).await?.ok_or_else(|| {
                    io::Error::other("legacy scratch payload missing in local store")
                })?;
                let mut bytes = bytes.to_vec();
                bytes.resize((end - start) as usize, 0);
                Some(bytes)
            } else {
                None
            };
        let mut cursor = start;
        while cursor < end {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let chunk_end = (chunk_start + u64::from(DEFAULT_CHUNK_SIZE)).min(end);
            if let Some(chunk) = self.sync_meta().get_scratch_overlay(ino, chunk_id)? {
                let range = SliceRange::new(
                    (cursor - chunk_start) as u32,
                    (chunk_end - chunk_start) as u32,
                )
                .expect("nonempty range within a chunk");
                for span in resolve_chunk_slice_spans(chunk_id, range, Some(&chunk), None) {
                    let Some(payload) = span.payload else {
                        continue;
                    };
                    let offset = (chunk_start + u64::from(span.range.start) - start) as usize;
                    let bytes = self.payload_store_get_async(payload.object_id).await?;
                    let bytes = bytes
                        .as_ref()
                        .and_then(|bytes| {
                            bytes.get(
                                payload.offset as usize
                                    ..payload.offset as usize + payload.length as usize,
                            )
                        })
                        .ok_or_else(|| {
                            io::Error::other("scratch payload missing or truncated in local store")
                        })?;
                    let data = data.get_or_insert_with(|| vec![0; (end - start) as usize]);
                    data[offset..offset + bytes.len()].copy_from_slice(bytes);
                }
            }
            cursor = chunk_end;
        }
        Ok(data.map(Bytes::from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_meta::trash::MetaVolumeSettings;
    use studiofs_meta::{MetaEngineCore, MetaGc, MetaInodes};

    const CRASH_ROOT: &str = "STUDIOFS_TEST_SCRATCH_CRASH_ROOT";
    const VOLUME: &str = "scratch-storage";
    const NAME: &[u8] = b"scene.tmp";
    const DATA: &[u8] = b"locally durable data";

    fn scratch_object(meta: &dyn MetaEngineCore, ino: Ino) -> u64 {
        meta.get_scratch_overlay(ino, studiofs_meta::schema::ChunkId(0))
            .unwrap()
            .unwrap()
            .slices
            .values()
            .next()
            .unwrap()
            .payload
            .unwrap()
            .object_id
    }

    #[test]
    fn scratch_storage_survives_unclean_process_exit_without_publication() {
        if let Some(root) = std::env::var_os(CRASH_ROOT) {
            let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs_at(
                Path::new(&root),
                VOLUME,
                // Keep writeback out of the write-path observations below.
                VfsConfig {
                    disable_background_slab_flush: true,
                    ..VfsConfig::default()
                },
                None,
            );
            let settings = meta.volume_settings().unwrap();
            assert!(
                meta.compare_and_set_logical_quota_bytes(settings.revision, Some(1))
                    .unwrap()
                    .is_some()
            );
            let usage = meta.logical_usage(&HashMap::new()).unwrap();
            let events = meta.read_events_range(0, u64::MAX).unwrap().len();
            let pending = vfs.cached_store.pending_upload_count();
            let attr = vfs
                .store_scratch_entry(NAME, InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
                .unwrap();
            assert!(attr.ino < 1 << 63);
            meta.set_scratch_xattr(attr.ino, b"user.test", b"local")
                .unwrap();
            let counters = vfs.cached_store.snapshot_writer_counters();
            let commits = meta.metadata_write_observability().commit.calls;
            let submissions = vfs
                .cached_store
                .cache_pipeline_metrics()
                .upload_queue
                .submitted;
            let syncs = vfs
                .state
                .metrics
                .metadata_sync_barrier_calls
                .load(Ordering::Relaxed);
            runtime
                .block_on(vfs.write_scratch(0, attr.ino, 0, DATA))
                .unwrap();
            let accepted = vfs.cached_store.snapshot_writer_counters();
            assert!(accepted.dirty_bytes > counters.dirty_bytes);
            assert_eq!(accepted.staged_bytes, counters.staged_bytes);
            assert_eq!(meta.metadata_write_observability().commit.calls, commits);
            assert_eq!(
                vfs.state
                    .metrics
                    .metadata_sync_barrier_calls
                    .load(Ordering::Relaxed),
                syncs
            );
            assert_eq!(meta.get_scratch_attr(attr.ino).unwrap().size, 0);
            assert!(
                meta.get_scratch_overlay(attr.ino, studiofs_meta::schema::ChunkId(0))
                    .unwrap()
                    .is_none()
            );
            runtime.block_on(vfs.fsync(attr.ino, false)).unwrap();
            assert_eq!(meta.read_events_range(0, u64::MAX).unwrap().len(), events);
            assert_eq!(vfs.cached_store.pending_upload_count(), pending);
            assert_eq!(
                vfs.cached_store
                    .cache_pipeline_metrics()
                    .upload_queue
                    .submitted,
                submissions
            );
            assert_eq!(meta.logical_usage(&HashMap::new()).unwrap(), usage);
            // Exit immediately with the database and VFS still open.
            std::process::exit(42);
        }

        let temp = studiofs_test_support::workspace_target_tempdir("scratch-crash-");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "vfs::scratch_storage::tests::scratch_storage_survives_unclean_process_exit_without_publication"])
            .env(CRASH_ROOT, temp.path())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(42));

        let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs(&temp, VOLUME);
        let ino = meta.lookup_scratch_entry(ROOT_INO, NAME).unwrap().unwrap();
        assert_eq!(meta.get_scratch_attr(ino).unwrap().size, DATA.len() as u64);
        assert_eq!(
            meta.get_scratch_xattr(ino, b"user.test").unwrap().unwrap(),
            b"local"
        );
        let object = scratch_object(meta.as_ref(), ino);
        assert!(
            meta.get_local_cache_referenced_object_ids()
                .unwrap()
                .contains(&object)
        );
        assert!(
            !meta
                .get_local_referenced_object_ids()
                .unwrap()
                .contains(&object)
        );
        assert_eq!(vfs.cached_store.pending_upload_count(), 0);
        assert_eq!(
            runtime
                .block_on(vfs.read_scratch_block(ino, 0))
                .unwrap()
                .unwrap(),
            DATA
        );
    }

    #[test]
    fn scratch_overwrite_releases_the_displaced_payload() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-overwrite-");
        let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs(&temp, VOLUME);
        let attr = vfs
            .store_scratch_entry(NAME, InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
            .unwrap();
        runtime
            .block_on(vfs.write_scratch(0, attr.ino, 0, DATA))
            .unwrap();
        runtime.block_on(vfs.flush(attr.ino)).unwrap();
        let first = scratch_object(meta.as_ref(), attr.ino);
        runtime
            .block_on(vfs.write_scratch(0, attr.ino, 0, b"second durable data!"))
            .unwrap();

        runtime.block_on(vfs.flush(attr.ino)).unwrap();
        let second = scratch_object(meta.as_ref(), attr.ino);
        assert_ne!(first, second);
        assert!(!vfs.local_payload_backend().contains(first));
        assert!(
            !meta
                .get_local_cache_referenced_object_ids()
                .unwrap()
                .contains(&first)
        );
        assert_eq!(
            runtime
                .block_on(vfs.read_scratch_block(attr.ino, 0))
                .unwrap()
                .unwrap(),
            b"second durable data!".as_slice()
        );
        runtime.block_on(async {
            vfs.write_scratch(0, attr.ino, 7, b"saved").await.unwrap();
            vfs.flush_volatile_writeback_with_outcome(attr.ino, 0)
                .await
                .unwrap();
            vfs.wait_for_deferred_materialization_inflight(attr.ino)
                .await;
            assert_eq!(
                vfs.read_scratch_block(attr.ino, 0).await.unwrap().unwrap(),
                b"second savedle data!".as_slice()
            );
        });
        // The size only grows; SFS-1077 owns truncation.
        assert_eq!(
            meta.get_scratch_attr(attr.ino).unwrap().size,
            DATA.len() as u64
        );
    }

    #[test]
    fn scratch_write_schedules_background_materialization() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-writeback-");
        let (runtime, vfs, _meta) = super::super::tests::make_unstarted_test_vfs(&temp, VOLUME);
        let attr = vfs
            .store_scratch_entry(NAME, InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
            .unwrap();
        let block = vec![7u8; vfs.block_size as usize];
        runtime.block_on(async {
            vfs.write_scratch(0, attr.ino, 0, &block).await.unwrap();
            // Like a tracked write, accepted bytes start writeback without a flush.
            vfs.wait_for_deferred_materialization_inflight(attr.ino)
                .await;
        });
        assert_eq!(vfs.state.writer_stack.dirty_bytes_for_inode(attr.ino), 0);
        assert_eq!(
            vfs.cached_store
                .cache_pipeline_metrics()
                .upload_queue
                .submitted,
            0
        );
    }

    #[test]
    fn scratch_materialization_consumes_prefetched_object_ids() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-prefetch-");
        let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs_at(
            temp.path(),
            VOLUME,
            VfsConfig {
                disable_background_slab_flush: true,
                ..VfsConfig::default()
            },
            None,
        );
        let attr = vfs
            .store_scratch_entry(NAME, InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
            .unwrap();
        let block = vec![7u8; vfs.block_size as usize];
        runtime.block_on(async {
            vfs.write_scratch(0, attr.ino, 0, &block).await.unwrap();
            vfs.prefetch_writer_stack_object_ids_for_completed_blocks(attr.ino)
                .await
                .unwrap();
            let prefetched = vfs
                .state
                .writer_stack_prefetched_object_ids
                .lock()
                .iter()
                .find(|range| range.ino == attr.ino)
                .map(|range| range.start)
                .unwrap();
            assert!(
                vfs.materialize_writer_stack_completed_blocks_for_inode(attr.ino)
                    .await
                    .unwrap()
                    > 0
            );
            assert!(
                !vfs.state
                    .writer_stack_prefetched_object_ids
                    .lock()
                    .iter()
                    .any(|range| range.ino == attr.ino)
            );
            vfs.flush(attr.ino).await.unwrap();
            assert_eq!(scratch_object(meta.as_ref(), attr.ino), prefetched);
        });
    }

    #[test]
    fn scratch_deferred_release_drains_writer_pages() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-release-");
        let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs(&temp, VOLUME);
        let attr = vfs
            .store_scratch_entry(NAME, InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
            .unwrap();
        let ino = attr.ino;
        // Emulate the handle registration that the scratch namespace will use.
        let file = vfs
            .handle_table
            .open(ino, OpenFlags::RDWR, Some(attr), false);
        vfs.state.file_session.note_open_handle(ino);
        vfs.sync_meta().note_open_handle(ino, true);
        vfs.state
            .read
            .increment_active_access_class(file.current_access_class());

        runtime.block_on(async {
            let events = meta.read_events_range(0, u64::MAX).unwrap().len();
            vfs.write_scratch(file.fh, ino, 0, DATA).await.unwrap();
            let release =
                vfs.release_fh_with_deferred_last_handle_barrier(file.fh, runtime.handle());
            assert!(release.closed_last_handle && release.flushed_local_dirty_state);
            vfs.wait_for_deferred_release_inflight(ino).await;
            assert_eq!(vfs.read_scratch_block(ino, 0).await.unwrap().unwrap(), DATA);
            assert_eq!(meta.read_events_range(0, u64::MAX).unwrap().len(), events);
            assert_eq!(
                vfs.cached_store
                    .cache_pipeline_metrics()
                    .upload_queue
                    .submitted,
                0
            );
        });
    }

    #[test]
    fn scratch_storage_rejects_writes_below_cached_reserve() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-reserve-");
        let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs(&temp, VOLUME);
        let attr = vfs
            .store_scratch_entry(NAME, InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
            .unwrap();
        runtime
            .block_on(vfs.write_scratch(0, attr.ino, 0, DATA))
            .unwrap();
        let backend = vfs
            .local_payload_backend
            .as_any()
            .downcast_ref::<studiofs_cache::PersistentLocalPayloadBackend>()
            .unwrap();
        backend.run_low_disk_space_for_test().unwrap();
        let error = runtime
            .block_on(vfs.write_scratch(0, attr.ino, 0, b"replacement"))
            .unwrap_err();
        assert_eq!(
            crate::bridge::classify_vfs_error(&error),
            crate::BridgeVfsErrorKind::StorageFull
        );
        // Bytes accepted before the sample changed still drain.
        runtime.block_on(vfs.flush(attr.ino)).unwrap();
        assert_eq!(
            runtime
                .block_on(vfs.read_scratch_block(attr.ino, 0))
                .unwrap()
                .unwrap(),
            DATA
        );
        let object = scratch_object(meta.as_ref(), attr.ino);
        assert!(!backend.is_evictable(object));
        assert_eq!(backend.storage_stats().dirty_bytes, 0);
    }
}
