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

    /// Reports whether `ino` is a local-only scratch inode.
    ///
    /// Bridges use this to keep tracked-only maintenance, such as WinFSP
    /// orphan healing, away from scratch directories.
    pub fn is_scratch_inode(&self, ino: Ino) -> VfsResult<bool> {
        match self.sync_meta().get_scratch_attr(ino) {
            Ok(_) => Ok(true),
            Err(MetaError::InodeNotFound(_)) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn record_scratch_writer_stack_payload(
        &self,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: super::sync_ops::VfsWritePayload<'_>,
    ) -> VfsResult<u32> {
        let base_attr = self
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&ino)
            .cloned()
            .map(Ok)
            .unwrap_or_else(|| {
                self.sync_meta()
                    .get_scratch_attr(ino)
                    .map_err(VfsError::Meta)
            })?;
        if base_attr.kind != FileType::RegularFile {
            return Err(MetaError::TargetChanged.into());
        }
        if !data.is_empty() {
            self.local_payload_backend
                .admit_scratch_write()
                .map_err(VfsError::LocalPayload)?;
        }
        let spec = super::writer_stack::WriterWriteSpec {
            fh,
            ino,
            current_size: base_attr.size,
            content_epoch: base_attr.content_epoch,
            first_object_id: 0,
            offset,
        };
        let accepted = match data {
            super::sync_ops::VfsWritePayload::Borrowed(bytes) => {
                self.state.writer_stack.write_bytes(spec, bytes)
            }
            super::sync_ops::VfsWritePayload::Owned(bytes) => {
                self.state.writer_stack.write_owned_bytes(spec, bytes)
            }
        }
        .map_err(|error| error.into_vfs_error("scratch writer stack write failed"))?;
        {
            let mut dirty = self.state.namespace.dirty_attrs.lock();
            let attr = dirty.entry(ino).or_insert_with(|| base_attr.clone());
            attr.kind = FileType::RegularFile;
            attr.size = attr.size.max(accepted.visible_len);
            attr.touch_mtime();
        }
        self.invalidate_attr_read_cache([ino]);
        self.retain_kernel_inode_attr_refresh(ino);
        Ok(accepted.accepted_bytes)
    }

    pub(super) async fn cleanup_scratch_inode_runtime_and_payloads(
        &self,
        ino: Ino,
        mut object_ids: Vec<u64>,
    ) {
        self.state.writer_stack.clear_inode(ino);
        let pending_objects = self.take_pending_dirty_slice_object_ids_for_inode(ino);
        object_ids.extend(pending_objects);
        self.clear_pending_payloads_for_inode(ino);
        self.state.namespace.dirty_attrs.lock().remove(&ino);
        self.invalidate_attr_read_cache([ino]);
        self.retire_os_cache_inode(ino);
        object_ids.sort_unstable();
        object_ids.dedup();
        if !object_ids.is_empty()
            && let Err(error) = self.payload_store_remove_batch_async(&object_ids).await
        {
            warn!(ino, %error, "failed to remove freed scratch payload objects");
        }
    }

    /// Accepts scratch bytes into the mounted writer stack.
    ///
    /// Like an opportunistic tracked write, admission applies access checks and
    /// dirty-buffer backpressure and starts background materialization; flush
    /// and release use the same entry points as tracked files.
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
        self.enforce_access(ino, AccessLevel::ReadWrite)?;
        if fh != 0 {
            self.enforce_mutating_handle_access(fh, ino)?;
        }
        let accepted_bytes = self.record_scratch_writer_stack_payload(
            fh,
            ino,
            offset,
            super::sync_ops::VfsWritePayload::Borrowed(data),
        )?;
        drop(session);
        if accepted_bytes > 0 {
            self.spawn_deferred_materialization_if_needed(ino);
        }
        Ok(accepted_bytes)
    }

    /// Reads scratch bytes for a block, including unflushed writer pages and
    /// pending local-dirty slices. An uncovered block denotes a hole.
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
        let size = self.getattr_async_unchecked(ino).await?.size;
        let end = (start + u64::from(self.block_size)).min(size);
        if start >= end {
            return Ok(None);
        }
        let block_len = (end - start) as usize;
        let mut data: Option<Vec<u8>> = None;
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
                    let data = data.get_or_insert_with(|| vec![0; block_len]);
                    data[offset..offset + bytes.len()].copy_from_slice(bytes);
                }
            }
            cursor = chunk_end;
        }

        // Overlay pending local-dirty slices (oldest-to-newest so newer wins).
        let mut pending_spans =
            self.snapshot_pending_dirty_slice_read_spans(ino, start, end - start);
        pending_spans.extend(self.snapshot_pending_local_dirty_block_read_spans(
            ino,
            start,
            end - start,
        ));
        // snapshot_pending_dirty_slice_read_spans returns newest-first; apply oldest-first.
        for span in pending_spans.into_iter().rev() {
            let overlap_start = span.start.max(start);
            let overlap_end = span.end.min(end);
            if overlap_start >= overlap_end {
                continue;
            }
            let delta = (overlap_start - span.start) as usize;
            let len = (overlap_end - overlap_start) as usize;
            let dst_offset = (overlap_start - start) as usize;
            let src_offset = span.block_offset as usize + delta;
            let bytes = if let Some(object_id) = span.object_id {
                let obj = self.payload_store_get_async(object_id).await?;
                obj.and_then(|buf| {
                    buf.get(src_offset..src_offset + len)
                        .map(Bytes::copy_from_slice)
                })
                .ok_or_else(|| {
                    io::Error::other("scratch payload missing or truncated in local store")
                })?
            } else {
                span.data.slice(src_offset..src_offset + len)
            };
            let data = data.get_or_insert_with(|| vec![0; block_len]);
            data[dst_offset..dst_offset + len].copy_from_slice(&bytes);
        }

        // Overlay unflushed writer-stack pages on top of committed and pending slices.
        let writer_claims = self
            .state
            .writer_stack
            .snapshot_read_claims(ino, start, end - start);
        if !writer_claims.is_empty() {
            let buf = data.get_or_insert_with(|| vec![0; block_len]);
            let _ = self
                .state
                .writer_stack
                .copy_read_spans_into(ino, start, buf);
        }

        Ok(data.map(Bytes::from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_meta::engine::{XATTR_CREATE_FLAG, XATTR_REPLACE_FLAG};
    use studiofs_meta::trash::MetaVolumeSettings;
    use studiofs_meta::{MetaEngineCore, MetaGc, MetaInodes, MetaNamespace};

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

    fn make_started_test_vfs(
        temp: &tempfile::TempDir,
        volume: &str,
    ) -> (
        Arc<tokio::runtime::Runtime>,
        Arc<StudioVfs>,
        Arc<studiofs_meta::RedbMetaEngine>,
    ) {
        let (rt, vfs, meta) = super::super::tests::make_unstarted_test_vfs(temp, volume);
        let _guard = rt.enter();
        vfs.start().unwrap();
        (rt, vfs, meta)
    }

    async fn install_ignore_rules(vfs: &Arc<StudioVfs>, parent: Ino, payload: &[u8]) {
        let staging = b"studiofsignore_stage";
        let attr = vfs.create(parent, staging, 0, 0, 0o644).unwrap();
        let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, true).unwrap();
        if !payload.is_empty() {
            vfs.write_fh(fh, attr.ino, 0, payload).await.unwrap();
        }
        vfs.fsync(attr.ino, false).await.unwrap();
        vfs.release_fh(fh).await.unwrap();
        let mut changed = vfs.subscribe_ignore_rules_changes();
        vfs.rename(
            parent,
            staging,
            parent,
            crate::scratch_rules::RULE_FILE,
            true,
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), changed.recv())
            .await
            .expect("ignore rule refresh completed")
            .expect("ignore rule change delivered");
    }

    #[test]
    fn scratch_namespace_create_mkdir_symlink_and_rule_change_does_not_reclassify() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-ns-create-");
        let (runtime, vfs, meta) = make_started_test_vfs(&temp, "scratch-ns-create");
        let _guard = runtime.enter();
        runtime.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\nbuild/\n!keep.tmp\n").await;
            let events_before = meta.read_events_range(0, u64::MAX).unwrap().len();
            let uploads_before = vfs
                .cached_store
                .cache_pipeline_metrics()
                .upload_queue
                .submitted;

            let tmp_file = vfs.create(ROOT_INO, b"scene.tmp", 0, 0, 0o644).unwrap();
            assert!(vfs.is_scratch_inode(tmp_file.ino).unwrap());
            let scratch_dir = vfs.mkdir(ROOT_INO, b"build", 0, 0, 0o755).unwrap();
            assert!(vfs.is_scratch_inode(scratch_dir.ino).unwrap());

            // Whole subtree of a scratch directory is scratch regardless of name.
            let nested_file = vfs
                .create(scratch_dir.ino, b"output.txt", 0, 0, 0o644)
                .unwrap();
            let nested_dir = vfs.mkdir(scratch_dir.ino, b"nested", 0, 0, 0o755).unwrap();
            let nested_link = vfs
                .symlink(nested_dir.ino, b"latest", b"../output.txt", 0, 0)
                .unwrap();
            assert!(vfs.is_scratch_inode(nested_file.ino).unwrap());
            assert!(vfs.is_scratch_inode(nested_dir.ino).unwrap());
            assert!(vfs.is_scratch_inode(nested_link.ino).unwrap());
            assert_eq!(vfs.readlink(nested_link.ino).unwrap(), b"../output.txt");

            // Zero event_log rows and zero upload submissions for all scratch creates.
            assert_eq!(
                meta.read_events_range(0, u64::MAX).unwrap().len(),
                events_before
            );
            assert_eq!(
                vfs.cached_store
                    .cache_pipeline_metrics()
                    .upload_queue
                    .submitted,
                uploads_before
            );

            // Negated and ordinary files stay tracked.
            let keep = vfs.create(ROOT_INO, b"keep.tmp", 0, 0, 0o644).unwrap();
            assert!(!vfs.is_scratch_inode(keep.ino).unwrap());
            let normal = vfs.create(ROOT_INO, b"normal.txt", 0, 0, 0o644).unwrap();
            assert!(!vfs.is_scratch_inode(normal.ino).unwrap());

            // Changing `.studiofsignore` never reclassifies existing scratch entries on lookup.
            install_ignore_rules(&vfs, ROOT_INO, b"").await;
            assert_eq!(
                vfs.lookup(ROOT_INO, b"scene.tmp").unwrap().ino,
                tmp_file.ino
            );
            assert!(vfs.is_scratch_inode(tmp_file.ino).unwrap());
            assert_eq!(
                vfs.lookup(scratch_dir.ino, b"output.txt").unwrap().ino,
                nested_file.ino
            );
            assert!(vfs.is_scratch_inode(nested_file.ino).unwrap());
            // Newly created `.tmp` file under the empty rules is now tracked.
            let after_change = vfs.create(ROOT_INO, b"fresh.tmp", 0, 0, 0o644).unwrap();
            assert!(!vfs.is_scratch_inode(after_change.ino).unwrap());
        });
    }

    #[test]
    fn scratch_read_write_unflushed_truncate_setattr_xattrs_and_acl() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-ns-io-");
        let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs_at(
            temp.path(),
            "scratch-ns-io",
            VfsConfig {
                disable_background_slab_flush: true,
                ..VfsConfig::default()
            },
            None,
        );
        let _guard = runtime.enter();
        runtime.block_on(async {
            let dir = vfs.mkdir(ROOT_INO, b"work", 0, 0, 0o755).unwrap();
            let events_before = meta.read_events_range(0, u64::MAX).unwrap().len();
            let uploads_before = vfs
                .cached_store
                .cache_pipeline_metrics()
                .upload_queue
                .submitted;

            // AppleDouble `._*` enters scratch through the built-in rule.
            let attr = vfs
                .create(dir.ino, b"._asset.bin", 1000, 1000, 0o644)
                .unwrap();
            assert!(vfs.is_scratch_inode(attr.ino).unwrap());
            assert!(!vfs.is_synthesized_ignored_ino(attr.ino));

            // Virtual read-only xattr `user.studiofs.scratch` marks scratch entries
            // and stays hidden from list_xattrs.
            assert_eq!(
                vfs.get_xattr(attr.ino, super::SCRATCH_XATTR_NAME)
                    .unwrap()
                    .as_deref(),
                Some(b"1".as_slice())
            );
            assert_eq!(
                vfs.get_xattr_size(attr.ino, super::SCRATCH_XATTR_NAME)
                    .unwrap(),
                Some(1)
            );
            assert!(vfs.list_xattrs(attr.ino).unwrap().is_empty());
            assert!(matches!(
                vfs.set_xattr(attr.ino, super::SCRATCH_XATTR_NAME, b"0", 0),
                Err(VfsError::Meta(MetaError::AccessDenied { .. }))
            ));
            assert!(matches!(
                vfs.remove_xattr(attr.ino, super::SCRATCH_XATTR_NAME),
                Err(VfsError::Meta(MetaError::AccessDenied { .. }))
            ));
            assert_eq!(
                vfs.get_xattr(dir.ino, super::SCRATCH_XATTR_NAME).unwrap(),
                None
            );
            assert!(matches!(
                vfs.set_xattr(dir.ino, super::SCRATCH_XATTR_NAME, b"1", 0),
                Err(VfsError::Meta(MetaError::AccessDenied { .. }))
            ));

            // Ordinary xattrs work with create/replace flags.
            vfs.set_xattr(attr.ino, b"user.note", b"v1", XATTR_CREATE_FLAG)
                .unwrap();
            assert!(matches!(
                vfs.set_xattr(attr.ino, b"user.note", b"v2", XATTR_CREATE_FLAG),
                Err(VfsError::Meta(MetaError::EntryExists { .. }))
            ));
            vfs.set_xattr(attr.ino, b"user.note", b"v2", XATTR_REPLACE_FLAG)
                .unwrap();
            assert_eq!(
                vfs.get_xattr(attr.ino, b"user.note").unwrap().as_deref(),
                Some(b"v2".as_slice())
            );
            assert_eq!(
                vfs.list_xattrs(attr.ino).unwrap(),
                vec![b"user.note".to_vec()]
            );
            vfs.remove_xattr(attr.ino, b"user.note").unwrap();
            assert!(vfs.list_xattrs(attr.ino).unwrap().is_empty());

            // Unflushed writer pages are immediately readable via both `read` and `read_scratch_block`.
            let fh = vfs.open_file(attr.ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh(fh, attr.ino, 0, b"hello-unflushed")
                .await
                .unwrap();
            assert_eq!(vfs.getattr(attr.ino).unwrap().size, 15);
            assert_eq!(
                vfs.read(attr.ino, 0, 32).await.unwrap().as_ref(),
                b"hello-unflushed"
            );
            assert_eq!(
                vfs.read_scratch_block(attr.ino, 0)
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                b"hello-unflushed"
            );

            // Flush & fsync commit scratch overlay, then truncate shrinks and frees displaced objects.
            vfs.fsync(attr.ino, false).await.unwrap();
            let full_obj = scratch_object(meta.as_ref(), attr.ino);
            assert!(vfs.local_payload_backend.contains(full_obj));

            let shrunk = vfs
                .setattr_with_handle_posix(
                    Some(fh),
                    attr.ino,
                    studiofs_meta::SetattrArgs::size_only(5),
                )
                .unwrap();
            assert_eq!(shrunk.size, 5);
            assert_eq!(vfs.read(attr.ino, 0, 16).await.unwrap().as_ref(), b"hello");

            let zeroed = vfs
                .setattr_with_handle_posix(
                    Some(fh),
                    attr.ino,
                    studiofs_meta::SetattrArgs::size_only(0),
                )
                .unwrap();
            assert_eq!(zeroed.size, 0);
            assert!(!vfs.local_payload_backend.contains(full_obj));

            // Attr-only setattr updates mode without journaling.
            let chmod = vfs
                .setattr_attr_only_deferred_metadata(
                    attr.ino,
                    studiofs_meta::SetattrArgs {
                        mode: Some(0o600),
                        ..studiofs_meta::SetattrArgs::default()
                    },
                    runtime.handle(),
                )
                .unwrap();
            assert_eq!(chmod.mode & 0o7777, 0o600);
            assert_eq!(vfs.getattr(attr.ino).unwrap().mode & 0o7777, 0o600);

            assert_eq!(
                meta.read_events_range(0, u64::MAX).unwrap().len(),
                events_before
            );
            assert_eq!(
                vfs.cached_store
                    .cache_pipeline_metrics()
                    .upload_queue
                    .submitted,
                uploads_before
            );

            // ACL checks apply to scratch write, write_scratch, setattr, xattr, and fsync.
            vfs.set_acl_rule(dir.ino, &vfs.acl_principal, AccessLevel::ReadOnly)
                .unwrap();
            assert!(matches!(
                vfs.write_scratch(fh, attr.ino, 0, b"blocked").await,
                Err(VfsError::Meta(MetaError::AccessDenied { .. }))
            ));
            assert!(matches!(
                vfs.write_fh(fh, attr.ino, 0, b"blocked").await,
                Err(VfsError::Meta(MetaError::AccessDenied { .. }))
            ));
            assert!(matches!(
                vfs.set_xattr(attr.ino, b"user.ro", b"no", 0),
                Err(VfsError::Meta(MetaError::AccessDenied { .. }))
            ));
            // ReadOnly still allows fsync and read.
            vfs.fsync(attr.ino, false).await.unwrap();

            // NoAccess blocks fsync and async read on scratch inodes.
            vfs.set_acl_rule(dir.ino, &vfs.acl_principal, AccessLevel::NoAccess)
                .unwrap();
            assert!(matches!(
                vfs.fsync(attr.ino, false).await,
                Err(VfsError::Meta(MetaError::InodeNotFound(_)))
            ));
            assert!(matches!(
                vfs.read(attr.ino, 0, 16).await,
                Err(VfsError::Meta(MetaError::InodeNotFound(_)))
            ));

            vfs.set_acl_rule(dir.ino, &vfs.acl_principal, AccessLevel::ReadWrite)
                .unwrap();
            vfs.release_fh(fh).await.unwrap();
        });
    }

    #[test]
    fn scratch_readdir_merges_tracked_and_scratch_with_stable_order() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-ns-readdir-");
        let (_runtime, vfs, _meta) =
            super::super::tests::make_unstarted_test_vfs(&temp, "scratch-ns-readdir");

        let _a = vfs.create(ROOT_INO, b"b_tracked.txt", 0, 0, 0o644).unwrap();
        let _b = vfs.create(ROOT_INO, b"._a_scratch", 0, 0, 0o644).unwrap();
        let _c = vfs.create(ROOT_INO, b"d_tracked.txt", 0, 0, 0o644).unwrap();
        let _d = vfs.create(ROOT_INO, b"._c_scratch", 0, 0, 0o644).unwrap();

        let first: Vec<Vec<u8>> = vfs
            .readdir(ROOT_INO)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        let second: Vec<Vec<u8>> = vfs
            .readdir_with_attrs(ROOT_INO)
            .unwrap()
            .into_iter()
            .map(|e| e.entry.name)
            .collect();
        let expected = vec![
            b"._a_scratch".to_vec(),
            b"._c_scratch".to_vec(),
            b"b_tracked.txt".to_vec(),
            b"d_tracked.txt".to_vec(),
        ];
        assert_eq!(first, expected);
        assert_eq!(second, expected);
    }

    #[test]
    fn scratch_unlink_open_handle_reclaims_payloads_on_last_close_and_rmdir_enotempty() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-ns-unlink-");
        let (runtime, vfs, meta) =
            super::super::tests::make_unstarted_test_vfs(&temp, "scratch-ns-unlink");
        let _guard = runtime.enter();
        runtime.block_on(async {
            let dir = vfs.mkdir(ROOT_INO, b"holder", 0, 0, 0o755).unwrap();
            let events_before = meta.read_events_range(0, u64::MAX).unwrap().len();

            let file = vfs.create(dir.ino, b"._open.tmp", 0, 0, 0o644).unwrap();
            let fh1 = vfs.open_file(file.ino, OpenFlags::RDWR, false).unwrap();
            let fh2 = vfs.open_file(file.ino, OpenFlags::RDONLY, false).unwrap();
            vfs.write_fh(fh1, file.ino, 0, b"payload-bytes")
                .await
                .unwrap();
            vfs.fsync(file.ino, false).await.unwrap();
            let obj_id = scratch_object(meta.as_ref(), file.ino);
            assert!(vfs.local_payload_backend.contains(obj_id));

            // Tracked directory containing only a scratch entry fails rmdir with ENOTEMPTY.
            assert!(matches!(
                vfs.rmdir(ROOT_INO, b"holder"),
                Err(VfsError::Meta(MetaError::DirNotEmpty(ino))) if ino == dir.ino
            ));

            // Unlink while open: dentry vanishes, zero events emitted, payload survives until last close.
            vfs.unlink(dir.ino, b"._open.tmp").unwrap();
            assert!(vfs.lookup(dir.ino, b"._open.tmp").is_err());
            assert!(vfs.local_payload_backend.contains(obj_id));
            assert_eq!(
                vfs.read(file.ino, 0, 32).await.unwrap().as_ref(),
                b"payload-bytes"
            );
            assert_eq!(
                meta.read_events_range(0, u64::MAX).unwrap().len(),
                events_before
            );

            // Closing first handle keeps payload alive.
            vfs.release_fh(fh1).await.unwrap();
            assert!(vfs.local_payload_backend.contains(obj_id));
            assert_eq!(
                vfs.read(file.ino, 0, 32).await.unwrap().as_ref(),
                b"payload-bytes"
            );

            // Closing last handle finalizes orphan scratch inode and frees payload object.
            vfs.release_fh(fh2).await.unwrap();
            assert!(!vfs.local_payload_backend.contains(obj_id));
            assert!(matches!(
                meta.get_scratch_attr(file.ino),
                Err(MetaError::InodeNotFound(_))
            ));
            assert_eq!(
                meta.read_events_range(0, u64::MAX).unwrap().len(),
                events_before
            );

            // Now tracked directory is empty and rmdir succeeds.
            vfs.rmdir(ROOT_INO, b"holder").unwrap();
        });
    }

    #[test]
    fn scratch_demote_keeps_pending_file_readable() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-demote-pending-");
        let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs(&temp, VOLUME);
        let dir = vfs
            .store_scratch_entry(b"local", InodeAttr::new_dir(0, ROOT_INO, 0, 0, 0o755))
            .unwrap();
        let file = vfs.create(ROOT_INO, b"shot.exr", 0, 0, 0o644).unwrap();
        runtime.block_on(vfs.write(file.ino, 0, DATA)).unwrap();
        runtime.block_on(vfs.flush(file.ino)).unwrap();
        assert_eq!(vfs.cached_store.pending_upload_count(), 1);

        vfs.rename(ROOT_INO, b"shot.exr", dir.ino, b"shot.exr", false)
            .unwrap();

        assert_eq!(vfs.cached_store.pending_upload_count(), 0);
        assert!(matches!(
            meta.lookup(ROOT_INO, b"shot.exr"),
            Err(MetaError::EntryNotFound { .. })
        ));
        assert_eq!(
            meta.lookup_scratch_entry(dir.ino, b"shot.exr").unwrap(),
            Some(file.ino)
        );
        assert_eq!(
            &runtime
                .block_on(vfs.read(file.ino, 0, DATA.len() as u32))
                .unwrap()[..],
            DATA
        );
    }

    #[test]
    fn scratch_demote_published_file_requires_every_payload_locally() {
        use studiofs_meta::schema::{
            ChunkId, PayloadRef, SliceId, SliceOrder, SliceRange, SliceRecord,
        };
        use studiofs_meta::{MetaEngine, MetaPayloadObjects};

        for fully_cached in [false, true] {
            let temp = studiofs_test_support::workspace_target_tempdir("scratch-demote-published-");
            let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs(&temp, VOLUME);
            let dir = vfs
                .store_scratch_entry(b"local", InodeAttr::new_dir(0, ROOT_INO, 0, 0, 0o755))
                .unwrap();
            let mut file = vfs.create(ROOT_INO, b"published", 0, 0, 0o644).unwrap();
            let ids = meta.next_object_ids(2).unwrap();
            let slices = ids
                .iter()
                .enumerate()
                .map(|(i, &id)| {
                    SliceRecord::new(
                        SliceId(i as u64 + 1),
                        SliceOrder(i as u64 + 1),
                        SliceRange::new(i as u32, i as u32 + 1).unwrap(),
                        PayloadRef::new_full(id, 1),
                    )
                })
                .collect();
            file.size = 2;
            file.content_epoch = 1;
            meta.flush_regular_file_overlay_slices_with_attr(
                file.ino,
                &[(ChunkId(0), slices)],
                Some(&file),
            )
            .unwrap();
            for &id in &ids {
                assert!(
                    meta.mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, id)
                        .unwrap()
                );
            }
            assert!(
                meta.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
                    .unwrap()
            );
            runtime
                .block_on(vfs.payload_store_put_async(ids[0], Bytes::from_static(b"a"), true))
                .unwrap();
            if fully_cached {
                runtime
                    .block_on(vfs.payload_store_put_async(ids[1], Bytes::from_static(b"b"), true))
                    .unwrap();
            }
            let events_before = meta.read_events_range(0, u64::MAX).unwrap().len();
            let result = vfs.rename(ROOT_INO, b"published", dir.ino, b"moved", false);
            if fully_cached {
                result.unwrap();
                assert!(meta.lookup(ROOT_INO, b"published").is_err());
                assert_eq!(
                    meta.lookup_scratch_entry(dir.ino, b"moved").unwrap(),
                    Some(file.ino)
                );
                assert_eq!(
                    runtime.block_on(vfs.read(file.ino, 0, 2)).unwrap().as_ref(),
                    b"ab"
                );
            } else {
                assert!(matches!(result, Err(VfsError::CrossDevice)));
                assert_eq!(meta.lookup(ROOT_INO, b"published").unwrap().ino, file.ino);
                assert_eq!(meta.lookup_scratch_entry(dir.ino, b"moved").unwrap(), None);
                assert_eq!(
                    meta.read_events_range(0, u64::MAX).unwrap().len(),
                    events_before
                );
                assert_eq!(vfs.local_payload_backend.object_ids(), [ids[0]]);
            }
        }
    }

    #[test]
    fn scratch_demote_directory_preserves_child_and_rejects_over_limit() {
        for limit in [1, 2] {
            let temp = studiofs_test_support::workspace_target_tempdir("scratch-demote-tree-");
            let (runtime, mut vfs, meta) =
                super::super::tests::make_unstarted_test_vfs(&temp, VOLUME);
            Arc::get_mut(&mut vfs).unwrap().scratch_promote_entry_limit = limit;
            let dest = vfs
                .store_scratch_entry(b"local", InodeAttr::new_dir(0, ROOT_INO, 0, 0, 0o755))
                .unwrap();
            let tree = vfs.mkdir(ROOT_INO, b"tree", 0, 0, 0o755).unwrap();
            let file = vfs.create(tree.ino, b"child", 0, 0, 0o644).unwrap();
            runtime.block_on(vfs.write(file.ino, 0, DATA)).unwrap();
            runtime.block_on(vfs.flush(file.ino)).unwrap();
            vfs.set_xattr(file.ino, b"user.demo", b"value", 0).unwrap();
            let result = vfs.rename(ROOT_INO, b"tree", dest.ino, b"tree", false);
            if limit == 1 {
                assert!(matches!(result, Err(VfsError::CrossDevice)));
                assert_eq!(meta.lookup(ROOT_INO, b"tree").unwrap().ino, tree.ino);
                assert_eq!(meta.lookup_scratch_entry(dest.ino, b"tree").unwrap(), None);
            } else {
                result.unwrap();
                assert!(meta.lookup(ROOT_INO, b"tree").is_err());
                assert_eq!(
                    meta.lookup_scratch_entry(dest.ino, b"tree").unwrap(),
                    Some(tree.ino)
                );
                assert_eq!(
                    meta.lookup_scratch_entry(tree.ino, b"child").unwrap(),
                    Some(file.ino)
                );
                assert_eq!(
                    vfs.get_xattr(file.ino, b"user.demo").unwrap().unwrap(),
                    b"value"
                );
                assert_eq!(
                    runtime
                        .block_on(vfs.read(file.ino, 0, DATA.len() as u32))
                        .unwrap()
                        .as_ref(),
                    DATA,
                );
            }
        }
    }

    #[test]
    fn scratch_promote_preserves_open_writer() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-promote-writer-");
        let (runtime, vfs, _) = make_started_test_vfs(&temp, "scratch-promote-writer");
        let _guard = runtime.enter();
        runtime.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\n").await;
            let file = vfs.create(ROOT_INO, b"shot.exr.tmp", 0, 0, 0o644).unwrap();
            let fh = vfs.open_file(file.ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh(fh, file.ino, 0, b"before").await.unwrap();
            vfs.rename(ROOT_INO, b"shot.exr.tmp", ROOT_INO, b"shot.exr", false)
                .unwrap();
            assert_eq!(vfs.lookup(ROOT_INO, b"shot.exr").unwrap().ino, file.ino);
            assert!(!vfs.is_scratch_inode(file.ino).unwrap());
            vfs.write_fh(fh, file.ino, 6, b"after").await.unwrap();
            vfs.flush(file.ino).await.unwrap();
            assert_eq!(
                &vfs.read(file.ino, 0, 11).await.unwrap()[..],
                b"beforeafter"
            );
        });
    }

    #[test]
    fn scratch_promote_replace_retains_displaced_tracked_identity() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-promote-replace-");
        let (runtime, vfs, meta) = make_started_test_vfs(&temp, "scratch-promote-replace");
        let _guard = runtime.enter();
        runtime.block_on(async {
            install_ignore_rules(
                &vfs, ROOT_INO, b"*.tmp
",
            )
            .await;
            let old = vfs.create(ROOT_INO, b"shot.exr", 0, 0, 0o644).unwrap();
            let old_fh = vfs.open_file(old.ino, OpenFlags::RDWR, false).unwrap();
            vfs.write_fh(old_fh, old.ino, 0, b"old").await.unwrap();
            vfs.fsync_fh(old_fh).await.unwrap();

            let new = vfs.create(ROOT_INO, b"shot.exr.tmp", 0, 0, 0o644).unwrap();
            vfs.write(new.ino, 0, b"new").await.unwrap();
            vfs.flush(new.ino).await.unwrap();
            vfs.rename(ROOT_INO, b"shot.exr.tmp", ROOT_INO, b"shot.exr", true)
                .unwrap();

            assert_eq!(meta.lookup(ROOT_INO, b"shot.exr").unwrap().ino, new.ino);
            // The displaced tracked inode survives for its open handle.
            assert_eq!(&vfs.read(old.ino, 0, 3).await.unwrap()[..], b"old");
            assert_eq!(&vfs.read(new.ino, 0, 3).await.unwrap()[..], b"new");
            vfs.release_fh(old_fh).await.unwrap();
        });
    }

    #[test]
    fn scratch_promote_replays_payload_after_commit_before_submission() {
        const CRASH: &str = "STUDIOFS_TEST_SCRATCH_PROMOTE_CRASH";
        const TARGET: &[u8] = b"scene.exr";
        if let Some(root) = std::env::var_os(CRASH) {
            let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs_at(
                Path::new(&root),
                VOLUME,
                VfsConfig::default(),
                None,
            );
            let file = vfs
                .store_scratch_entry(NAME, InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
                .unwrap();
            runtime
                .block_on(vfs.write_scratch(0, file.ino, 0, DATA))
                .unwrap();
            runtime.block_on(vfs.flush(file.ino)).unwrap();
            let object = scratch_object(meta.as_ref(), file.ino);
            assert_eq!(vfs.cached_store.pending_upload_count(), 0);
            let (result, _) = meta
                .promote_scratch_entry(
                    (ROOT_INO, NAME),
                    (ROOT_INO, TARGET),
                    file.ino,
                    &std::collections::HashSet::from([file.ino]),
                    false,
                )
                .unwrap();
            assert_eq!(result.moved_ino, file.ino);
            assert_eq!(vfs.cached_store.pending_upload_count(), 0);
            assert_eq!(vfs.local_payload_backend.object_ids(), [object]);
            assert!(
                meta.get_local_referenced_object_ids()
                    .unwrap()
                    .contains(&object)
            );
            assert!(!vfs.cached_store.has_staged_upload(object));
            std::process::exit(42);
        }

        let temp = studiofs_test_support::workspace_target_tempdir("scratch-promote-crash-");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "vfs::scratch_storage::tests::scratch_promote_replays_payload_after_commit_before_submission"])
            .env(CRASH, temp.path())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(42));
        let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs(&temp, VOLUME);
        let ino = meta.lookup(ROOT_INO, TARGET).unwrap().ino;
        let object_ids = vfs.local_payload_backend.object_ids();
        assert_eq!(object_ids.len(), 1);
        let object = object_ids[0];
        assert_eq!(vfs.cached_store.pending_upload_count(), 1);
        assert!(vfs.cached_store.has_staged_upload(object));
        assert_eq!(
            &runtime
                .block_on(vfs.read(ino, 0, DATA.len() as u32))
                .unwrap()[..],
            DATA
        );
    }

    #[test]
    fn scratch_promote_moves_subtree_and_reuses_staged_objects() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-promote-tree-");
        let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs(&temp, VOLUME);
        let dir = vfs
            .store_scratch_entry(b"tree.tmp", InodeAttr::new_dir(0, ROOT_INO, 0, 0, 0o755))
            .unwrap();
        let file = vfs
            .store_scratch_entry(b"child.exr", InodeAttr::new_file(0, dir.ino, 0, 0, 0o644))
            .unwrap();
        runtime
            .block_on(vfs.write_scratch(0, file.ino, 0, DATA))
            .unwrap();
        runtime.block_on(vfs.flush(file.ino)).unwrap();
        let object = scratch_object(meta.as_ref(), file.ino);
        meta.set_scratch_xattr(file.ino, b"user.test", b"preserved")
            .unwrap();
        let target = vfs.mkdir(ROOT_INO, b"tree", 0, 0, 0o755).unwrap();
        assert!(matches!(
            vfs.rename(ROOT_INO, b"tree.tmp", ROOT_INO, b"tree", false),
            Err(VfsError::Meta(MetaError::EntryExists { .. }))
        ));
        assert_eq!(vfs.cached_store.pending_upload_count(), 0);
        assert_eq!(vfs.lookup(ROOT_INO, b"tree").unwrap().ino, target.ino);
        vfs.rename(ROOT_INO, b"tree.tmp", ROOT_INO, b"tree", true)
            .unwrap();
        assert_eq!(meta.lookup(ROOT_INO, b"tree").unwrap().ino, dir.ino);
        assert_eq!(meta.lookup(dir.ino, b"child.exr").unwrap().ino, file.ino);
        assert!(!vfs.is_scratch_inode(file.ino).unwrap());
        assert_eq!(
            vfs.get_xattr(file.ino, b"user.test").unwrap().unwrap(),
            b"preserved"
        );
        assert_eq!(vfs.local_payload_backend.object_ids(), [object]);
        assert!(vfs.cached_store.has_staged_upload(object));
        assert_eq!(vfs.cached_store.pending_upload_count(), 1);
    }

    #[test]
    fn scratch_promote_rejects_subtree_over_entry_limit() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-promote-limit-");
        let (_runtime, mut vfs, meta) = super::super::tests::make_unstarted_test_vfs(&temp, VOLUME);
        Arc::get_mut(&mut vfs).unwrap().scratch_promote_entry_limit = 1;
        let dir = vfs
            .store_scratch_entry(b"tree.tmp", InodeAttr::new_dir(0, ROOT_INO, 0, 0, 0o755))
            .unwrap();
        vfs.store_scratch_entry(b"child", InodeAttr::new_file(0, dir.ino, 0, 0, 0o644))
            .unwrap();
        let before = meta.read_events_range(0, u64::MAX).unwrap();
        assert!(matches!(
            vfs.rename(ROOT_INO, b"tree.tmp", ROOT_INO, b"tree", false),
            Err(VfsError::CrossDevice)
        ));
        assert_eq!(
            meta.lookup_scratch_entry(ROOT_INO, b"tree.tmp").unwrap(),
            Some(dir.ino)
        );
        assert_eq!(
            meta.read_events_range(0, u64::MAX).unwrap().len(),
            before.len()
        );
    }

    #[test]
    fn scratch_rename_hardlink_discard_split_and_zero_events() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-ns-rename-");
        let (runtime, vfs, meta) = make_started_test_vfs(&temp, "scratch-ns-rename");
        let _guard = runtime.enter();
        runtime.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"*.tmp\nscratch_dir/\n").await;
            let tracked_dir = vfs.mkdir(ROOT_INO, b"tracked_dir", 0, 0, 0o755).unwrap();
            let tracked_file = vfs.create(ROOT_INO, b"doc.txt", 0, 0, 0o644).unwrap();
            let scratch_dir = vfs.mkdir(ROOT_INO, b"scratch_dir", 0, 0, 0o755).unwrap();
            let scratch_a = vfs.create(ROOT_INO, b"a.tmp", 0, 0, 0o644).unwrap();
            let scratch_b = vfs.create(ROOT_INO, b"b.tmp", 0, 0, 0o644).unwrap();
            assert!(vfs.is_scratch_inode(scratch_dir.ino).unwrap());
            assert!(vfs.is_scratch_inode(scratch_a.ino).unwrap());

            let events_before_scratch_ops = meta.read_events_range(0, u64::MAX).unwrap().len();

            // Rename within scratch class stays scratch (including overwrite & case-only).
            vfs.rename(ROOT_INO, b"a.tmp", ROOT_INO, b"b.tmp", true)
                .unwrap();
            assert_eq!(vfs.lookup(ROOT_INO, b"b.tmp").unwrap().ino, scratch_a.ino);
            assert!(vfs.lookup(ROOT_INO, b"a.tmp").is_err());
            assert!(matches!(
                meta.get_scratch_attr(scratch_b.ino),
                Err(MetaError::InodeNotFound(_))
            ));
            vfs.rename(ROOT_INO, b"b.tmp", scratch_dir.ino, b"moved.txt", false)
                .unwrap();
            assert_eq!(
                vfs.lookup(scratch_dir.ino, b"moved.txt").unwrap().ino,
                scratch_a.ino
            );
            assert!(vfs.is_scratch_inode(scratch_a.ino).unwrap());
            vfs.rename(
                scratch_dir.ino,
                b"moved.txt",
                scratch_dir.ino,
                b"MOVED.TXT",
                true,
            )
            .unwrap();
            assert_eq!(
                vfs.lookup(scratch_dir.ino, b"MOVED.TXT").unwrap().ino,
                scratch_a.ino
            );

            // Hardlinks: scratch -> any name returns EXDEV; tracked -> into scratch dir returns EXDEV.
            assert!(matches!(
                vfs.link(scratch_a.ino, ROOT_INO, b"link.tmp"),
                Err(VfsError::CrossDevice)
            ));
            assert!(matches!(
                vfs.link(scratch_a.ino, scratch_dir.ino, b"link2"),
                Err(VfsError::CrossDevice)
            ));
            assert!(matches!(
                vfs.link(tracked_file.ino, scratch_dir.ino, b"link_in_scratch"),
                Err(VfsError::CrossDevice)
            ));

            // A tracked entry replacing a scratch destination crosses classes;
            // both entries stay intact.
            let scratch_victim = vfs.create(ROOT_INO, b"victim.tmp", 0, 0, 0o644).unwrap();
            assert!(matches!(
                vfs.rename(ROOT_INO, b"doc.txt", ROOT_INO, b"victim.tmp", false),
                Err(VfsError::Meta(MetaError::EntryExists { .. }))
            ));
            assert!(matches!(
                vfs.rename(ROOT_INO, b"doc.txt", ROOT_INO, b"victim.tmp", true),
                Err(VfsError::CrossDevice)
            ));
            assert_eq!(
                vfs.lookup(ROOT_INO, b"victim.tmp").unwrap().ino,
                scratch_victim.ino
            );
            assert_eq!(
                vfs.lookup(ROOT_INO, b"doc.txt").unwrap().ino,
                tracked_file.ino
            );
            assert!(meta.lookup(ROOT_INO, b"victim.tmp").is_err());

            // Discard paths stay in IgnoredEntryRegistry (not scratch).
            let spotlight = vfs
                .mkdir(ROOT_INO, b".Spotlight-V100", 0, 0, 0o755)
                .unwrap();
            let fsevents = vfs.create(ROOT_INO, b".fseventsd", 0, 0, 0o644).unwrap();
            assert!(vfs.is_synthesized_ignored_ino(spotlight.ino));
            assert!(!vfs.is_scratch_inode(spotlight.ino).unwrap());
            assert!(vfs.is_synthesized_ignored_ino(fsevents.ino));
            assert!(!vfs.is_scratch_inode(fsevents.ino).unwrap());

            // Zero events emitted by all scratch/cross-class/discard operations above.
            assert_eq!(
                meta.read_events_range(0, u64::MAX).unwrap().len(),
                events_before_scratch_ops
            );

            // Tracked -> matching name in a tracked parent stays tracked (hardlink & rename).
            let hardlinked = vfs
                .link(tracked_file.ino, tracked_dir.ino, b"hardlink.tmp")
                .unwrap();
            assert_eq!(hardlinked.ino, tracked_file.ino);
            assert_eq!(hardlinked.nlink, 2);
            assert!(!vfs.is_scratch_inode(hardlinked.ino).unwrap());

            vfs.rename(ROOT_INO, b"doc.txt", tracked_dir.ino, b"renamed.tmp", false)
                .unwrap();
            let renamed = vfs.lookup(tracked_dir.ino, b"renamed.tmp").unwrap();
            assert_eq!(renamed.ino, tracked_file.ino);
            assert!(!vfs.is_scratch_inode(renamed.ino).unwrap());
            assert!(meta.lookup(tracked_dir.ino, b"renamed.tmp").is_ok());

            // Demoting either tracked alias would leave the other across classes.
            assert!(matches!(
                vfs.rename(
                    tracked_dir.ino,
                    b"renamed.tmp",
                    scratch_dir.ino,
                    b"demoted.txt",
                    true
                ),
                Err(VfsError::CrossDevice)
            ));
            assert_eq!(
                vfs.lookup(tracked_dir.ino, b"hardlink.tmp").unwrap().ino,
                tracked_file.ino
            );
        });
    }

    #[test]
    fn scratch_dir_rule_cache_matches_full_classify_and_invalidates_on_epoch_change() {
        let mut rules = crate::scratch_rules::ScratchRules::new(true);
        rules.update(b"", b"*.tmp\nbuild/\n!build/keep\n").unwrap();
        rules.update(b"sub", b"*.bak\n!special.tmp\n").unwrap();

        for (dir, name, is_dir) in [
            (b"".as_slice(), b"a.tmp".as_slice(), false),
            (b"", b"a.txt", false),
            (b"", b"._apple", false),
            (b"", b".studiofsignore", false),
            (b"sub", b"x.bak", false),
            (b"sub", b"special.tmp", false),
            (b"sub", b"other.tmp", false),
            (b"sub/deep", b"nested.bak", false),
            (b"build", b"anything.txt", false),
            (b"build/deep", b"nested", true),
        ] {
            let compiled = rules.compile_directory(dir);
            let mut full_path = dir.to_vec();
            if !full_path.is_empty() {
                full_path.push(b'/');
            }
            full_path.extend_from_slice(name);
            assert_eq!(
                compiled.classify_child(name, is_dir),
                rules.classify(&full_path, is_dir),
                "mismatch for dir={:?} name={:?}",
                String::from_utf8_lossy(dir),
                String::from_utf8_lossy(name)
            );
        }

        // Verify VFS per-directory cache hits and epoch invalidation.
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-rule-cache-");
        let (runtime, vfs, _meta) = make_started_test_vfs(&temp, "scratch-rule-cache");
        let _guard = runtime.enter();
        runtime.block_on(async {
            install_ignore_rules(&vfs, ROOT_INO, b"dir_a/*.log\n").await;
            let dir = vfs.mkdir(ROOT_INO, b"dir_a", 0, 0, 0o755).unwrap();
            assert_eq!(
                vfs.classify_new_entry(dir.ino, b"app.log", false).unwrap(),
                crate::scratch_rules::ScratchClass::Scratch
            );
            // Second classification uses cached CompiledDirRules for dir.ino.
            assert_eq!(
                vfs.classify_new_entry(dir.ino, b"other.log", false)
                    .unwrap(),
                crate::scratch_rules::ScratchClass::Scratch
            );
            // Renaming dir_a -> dir_b bumps dir_rename_epoch and invalidates the cache.
            vfs.rename(ROOT_INO, b"dir_a", ROOT_INO, b"dir_b", false)
                .unwrap();
            assert_eq!(
                vfs.classify_new_entry(dir.ino, b"app.log", false).unwrap(),
                crate::scratch_rules::ScratchClass::Tracked
            );
        });
    }
}
