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

    /// Writes one complete scratch block and durably updates its local mapping.
    ///
    /// The caller assembles partial edits into a full block before replacement.
    /// The final block may be shorter than the volume block size.
    pub async fn write_scratch_block(&self, ino: Ino, block: u32, data: Bytes) -> VfsResult<()> {
        self.enforce_metadata_mutation_authority(ino)?;
        if data.is_empty() || data.len() > self.block_size as usize {
            return Err(VfsError::Io(io::ErrorKind::InvalidInput.into()));
        }
        let end = u64::from(block) * self.block_size as u64 + data.len() as u64;
        let _session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Write)
            .await;
        let object_id = run_meta(self.meta.clone(), move |meta: Arc<dyn VfsMeta>| {
            let attr = meta.get_scratch_attr(ino)?;
            if attr.kind != FileType::RegularFile {
                return Err(MetaError::TargetChanged);
            }
            let id = meta.next_object_ids(1)?.into_iter().next().ok_or_else(|| {
                MetaError::Storage("allocated scratch object batch is empty".into())
            })?;
            // Fence the ID before a payload file can exist after a crash.
            meta.sync_local_metadata()?;
            Ok(id)
        })
        .await?;

        self.payload_store_put_batch_with_mode(
            vec![LocalPayloadPut {
                object_id,
                data,
                evictable: false,
            }],
            true,
        )
        .await?;

        let previous = run_meta(self.meta.clone(), move |meta: Arc<dyn VfsMeta>| {
            meta.commit_scratch_block(ino, block, object_id, end)
        })
        .await?;
        if let Some(previous) = previous
            && let Err(error) = self.payload_store_remove_batch_async(&[previous]).await
        {
            warn!(ino, previous, %error, "failed to remove displaced scratch payload");
        }
        Ok(())
    }

    /// Reads a scratch block from local storage. An absent mapping denotes a hole.
    pub async fn read_scratch_block(&self, ino: Ino, block: u32) -> VfsResult<Option<Bytes>> {
        let _session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Read)
            .await;
        let object = run_meta(self.meta.clone(), move |meta: Arc<dyn VfsMeta>| {
            meta.get_scratch_block(ino, block)
        })
        .await?;
        let Some(object) = object else {
            return Ok(None);
        };
        self.payload_store_get_async(object)
            .await?
            .map(Some)
            .ok_or_else(|| {
                VfsError::Io(io::Error::other(
                    "scratch payload missing from the local store",
                ))
            })
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

    #[test]
    fn scratch_storage_survives_unclean_process_exit_without_publication() {
        if let Some(root) = std::env::var_os(CRASH_ROOT) {
            let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs_at(
                Path::new(&root),
                VOLUME,
                VfsConfig::default(),
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
            runtime
                .block_on(vfs.write_scratch_block(attr.ino, 0, Bytes::from_static(DATA)))
                .unwrap();
            assert_eq!(meta.read_events_range(0, u64::MAX).unwrap().len(), events);
            assert_eq!(vfs.cached_store.pending_upload_count(), pending);
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
        let object = meta.get_scratch_block(ino, 0).unwrap().unwrap();
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
            .block_on(vfs.write_scratch_block(attr.ino, 0, Bytes::from_static(DATA)))
            .unwrap();
        let first = meta.get_scratch_block(attr.ino, 0).unwrap().unwrap();
        runtime
            .block_on(vfs.write_scratch_block(attr.ino, 0, Bytes::from_static(b"second")))
            .unwrap();

        let second = meta.get_scratch_block(attr.ino, 0).unwrap().unwrap();
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
            b"second".as_slice()
        );
        // The size only grows; SFS-1077 owns truncation.
        assert_eq!(
            meta.get_scratch_attr(attr.ino).unwrap().size,
            DATA.len() as u64
        );
    }

    #[test]
    fn scratch_storage_rejects_writes_below_cached_reserve() {
        let temp = studiofs_test_support::workspace_target_tempdir("scratch-reserve-");
        let (runtime, vfs, meta) = super::super::tests::make_unstarted_test_vfs(&temp, VOLUME);
        let attr = vfs
            .store_scratch_entry(NAME, InodeAttr::new_file(0, ROOT_INO, 0, 0, 0o644))
            .unwrap();
        runtime
            .block_on(vfs.write_scratch_block(attr.ino, 0, Bytes::from_static(DATA)))
            .unwrap();
        let backend = vfs
            .local_payload_backend
            .as_any()
            .downcast_ref::<studiofs_cache::PersistentLocalPayloadBackend>()
            .unwrap();
        backend.run_low_disk_space_for_test().unwrap();
        let error = runtime
            .block_on(vfs.write_scratch_block(attr.ino, 0, Bytes::from_static(b"replacement")))
            .unwrap_err();
        assert_eq!(
            crate::bridge::classify_vfs_error(&error),
            crate::BridgeVfsErrorKind::StorageFull
        );
        assert_eq!(
            runtime
                .block_on(vfs.read_scratch_block(attr.ino, 0))
                .unwrap()
                .unwrap(),
            DATA
        );
        let object = meta.get_scratch_block(attr.ino, 0).unwrap().unwrap();
        assert!(!backend.is_evictable(object));
        assert_eq!(backend.storage_stats().dirty_bytes, 0);
    }
}
