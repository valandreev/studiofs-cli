use super::{StatFs, StudioVfs, VfsResult};

pub(super) struct GrowthReservation<'a> {
    pending: &'a parking_lot::Mutex<Vec<(super::Ino, u64)>>,
    entry: (super::Ino, u64),
}

impl Drop for GrowthReservation<'_> {
    fn drop(&mut self) {
        let mut pending = self.pending.lock();
        if let Some(index) = pending.iter().position(|entry| *entry == self.entry) {
            pending.swap_remove(index);
        }
    }
}

impl StudioVfs {
    fn logical_quota_limit(&self) -> VfsResult<Option<u64>> {
        Ok(self
            .sync_meta()
            .retained_store()
            .map(|store| store.volume_settings())
            .transpose()?
            .and_then(|settings| settings.logical_quota_bytes))
    }

    fn logical_quota_usage(&self, pending: &[(super::Ino, u64)]) -> VfsResult<u128> {
        let mut volatile = std::collections::HashMap::new();
        for (&ino, attr) in self.state.namespace.dirty_attrs.lock().iter() {
            if attr.kind == super::FileType::RegularFile {
                volatile.insert(ino, attr.size);
            }
        }
        for &(ino, size) in pending {
            let eof = volatile.entry(ino).or_default();
            *eof = (*eof).max(size);
        }
        let mut usage = 0u128;
        for attr in self.sync_meta().list_inode_attrs()? {
            if attr.kind == super::FileType::RegularFile {
                usage += u128::from(
                    attr.size
                        .max(volatile.remove(&attr.ino).unwrap_or_default()),
                );
            }
        }
        Ok(usage + volatile.into_values().map(u128::from).sum::<u128>())
    }

    pub(super) fn reserve_logical_growth(
        &self,
        ino: super::Ino,
        current: u64,
        end: u64,
    ) -> VfsResult<Option<GrowthReservation<'_>>> {
        if end <= current {
            return Ok(None);
        }
        let reservations = &self.state.namespace.quota_reservations;
        let mut pending = reservations.lock();
        pending.push((ino, end));
        let check = self.logical_quota_limit().and_then(|limit| {
            if let Some(limit) = limit
                && self.logical_quota_usage(&pending)? > u128::from(limit)
            {
                return Err(super::VfsError::Io(std::io::ErrorKind::StorageFull.into()));
            }
            Ok(())
        });
        if let Err(error) = check {
            pending.pop();
            return Err(error);
        }
        Ok(Some(GrowthReservation {
            pending: reservations,
            entry: (ino, end),
        }))
    }

    /// Get filesystem statistics.
    ///
    /// Returns real inode count and used space from redb counters.
    /// Total capacity is virtual (1 PB) since the backing store is cloud.
    pub fn statfs(&self) -> VfsResult<StatFs> {
        use studiofs_meta::types::counters;

        self.enforce_metadata_serving_lifecycle()?;
        let meta = self.sync_meta();
        let used_inodes = meta.get_counter(counters::TOTAL_INODES)?;
        let frsize = self.block_size.max(1);
        let bsize = u64::from(frsize);
        let (total_blocks, free_blocks) = if let Some(limit) = self.logical_quota_limit()? {
            let pending = self.state.namespace.quota_reservations.lock();
            let used = self.logical_quota_usage(&pending)?;
            let free = u128::from(limit).saturating_sub(used) as u64;
            (limit / bsize, free / bsize)
        } else {
            let total = (1u64 << 50) / bsize;
            let used = meta.get_counter(counters::USED_SPACE)? / bsize;
            (total, total.saturating_sub(used))
        };

        Ok(StatFs {
            blocks: total_blocks,
            bfree: free_blocks,
            bavail: free_blocks,
            files: used_inodes,
            ffree: u64::MAX.saturating_sub(used_inodes),
            bsize: frsize,
            namelen: super::MAX_NAME_BYTES as u32,
            frsize,
        })
    }
}
