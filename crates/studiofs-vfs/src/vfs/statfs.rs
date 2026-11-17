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
        Ok(self.sync_meta().logical_usage(&volatile)?)
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

    /// Exact local quota capacity, including accepted growth awaiting persistence.
    pub fn logical_quota_status(&self) -> VfsResult<Option<super::LogicalQuotaStatus>> {
        self.enforce_metadata_serving_lifecycle()?;
        let Some(limit_bytes) = self.logical_quota_limit()? else {
            return Ok(None);
        };
        let pending = self.state.namespace.quota_reservations.lock();
        let used_bytes = self.logical_quota_usage(&pending)?;
        let free_bytes = u128::from(limit_bytes).saturating_sub(used_bytes) as u64;
        Ok(Some(super::LogicalQuotaStatus {
            limit_bytes,
            used_bytes,
            free_bytes,
        }))
    }

    /// Get capacity in filesystem blocks and the local inode count.
    pub fn statfs(&self) -> VfsResult<StatFs> {
        use studiofs_meta::types::counters;

        self.enforce_metadata_serving_lifecycle()?;
        let meta = self.sync_meta();
        let used_inodes = meta.get_counter(counters::TOTAL_INODES)?;
        let frsize = self.block_size.max(1);
        let bsize = u64::from(frsize);
        let (total_blocks, free_blocks) = if let Some(quota) = self.logical_quota_status()? {
            (quota.limit_bytes / bsize, quota.free_bytes / bsize)
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
