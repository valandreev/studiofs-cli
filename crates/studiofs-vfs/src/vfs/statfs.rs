use super::{StatFs, StudioVfs, VfsResult};

/// Reported capacity of a volume without a logical quota.
const UNLIMITED_CAPACITY_BYTES: u64 = 1 << 50;

/// Growth admitted against the quota but not yet visible in dirty attrs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PendingGrowth {
    pub(super) ino: super::Ino,
    /// Logical EOF the admitted operation may extend the inode to.
    pub(super) projected_eof: u64,
}

/// Releases its [`PendingGrowth`] when the admitted operation finishes.
pub(super) struct GrowthReservation<'a> {
    pending: &'a parking_lot::Mutex<Vec<PendingGrowth>>,
    growth: PendingGrowth,
}

impl Drop for GrowthReservation<'_> {
    fn drop(&mut self) {
        let mut pending = self.pending.lock();
        if let Some(index) = pending.iter().position(|growth| *growth == self.growth) {
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

    fn logical_quota_usage(&self, pending: &[PendingGrowth]) -> VfsResult<u128> {
        let mut volatile = std::collections::HashMap::new();
        for (&ino, attr) in self.state.namespace.dirty_attrs.lock().iter() {
            if attr.kind == super::FileType::RegularFile {
                volatile.insert(ino, attr.size);
            }
        }
        for growth in pending {
            let eof = volatile.entry(growth.ino).or_default();
            *eof = (*eof).max(growth.projected_eof);
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
        // Unlimited volumes skip the client-wide reservation lock. A quota
        // enabled concurrently applies to growth admitted after it is read.
        let Some(limit) = self.logical_quota_limit()? else {
            return Ok(None);
        };
        let reservations = &self.state.namespace.quota_reservations;
        let mut pending = reservations.lock();
        let growth = PendingGrowth {
            ino,
            projected_eof: end,
        };
        pending.push(growth);
        let check = self.logical_quota_usage(&pending).and_then(|used| {
            if used > u128::from(limit) {
                Err(super::VfsError::Io(std::io::ErrorKind::StorageFull.into()))
            } else {
                Ok(())
            }
        });
        if let Err(error) = check {
            pending.pop();
            return Err(error);
        }
        Ok(Some(GrowthReservation {
            pending: reservations,
            growth,
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

    /// Exact capacity in bytes: the logical quota when configured, otherwise
    /// a virtual 1 PiB less referenced payload space.
    pub fn capacity_bytes(&self) -> VfsResult<super::VolumeCapacity> {
        self.enforce_metadata_serving_lifecycle()?;
        if let Some(quota) = self.logical_quota_status()? {
            return Ok(super::VolumeCapacity {
                total_bytes: quota.limit_bytes,
                free_bytes: quota.free_bytes,
            });
        }
        let used = self
            .sync_meta()
            .get_counter(studiofs_meta::types::counters::USED_SPACE)?;
        Ok(super::VolumeCapacity {
            total_bytes: UNLIMITED_CAPACITY_BYTES,
            free_bytes: UNLIMITED_CAPACITY_BYTES.saturating_sub(used),
        })
    }

    /// Get capacity in filesystem blocks and the local inode count.
    pub fn statfs(&self) -> VfsResult<StatFs> {
        let capacity = self.capacity_bytes()?;
        let used_inodes = self
            .sync_meta()
            .get_counter(studiofs_meta::types::counters::TOTAL_INODES)?;
        let frsize = self.block_size.max(1);
        let bsize = u64::from(frsize);
        let total_blocks = capacity.total_bytes / bsize;
        let free_blocks = capacity.free_bytes / bsize;

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
