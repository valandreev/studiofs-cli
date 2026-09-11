use super::{StatFs, StudioVfs, VfsResult};

impl StudioVfs {
    /// Get filesystem statistics.
    ///
    /// Returns real inode count and used space from redb counters.
    /// Total capacity is virtual (1 PB) since the backing store is cloud.
    pub fn statfs(&self) -> VfsResult<StatFs> {
        use studiofs_meta::types::counters;

        self.enforce_metadata_serving_lifecycle()?;
        let meta = self.sync_meta();
        let used_inodes = meta.get_counter(counters::TOTAL_INODES)?;
        let used_bytes = meta.get_counter(counters::USED_SPACE)?;
        let bsize = u64::from(self.block_size.max(1));
        let frsize = self.block_size.max(1);

        let total_bytes: u64 = 1024 * 1024 * 1024 * 1024 * 1024;
        let total_blocks = total_bytes / bsize;
        let used_blocks = used_bytes / bsize;
        let free_blocks = total_blocks.saturating_sub(used_blocks);

        Ok(StatFs {
            blocks: total_blocks,
            bfree: free_blocks,
            bavail: free_blocks,
            files: used_inodes,
            ffree: u64::MAX.saturating_sub(used_inodes),
            bsize: frsize,
            namelen: 255,
            frsize,
        })
    }
}
