//! Cache v3 on-disk layout contract.
//!
//! This module intentionally freezes path conventions before the backend
//! implementation starts so mount/status code can reason about cache shape
//! without mutating legacy state.

use std::path::{Path, PathBuf};

/// Layout contract version for Cache 3.0.
pub const CACHE_V3_LAYOUT_VERSION: u32 = 1;
/// Dedicated Cache 3.0 subtree under the per-volume cache root.
pub const CACHE_V3_SUBTREE: &str = "v3";
/// Marker file used to indicate the v3 layout has been provisioned.
pub const CACHE_V3_LAYOUT_MARKER_FILE: &str = "LAYOUT_V1";
/// Temporary-file area for atomic writes and staging.
pub const CACHE_V3_TMP_DIR: &str = "tmp";
/// Root directory for on-disk payload data.
pub const CACHE_V3_PAYLOAD_DIR: &str = "payload";
/// Nested tree under `payload/` holding block files.
pub const CACHE_V3_PAYLOAD_BLOCKS_DIR: &str = "blocks";
/// Number of shard directories in the payload block tree.
pub const CACHE_V3_PAYLOAD_SHARD_COUNT: usize = 256;
/// Root directory for metadata/index files.
pub const CACHE_V3_INDEX_DIR: &str = "index";
/// redb index file location inside the v3 subtree.
pub const CACHE_V3_REDB_INDEX_FILE: &str = "cache.redb";

/// Fully resolved Cache v3 paths for a cache root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheV3Layout {
    pub cache_root: PathBuf,
    pub v3_root: PathBuf,
    pub layout_marker: PathBuf,
    pub tmp_dir: PathBuf,
    pub payload_root: PathBuf,
    pub payload_blocks_dir: PathBuf,
    pub redb_index_path: PathBuf,
}

impl CacheV3Layout {
    /// Resolve all Cache v3 contract paths from a per-volume cache root.
    pub fn from_cache_root(cache_root: impl AsRef<Path>) -> Self {
        let cache_root = cache_root.as_ref().to_path_buf();
        let v3_root = cache_root.join(CACHE_V3_SUBTREE);
        let payload_root = v3_root.join(CACHE_V3_PAYLOAD_DIR);
        let payload_blocks_dir = payload_root.join(CACHE_V3_PAYLOAD_BLOCKS_DIR);

        Self {
            cache_root,
            v3_root: v3_root.clone(),
            layout_marker: v3_root.join(CACHE_V3_LAYOUT_MARKER_FILE),
            tmp_dir: v3_root.join(CACHE_V3_TMP_DIR),
            payload_root,
            payload_blocks_dir: payload_blocks_dir.clone(),
            redb_index_path: v3_root
                .join(CACHE_V3_INDEX_DIR)
                .join(CACHE_V3_REDB_INDEX_FILE),
        }
    }

    /// Determine the payload shard id for a block object id.
    pub fn payload_shard(object_id: u64) -> u8 {
        (object_id >> 56) as u8
    }

    /// Resolve the canonical payload block path for an object id.
    pub fn payload_block_path(&self, object_id: u64) -> PathBuf {
        let hex = format!("{object_id:016x}");
        let shard = &hex[..2];
        self.payload_blocks_dir
            .join(shard)
            .join(format!("{hex}.block"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_contract_paths_from_cache_root() {
        let layout = CacheV3Layout::from_cache_root("/cache/vol-a");

        assert_eq!(layout.cache_root, PathBuf::from("/cache/vol-a"));
        assert_eq!(layout.v3_root, PathBuf::from("/cache/vol-a/v3"));
        assert_eq!(
            layout.layout_marker,
            PathBuf::from("/cache/vol-a/v3/LAYOUT_V1")
        );
        assert_eq!(layout.tmp_dir, PathBuf::from("/cache/vol-a/v3/tmp"));
        assert_eq!(
            layout.payload_blocks_dir,
            PathBuf::from("/cache/vol-a/v3/payload/blocks")
        );
        assert_eq!(
            layout.redb_index_path,
            PathBuf::from("/cache/vol-a/v3/index/cache.redb")
        );
    }

    #[test]
    fn payload_block_path_uses_two_hex_shards() {
        let layout = CacheV3Layout::from_cache_root("/cache/vol-a");
        let path = layout.payload_block_path(0xAB00_0000_0000_0042);

        assert_eq!(
            path,
            PathBuf::from("/cache/vol-a/v3/payload/blocks/ab/ab00000000000042.block")
        );
    }
}
