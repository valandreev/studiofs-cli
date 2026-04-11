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
/// Extension used by committed Cache 3.0 payload files.
pub const CACHE_V3_PAYLOAD_BLOCK_FILE_EXTENSION: &str = "block";
/// Number of shard directories in the payload block tree.
pub const CACHE_V3_PAYLOAD_SHARD_COUNT: usize = 256 * 256;
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
    pub index_dir: PathBuf,
    pub redb_index_path: PathBuf,
}

impl CacheV3Layout {
    /// Resolve all Cache v3 contract paths from a per-volume cache root.
    pub fn from_cache_root(cache_root: impl AsRef<Path>) -> Self {
        let cache_root = cache_root.as_ref().to_path_buf();
        let v3_root = cache_root.join(CACHE_V3_SUBTREE);
        let payload_root = v3_root.join(CACHE_V3_PAYLOAD_DIR);
        let payload_blocks_dir = payload_root.join(CACHE_V3_PAYLOAD_BLOCKS_DIR);
        let index_dir = v3_root.join(CACHE_V3_INDEX_DIR);

        Self {
            cache_root,
            v3_root: v3_root.clone(),
            layout_marker: v3_root.join(CACHE_V3_LAYOUT_MARKER_FILE),
            tmp_dir: v3_root.join(CACHE_V3_TMP_DIR),
            payload_root,
            payload_blocks_dir: payload_blocks_dir.clone(),
            index_dir: index_dir.clone(),
            redb_index_path: index_dir.join(CACHE_V3_REDB_INDEX_FILE),
        }
    }

    /// Determine the two-level payload shard for a block object id.
    ///
    /// We hash object ids before slicing shard bytes so early sequential ids do
    /// not concentrate into a single top-level shard directory.
    pub fn payload_shard(object_id: u64) -> [u8; 2] {
        let hash = splitmix64(object_id);
        let bytes = hash.to_be_bytes();
        [bytes[0], bytes[1]]
    }

    /// Resolve the canonical payload block path for an object id.
    pub fn payload_block_path(&self, object_id: u64) -> PathBuf {
        self.v3_root
            .join(self.payload_block_relative_path(object_id))
    }

    /// Resolve the canonical payload block path relative to `v3_root`.
    pub fn payload_block_relative_path(&self, object_id: u64) -> PathBuf {
        let [shard_0, shard_1] = Self::payload_shard(object_id);
        PathBuf::from(CACHE_V3_PAYLOAD_DIR)
            .join(CACHE_V3_PAYLOAD_BLOCKS_DIR)
            .join(format!("{shard_0:02x}"))
            .join(format!("{shard_1:02x}"))
            .join(Self::payload_block_file_name(object_id))
    }

    /// Canonical payload filename for an object id.
    pub fn payload_block_file_name(object_id: u64) -> String {
        format!("{object_id:016x}.{CACHE_V3_PAYLOAD_BLOCK_FILE_EXTENSION}")
    }

    /// Parse an object id from a canonical payload filename.
    pub fn parse_payload_block_file_name(file_name: &str) -> Option<u64> {
        let (hex, ext) = file_name.rsplit_once('.')?;
        if ext != CACHE_V3_PAYLOAD_BLOCK_FILE_EXTENSION || hex.len() != 16 {
            return None;
        }
        if !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        u64::from_str_radix(hex, 16).ok()
    }
}

fn splitmix64(input: u64) -> u64 {
    let mut z = input.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
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
        assert_eq!(layout.index_dir, PathBuf::from("/cache/vol-a/v3/index"));
        assert_eq!(
            layout.redb_index_path,
            PathBuf::from("/cache/vol-a/v3/index/cache.redb")
        );
    }

    #[test]
    fn payload_block_path_uses_two_hex_shards() {
        let layout = CacheV3Layout::from_cache_root("/cache/vol-a");
        let object_id = 0xAB00_0000_0000_0042;
        let hex = format!("{object_id:016x}");
        let [shard_0, shard_1] = CacheV3Layout::payload_shard(object_id);
        let path = layout.payload_block_path(object_id);

        assert_eq!(
            path,
            PathBuf::from(format!(
                "/cache/vol-a/v3/payload/blocks/{shard_0:02x}/{shard_1:02x}/{hex}.block"
            ))
        );

        assert_eq!(
            layout.payload_block_relative_path(object_id),
            PathBuf::from(format!(
                "payload/blocks/{shard_0:02x}/{shard_1:02x}/{hex}.block"
            ))
        );
    }

    #[test]
    fn payload_shard_spreads_sequential_ids() {
        let shard_1 = CacheV3Layout::payload_shard(1);
        let shard_2 = CacheV3Layout::payload_shard(2);
        let shard_257 = CacheV3Layout::payload_shard(257);

        assert_ne!(shard_1, shard_2);
        assert_ne!(shard_1, shard_257);
        assert_eq!(shard_1, CacheV3Layout::payload_shard(1));
        assert_eq!(shard_2, CacheV3Layout::payload_shard(2));
        assert_eq!(shard_257, CacheV3Layout::payload_shard(257));
    }

    #[test]
    fn parse_payload_block_file_name_accepts_canonical_shape() {
        let object_id = 0xABCD_EF12_0000_0042;
        let file_name = CacheV3Layout::payload_block_file_name(object_id);
        assert_eq!(
            CacheV3Layout::parse_payload_block_file_name(&file_name),
            Some(object_id)
        );
    }

    #[test]
    fn parse_payload_block_file_name_rejects_noncanonical_names() {
        assert_eq!(CacheV3Layout::parse_payload_block_file_name(""), None);
        assert_eq!(
            CacheV3Layout::parse_payload_block_file_name("0000000000000001"),
            None
        );
        assert_eq!(
            CacheV3Layout::parse_payload_block_file_name("xyz.block"),
            None
        );
        assert_eq!(
            CacheV3Layout::parse_payload_block_file_name("0000000000000001.tmp"),
            None
        );
    }
}
