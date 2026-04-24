//! Persistent local payload object runtime layout contract.
//!
//! The UUID-scoped cache root itself is the runtime root. Production code
//! should avoid versioned subtree vocabulary for the active on-disk layout.

use std::io;
use std::path::{Path, PathBuf};

/// Temporary-file area for atomic writes and staging.
pub const LOCAL_BLOCK_TMP_DIR: &str = "tmp";
/// Root directory for on-disk payload data.
pub const LOCAL_BLOCK_PAYLOAD_DIR: &str = "payload";
/// Root directory for upload data used by writeback.
pub const LOCAL_BLOCK_UPLOAD_STAGE_DIR: &str = "upload";
/// Nested tree under `payload/` holding payload object files.
pub const LOCAL_BLOCK_PAYLOAD_OBJECTS_DIR: &str = "objects";
/// Extension used by committed payload files.
pub const LOCAL_BLOCK_PAYLOAD_OBJECT_FILE_EXTENSION: &str = "object";
/// Extension used by upload files.
pub const LOCAL_BLOCK_UPLOAD_STAGE_FILE_EXTENSION: &str = "stage";
/// Number of shard directories in the payload object tree.
pub const LOCAL_BLOCK_PAYLOAD_SHARD_COUNT: usize = 256 * 256;
/// Root directory for metadata/index files.
pub const LOCAL_BLOCK_INDEX_DIR: &str = "index";
/// redb index file location inside the runtime subtree.
pub const LOCAL_BLOCK_REDB_INDEX_FILE: &str = "cache.redb";

/// Fully resolved local payload object runtime paths for a cache root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalBlockLayout {
    pub cache_root: PathBuf,
    pub runtime_root: PathBuf,
    pub tmp_dir: PathBuf,
    pub payload_root: PathBuf,
    pub payload_objects_dir: PathBuf,
    pub upload_stage_root: PathBuf,
    pub upload_stage_objects_dir: PathBuf,
    pub index_dir: PathBuf,
    pub redb_index_path: PathBuf,
}

impl LocalBlockLayout {
    /// Resolve all runtime paths from a per-volume cache root.
    pub fn from_cache_root(cache_root: impl AsRef<Path>) -> Self {
        let cache_root = cache_root.as_ref().to_path_buf();
        let runtime_root = cache_root.clone();
        let payload_root = runtime_root.join(LOCAL_BLOCK_PAYLOAD_DIR);
        let payload_objects_dir = payload_root.join(LOCAL_BLOCK_PAYLOAD_OBJECTS_DIR);
        let upload_stage_root = runtime_root.join(LOCAL_BLOCK_UPLOAD_STAGE_DIR);
        let upload_stage_objects_dir = upload_stage_root.join(LOCAL_BLOCK_PAYLOAD_OBJECTS_DIR);
        let index_dir = runtime_root.join(LOCAL_BLOCK_INDEX_DIR);

        Self {
            cache_root,
            runtime_root: runtime_root.clone(),
            tmp_dir: runtime_root.join(LOCAL_BLOCK_TMP_DIR),
            payload_root,
            payload_objects_dir: payload_objects_dir.clone(),
            upload_stage_root,
            upload_stage_objects_dir: upload_stage_objects_dir.clone(),
            index_dir: index_dir.clone(),
            redb_index_path: index_dir.join(LOCAL_BLOCK_REDB_INDEX_FILE),
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

    /// Resolve the canonical payload object path for an object id.
    pub fn payload_object_path(&self, object_id: u64) -> PathBuf {
        self.runtime_root
            .join(self.payload_object_relative_path(object_id))
    }

    /// Resolve the canonical payload object path relative to the runtime root.
    pub fn payload_object_relative_path(&self, object_id: u64) -> PathBuf {
        let [shard_0, shard_1] = Self::payload_shard(object_id);
        PathBuf::from(LOCAL_BLOCK_PAYLOAD_DIR)
            .join(LOCAL_BLOCK_PAYLOAD_OBJECTS_DIR)
            .join(format!("{shard_0:02x}"))
            .join(format!("{shard_1:02x}"))
            .join(Self::payload_object_file_name(object_id))
    }

    /// Canonical payload filename for an object id.
    pub fn payload_object_file_name(object_id: u64) -> String {
        format!("{object_id:016x}.{LOCAL_BLOCK_PAYLOAD_OBJECT_FILE_EXTENSION}")
    }

    /// Resolve the canonical upload path for an object id.
    pub fn upload_stage_object_path(&self, object_id: u64) -> PathBuf {
        self.runtime_root
            .join(self.upload_stage_object_relative_path(object_id))
    }

    /// Resolve the canonical upload path relative to the runtime root.
    pub fn upload_stage_object_relative_path(&self, object_id: u64) -> PathBuf {
        let [shard_0, shard_1] = Self::payload_shard(object_id);
        PathBuf::from(LOCAL_BLOCK_UPLOAD_STAGE_DIR)
            .join(LOCAL_BLOCK_PAYLOAD_OBJECTS_DIR)
            .join(format!("{shard_0:02x}"))
            .join(format!("{shard_1:02x}"))
            .join(Self::upload_stage_object_file_name(object_id))
    }

    /// Canonical upload filename for an object id.
    pub fn upload_stage_object_file_name(object_id: u64) -> String {
        format!("{object_id:016x}.{LOCAL_BLOCK_UPLOAD_STAGE_FILE_EXTENSION}")
    }

    /// Parse an object id from a canonical payload filename.
    pub fn parse_payload_object_file_name(file_name: &str) -> Option<u64> {
        let (hex, ext) = file_name.rsplit_once('.')?;
        if ext != LOCAL_BLOCK_PAYLOAD_OBJECT_FILE_EXTENSION || hex.len() != 16 {
            return None;
        }
        if !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        u64::from_str_radix(hex, 16).ok()
    }

    /// Create the required runtime directories.
    pub fn provision(&self) -> io::Result<()> {
        std::fs::create_dir_all(&self.tmp_dir)?;
        std::fs::create_dir_all(&self.payload_objects_dir)?;
        std::fs::create_dir_all(&self.upload_stage_objects_dir)?;
        std::fs::create_dir_all(&self.index_dir)?;
        Ok(())
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
        let layout = LocalBlockLayout::from_cache_root("/cache/vol-a");

        assert_eq!(layout.cache_root, PathBuf::from("/cache/vol-a"));
        assert_eq!(layout.runtime_root, PathBuf::from("/cache/vol-a"));
        assert_eq!(layout.tmp_dir, PathBuf::from("/cache/vol-a/tmp"));
        assert_eq!(
            layout.payload_objects_dir,
            PathBuf::from("/cache/vol-a/payload/objects")
        );
        assert_eq!(
            layout.upload_stage_objects_dir,
            PathBuf::from("/cache/vol-a/upload/objects")
        );
        assert_eq!(layout.index_dir, PathBuf::from("/cache/vol-a/index"));
        assert_eq!(
            layout.redb_index_path,
            PathBuf::from("/cache/vol-a/index/cache.redb")
        );
    }

    #[test]
    fn payload_object_path_uses_two_hex_shards() {
        let layout = LocalBlockLayout::from_cache_root("/cache/vol-a");
        let object_id = 0xAB00_0000_0000_0042;
        let hex = format!("{object_id:016x}");
        let [shard_0, shard_1] = LocalBlockLayout::payload_shard(object_id);
        let path = layout.payload_object_path(object_id);

        assert_eq!(
            path,
            PathBuf::from(format!(
                "/cache/vol-a/payload/objects/{shard_0:02x}/{shard_1:02x}/{hex}.object"
            ))
        );

        assert_eq!(
            layout.payload_object_relative_path(object_id),
            PathBuf::from(format!(
                "payload/objects/{shard_0:02x}/{shard_1:02x}/{hex}.object"
            ))
        );
        assert_eq!(
            layout.upload_stage_object_path(object_id),
            PathBuf::from(format!(
                "/cache/vol-a/upload/objects/{shard_0:02x}/{shard_1:02x}/{hex}.stage"
            ))
        );
    }

    #[test]
    fn payload_shard_spreads_sequential_ids() {
        let shard_1 = LocalBlockLayout::payload_shard(1);
        let shard_2 = LocalBlockLayout::payload_shard(2);
        let shard_257 = LocalBlockLayout::payload_shard(257);

        assert_ne!(shard_1, shard_2);
        assert_ne!(shard_1, shard_257);
        assert_eq!(shard_1, LocalBlockLayout::payload_shard(1));
        assert_eq!(shard_2, LocalBlockLayout::payload_shard(2));
        assert_eq!(shard_257, LocalBlockLayout::payload_shard(257));
    }

    #[test]
    fn parse_payload_object_file_name_accepts_canonical_shape() {
        let object_id = 0xABCD_EF12_0000_0042;
        let file_name = LocalBlockLayout::payload_object_file_name(object_id);
        assert_eq!(
            LocalBlockLayout::parse_payload_object_file_name(&file_name),
            Some(object_id)
        );
    }

    #[test]
    fn parse_payload_object_file_name_rejects_noncanonical_names() {
        assert_eq!(LocalBlockLayout::parse_payload_object_file_name(""), None);
        assert_eq!(
            LocalBlockLayout::parse_payload_object_file_name("0000000000000001"),
            None
        );
        assert_eq!(
            LocalBlockLayout::parse_payload_object_file_name("xyz.object"),
            None
        );
        assert_eq!(
            LocalBlockLayout::parse_payload_object_file_name("0000000000000001.tmp"),
            None
        );
    }
}
