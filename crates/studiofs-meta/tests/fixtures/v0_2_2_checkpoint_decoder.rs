// Actual v0.2.2 checkpoint decoder, commit 2680630adc8f61ce39ca1fd59bdd023e8977cb62.
// Only item visibility is widened for this integration test.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct ServingCheckpoint {
    /// Remote-visible inode attributes.
    pub inodes: Vec<(u64, Vec<u8>)>,
    /// Remote-visible directory entries.
    pub dentries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Remote-visible regular-file chunk mappings.
    pub regular_file_base: Vec<(u64, Vec<u8>)>,
    /// Remote-visible symlink targets.
    pub symlinks: Vec<(u64, Vec<u8>)>,
    /// Remote-visible extended attributes.
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    /// Remote-visible ACL rules.
    pub acl_rules: Vec<(Vec<u8>, u8)>,
    /// Replay-derived directory merges as `(alias_ino, canonical_ino)` pairs.
    ///
    /// Appended in format version 2; peers that created an alias may still
    /// publish events naming it after this checkpoint's cut.
    pub directory_aliases: Vec<(u64, u64)>,
}

/// Format version 1 layout: the version 2 fields without directory aliases.
#[derive(Deserialize)]
struct ServingCheckpointV1 {
    inodes: Vec<(u64, Vec<u8>)>,
    dentries: Vec<(Vec<u8>, Vec<u8>)>,
    regular_file_base: Vec<(u64, Vec<u8>)>,
    symlinks: Vec<(u64, Vec<u8>)>,
    xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    acl_rules: Vec<(Vec<u8>, u8)>,
}

impl ServingCheckpoint {
    /// The persisted checkpoint format version.
    pub const FORMAT_VERSION: i16 = 2;
    /// The serving-state schema version.
    pub const SCHEMA_VERSION: i16 = 1;

    /// Whether checkpoints written with `version` can be decoded.
    pub fn supports_format_version(version: i16) -> bool {
        (1..=Self::FORMAT_VERSION).contains(&version)
    }

    /// Decodes either format version.
    ///
    /// Both layouts share the version 1 prefix. Only an empty remainder means
    /// version 1; a present version 2 alias suffix must decode completely.
    pub fn decode(bytes: &[u8]) -> Result<Self, postcard::Error> {
        let (v1, suffix) = postcard::take_from_bytes::<ServingCheckpointV1>(bytes)?;
        let directory_aliases = if suffix.is_empty() {
            Vec::new()
        } else {
            let (aliases, remainder) = postcard::take_from_bytes(suffix)?;
            if !remainder.is_empty() {
                return Err(postcard::Error::DeserializeBadEncoding);
            }
            aliases
        };
        Ok(Self {
            inodes: v1.inodes,
            dentries: v1.dentries,
            regular_file_base: v1.regular_file_base,
            symlinks: v1.symlinks,
            xattrs: v1.xattrs,
            acl_rules: v1.acl_rules,
            directory_aliases,
        })
    }
}
