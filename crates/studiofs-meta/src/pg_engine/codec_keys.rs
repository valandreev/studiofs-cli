use super::PgMetaEngine;
use crate::engine::{MetaError, MetaResult};
use crate::redb_engine::RedbSnapshot;
use crate::types::Ino;

impl PgMetaEngine {
    pub(super) fn decode_snapshot(bytes: &[u8]) -> MetaResult<RedbSnapshot> {
        serde_json::from_slice(bytes)
            .map_err(|e| MetaError::Serialization(format!("snapshot decode failed: {e}")))
    }

    pub(super) fn encode_postcard<T: serde::Serialize>(val: &T) -> MetaResult<Vec<u8>> {
        postcard::to_stdvec(val)
            .map_err(|e| MetaError::Serialization(format!("postcard encode failed: {e}")))
    }

    pub(super) fn xattr_key(ino: Ino, name: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(8 + name.len());
        key.extend_from_slice(&ino.to_le_bytes());
        key.extend_from_slice(name);
        key
    }

    pub(super) fn acl_key(ino: Ino, principal: &str) -> Vec<u8> {
        let principal = principal.as_bytes();
        let mut key = Vec::with_capacity(8 + principal.len());
        key.extend_from_slice(&ino.to_le_bytes());
        key.extend_from_slice(principal);
        key
    }

    pub(super) fn dentry_key(parent: Ino, name: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(8 + name.len());
        key.extend_from_slice(&parent.to_le_bytes());
        key.extend_from_slice(name);
        key
    }
}
