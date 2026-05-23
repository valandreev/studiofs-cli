use super::PgMetaEngine;
use crate::engine::{MetaError, MetaResult};
use crate::redb_engine::RedbSnapshot;

impl PgMetaEngine {
    pub(super) fn decode_snapshot(bytes: &[u8]) -> MetaResult<RedbSnapshot> {
        serde_json::from_slice(bytes)
            .map_err(|e| MetaError::Serialization(format!("snapshot decode failed: {e}")))
    }
}
