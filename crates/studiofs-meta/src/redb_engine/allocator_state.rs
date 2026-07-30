#![allow(
    dead_code,
    reason = "SFS-632 wires this staged private state into PostgreSQL-backed local roots."
)]

use super::*;
use crate::allocator_state::AllocatorStateV1;

const LOCAL_ALLOCATOR_STATE_KEY: &str = "state";

impl RedbMetaEngine {
    pub(crate) fn read_allocator_state(&self) -> MetaResult<Option<AllocatorStateV1>> {
        let txn = self.begin_read_txn()?;
        let table = txn
            .open_table(LOCAL_ALLOCATOR_STATE_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let Some(value) = table
            .get(LOCAL_ALLOCATOR_STATE_KEY)
            .map_err(|error| MetaError::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        AllocatorStateV1::decode(value.value())
            .map(Some)
            .map_err(|error| MetaError::Storage(format!("invalid allocator state: {error:?}")))
    }

    pub(crate) fn replace_allocator_state(&self, state: AllocatorStateV1) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
        {
            let encoded = state.encode();
            let mut table = txn
                .open_table(LOCAL_ALLOCATOR_STATE_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            table
                .insert(LOCAL_ALLOCATOR_STATE_KEY, encoded.as_slice())
                .map_err(|error| MetaError::Storage(error.to_string()))?;
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))
    }
}
