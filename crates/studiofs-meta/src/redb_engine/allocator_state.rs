#![allow(
    dead_code,
    reason = "SFS-632 wires this staged private state into PostgreSQL-backed local roots."
)]

use super::*;
use crate::allocator_state::AllocatorStateV1;

const LOCAL_ALLOCATOR_STATE_KEY: &str = "state";
const PENDING_SELF_REPLAY_KEY: &str = "pending_self_replay";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PendingSelfReplayState {
    pub(crate) cursor: u64,
    pub(crate) batches: Vec<Vec<crate::events::MetaEvent>>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct EncodedPendingSelfReplayState {
    cursor: u64,
    batches: Vec<Vec<Vec<u8>>>,
}

impl RedbMetaEngine {
    pub(crate) fn read_pending_self_replay_in_txn(
        txn: &RedbWriteTransaction<'_>,
    ) -> MetaResult<PendingSelfReplayState> {
        let table = txn
            .open_table(LOCAL_ALLOCATOR_STATE_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let Some(value) = table
            .get(PENDING_SELF_REPLAY_KEY)
            .map_err(|error| MetaError::Storage(error.to_string()))?
        else {
            return Ok(PendingSelfReplayState::default());
        };
        let encoded: EncodedPendingSelfReplayState = postcard::from_bytes(value.value())
            .map_err(|error| MetaError::Serialization(error.to_string()))?;
        let batches = encoded
            .batches
            .into_iter()
            .map(|batch| {
                batch
                    .into_iter()
                    .map(|bytes| crate::events::decode_event(&bytes))
                    .collect::<MetaResult<Vec<_>>>()
            })
            .collect::<MetaResult<Vec<_>>>()?;
        Ok(PendingSelfReplayState {
            cursor: encoded.cursor,
            batches,
        })
    }

    pub(crate) fn write_pending_self_replay_in_txn(
        txn: &RedbWriteTransaction<'_>,
        state: &PendingSelfReplayState,
    ) -> MetaResult<()> {
        let mut table = txn
            .open_table(LOCAL_ALLOCATOR_STATE_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        if state.batches.is_empty() {
            table
                .remove(PENDING_SELF_REPLAY_KEY)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
        } else {
            let encoded_batches = state
                .batches
                .iter()
                .map(|batch| {
                    batch
                        .iter()
                        .map(crate::events::encode_event)
                        .collect::<MetaResult<Vec<_>>>()
                })
                .collect::<MetaResult<Vec<_>>>()?;
            let encoded = postcard::to_stdvec(&EncodedPendingSelfReplayState {
                cursor: state.cursor,
                batches: encoded_batches,
            })
            .map_err(|error| MetaError::Serialization(error.to_string()))?;
            table
                .insert(PENDING_SELF_REPLAY_KEY, encoded.as_slice())
                .map_err(|error| MetaError::Storage(error.to_string()))?;
        }
        Ok(())
    }

    pub(crate) fn clear_pending_self_replay(&self) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
        {
            let mut table = txn
                .open_table(LOCAL_ALLOCATOR_STATE_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            table
                .remove(PENDING_SELF_REPLAY_KEY)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))
    }

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
        #[cfg(test)]
        if let Some(hook) = self
            .allocator_state_write_hook
            .lock()
            .expect("allocator state write hook lock")
            .take()
        {
            hook();
        }
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
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        self.sync_local_metadata()
    }

    #[cfg(test)]
    pub(crate) fn set_allocator_state_write_hook_for_test(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) {
        *self
            .allocator_state_write_hook
            .lock()
            .expect("allocator state write hook lock") = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn write_invalid_allocator_state_for_test(&self) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
        {
            let mut table = txn
                .open_table(LOCAL_ALLOCATOR_STATE_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            table
                .insert(LOCAL_ALLOCATOR_STATE_KEY, b"invalid".as_slice())
                .map_err(|error| MetaError::Storage(error.to_string()))?;
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        self.sync_local_metadata()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator_state::AllocatorRecord;

    #[test]
    fn sfs623_allocator_state_replace_forces_immediate_durability() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let engine = RedbMetaEngine::open_relaxed_for_test(dir.path().join("meta.redb"));
        let state = AllocatorStateV1::new(
            AllocatorRecord::new(1, 4_096).expect("inode record"),
            AllocatorRecord::new(2, 8_192).expect("payload record"),
        )
        .expect("allocator state");

        engine
            .replace_allocator_state(state)
            .expect("persist allocator state");

        assert_eq!(engine.immediate_barriers_for_test(), 1);
    }
}
