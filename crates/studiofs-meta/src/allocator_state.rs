#![allow(
    dead_code,
    reason = "SFS-632 wires this staged private state into PostgreSQL-backed local roots."
)]

use std::ops::Range;

const STATE_VERSION: u8 = 1;
const RECORD_ENCODED_LEN: usize = 11;
const ENCODED_LEN: usize = 1 + 2 * RECORD_ENCODED_LEN;
pub(crate) const COUNTER_BITS: u32 = 39;
pub(crate) const COUNTER_LIMIT: u64 = 1 << COUNTER_BITS;
pub(crate) const MAX_COUNTER: u64 = COUNTER_LIMIT - 1;
pub(crate) const MAX_NAMESPACE: u32 = 0x00ff_ffff;
const LOCAL_ID_BATCH_SIZE: u64 = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum AllocatorStateError {
    #[error("invalid allocator namespace")]
    InvalidNamespace,
    #[error("duplicate allocator namespaces")]
    DuplicateNamespace,
    #[error("invalid allocator reserved-through counter")]
    InvalidReservedThrough,
    #[error("invalid allocator counter")]
    InvalidCounter,
    #[error("invalid allocator state encoding")]
    InvalidEncoding,
    #[error("allocator counter exhausted")]
    CounterExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AllocatorRecord {
    namespace: u32,
    reserved_through: u64,
}

impl AllocatorRecord {
    pub(crate) fn new(namespace: u32, reserved_through: u64) -> Result<Self, AllocatorStateError> {
        let record = Self {
            namespace,
            reserved_through,
        };
        record.validate()?;
        Ok(record)
    }

    pub(crate) const fn namespace(self) -> u32 {
        self.namespace
    }

    pub(crate) const fn reserved_through(self) -> u64 {
        self.reserved_through
    }

    pub(crate) fn id_at(self, counter: u64) -> Result<u64, AllocatorStateError> {
        if counter >= COUNTER_LIMIT {
            return Err(AllocatorStateError::InvalidCounter);
        }
        Ok((u64::from(self.namespace) << COUNTER_BITS) | counter)
    }

    pub(crate) fn reserve(
        self,
        requested_count: u64,
    ) -> Result<(Self, Range<u64>), AllocatorStateError> {
        let available = COUNTER_LIMIT - self.reserved_through;
        if available == 0 || requested_count > available {
            return Err(AllocatorStateError::CounterExhausted);
        }
        let end = self.reserved_through + requested_count.max(LOCAL_ID_BATCH_SIZE).min(available);
        let next = Self::new(self.namespace, end)?;
        Ok((next, self.reserved_through..end))
    }

    fn validate(self) -> Result<(), AllocatorStateError> {
        if self.namespace == 0 || self.namespace > MAX_NAMESPACE {
            return Err(AllocatorStateError::InvalidNamespace);
        }
        if self.reserved_through > COUNTER_LIMIT {
            return Err(AllocatorStateError::InvalidReservedThrough);
        }
        Ok(())
    }

    fn encode(self) -> [u8; RECORD_ENCODED_LEN] {
        let mut bytes = [0; RECORD_ENCODED_LEN];
        bytes[..3].copy_from_slice(&self.namespace.to_be_bytes()[1..]);
        bytes[3..].copy_from_slice(&self.reserved_through.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, AllocatorStateError> {
        if bytes.len() != RECORD_ENCODED_LEN {
            return Err(AllocatorStateError::InvalidEncoding);
        }
        let namespace = u32::from_be_bytes([0, bytes[0], bytes[1], bytes[2]]);
        let reserved_through = u64::from_be_bytes(
            bytes[3..]
                .try_into()
                .map_err(|_| AllocatorStateError::InvalidEncoding)?,
        );
        Self::new(namespace, reserved_through)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AllocatorStateV1 {
    pub(crate) inode: AllocatorRecord,
    pub(crate) payload: AllocatorRecord,
}

impl AllocatorStateV1 {
    pub(crate) fn new(
        inode: AllocatorRecord,
        payload: AllocatorRecord,
    ) -> Result<Self, AllocatorStateError> {
        if inode.namespace() == payload.namespace() {
            return Err(AllocatorStateError::DuplicateNamespace);
        }
        Ok(Self { inode, payload })
    }

    pub(crate) fn encode(self) -> [u8; ENCODED_LEN] {
        let mut bytes = [0; ENCODED_LEN];
        bytes[0] = STATE_VERSION;
        bytes[1..1 + RECORD_ENCODED_LEN].copy_from_slice(&self.inode.encode());
        bytes[1 + RECORD_ENCODED_LEN..].copy_from_slice(&self.payload.encode());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, AllocatorStateError> {
        if bytes.len() != ENCODED_LEN || bytes.first() != Some(&STATE_VERSION) {
            return Err(AllocatorStateError::InvalidEncoding);
        }
        Self::new(
            AllocatorRecord::decode(&bytes[1..1 + RECORD_ENCODED_LEN])?,
            AllocatorRecord::decode(&bytes[1 + RECORD_ENCODED_LEN..])?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sfs623_allocator_state_encodes_lowest_and_highest_valid_ids() {
        let lowest = AllocatorRecord::new(1, 0).expect("lowest record");
        let highest = AllocatorRecord::new(MAX_NAMESPACE, MAX_COUNTER).expect("highest record");

        assert_eq!(lowest.id_at(0).expect("lowest id"), 1 << COUNTER_BITS);
        assert_eq!(
            highest.id_at(MAX_COUNTER).expect("highest id"),
            i64::MAX as u64
        );
    }

    #[test]
    fn sfs623_allocator_state_record_encoding_is_fixed_size() {
        let record = AllocatorRecord::new(0x12_3456, 0x0000_0004_0506_0708).expect("record");

        assert_eq!(record.encode(), [0x12, 0x34, 0x56, 0, 0, 0, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn sfs623_allocator_state_rejects_reserved_namespace_zero() {
        assert_eq!(
            AllocatorRecord::new(0, 0),
            Err(AllocatorStateError::InvalidNamespace)
        );
    }

    #[test]
    fn sfs623_allocator_state_errors_have_display_messages() {
        assert_eq!(
            AllocatorStateError::CounterExhausted.to_string(),
            "allocator counter exhausted"
        );
    }

    #[test]
    fn sfs623_allocator_state_rejects_namespace_overflow() {
        assert_eq!(
            AllocatorRecord::new(MAX_NAMESPACE + 1, 0),
            Err(AllocatorStateError::InvalidNamespace)
        );
    }

    #[test]
    fn sfs623_allocator_state_rejects_exclusive_high_water_overflow() {
        assert_eq!(
            AllocatorRecord::new(1, COUNTER_LIMIT + 1),
            Err(AllocatorStateError::InvalidReservedThrough)
        );
    }

    #[test]
    fn sfs623_allocator_state_rejects_counter_overflow() {
        let record = AllocatorRecord::new(1, 0).expect("record");

        assert_eq!(
            record.id_at(COUNTER_LIMIT),
            Err(AllocatorStateError::InvalidCounter)
        );
    }

    #[test]
    fn sfs623_allocator_state_keeps_equal_counters_distinct_across_namespaces() {
        let inode = AllocatorRecord::new(1, 0).expect("inode record");
        let payload = AllocatorRecord::new(2, 0).expect("payload record");

        assert_ne!(
            inode.id_at(41).expect("inode id"),
            payload.id_at(41).expect("payload id")
        );
    }

    #[test]
    fn sfs623_allocator_state_rejects_duplicate_namespaces() {
        let state = AllocatorStateV1::new(
            AllocatorRecord::new(1, 0).expect("inode record"),
            AllocatorRecord::new(2, 0).expect("payload record"),
        )
        .expect("allocator state");
        let mut encoded = state.encode();
        encoded[1 + RECORD_ENCODED_LEN..1 + RECORD_ENCODED_LEN + 3].copy_from_slice(&[0, 0, 1]);

        assert_eq!(
            AllocatorStateV1::decode(&encoded),
            Err(AllocatorStateError::DuplicateNamespace)
        );
    }

    #[test]
    fn sfs623_allocator_state_accepts_final_exclusive_endpoint_but_cannot_reserve_past_it() {
        let record = AllocatorRecord::new(1, COUNTER_LIMIT - LOCAL_ID_BATCH_SIZE).expect("record");

        let (advanced, reservation) = record.reserve(1).expect("final reservation");

        assert_eq!(
            reservation,
            (COUNTER_LIMIT - LOCAL_ID_BATCH_SIZE)..COUNTER_LIMIT
        );
        assert_eq!(advanced.reserved_through(), COUNTER_LIMIT);
        assert_eq!(
            advanced.reserve(1),
            Err(AllocatorStateError::CounterExhausted)
        );
    }

    #[test]
    fn sfs623_allocator_state_reserves_a_final_partial_batch() {
        let record = AllocatorRecord::new(1, COUNTER_LIMIT - 1).expect("record");

        assert_eq!(
            record.reserve(1),
            Ok((
                AllocatorRecord::new(1, COUNTER_LIMIT).expect("advanced record"),
                (COUNTER_LIMIT - 1)..COUNTER_LIMIT,
            ))
        );
    }

    #[test]
    fn sfs623_allocator_state_restarts_after_the_reserved_tail() {
        let state = AllocatorStateV1::new(
            AllocatorRecord::new(1, 0).expect("inode record"),
            AllocatorRecord::new(2, 0).expect("payload record"),
        )
        .expect("allocator state");
        let (inode, first) = state.inode.reserve(1).expect("first reservation");
        let persisted = AllocatorStateV1::new(inode, state.payload).expect("persisted state");
        let restarted = AllocatorStateV1::decode(&persisted.encode())
            .expect("decode persisted state")
            .inode;

        let (_, second) = restarted.reserve(1).expect("post-restart reservation");

        assert_eq!(first.end, second.start);
    }

    #[test]
    fn sfs623_allocator_state_replaces_persisted_pair_atomically() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let path = dir.path().join("meta.redb");
        let first = AllocatorStateV1::new(
            AllocatorRecord::new(1, 4_096).expect("inode record"),
            AllocatorRecord::new(2, 8_192).expect("payload record"),
        )
        .expect("first state");
        let engine = crate::redb_engine::RedbMetaEngine::open(&path).expect("open metadata");

        assert_eq!(
            engine.read_allocator_state().expect("read empty state"),
            None
        );
        engine
            .replace_allocator_state(first)
            .expect("persist first state");
        drop(engine);

        let engine = crate::redb_engine::RedbMetaEngine::open(&path).expect("reopen metadata");
        assert_eq!(
            engine.read_allocator_state().expect("read first state"),
            Some(first)
        );

        let second = AllocatorStateV1::new(
            AllocatorRecord::new(1, 12_288).expect("inode record"),
            AllocatorRecord::new(2, 16_384).expect("payload record"),
        )
        .expect("second state");
        engine
            .replace_allocator_state(second)
            .expect("replace allocator state");

        assert_eq!(
            engine.read_allocator_state().expect("read second state"),
            Some(second)
        );
    }

    #[test]
    fn sfs623_allocator_state_rejects_partial_or_unknown_versioned_pair() {
        let state = AllocatorStateV1::new(
            AllocatorRecord::new(1, 0).expect("inode record"),
            AllocatorRecord::new(2, 0).expect("payload record"),
        )
        .expect("allocator state");
        let mut encoded = state.encode();
        encoded[0] = 2;

        assert_eq!(
            AllocatorStateV1::decode(&encoded[..encoded.len() - 1]),
            Err(AllocatorStateError::InvalidEncoding)
        );
        assert_eq!(
            AllocatorStateV1::decode(&encoded),
            Err(AllocatorStateError::InvalidEncoding)
        );
    }
}
