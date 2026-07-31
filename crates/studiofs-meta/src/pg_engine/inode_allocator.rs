//! Owns PG-backed inode and client-slot allocation, including local payload namespaces.

use super::{
    LOCAL_PG_CLIENT_SLOT_COUNTER, PG_NEXT_ALLOCATOR_NAMESPACE_COUNTER, PG_NEXT_CLIENT_SLOT_COUNTER,
    PG_NEXT_INODE_COUNTER, PgMetaEngine,
};
use crate::allocator_state::{AllocatorRecord, AllocatorStateV1, COUNTER_LIMIT, MAX_NAMESPACE};
use crate::engine::{MetaError, MetaInodes, MetaResult};
use crate::redb_engine::RedbMetaEngine;
use std::ops::Range;
use std::sync::atomic::Ordering;
use tracing::warn;

#[derive(Default)]
pub(super) struct AllocatorRuntime {
    state: Option<AllocatorStateV1>,
    inode_batch: Range<u64>,
    payload_batch: Range<u64>,
}

impl PgMetaEngine {
    pub(super) fn load_persisted_client_slot(local: &RedbMetaEngine) -> MetaResult<Option<u64>> {
        let stored = local.get_counter(LOCAL_PG_CLIENT_SLOT_COUNTER)?;
        Ok(if stored == 0 { None } else { Some(stored - 1) })
    }

    pub(super) fn persist_client_slot(local: &RedbMetaEngine, slot: u64) -> MetaResult<()> {
        local.set_counter(
            LOCAL_PG_CLIENT_SLOT_COUNTER,
            slot.checked_add(1)
                .ok_or_else(|| MetaError::Storage("pg client slot overflow".to_string()))?,
        )
    }

    pub(super) fn cache_client_slot(&self, slot: u64) {
        self.client_slot_plus_one
            .store(slot.saturating_add(1), Ordering::Release);
    }

    pub(super) fn reserve_global_client_slot(
        postgres_url: &str,
        volume_id: i64,
    ) -> MetaResult<u64> {
        let postgres_url = postgres_url.to_string();
        let connect_timeout = Self::default_connect_timeout();
        let command_timeout = Self::default_command_timeout();
        Self::run_async_once(async move {
            Self::with_postgres_client_timeout_once(
                postgres_url,
                connect_timeout,
                command_timeout,
                "reserve client slot",
                move |mut client| {
                    Box::pin(async move {
                        let tx = client.transaction().await.map_err(|error| {
                            MetaError::Storage(format!(
                                "postgres client-slot transaction failed: {}",
                                Self::format_pg_error(error)
                            ))
                        })?;
                        tx.execute(
                            "INSERT INTO counters (vol, name, value) VALUES ($1, $2, 0) ON CONFLICT (vol, name) DO NOTHING",
                            &[&volume_id, &PG_NEXT_CLIENT_SLOT_COUNTER],
                        )
                        .await
                        .map_err(|error| MetaError::Storage(format!("postgres client-slot seed failed: {}", Self::format_pg_error(error))))?;
                        let row = tx
                            .query_one(
                                "UPDATE counters SET value = value + 1 WHERE vol = $1 AND name = $2 RETURNING value - 1",
                                &[&volume_id, &PG_NEXT_CLIENT_SLOT_COUNTER],
                            )
                            .await
                            .map_err(|error| MetaError::Storage(format!("postgres client-slot reservation failed: {}", Self::format_pg_error(error))))?;
                        tx.commit().await.map_err(|error| MetaError::Storage(format!("postgres client-slot commit failed: {}", Self::format_pg_error(error))))?;
                        let slot: i64 = row.get(0);
                        u64::try_from(slot).map_err(|_| MetaError::Storage(format!("invalid negative pg client slot: {slot}")))
                    })
                },
            )
            .await
        })
    }

    pub(super) fn ensure_client_slot(
        local: &RedbMetaEngine,
        postgres_url: &str,
        volume_id: i64,
    ) -> MetaResult<u64> {
        if let Some(slot) = Self::load_persisted_client_slot(local)? {
            return Ok(slot);
        }
        let slot = Self::reserve_global_client_slot(postgres_url, volume_id)?;
        Self::persist_client_slot(local, slot)?;
        Ok(slot)
    }

    fn mint_allocator_state(&self) -> MetaResult<AllocatorStateV1> {
        let command_pool = self.command_pool.clone();
        let command_timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Self::run_async_once(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "mint allocator namespaces",
                move |client| {
                    Box::pin(async move {
                        let tx = client.transaction().await.map_err(|error| {
                            MetaError::Storage(format!(
                                "postgres allocator transaction failed: {}",
                                Self::format_pg_error(error)
                            ))
                        })?;
                        let legacy = tx
                            .query_opt(
                                "SELECT value FROM counters WHERE vol = $1 AND name = $2",
                                &[&volume_id, &PG_NEXT_INODE_COUNTER],
                            )
                            .await
                            .map_err(|error| MetaError::Storage(format!("postgres legacy allocator guard failed: {}", Self::format_pg_error(error))))?;
                        if legacy.is_some() {
                            return Err(MetaError::Storage(
                                "legacy next_inode counter requires allocator migration before writes can be enabled; see docs/plans/sfs-623-collision-free-local-id-allocation.md".to_string(),
                            ));
                        }
                        tx.execute(
                            "INSERT INTO counters (vol, name, value) VALUES ($1, $2, 1) ON CONFLICT (vol, name) DO NOTHING",
                            &[&volume_id, &PG_NEXT_ALLOCATOR_NAMESPACE_COUNTER],
                        )
                        .await
                        .map_err(|error| MetaError::Storage(format!("postgres allocator namespace seed failed: {}", Self::format_pg_error(error))))?;
                        let row = tx
                            .query_one(
                                "UPDATE counters SET value = value + 2 WHERE vol = $1 AND name = $2 RETURNING value - 2, value - 1",
                                &[&volume_id, &PG_NEXT_ALLOCATOR_NAMESPACE_COUNTER],
                            )
                            .await
                            .map_err(|error| MetaError::Storage(format!("postgres allocator namespace reservation failed: {}", Self::format_pg_error(error))))?;
                        let inode: i64 = row.get(0);
                        let payload: i64 = row.get(1);
                        let inode = u32::try_from(inode).map_err(|_| MetaError::Storage("invalid allocator inode namespace".to_string()))?;
                        let payload = u32::try_from(payload).map_err(|_| MetaError::Storage("invalid allocator payload namespace".to_string()))?;
                        if inode == 0 || payload == 0 || inode > MAX_NAMESPACE || payload > MAX_NAMESPACE {
                            return Err(MetaError::Storage("postgres allocator namespace exhausted".to_string()));
                        }
                        let state = AllocatorStateV1::new(
                            AllocatorRecord::new(inode, 0).map_err(|error| MetaError::Storage(error.to_string()))?,
                            AllocatorRecord::new(payload, 0).map_err(|error| MetaError::Storage(error.to_string()))?,
                        )
                        .map_err(|error| MetaError::Storage(error.to_string()))?;
                        tx.commit().await.map_err(|error| MetaError::Storage(format!("postgres allocator namespace commit failed: {}", Self::format_pg_error(error))))?;
                        Ok(state)
                    })
                },
            )
            .await
        })
    }

    pub(super) fn bootstrap_allocator_state(&self) {
        let loaded_state_result = match self.local.read_allocator_state() {
            Ok(Some(state)) => Ok(state),
            Ok(None) | Err(_) => self.mint_allocator_state().and_then(|state| {
                self.local.replace_allocator_state(state)?;
                Ok(state)
            }),
        };
        match loaded_state_result {
            Ok(state) => {
                let inode_id_headroom = state.inode.reserved_through() < COUNTER_LIMIT;
                let object_id_headroom = state.payload.reserved_through() < COUNTER_LIMIT;
                if let Ok(mut runtime) = self.allocator_runtime.lock() {
                    runtime.state = Some(state);
                    runtime.inode_batch = 0..0;
                    runtime.payload_batch = 0..0;
                }
                self.inode_id_headroom_available
                    .store(inode_id_headroom, Ordering::Release);
                self.object_id_headroom_available
                    .store(object_id_headroom, Ordering::Release);
            }
            Err(error) => {
                self.inode_id_headroom_available
                    .store(false, Ordering::Release);
                self.object_id_headroom_available
                    .store(false, Ordering::Release);
                if error.to_string().contains("legacy next_inode") {
                    warn!(error = %error, volume_id = self.volume_id, "id_allocator_legacy_migration_required");
                } else {
                    warn!(error = %error, volume_id = self.volume_id, "id_allocator_recovery_required");
                }
            }
        }
    }

    fn take_from_batch<T>(
        runtime: &mut AllocatorRuntime,
        count: u64,
        inode: bool,
        build: impl FnOnce(AllocatorRecord, Range<u64>) -> MetaResult<T>,
    ) -> MetaResult<Option<T>> {
        let state = runtime
            .state
            .ok_or_else(|| MetaError::Storage("allocator namespace is unavailable".to_string()))?;
        let (first, end) =
            if inode {
                let batch = &runtime.inode_batch;
                let available = batch.end.saturating_sub(batch.start);
                if available < count {
                    return Ok(None);
                }
                let end = batch.start.checked_add(count).ok_or_else(|| {
                    MetaError::Storage("allocator batch range overflow".to_string())
                })?;
                (batch.start, end)
            } else {
                let batch = &runtime.payload_batch;
                let available = batch.end.saturating_sub(batch.start);
                if available < count {
                    return Ok(None);
                }
                let end = batch.start.checked_add(count).ok_or_else(|| {
                    MetaError::Storage("allocator batch range overflow".to_string())
                })?;
                (batch.start, end)
            };
        let record = if inode { state.inode } else { state.payload };
        let value = build(record, first..end)?;
        if inode {
            runtime.inode_batch.start = end;
        } else {
            runtime.payload_batch.start = end;
        }
        Ok(Some(value))
    }

    fn take_ids_from_batch(
        runtime: &mut AllocatorRuntime,
        count: u64,
        inode: bool,
    ) -> MetaResult<Option<Vec<u64>>> {
        Self::take_from_batch(runtime, count, inode, |record, range| {
            range
                .map(|counter| {
                    record
                        .id_at(counter)
                        .map_err(|error| MetaError::Storage(error.to_string()))
                })
                .collect()
        })
    }

    fn take_id_from_batch(
        runtime: &mut AllocatorRuntime,
        count: u64,
        inode: bool,
    ) -> MetaResult<Option<u64>> {
        if count != 1 {
            return Err(MetaError::Storage(
                "allocator single-ID path requires count = 1".to_string(),
            ));
        }
        Self::take_from_batch(runtime, count, inode, |record, range| {
            let Some(counter) = range.into_iter().next() else {
                return Err(MetaError::Storage(
                    "allocator single-ID batch is empty".to_string(),
                ));
            };
            record
                .id_at(counter)
                .map_err(|error| MetaError::Storage(error.to_string()))
        })
    }

    fn allocate_with<T>(
        &self,
        count: u32,
        inode: bool,
        take: fn(&mut AllocatorRuntime, u64, bool) -> MetaResult<Option<T>>,
    ) -> MetaResult<T> {
        let count = u64::from(count);
        {
            let mut runtime = self
                .allocator_runtime
                .lock()
                .map_err(|_| MetaError::Storage("allocator runtime lock poisoned".to_string()))?;
            if let Some(value) = take(&mut runtime, count, inode)? {
                return Ok(value);
            }
        }

        let _refill_guard = self
            .allocator_refill_lock
            .lock()
            .map_err(|_| MetaError::Storage("allocator refill lock poisoned".to_string()))?;
        let (next_state, next_batch) = {
            let mut runtime = self
                .allocator_runtime
                .lock()
                .map_err(|_| MetaError::Storage("allocator runtime lock poisoned".to_string()))?;
            if let Some(value) = take(&mut runtime, count, inode)? {
                return Ok(value);
            }
            let state = runtime.state.ok_or_else(|| {
                MetaError::Storage("allocator namespace is unavailable".to_string())
            })?;
            let record = if inode { state.inode } else { state.payload };
            // Counter exhaustion is terminal for this mount; namespace minting stays
            // bootstrap-only so foreground allocation never re-enters PostgreSQL.
            let (advanced, next_batch) = record.reserve(count).map_err(|error| {
                if inode {
                    self.inode_id_headroom_available
                        .store(false, Ordering::Release);
                } else {
                    self.object_id_headroom_available
                        .store(false, Ordering::Release);
                }
                warn!(error = %error, volume_id = self.volume_id, "id_allocator_counter_exhausted");
                MetaError::Storage(error.to_string())
            })?;
            let next_state = if inode {
                AllocatorStateV1::new(advanced, state.payload)
            } else {
                AllocatorStateV1::new(state.inode, advanced)
            }
            .map_err(|error| MetaError::Storage(error.to_string()))?;
            (next_state, next_batch)
        };

        self.local.replace_allocator_state(next_state)?;

        let mut runtime = self
            .allocator_runtime
            .lock()
            .map_err(|_| MetaError::Storage("allocator runtime lock poisoned".to_string()))?;
        runtime.state = Some(next_state);
        if inode {
            runtime.inode_batch = next_batch;
        } else {
            runtime.payload_batch = next_batch;
        }
        take(&mut runtime, count, inode)?.ok_or_else(|| {
            MetaError::Storage("allocator batch refill did not provide requested IDs".to_string())
        })
    }

    fn allocate_ids(&self, count: u32, inode: bool) -> MetaResult<Vec<u64>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        self.allocate_with(count, inode, Self::take_ids_from_batch)
    }

    pub(super) fn allocate_id(&self, inode: bool) -> MetaResult<u64> {
        self.allocate_with(1, inode, Self::take_id_from_batch)
    }

    pub(super) fn allocate_inode_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        self.allocate_ids(count, true)
    }

    pub(super) fn allocate_object_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        self.allocate_ids(count, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_id_generation_does_not_consume_batch() {
        let state = AllocatorStateV1::new(
            AllocatorRecord::new(1, 0).expect("inode namespace"),
            AllocatorRecord::new(2, 0).expect("payload namespace"),
        )
        .expect("allocator state");
        let mut runtime = AllocatorRuntime {
            state: Some(state),
            inode_batch: crate::allocator_state::COUNTER_LIMIT
                ..crate::allocator_state::COUNTER_LIMIT + 1,
            payload_batch: 0..0,
        };

        let result = PgMetaEngine::take_ids_from_batch(&mut runtime, 1, true);

        assert!(result.is_err(), "counter at the exclusive limit must fail");
        assert_eq!(
            runtime.inode_batch.start,
            crate::allocator_state::COUNTER_LIMIT,
            "failed ID generation must not consume the batch"
        );
    }

    #[test]
    fn single_id_take_returns_one_id_without_batch_vector() {
        let state = AllocatorStateV1::new(
            AllocatorRecord::new(3, 0).expect("inode namespace"),
            AllocatorRecord::new(4, 0).expect("payload namespace"),
        )
        .expect("allocator state");
        let mut runtime = AllocatorRuntime {
            state: Some(state),
            inode_batch: 0..1,
            payload_batch: 0..0,
        };

        let result = PgMetaEngine::take_id_from_batch(&mut runtime, 1, true)
            .expect("single ID take")
            .expect("single ID");

        assert_eq!(result, state.inode.id_at(0).expect("first inode ID"));
        assert_eq!(runtime.inode_batch.start, 1);
    }

    #[test]
    fn single_id_take_rejects_multiple_count_without_consuming_batch() {
        let state = AllocatorStateV1::new(
            AllocatorRecord::new(5, 0).expect("inode namespace"),
            AllocatorRecord::new(6, 0).expect("payload namespace"),
        )
        .expect("allocator state");
        let mut runtime = AllocatorRuntime {
            state: Some(state),
            inode_batch: 0..2,
            payload_batch: 0..0,
        };

        let result = PgMetaEngine::take_id_from_batch(&mut runtime, 2, true);

        assert!(
            result.is_err(),
            "single-ID path must reject multi-ID requests"
        );
        assert_eq!(runtime.inode_batch.start, 0);
    }
}
