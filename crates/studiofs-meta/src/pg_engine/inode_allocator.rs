//! Owns PostgreSQL-backed client-slot and local-ID namespace allocation.

use super::{
    LOCAL_PG_CLIENT_SLOT_COUNTER, PG_NEXT_ALLOCATOR_NAMESPACE_COUNTER, PG_NEXT_CLIENT_SLOT_COUNTER,
    PG_NEXT_INODE_COUNTER, PgMetaEngine,
};
use crate::allocator_state::{AllocatorRecord, AllocatorStateV1, MAX_NAMESPACE};
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
                                "legacy next_inode counter requires allocator migration".to_string(),
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
        let state = match self.local.read_allocator_state() {
            Ok(Some(state)) => Ok(state),
            Ok(None) | Err(_) => self.mint_allocator_state().and_then(|state| {
                self.local.replace_allocator_state(state)?;
                Ok(state)
            }),
        };
        match state {
            Ok(state) => {
                if let Ok(mut runtime) = self.allocator_runtime.lock() {
                    runtime.state = Some(state);
                    runtime.inode_batch = 0..0;
                    runtime.payload_batch = 0..0;
                }
                self.inode_id_headroom_available
                    .store(true, Ordering::Release);
                self.object_id_headroom_available
                    .store(true, Ordering::Release);
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

    fn take_ids_from_batch(
        runtime: &mut AllocatorRuntime,
        count: u64,
        inode: bool,
    ) -> MetaResult<Option<Vec<u64>>> {
        let state = runtime
            .state
            .ok_or_else(|| MetaError::Storage("allocator namespace is unavailable".to_string()))?;
        let available = if inode {
            runtime
                .inode_batch
                .end
                .saturating_sub(runtime.inode_batch.start)
        } else {
            runtime
                .payload_batch
                .end
                .saturating_sub(runtime.payload_batch.start)
        };
        if available < count {
            return Ok(None);
        }
        let batch = if inode {
            &mut runtime.inode_batch
        } else {
            &mut runtime.payload_batch
        };
        let first = batch.start;
        batch.start += count;
        let record = if inode { state.inode } else { state.payload };
        (first..first + count)
            .map(|counter| {
                record
                    .id_at(counter)
                    .map_err(|error| MetaError::Storage(error.to_string()))
            })
            .collect::<MetaResult<Vec<_>>>()
            .map(Some)
    }

    fn allocate_ids(&self, count: u32, inode: bool) -> MetaResult<Vec<u64>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let count = u64::from(count);
        {
            let mut runtime = self
                .allocator_runtime
                .lock()
                .map_err(|_| MetaError::Storage("allocator runtime lock poisoned".to_string()))?;
            if let Some(ids) = Self::take_ids_from_batch(&mut runtime, count, inode)? {
                return Ok(ids);
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
            if let Some(ids) = Self::take_ids_from_batch(&mut runtime, count, inode)? {
                return Ok(ids);
            }
            let state = runtime.state.ok_or_else(|| {
                MetaError::Storage("allocator namespace is unavailable".to_string())
            })?;
            let record = if inode { state.inode } else { state.payload };
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
        Self::take_ids_from_batch(&mut runtime, count, inode)?.ok_or_else(|| {
            MetaError::Storage("allocator batch refill did not provide requested IDs".to_string())
        })
    }

    pub(super) fn allocate_inode_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        self.allocate_ids(count, true)
    }

    pub(super) fn allocate_object_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        self.allocate_ids(count, false)
    }
}
