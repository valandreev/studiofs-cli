//! Owns PG-backed inode and client-slot allocation.

use super::{
    LEGACY_INODE_RANGE_SPEC, LOCAL_PG_CLIENT_SLOT_COUNTER,
    LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER, LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER,
    LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER, LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER,
    LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER, PG_INODE_PREFETCH_LOW_WATERMARK,
    PG_INODE_WINDOW_SIZE, PG_NEXT_CLIENT_SLOT_COUNTER, PG_NEXT_INODE_COUNTER, PgMetaEngine,
};
use crate::engine::{MetaError, MetaInodes, MetaResult};
use crate::redb_engine::RedbMetaEngine;
use crate::schema::counters;
use crate::types::ROOT_INO;
use std::sync::atomic::Ordering;
use tracing::warn;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct InodeWindow {
    pub(super) start: u64,
    pub(super) next: u64,
    pub(super) end: u64,
}

impl InodeWindow {
    pub(super) const fn is_empty(self) -> bool {
        self.start == 0 && self.next == 0 && self.end == 0
    }

    pub(super) const fn is_valid(self) -> bool {
        self.is_empty()
            || (self.start > ROOT_INO && self.start <= self.next && self.next <= self.end)
    }

    pub(super) const fn has_headroom(self) -> bool {
        self.next < self.end
    }

    pub(super) const fn remaining(self) -> u64 {
        self.end.saturating_sub(self.next)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct InodeAllocatorState {
    pub(super) current: InodeWindow,
    pub(super) prefetched: InodeWindow,
}

impl InodeAllocatorState {
    pub(super) const fn is_valid(self) -> bool {
        self.current.is_valid()
            && self.prefetched.is_valid()
            && (!self.current.is_empty() || self.prefetched.is_empty())
    }

    pub(super) const fn has_headroom(self) -> bool {
        self.current.has_headroom() || self.prefetched.has_headroom()
    }
}

#[derive(Clone, Copy)]
pub(super) enum InodeWindowReservationReason {
    Current,
    Prefetch,
}

#[derive(Clone, Copy)]
pub(super) struct LocalIdRangeSpec {
    pub(super) next_counter: &'static str,
    pub(super) active_start_counter: &'static str,
    pub(super) active_end_counter: &'static str,
    pub(super) reserved_end_counter: &'static str,
    pub(super) min_value: u64,
    pub(super) slot_stride: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct LocalIdRangeState {
    pub(super) next: u64,
    pub(super) active_start: u64,
    pub(super) active_end: u64,
    pub(super) reserved_end: u64,
}

impl PgMetaEngine {
    pub(super) fn load_persisted_client_slot(local: &RedbMetaEngine) -> MetaResult<Option<u64>> {
        let stored = local.get_counter(LOCAL_PG_CLIENT_SLOT_COUNTER)?;
        if stored == 0 {
            Ok(None)
        } else {
            Ok(Some(stored - 1))
        }
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

    pub(super) fn client_slot(&self) -> MetaResult<u64> {
        let cached = self.client_slot_plus_one.load(Ordering::Acquire);
        if cached != 0 {
            return Ok(cached - 1);
        }
        let slot = Self::load_persisted_client_slot(&self.local)?
            .ok_or_else(|| MetaError::Storage("missing persisted pg client slot".to_string()))?;
        self.cache_client_slot(slot);
        Ok(slot)
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
                        let tx = client.transaction().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres tx failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        tx.execute(
                            r#"
                        INSERT INTO counters (vol, name, value)
                        VALUES ($1, $2, 0)
                        ON CONFLICT (vol, name) DO NOTHING
                        "#,
                            &[&volume_id, &PG_NEXT_CLIENT_SLOT_COUNTER],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres slot seed failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        let row = tx
                            .query_one(
                                r#"
                            UPDATE counters
                            SET value = value + 1
                            WHERE vol = $1 AND name = $2
                            RETURNING value - 1
                            "#,
                                &[&volume_id, &PG_NEXT_CLIENT_SLOT_COUNTER],
                            )
                            .await
                            .map_err(|e| {
                                MetaError::Storage(format!(
                                    "postgres slot reserve failed: {}",
                                    Self::format_pg_error(e)
                                ))
                            })?;
                        tx.commit().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres slot commit failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;

                        let slot: i64 = row.get(0);
                        u64::try_from(slot).map_err(|_| {
                            MetaError::Storage(format!("invalid negative pg client slot: {slot}"))
                        })
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

    pub(super) fn load_persisted_inode_allocator_state(
        local: &RedbMetaEngine,
    ) -> MetaResult<InodeAllocatorState> {
        Ok(InodeAllocatorState {
            current: InodeWindow {
                start: local.get_counter(LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER)?,
                next: local.get_counter(counters::NEXT_INODE)?,
                end: local.get_counter(LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER)?,
            },
            prefetched: InodeWindow {
                start: local.get_counter(LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER)?,
                next: local.get_counter(LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER)?,
                end: local.get_counter(LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER)?,
            },
        })
    }

    fn load_valid_persisted_inode_allocator_state(
        local: &RedbMetaEngine,
    ) -> MetaResult<Option<InodeAllocatorState>> {
        let state = Self::load_persisted_inode_allocator_state(local)?;
        if state.is_valid() && !state.current.is_empty() {
            Ok(Some(state))
        } else {
            Ok(None)
        }
    }

    fn load_legacy_persisted_inode_allocator_state(
        local: &RedbMetaEngine,
        slot: u64,
    ) -> MetaResult<Option<InodeAllocatorState>> {
        let Some(state) =
            Self::load_valid_persisted_local_id_range_state(local, slot, LEGACY_INODE_RANGE_SPEC)?
        else {
            return Ok(None);
        };
        if state.next > state.active_end {
            return Ok(None);
        }

        let translated = InodeAllocatorState {
            current: InodeWindow {
                start: state.active_start,
                next: state.next,
                end: state.active_end,
            },
            prefetched: if state.reserved_end > state.active_end {
                InodeWindow {
                    start: state.active_end,
                    next: state.active_end,
                    end: state.reserved_end,
                }
            } else {
                InodeWindow::default()
            },
        };
        Ok(translated.is_valid().then_some(translated))
    }

    pub(super) fn load_compatible_persisted_inode_allocator_state(
        local: &RedbMetaEngine,
        slot: Option<u64>,
    ) -> MetaResult<Option<(InodeAllocatorState, bool)>> {
        if let Some(state) = Self::load_valid_persisted_inode_allocator_state(local)? {
            return Ok(Some((state, false)));
        }
        if let Some(slot) = slot
            && let Some(state) = Self::load_legacy_persisted_inode_allocator_state(local, slot)?
        {
            return Ok(Some((state, true)));
        }
        Ok(None)
    }

    pub(super) fn persist_inode_allocator_state_locked(
        counters: &crate::redb_engine::CounterWriteTransaction<'_>,
        state: InodeAllocatorState,
    ) -> MetaResult<()> {
        counters.set(counters::NEXT_INODE, state.current.next)?;
        counters.set(
            LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER,
            state.current.start,
        )?;
        counters.set(LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER, state.current.end)?;
        counters.set(
            LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER,
            state.prefetched.start,
        )?;
        counters.set(
            LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER,
            state.prefetched.next,
        )?;
        counters.set(
            LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER,
            state.prefetched.end,
        )?;
        Ok(())
    }

    pub(super) fn persist_inode_allocator_state(
        &self,
        state: InodeAllocatorState,
    ) -> MetaResult<()> {
        self.local
            .write_counters(|counters| Self::persist_inode_allocator_state_locked(counters, state))
    }

    pub(super) fn update_inode_headroom_flag(&self, state: InodeAllocatorState) {
        self.inode_id_headroom_available
            .store(state.has_headroom(), Ordering::Release);
    }

    pub(super) fn load_runtime_inode_allocator_state(
        &self,
        slot: u64,
    ) -> MetaResult<InodeAllocatorState> {
        let Some((state, migrated_from_legacy)) =
            Self::load_compatible_persisted_inode_allocator_state(&self.local, Some(slot))?
        else {
            return Ok(InodeAllocatorState::default());
        };
        if migrated_from_legacy {
            self.persist_inode_allocator_state(state)?;
        }
        Ok(state)
    }

    fn reserve_inode_window_from_postgres(
        &self,
        initial_next_hint: u64,
        minimum_window_size: u64,
    ) -> MetaResult<InodeWindow> {
        #[cfg(test)]
        if let Some(override_result) = super::tests::pop_inode_window_reservation_override() {
            return override_result.map_err(MetaError::Storage);
        }

        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let initial_next_hint = initial_next_hint.max(ROOT_INO + 1);
        let window_size = PG_INODE_WINDOW_SIZE.max(minimum_window_size);
        Self::run_async_once(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "reserve inode window",
                move |client| Box::pin(async move {
                    let tx = client.transaction().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres tx failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    let initial_next_hint = i64::try_from(initial_next_hint).map_err(|_| {
                        MetaError::Storage(format!(
                            "invalid inode window seed hint: {initial_next_hint} exceeds PostgreSQL BIGINT max {}",
                            i64::MAX
                        ))
                    })?;
                    let window_size = i64::try_from(window_size).map_err(|_| {
                        MetaError::Storage(format!(
                            "invalid inode window size: {window_size} exceeds PostgreSQL BIGINT max {}",
                            i64::MAX
                        ))
                    })?;

                    tx.execute(
                        r#"
                        INSERT INTO counters (vol, name, value)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (vol, name) DO NOTHING
                        "#,
                        &[&volume_id, &PG_NEXT_INODE_COUNTER, &initial_next_hint],
                    )
                    .await
                    .map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres inode counter seed failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;

                    let row = tx
                        .query_one(
                            r#"
                            UPDATE counters
                            SET value = GREATEST(value, $3) + $4
                            WHERE vol = $1 AND name = $2
                            RETURNING value - $4, value
                            "#,
                            &[
                                &volume_id,
                                &PG_NEXT_INODE_COUNTER,
                                &initial_next_hint,
                                &window_size,
                            ],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres inode window reserve failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    tx.commit().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres inode window commit failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;

                    let start: i64 = row.get(0);
                    let end: i64 = row.get(1);
                    let start = u64::try_from(start).map_err(|_| {
                        MetaError::Storage(format!("invalid negative inode window start: {start}"))
                    })?;
                    let end = u64::try_from(end).map_err(|_| {
                        MetaError::Storage(format!("invalid negative inode window end: {end}"))
                    })?;
                    if start < ROOT_INO + 1 || end < start {
                        return Err(MetaError::Storage(format!(
                            "invalid inode window reservation: [{start}, {end})"
                        )));
                    }
                    Ok(InodeWindow {
                        start,
                        next: start,
                        end,
                    })
                }),
            )
            .await
        })
    }

    pub(super) fn inode_window_reservation_needed(
        state: InodeAllocatorState,
        reason: InodeWindowReservationReason,
        minimum_window_size: u64,
    ) -> bool {
        match reason {
            InodeWindowReservationReason::Current => {
                state.current.remaining() < minimum_window_size
                    && state.prefetched.remaining() < minimum_window_size
            }
            InodeWindowReservationReason::Prefetch => {
                state.current.has_headroom()
                    && state.current.remaining() <= PG_INODE_PREFETCH_LOW_WATERMARK
                    && !state.prefetched.has_headroom()
            }
        }
    }

    pub(super) fn apply_reserved_inode_window(
        state: &mut InodeAllocatorState,
        reason: InodeWindowReservationReason,
        window: InodeWindow,
        minimum_window_size: u64,
    ) -> bool {
        match reason {
            InodeWindowReservationReason::Current => {
                let current_fits = state.current.remaining() >= minimum_window_size;
                let prefetched_fits = state.prefetched.remaining() >= minimum_window_size;
                if current_fits || prefetched_fits {
                    false
                } else if state.current.has_headroom() {
                    state.prefetched = window;
                    true
                } else {
                    state.current = window;
                    state.prefetched = InodeWindow::default();
                    true
                }
            }
            InodeWindowReservationReason::Prefetch => {
                if state.prefetched.has_headroom() {
                    false
                } else if state.current.has_headroom() {
                    state.prefetched = window;
                    true
                } else {
                    state.current = window;
                    state.prefetched = InodeWindow::default();
                    true
                }
            }
        }
    }

    fn inode_window_seed_hint(
        &self,
        state: InodeAllocatorState,
        reason: InodeWindowReservationReason,
    ) -> MetaResult<u64> {
        match reason {
            InodeWindowReservationReason::Current => {
                if state.current.next > ROOT_INO {
                    Ok(state.current.next.max(ROOT_INO + 1))
                } else {
                    Ok(self
                        .local
                        .get_counter(counters::NEXT_INODE)?
                        .max(ROOT_INO + 1))
                }
            }
            InodeWindowReservationReason::Prefetch => {
                Ok(state.current.next.max(state.current.end).max(ROOT_INO + 1))
            }
        }
    }

    pub(super) fn reserve_inode_window_if_needed(
        &self,
        slot: u64,
        reason: InodeWindowReservationReason,
    ) -> MetaResult<InodeAllocatorState> {
        self.reserve_inode_window_if_needed_with_min_size(slot, reason, PG_INODE_WINDOW_SIZE)
    }

    fn reserve_inode_window_if_needed_with_min_size(
        &self,
        slot: u64,
        reason: InodeWindowReservationReason,
        minimum_window_size: u64,
    ) -> MetaResult<InodeAllocatorState> {
        let _reservation_guard = self.inode_window_reservation_lock.lock().map_err(|_| {
            MetaError::Storage("inode window reservation lock poisoned".to_string())
        })?;
        let seed_hint = {
            let _allocator_guard = self
                .inode_allocator_lock
                .lock()
                .map_err(|_| MetaError::Storage("inode allocator lock poisoned".to_string()))?;
            let state = self.load_runtime_inode_allocator_state(slot)?;
            self.update_inode_headroom_flag(state);
            if !Self::inode_window_reservation_needed(state, reason, minimum_window_size) {
                return Ok(state);
            }
            self.inode_window_seed_hint(state, reason)?
        };

        let window = self.reserve_inode_window_from_postgres(seed_hint, minimum_window_size)?;
        let _allocator_guard = self
            .inode_allocator_lock
            .lock()
            .map_err(|_| MetaError::Storage("inode allocator lock poisoned".to_string()))?;
        let mut state = self.load_runtime_inode_allocator_state(slot)?;
        let applied =
            Self::apply_reserved_inode_window(&mut state, reason, window, minimum_window_size);
        if applied {
            self.persist_inode_allocator_state(state)?;
        }
        self.update_inode_headroom_flag(state);
        Ok(state)
    }

    fn format_inode_window_reservation_error(
        &self,
        state: InodeAllocatorState,
        error: &MetaError,
    ) -> String {
        let has_local_headroom = state.current.has_headroom() || state.prefetched.has_headroom();
        let error_text = error.to_string();
        let lower_error_text = error_text.to_ascii_lowercase();
        if has_local_headroom {
            format!("failed to ensure current local inode window: {error_text}")
        } else if lower_error_text.contains("exhaust") {
            format!("pg local inode window exhausted: {error_text}")
        } else {
            format!(
                "failed to reserve a new inode window from PostgreSQL after exhausting local inode windows: {error_text}"
            )
        }
    }

    pub(super) fn allocate_inode_object_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let slot = self.client_slot()?;
        let count = u64::from(count);
        loop {
            let allocation = {
                let _guard = self
                    .inode_allocator_lock
                    .lock()
                    .map_err(|_| MetaError::Storage("inode allocator lock poisoned".to_string()))?;
                let mut state = self.load_runtime_inode_allocator_state(slot)?;
                if state.current.remaining() < count && state.prefetched.remaining() >= count {
                    state.current = state.prefetched;
                    state.prefetched = InodeWindow::default();
                }
                if state.current.remaining() < count {
                    self.update_inode_headroom_flag(state);
                    None
                } else {
                    let first = state.current.next;
                    state.current.next =
                        state.current.next.checked_add(count).ok_or_else(|| {
                            MetaError::Storage("pg inode/object ID allocation overflow".to_string())
                        })?;

                    if state.current.next == state.current.end && state.prefetched.has_headroom() {
                        state.current = state.prefetched;
                        state.prefetched = InodeWindow::default();
                    }

                    let needs_current_refill = Self::inode_window_reservation_needed(
                        state,
                        InodeWindowReservationReason::Current,
                        1,
                    );
                    let needs_prefetch = Self::inode_window_reservation_needed(
                        state,
                        InodeWindowReservationReason::Prefetch,
                        1,
                    );
                    self.persist_inode_allocator_state(state)?;
                    self.update_inode_headroom_flag(state);
                    Some((first, state, needs_current_refill, needs_prefetch))
                }
            };

            let Some((first, state, needs_current_refill, needs_prefetch)) = allocation else {
                match self.reserve_inode_window_if_needed_with_min_size(
                    slot,
                    InodeWindowReservationReason::Current,
                    count,
                ) {
                    Ok(_) => continue,
                    Err(error) => {
                        let state = {
                            let _guard = self.inode_allocator_lock.lock().map_err(|_| {
                                MetaError::Storage("inode allocator lock poisoned".to_string())
                            })?;
                            let state = self.load_runtime_inode_allocator_state(slot)?;
                            self.update_inode_headroom_flag(state);
                            state
                        };
                        return Err(MetaError::Storage(
                            self.format_inode_window_reservation_error(state, &error),
                        ));
                    }
                }
            };

            if needs_current_refill {
                if let Err(error) =
                    self.reserve_inode_window_if_needed(slot, InodeWindowReservationReason::Current)
                {
                    warn!(
                        error = %error,
                        volume_id = self.volume_id,
                        exhausted_end = state.current.next,
                        "postgres inode/object window refill unavailable after exhausting current window"
                    );
                }
            } else if needs_prefetch
                && let Err(error) = self
                    .reserve_inode_window_if_needed(slot, InodeWindowReservationReason::Prefetch)
            {
                warn!(
                    error = %error,
                    volume_id = self.volume_id,
                    current_start = state.current.start,
                    current_next = state.current.next,
                    current_end = state.current.end,
                    "postgres inode/object window prefetch failed; continuing with current local window"
                );
            }

            return Ok((first..first + count).collect());
        }
    }

    pub(super) fn reserve_counter_range_base(slot: u64, stride: u64, min_value: u64) -> u64 {
        let base = slot.saturating_mul(stride);
        base.max(min_value)
    }

    fn counter_range_limit(slot: u64, spec: LocalIdRangeSpec) -> u64 {
        Self::reserve_counter_range_base(slot.saturating_add(1), spec.slot_stride, spec.min_value)
    }

    fn load_persisted_local_id_range_state(
        local: &RedbMetaEngine,
        spec: LocalIdRangeSpec,
    ) -> MetaResult<Option<LocalIdRangeState>> {
        let active_start = local.get_counter(spec.active_start_counter)?;
        let active_end = local.get_counter(spec.active_end_counter)?;
        let reserved_end = local.get_counter(spec.reserved_end_counter)?;
        if active_start == 0 || active_end == 0 || reserved_end == 0 {
            return Ok(None);
        }
        Ok(Some(LocalIdRangeState {
            next: local.get_counter(spec.next_counter)?,
            active_start,
            active_end,
            reserved_end,
        }))
    }

    fn load_valid_persisted_local_id_range_state(
        local: &RedbMetaEngine,
        slot: u64,
        spec: LocalIdRangeSpec,
    ) -> MetaResult<Option<LocalIdRangeState>> {
        Ok(Self::load_persisted_local_id_range_state(local, spec)?
            .filter(|state| Self::local_id_range_state_is_valid(slot, spec, *state)))
    }

    fn local_id_range_state_is_valid(
        slot: u64,
        spec: LocalIdRangeSpec,
        state: LocalIdRangeState,
    ) -> bool {
        let base = Self::reserve_counter_range_base(slot, spec.slot_stride, spec.min_value);
        let limit = Self::counter_range_limit(slot, spec);
        state.active_start >= base
            && state.active_start <= state.active_end
            && state.active_end <= state.reserved_end
            && state.reserved_end <= limit
            && state.next >= state.active_start
            && state.next <= state.reserved_end
    }

    #[cfg(test)]
    pub(super) fn persist_local_id_range_state_locked(
        counters: &crate::redb_engine::CounterWriteTransaction<'_>,
        spec: LocalIdRangeSpec,
        state: LocalIdRangeState,
    ) -> MetaResult<()> {
        counters.set(spec.next_counter, state.next)?;
        counters.set(spec.active_start_counter, state.active_start)?;
        counters.set(spec.active_end_counter, state.active_end)?;
        counters.set(spec.reserved_end_counter, state.reserved_end)?;
        Ok(())
    }

    fn can_refresh_local_id_headroom(&self) -> MetaResult<bool> {
        #[cfg(test)]
        if let Some(value) = super::tests::headroom_refresh_test_override() {
            return Ok(value);
        }

        let command_pool = self.command_pool.clone();
        let command_timeout = self.command_timeout;
        Self::run_async_once(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "refresh local id headroom",
                move |client| {
                    Box::pin(async move {
                        client.simple_query("SELECT 1").await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres local id headroom refresh check failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        Ok(true)
                    })
                },
            )
            .await
            .or(Ok(false))
        })
    }

    pub(super) fn prime_local_id_ranges(&self, slot: u64) -> MetaResult<()> {
        self.cache_client_slot(slot);

        let inode_prime = (|| -> MetaResult<()> {
            let state = {
                let _guard = self
                    .inode_allocator_lock
                    .lock()
                    .map_err(|_| MetaError::Storage("inode allocator lock poisoned".to_string()))?;
                let state = self.load_runtime_inode_allocator_state(slot)?;
                self.update_inode_headroom_flag(state);
                state
            };
            if !Self::inode_window_reservation_needed(
                state,
                InodeWindowReservationReason::Current,
                1,
            ) {
                return Ok(());
            }
            let _ =
                self.reserve_inode_window_if_needed(slot, InodeWindowReservationReason::Current)?;
            Ok(())
        })();
        if inode_prime.is_err() {
            self.inode_id_headroom_available
                .store(false, Ordering::Release);
        }

        // Refill stale no-headroom state when PostgreSQL has recovered.
        let pg_available = self.can_refresh_local_id_headroom().unwrap_or(false);
        if pg_available && !self.inode_id_headroom_available.load(Ordering::Acquire) {
            let _ =
                self.reserve_inode_window_if_needed(slot, InodeWindowReservationReason::Current);
        }
        inode_prime
    }
}
