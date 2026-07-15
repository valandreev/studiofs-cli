use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryPressureSource {
    Host,
    Cgroup,
    HostAndCgroup,
    Fallback,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryPressureState {
    Normal,
    Constrained,
    Disabled,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryComponentLimits {
    pub memory_cache_bytes: u64,
    pub dirty_bytes: u64,
    pub transit_bytes: u64,
    pub async_write_bytes: u64,
}

impl MemoryComponentLimits {
    #[must_use]
    pub fn managed_total(self) -> u64 {
        self.memory_cache_bytes
            .saturating_add(self.dirty_bytes)
            .saturating_add(self.transit_bytes)
            .saturating_add(self.async_write_bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryPressureInput {
    pub host_total_bytes: u64,
    pub host_available_bytes: u64,
    pub cgroup_limit_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryPressureSnapshot {
    pub source: MemoryPressureSource,
    pub state: MemoryPressureState,
    pub generation: u64,
    pub effective_limit_bytes: u64,
    pub available_bytes: u64,
    pub reserve_bytes: u64,
    pub envelope_bytes: u64,
    pub limits: MemoryComponentLimits,
    pub transition_count: u64,
    pub sampling_failure_count: u64,
    pub overcommitted_minimum: bool,
}

impl MemoryPressureSnapshot {
    #[must_use]
    pub fn disabled(limits: MemoryComponentLimits) -> Self {
        Self {
            source: MemoryPressureSource::Disabled,
            state: MemoryPressureState::Disabled,
            generation: 0,
            effective_limit_bytes: 0,
            available_bytes: 0,
            reserve_bytes: 0,
            envelope_bytes: 0,
            limits,
            transition_count: 0,
            sampling_failure_count: 0,
            overcommitted_minimum: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoryPressureController {
    snapshot: Arc<RwLock<MemoryPressureSnapshot>>,
    memory_cache_limit_bytes: Arc<AtomicU64>,
}

impl MemoryPressureController {
    #[must_use]
    pub fn new(initial: MemoryPressureSnapshot) -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(initial)),
            memory_cache_limit_bytes: Arc::new(AtomicU64::new(initial.limits.memory_cache_bytes)),
        }
    }

    #[must_use]
    pub fn disabled(configured_limits: MemoryComponentLimits) -> Self {
        Self::new(MemoryPressureSnapshot::disabled(configured_limits))
    }

    #[must_use]
    pub fn snapshot(&self) -> MemoryPressureSnapshot {
        *self.snapshot.read()
    }

    pub fn publish(&self, snapshot: MemoryPressureSnapshot) {
        self.memory_cache_limit_bytes
            .store(snapshot.limits.memory_cache_bytes, Ordering::Release);
        *self.snapshot.write() = snapshot;
    }

    #[must_use]
    pub fn memory_cache_limit_bytes(&self) -> Arc<AtomicU64> {
        self.memory_cache_limit_bytes.clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MemoryPressureReadError {
    #[error("failed to read memory pressure inputs")]
    ReadFailed,
}

#[derive(Debug)]
pub struct MemoryPressureModel {
    configured: MemoryComponentLimits,
    block_size: u64,
    negotiated_max_write: u64,
    previous: Option<MemoryPressureSnapshot>,
    healthy_sample_count: u8,
}

impl MemoryPressureModel {
    #[must_use]
    pub fn new(
        configured: MemoryComponentLimits,
        block_size: u64,
        negotiated_max_write: u64,
    ) -> Self {
        Self {
            configured,
            block_size,
            negotiated_max_write,
            previous: None,
            healthy_sample_count: 0,
        }
    }

    pub fn observe(
        &mut self,
        input: Result<MemoryPressureInput, MemoryPressureReadError>,
        admitted_managed_bytes: u64,
    ) -> MemoryPressureSnapshot {
        let snapshot = match input {
            Ok(input) => self.observe_healthy(input, admitted_managed_bytes),
            Err(_) => self.observe_failure(),
        };
        self.previous = Some(snapshot);
        snapshot
    }

    fn observe_healthy(
        &mut self,
        input: MemoryPressureInput,
        admitted_managed_bytes: u64,
    ) -> MemoryPressureSnapshot {
        let finite_cgroup = input
            .cgroup_limit_bytes
            .filter(|limit| *limit != u64::MAX)
            .zip(input.cgroup_current_bytes);
        let (source, effective_limit_bytes, available_bytes) = finite_cgroup.map_or(
            (
                MemoryPressureSource::Host,
                input.host_total_bytes,
                input.host_available_bytes,
            ),
            |(limit, current)| {
                (
                    MemoryPressureSource::HostAndCgroup,
                    input.host_total_bytes.min(limit),
                    input
                        .host_available_bytes
                        .min(limit.saturating_sub(current)),
                )
            },
        );
        let reserve_bytes = GIB.max(percent(effective_limit_bytes, 15));
        let allocatable = available_bytes.saturating_sub(reserve_bytes);
        let configured_envelope = div_ceil_ten_ninths(self.configured.managed_total());
        let envelope_bytes = configured_envelope.min(percent(allocatable, 75));
        let mut limits = self.limits_for(envelope_bytes);

        if let Some(previous) = self.previous {
            limits.memory_cache_bytes = bounded_growth(
                previous.limits.memory_cache_bytes,
                limits.memory_cache_bytes,
                self.configured.memory_cache_bytes,
            );
            limits.dirty_bytes = bounded_growth(
                previous.limits.dirty_bytes,
                limits.dirty_bytes,
                self.configured.dirty_bytes,
            );
            limits.transit_bytes = bounded_growth(
                previous.limits.transit_bytes,
                limits.transit_bytes,
                self.configured.transit_bytes,
            );
            limits.async_write_bytes = bounded_growth(
                previous.limits.async_write_bytes,
                limits.async_write_bytes,
                self.configured.async_write_bytes,
            );
        }

        let constrained =
            available_bytes < reserve_bytes || envelope_bytes < admitted_managed_bytes;
        let previous_state = self.previous.map(|snapshot| snapshot.state);
        let state = if constrained {
            self.healthy_sample_count = 0;
            MemoryPressureState::Constrained
        } else if previous_state == Some(MemoryPressureState::Constrained) {
            self.healthy_sample_count = self.healthy_sample_count.saturating_add(1);
            if self.healthy_sample_count >= 2 {
                self.healthy_sample_count = 0;
                MemoryPressureState::Normal
            } else {
                MemoryPressureState::Constrained
            }
        } else {
            self.healthy_sample_count = 0;
            MemoryPressureState::Normal
        };
        let transition_count = self.previous.map_or(0, |previous| {
            previous
                .transition_count
                .saturating_add(u64::from(previous.state != state))
        });
        let sampling_failure_count = self
            .previous
            .map_or(0, |previous| previous.sampling_failure_count);

        MemoryPressureSnapshot {
            source,
            state,
            generation: self.next_generation(),
            effective_limit_bytes,
            available_bytes,
            reserve_bytes,
            envelope_bytes,
            limits,
            transition_count,
            sampling_failure_count,
            overcommitted_minimum: limits.managed_total() > percent(envelope_bytes, 90),
        }
    }

    fn observe_failure(&mut self) -> MemoryPressureSnapshot {
        self.healthy_sample_count = 0;
        if let Some(mut previous) = self.previous {
            previous.generation = previous.generation.saturating_add(1);
            previous.sampling_failure_count = previous.sampling_failure_count.saturating_add(1);
            return previous;
        }

        let limits = self.limits_for(0);
        MemoryPressureSnapshot {
            source: MemoryPressureSource::Fallback,
            state: MemoryPressureState::Constrained,
            generation: 1,
            effective_limit_bytes: 0,
            available_bytes: 0,
            reserve_bytes: GIB,
            envelope_bytes: 0,
            limits,
            transition_count: 0,
            sampling_failure_count: 1,
            overcommitted_minimum: limits.managed_total() > 0,
        }
    }

    fn limits_for(&self, envelope_bytes: u64) -> MemoryComponentLimits {
        MemoryComponentLimits {
            memory_cache_bytes: percent(envelope_bytes, 40)
                .min(self.configured.memory_cache_bytes)
                .max(self.block_size.max(64 * MIB)),
            dirty_bytes: percent(envelope_bytes, 25)
                .min(self.configured.dirty_bytes)
                .max(self.block_size.max(64 * MIB)),
            transit_bytes: percent(envelope_bytes, 15)
                .min(self.configured.transit_bytes)
                .max(self.block_size.max(32 * MIB)),
            async_write_bytes: percent(envelope_bytes, 10)
                .min(self.configured.async_write_bytes)
                .max(self.negotiated_max_write.max(32 * MIB)),
        }
    }

    fn next_generation(&self) -> u64 {
        self.previous
            .map_or(1, |previous| previous.generation.saturating_add(1))
    }
}

fn percent(value: u64, percentage: u64) -> u64 {
    (u128::from(value) * u128::from(percentage) / 100) as u64
}

fn div_ceil_ten_ninths(value: u64) -> u64 {
    let result = (u128::from(value) * 10).div_ceil(9);
    result.min(u128::from(u64::MAX)) as u64
}

fn bounded_growth(previous: u64, target: u64, configured_ceiling: u64) -> u64 {
    if target <= previous {
        target
    } else {
        target.min(previous.saturating_add(configured_ceiling / 4))
    }
}

#[cfg(test)]
mod controller_tests {
    use super::*;

    fn snapshot(dirty_bytes: u64) -> MemoryPressureSnapshot {
        MemoryPressureSnapshot {
            source: MemoryPressureSource::Host,
            state: MemoryPressureState::Normal,
            generation: 1,
            effective_limit_bytes: 8 * GIB,
            available_bytes: 3 * GIB,
            reserve_bytes: GIB,
            envelope_bytes: 2 * GIB,
            limits: MemoryComponentLimits {
                memory_cache_bytes: 512 * MIB,
                dirty_bytes,
                transit_bytes: 256 * MIB,
                async_write_bytes: 128 * MIB,
            },
            transition_count: 0,
            sampling_failure_count: 0,
            overcommitted_minimum: false,
        }
    }

    #[test]
    fn controller_publish_replaces_shared_snapshot() {
        let controller = MemoryPressureController::new(snapshot(512 * MIB));

        controller.publish(snapshot(64 * MIB));

        assert_eq!(controller.snapshot().limits.dirty_bytes, 64 * MIB);
    }

    #[test]
    fn controller_publish_updates_shared_memory_cache_limit() {
        let controller = MemoryPressureController::new(snapshot(512 * MIB));
        let memory_cache_limit = controller.memory_cache_limit_bytes();
        let mut constrained = snapshot(64 * MIB);
        constrained.limits.memory_cache_bytes = 64 * MIB;

        controller.publish(constrained);

        assert_eq!(memory_cache_limit.load(Ordering::Acquire), 64 * MIB);
    }

    #[test]
    fn disabled_controller_preserves_configured_limits() {
        let limits = snapshot(512 * MIB).limits;

        let controller = MemoryPressureController::disabled(limits);

        assert_eq!(
            controller.snapshot(),
            MemoryPressureSnapshot::disabled(limits)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gib(value: u64) -> u64 {
        value * 1024 * 1024 * 1024
    }

    fn mib(value: u64) -> u64 {
        value * 1024 * 1024
    }

    fn ceilings() -> MemoryComponentLimits {
        MemoryComponentLimits {
            memory_cache_bytes: gib(4),
            dirty_bytes: gib(2),
            transit_bytes: gib(1),
            async_write_bytes: gib(1),
        }
    }

    fn model() -> MemoryPressureModel {
        MemoryPressureModel::new(ceilings(), mib(4), mib(8))
    }

    fn input(total: u64, available: u64) -> MemoryPressureInput {
        MemoryPressureInput {
            host_total_bytes: total,
            host_available_bytes: available,
            cgroup_limit_bytes: None,
            cgroup_current_bytes: None,
        }
    }

    #[test]
    fn eight_gib_host_uses_exact_fifteen_percent_reserve() {
        let snapshot = model().observe(Ok(input(gib(8), gib(3))), 0);

        assert_eq!(snapshot.reserve_bytes, gib(8) * 15 / 100);
        assert_eq!(snapshot.available_bytes, gib(3));
        assert_eq!(snapshot.source, MemoryPressureSource::Host);
    }

    #[test]
    fn finite_cgroup_limits_host_headroom_and_effective_limit() {
        let mut sample = input(gib(16), gib(8));
        sample.cgroup_limit_bytes = Some(gib(6));
        sample.cgroup_current_bytes = Some(gib(5));

        let snapshot = model().observe(Ok(sample), 0);

        assert_eq!(snapshot.available_bytes, gib(1));
        assert_eq!(snapshot.effective_limit_bytes, gib(6));
        assert_eq!(snapshot.source, MemoryPressureSource::HostAndCgroup);
    }

    #[test]
    fn unlimited_cgroup_is_ignored() {
        let mut sample = input(gib(8), gib(3));
        sample.cgroup_limit_bytes = Some(u64::MAX);
        sample.cgroup_current_bytes = Some(gib(7));

        let snapshot = model().observe(Ok(sample), 0);

        assert_eq!(snapshot.source, MemoryPressureSource::Host);
        assert_eq!(snapshot.effective_limit_bytes, gib(8));
    }

    #[test]
    fn reserve_exhaustion_produces_zero_envelope() {
        let snapshot = model().observe(Ok(input(gib(8), gib(1))), 0);

        assert_eq!(snapshot.envelope_bytes, 0);
    }

    #[test]
    fn shares_round_down_without_exceeding_ninety_percent() {
        let configured = MemoryComponentLimits {
            memory_cache_bytes: u64::MAX,
            dirty_bytes: u64::MAX,
            transit_bytes: u64::MAX,
            async_write_bytes: u64::MAX,
        };
        let snapshot =
            MemoryPressureModel::new(configured, 1, 1).observe(Ok(input(gib(100), gib(100))), 0);

        assert!(snapshot.limits.managed_total() <= snapshot.envelope_bytes * 90 / 100);
    }

    #[test]
    fn block_and_write_minima_override_small_shares() {
        let snapshot = MemoryPressureModel::new(ceilings(), mib(96), mib(128))
            .observe(Ok(input(gib(8), gib(1))), 0);

        assert_eq!(
            snapshot.limits,
            MemoryComponentLimits {
                memory_cache_bytes: mib(96),
                dirty_bytes: mib(96),
                transit_bytes: mib(96),
                async_write_bytes: mib(128),
            }
        );
    }

    #[test]
    fn minima_report_envelope_overcommit() {
        let snapshot = model().observe(Ok(input(gib(8), gib(1))), 0);

        assert!(snapshot.overcommitted_minimum);
    }

    #[test]
    fn admitted_memory_above_new_envelope_constrains_immediately() {
        let snapshot = model().observe(Ok(input(gib(8), gib(3))), gib(2));

        assert_eq!(snapshot.state, MemoryPressureState::Constrained);
        assert_eq!(snapshot.transition_count, 0);
    }

    #[test]
    fn first_healthy_sample_retains_constrained_state() {
        let mut model = model();
        model.observe(Ok(input(gib(8), gib(1))), 0);

        let snapshot = model.observe(Ok(input(gib(8), gib(8))), 0);

        assert_eq!(snapshot.state, MemoryPressureState::Constrained);
    }

    #[test]
    fn second_healthy_sample_recovers_from_constrained_state() {
        let mut model = model();
        model.observe(Ok(input(gib(8), gib(1))), 0);
        model.observe(Ok(input(gib(8), gib(8))), 0);

        let snapshot = model.observe(Ok(input(gib(8), gib(8))), 0);

        assert_eq!(snapshot.state, MemoryPressureState::Normal);
        assert_eq!(snapshot.transition_count, 1);
    }

    #[test]
    fn state_transition_saturates_transition_count() {
        let mut model = model();
        model.previous = Some(MemoryPressureSnapshot {
            source: MemoryPressureSource::Host,
            state: MemoryPressureState::Normal,
            generation: 1,
            effective_limit_bytes: gib(8),
            available_bytes: gib(8),
            reserve_bytes: gib(1),
            envelope_bytes: gib(1),
            limits: ceilings(),
            transition_count: u64::MAX,
            sampling_failure_count: 0,
            overcommitted_minimum: false,
        });

        let snapshot = model.observe(Ok(input(gib(8), 0)), 0);

        assert_eq!(snapshot.transition_count, u64::MAX);
    }

    #[test]
    fn recovery_growth_is_bounded_to_quarter_of_configured_ceiling() {
        let mut model = model();
        let small = model.observe(Ok(input(gib(8), gib(2))), 0);

        let grown = model.observe(Ok(input(gib(64), gib(64))), 0);

        assert!(
            grown.limits.memory_cache_bytes
                <= small.limits.memory_cache_bytes + ceilings().memory_cache_bytes / 4
        );
    }

    #[test]
    fn first_failure_publishes_fallback_minimum_snapshot() {
        let snapshot = model().observe(Err(MemoryPressureReadError::ReadFailed), 0);

        assert_eq!(snapshot.source, MemoryPressureSource::Fallback);
        assert_eq!(snapshot.sampling_failure_count, 1);
        assert_eq!(snapshot.limits.memory_cache_bytes, mib(64));
    }

    #[test]
    fn later_failure_retains_values_and_increments_failure_count() {
        let mut model = model();
        let healthy = model.observe(Ok(input(gib(8), gib(3))), 0);

        let failed = model.observe(Err(MemoryPressureReadError::ReadFailed), 0);

        assert_eq!(failed.limits, healthy.limits);
        assert_eq!(failed.available_bytes, healthy.available_bytes);
        assert_eq!(failed.generation, healthy.generation + 1);
        assert_eq!(failed.sampling_failure_count, 1);
    }
}
