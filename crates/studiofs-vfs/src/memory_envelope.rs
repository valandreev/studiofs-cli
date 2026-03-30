//! Adaptive Memory Budget Controller (SFS-130).
//!
//! `MemoryEnvelope` is a background coordinator that distributes a global memory
//! budget across four elastic subsystems (L1 cache, transit buffer, dirty buffer,
//! staged upload).  It runs a 1-second tick loop that:
//!
//!  1. **Observes** pressure signals from existing atomic counters.
//!  2. **Decides** the workload phase and grant size.
//!  3. **Acts** by calling `set_budget()` on each subsystem.
//!
//! See `docs/ideas/adaptive-memory-budget.md` for the full design spec.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use studiofs_cache::BlockCache;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::fetch::FetchScheduler;
use crate::qos::TransitMemoryBudget;
use crate::vfs::StudioVfs;

// ── Constants ──────────────────────────────────────────────────────────────

/// Fixed overhead for caches not managed by the envelope (dentry, attr, path,
/// write-buffer, prefetch metadata).  ~180 MB.
const FIXED_OVERHEAD_BYTES: u64 = 180 * 1024 * 1024;

/// Consecutive ticks a phase must dominate before we switch.
const PHASE_HYSTERESIS_TICKS: u32 = 3;

/// Consecutive quiescent ticks before we start retreating.
const RETREAT_HYSTERESIS_TICKS: u32 = 5;

/// Consecutive ticks with available memory > 15 % before exiting emergency mode.
const EMERGENCY_EXIT_TICKS: u32 = 10;

/// OS available memory threshold (fraction) to enter emergency mode.
const EMERGENCY_ENTER_FRACTION: f64 = 0.10;

/// OS available memory threshold (fraction) to exit emergency mode.
const EMERGENCY_EXIT_FRACTION: f64 = 0.15;

// ── Workload phase ─────────────────────────────────────────────────────────

/// Detected workload pattern used to choose subsystem ratios.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadPhase {
    /// Predominantly foreground reads, minimal writes.
    ReadHeavy,
    /// Heavy write ingestion / backpressure.
    WriteHeavy,
    /// Concurrent reads and writes.
    Mixed,
    /// No significant I/O activity.
    Idle,
}

impl WorkloadPhase {
    /// Budget ratios: (l1, transit, dirty, staged).  Fractions must sum to 1.0.
    fn ratios(self) -> (f64, f64, f64, f64) {
        match self {
            Self::ReadHeavy => (0.60, 0.25, 0.10, 0.05),
            Self::WriteHeavy => (0.35, 0.15, 0.40, 0.10),
            Self::Mixed => (0.55, 0.25, 0.15, 0.05),
            Self::Idle => (0.50, 0.20, 0.20, 0.10),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::ReadHeavy => "read-heavy",
            Self::WriteHeavy => "write-heavy",
            Self::Mixed => "mixed",
            Self::Idle => "idle",
        }
    }
}

impl std::fmt::Display for WorkloadPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── Pressure signals (snapshot per tick) ───────────────────────────────────

/// A point-in-time snapshot of all pressure signals the controller uses.
#[derive(Debug, Clone)]
pub struct PressureSignals {
    pub fg_read_p90_ns: u64,
    pub fg_reads_inflight: usize,
    pub write_bp_events: u64,
    pub fast_bp_stall_max_ms: u64,
    pub transit_used_bytes: usize,
    pub transit_budget_bytes: usize,
    pub transit_fg_waiting: usize,
    pub l1_used_bytes: u64,
    pub l1_max_bytes: u64,
    pub l1_eviction_count: u64,
    pub l1_hits: u64,
    pub l2_hits: u64,
    pub prefetch_skips: u64,
    pub drained_mem_bytes: u64,
    pub drained_spill_bytes: u64,
    pub overlay_miss_exhaustions: u64,
    pub dirty_mem_bytes: usize,
    pub aimd_window: f64,
}

// ── Floor configuration ────────────────────────────────────────────────────

/// Minimum viable budgets (bytes) for each subsystem.
#[derive(Debug, Clone, Copy)]
pub struct SubsystemFloors {
    pub l1: u64,
    pub transit: u64,
    pub dirty: u64,
    pub staged: u64,
}

impl SubsystemFloors {
    /// Compute floors from the block size.
    pub fn from_block_size(block_size: u64) -> Self {
        Self {
            l1: 32 * block_size,                         // ~512 MB at 16 MB blocks
            transit: 4 * (block_size + 8 * 1024 * 1024), // ~128 MB (+ headroom)
            dirty: block_size,                           // 16 MB
            staged: 2 * block_size,                      // 32 MB
        }
    }

    pub fn total(&self) -> u64 {
        self.l1 + self.transit + self.dirty + self.staged
    }
}

// ── MemoryEnvelope ─────────────────────────────────────────────────────────

/// Adaptive memory budget coordinator.
///
/// Holds shared references to all elastic subsystems and distributes a global
/// memory grant across them based on workload phase and pressure signals.
pub struct MemoryEnvelope {
    /// Total detected system RAM (bytes).
    pub total_ram: u64,
    /// Soft target = `total_ram × target_pct`.
    pub soft_target: u64,
    /// Hard ceiling = `total_ram × ceiling_pct`.
    pub hard_ceiling: u64,
    /// Current memory grant (between soft_target and hard_ceiling).
    pub current_grant: AtomicU64,
    /// Block size used for step_up / step_down sizing.
    block_size: u64,
    /// Minimum budgets per subsystem.
    floors: SubsystemFloors,

    // ── handles to elastic subsystems ────────────────────────
    block_cache: Arc<BlockCache>,
    transit_budget: Arc<TransitMemoryBudget>,
    fetch_scheduler: Arc<FetchScheduler>,

    // ── controller state (interior, updated each tick) ───────
    phase: parking_lot::Mutex<PhaseState>,

    /// Cached `sysinfo::System` instance — reused across ticks so we only call
    /// `refresh_memory()` instead of re-scanning processes/CPUs every second.
    sys: parking_lot::Mutex<sysinfo::System>,
}

/// Mutable state guarded by a single mutex, updated once per tick.
struct PhaseState {
    current_phase: WorkloadPhase,
    /// The phase that has been proposed but hasn't reached hysteresis yet.
    candidate_phase: WorkloadPhase,
    /// How many consecutive ticks the candidate phase has been observed.
    candidate_ticks: u32,
    /// Consecutive ticks all retreat criteria have been met.
    retreat_ticks: u32,
    /// Whether we are in emergency mode.
    emergency: bool,
    /// Consecutive ticks OS memory has been above exit threshold.
    emergency_exit_ticks: u32,
    // ── delta tracking (previous tick's counter values) ──────
    prev_write_bp_events: u64,
    prev_drained_mem_bytes: u64,
    prev_drained_spill_bytes: u64,
    prev_overlay_miss_exhaustions: u64,
    prev_l1_eviction_count: u64,
    prev_prefetch_skips: u64,
}

impl PhaseState {
    fn new() -> Self {
        Self {
            current_phase: WorkloadPhase::Idle,
            candidate_phase: WorkloadPhase::Idle,
            candidate_ticks: 0,
            retreat_ticks: 0,
            emergency: false,
            emergency_exit_ticks: 0,
            prev_write_bp_events: 0,
            prev_drained_mem_bytes: 0,
            prev_drained_spill_bytes: 0,
            prev_overlay_miss_exhaustions: 0,
            prev_l1_eviction_count: 0,
            prev_prefetch_skips: 0,
        }
    }
}

/// Snapshot of the budgets applied by the controller per tick (for observability).
#[derive(Debug, Clone, Copy)]
pub struct EnvelopeSnapshot {
    pub current_grant: u64,
    pub phase: WorkloadPhase,
    pub l1_budget: u64,
    pub transit_budget: u64,
    pub dirty_budget: u64,
    pub staged_budget: u64,
    pub emergency: bool,
}

impl MemoryEnvelope {
    /// Create a new `MemoryEnvelope`.
    ///
    /// # Arguments
    /// - `total_ram`: Detected system RAM in bytes.
    /// - `soft_target_bytes`: Absolute soft target in bytes (e.g. 4 GiB).
    /// - `ceiling_pct`: Hard ceiling as fraction of total RAM (e.g. 0.50).
    /// - `block_size`: VFS block size in bytes (typically 16 MiB).
    /// - `block_cache`: Shared block cache handle.
    /// - `transit_budget`: Shared transit memory budget handle.
    /// - `fetch_scheduler`: Shared fetch scheduler handle.
    pub fn new(
        total_ram: u64,
        soft_target_bytes: u64,
        ceiling_pct: f64,
        block_size: u64,
        block_cache: Arc<BlockCache>,
        transit_budget: Arc<TransitMemoryBudget>,
        fetch_scheduler: Arc<FetchScheduler>,
    ) -> Self {
        let soft_target = soft_target_bytes;
        let hard_ceiling = (total_ram as f64 * ceiling_pct) as u64;
        let floors = SubsystemFloors::from_block_size(block_size);

        // Ensure soft target is at least enough to meet all floor constraints.
        let minimum_grant = floors.total() + FIXED_OVERHEAD_BYTES;
        let soft_target = soft_target.max(minimum_grant);
        let hard_ceiling = hard_ceiling.max(soft_target);

        if minimum_grant > soft_target_bytes {
            warn!(
                total_ram_mb = total_ram / (1024 * 1024),
                floor_total_mb = floors.total() / (1024 * 1024),
                effective_soft_mb = soft_target / (1024 * 1024),
                "soft target too low for floor constraints — grant raised to floor sum"
            );
        }

        info!(
            total_ram_mb = total_ram / (1024 * 1024),
            soft_target_mb = soft_target / (1024 * 1024),
            hard_ceiling_mb = hard_ceiling / (1024 * 1024),
            block_size_mb = block_size / (1024 * 1024),
            "MemoryEnvelope initialised"
        );

        // Pre-create the sysinfo::System with memory info so the first tick
        // only needs a cheap `refresh_memory()` call.
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();

        Self {
            total_ram,
            soft_target,
            hard_ceiling,
            current_grant: AtomicU64::new(soft_target),
            block_size,
            floors,
            block_cache,
            transit_budget,
            fetch_scheduler,
            phase: parking_lot::Mutex::new(PhaseState::new()),
            sys: parking_lot::Mutex::new(sys),
        }
    }

    /// Read the current grant.
    pub fn grant(&self) -> u64 {
        self.current_grant.load(Ordering::Acquire)
    }

    /// Build a snapshot for observability / status output.
    pub fn snapshot(&self) -> EnvelopeSnapshot {
        let grant = self.grant();
        let ps = self.phase.lock();
        let (l1, transit, dirty, staged) = self.compute_budgets(grant, ps.current_phase);
        EnvelopeSnapshot {
            current_grant: grant,
            phase: ps.current_phase,
            l1_budget: l1,
            transit_budget: transit,
            dirty_budget: dirty,
            staged_budget: staged,
            emergency: ps.emergency,
        }
    }

    // ── Tick loop ──────────────────────────────────────────────

    /// Run the control loop.  Spawned as a background tokio task.
    pub async fn run(self: &Arc<Self>, vfs: Arc<StudioVfs>, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    debug!("MemoryEnvelope shutting down");
                    return;
                }
                _ = interval.tick() => {
                    self.tick(&vfs);
                }
            }
        }
    }

    /// Execute one control cycle.
    fn tick(&self, vfs: &StudioVfs) {
        // 1. Observe
        let signals = Self::collect_signals(vfs);

        // 2. Decide + Act (under lock)
        let mut ps = self.phase.lock();

        // Compute deltas from previous tick.
        let write_bp_delta = signals
            .write_bp_events
            .saturating_sub(ps.prev_write_bp_events);
        let _drained_delta = (signals.drained_mem_bytes + signals.drained_spill_bytes)
            .saturating_sub(ps.prev_drained_mem_bytes + ps.prev_drained_spill_bytes);
        let _overlay_miss_delta = signals
            .overlay_miss_exhaustions
            .saturating_sub(ps.prev_overlay_miss_exhaustions);
        let eviction_delta = signals
            .l1_eviction_count
            .saturating_sub(ps.prev_l1_eviction_count);

        // Update previous counters.
        ps.prev_write_bp_events = signals.write_bp_events;
        ps.prev_drained_mem_bytes = signals.drained_mem_bytes;
        ps.prev_drained_spill_bytes = signals.drained_spill_bytes;
        ps.prev_overlay_miss_exhaustions = signals.overlay_miss_exhaustions;
        ps.prev_l1_eviction_count = signals.l1_eviction_count;
        ps.prev_prefetch_skips = signals.prefetch_skips;

        // ── Emergency mode check ──

        // We read OS available memory from the `sysinfo` crate lazily.
        // To avoid the cost of `sysinfo::System::new()` every tick, we only
        // check when the RSS signals suggest trouble (or we're already in
        // emergency mode).
        let os_memory_low = self.check_os_memory_pressure();

        if os_memory_low && !ps.emergency {
            warn!("system memory low — StudioFS retreated to minimum");
            ps.emergency = true;
            ps.emergency_exit_ticks = 0;
            self.apply_emergency(&signals, vfs);
            return;
        }

        if ps.emergency {
            if self.check_os_memory_recovered() {
                ps.emergency_exit_ticks += 1;
            } else {
                ps.emergency_exit_ticks = 0;
            }
            if ps.emergency_exit_ticks >= EMERGENCY_EXIT_TICKS {
                info!("system memory recovered — resuming adaptive control");
                ps.emergency = false;
                // Fall through to normal control loop.
            } else {
                // Stay in emergency — budgets already at floor.
                return;
            }
        }

        // ── Phase detection (hysteresis) ──

        let proposed = Self::detect_phase(&signals, write_bp_delta);
        if proposed == ps.candidate_phase {
            ps.candidate_ticks += 1;
        } else {
            ps.candidate_phase = proposed;
            ps.candidate_ticks = 1;
        }
        if ps.candidate_ticks >= PHASE_HYSTERESIS_TICKS && ps.candidate_phase != ps.current_phase {
            debug!(
                from = ps.current_phase.as_str(),
                to = ps.candidate_phase.as_str(),
                "workload phase transition"
            );
            ps.current_phase = ps.candidate_phase;
        }

        // ── Grant sizing (burst / retreat) ──

        let current_grant = self.grant();
        let new_grant = self.compute_grant(
            current_grant,
            &signals,
            write_bp_delta,
            eviction_delta,
            &mut ps,
        );
        if new_grant != current_grant {
            self.current_grant.store(new_grant, Ordering::Release);
        }

        let phase = ps.current_phase;
        // Drop the lock before doing I/O (subsystem budget calls).
        drop(ps);

        // 3. Act — apply budgets.
        let (l1, transit, dirty, staged) = self.compute_budgets(new_grant, phase);
        self.apply_budgets(l1, transit, dirty, staged, &signals, vfs);

        debug!(
            grant_mb = new_grant / (1024 * 1024),
            phase = phase.as_str(),
            l1_mb = l1 / (1024 * 1024),
            transit_mb = transit / (1024 * 1024),
            dirty_mb = dirty / (1024 * 1024),
            staged_mb = staged / (1024 * 1024),
            "envelope tick"
        );
    }

    // ── Signal collection ──────────────────────────────────────

    fn collect_signals(vfs: &StudioVfs) -> PressureSignals {
        let ingress = vfs.ingress_stats();
        let fg_stats = vfs.foreground_read_protection_stats();
        let transit_snap = vfs.transit_memory_budget().snapshot();

        PressureSignals {
            fg_read_p90_ns: fg_stats.p90_latency_ns.unwrap_or(0),
            fg_reads_inflight: vfs.foreground_reads_inflight(),
            write_bp_events: ingress.write_backpressure_events,
            fast_bp_stall_max_ms: ingress.fast_backpressure_stall_max_ms,
            transit_used_bytes: transit_snap.used_bytes,
            transit_budget_bytes: transit_snap.budget_bytes,
            transit_fg_waiting: transit_snap.waiting_foreground,
            l1_used_bytes: vfs.block_cache().l1_bytes(),
            l1_max_bytes: vfs.block_cache().l1_max_bytes(),
            l1_eviction_count: vfs.block_cache().l1_eviction_count(),
            l1_hits: vfs.block_cache().l1_hit_count(),
            l2_hits: vfs.block_cache().l2_hit_count(),
            prefetch_skips: vfs
                .fetch_scheduler()
                .metrics
                .prefetch_skips
                .load(Ordering::Relaxed),
            drained_mem_bytes: ingress.drained_mem_bytes,
            drained_spill_bytes: ingress.drained_spill_bytes,
            overlay_miss_exhaustions: ingress.local_overlay_cache_miss_retry_exhaustions,
            dirty_mem_bytes: ingress.dirty_mem_bytes,
            aimd_window: vfs.fetch_scheduler().window(),
        }
    }

    // ── Phase detection ────────────────────────────────────────

    fn detect_phase(signals: &PressureSignals, write_bp_delta: u64) -> WorkloadPhase {
        let has_reads = signals.fg_reads_inflight > 2;
        let has_writes = write_bp_delta > 0
            || (signals.l1_max_bytes > 0
                && signals.dirty_mem_bytes as u64 > signals.l1_max_bytes / 2);

        match (has_reads, has_writes) {
            (true, false) => WorkloadPhase::ReadHeavy,
            (false, true) => WorkloadPhase::WriteHeavy,
            (true, true) => WorkloadPhase::Mixed,
            (false, false) => WorkloadPhase::Idle,
        }
    }

    // ── Grant sizing ───────────────────────────────────────────

    fn compute_grant(
        &self,
        current: u64,
        signals: &PressureSignals,
        write_bp_delta: u64,
        eviction_delta: u64,
        ps: &mut PhaseState,
    ) -> u64 {
        let step_up = 4 * self.block_size; // 64 MB per tick (fast ramp-up)
        let step_down = 2 * self.block_size; // 32 MB per tick (2:1 asymmetry)

        // ── Burst criteria (any one triggers) ──
        let fg_p90_ms = signals.fg_read_p90_ns / 1_000_000;
        let l1_utilization = if signals.l1_max_bytes > 0 {
            signals.l1_used_bytes as f64 / signals.l1_max_bytes as f64
        } else {
            0.0
        };

        let cache_thrashing = fg_p90_ms > 50 && l1_utilization > 0.85;
        let l1_pressure = eviction_delta > 0 && l1_utilization > 0.80;
        let transit_starved = signals.transit_fg_waiting > 0;
        let write_stalling = write_bp_delta > 0 && signals.fast_bp_stall_max_ms > 200;

        let should_burst = cache_thrashing || l1_pressure || transit_starved || write_stalling;

        // ── Retreat criteria (all must hold) ──
        let retreat_ok = fg_p90_ms < 20
            && write_bp_delta == 0
            && signals.transit_fg_waiting == 0
            && l1_utilization < 0.85;

        if should_burst {
            ps.retreat_ticks = 0;
            return current.saturating_add(step_up).min(self.hard_ceiling);
        }

        if retreat_ok {
            ps.retreat_ticks += 1;
        } else {
            ps.retreat_ticks = 0;
        }

        if ps.retreat_ticks >= RETREAT_HYSTERESIS_TICKS {
            return current.saturating_sub(step_down).max(self.soft_target);
        }

        // No change.
        current
    }

    // ── Budget computation ─────────────────────────────────────

    fn compute_budgets(&self, grant: u64, phase: WorkloadPhase) -> (u64, u64, u64, u64) {
        let allocatable = grant.saturating_sub(FIXED_OVERHEAD_BYTES);
        let (r_l1, r_transit, r_dirty, r_staged) = phase.ratios();

        let raw_l1 = (allocatable as f64 * r_l1) as u64;
        let raw_transit = (allocatable as f64 * r_transit) as u64;
        let raw_dirty = (allocatable as f64 * r_dirty) as u64;
        let raw_staged = (allocatable as f64 * r_staged) as u64;

        // Enforce floors.
        let l1 = raw_l1.max(self.floors.l1);
        let transit = raw_transit.max(self.floors.transit);
        let dirty = raw_dirty.max(self.floors.dirty);
        let staged = raw_staged.max(self.floors.staged);

        (l1, transit, dirty, staged)
    }

    // ── Budget application ─────────────────────────────────────

    fn apply_budgets(
        &self,
        l1: u64,
        transit: u64,
        dirty: u64,
        staged: u64,
        signals: &PressureSignals,
        vfs: &StudioVfs,
    ) {
        // L1 cache — shrinks > 20 % trigger immediate evictor signal.
        let old_l1 = self.block_cache.l1_max_bytes();
        self.block_cache.set_l1_max_bytes(l1);
        if l1 < old_l1 && (old_l1 - l1) > old_l1 / 5 {
            // set_l1_max_bytes already signals via evict_notify on shrink.
            debug!(
                old_mb = old_l1 / (1024 * 1024),
                new_mb = l1 / (1024 * 1024),
                "L1 budget shrink > 20%, evictor notified"
            );
        }

        // Transit memory budget.
        self.transit_budget.set_budget_bytes(transit as usize);

        // Dirty buffer — apply proportional cap with legacy ceiling.
        let effective_dirty = dirty
            .min(l1 * 2) // never more than 2× L1
            .min(12 * 1024 * 1024 * 1024); // 12 GB legacy cap
        vfs.set_max_dirty_bytes(effective_dirty as usize);

        // Staged upload.
        vfs.retarget_staged_upload(staged as usize);

        // ── Adaptive prefetch timeout ──
        self.adjust_prefetch_timeout(signals);
    }

    // ── Adaptive prefetch timeout (§3.1) ───────────────────────

    fn adjust_prefetch_timeout(&self, signals: &PressureSignals) {
        let utilization = if signals.transit_budget_bytes > 0 {
            signals.transit_used_bytes as f64 / signals.transit_budget_bytes as f64
        } else {
            1.0
        };

        let timeout_ms = if utilization < 0.50 {
            150
        } else if utilization < 0.75 {
            75
        } else if utilization < 0.90 {
            25
        } else {
            0
        };

        self.fetch_scheduler
            .set_prefetch_acquire_wait_ms(timeout_ms);
    }

    // ── Emergency mode ─────────────────────────────────────────

    fn apply_emergency(&self, _signals: &PressureSignals, vfs: &StudioVfs) {
        let grant = self.floors.total() + FIXED_OVERHEAD_BYTES;
        self.current_grant.store(grant, Ordering::Release);

        // All subsystems to floor.
        self.block_cache.set_l1_max_bytes(self.floors.l1);
        self.transit_budget
            .set_budget_bytes(self.floors.transit as usize);
        vfs.set_max_dirty_bytes(self.floors.dirty as usize);
        vfs.retarget_staged_upload(self.floors.staged as usize);

        // Aggressive prefetch cutoff.
        self.fetch_scheduler.set_prefetch_acquire_wait_ms(0);

        warn!(
            grant_mb = grant / (1024 * 1024),
            l1_floor_mb = self.floors.l1 / (1024 * 1024),
            transit_floor_mb = self.floors.transit / (1024 * 1024),
            dirty_floor_mb = self.floors.dirty / (1024 * 1024),
            staged_floor_mb = self.floors.staged / (1024 * 1024),
            "emergency mode — budgets at floor"
        );
    }

    // ── OS memory pressure detection ───────────────────────────

    /// Check if OS available physical memory is below the emergency threshold.
    ///
    /// Reuses the cached `sysinfo::System` — only `refresh_memory()` is called,
    /// which is ~10-20× cheaper than `System::new()` on Windows.
    fn check_os_memory_pressure(&self) -> bool {
        let mut sys = self.sys.lock();
        sys.refresh_memory();
        let available = sys.available_memory(); // bytes
        drop(sys);
        let threshold = (self.total_ram as f64 * EMERGENCY_ENTER_FRACTION) as u64;
        available < threshold
    }

    /// Check if OS memory has recovered above the exit threshold.
    fn check_os_memory_recovered(&self) -> bool {
        let mut sys = self.sys.lock();
        sys.refresh_memory();
        let available = sys.available_memory();
        drop(sys);
        let threshold = (self.total_ram as f64 * EMERGENCY_EXIT_FRACTION) as u64;
        available >= threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_ratios_sum_to_one() {
        for phase in [
            WorkloadPhase::ReadHeavy,
            WorkloadPhase::WriteHeavy,
            WorkloadPhase::Mixed,
            WorkloadPhase::Idle,
        ] {
            let (a, b, c, d) = phase.ratios();
            let sum = a + b + c + d;
            assert!(
                (sum - 1.0).abs() < 1e-9,
                "phase {phase:?} ratios sum to {sum}"
            );
        }
    }

    #[test]
    fn floors_from_block_size() {
        let block_size = 16 * 1024 * 1024; // 16 MiB
        let floors = SubsystemFloors::from_block_size(block_size);
        assert_eq!(floors.l1, 512 * 1024 * 1024);
        assert_eq!(floors.dirty, 16 * 1024 * 1024);
        assert_eq!(floors.staged, 32 * 1024 * 1024);
        // transit = 4 * (16 MiB + 8 MiB) = 96 MiB
        assert_eq!(floors.transit, 4 * (16 + 8) * 1024 * 1024);
    }

    #[test]
    fn detect_phase_idle_when_quiet() {
        let signals = make_test_signals();
        let phase = MemoryEnvelope::detect_phase(&signals, 0);
        assert_eq!(phase, WorkloadPhase::Idle);
    }

    #[test]
    fn detect_phase_read_heavy() {
        let mut signals = make_test_signals();
        signals.fg_reads_inflight = 5;
        let phase = MemoryEnvelope::detect_phase(&signals, 0);
        assert_eq!(phase, WorkloadPhase::ReadHeavy);
    }

    #[test]
    fn detect_phase_write_heavy() {
        let signals = make_test_signals();
        let phase = MemoryEnvelope::detect_phase(&signals, 10);
        assert_eq!(phase, WorkloadPhase::WriteHeavy);
    }

    #[test]
    fn detect_phase_mixed() {
        let mut signals = make_test_signals();
        signals.fg_reads_inflight = 5;
        let phase = MemoryEnvelope::detect_phase(&signals, 10);
        assert_eq!(phase, WorkloadPhase::Mixed);
    }

    fn make_test_signals() -> PressureSignals {
        PressureSignals {
            fg_read_p90_ns: 0,
            fg_reads_inflight: 0,
            write_bp_events: 0,
            fast_bp_stall_max_ms: 0,
            transit_used_bytes: 0,
            transit_budget_bytes: 1024 * 1024 * 1024,
            transit_fg_waiting: 0,
            l1_used_bytes: 0,
            l1_max_bytes: 2 * 1024 * 1024 * 1024,
            l1_eviction_count: 0,
            l1_hits: 0,
            l2_hits: 0,
            prefetch_skips: 0,
            drained_mem_bytes: 0,
            drained_spill_bytes: 0,
            overlay_miss_exhaustions: 0,
            dirty_mem_bytes: 0,
            aimd_window: 4.0,
        }
    }
}
