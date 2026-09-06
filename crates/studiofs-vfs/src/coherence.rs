//! Platform delivery contract for committed local-view invalidation scopes.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Immediate admission result, never proof of native cache invalidation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidationAcceptance {
    /// The adapter retained all scopes for asynchronous processing.
    Queued,
    /// The adapter cannot establish all requested effects.
    Unsupported,
    /// Work could not be retained, for example during shutdown or saturation.
    Failed,
}

/// Mount-local replication/cache coherence state.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoherenceHealthState {
    /// Startup or restart has not established a safe boundary yet.
    #[default]
    Unknown,
    /// A fresh boundary is being replayed and invalidation coverage is being restored.
    CatchingUp,
    /// The applied boundary and retained invalidations are within the configured budgets.
    Healthy,
    /// A synchronization, lag, or delivery condition makes cached metadata unsafe.
    Degraded,
    /// The mount is shutting down and cannot admit new cache state.
    Stopped,
}

/// Stable reason exposed with a coherence-health snapshot.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoherenceHealthReason {
    /// No progress evidence has been recorded since mount startup.
    #[default]
    Startup,
    /// A synchronization pass is restoring a boundary after startup/reconnect.
    CatchingUp,
    /// A previously degraded mount has observed a reconnect and must catch up.
    Reconnected,
    /// The applied boundary and retained invalidations are within budget.
    Healthy,
    /// The metadata source or apply operation failed.
    ApplyFailed,
    /// The metadata source could not be reached.
    Disconnected,
    /// The source reports more unapplied metadata than the configured budget.
    ExcessiveLag,
    /// An invalidation batch could not be retained.
    InvalidationFailed,
    /// The adapter does not support the required invalidation scope.
    InvalidationUnsupported,
    /// A generation was only partially retained (for example an active writer deferred it).
    InvalidationIncomplete,
    /// Delivery stayed pending beyond the configured age budget.
    DeliveryTimeout,
    /// The source did not provide enough progress evidence to promote the cache.
    MissingProgress,
    /// The mount was stopped.
    Shutdown,
}

/// Effective cache policy selected from health evidence.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoherenceCachePolicy {
    /// Do not admit long-lived operating-system metadata cache state.
    #[default]
    Conservative,
    /// Health permits a later policy layer to promote cache retention.
    AggressiveEligible,
    /// Reserved for an explicit adaptive policy selection (not enabled here).
    Aggressive,
}

/// Finite budgets used by the health state machine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoherenceHealthBudget {
    /// Maximum number of metadata versions that may remain unapplied.
    pub max_apply_lag: u64,
    /// Maximum age of retained invalidation work before health degrades.
    pub max_delivery_age_ms: u64,
}

impl Default for CoherenceHealthBudget {
    fn default() -> Self {
        Self {
            max_apply_lag: 4096,
            max_delivery_age_ms: 5000,
        }
    }
}

impl CoherenceHealthBudget {
    /// Derive a finite delivery budget from the existing metadata poll cadence.
    pub const fn from_invalidation_interval_ms(interval_ms: u64) -> Self {
        let delivery_age_ms = interval_ms.saturating_mul(3);
        Self {
            max_apply_lag: 4096,
            max_delivery_age_ms: if delivery_age_ms < 1000 {
                1000
            } else {
                delivery_age_ms
            },
        }
    }
}

/// Serializable health information safe to expose through diagnostics.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct CoherenceHealthSnapshot {
    /// Current mount-local state.
    pub state: CoherenceHealthState,
    /// Why the state was last changed.
    pub reason: CoherenceHealthReason,
    /// Effective cache policy derived from the health state.
    pub policy: CoherenceCachePolicy,
    /// Highest metadata boundary applied to the local serving view.
    pub applied_boundary: Option<u64>,
    /// Highest invalidation generation retained by the adapter.
    pub covered_boundary: Option<u64>,
    /// Difference between observed remote head and applied boundary.
    pub apply_lag: Option<u64>,
    /// Age of currently retained delivery work.
    pub pending_delivery_age_ms: Option<u64>,
    /// Monotonic count of invalidation delivery failures observed by the bridge.
    pub delivery_failure_count: u64,
    /// Last time progress or coverage was observed.
    pub last_progress_unix_ms: Option<u64>,
    /// Effective finite safety budgets.
    pub budget: CoherenceHealthBudget,
}

impl Default for CoherenceHealthSnapshot {
    fn default() -> Self {
        Self {
            state: CoherenceHealthState::Unknown,
            reason: CoherenceHealthReason::Startup,
            policy: CoherenceCachePolicy::Conservative,
            applied_boundary: None,
            covered_boundary: None,
            apply_lag: None,
            pending_delivery_age_ms: None,
            delivery_failure_count: 0,
            last_progress_unix_ms: None,
            budget: CoherenceHealthBudget::default(),
        }
    }
}

/// Deterministic, mount-local health transition coordinator.
#[derive(Debug, Clone)]
pub(crate) struct CoherenceHealthTracker {
    snapshot: CoherenceHealthSnapshot,
    awaiting_coverage: bool,
    reconnecting: bool,
    has_sync_boundary: bool,
    coverage_gap: bool,
    unresolved_semantic_generations: BTreeSet<u64>,
}

impl CoherenceHealthTracker {
    /// Start conservatively before any replication or invalidation evidence exists.
    pub(crate) fn new(budget: CoherenceHealthBudget) -> Self {
        Self {
            snapshot: CoherenceHealthSnapshot {
                budget,
                ..CoherenceHealthSnapshot::default()
            },
            awaiting_coverage: true,
            reconnecting: false,
            has_sync_boundary: false,
            coverage_gap: false,
            unresolved_semantic_generations: BTreeSet::new(),
        }
    }

    /// Return the current serializable state.
    pub(crate) fn snapshot(&self) -> CoherenceHealthSnapshot {
        let mut snapshot = self.snapshot;
        snapshot.policy = if self.allows_cache() {
            CoherenceCachePolicy::AggressiveEligible
        } else {
            CoherenceCachePolicy::Conservative
        };
        snapshot
    }

    /// Return whether a read-only OS cache admission is currently safe.
    pub(crate) fn allows_cache(&self) -> bool {
        self.snapshot.state == CoherenceHealthState::Healthy
            && !self.awaiting_coverage
            && self
                .snapshot
                .apply_lag
                .is_none_or(|lag| lag <= self.snapshot.budget.max_apply_lag)
            && self
                .snapshot
                .pending_delivery_age_ms
                .is_none_or(|age| age <= self.snapshot.budget.max_delivery_age_ms)
    }

    /// Mark the beginning of a startup or reconnect catch-up pass.
    pub(crate) fn note_sync_started(&mut self) {
        if self.snapshot.state == CoherenceHealthState::Stopped {
            return;
        }
        if self.snapshot.state == CoherenceHealthState::Degraded {
            self.reconnecting = true;
            self.has_sync_boundary = false;
            return;
        }
        if self.snapshot.state != CoherenceHealthState::Healthy {
            self.has_sync_boundary = false;
            self.snapshot.state = CoherenceHealthState::CatchingUp;
            self.snapshot.reason = CoherenceHealthReason::CatchingUp;
        }
    }

    /// Record a successfully applied metadata boundary.
    pub(crate) fn note_sync_success(
        &mut self,
        applied_boundary: Option<u64>,
        remote_boundary: Option<u64>,
        coverage_complete: bool,
        now_unix_ms: u64,
    ) {
        if self.snapshot.state == CoherenceHealthState::Stopped {
            return;
        }

        self.has_sync_boundary = false;

        let was_degraded =
            self.snapshot.state == CoherenceHealthState::Degraded || self.reconnecting;
        self.snapshot.applied_boundary = applied_boundary;
        self.snapshot.apply_lag = remote_boundary
            .zip(applied_boundary)
            .map(|(remote, applied)| remote.saturating_sub(applied));
        self.snapshot.last_progress_unix_ms = Some(now_unix_ms);
        // A reconnect always requires an explicit post-sync coverage
        // confirmation, even when the sync produced no new batches. This
        // keeps `Degraded -> CatchingUp` observable and prevents a successful
        // command alone from promoting the cache.
        self.awaiting_coverage = !coverage_complete || was_degraded || self.coverage_gap;
        self.reconnecting = false;

        if remote_boundary.is_some() && applied_boundary.is_none() {
            self.degrade(CoherenceHealthReason::MissingProgress);
            return;
        }

        if remote_boundary
            .zip(applied_boundary)
            .is_some_and(|(remote, applied)| remote < applied)
        {
            self.degrade(CoherenceHealthReason::MissingProgress);
            return;
        }

        if self
            .snapshot
            .apply_lag
            .is_some_and(|lag| lag > self.snapshot.budget.max_apply_lag)
        {
            self.degrade(CoherenceHealthReason::ExcessiveLag);
            return;
        }

        self.has_sync_boundary = true;
        self.snapshot.state = CoherenceHealthState::CatchingUp;
        self.snapshot.reason = if was_degraded {
            CoherenceHealthReason::Reconnected
        } else {
            CoherenceHealthReason::CatchingUp
        };
        self.try_promote();
    }

    /// Record semantic invalidation coverage admitted by the platform adapter.
    pub(crate) fn note_invalidation(
        &mut self,
        generation: u64,
        acceptance: InvalidationAcceptance,
        generation_complete: bool,
        now_unix_ms: u64,
    ) {
        if self.snapshot.state == CoherenceHealthState::Stopped {
            return;
        }
        match acceptance {
            InvalidationAcceptance::Queued if generation_complete => {
                self.snapshot.covered_boundary =
                    Some(self.snapshot.covered_boundary.unwrap_or(0).max(generation));
                self.snapshot.pending_delivery_age_ms = Some(0);
                self.snapshot.last_progress_unix_ms = Some(now_unix_ms);
                self.awaiting_coverage = false;
                self.coverage_gap = false;
                self.unresolved_semantic_generations.remove(&generation);
                self.try_promote();
            }
            InvalidationAcceptance::Unsupported => {
                self.coverage_gap = true;
                self.unresolved_semantic_generations.insert(generation);
                self.degrade(CoherenceHealthReason::InvalidationUnsupported)
            }
            InvalidationAcceptance::Failed => {
                self.coverage_gap = true;
                self.unresolved_semantic_generations.insert(generation);
                self.degrade(if generation_complete {
                    CoherenceHealthReason::InvalidationFailed
                } else {
                    CoherenceHealthReason::InvalidationIncomplete
                })
            }
            InvalidationAcceptance::Queued => {
                self.coverage_gap = true;
                self.unresolved_semantic_generations.insert(generation);
                self.degrade(CoherenceHealthReason::InvalidationIncomplete)
            }
        }
    }

    /// Confirm that the adapter has retained the fresh catch-up boundary.
    pub(crate) fn note_coverage_confirmed(
        &mut self,
        covered_boundary: Option<u64>,
        now_unix_ms: u64,
    ) {
        if self.snapshot.state == CoherenceHealthState::Stopped {
            return;
        }
        if !self.unresolved_semantic_generations.is_empty() {
            return;
        }
        if let Some(boundary) = covered_boundary {
            self.snapshot.covered_boundary =
                Some(self.snapshot.covered_boundary.unwrap_or(0).max(boundary));
        }
        self.snapshot.last_progress_unix_ms = Some(now_unix_ms);
        self.coverage_gap = false;
        self.awaiting_coverage = false;
        self.try_promote();
    }

    /// Update pending delivery age, or clear it when the adapter reports no pending work.
    pub(crate) fn note_delivery_age(&mut self, age_ms: Option<u64>, now_unix_ms: u64) {
        if self.snapshot.state == CoherenceHealthState::Stopped {
            return;
        }
        self.snapshot.pending_delivery_age_ms = age_ms;
        if age_ms.is_some_and(|age| age > self.snapshot.budget.max_delivery_age_ms) {
            self.degrade(CoherenceHealthReason::DeliveryTimeout);
        } else if age_ms.is_some() {
            self.snapshot.last_progress_unix_ms = Some(now_unix_ms);
        }
    }

    /// Record monotonic post-admission delivery failures from the platform bridge.
    pub(crate) fn note_delivery_failure_count(&mut self, count: u64) {
        if self.snapshot.state == CoherenceHealthState::Stopped {
            return;
        }
        let increased = count > self.snapshot.delivery_failure_count;
        self.snapshot.delivery_failure_count = count;
        if increased {
            self.coverage_gap = true;
            self.degrade(CoherenceHealthReason::InvalidationFailed);
        }
    }

    /// Record a synchronization failure and revoke cache eligibility immediately.
    pub(crate) fn note_sync_failure(&mut self, reason: CoherenceHealthReason) {
        if self.snapshot.state != CoherenceHealthState::Stopped {
            self.degrade(reason);
        }
    }

    /// Permanently stop health transitions for this mount.
    pub(crate) fn stop(&mut self) {
        self.snapshot.state = CoherenceHealthState::Stopped;
        self.snapshot.reason = CoherenceHealthReason::Shutdown;
        self.awaiting_coverage = true;
        self.reconnecting = false;
        self.has_sync_boundary = false;
    }

    fn degrade(&mut self, reason: CoherenceHealthReason) {
        if self.snapshot.state == CoherenceHealthState::Stopped {
            return;
        }
        self.snapshot.state = CoherenceHealthState::Degraded;
        self.snapshot.reason = reason;
        self.awaiting_coverage = true;
        self.has_sync_boundary = false;
    }

    fn try_promote(&mut self) {
        if self.snapshot.state == CoherenceHealthState::CatchingUp
            && !self.awaiting_coverage
            && self.has_sync_boundary
            && self
                .snapshot
                .apply_lag
                .is_none_or(|lag| lag <= self.snapshot.budget.max_apply_lag)
        {
            self.snapshot.state = CoherenceHealthState::Healthy;
            self.snapshot.reason = CoherenceHealthReason::Healthy;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_requires_catch_up_and_coverage_before_cache_admission() {
        let mut tracker = CoherenceHealthTracker::new(CoherenceHealthBudget {
            max_apply_lag: 2,
            max_delivery_age_ms: 100,
        });

        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Unknown);
        assert!(!tracker.allows_cache());

        tracker.note_sync_started();
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::CatchingUp);
        tracker.note_sync_success(Some(5), Some(5), false, 20);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::CatchingUp);
        tracker.note_invalidation(7, InvalidationAcceptance::Queued, true, 21);

        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Healthy);
        assert!(tracker.allows_cache());
        assert_eq!(tracker.snapshot().applied_boundary, Some(5));
        assert_eq!(tracker.snapshot().covered_boundary, Some(7));
    }

    #[test]
    fn unsafe_events_degrade_and_reconnect_must_catch_up_again() {
        let mut tracker = CoherenceHealthTracker::new(CoherenceHealthBudget {
            max_apply_lag: 2,
            max_delivery_age_ms: 100,
        });
        tracker.note_sync_started();
        tracker.note_sync_success(Some(2), Some(2), true, 2);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Healthy);

        tracker.note_invalidation(3, InvalidationAcceptance::Failed, false, 3);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Degraded);
        assert!(!tracker.allows_cache());

        tracker.note_sync_started();
        tracker.note_sync_success(Some(4), Some(4), false, 5);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::CatchingUp);
        tracker.note_invalidation(4, InvalidationAcceptance::Queued, true, 6);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Healthy);
    }

    #[test]
    fn lag_delivery_age_and_shutdown_are_fail_safe() {
        let mut tracker = CoherenceHealthTracker::new(CoherenceHealthBudget {
            max_apply_lag: 2,
            max_delivery_age_ms: 10,
        });
        tracker.note_sync_started();
        tracker.note_sync_success(Some(2), Some(5), true, 2);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Degraded);

        tracker.note_sync_started();
        tracker.note_sync_success(Some(5), Some(5), false, 4);
        tracker.note_invalidation(5, InvalidationAcceptance::Queued, true, 5);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Healthy);
        tracker.note_delivery_age(Some(11), 16);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Degraded);

        tracker.stop();
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Stopped);
        tracker.note_sync_started();
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Stopped);
    }

    #[test]
    fn reconnect_success_waits_for_explicit_coverage_confirmation() {
        let mut tracker = CoherenceHealthTracker::new(CoherenceHealthBudget::default());
        tracker.note_sync_started();
        tracker.note_sync_success(Some(1), Some(1), true, 2);
        assert!(tracker.allows_cache());

        tracker.note_sync_failure(CoherenceHealthReason::Disconnected);
        tracker.note_sync_started();
        tracker.note_sync_success(Some(2), Some(2), true, 5);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::CatchingUp);
        assert!(!tracker.allows_cache());

        tracker.note_coverage_confirmed(Some(2), 6);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Healthy);
    }

    #[test]
    fn invalidation_coverage_cannot_promote_without_sync_boundary() {
        let mut tracker = CoherenceHealthTracker::new(CoherenceHealthBudget::default());
        tracker.note_sync_started();
        tracker.note_invalidation(1, InvalidationAcceptance::Queued, true, 2);

        assert_eq!(tracker.snapshot().state, CoherenceHealthState::CatchingUp);
        assert!(!tracker.allows_cache());

        tracker.note_delivery_failure_count(1);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Degraded);
        assert_eq!(tracker.snapshot().delivery_failure_count, 1);

        tracker.note_sync_started();
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Degraded);
        tracker.note_sync_success(Some(2), Some(2), true, 5);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::CatchingUp);
        tracker.note_coverage_confirmed(Some(2), 6);
        assert_eq!(tracker.snapshot().state, CoherenceHealthState::Healthy);
        assert!(tracker.allows_cache());
    }

    #[test]
    fn unsupported_semantic_coverage_cannot_heal_from_an_empty_sync() {
        let mut tracker = CoherenceHealthTracker::new(CoherenceHealthBudget::default());
        tracker.note_sync_started();
        tracker.note_sync_success(Some(1), Some(1), true, 2);
        assert!(tracker.allows_cache());

        tracker.note_invalidation(2, InvalidationAcceptance::Unsupported, true, 3);
        tracker.note_sync_started();
        tracker.note_sync_success(Some(2), Some(2), true, 4);
        tracker.note_coverage_confirmed(Some(2), 5);

        assert_eq!(tracker.snapshot().state, CoherenceHealthState::CatchingUp);
        assert!(!tracker.allows_cache());
    }
}
