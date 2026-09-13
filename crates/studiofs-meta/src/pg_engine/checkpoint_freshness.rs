use crate::engine::{MetadataCheckpointFreshnessDiagnostics, MetadataCheckpointRefreshReason};
use std::time::Duration;
use tracing::warn;

pub(super) const DEFAULT_MAX_VERSION_LAG: u64 = 10_000;
pub(super) const DEFAULT_MAX_EVENT_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const DEFAULT_MAX_CHECKPOINT_AGE_SECS: u64 = 60 * 60;

#[derive(Clone, Copy, Debug)]
pub(super) struct CheckpointFreshnessPolicy {
    pub(super) max_version_lag: u64,
    pub(super) max_event_payload_bytes: u64,
    pub(super) max_checkpoint_age: Duration,
}

impl CheckpointFreshnessPolicy {
    pub(super) const fn new(
        max_version_lag: u64,
        max_event_payload_bytes: u64,
        max_checkpoint_age: Duration,
    ) -> Self {
        Self {
            max_version_lag,
            max_event_payload_bytes,
            max_checkpoint_age,
        }
    }

    pub(super) fn checkpoint_refresh_needed(
        self,
        input: CheckpointFreshnessInput,
    ) -> CheckpointFreshnessDecision {
        let version_lag = input.head_version.saturating_sub(input.checkpoint_version);
        let volume_changed = version_lag > 0;
        let checkpoint_age = volume_changed.then_some(input.checkpoint_age);

        let reason = if !volume_changed {
            None
        } else if version_lag >= self.max_version_lag {
            Some(MetadataCheckpointRefreshReason::VersionLag)
        } else if input.event_payload_bytes >= self.max_event_payload_bytes {
            Some(MetadataCheckpointRefreshReason::EventPayloadBytes)
        } else if input.checkpoint_age >= self.max_checkpoint_age {
            Some(MetadataCheckpointRefreshReason::Age)
        } else {
            None
        };

        CheckpointFreshnessDecision {
            head_version: input.head_version,
            checkpoint_version: input.checkpoint_version,
            version_lag,
            event_payload_bytes: input.event_payload_bytes,
            reason,
            checkpoint_age_ms: checkpoint_age
                .map(|age| age.as_millis().try_into().unwrap_or(u64::MAX)),
        }
    }
}

impl Default for CheckpointFreshnessPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_MAX_VERSION_LAG,
            DEFAULT_MAX_EVENT_PAYLOAD_BYTES,
            Duration::from_secs(DEFAULT_MAX_CHECKPOINT_AGE_SECS),
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct CheckpointFreshnessInput {
    pub(super) head_version: u64,
    pub(super) checkpoint_version: u64,
    pub(super) event_payload_bytes: u64,
    pub(super) checkpoint_age: Duration,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct CheckpointFreshnessDecision {
    pub(super) head_version: u64,
    pub(super) checkpoint_version: u64,
    pub(super) version_lag: u64,
    pub(super) event_payload_bytes: u64,
    pub(super) reason: Option<MetadataCheckpointRefreshReason>,
    pub(super) checkpoint_age_ms: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct CheckpointFreshnessState {
    diagnostics: MetadataCheckpointFreshnessDiagnostics,
}

impl CheckpointFreshnessState {
    pub(super) fn new(policy: &CheckpointFreshnessPolicy) -> Self {
        let mut state = Self::default();
        state.diagnostics.max_version_lag = policy.max_version_lag;
        state.diagnostics.max_event_payload_bytes = policy.max_event_payload_bytes;
        state.diagnostics.max_checkpoint_age_ms = policy
            .max_checkpoint_age
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        state
    }

    pub(super) fn observe(
        &mut self,
        policy: &CheckpointFreshnessPolicy,
        input: CheckpointFreshnessInput,
    ) {
        let decision = policy.checkpoint_refresh_needed(input);
        let was_needed = self.diagnostics.refresh_needed;
        let refresh_needed = decision.reason.is_some();
        let refresh_requests = self
            .diagnostics
            .refresh_requests
            .saturating_add(u64::from(!was_needed && refresh_needed));
        self.diagnostics = MetadataCheckpointFreshnessDiagnostics {
            head_version: Some(decision.head_version),
            checkpoint_version: Some(decision.checkpoint_version),
            version_lag: decision.version_lag,
            event_payload_bytes_since_checkpoint: decision.event_payload_bytes,
            checkpoint_age_ms: decision.checkpoint_age_ms,
            max_version_lag: policy.max_version_lag,
            max_event_payload_bytes: policy.max_event_payload_bytes,
            max_checkpoint_age_ms: policy
                .max_checkpoint_age
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            refresh_needed,
            refresh_reason: decision.reason,
            refresh_requests,
        };
    }

    pub(super) fn diagnostics(&self) -> MetadataCheckpointFreshnessDiagnostics {
        self.diagnostics.clone()
    }
}

impl From<super::metadata_io::CheckpointFreshnessObservation> for CheckpointFreshnessInput {
    fn from(observation: super::metadata_io::CheckpointFreshnessObservation) -> Self {
        Self {
            head_version: observation.head_version,
            checkpoint_version: observation.checkpoint_version,
            event_payload_bytes: observation.event_payload_bytes,
            checkpoint_age: observation.checkpoint_age,
        }
    }
}

impl super::PgMetaEngine {
    pub(super) fn observe_checkpoint_freshness(&self) {
        if let Err(error) = self.ensure_remote_format_validated() {
            warn!(
                error = %error,
                volume_id = self.volume_id,
                "checkpoint freshness observation could not validate postgres format"
            );
            return;
        }

        let observation = match self.read_remote_checkpoint_freshness() {
            Ok(observation) => observation,
            Err(error) => {
                warn!(
                    error = %error,
                    volume_id = self.volume_id,
                    "checkpoint freshness observation failed"
                );
                return;
            }
        };
        self.record_checkpoint_freshness_observation(observation);
    }

    pub(super) fn record_checkpoint_freshness_observation(
        &self,
        observation: super::metadata_io::CheckpointFreshnessObservation,
    ) {
        if observation.head_version < observation.checkpoint_version {
            warn!(
                volume_id = self.volume_id,
                head_version = observation.head_version,
                checkpoint_version = observation.checkpoint_version,
                "postgres metadata checkpoint version exceeds metadata head"
            );
        }

        let refresh_needed = {
            let mut state = self
                .checkpoint_freshness_state
                .lock()
                .unwrap_or_else(|poisoned| {
                    warn!(
                        volume_id = self.volume_id,
                        "checkpoint freshness state lock was poisoned; recovering state"
                    );
                    poisoned.into_inner()
                });
            state.observe(&self.checkpoint_freshness_policy, observation.into());
            state.diagnostics().refresh_needed
        };
        if refresh_needed {
            self.request_checkpoint_refresh();
        } else {
            self.clear_checkpoint_refresh_request();
        }
    }

    pub(super) fn checkpoint_freshness_diagnostics(
        &self,
    ) -> MetadataCheckpointFreshnessDiagnostics {
        self.checkpoint_freshness_state
            .lock()
            .map(|state| state.diagnostics())
            .unwrap_or_else(|poisoned| {
                warn!(
                    volume_id = self.volume_id,
                    "checkpoint freshness state lock was poisoned; recovering state"
                );
                poisoned.into_inner().diagnostics()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::MetadataCheckpointRefreshReason;
    use std::time::Duration;

    fn input(
        head_version: u64,
        checkpoint_version: u64,
        event_payload_bytes: u64,
        checkpoint_age: Duration,
    ) -> CheckpointFreshnessInput {
        CheckpointFreshnessInput {
            head_version,
            checkpoint_version,
            event_payload_bytes,
            checkpoint_age,
        }
    }

    fn policy() -> CheckpointFreshnessPolicy {
        CheckpointFreshnessPolicy::new(10, 100, Duration::from_secs(60))
    }

    #[test]
    fn version_lag_requests_checkpoint_refresh() {
        let decision = policy().checkpoint_refresh_needed(input(15, 5, 0, Duration::ZERO));

        assert!(decision.reason.is_some());
        assert_eq!(
            decision.reason,
            Some(MetadataCheckpointRefreshReason::VersionLag)
        );
        assert_eq!(decision.version_lag, 10);
    }

    #[test]
    fn event_payload_bytes_request_checkpoint_refresh() {
        let decision = policy().checkpoint_refresh_needed(input(2, 1, 100, Duration::ZERO));

        assert!(decision.reason.is_some());
        assert_eq!(
            decision.reason,
            Some(MetadataCheckpointRefreshReason::EventPayloadBytes)
        );
        assert_eq!(decision.event_payload_bytes, 100);
    }

    #[test]
    fn checkpoint_age_requests_refresh_only_after_volume_change() {
        let stale = policy().checkpoint_refresh_needed(input(2, 1, 0, Duration::from_secs(60)));
        let quiet = policy().checkpoint_refresh_needed(input(1, 1, 0, Duration::from_secs(600)));

        assert!(stale.reason.is_some());
        assert_eq!(stale.reason, Some(MetadataCheckpointRefreshReason::Age));
        assert_eq!(stale.checkpoint_age_ms, Some(60_000));
        assert!(quiet.reason.is_none());
        assert_eq!(quiet.checkpoint_age_ms, None);
    }

    #[test]
    fn freshness_thresholds_are_inclusive_and_ordered_by_version_then_bytes_then_age() {
        let version =
            policy().checkpoint_refresh_needed(input(10, 0, 100, Duration::from_secs(60)));
        let bytes = policy().checkpoint_refresh_needed(input(2, 1, 100, Duration::from_secs(60)));
        let age = policy().checkpoint_refresh_needed(input(2, 1, 0, Duration::from_secs(60)));

        assert_eq!(
            version.reason,
            Some(MetadataCheckpointRefreshReason::VersionLag)
        );
        assert_eq!(
            bytes.reason,
            Some(MetadataCheckpointRefreshReason::EventPayloadBytes)
        );
        assert_eq!(age.reason, Some(MetadataCheckpointRefreshReason::Age));
    }

    #[test]
    fn inverted_versions_fail_closed_without_age_or_refresh_request() {
        let decision =
            policy().checkpoint_refresh_needed(input(4, 5, 100, Duration::from_secs(60)));

        assert_eq!(decision.version_lag, 0);
        assert_eq!(decision.reason, None);
        assert_eq!(decision.checkpoint_age_ms, None);
    }

    #[test]
    fn quiet_up_to_date_volume_does_not_repeat_refresh_requests() {
        let mut tracker = CheckpointFreshnessState::default();

        tracker.observe(&policy(), input(15, 5, 0, Duration::ZERO));
        tracker.observe(&policy(), input(15, 5, 0, Duration::ZERO));
        assert_eq!(tracker.diagnostics().refresh_requests, 1);

        tracker.observe(&policy(), input(5, 5, 0, Duration::from_secs(600)));
        assert!(!tracker.diagnostics().refresh_needed);

        tracker.observe(&policy(), input(16, 5, 0, Duration::ZERO));
        assert_eq!(tracker.diagnostics().refresh_requests, 2);
    }
}
