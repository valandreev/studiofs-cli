//! Cache 3.0 budget governor and eviction thresholds.
//!
//! The governor keeps budget policy separate from backend I/O mechanics so
//! `mod.rs` can focus on applying deterministic eviction passes.

const HIGH_WATERMARK_PERCENT: u64 = 90;
const LOW_WATERMARK_PERCENT: u64 = 80;
const WATERMARK_MAX_GAP_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Number of access-order candidates to read per index page while evicting.
pub(crate) const EVICTION_CANDIDATE_PAGE_SIZE: usize = 512;
/// Number of object ids to delete per batch mutation.
pub(crate) const EVICTION_DELETE_BATCH_SIZE: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CacheBudgetGovernor {
    budget_bytes: Option<u64>,
    high_watermark_bytes: u64,
    low_watermark_bytes: u64,
}

impl CacheBudgetGovernor {
    pub(crate) fn unbounded() -> Self {
        Self {
            budget_bytes: None,
            high_watermark_bytes: 0,
            low_watermark_bytes: 0,
        }
    }

    pub(crate) fn bounded(budget_bytes: u64) -> Self {
        let mut high = budget_bytes.saturating_mul(HIGH_WATERMARK_PERCENT) / 100;
        if budget_bytes > 0 {
            high = high.max(1);
        }
        let mut low = budget_bytes.saturating_mul(LOW_WATERMARK_PERCENT) / 100;
        if high > 0 {
            low = low.min(high.saturating_sub(1));
            low = low.max(high.saturating_sub(WATERMARK_MAX_GAP_BYTES));
        }
        Self {
            budget_bytes: Some(budget_bytes),
            high_watermark_bytes: high,
            low_watermark_bytes: low,
        }
    }

    pub(crate) fn budget_bytes(self) -> Option<u64> {
        self.budget_bytes
    }

    /// Target current-bytes value before admitting a new payload write.
    pub(crate) fn target_before_insert(
        self,
        current_bytes: u64,
        incoming_bytes: u64,
    ) -> Option<u64> {
        let budget = self.budget_bytes?;
        if current_bytes.saturating_add(incoming_bytes) <= budget {
            return None;
        }
        Some(budget.saturating_sub(incoming_bytes))
    }

    /// Target current-bytes value for background pressure passes.
    pub(crate) fn background_target(self, current_bytes: u64) -> Option<u64> {
        self.budget_bytes?;
        if current_bytes > self.high_watermark_bytes {
            Some(self.low_watermark_bytes)
        } else {
            None
        }
    }

    pub(crate) fn exceeds_budget_after_insert(
        self,
        current_bytes: u64,
        incoming_bytes: u64,
    ) -> bool {
        let Some(budget) = self.budget_bytes else {
            return false;
        };
        current_bytes.saturating_add(incoming_bytes) > budget
    }

    pub(crate) fn free_bytes(self, current_bytes: u64) -> u64 {
        match self.budget_bytes {
            Some(budget) => budget.saturating_sub(current_bytes),
            None => u64::MAX,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CacheBudgetGovernor, WATERMARK_MAX_GAP_BYTES};

    #[test]
    fn bounded_governor_uses_expected_watermarks() {
        let governor = CacheBudgetGovernor::bounded(100);
        assert_eq!(governor.high_watermark_bytes, 90);
        assert_eq!(governor.low_watermark_bytes, 80);
    }

    #[test]
    fn bounded_governor_handles_tiny_budget() {
        let governor = CacheBudgetGovernor::bounded(1);
        assert_eq!(governor.high_watermark_bytes, 1);
        assert_eq!(governor.low_watermark_bytes, 0);
    }

    #[test]
    fn target_before_insert_triggers_only_on_hard_pressure() {
        let governor = CacheBudgetGovernor::bounded(100);
        assert_eq!(governor.target_before_insert(70, 20), None);
        assert_eq!(governor.target_before_insert(90, 20), Some(80));
    }

    #[test]
    fn unbounded_governor_never_requests_pressure_targets() {
        let governor = CacheBudgetGovernor::unbounded();
        assert_eq!(governor.target_before_insert(1_000, 1_000), None);
        assert_eq!(governor.background_target(1_000), None);
        assert!(!governor.exceeds_budget_after_insert(1_000, 1_000));
    }

    #[test]
    fn bounded_governor_caps_reclaim_window_for_large_cache_budgets() {
        let tebibyte = 1024_u64 * 1024 * 1024 * 1024;
        let governor = CacheBudgetGovernor::bounded(tebibyte);
        let gap = governor
            .high_watermark_bytes
            .saturating_sub(governor.low_watermark_bytes);
        assert_eq!(gap, WATERMARK_MAX_GAP_BYTES);
    }
}
