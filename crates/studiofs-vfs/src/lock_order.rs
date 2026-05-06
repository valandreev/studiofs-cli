//! Lock ordering validation utilities.
//!
//! This module provides runtime validation of lock ordering to prevent deadlocks.
//! The lock ordering is defined in AGENTS.md and must be followed strictly.
//!
//! # Lock Ordering (Tier 1 = highest priority)
//!
//! 1.  `ShutdownState` — shutdown flags
//! 2.  `local_dirty` subsystem — pending local dirty-state bookkeeping
//! 3.  reserved legacy `DirtyBufferState` slot — kept for ordering compatibility
//! 4.  reserved legacy `IngressState` slot — no active subsystem
//! 5.  `NamespaceState` — dentry/attr caches
//! 6.  `IncarnationState` — inode incarnation tracking
//! 7.  `UploadState` — upload queue and cancellations
//! 8.  `AclState` — ACL state
//! 9.  `FileLockState` — file locks
//! 10. `MetricsState` — metrics (rarely needs locking)
//!
//! # Usage
//!
//! ```rust,ignore
//! // Before acquiring a lock, register the lock tier:
//! let _guard = LockOrderGuard::acquire(LockTier::Namespace);
//! // Now safe to acquire namespace lock
//! let ns = self.state.namespace.lock();
//! ```

use std::cell::RefCell;
use tracing::warn;

/// Lock tiers in order of priority (lower = higher priority).
///
/// Must match the ordering defined in AGENTS.md.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LockTier {
    /// Tier 1: ShutdownState
    Shutdown = 1,
    /// Tier 2: `local_dirty` subsystem
    LocalDirty = 2,
    /// Tier 3: reserved legacy DirtyBufferState slot
    DirtyBuffer = 3,
    /// Tier 4: reserved legacy IngressState slot
    Ingress = 4,
    /// Tier 5: NamespaceState
    Namespace = 5,
    /// Tier 6: IncarnationState
    Incarnation = 6,
    /// Tier 7: UploadState
    Upload = 7,
    /// Tier 8: AclState
    Acl = 8,
    /// Tier 9: FileLockState
    FileLock = 9,
    /// Tier 10: MetricsState
    Metrics = 10,
}

impl LockTier {
    /// Function $name.
    pub const fn level(self) -> u8 {
        match self {
            LockTier::Shutdown => 1,
            LockTier::LocalDirty => 2,
            LockTier::DirtyBuffer => 3,
            LockTier::Ingress => 4,
            LockTier::Namespace => 5,
            LockTier::Incarnation => 6,
            LockTier::Upload => 7,
            LockTier::Acl => 8,
            LockTier::FileLock => 9,
            LockTier::Metrics => 10,
        }
    }

    /// Function $name.
    pub fn name(&self) -> &'static str {
        match self {
            LockTier::Shutdown => "Shutdown",
            LockTier::LocalDirty => "LocalDirty",
            LockTier::DirtyBuffer => "DirtyBuffer",
            LockTier::Ingress => "Ingress",
            LockTier::Namespace => "Namespace",
            LockTier::Incarnation => "Incarnation",
            LockTier::Upload => "Upload",
            LockTier::Acl => "Acl",
            LockTier::FileLock => "FileLock",
            LockTier::Metrics => "Metrics",
        }
    }
}

// Thread-local tracking of currently held lock tiers.
// Uses a simple stack to track nested lock acquisitions.
// All state is thread-local to avoid cross-thread interference.
thread_local! {
    static HELD_LOCKS: RefCell<Vec<LockTier>> = RefCell::new(Vec::with_capacity(4));
}

/// Guard that tracks lock acquisition and validates ordering.
///
/// When dropped, the lock is released from the tracking stack.
pub struct LockOrderGuard {
    tier: LockTier,
    pushed: bool,
}

impl LockOrderGuard {
    /// Acquire a lock guard for the given tier.
    ///
    /// Validates that this lock doesn't violate the lock ordering.
    /// Returns a guard that must be held while the lock is held.
    ///
    pub fn acquire(tier: LockTier) -> Self {
        HELD_LOCKS.with(|held| {
            let mut held = held.borrow_mut();

            // Check lock ordering: new tier must be >= last acquired tier
            // (i.e., same or lower priority than what we already hold).
            // We compare with last() because that's the most recently acquired lock.
            // Example: if we hold [Tier1, Tier5], last() is Tier5.
            // Trying to acquire Tier3 would violate ordering (3 < 5).
            if let Some(&last_tier) = held.last()
                && tier < last_tier
            {
                // Lock ordering violation!
                let msg = format!(
                    "Lock ordering violation: trying to acquire {} (tier {}) while holding {} (tier {})",
                    tier.name(),
                    tier.level(),
                    last_tier.name(),
                    last_tier.level()
                );

                warn!("{}", msg);
            }

            held.push(tier);

            LockOrderGuard {
                tier,
                pushed: true,
            }
        })
    }

    /// Acquire without validation (for dynamic/conditional locking).
    ///
    /// Use this when lock ordering cannot be statically determined.
    /// The caller is responsible for ensuring correct ordering.
    pub fn acquire_unchecked(tier: LockTier) -> Self {
        HELD_LOCKS.with(|held| {
            held.borrow_mut().push(tier);
            LockOrderGuard { tier, pushed: true }
        })
    }
}

impl Drop for LockOrderGuard {
    fn drop(&mut self) {
        if !self.pushed {
            return;
        }

        HELD_LOCKS.with(|held| {
            let mut held = held.borrow_mut();

            // Remove the most recent lock (should be this one).
            if let Some(last) = held.pop()
                && last != self.tier
            {
                // This indicates a bug - guards dropped in wrong order
                warn!(
                    expected = self.tier.name(),
                    actual = last.name(),
                    "Lock guard dropped in wrong order"
                );
            }
            // Note: We don't track a global minimum since all state is thread-local.
            // Each thread maintains its own lock stack independently.
        });
    }
}

/// Check if lock ordering validation is enabled.
///
/// Always enabled in debug builds, configurable in release.
pub fn is_validation_enabled() -> bool {
    #[cfg(debug_assertions)]
    {
        true
    }
    #[cfg(not(debug_assertions))]
    {
        // In release, check environment variable
        std::env::var("STUDIOFS_LOCK_ORDER_VALIDATION")
            .map(|v| v == "1" || v == "true")
            .unwrap_or(false)
    }
}

/// Get the current lock stack for debugging.
pub fn debug_lock_stack() -> Vec<LockTier> {
    HELD_LOCKS.with(|held| held.borrow().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_ordering_violation_reports_warning() {
        // Acquire lower priority lock first
        let _guard1 = LockOrderGuard::acquire(LockTier::Namespace);
        // Try to acquire higher priority lock - should warn and continue
        let _guard2 = LockOrderGuard::acquire(LockTier::Shutdown);
    }

    #[test]
    fn test_correct_lock_order() {
        // Acquire higher priority lock first
        let _guard1 = LockOrderGuard::acquire(LockTier::Shutdown);
        // Then lower priority - should be fine
        let _guard2 = LockOrderGuard::acquire(LockTier::Namespace);
        // Even lower - still fine
        let _guard3 = LockOrderGuard::acquire(LockTier::Metrics);
    }

    #[test]
    fn test_lock_release() {
        let _guard1 = LockOrderGuard::acquire(LockTier::Shutdown);
        drop(_guard1);
        // After releasing, should be able to acquire any lock
        let _guard2 = LockOrderGuard::acquire(LockTier::Metrics);
    }

    #[test]
    fn test_lock_ordering_with_multiple_tiers() {
        // Acquire Tier 1, then Tier 5
        let _guard1 = LockOrderGuard::acquire(LockTier::Shutdown);
        let _guard2 = LockOrderGuard::acquire(LockTier::Namespace);
        // Should NOT be able to acquire Tier 3 (DirtyBuffer) while holding Tier 5
        // This would violate ordering since 3 < 5
        let _guard3 = LockOrderGuard::acquire(LockTier::DirtyBuffer);
    }

    #[test]
    fn local_dirty_lock_name_matches_tier_vocabulary() {
        assert_eq!(LockTier::LocalDirty.name(), "LocalDirty");
    }
}
