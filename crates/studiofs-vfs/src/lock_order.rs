//! Lock ordering validation utilities.
//!
//! This module provides runtime validation of lock ordering to prevent deadlocks.
//! The lock ordering is defined in AGENTS.md and must be followed strictly.
//!
//! # Lock Ordering (Tier 1 = highest priority)
//!
//! 1.  `ShutdownState` — shutdown flags
//! 2.  `local_dirty` subsystem — pending local dirty-state bookkeeping
//! 3.  `NamespaceState` — dentry/attr caches
//! 4.  `IncarnationState` — inode incarnation tracking
//! 5.  `UploadState` — upload queue and cancellations
//! 6.  `AclState` — ACL state
//! 7.  `FileLockState` — file locks
//! 8.  `MetricsState` — metrics (rarely needs locking)
//!
//! When nesting the sustained-write tracker with `HandleTable`, acquire the
//! tracker mutex before reading or mutating the handle table. Writer opens use
//! the tracker around the table's zero-to-one writer transition. Do not acquire
//! the tracker from a `HandleTable::close_with` callback, which runs under the
//! table lock.
//! Release uses the remaining-write count returned by `close_with` to skip the
//! tracker while another writer remains, then rechecks under the tracker after
//! a zero count because a writer may reopen. That reopen clears the old streak
//! as it registers the first writer. These auxiliary locks are not registered
//! with `LockTier` or validated by `LockOrderGuard`; their ordering is enforced
//! by the call structure above.
//!
//! With `LocalPayloadBackendRuntime`, cached-store first/final pin transitions
//! acquire locks in this order:
//! `CachedStore::pinned` -> local backend payload mutation stripe gates ->
//! backend `runtime_pinned`. `pin`, `unpin`, and the first pins in
//! `submit_uploads_batch` hold the store-wide pin mutex through the backend
//! transition so another owner cannot overtake its first/final reference.
//! Backend code holding a payload gate or `runtime_pinned` must not re-enter
//! cached-store pin accounting. Batch gates are deduplicated and acquired in
//! ascending stripe order. These auxiliary locks are outside `LockTier` and
//! `LockOrderGuard`; the ordering is enforced by the call structure.
//!
//! Writer upload byte accounting acquires
//! `CachedStore::pending_upload_bytes_by_object` before staged queue submission,
//! rebased queue replacement or terminal queue removal, then the queue's
//! `pending_state`. A successful rebase refreshes an existing staged contribution
//! before releasing accounting. Staged submission
//! acquires its provisional pin before accounting and compensates it after
//! accounting is released, so backend pin/unpin waits do not retain accounting.
//! Completion/cancellation acquire `pending_state` under the accounting lock,
//! reconcile the accepted owner's bytes, and release accounting before backend
//! reads or unpinning. Queue/backend code must not re-enter byte accounting.
//! These locks are also outside `LockTier` and `LockOrderGuard`.
//!
//! Payload writes/removals hold the same stripe gates across I/O. A colliding
//! gate can therefore stall unrelated pin transitions behind the store-wide
//! mutex; a batch may wait for all 64 stripes. Per-object transition ordering
//! is a possible performance change if contention is measured, and must retain
//! the same logical/backend first/final ordering and stable batch lock order.
//!
//! # Usage
//!
//! ```rust,ignore
//! // Before acquiring a lock, register the lock tier:
//! let _guard = LockOrderGuard::acquire(LockTier::Namespace);
//! // Now safe to acquire namespace lock
//! let ns = self.state.namespace.lock();
//! ```

use std::cell::{Cell, RefCell};
use std::marker::PhantomData;

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
    /// Tier 3: NamespaceState
    Namespace = 3,
    /// Tier 4: IncarnationState
    Incarnation = 4,
    /// Tier 5: UploadState
    Upload = 5,
    /// Tier 6: AclState
    Acl = 6,
    /// Tier 7: FileLockState
    FileLock = 7,
    /// Tier 8: MetricsState
    Metrics = 8,
}

impl LockTier {
    /// Returns this tier's numeric priority level.
    ///
    /// Lower values indicate higher lock priority and must be acquired before higher values.
    pub const fn level(self) -> u8 {
        match self {
            LockTier::Shutdown => 1,
            LockTier::LocalDirty => 2,
            LockTier::Namespace => 3,
            LockTier::Incarnation => 4,
            LockTier::Upload => 5,
            LockTier::Acl => 6,
            LockTier::FileLock => 7,
            LockTier::Metrics => 8,
        }
    }

    /// Returns the canonical, human-readable name for the lock tier.
    ///
    /// The returned string matches the tier vocabulary used in diagnostics and lock-order warnings.
    pub fn name(&self) -> &'static str {
        match self {
            LockTier::Shutdown => "Shutdown",
            LockTier::LocalDirty => "LocalDirty",
            LockTier::Namespace => "Namespace",
            LockTier::Incarnation => "Incarnation",
            LockTier::Upload => "Upload",
            LockTier::Acl => "Acl",
            LockTier::FileLock => "FileLock",
            LockTier::Metrics => "Metrics",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HeldLock {
    tier: LockTier,
    id: u64,
}

// Thread-local tracking of currently held lock acquisitions.
// Uses a simple stack to track nested lock acquisitions.
// All state is thread-local to avoid cross-thread interference.
thread_local! {
    static HELD_LOCKS: RefCell<Vec<HeldLock>> = RefCell::new(Vec::with_capacity(4));
    static NEXT_LOCK_ID: Cell<u64> = const { Cell::new(0) };
}

/// Hands out the identity for the next lock acquisition.
fn next_lock_id() -> u64 {
    NEXT_LOCK_ID.with(|next| {
        let id = next.get().wrapping_add(1);
        next.set(id);
        id
    })
}

/// Guard that tracks lock acquisition and validates ordering.
///
/// When dropped, the lock is released from the tracking stack.
pub struct LockOrderGuard {
    tier: LockTier,
    id: u64,
    pushed: bool,
    _marker: PhantomData<*mut ()>,
}

impl LockOrderGuard {
    /// Acquire a lock guard for the given tier.
    ///
    /// Validates that this lock doesn't violate the lock ordering.
    /// Returns a guard that must be held while the lock is held.
    ///
    pub fn acquire(tier: LockTier) -> Self {
        if !is_validation_enabled() {
            return LockOrderGuard {
                tier,
                id: 0,
                pushed: false,
                _marker: PhantomData,
            };
        }

        HELD_LOCKS.with(|held| {
            let mut held = held.borrow_mut();
            let id = next_lock_id();

            // Check lock ordering: new tier must be >= last acquired tier
            // (i.e., same or lower priority than what we already hold).
            // We compare with last() because that's the most recently acquired lock.
            // Example: if we hold [Tier1, Tier5], last() is Tier5.
            // Trying to acquire Tier3 would violate ordering (3 < 5).
            if let Some(last_lock) = held.last()
                && tier < last_lock.tier
            {
                let msg = format!(
                    "Lock ordering violation: trying to acquire {} (tier {}) while holding {} (tier {})",
                    tier.name(),
                    tier.level(),
                    last_lock.tier.name(),
                    last_lock.tier.level()
                );

                if cfg!(test) {
                    panic!("{msg}");
                } else {
                    warn!("{}", msg);
                }
            }

            held.push(HeldLock { tier, id });

            LockOrderGuard {
                tier,
                id,
                pushed: true,
                _marker: PhantomData,
            }
        })
    }

    /// Acquire without validation (for dynamic/conditional locking).
    ///
    /// Use this when lock ordering cannot be statically determined.
    /// The caller is responsible for ensuring correct ordering.
    pub fn acquire_unchecked(tier: LockTier) -> Self {
        if !is_validation_enabled() {
            return LockOrderGuard {
                tier,
                id: 0,
                pushed: false,
                _marker: PhantomData,
            };
        }

        HELD_LOCKS.with(|held| {
            let id = next_lock_id();
            held.borrow_mut().push(HeldLock { tier, id });
            LockOrderGuard {
                tier,
                id,
                pushed: true,
                _marker: PhantomData,
            }
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

            match held.iter().position(|lock| lock.id == self.id) {
                Some(pos) => {
                    if pos != held.len() - 1 {
                        warn!(
                            expected = self.tier.name(),
                            "Lock guard dropped in wrong order"
                        );
                    }
                    held.remove(pos);
                }
                None => {
                    warn!(
                        expected = self.tier.name(),
                        "Lock guard dropped but tier was not found in thread-local stack"
                    );
                }
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
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ENABLED.get_or_init(|| {
            std::env::var("STUDIOFS_LOCK_ORDER_VALIDATION")
                .map(|v| v == "1" || v == "true")
                .unwrap_or(false)
        })
    }
}

/// Get the current lock stack for debugging.
pub fn debug_lock_stack() -> Vec<LockTier> {
    HELD_LOCKS.with(|held| held.borrow().iter().map(|lock| lock.tier).collect())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// Shared tracing text sink so tests can observe diagnostics.
    #[derive(Clone, Default)]
    struct CapturedOutput(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedOutput {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("lock captured output")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::writer::MakeWriter<'a> for CapturedOutput {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Runs body with tracing text routed into a string.
    fn capture_warnings_while(body: impl FnOnce()) -> String {
        let output = CapturedOutput::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(output.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, body);
        String::from_utf8(output.0.lock().expect("lock captured output").clone())
            .expect("captured tracing output is UTF-8")
    }

    #[test]
    #[should_panic(expected = "Lock ordering violation")]
    fn test_lock_ordering_violation_panics_in_tests() {
        // Acquire lower priority lock first
        let _guard1 = LockOrderGuard::acquire(LockTier::Namespace);
        // Try to acquire higher priority lock - should panic in tests.
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
    fn test_out_of_order_release_keeps_stack_consistent() {
        let mut guards = vec![
            LockOrderGuard::acquire(LockTier::Shutdown),
            LockOrderGuard::acquire(LockTier::Namespace),
        ];

        drop(guards.remove(0));
        assert_eq!(debug_lock_stack(), vec![LockTier::Namespace]);

        drop(guards);
        assert!(debug_lock_stack().is_empty());
    }

    #[test]
    fn test_same_tier_out_of_order_release_keeps_stack_consistent() {
        let captured = capture_warnings_while(|| {
            let mut guards = vec![
                LockOrderGuard::acquire(LockTier::Namespace),
                LockOrderGuard::acquire(LockTier::Namespace),
            ];

            drop(guards.remove(0));
            assert_eq!(debug_lock_stack(), vec![LockTier::Namespace]);

            drop(guards);
            assert!(debug_lock_stack().is_empty());
        });
        assert_eq!(
            captured
                .matches("Lock guard dropped in wrong order")
                .count(),
            1,
            "expected exactly one out-of-order diagnostic, got: {captured}"
        );
    }

    #[test]
    fn test_same_tier_nested_release_is_silent() {
        let captured = capture_warnings_while(|| {
            let guard1 = LockOrderGuard::acquire(LockTier::Namespace);
            let guard2 = LockOrderGuard::acquire(LockTier::Namespace);

            drop(guard2);
            assert_eq!(debug_lock_stack(), vec![LockTier::Namespace]);

            drop(guard1);
            assert!(debug_lock_stack().is_empty());
        });
        assert!(
            captured.trim().is_empty(),
            "expected no diagnostic, got: {captured}"
        );
    }

    #[test]
    #[should_panic(expected = "Lock ordering violation")]
    fn test_lock_ordering_with_multiple_tiers() {
        // Acquire Tier 1, then Tier 5
        let _guard1 = LockOrderGuard::acquire(LockTier::Shutdown);
        let _guard2 = LockOrderGuard::acquire(LockTier::Namespace);
        // Should NOT be able to acquire LocalDirty while holding Namespace.
        // This would violate ordering since 2 < 3.
        let _guard3 = LockOrderGuard::acquire(LockTier::LocalDirty);
    }

    #[test]
    fn local_dirty_lock_name_matches_tier_vocabulary() {
        assert_eq!(LockTier::LocalDirty.name(), "LocalDirty");
    }
}
