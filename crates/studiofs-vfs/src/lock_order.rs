//! Lock ordering validation utilities.
//!
//! This module provides runtime validation of lock ordering to prevent deadlocks.
//! The lock ordering is defined in AGENTS.md and must be followed strictly.
//! With validation enabled, thread-local acquisition IDs distinguish guards of
//! the same tier. Out-of-order releases warn and remove only the dropped guard's
//! acquisition, preserving the remaining stack.
//!
//! # Lock Ordering (Tier 1 = highest priority)
//!
//! 1.  `ShutdownState` — shutdown phase and synchronous task ownership
//! 2.  Writer/cache dirty-state owners — mounted writer shards/files and
//!     `local_dirty` bookkeeping (`LockTier::LocalDirty`)
//! 3.  `NamespaceState` — dentry/attr caches
//! 4.  `IncarnationState` — inode incarnation tracking
//! 5.  `UploadState` — upload queue and cancellations
//! 6.  `AclState` — ACL state
//! 7.  `FileLockState` — file locks
//! 8.  `MetricsState` — metrics (rarely needs locking)
//!
//! Mounted writer inode shards, handle shards, and per-file state use tier 2.
//! Within that tier, nested acquisitions follow inode shard -> file. Handle
//! shards are released before acquiring inode shards, and file guards are
//! released before reacquiring a shard or staging detached payloads. Equal
//! tiers are accepted by the validator; the shard/file order is enforced by
//! these acquisition scopes. Ordinary writer and namespace operations release
//! their guards between phases. Synchronous guards must not cross an await;
//! async admission and metadata-sync locks are outside thread-local validation.
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
    /// Tier 1: shutdown phase and synchronous task ownership.
    Shutdown = 1,
    /// Tier 2: writer/cache dirty-state owners, including mounted writer
    /// inode/handle shards, per-file state, and `local_dirty` bookkeeping.
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

// Thread-local tracking of currently held lock acquisitions.
// Uses a simple stack to track nested lock acquisitions.
// All state is thread-local to avoid cross-thread interference.
thread_local! {
    static HELD_LOCKS: RefCell<Vec<(LockTier, u64)>> = RefCell::new(Vec::with_capacity(4));
    static NEXT_LOCK_ID: Cell<u64> = const { Cell::new(0) };
}

/// Hands out the identity for the next lock acquisition.
fn next_lock_id() -> u64 {
    NEXT_LOCK_ID.with(|next| {
        next.set(next.get().wrapping_add(1));
        next.get()
    })
}

/// Guard that tracks lock acquisition and validates ordering.
///
/// With validation enabled, each guard retains its acquisition ID, so release
/// inversions are detected even between guards of the same tier. Dropping a
/// guard removes only its acquisition from the tracking stack.
/// Guards cannot be sent or shared across threads because they track the
/// acquiring thread's stack.
///
/// ```compile_fail
/// use studiofs_vfs::LockOrderGuard;
/// fn require_send<T: Send>() {}
/// require_send::<LockOrderGuard>();
/// ```
///
/// ```compile_fail
/// use studiofs_vfs::LockOrderGuard;
/// fn require_sync<T: Sync>() {}
/// require_sync::<LockOrderGuard>();
/// ```
pub struct LockOrderGuard {
    tier: LockTier,
    // Zero is used when validation is disabled and also occurs at counter wrap.
    // `pushed` determines whether this acquisition is tracked.
    id: u64,
    pushed: bool,
    // Keep `*mut ()`: it makes this guard !Send and !Sync for the thread-local stack.
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

            // Check every held acquisition: unchecked or warned acquisitions
            // can leave the stack's tiers out of order.
            if let Some(&(violating_tier, _)) =
                held.iter().rfind(|(held_tier, _)| tier < *held_tier)
            {
                let msg = format!(
                    "Lock ordering violation: trying to acquire {} (tier {}) while holding {} (tier {})",
                    tier.name(),
                    tier.level(),
                    violating_tier.name(),
                    violating_tier.level()
                );

                if cfg!(test) {
                    panic!("{msg}");
                } else {
                    warn!("{}", msg);
                }
            }

            held.push((tier, id));

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
    /// This acquisition is still tracked and checked by later calls to `acquire`.
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
            held.borrow_mut().push((tier, id));
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

            match held.iter().rposition(|(_, id)| *id == self.id) {
                Some(pos) => {
                    if pos != held.len() - 1
                        && let Some(&(top_tier, top_id)) = held.last()
                    {
                        warn!(
                            dropped_tier = self.tier.name(),
                            dropped_id = self.id,
                            top_tier = top_tier.name(),
                            top_id,
                            stack_depth = held.len(),
                            "Lock guard dropped in wrong order"
                        );
                    }
                    held.remove(pos);
                }
                None => {
                    warn!(
                        dropped_tier = self.tier.name(),
                        dropped_id = self.id,
                        "Lock guard dropped but acquisition was not found in thread-local stack"
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
    HELD_LOCKS.with(|held| held.borrow().iter().map(|(tier, _)| *tier).collect())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek};
    use std::sync::Arc;

    use super::*;

    static_assertions::assert_not_impl_any!(LockOrderGuard: Send, Sync);

    /// Runs body with tracing text routed into a string.
    fn capture_warnings_while(body: impl FnOnce()) -> String {
        let output = Arc::new(tempfile::tempfile().expect("create captured tracing output"));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(output.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, body);
        let mut file = output.as_ref();
        file.rewind().expect("rewind captured tracing output");
        let mut captured = String::new();
        file.read_to_string(&mut captured)
            .expect("read captured tracing output");
        captured
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
    #[should_panic(expected = "while holding Upload (tier 5)")]
    fn test_checked_acquire_rejects_older_tier_after_unchecked_acquire() {
        let _guard1 = LockOrderGuard::acquire(LockTier::Upload);
        let _guard2 = LockOrderGuard::acquire_unchecked(LockTier::Shutdown);
        let _guard3 = LockOrderGuard::acquire(LockTier::Namespace);
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
        let guard1 = LockOrderGuard::acquire(LockTier::Shutdown);
        let guard2 = LockOrderGuard::acquire(LockTier::Namespace);
        let dropped_id = guard1.id;
        let top_id = guard2.id;

        let captured = capture_warnings_while(|| {
            drop(guard1);
            assert_eq!(debug_lock_stack(), vec![LockTier::Namespace]);

            drop(guard2);
            assert!(debug_lock_stack().is_empty());
        });
        for field in [
            "dropped_tier=\"Shutdown\"".to_owned(),
            format!("dropped_id={dropped_id}"),
            "top_tier=\"Namespace\"".to_owned(),
            format!("top_id={top_id}"),
            "stack_depth=2".to_owned(),
        ] {
            assert!(
                captured.split_whitespace().any(|token| token == field),
                "missing {field}: {captured}"
            );
        }
    }

    #[test]
    fn test_same_tier_out_of_order_release_keeps_stack_consistent() {
        let guard1 = LockOrderGuard::acquire(LockTier::Namespace);
        let guard2 = LockOrderGuard::acquire(LockTier::Namespace);
        let dropped_id = guard1.id;
        let top_id = guard2.id;
        assert_ne!(dropped_id, top_id);

        let captured = capture_warnings_while(|| {
            drop(guard1);
            assert_eq!(debug_lock_stack(), vec![LockTier::Namespace]);

            drop(guard2);
            assert!(debug_lock_stack().is_empty());
        });
        assert_eq!(
            captured
                .matches("Lock guard dropped in wrong order")
                .count(),
            1,
            "expected exactly one out-of-order diagnostic, got: {captured}"
        );
        for field in [
            "dropped_tier=\"Namespace\"".to_owned(),
            format!("dropped_id={dropped_id}"),
            "top_tier=\"Namespace\"".to_owned(),
            format!("top_id={top_id}"),
            "stack_depth=2".to_owned(),
        ] {
            assert!(
                captured.split_whitespace().any(|token| token == field),
                "missing {field}: {captured}"
            );
        }
    }

    #[test]
    fn test_missing_acquisition_warns_and_preserves_other_guards() {
        let remaining = LockOrderGuard::acquire(LockTier::Shutdown);
        let missing = LockOrderGuard::acquire(LockTier::Namespace);
        let missing_id = missing.id;
        // Simulate losing only the newer acquisition's stack entry.
        let _ = HELD_LOCKS.with(|held| held.borrow_mut().pop());

        let captured = capture_warnings_while(|| drop(missing));
        assert!(
            captured
                .contains("Lock guard dropped but acquisition was not found in thread-local stack")
        );
        for field in [
            "dropped_tier=\"Namespace\"".to_owned(),
            format!("dropped_id={missing_id}"),
        ] {
            assert!(
                captured.split_whitespace().any(|token| token == field),
                "missing {field}: {captured}"
            );
        }
        assert_eq!(debug_lock_stack(), vec![LockTier::Shutdown]);
        drop(remaining);
        assert!(debug_lock_stack().is_empty());
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
    fn test_unchecked_middle_release_keeps_stack_consistent() {
        let captured = capture_warnings_while(|| {
            let guard1 = LockOrderGuard::acquire(LockTier::Shutdown);
            let guard2 = LockOrderGuard::acquire_unchecked(LockTier::Namespace);
            let guard3 = LockOrderGuard::acquire_unchecked(LockTier::Namespace);

            drop(guard2);
            assert_eq!(
                debug_lock_stack(),
                vec![LockTier::Shutdown, LockTier::Namespace]
            );
            drop(guard3);
            assert_eq!(debug_lock_stack(), vec![LockTier::Shutdown]);
            drop(guard1);
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
    fn test_lock_tracking_is_thread_local() {
        let guard = LockOrderGuard::acquire(LockTier::Namespace);
        let next_id = NEXT_LOCK_ID.with(Cell::get);
        std::thread::spawn(|| {
            assert!(debug_lock_stack().is_empty());
            let guard = LockOrderGuard::acquire(LockTier::Shutdown);
            assert_eq!(guard.id, 1);
            assert_eq!(debug_lock_stack(), vec![LockTier::Shutdown]);
            drop(guard);
            assert!(debug_lock_stack().is_empty());
        })
        .join()
        .expect("lock tracking thread should complete");
        assert_eq!(NEXT_LOCK_ID.with(Cell::get), next_id);
        assert_eq!(debug_lock_stack(), vec![LockTier::Namespace]);
        drop(guard);
        assert!(debug_lock_stack().is_empty());
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
