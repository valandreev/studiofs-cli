//! Lock-order-aware mutex wrapper for VFS subsystem state.

use std::ops::{Deref, DerefMut};

use parking_lot::{Mutex, MutexGuard};

use crate::lock_order::{LockOrderGuard, LockOrderSubtier, LockTier};

/// A `parking_lot::Mutex` wrapper that records a lock-order tier while locked.
pub struct OrderedMutex<T> {
    tier: LockTier,
    subtier: Option<LockOrderSubtier>,
    inner: Mutex<T>,
}

impl<T> OrderedMutex<T> {
    /// Creates a mutex associated with the provided lock tier.
    pub fn new(value: T, tier: LockTier) -> Self {
        Self {
            tier,
            subtier: None,
            inner: Mutex::new(value),
        }
    }

    /// Creates a mutex with a fine-grained order within its lock tier.
    pub(crate) fn new_with_subtier(value: T, tier: LockTier, subtier: LockOrderSubtier) -> Self {
        Self {
            tier,
            subtier: Some(subtier),
            inner: Mutex::new(value),
        }
    }

    /// Locks the mutex and keeps the lock-order guard alive for the same scope.
    pub fn lock(&self) -> OrderedMutexGuard<'_, T> {
        let lock_order = LockOrderGuard::acquire_with_subtier(self.tier, self.subtier);
        let guard = self.inner.lock();
        OrderedMutexGuard { guard, lock_order }
    }
}

/// Guard returned by [`OrderedMutex::lock`].
pub struct OrderedMutexGuard<'a, T> {
    // Field order is intentional: Rust drops fields top-to-bottom, so the
    // actual mutex guard must be released before the validation tier is popped.
    guard: MutexGuard<'a, T>,
    #[allow(dead_code)]
    lock_order: LockOrderGuard,
}

impl<T> Deref for OrderedMutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<T> DerefMut for OrderedMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}
