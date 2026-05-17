use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::vfs::VfsConfig;

#[derive(Debug)]
/// Controls concurrent request budgets for the four main VFS pipelines.
///
/// `QosController` owns semaphores that limit in-flight operations across:
/// acceptance, materialization, commit, and fetch stages.
pub struct QosController {
    accept_slots: Arc<Semaphore>,
    materialize_slots: Arc<Semaphore>,
    commit_slots: Arc<Semaphore>,
    fetch_slots: Arc<Semaphore>,
}

impl QosController {
    /// Builds controller limits from VFS configuration.
    ///
    /// Each slot count is clamped to at least one permit to keep the semaphores
    /// functional even when the config sets zero.
    pub fn from_config(config: &VfsConfig) -> Self {
        Self {
            accept_slots: Arc::new(Semaphore::new(config.qos_accept_slots.max(1))),
            materialize_slots: Arc::new(Semaphore::new(config.qos_materialize_slots.max(1))),
            commit_slots: Arc::new(Semaphore::new(config.qos_commit_slots.max(1))),
            fetch_slots: Arc::new(Semaphore::new(config.qos_fetch_slots.max(1))),
        }
    }

    /// Acquires one accept permit, waiting until one becomes available.
    pub async fn acquire_accept(&self) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.accept_slots.clone().acquire_owned().await
    }

    /// Tries to acquire one accept permit without waiting.
    ///
    /// Returns `Some` when acquired, otherwise `None` if no permit is available.
    pub fn try_acquire_accept(&self) -> Option<OwnedSemaphorePermit> {
        self.accept_slots.clone().try_acquire_owned().ok()
    }

    /// Acquires one materialization permit, waiting until one becomes available.
    pub async fn acquire_materialize(
        &self,
    ) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.materialize_slots.clone().acquire_owned().await
    }

    /// Acquires one commit permit, waiting until one becomes available.
    pub async fn acquire_commit(&self) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.commit_slots.clone().acquire_owned().await
    }

    /// Acquires one fetch permit, waiting until one becomes available.
    pub async fn acquire_fetch(&self) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.fetch_slots.clone().acquire_owned().await
    }

    /// Returns currently available accept permits.
    pub fn accept_available(&self) -> usize {
        self.accept_slots.available_permits()
    }

    /// Returns currently available materialization permits.
    pub fn materialize_available(&self) -> usize {
        self.materialize_slots.available_permits()
    }

    /// Returns currently available commit permits.
    pub fn commit_available(&self) -> usize {
        self.commit_slots.available_permits()
    }

    /// Returns currently available fetch permits.
    pub fn fetch_available(&self) -> usize {
        self.fetch_slots.available_permits()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Priority class for transit-memory acquisition.
pub enum TransitPriority {
    /// Foreground work that may preempt waiting background work.
    Foreground,
    /// Background work that yields behind foreground waiters.
    Background,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Snapshot of the current transit-memory budget and queue state.
pub struct TransitMemorySnapshot {
    /// Total budget limit in bytes.
    pub budget_bytes: usize,
    /// Currently used budget in bytes.
    pub used_bytes: usize,
    /// Remaining budget in bytes.
    pub available_bytes: usize,
    /// Number of foreground waiters currently queued.
    pub waiting_foreground: usize,
    /// Number of background waiters currently queued.
    pub waiting_background: usize,
}

#[derive(Debug)]
/// In-memory budget tracker for limiting transient transit-memory usage.
pub struct TransitMemoryBudget {
    budget_bytes: AtomicUsize,
    state: Mutex<TransitMemoryBudgetState>,
    notify: Notify,
}

#[derive(Debug, Default)]
struct TransitMemoryBudgetState {
    used_bytes: usize,
    waiting_foreground: usize,
    waiting_background: usize,
}

#[derive(Debug)]
#[must_use = "guard must be held to maintain transit memory budget reservation"]
/// RAII guard for a reserved transit-memory budget chunk.
pub struct TransitMemoryGuard {
    budget: Arc<TransitMemoryBudget>,
    bytes: usize,
}

impl TransitMemoryBudget {
    /// Creates a budget with the provided byte limit.
    ///
    /// The limit is clamped to at least one byte.
    pub fn new(budget_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            budget_bytes: AtomicUsize::new(budget_bytes.max(1)),
            state: Mutex::new(TransitMemoryBudgetState::default()),
            notify: Notify::new(),
        })
    }

    /// Returns the current total budget in bytes.
    pub fn budget_bytes(&self) -> usize {
        self.budget_bytes.load(Ordering::Acquire)
    }

    /// Update the transit memory budget at runtime.
    ///
    /// Wakes any blocked allocators so they can re-evaluate against the new
    /// limit.  A shrink while transit is fully used will not abort in-flight
    /// guards — it simply delays new acquires until existing guards are
    /// dropped.
    pub fn set_budget_bytes(&self, value: usize) {
        self.budget_bytes.store(value.max(1), Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Takes a snapshot of current budget usage and waiter queue lengths.
    pub fn snapshot(&self) -> TransitMemorySnapshot {
        let budget = self.budget_bytes();
        let state = self.state.lock();
        TransitMemorySnapshot {
            budget_bytes: budget,
            used_bytes: state.used_bytes,
            available_bytes: budget.saturating_sub(state.used_bytes),
            waiting_foreground: state.waiting_foreground,
            waiting_background: state.waiting_background,
        }
    }

    /// Acquires transit-memory budget for a foreground request.
    ///
    /// This waits until enough budget is available.
    pub async fn acquire_foreground(self: &Arc<Self>, bytes: usize) -> TransitMemoryGuard {
        self.acquire(TransitPriority::Foreground, bytes).await
    }

    /// Acquires transit-memory budget for a background request.
    ///
    /// This waits until enough budget is available.
    pub async fn acquire_background(self: &Arc<Self>, bytes: usize) -> TransitMemoryGuard {
        self.acquire(TransitPriority::Background, bytes).await
    }

    /// Acquires transit-memory budget for a background request with cancellation.
    ///
    /// Returns `None` if `cancel_token` is triggered before allocation completes.
    pub async fn acquire_background_cancelable(
        self: &Arc<Self>,
        bytes: usize,
        cancel_token: &CancellationToken,
    ) -> Option<TransitMemoryGuard> {
        let acquire = self.acquire(TransitPriority::Background, bytes);
        tokio::pin!(acquire);
        tokio::select! {
            guard = &mut acquire => Some(guard),
            _ = cancel_token.cancelled() => None,
        }
    }

    async fn acquire(
        self: &Arc<Self>,
        priority: TransitPriority,
        requested_bytes: usize,
    ) -> TransitMemoryGuard {
        let budget = self.budget_bytes();
        let mut requested_bytes = requested_bytes.max(1).min(budget);
        let mut waiter = TransitWaiter::new(self, priority);

        loop {
            let notified = self.notify.notified();
            {
                let current_budget = self.budget_bytes();
                // Re-clamp after a potential budget shrink so we don't block
                // forever waiting for a grant larger than the current budget.
                requested_bytes = requested_bytes.min(current_budget);
                let mut state = self.state.lock();
                if can_allocate(&state, current_budget, requested_bytes, priority) {
                    waiter.disarm(&mut state);
                    state.used_bytes = state.used_bytes.saturating_add(requested_bytes);
                    return TransitMemoryGuard {
                        budget: self.clone(),
                        bytes: requested_bytes,
                    };
                }
                waiter.ensure_registered(&mut state);
            }
            notified.await;
        }
    }

    fn release(&self, bytes: usize) {
        {
            let mut state = self.state.lock();
            state.used_bytes = state.used_bytes.saturating_sub(bytes);
        }
        self.notify.notify_waiters();
    }
}

impl Drop for TransitMemoryGuard {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

fn can_allocate(
    state: &TransitMemoryBudgetState,
    budget_bytes: usize,
    requested_bytes: usize,
    priority: TransitPriority,
) -> bool {
    if state.used_bytes.saturating_add(requested_bytes) > budget_bytes {
        return false;
    }
    match priority {
        TransitPriority::Foreground => true,
        TransitPriority::Background => state.waiting_foreground == 0,
    }
}

struct TransitWaiter<'a> {
    budget: &'a Arc<TransitMemoryBudget>,
    priority: TransitPriority,
    registered: bool,
}

impl<'a> TransitWaiter<'a> {
    fn new(budget: &'a Arc<TransitMemoryBudget>, priority: TransitPriority) -> Self {
        Self {
            budget,
            priority,
            registered: false,
        }
    }

    fn ensure_registered(&mut self, state: &mut TransitMemoryBudgetState) {
        if self.registered {
            return;
        }
        self.registered = true;
        match self.priority {
            TransitPriority::Foreground => {
                state.waiting_foreground = state.waiting_foreground.saturating_add(1);
            }
            TransitPriority::Background => {
                state.waiting_background = state.waiting_background.saturating_add(1);
            }
        }
    }

    fn disarm(&mut self, state: &mut TransitMemoryBudgetState) {
        if !self.registered {
            return;
        }
        self.registered = false;
        match self.priority {
            TransitPriority::Foreground => {
                state.waiting_foreground = state.waiting_foreground.saturating_sub(1);
            }
            TransitPriority::Background => {
                state.waiting_background = state.waiting_background.saturating_sub(1);
            }
        }
    }
}

impl Drop for TransitWaiter<'_> {
    fn drop(&mut self) {
        if !self.registered {
            return;
        }
        let mut state = self.budget.state.lock();
        self.disarm(&mut state);
        drop(state);
        // Wake background waiters that may have been blocked only by
        // `waiting_foreground > 0` before this cancellation/drop.
        self.budget.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::TransitMemoryBudget;
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn transit_memory_budget_prioritizes_foreground_waiters() {
        let budget = TransitMemoryBudget::new(2);
        let bg_holder = budget.acquire_background(2).await;

        let bg_budget = budget.clone();
        let (bg_tx, bg_rx) = oneshot::channel::<()>();
        let bg_waiter = tokio::spawn(async move {
            let _guard = bg_budget.acquire_background(2).await;
            let _ = bg_tx.send(());
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let fg_budget = budget.clone();
        let (fg_tx, fg_rx) = oneshot::channel::<()>();
        let fg_waiter = tokio::spawn(async move {
            let _guard = fg_budget.acquire_foreground(1).await;
            let _ = fg_tx.send(());
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(bg_holder);

        timeout(Duration::from_millis(250), fg_rx)
            .await
            .expect("foreground waiter should complete first")
            .expect("foreground waiter should notify");
        assert!(timeout(Duration::from_millis(50), bg_rx).await.is_err());

        fg_waiter.await.unwrap();
        timeout(Duration::from_millis(250), bg_waiter)
            .await
            .expect("background waiter should complete after foreground releases")
            .expect("background waiter join should succeed");
    }

    #[tokio::test]
    async fn transit_waiter_drop_notifies_background_waiters() {
        let budget = TransitMemoryBudget::new(2);
        let _holder = budget.acquire_background(1).await;
        let fg_budget = budget.clone();
        let fg_waiter = tokio::spawn(async move {
            let _guard = fg_budget.acquire_foreground(2).await;
        });

        let register_deadline = tokio::time::Instant::now() + Duration::from_millis(250);
        loop {
            let snap = budget.snapshot();
            if snap.waiting_foreground > 0 {
                break;
            }
            if tokio::time::Instant::now() >= register_deadline {
                panic!("foreground waiter did not register in time");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let bg_budget = budget.clone();
        let (bg_tx, bg_rx) = oneshot::channel::<()>();
        let bg_waiter = tokio::spawn(async move {
            let _guard = bg_budget.acquire_background(1).await;
            let _ = bg_tx.send(());
        });
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(timeout(Duration::from_millis(40), bg_rx).await.is_err());

        fg_waiter.abort();
        timeout(Duration::from_millis(250), bg_waiter)
            .await
            .unwrap()
            .unwrap();
    }
}
