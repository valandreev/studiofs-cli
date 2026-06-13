//! AIMD Controller — Adaptive concurrency limiter (TCP-cwnd inspired).
//!
//! Models the S3 endpoint as a "pipe" with unknown bandwidth.  The concurrency
//! window starts small and **additively increases** on every successful request,
//! then **multiplicatively decreases** on failure — exactly like TCP congestion
//! control.
//!
//! Unlike a fixed `Semaphore`, AIMD adapts to the real-time health of the
//! storage backend: cold-start connections ramp up gradually, and an overloaded
//! endpoint causes automatic back-off.

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::warn;

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Parameters for an [`AimdController`].
#[derive(Debug, Clone)]
pub struct AimdConfig {
    /// Initial concurrency window.
    pub initial_window: f64,
    /// Minimum concurrency window (never drops below this).
    pub min_window: f64,
    /// Maximum concurrency window (hard upper bound).
    pub max_window: f64,
    /// Additive increase per successful request.
    pub increase_step: f64,
    /// Multiplicative factor applied on failure (e.g. 0.5 = halve).
    pub decrease_factor: f64,
    /// After a decrease, ignore subsequent failures for this duration
    /// to avoid over-reacting to a burst of errors from the same event.
    pub decrease_cooldown: Duration,
    /// Reset the window to the initial value after this much idle time.
    ///
    /// `None` disables idle reset. Fetch and upload callers can choose
    /// independent values because their traffic shapes differ.
    pub idle_reset_after: Option<Duration>,
}

impl AimdConfig {
    /// Sensible defaults for **fetch** (download) traffic.
    pub fn fetch_default() -> Self {
        Self {
            initial_window: 2.0,
            min_window: 1.0,
            max_window: 32.0,
            increase_step: 1.0,
            decrease_factor: 0.5,
            decrease_cooldown: Duration::from_secs(5),
            idle_reset_after: Some(Duration::from_secs(60)),
        }
    }

    /// Sensible defaults for **upload** traffic (lower ceiling).
    pub fn upload_default() -> Self {
        Self {
            initial_window: 2.0,
            min_window: 1.0,
            max_window: 8.0,
            increase_step: 0.5,
            decrease_factor: 0.5,
            decrease_cooldown: Duration::from_secs(5),
            idle_reset_after: Some(Duration::from_secs(60)),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Controller
// ─────────────────────────────────────────────────────────────────────────────

/// Internal mutable state protected by a `parking_lot::Mutex`.
struct AimdState {
    /// Current floating-point window.  `floor(window)` is the actual
    /// concurrency limit at any given instant.
    window: f64,
    /// Number of requests currently in flight.
    in_flight: usize,
    /// Configuration snapshot.
    initial_window: f64,
    min_window: f64,
    max_window: f64,
    increase_step: f64,
    decrease_factor: f64,
    /// Timestamp of the last multiplicative decrease.
    last_decrease: Instant,
    decrease_cooldown: Duration,
    /// Timestamp of the last acquire/release feedback path.
    last_activity: Instant,
    idle_reset_after: Option<Duration>,
}

/// Adaptive concurrency controller using the AIMD algorithm.
///
/// # Usage
///
/// ```ignore
/// let permit = controller.acquire().await;
/// match do_work().await {
///     Ok(_)  => permit.success(),
///     Err(_) => permit.failure(),
/// }
/// // if `permit` is dropped without calling success/failure, a neutral
/// // release is performed (no window change).
/// ```
pub struct AimdController {
    state: Mutex<AimdState>,
    /// Woken whenever in-flight drops or the window grows, so that tasks
    /// blocked in `acquire()` can re-evaluate.
    notify: Notify,
    // Observability counters (atomic, lock-free reads).
    pub(crate) successes: AtomicU64,
    pub(crate) failures: AtomicU64,
}

impl AimdController {
    /// Create a new controller from the given configuration.
    pub fn new(cfg: AimdConfig) -> Self {
        let min_window = sanitize_window_min(cfg.min_window);
        let max_window = sanitize_window_max(cfg.max_window, min_window);
        let initial = cfg.initial_window.clamp(min_window, max_window);
        let now = Instant::now();
        Self {
            state: Mutex::new(AimdState {
                window: initial,
                in_flight: 0,
                initial_window: initial,
                min_window,
                max_window,
                increase_step: cfg.increase_step,
                decrease_factor: cfg.decrease_factor,
                // Large cooldown values (e.g. bad env/config) can underflow here.
                // Fall back to `now` instead of panicking during startup.
                last_decrease: now.checked_sub(cfg.decrease_cooldown).unwrap_or(now),
                decrease_cooldown: cfg.decrease_cooldown,
                last_activity: now,
                idle_reset_after: cfg.idle_reset_after,
            }),
            notify: Notify::new(),
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
        }
    }

    /// Acquire a permit.  Blocks (async) until the number of in-flight
    /// requests drops below `floor(window)`.
    pub async fn acquire(&self) -> AimdPermit<'_> {
        loop {
            // Register interest *before* checking state — the standard
            // pattern to avoid lost wake-ups with `tokio::sync::Notify`.
            let notified = self.notify.notified();
            {
                let now = Instant::now();
                let mut st = self.state.lock();
                revalidate_idle_locked(&mut st, now);
                let limit = effective_limit_from_state(&st);
                if st.in_flight < limit {
                    st.in_flight += 1;
                    st.last_activity = now;
                    return AimdPermit {
                        controller: self,
                        resolved: false,
                    };
                }
            }
            notified.await;
        }
    }

    /// Non-blocking attempt to acquire a permit.  Returns `None` if the
    /// window is fully utilised.
    pub fn try_acquire(&self) -> Option<AimdPermit<'_>> {
        let now = Instant::now();
        let mut st = self.state.lock();
        revalidate_idle_locked(&mut st, now);
        let limit = effective_limit_from_state(&st);
        if st.in_flight < limit {
            st.in_flight += 1;
            st.last_activity = now;
            Some(AimdPermit {
                controller: self,
                resolved: false,
            })
        } else {
            None
        }
    }

    /// Return a future that resolves when AIMD state changes and waiters are notified.
    ///
    /// This is a low-level wake signal: it does **not** guarantee that a slot
    /// is now available. Callers must re-check their admission condition after
    /// awaiting it.
    ///
    /// Register the future *before* checking admission conditions to avoid
    /// missed wakeups.
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    /// Record a successful completion and release one in-flight slot.
    fn on_success(&self) {
        self.successes.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let grew;
        {
            let mut st = self.state.lock();
            st.in_flight = st.in_flight.saturating_sub(1);
            st.last_activity = now;
            let old_floor = floor_to_usize(st.window);
            st.window = (st.window + st.increase_step).min(st.max_window);
            let new_floor = floor_to_usize(st.window);
            grew = new_floor > old_floor;
        }
        // If the window grew, multiple waiters may now be able to proceed.
        if grew {
            self.notify.notify_waiters();
        } else {
            self.notify.notify_one();
        }
    }

    /// Record a failed completion and release one in-flight slot.
    fn on_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        {
            let mut st = self.state.lock();
            st.in_flight = st.in_flight.saturating_sub(1);
            st.last_activity = now;
            if now.duration_since(st.last_decrease) >= st.decrease_cooldown {
                let old = st.window;
                st.window = (st.window * st.decrease_factor).max(st.min_window);
                st.last_decrease = now;
                warn!(
                    old_window = format!("{:.1}", old),
                    new_window = format!("{:.1}", st.window),
                    "AIMD: window decreased on failure"
                );
            }
        }
        // Still wake one waiter since we released a slot.
        self.notify.notify_one();
    }

    /// Neutral release — gives back the in-flight slot without adjusting
    /// the window.  Used by the `Drop` impl when the caller forgets to
    /// call `success()` / `failure()`.
    fn release(&self) {
        let now = Instant::now();
        {
            let mut st = self.state.lock();
            st.in_flight = st.in_flight.saturating_sub(1);
            st.last_activity = now;
        }
        self.notify.notify_one();
    }

    // ── Observability ────────────────────────────────────────────────────

    /// Current window size (float), after applying any pending idle reset.
    pub fn window(&self) -> f64 {
        let now = Instant::now();
        let mut st = self.state.lock();
        revalidate_idle_locked(&mut st, now);
        st.window
    }

    /// Current number of in-flight requests.
    pub fn in_flight(&self) -> usize {
        self.state.lock().in_flight
    }

    /// Effective concurrency limit after applying any pending idle reset.
    ///
    /// The returned value is the floor of the current window, clamped to
    /// `min_window`.
    pub fn effective_limit(&self) -> usize {
        let now = Instant::now();
        let mut st = self.state.lock();
        revalidate_idle_locked(&mut st, now);
        effective_limit_from_state(&st)
    }

    /// Total successful completions since creation.
    pub fn total_successes(&self) -> u64 {
        self.successes.load(Ordering::Relaxed)
    }

    /// Total failed completions since creation.
    pub fn total_failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }
}

fn sanitize_window_min(raw_min_window: f64) -> f64 {
    if raw_min_window.is_finite() {
        raw_min_window.max(1.0)
    } else {
        1.0
    }
}

fn sanitize_window_max(raw_max_window: f64, min_window: f64) -> f64 {
    if raw_max_window.is_finite() {
        raw_max_window.max(min_window).max(1.0)
    } else {
        min_window.max(1.0)
    }
}

fn revalidate_idle_locked(st: &mut AimdState, now: Instant) {
    let idle_for = now.saturating_duration_since(st.last_activity);
    revalidate_idle_duration_locked(st, idle_for);
}

fn revalidate_idle_duration_locked(st: &mut AimdState, idle_for: Duration) {
    if st.in_flight != 0 {
        return;
    }
    let Some(idle_reset_after) = st.idle_reset_after else {
        return;
    };
    if idle_for >= idle_reset_after {
        st.window = st.initial_window;
    }
}

fn effective_limit_from_state(st: &AimdState) -> usize {
    let window_floor = st.window.max(1.0).floor();
    if !window_floor.is_finite() {
        return 1;
    }
    if window_floor >= usize::MAX as f64 {
        usize::MAX
    } else {
        let limit_u64 = window_floor as u64;
        usize::try_from(limit_u64).unwrap_or(usize::MAX)
    }
}

fn floor_to_usize(value: f64) -> usize {
    if !value.is_finite() {
        return 0;
    }
    if value <= 0.0 {
        return 0;
    }
    if value >= usize::MAX as f64 {
        usize::MAX
    } else {
        let floored = value.floor();
        if floored >= usize::MAX as f64 {
            usize::MAX
        } else {
            floored as usize
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RAII permit
// ─────────────────────────────────────────────────────────────────────────────

/// An RAII permit obtained from [`AimdController::acquire`].
///
/// **Must** be consumed via [`success()`](Self::success) or
/// [`failure()`](Self::failure).  If dropped without either call the slot
/// is released neutrally (window unchanged).
#[must_use = "permit must be consumed via success() or failure() to maintain correct AIMD state"]
pub struct AimdPermit<'a> {
    controller: &'a AimdController,
    /// Set to `true` once `success()` or `failure()` has been called.
    resolved: bool,
}

impl AimdPermit<'_> {
    /// Signal that the guarded operation completed successfully.
    /// Increases the AIMD window by `increase_step`.
    pub fn success(mut self) {
        self.resolved = true;
        self.controller.on_success();
    }

    /// Signal that the guarded operation failed.
    /// Multiplicatively decreases the AIMD window (subject to cooldown).
    pub fn failure(mut self) {
        self.resolved = true;
        self.controller.on_failure();
    }
}

impl Drop for AimdPermit<'_> {
    fn drop(&mut self) {
        if !self.resolved {
            // Panic path or early return — release slot without window change.
            self.controller.release();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> AimdConfig {
        AimdConfig {
            initial_window: 2.0,
            min_window: 1.0,
            max_window: 8.0,
            increase_step: 1.0,
            decrease_factor: 0.5,
            decrease_cooldown: Duration::from_millis(0), // instant for tests
            idle_reset_after: None,
        }
    }

    #[test]
    fn new_with_huge_cooldown_does_not_panic() {
        let cfg = AimdConfig {
            decrease_cooldown: Duration::MAX,
            ..test_config()
        };
        let c = AimdController::new(cfg);
        assert_eq!(c.effective_limit(), 2);
    }

    #[tokio::test]
    async fn acquire_and_success_grows_window() {
        let c = AimdController::new(test_config());
        assert_eq!(c.effective_limit(), 2);

        let p = c.acquire().await;
        p.success();
        // window: 2 + 1 = 3
        assert_eq!(c.effective_limit(), 3);

        let p = c.acquire().await;
        p.success();
        // window: 3 + 1 = 4
        assert_eq!(c.effective_limit(), 4);
    }

    #[tokio::test]
    async fn failure_shrinks_window() {
        let c = AimdController::new(test_config());
        // Grow to 4 first
        c.acquire().await.success();
        c.acquire().await.success();
        assert_eq!(c.effective_limit(), 4);

        c.acquire().await.failure();
        // window: 4 * 0.5 = 2
        assert_eq!(c.effective_limit(), 2);
    }

    #[tokio::test]
    async fn window_never_below_min() {
        let c = AimdController::new(test_config());
        // Shrink repeatedly
        c.acquire().await.failure(); // 2 * 0.5 = 1
        assert_eq!(c.effective_limit(), 1);

        c.acquire().await.failure(); // 1 * 0.5 = 0.5 → clamped to 1.0
        assert_eq!(c.effective_limit(), 1);
    }

    #[tokio::test]
    async fn window_never_above_max() {
        let c = AimdController::new(test_config());
        for _ in 0..20 {
            c.acquire().await.success();
        }
        assert_eq!(c.effective_limit(), 8);
    }

    #[tokio::test]
    async fn drop_without_resolve_releases_slot() {
        let c = AimdController::new(test_config());
        {
            let _p = c.acquire().await;
            // drop without success/failure
        }
        assert_eq!(c.in_flight(), 0);
        assert_eq!(c.effective_limit(), 2); // window unchanged
    }

    #[tokio::test]
    async fn try_acquire_respects_limit() {
        let c = AimdController::new(test_config());
        // effective_limit = 2
        let p1 = c.try_acquire().unwrap();
        let _p2 = c.try_acquire().unwrap();
        assert!(c.try_acquire().is_none()); // full
        p1.success();
        assert!(c.try_acquire().is_some()); // slot freed + window grew
    }

    #[tokio::test]
    async fn decrease_cooldown_prevents_rapid_shrink() {
        let cfg = AimdConfig {
            decrease_cooldown: Duration::from_secs(60), // long cooldown
            ..test_config()
        };
        let c = AimdController::new(cfg);
        // Grow first
        c.acquire().await.success();
        c.acquire().await.success();
        assert_eq!(c.effective_limit(), 4);

        // First failure triggers decrease
        c.acquire().await.failure(); // 4 * 0.5 = 2
        assert_eq!(c.effective_limit(), 2);

        // Second failure within cooldown — no further decrease
        c.acquire().await.failure();
        assert_eq!(c.effective_limit(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn revalidate_after_idle_resets_stale_high_window_to_initial() {
        let cfg = AimdConfig {
            idle_reset_after: Some(Duration::from_secs(30)),
            ..test_config()
        };
        let c = AimdController::new(cfg);

        for _ in 0..4 {
            c.acquire().await.success();
        }
        assert_eq!(c.effective_limit(), 6);

        tokio::time::advance(Duration::from_secs(31)).await;

        assert_eq!(c.effective_limit(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn revalidate_after_idle_resets_stale_low_window_to_initial() {
        let cfg = AimdConfig {
            idle_reset_after: Some(Duration::from_secs(30)),
            ..test_config()
        };
        let c = AimdController::new(cfg);

        c.acquire().await.failure();
        assert_eq!(c.effective_limit(), 1);

        tokio::time::advance(Duration::from_secs(31)).await;

        assert_eq!(c.effective_limit(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn revalidate_before_idle_threshold_preserves_current_window() {
        let cfg = AimdConfig {
            idle_reset_after: Some(Duration::from_secs(30)),
            ..test_config()
        };
        let c = AimdController::new(cfg);

        c.acquire().await.failure();
        assert_eq!(c.effective_limit(), 1);

        tokio::time::advance(Duration::from_secs(29)).await;

        assert_eq!(c.effective_limit(), 1);
    }

    #[tokio::test]
    async fn concurrent_acquire_blocks_at_limit() {
        let c = AimdController::new(test_config());
        // Hold 2 permits (the limit)
        let p1 = c.try_acquire().unwrap();
        let p2 = c.try_acquire().unwrap();

        // Without releasing, a third acquire should be rejected by the fast path.
        assert!(
            c.try_acquire().is_none(),
            "acquire should be blocked at limit"
        );

        // Release one — now acquire should succeed
        drop(p1);
        let p3 = c
            .try_acquire()
            .expect("acquire should succeed after release");
        p3.success();

        p2.success();
    }
}
