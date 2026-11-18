//! Per-direction token-bucket bandwidth throttle for payload object transfers.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use std::collections::VecDeque;
use std::sync::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;

/// Upload/download bandwidth limiter shared by all payload transfers of a mount.
#[derive(Default)]
pub struct PayloadThrottle {
    upload: Direction,
    download: Direction,
    cancelled: AtomicBool,
    cancellation_changed: Notify,
}

impl PayloadThrottle {
    /// Create a throttle with the given limits in Mbit/s; `0` means unlimited.
    pub fn new(upload_mbps: u64, download_mbps: u64) -> Self {
        let throttle = Self::default();
        throttle.set_limits(Some(upload_mbps), Some(download_mbps));
        throttle
    }

    /// Update only the provided directions, so partial updates never overwrite each other.
    pub fn set_limits(&self, upload_mbps: Option<u64>, download_mbps: Option<u64>) {
        if let Some(mbps) = upload_mbps {
            self.upload.set_limit(mbps);
        }
        if let Some(mbps) = download_mbps {
            self.download.set_limit(mbps);
        }
    }

    /// Cancel the throttle and wake every waiter; pending and later acquires return `false`.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.cancellation_changed.notify_waiters();
        self.upload.changed.notify_waiters();
        self.download.changed.notify_waiters();
    }

    /// Current `(upload, download)` limits in Mbit/s.
    pub fn limits(&self) -> (u64, u64) {
        (self.upload.limit(), self.download.limit())
    }

    /// Cumulative `(upload, download)` time spent waiting for budget, in nanoseconds.
    pub fn wait_nanos(&self) -> (u64, u64) {
        (self.upload.wait_nanos(), self.download.wait_nanos())
    }

    /// Wait for upload budget for `bytes`; returns `false` if the throttle was cancelled.
    pub async fn acquire_upload(&self, bytes: usize) -> bool {
        self.upload
            .acquire(bytes, false, &self.cancelled, &self.cancellation_changed)
            .await
    }

    /// Wait for download budget for `bytes`, with `foreground` reads served first;
    /// returns `false` if the throttle was cancelled.
    pub async fn acquire_download(&self, bytes: usize, foreground: bool) -> bool {
        self.download
            .acquire(
                bytes,
                foreground,
                &self.cancelled,
                &self.cancellation_changed,
            )
            .await
    }
}

#[derive(Default)]
struct Direction {
    wait_nanos: AtomicU64,
    state: Mutex<Queue>,
    changed: Notify,
}

#[derive(Default)]
struct Queue {
    limit_mbps: u64,
    next_id: u64,
    waiters: VecDeque<(u64, bool)>,
}

impl Direction {
    fn limit(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .limit_mbps
    }

    fn wait_nanos(&self) -> u64 {
        self.wait_nanos.load(Ordering::Relaxed)
    }

    fn set_limit(&self, mbps: u64) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .limit_mbps = mbps;
        self.changed.notify_waiters();
    }

    async fn acquire(
        &self,
        bytes: usize,
        foreground: bool,
        cancelled: &AtomicBool,
        cancellation_changed: &Notify,
    ) -> bool {
        if cancelled.load(Ordering::Acquire) {
            return false;
        }
        // Unlimited mode never waits.
        if bytes == 0 || self.limit() == 0 {
            return true;
        }
        let started = Instant::now();
        let ticket_id = {
            let mut queue = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            queue.next_id = queue.next_id.wrapping_add(1);
            let id = queue.next_id;
            if foreground {
                let insert = queue
                    .waiters
                    .iter()
                    .position(|(_, is_foreground)| !is_foreground);
                let index = insert.unwrap_or(queue.waiters.len());
                queue.waiters.insert(index, (id, true));
            } else {
                queue.waiters.push_back((id, false));
            }
            id
        };
        if foreground {
            // Wake queued waiters after the guard is released so they re-check their position.
            self.changed.notify_waiters();
        }
        let ticket = WaitTicket {
            direction: self,
            id: ticket_id,
        };

        let mut remaining = bytes as f64;
        let mut last_update = Instant::now();
        let mut last_rate = self.limit();
        let mut was_front = false;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let cancelled_signal = cancellation_changed.notified();
            tokio::pin!(cancelled_signal);
            cancelled_signal.as_mut().enable();
            if cancelled.load(Ordering::Acquire) {
                return false;
            }
            let (rate, is_front) = {
                let queue = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (
                    queue.limit_mbps,
                    queue
                        .waiters
                        .front()
                        .is_some_and(|(id, _)| *id == ticket_id),
                )
            };
            if rate == 0 {
                if cancelled.load(Ordering::Acquire) {
                    return false;
                }
                drop(ticket);
                self.record_wait(started);
                return true;
            }
            if !is_front {
                was_front = false;
                tokio::select! {
                    _ = &mut changed => continue,
                    _ = &mut cancelled_signal => return false,
                }
            }
            let now = Instant::now();
            if !was_front {
                last_update = now;
                was_front = true;
                last_rate = rate;
            }
            remaining = (remaining
                - now.duration_since(last_update).as_secs_f64() * last_rate as f64 * 125_000.0)
                .max(0.0);
            // Sub-byte remainders would round to a zero sleep and spin.
            if remaining < 1.0 {
                self.record_wait(started);
                return true;
            }
            last_update = now;
            last_rate = rate;
            let wait = Duration::from_secs_f64(remaining / (rate as f64 * 125_000.0));
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = &mut changed => {}
                _ = &mut cancelled_signal => return false,
            }
        }
    }

    fn record_wait(&self, started: Instant) {
        self.wait_nanos.fetch_add(
            started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}

struct WaitTicket<'a> {
    direction: &'a Direction,
    id: u64,
}

impl WaitTicket<'_> {
    fn remove(&mut self) {
        let mut queue = self
            .direction
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(index) = queue.waiters.iter().position(|(id, _)| *id == self.id) {
            queue.waiters.remove(index);
        }
        drop(queue);
        self.direction.changed.notify_waiters();
    }
}

impl Drop for WaitTicket<'_> {
    fn drop(&mut self) {
        self.remove();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test(start_paused = true)]
    async fn concurrent_objects_share_the_whole_object_budget() {
        let throttle = Arc::new(PayloadThrottle::new(1, 0));
        let started = Instant::now();
        let first = {
            let throttle = throttle.clone();
            tokio::spawn(async move { throttle.acquire_upload(62_500).await })
        };
        let second = {
            let throttle = throttle.clone();
            tokio::spawn(async move { throttle.acquire_upload(62_500).await })
        };
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(520)).await;
        tokio::task::yield_now().await;
        assert!(first.is_finished());
        assert!(!second.is_finished());
        tokio::time::advance(Duration::from_millis(520)).await;
        first.await.unwrap();
        second.await.unwrap();
        assert!(Instant::now() - started >= Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn foreground_download_jumps_a_queued_prefetch() {
        let throttle = Arc::new(PayloadThrottle::new(0, 1));
        let first = throttle.clone();
        let prefetch = tokio::spawn(async move { first.acquire_download(125_000, false).await });
        tokio::task::yield_now().await;
        let foreground_started = Instant::now();
        assert!(throttle.acquire_download(1, true).await);
        assert!(Instant::now() - foreground_started < Duration::from_millis(10));
        assert!(!prefetch.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(prefetch.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn object_larger_than_one_second_is_charged_in_full() {
        let throttle = PayloadThrottle::new(1, 0);
        let task = tokio::spawn(async move { throttle.acquire_upload(250_000).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(task.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_waiter_refunds_budget_for_next_object() {
        let throttle = Arc::new(PayloadThrottle::new(1, 0));
        let first = throttle.clone();
        let task = tokio::spawn(async move { first.acquire_upload(125_000).await });
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let start = Instant::now();
        assert!(throttle.acquire_upload(125_000).await);
        assert_eq!(Instant::now() - start, Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn unlimited_rate_does_not_wait() {
        let throttle = PayloadThrottle::new(0, 0);
        let start = Instant::now();
        assert!(throttle.acquire_upload(1_000_000).await);
        assert_eq!(Instant::now(), start);
        assert_eq!(throttle.wait_nanos(), (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn finite_live_rate_update_keeps_elapsed_credit() {
        let throttle = Arc::new(PayloadThrottle::new(1, 0));
        let waiting = throttle.clone();
        let task = tokio::spawn(async move { waiting.acquire_upload(125_000).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(500)).await;
        throttle.set_limits(Some(2), Some(0));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(250)).await;
        tokio::task::yield_now().await;
        assert!(task.is_finished());
        assert!(task.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_wait_without_holding_the_permit() {
        let throttle = Arc::new(PayloadThrottle::new(1, 0));
        let waiter = throttle.clone();
        let task = tokio::spawn(async move { waiter.acquire_upload(125_000).await });
        tokio::task::yield_now().await;
        throttle.cancel();
        assert!(!task.await.unwrap());
        assert!(!throttle.acquire_upload(1,).await);
    }
}
