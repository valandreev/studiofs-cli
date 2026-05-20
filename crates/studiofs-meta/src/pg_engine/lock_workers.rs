use super::PgMetaEngine;
use crate::engine::{MetaEngineCore, MetaError, MetaResult};
use crate::types::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

impl PgMetaEngine {
    pub(super) fn ensure_lock_notify_listener_started(&self) -> MetaResult<()> {
        if !self.lock_workers_enabled {
            return Ok(());
        }

        if self
            .lock_notify_handle
            .lock()
            .map(|guard| guard.is_some())
            .unwrap_or(false)
        {
            return Ok(());
        }

        let _start_guard = self
            .lock_worker_start_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg lock worker start state poisoned".to_string()))?;
        let engine = self.self_weak.get().cloned().ok_or_else(|| {
            MetaError::Storage("pg lock worker bootstrap missing self weak ref".to_string())
        })?;

        let mut notify_guard = self
            .lock_notify_handle
            .lock()
            .map_err(|_| MetaError::Storage("pg lock notify handle state poisoned".to_string()))?;
        if notify_guard.is_none() {
            *notify_guard = Some(Self::start_lock_notify_listener(
                engine.clone(),
                self.poll_shutdown.clone(),
                self.lock_notify_cancel.clone(),
            )?);
        }
        drop(notify_guard);

        Ok(())
    }

    pub(super) fn ensure_lock_renew_worker_started(&self) -> MetaResult<()> {
        if !self.lock_workers_enabled {
            return Ok(());
        }

        if self
            .lock_renew_handle
            .lock()
            .map(|guard| guard.is_some())
            .unwrap_or(false)
        {
            return Ok(());
        }

        let _start_guard = self
            .lock_worker_start_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg lock worker start state poisoned".to_string()))?;
        let engine = self.self_weak.get().cloned().ok_or_else(|| {
            MetaError::Storage("pg lock worker bootstrap missing self weak ref".to_string())
        })?;

        let mut renew_guard = self
            .lock_renew_handle
            .lock()
            .map_err(|_| MetaError::Storage("pg lock renew handle state poisoned".to_string()))?;
        if renew_guard.is_none() {
            *renew_guard = Some(Self::start_lock_renew_worker(
                engine,
                self.poll_shutdown.clone(),
            )?);
        }

        Ok(())
    }

    pub(super) fn ensure_lock_workers_started(&self) -> MetaResult<()> {
        self.ensure_lock_notify_listener_started()?;
        self.ensure_lock_renew_worker_started()
    }

    fn start_lock_notify_listener(
        engine: Weak<Self>,
        shutdown: Arc<AtomicBool>,
        cancel: CancellationToken,
    ) -> MetaResult<std::thread::JoinHandle<()>> {
        std::thread::Builder::new()
            .name("pg-lock-notify".into())
            .spawn(move || {
                while !shutdown.load(Ordering::Acquire) {
                    let (postgres_url, volume_id, connect_timeout, safety_refresh_interval) =
                        match engine.upgrade() {
                            Some(engine_arc) => (
                                engine_arc.postgres_url.clone(),
                                engine_arc.volume_id,
                                engine_arc.connect_timeout,
                                Self::default_lock_notify_safety_refresh_interval(),
                            ),
                            None => break,
                        };

                    let channel = Self::lock_notify_channel(volume_id);
                    let weak_engine = engine.clone();
                    let shutdown_for_task = shutdown.clone();
                    let cancel_for_task = cancel.clone();
                    let connect_result = (|| -> MetaResult<()> {
                        let fut = async move {
                            let connect_result = tokio::select! {
                                biased;
                                _ = cancel_for_task.cancelled() => return Ok(()),
                                r = tokio::time::timeout(
                                    connect_timeout,
                                    tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls),
                                ) => r,
                            };
                            let (client, mut connection) = connect_result
                                .map_err(|_| {
                                    Self::pg_timeout_error("lock notify connect", connect_timeout)
                                })?
                                .map_err(|e| {
                                    MetaError::Storage(format!(
                                        "postgres lock notify connect failed: {}",
                                        Self::format_pg_error(e)
                                    ))
                                })?;

                            let Some(engine_arc) = weak_engine.upgrade() else {
                                return Ok(());
                            };

                            let listen_cmd = format!("LISTEN {channel}");
                            tokio::select! {
                                biased;
                                _ = cancel_for_task.cancelled() => return Ok(()),
                                r = client.batch_execute(&listen_cmd) => {
                                    r.map_err(|e| MetaError::Storage(format!(
                                        "postgres LISTEN {channel} failed: {}",
                                        Self::format_pg_error(e)
                                    )))?;
                                }
                            }

                            let mut next_safety_refresh = Instant::now() + safety_refresh_interval;
                            while !shutdown_for_task.load(Ordering::Acquire) {
                                let message = tokio::select! {
                                    biased;
                                    _ = cancel_for_task.cancelled() => break,
                                    msg = tokio::time::timeout(
                                        Duration::from_millis(Self::LOCK_NOTIFY_IDLE_POLL_INTERVAL_MS),
                                        std::future::poll_fn(|cx| connection.poll_message(cx)),
                                    ) => msg,
                                };
                                match message {
                                    Ok(Some(Ok(tokio_postgres::AsyncMessage::Notification(notification)))) => {
                                        if let Some(ino) = Self::parse_file_lock_notify_payload(notification.payload()) {
                                            tokio::select! {
                                                biased;
                                                _ = cancel_for_task.cancelled() => break,
                                                r = engine_arc.refresh_known_file_lock_lease_with_client(&client, ino) => { r?; }
                                            }
                                        }
                                    }
                                    Ok(Some(Ok(tokio_postgres::AsyncMessage::Notice(_)))) => {}
                                    Ok(Some(Ok(_))) => {}
                                    Ok(Some(Err(err))) => {
                                        return Err(MetaError::Storage(format!(
                                            "postgres lock notify poll failed: {}",
                                            Self::format_pg_error(err)
                                        )));
                                    }
                                    Ok(None) => return Ok(()),
                                    Err(_) => {
                                        if Instant::now() >= next_safety_refresh {
                                            let cached_inos: Vec<Ino> = engine_arc
                                                .file_lock_cache
                                                .lock()
                                                .map(|cache| cache.keys().copied().collect())
                                                .unwrap_or_default();
                                            for ino in cached_inos {
                                                tokio::select! {
                                                    biased;
                                                    _ = cancel_for_task.cancelled() => return Ok(()),
                                                    r = engine_arc.refresh_known_file_lock_lease_with_client(&client, ino) => { r?; }
                                                }
                                            }
                                            next_safety_refresh = Instant::now() + safety_refresh_interval;
                                        }
                                    }
                                }
                            }
                            Ok(())
                        };

                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|e| {
                                MetaError::Storage(format!("failed to build tokio runtime: {e}"))
                            })?;
                        rt.block_on(fut)
                    })();

                    if shutdown.load(Ordering::Acquire) || cancel.is_cancelled() {
                        break;
                    }
                    if connect_result.is_err() {
                        std::thread::sleep(Duration::from_millis(250));
                    }
                }
            })
            .map_err(|e| MetaError::Storage(format!("failed to spawn pg-lock-notify thread: {e}")))
    }

    fn start_lock_renew_worker(
        engine: Weak<Self>,
        shutdown: Arc<AtomicBool>,
    ) -> MetaResult<std::thread::JoinHandle<()>> {
        std::thread::Builder::new()
            .name("pg-lock-renew".into())
            .spawn(move || {
                while !shutdown.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(50));
                    let Some(engine_arc) = engine.upgrade() else {
                        break;
                    };
                    let now = Instant::now();
                    let due: Vec<(Ino, Duration)> = match engine_arc.held_file_locks.lock() {
                        Ok(held) => held
                            .iter()
                            .filter_map(|(&ino, state)| {
                                (state.next_renew_at <= now).then_some((ino, state.lease_ttl))
                            })
                            .collect(),
                        Err(_) => Vec::new(),
                    };

                    for (ino, lease_ttl) in due {
                        if shutdown.load(Ordering::Acquire) {
                            break;
                        }
                        let result = engine_arc.renew_file_lock_lease(ino, lease_ttl);
                        match result {
                            Ok(FileLockLeaseRenewOutcome::Renewed(_)) => {
                                if let Ok(mut held) = engine_arc.held_file_locks.lock()
                                    && let Some(state) = held.get_mut(&ino)
                                {
                                    state.next_renew_at =
                                        Instant::now() + Self::half_duration(lease_ttl);
                                }
                            }
                            Ok(FileLockLeaseRenewOutcome::Lost(_)) => {
                                engine_arc.forget_held_file_lock(ino);
                            }
                            Err(_) => {
                                if let Ok(mut held) = engine_arc.held_file_locks.lock()
                                    && let Some(state) = held.get_mut(&ino)
                                {
                                    state.next_renew_at = Instant::now()
                                        + Self::half_duration(
                                            lease_ttl.min(Duration::from_secs(1)),
                                        );
                                }
                            }
                        }
                    }
                }
            })
            .map_err(|e| MetaError::Storage(format!("failed to spawn pg-lock-renew thread: {e}")))
    }
}
