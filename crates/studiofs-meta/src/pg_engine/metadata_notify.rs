use super::{MetadataSyncSource, PgMetaEngine};
use crate::engine::{MetaError, MetaResult};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

impl PgMetaEngine {
    pub(super) fn ensure_metadata_notify_listener_started(&self) -> MetaResult<()> {
        if self
            .metadata_notify_handle
            .lock()
            .map(|guard| guard.is_some())
            .unwrap_or(false)
        {
            return Ok(());
        }

        let engine = self.self_weak.get().cloned().ok_or_else(|| {
            MetaError::Storage("pg metadata notify bootstrap missing self weak ref".to_string())
        })?;

        let mut notify_guard = self.metadata_notify_handle.lock().map_err(|_| {
            MetaError::Storage("pg metadata notify handle state poisoned".to_string())
        })?;
        if notify_guard.is_none() {
            *notify_guard = Some(Self::start_metadata_notify_listener(
                engine,
                self.poll_shutdown.clone(),
                self.metadata_notify_cancel.clone(),
            )?);
        }

        Ok(())
    }

    fn start_metadata_notify_listener(
        engine: Weak<Self>,
        shutdown: Arc<AtomicBool>,
        cancel: CancellationToken,
    ) -> MetaResult<std::thread::JoinHandle<()>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| MetaError::Storage(format!("failed to build tokio runtime: {e}")))?;
        std::thread::Builder::new()
            .name("pg-meta-notify".into())
            .spawn(move || {
                let mut attempted_listen = false;
                let mut reconnect_delay =
                    Duration::from_millis(Self::METADATA_NOTIFY_RECONNECT_MIN_DELAY_MS);
                while !shutdown.load(Ordering::Acquire) {
                    let (postgres_url, volume_id, connect_timeout) = match engine.upgrade() {
                        Some(engine_arc) => (
                            engine_arc.postgres_url.clone(),
                            engine_arc.volume_id,
                            engine_arc.connect_timeout,
                        ),
                        None => break,
                    };

                    if attempted_listen
                        && let Some(engine_arc) = engine.upgrade()
                    {
                        engine_arc
                            .metadata_notify_reconnects
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    attempted_listen = true;

                    let channel = Self::metadata_notify_channel(volume_id);
                    let weak_engine = engine.clone();
                    let shutdown_for_task = shutdown.clone();
                    let cancel_for_task = cancel.clone();
                    let connect_result = {
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
                                    Self::pg_timeout_error(
                                        "metadata notify connect",
                                        connect_timeout,
                                    )
                                })?
                                .map_err(|e| {
                                    MetaError::Storage(format!(
                                        "postgres metadata notify connect failed: {}",
                                        Self::format_pg_error(e)
                                    ))
                                })?;

                            let listen_cmd = format!("LISTEN {channel}");
                            let listen = client.batch_execute(&listen_cmd);
                            tokio::pin!(listen);
                            loop {
                                let message = tokio::time::timeout(
                                    Duration::from_millis(
                                        Self::METADATA_NOTIFY_IDLE_POLL_INTERVAL_MS,
                                    ),
                                    std::future::poll_fn(|cx| connection.poll_message(cx)),
                                );
                                tokio::select! {
                                    biased;
                                    _ = cancel_for_task.cancelled() => return Ok(()),
                                    r = &mut listen => {
                                        r.map_err(|e| MetaError::Storage(format!(
                                            "postgres LISTEN {channel} failed: {}",
                                            Self::format_pg_error(e)
                                        )))?;
                                        break;
                                    }
                                    msg = message => {
                                        match msg {
                                            Ok(Some(Ok(tokio_postgres::AsyncMessage::Notice(_)))) => {}
                                            Ok(Some(Ok(_))) => {}
                                            Ok(Some(Err(err))) => {
                                                return Err(MetaError::Storage(format!(
                                                    "postgres metadata notify LISTEN poll failed: {}",
                                                    Self::format_pg_error(err)
                                                )));
                                            }
                                            Ok(None) => return Ok(()),
                                            Err(_) => {}
                                        }
                                    }
                                }
                            }
                            {
                                let Some(engine_arc) = weak_engine.upgrade() else {
                                    return Ok(());
                                };
                                let changed =
                                    engine_arc.sync_metadata_with_source(MetadataSyncSource::Notify)?;
                                if changed {
                                    engine_arc.metadata_invalidation_notify.notify_one();
                                }
                            }

                            while !shutdown_for_task.load(Ordering::Acquire) {
                                let message = tokio::select! {
                                    biased;
                                    _ = cancel_for_task.cancelled() => break,
                                    msg = tokio::time::timeout(
                                        Duration::from_millis(Self::METADATA_NOTIFY_IDLE_POLL_INTERVAL_MS),
                                        std::future::poll_fn(|cx| connection.poll_message(cx)),
                                    ) => msg,
                                };
                                match message {
                                    Ok(Some(Ok(tokio_postgres::AsyncMessage::Notification(notification)))) => {
                                        if notification.channel() != channel {
                                            continue;
                                        }
                                        let Some(engine_arc) = weak_engine.upgrade() else {
                                            return Ok(());
                                        };
                                        engine_arc
                                            .metadata_notify_received
                                            .fetch_add(1, Ordering::Relaxed);
                                        let changed = engine_arc
                                            .sync_metadata_with_source(MetadataSyncSource::Notify)?;
                                        if changed {
                                            engine_arc.metadata_invalidation_notify.notify_one();
                                        }
                                    }
                                    Ok(Some(Ok(tokio_postgres::AsyncMessage::Notice(_)))) => {}
                                    Ok(Some(Ok(_))) => {}
                                    Ok(Some(Err(err))) => {
                                        return Err(MetaError::Storage(format!(
                                            "postgres metadata notify poll failed: {}",
                                            Self::format_pg_error(err)
                                        )));
                                    }
                                    Ok(None) => return Ok(()),
                                    Err(_) => {
                                        if weak_engine.upgrade().is_none() {
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Ok(())
                        };

                        rt.block_on(fut)
                    };

                    if shutdown.load(Ordering::Acquire) || cancel.is_cancelled() {
                        break;
                    }
                    if connect_result.is_err() {
                        std::thread::sleep(reconnect_delay);
                        reconnect_delay = reconnect_delay.saturating_mul(2).min(
                            Duration::from_millis(Self::METADATA_NOTIFY_RECONNECT_MAX_DELAY_MS),
                        );
                    } else {
                        reconnect_delay =
                            Duration::from_millis(Self::METADATA_NOTIFY_RECONNECT_MIN_DELAY_MS);
                    }
                }
            })
            .map_err(|e| {
                MetaError::Storage(format!("failed to spawn pg-meta-notify thread: {e}"))
            })
    }
}
