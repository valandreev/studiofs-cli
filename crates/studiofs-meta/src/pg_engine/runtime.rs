use super::PgMetaEngine;
use crate::engine::{MetaError, MetaResult};
use std::future::Future;
use std::time::Duration;
use tokio::runtime::RuntimeFlavor;

impl PgMetaEngine {
    pub(super) fn format_pg_error(e: tokio_postgres::Error) -> String {
        if let Some(db) = e.as_db_error() {
            format!("{} (sqlstate={})", db.message(), db.code().code())
        } else {
            e.to_string()
        }
    }

    pub(super) fn pg_timeout_error(context: &str, timeout: Duration) -> MetaError {
        MetaError::Storage(format!(
            "postgres {context} timed out after {} ms",
            timeout.as_millis()
        ))
    }

    pub(super) fn run_async_once<T, F>(fut: F) -> MetaResult<T>
    where
        T: Send,
        F: Future<Output = MetaResult<T>> + Send,
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return match handle.runtime_flavor() {
                RuntimeFlavor::MultiThread => {
                    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
                }
                RuntimeFlavor::CurrentThread => std::thread::scope(|scope| {
                    let (tx, rx) = std::sync::mpsc::sync_channel(1);
                    scope.spawn(move || {
                        let result = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|e| {
                                MetaError::Storage(format!("failed to build tokio runtime: {e}"))
                            })
                            .and_then(|rt| rt.block_on(fut));
                        let _ = tx.send(result);
                    });
                    rx.recv().map_err(|e| {
                        MetaError::Storage(format!(
                            "failed to receive result from fallback runtime: {e}"
                        ))
                    })?
                }),
                _ => std::thread::scope(|scope| {
                    let (tx, rx) = std::sync::mpsc::sync_channel(1);
                    scope.spawn(move || {
                        let result = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|e| {
                                MetaError::Storage(format!("failed to build tokio runtime: {e}"))
                            })
                            .and_then(|rt| rt.block_on(fut));
                        let _ = tx.send(result);
                    });
                    rx.recv().map_err(|e| {
                        MetaError::Storage(format!(
                            "failed to receive result from fallback runtime: {e}"
                        ))
                    })?
                }),
            };
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| MetaError::Storage(format!("failed to build tokio runtime: {e}")))?;
        rt.block_on(fut)
    }

    pub(super) async fn with_postgres_client_timeout<T, F, Fut>(
        postgres_url: String,
        connect_timeout: Duration,
        command_timeout: Duration,
        context: &'static str,
        f: F,
    ) -> MetaResult<T>
    where
        F: FnOnce(tokio_postgres::Client) -> Fut,
        Fut: Future<Output = MetaResult<T>>,
    {
        let connect = tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls);
        let (client, connection) = tokio::time::timeout(connect_timeout, connect)
            .await
            .map_err(|_| Self::pg_timeout_error(&format!("{context} connect"), connect_timeout))?
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres connect failed: {}",
                    Self::format_pg_error(e)
                ))
            })?;

        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });

        let result = tokio::time::timeout(command_timeout, f(client))
            .await
            .map_err(|_| Self::pg_timeout_error(context, command_timeout));

        conn_task.abort();
        match result {
            Ok(result) => result,
            Err(err) => Err(err),
        }
    }

    pub(super) fn run_async<T, F>(&self, fut: F) -> MetaResult<T>
    where
        T: Send,
        F: Future<Output = MetaResult<T>> + Send,
    {
        Self::run_async_once(fut)
    }
}
