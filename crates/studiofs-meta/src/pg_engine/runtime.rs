use super::PgMetaEngine;
use crate::engine::{MetaError, MetaResult};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Mutex, OnceLock};
use std::thread::{JoinHandle, ThreadId};
use std::time::Duration;
use tokio::runtime::RuntimeFlavor;

type RuntimeJob = Box<dyn FnOnce(&tokio::runtime::Runtime) + Send + 'static>;

enum RuntimeMessage {
    Run(RuntimeJob),
    Shutdown,
}

pub(super) struct PgRemoteRuntime {
    tx: Mutex<Option<std::sync::mpsc::SyncSender<RuntimeMessage>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    runtime_thread_id: ThreadId,
}

impl PgRemoteRuntime {
    pub(super) fn new(name: &'static str) -> MetaResult<Self> {
        let (tx, rx) = std::sync::mpsc::sync_channel::<RuntimeMessage>(128);
        let (startup_tx, startup_rx) = std::sync::mpsc::sync_channel::<Result<ThreadId, String>>(1);
        let handle = std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(error) => {
                        let _ = startup_tx
                            .send(Err(format!("failed to build postgres runtime: {error}")));
                        return;
                    }
                };
                let _ = startup_tx.send(Ok(std::thread::current().id()));
                while let Ok(message) = rx.recv() {
                    match message {
                        RuntimeMessage::Run(job) => {
                            let _ = catch_unwind(AssertUnwindSafe(|| job(&rt)));
                        }
                        RuntimeMessage::Shutdown => break,
                    }
                }
            })
            .map_err(|e| MetaError::Storage(format!("failed to spawn postgres runtime: {e}")))?;
        let runtime_thread_id = startup_rx
            .recv()
            .map_err(|e| {
                MetaError::Storage(format!("failed to receive postgres runtime startup: {e}"))
            })?
            .map_err(MetaError::Storage)?;
        Ok(Self {
            tx: Mutex::new(Some(tx)),
            handle: Mutex::new(Some(handle)),
            runtime_thread_id,
        })
    }

    fn block_on<T, F>(&self, fut: F) -> MetaResult<T>
    where
        T: Send + 'static,
        F: Future<Output = MetaResult<T>> + Send + 'static,
    {
        // The runtime is single-threaded; recursive submission from its worker
        // would wait forever because the worker cannot service its own queue.
        if std::thread::current().id() == self.runtime_thread_id {
            return Err(MetaError::Storage(
                "recursive postgres runtime execution is not supported".to_string(),
            ));
        }
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        let job = RuntimeMessage::Run(Box::new(move |rt| {
            let _ = result_tx.send(rt.block_on(fut));
        }));
        let tx = self
            .tx
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .ok_or_else(|| MetaError::Storage("postgres runtime is shut down".to_string()))?;
        tx.send(job).map_err(|e| {
            MetaError::Storage(format!("failed to submit postgres runtime job: {e}"))
        })?;
        result_rx.recv().map_err(|e| {
            MetaError::Storage(format!(
                "failed to receive result from postgres runtime: {e}"
            ))
        })?
    }
}

impl Drop for PgRemoteRuntime {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.lock().unwrap_or_else(|p| p.into_inner()).take() {
            let _ = tx.send(RuntimeMessage::Shutdown);
        }
        if let Some(handle) = self.handle.lock().unwrap_or_else(|p| p.into_inner()).take() {
            let _ = handle.join();
        }
    }
}

fn shared_once_runtime() -> MetaResult<&'static PgRemoteRuntime> {
    static RUNTIME: OnceLock<Result<PgRemoteRuntime, String>> = OnceLock::new();
    match RUNTIME.get_or_init(|| {
        PgRemoteRuntime::new("studiofs-pg-once-runtime").map_err(|error| error.to_string())
    }) {
        Ok(runtime) => Ok(runtime),
        Err(error) => Err(MetaError::Storage(error.clone())),
    }
}

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
        T: Send + 'static,
        F: Future<Output = MetaResult<T>> + Send + 'static,
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return match handle.runtime_flavor() {
                RuntimeFlavor::MultiThread => {
                    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
                }
                RuntimeFlavor::CurrentThread => shared_once_runtime()?.block_on(fut),
                _ => shared_once_runtime()?.block_on(fut),
            };
        }

        shared_once_runtime()?.block_on(fut)
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
        T: Send + 'static,
        F: Future<Output = MetaResult<T>> + Send + 'static,
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return match handle.runtime_flavor() {
                RuntimeFlavor::MultiThread => {
                    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
                }
                RuntimeFlavor::CurrentThread => self.remote_runtime.block_on(fut),
                _ => self.remote_runtime.block_on(fut),
            };
        }

        self.remote_runtime.block_on(fut)
    }
}

#[cfg(test)]
mod tests {
    use super::PgRemoteRuntime;
    use crate::engine::MetaError;
    use crate::pg_engine::tests::temp_pg_engine_for_unit;
    use std::sync::Arc;
    use std::thread::ThreadId;

    #[test]
    fn run_async_reuses_current_thread_fallback_runtime_thread() {
        let (_dir, engine) = temp_pg_engine_for_unit();
        let current_thread_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");

        current_thread_runtime.block_on(async {
            let first: ThreadId = engine
                .run_async(async { Ok(std::thread::current().id()) })
                .expect("first run_async");
            let second: ThreadId = engine
                .run_async(async { Ok(std::thread::current().id()) })
                .expect("second run_async");

            assert_eq!(
                first, second,
                "current-thread callers should reuse a long-lived fallback runtime thread"
            );
        });
    }

    #[test]
    fn remote_runtime_rejects_recursive_block_on() {
        let runtime = Arc::new(PgRemoteRuntime::new("studiofs-pg-recursive-runtime-test").unwrap());
        let nested = runtime.clone();

        let error = runtime
            .block_on(async move { nested.block_on(async { Ok::<_, MetaError>(()) }) })
            .expect_err("recursive block_on should fail");

        assert!(
            error
                .to_string()
                .contains("recursive postgres runtime execution is not supported"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn remote_runtime_survives_panicking_job() {
        let runtime = PgRemoteRuntime::new("studiofs-pg-panic-runtime-test").unwrap();

        let error = runtime
            .block_on(async {
                panic!("intentional runtime job panic");
                #[allow(unreachable_code)]
                Ok::<_, MetaError>(())
            })
            .expect_err("panicking job should return an error to caller");
        assert!(
            error
                .to_string()
                .contains("failed to receive result from postgres runtime"),
            "unexpected error: {error}"
        );

        let value = runtime
            .block_on(async { Ok::<_, MetaError>(42) })
            .expect("runtime should survive one panicking job");
        assert_eq!(value, 42);
    }
}
