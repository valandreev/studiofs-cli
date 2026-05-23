use super::PgMetaEngine;
use crate::engine::{MetaError, MetaResult};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{JoinHandle, ThreadId};
use std::time::Duration;
use tokio::runtime::RuntimeFlavor;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

type RuntimeJob = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(super) type PgClientFuture<'a, T> = Pin<Box<dyn Future<Output = MetaResult<T>> + Send + 'a>>;

enum RuntimeMessage {
    Run(RuntimeJob),
    Shutdown,
}

pub(super) struct PgRemoteRuntime {
    tx: Mutex<Option<std::sync::mpsc::SyncSender<RuntimeMessage>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    runtime_thread_id: ThreadId,
}

struct IdlePgConnection {
    client: tokio_postgres::Client,
    task: tokio::task::JoinHandle<()>,
}

struct PooledPgConnection {
    idle: IdlePgConnection,
    _active_permit: OwnedSemaphorePermit,
}

#[derive(Default)]
struct PgConnectionPoolState {
    idle: Vec<IdlePgConnection>,
    total: usize,
}

pub(super) struct PgConnectionPool {
    postgres_url: String,
    connect_timeout: Duration,
    checkout_timeout: Duration,
    max_size: usize,
    active_slots: Arc<Semaphore>,
    state: Mutex<PgConnectionPoolState>,
    connection_creations: AtomicU64,
    checkouts: AtomicU64,
    checkout_waits: AtomicU64,
    discarded: AtomicU64,
}

#[cfg(test)]
pub(super) struct PgConnectionPoolStats {
    pub(super) connection_creations: u64,
    pub(super) checkouts: u64,
    pub(super) checkout_waits: u64,
    pub(super) discarded: u64,
    pub(super) total: usize,
}

struct PendingPgConnectionSlot<'a> {
    state: &'a Mutex<PgConnectionPoolState>,
    armed: bool,
}

impl<'a> PendingPgConnectionSlot<'a> {
    fn new(state: &'a Mutex<PgConnectionPoolState>) -> Self {
        Self { state, armed: true }
    }

    fn commit(mut self) {
        self.armed = false;
    }
}

impl Drop for PendingPgConnectionSlot<'_> {
    fn drop(&mut self) {
        if self.armed {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.total = state.total.saturating_sub(1);
        }
    }
}

impl PgConnectionPool {
    const DEFAULT_MAX_SIZE: usize = 4;

    pub(super) fn new(
        postgres_url: String,
        connect_timeout: Duration,
        checkout_timeout: Duration,
    ) -> Self {
        Self {
            postgres_url,
            connect_timeout,
            checkout_timeout,
            max_size: Self::DEFAULT_MAX_SIZE,
            active_slots: Arc::new(Semaphore::new(Self::DEFAULT_MAX_SIZE)),
            state: Mutex::new(PgConnectionPoolState::default()),
            connection_creations: AtomicU64::new(0),
            checkouts: AtomicU64::new(0),
            checkout_waits: AtomicU64::new(0),
            discarded: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    pub(super) fn stats(&self) -> PgConnectionPoolStats {
        PgConnectionPoolStats {
            connection_creations: self.connection_creations.load(Ordering::SeqCst),
            checkouts: self.checkouts.load(Ordering::SeqCst),
            checkout_waits: self.checkout_waits.load(Ordering::SeqCst),
            discarded: self.discarded.load(Ordering::SeqCst),
            total: self.state.lock().unwrap_or_else(|p| p.into_inner()).total,
        }
    }

    async fn checkout(&self, context: &'static str) -> MetaResult<PooledPgConnection> {
        let permit = if self.active_slots.available_permits() == 0 {
            self.checkout_waits.fetch_add(1, Ordering::SeqCst);
            tokio::time::timeout(
                self.checkout_timeout,
                self.active_slots.clone().acquire_owned(),
            )
            .await
            .map_err(|_| {
                PgMetaEngine::pg_timeout_error(
                    &format!("{context} checkout"),
                    self.checkout_timeout,
                )
            })?
            .map_err(|_| MetaError::Storage("postgres command pool is closed".to_string()))?
        } else {
            self.active_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| MetaError::Storage("postgres command pool is closed".to_string()))?
        };

        let checkout = async {
            let pending_slot = {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                while let Some(connection) = state.idle.pop() {
                    if connection.task.is_finished() {
                        state.total = state.total.saturating_sub(1);
                        self.discarded.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                    self.checkouts.fetch_add(1, Ordering::SeqCst);
                    return Ok(PooledPgConnection {
                        idle: connection,
                        _active_permit: permit,
                    });
                }

                if state.total < self.max_size {
                    state.total += 1;
                    Some(PendingPgConnectionSlot::new(&self.state))
                } else {
                    None
                }
            };

            debug_assert!(
                pending_slot.is_some(),
                "active semaphore should bound live checkouts"
            );
            match self.connect().await {
                Ok(connection) => {
                    if let Some(pending_slot) = pending_slot {
                        pending_slot.commit();
                    }
                    self.checkouts.fetch_add(1, Ordering::SeqCst);
                    Ok(PooledPgConnection {
                        idle: connection,
                        _active_permit: permit,
                    })
                }
                Err(error) => Err(error),
            }
        };

        tokio::time::timeout(self.checkout_timeout, checkout)
            .await
            .map_err(|_| {
                PgMetaEngine::pg_timeout_error(
                    &format!("{context} checkout"),
                    self.checkout_timeout,
                )
            })?
    }

    async fn connect(&self) -> MetaResult<IdlePgConnection> {
        let connect = tokio_postgres::connect(&self.postgres_url, tokio_postgres::NoTls);
        let (client, connection) = tokio::time::timeout(self.connect_timeout, connect)
            .await
            .map_err(|_| PgMetaEngine::pg_timeout_error("connect", self.connect_timeout))?
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres connect failed: {}",
                    PgMetaEngine::format_pg_error(e)
                ))
            })?;
        self.connection_creations.fetch_add(1, Ordering::SeqCst);
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(IdlePgConnection { client, task })
    }

    fn put_back(&self, connection: PooledPgConnection) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if connection.idle.task.is_finished() || state.idle.len() >= self.max_size {
            state.total = state.total.saturating_sub(1);
            self.discarded.fetch_add(1, Ordering::SeqCst);
            connection.idle.task.abort();
            return;
        }
        state.idle.push(connection.idle);
    }

    fn discard(&self, connection: PooledPgConnection) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.total = state.total.saturating_sub(1);
        self.discarded.fetch_add(1, Ordering::SeqCst);
        connection.idle.task.abort();
    }
}

impl Drop for PgConnectionPool {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        for connection in state.idle.drain(..) {
            connection.task.abort();
        }
        state.total = 0;
    }
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
                let rx = std::sync::Arc::new(std::sync::Mutex::new(rx));
                rt.block_on(async move {
                    loop {
                        let rx = rx.clone();
                        let message = tokio::task::spawn_blocking(move || {
                            rx.lock().unwrap_or_else(|p| p.into_inner()).recv()
                        })
                        .await;
                        let Ok(Ok(message)) = message else {
                            break;
                        };
                        match message {
                            RuntimeMessage::Run(job) => {
                                tokio::spawn(job);
                            }
                            RuntimeMessage::Shutdown => break,
                        }
                    }
                });
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
        let job = RuntimeMessage::Run(Box::pin(async move {
            let _ = result_tx.send(fut.await);
        }));
        let tx = self
            .tx
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .ok_or_else(|| MetaError::Storage("postgres runtime is shut down".to_string()))?;
        tx.try_send(job).map_err(|e| {
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

    pub(super) async fn with_postgres_client_timeout_once<T, F, Fut>(
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

    pub(super) async fn with_postgres_client_timeout<T, F>(
        command_pool: Arc<PgConnectionPool>,
        command_timeout: Duration,
        context: &'static str,
        f: F,
    ) -> MetaResult<T>
    where
        T: Send,
        F: for<'a> FnOnce(&'a mut tokio_postgres::Client) -> PgClientFuture<'a, T>,
    {
        let mut connection = command_pool.checkout(context).await?;
        let result = tokio::time::timeout(command_timeout, f(&mut connection.idle.client))
            .await
            .map_err(|_| Self::pg_timeout_error(context, command_timeout));

        match result {
            Ok(Ok(value)) => {
                command_pool.put_back(connection);
                Ok(value)
            }
            Ok(Err(error)) => {
                if connection.idle.client.is_closed() {
                    command_pool.discard(connection);
                } else {
                    command_pool.put_back(connection);
                }
                Err(error)
            }
            Err(error) => {
                command_pool.discard(connection);
                Err(error)
            }
        }
    }

    #[cfg(test)]
    pub(super) fn command_pool_stats(&self) -> PgConnectionPoolStats {
        self.command_pool.stats()
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
    use super::{PgConnectionPool, PgRemoteRuntime};
    use crate::engine::MetaError;
    use crate::pg_engine::tests::temp_pg_engine_for_unit;
    use std::sync::Arc;
    use std::thread::ThreadId;
    use std::time::Duration;

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

    #[test]
    fn command_pool_connect_timeout_does_not_leak_total_slots() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let pool = PgConnectionPool::new(
            "postgres://10.255.255.1:5432/studiofs".to_string(),
            Duration::from_millis(1),
            Duration::from_millis(1),
        );

        let result = runtime.block_on(pool.checkout("unit test connect timeout"));

        assert!(result.is_err(), "checkout should fail quickly");
        assert_eq!(
            pool.stats().total,
            0,
            "failed or canceled connection creation must release its reserved total slot"
        );
    }
}
