use super::{PgMetaEngine, PgTimeoutContext};
use crate::engine::{MetaError, MetaResult};
use crate::pg_transport::{classify_postgres_error, connect_studiofs_postgres};
use std::cell::Cell;
use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{JoinHandle, ThreadId};
use std::time::Duration;
use tokio::runtime::RuntimeFlavor;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::warn;

// Tokio reserves three bits in its permit counter; this mirrors its private
// Semaphore::MAX_PERMITS limit in the pinned Tokio release.
const TOKIO_MAX_SEMAPHORE_PERMITS: usize = usize::MAX >> 3;

type RuntimeJob = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(super) type PgClientFuture<'a, T> = Pin<Box<dyn Future<Output = MetaResult<T>> + Send + 'a>>;

thread_local! {
    static WORKER_REMOTE_CANCELLATION: Cell<bool> = const { Cell::new(false) };
}

/// Restrict shutdown cancellation to remote work performed by a worker step.
pub(super) struct WorkerRemoteCancellationScope(bool);

impl WorkerRemoteCancellationScope {
    pub(super) fn enter() -> Self {
        Self(WORKER_REMOTE_CANCELLATION.replace(true))
    }
}

impl Drop for WorkerRemoteCancellationScope {
    fn drop(&mut self) {
        WORKER_REMOTE_CANCELLATION.set(self.0);
    }
}

enum RuntimeMessage {
    Run(RuntimeJob),
}

pub(super) struct PgRemoteRuntime {
    tx: Mutex<Option<tokio::sync::mpsc::Sender<RuntimeMessage>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    runtime_thread_id: ThreadId,
}

struct IdlePgConnection {
    client: tokio_postgres::Client,
    task: tokio::task::JoinHandle<()>,
}

struct PooledPgConnection {
    idle: Option<IdlePgConnection>,
    pool: Arc<PgConnectionPool>,
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
    state: Arc<Mutex<PgConnectionPoolState>>,
    connection_creations: AtomicU64,
    checkouts: AtomicU64,
    checkout_waits: AtomicU64,
    discarded: AtomicU64,
}

#[cfg(test)]
pub(super) struct PgConnectionPoolStats {
    pub(super) max_size: usize,
    pub(super) connection_creations: u64,
    pub(super) checkouts: u64,
    pub(super) checkout_waits: u64,
    pub(super) discarded: u64,
    pub(super) total: usize,
}

struct PendingPgConnectionSlot {
    state: Arc<Mutex<PgConnectionPoolState>>,
    armed: bool,
}

impl PendingPgConnectionSlot {
    fn new(state: Arc<Mutex<PgConnectionPoolState>>) -> Self {
        Self { state, armed: true }
    }

    fn commit(mut self) {
        self.armed = false;
    }
}

impl Drop for PendingPgConnectionSlot {
    fn drop(&mut self) {
        if self.armed {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.total = state.total.saturating_sub(1);
        }
    }
}

impl PooledPgConnection {
    fn client_mut(&mut self) -> MetaResult<&mut tokio_postgres::Client> {
        let idle = self.idle.as_mut().ok_or_else(|| {
            MetaError::Storage("pooled postgres connection missing client".to_string())
        })?;
        Ok(&mut idle.client)
    }

    fn discard(mut self) {
        if let Some(idle) = self.idle.take() {
            self.pool.discard_idle(idle);
        }
    }
}

impl Drop for PooledPgConnection {
    fn drop(&mut self) {
        if let Some(idle) = self.idle.take() {
            if idle.client.is_closed() || idle.task.is_finished() {
                self.pool.discard_idle(idle);
            } else {
                self.pool.put_back_idle(idle);
            }
        }
    }
}

impl PgConnectionPool {
    pub(super) fn new(
        postgres_url: String,
        connect_timeout: Duration,
        checkout_timeout: Duration,
        max_size: usize,
    ) -> MetaResult<Self> {
        if max_size == 0 {
            return Err(MetaError::Storage(
                "postgres command pool size must be greater than zero".to_string(),
            ));
        }
        if max_size > TOKIO_MAX_SEMAPHORE_PERMITS {
            return Err(MetaError::Storage(format!(
                "postgres command pool size exceeds Tokio semaphore maximum ({TOKIO_MAX_SEMAPHORE_PERMITS})"
            )));
        }
        Ok(Self {
            postgres_url,
            connect_timeout,
            checkout_timeout,
            max_size,
            active_slots: Arc::new(Semaphore::new(max_size)),
            state: Arc::new(Mutex::new(PgConnectionPoolState::default())),
            connection_creations: AtomicU64::new(0),
            checkouts: AtomicU64::new(0),
            checkout_waits: AtomicU64::new(0),
            discarded: AtomicU64::new(0),
        })
    }

    #[cfg(test)]
    pub(super) fn stats(&self) -> PgConnectionPoolStats {
        PgConnectionPoolStats {
            max_size: self.max_size,
            connection_creations: self.connection_creations.load(Ordering::Relaxed),
            checkouts: self.checkouts.load(Ordering::Relaxed),
            checkout_waits: self.checkout_waits.load(Ordering::Relaxed),
            discarded: self.discarded.load(Ordering::Relaxed),
            total: self.state.lock().unwrap_or_else(|p| p.into_inner()).total,
        }
    }

    async fn checkout(self: &Arc<Self>, context: &'static str) -> MetaResult<PooledPgConnection> {
        let checkout = async {
            let permit = match self.active_slots.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    self.checkout_waits.fetch_add(1, Ordering::Relaxed);
                    self.active_slots
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| {
                            MetaError::Storage("postgres command pool is closed".to_string())
                        })?
                }
            };

            let pending_slot = {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                while let Some(connection) = state.idle.pop() {
                    if connection.client.is_closed() || connection.task.is_finished() {
                        state.total = state.total.saturating_sub(1);
                        self.discarded.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    self.checkouts.fetch_add(1, Ordering::Relaxed);
                    return Ok(PooledPgConnection {
                        idle: Some(connection),
                        pool: Arc::clone(self),
                        _active_permit: permit,
                    });
                }

                if state.total < self.max_size {
                    state.total += 1;
                    Some(PendingPgConnectionSlot::new(self.state.clone()))
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
                    self.checkouts.fetch_add(1, Ordering::Relaxed);
                    Ok(PooledPgConnection {
                        idle: Some(connection),
                        pool: Arc::clone(self),
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
                    PgTimeoutContext::Command(&format!("{context} checkout")),
                    self.checkout_timeout,
                )
            })?
    }

    async fn connect(&self) -> MetaResult<IdlePgConnection> {
        let connect = connect_studiofs_postgres(&self.postgres_url);
        let (client, connection) = tokio::time::timeout(self.connect_timeout, connect)
            .await
            .map_err(|_| {
                PgMetaEngine::pg_timeout_error(
                    PgTimeoutContext::Connection("connect"),
                    self.connect_timeout,
                )
            })??;
        self.connection_creations.fetch_add(1, Ordering::Relaxed);
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(IdlePgConnection { client, task })
    }

    fn put_back_idle(&self, connection: IdlePgConnection) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if connection.task.is_finished() || state.idle.len() >= self.max_size {
            state.total = state.total.saturating_sub(1);
            self.discarded.fetch_add(1, Ordering::Relaxed);
            connection.task.abort();
            return;
        }
        state.idle.push(connection);
    }

    fn discard_idle(&self, connection: IdlePgConnection) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.total = state.total.saturating_sub(1);
        self.discarded.fetch_add(1, Ordering::Relaxed);
        connection.task.abort();
    }
}

impl Drop for PgConnectionPool {
    fn drop(&mut self) {
        self.active_slots.close();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.total > state.idle.len() {
            warn!(
                checked_out = state.total - state.idle.len(),
                "postgres command pool dropped with checked-out connections"
            );
        }
        for connection in state.idle.drain(..) {
            connection.task.abort();
        }
        state.total = 0;
    }
}

struct _AssertPgConnectionPoolSendSync
where
    PgConnectionPool: Send + Sync;

impl PgRemoteRuntime {
    pub(super) fn new(name: &'static str) -> MetaResult<Self> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<RuntimeMessage>(128);
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
                rt.block_on(async move {
                    while let Some(message) = rx.recv().await {
                        match message {
                            RuntimeMessage::Run(job) => {
                                tokio::spawn(job);
                            }
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
        if tokio::runtime::Handle::try_current().is_ok() {
            tx.try_send(job).map_err(|e| {
                MetaError::Storage(format!("failed to submit postgres runtime job: {e}"))
            })?;
        } else {
            tx.blocking_send(job).map_err(|e| {
                MetaError::Storage(format!("failed to submit postgres runtime job: {e}"))
            })?;
        }
        result_rx.recv().map_err(|e| {
            MetaError::Storage(format!(
                "failed to receive result from postgres runtime: {e}"
            ))
        })?
    }
}

impl Drop for PgRemoteRuntime {
    fn drop(&mut self) {
        self.tx.lock().unwrap_or_else(|p| p.into_inner()).take();
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
    pub(super) fn classify_pg_error(
        context: impl Display,
        error: tokio_postgres::Error,
    ) -> MetaError {
        classify_postgres_error(context, error)
    }

    pub(super) fn pg_timeout_error(context: PgTimeoutContext<'_>, timeout: Duration) -> MetaError {
        let context = match context {
            PgTimeoutContext::Connection(context) | PgTimeoutContext::Command(context) => context,
        };
        let message = format!(
            "postgres {context} timed out after {} ms",
            timeout.as_millis()
        );
        MetaError::Timeout(message)
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
        let connect = connect_studiofs_postgres(&postgres_url);
        let (client, connection) = tokio::time::timeout(connect_timeout, connect)
            .await
            .map_err(|_| {
                Self::pg_timeout_error(
                    PgTimeoutContext::Connection(&format!("{context} connect")),
                    connect_timeout,
                )
            })??;

        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });

        let result = tokio::time::timeout(command_timeout, f(client))
            .await
            .map_err(|_| {
                Self::pg_timeout_error(PgTimeoutContext::Command(context), command_timeout)
            });

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
        let result = tokio::time::timeout(command_timeout, f(connection.client_mut()?))
            .await
            .map_err(|_| {
                Self::pg_timeout_error(PgTimeoutContext::Command(context), command_timeout)
            });

        match result {
            Ok(Ok(value)) => {
                drop(connection);
                Ok(value)
            }
            Ok(Err(error)) => {
                connection.discard();
                Err(error)
            }
            Err(error) => {
                connection.discard();
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
        // Foreground transactions keep their existing command timeout behavior.
        let cancel = WORKER_REMOTE_CANCELLATION
            .get()
            .then(|| self.background_workers.metadata_notify_cancel());
        let fut = async move {
            let Some(cancel) = cancel else {
                return fut.await;
            };
            tokio::select! {
                biased;
                () = cancel.cancelled() => Err(MetaError::WorkerShutdownCancelled),
                result = fut => result,
            }
        };
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
    fn prepare_for_close_cancels_inflight_remote_step() {
        let (_dir, engine) = temp_pg_engine_for_unit();
        let engine = Arc::new(engine);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let worker_engine = Arc::clone(&engine);
        let handle = std::thread::spawn(move || {
            let _scope = super::WorkerRemoteCancellationScope::enter();
            let result = worker_engine.run_async(async move {
                started_tx.send(()).expect("signal pending remote step");
                let _ = release_rx.await;
                Ok(())
            });
            result_tx.send(result).expect("send remote step result");
        });
        engine
            .background_workers
            .install_handle(crate::pg_engine::PgBackgroundWorker::Checkpoint, handle)
            .expect("install pending checkpoint worker");
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("remote step should start");

        let (closed_tx, closed_rx) = std::sync::mpsc::channel();
        let closing_engine = Arc::clone(&engine);
        let closer = std::thread::spawn(move || {
            closed_tx
                .send(
                    closing_engine
                        .background_workers
                        .shutdown(Duration::from_millis(200)),
                )
                .expect("send close result");
        });
        let close_result = closed_rx.recv_timeout(Duration::from_secs(1));
        // Release the controlled future even on the old unconditional-join path.
        let _ = release_tx.send(());
        let close_result = close_result.unwrap_or_else(|_| {
            closed_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("released close should finish")
        });
        closer.join().expect("join close caller");
        engine
            .background_workers
            .shutdown(Duration::from_secs(1))
            .expect("finish worker cleanup");
        let remote_result = result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("remote result");
        assert!(
            close_result.is_ok(),
            "cancellation should stop the pending worker: {close_result:?}"
        );
        assert!(
            matches!(remote_result, Err(MetaError::WorkerShutdownCancelled)),
            "the pending remote step must be cancelled before release"
        );
    }

    #[test]
    fn worker_shutdown_does_not_cancel_foreground_remote_operations() {
        let (_dir, engine) = temp_pg_engine_for_unit();
        engine.background_workers.signal_stop();
        assert_eq!(
            engine
                .run_async(async { Ok(7) })
                .expect("foreground result"),
            7
        );
    }

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
        let pool = Arc::new(
            PgConnectionPool::new(
                "postgres://10.255.255.1:5432/studiofs".to_string(),
                Duration::from_millis(1),
                Duration::from_millis(1),
                4,
            )
            .expect("non-zero command pool size"),
        );

        let result = runtime.block_on(pool.checkout("unit test connect timeout"));

        assert!(result.is_err(), "checkout should fail quickly");
        assert_eq!(
            pool.stats().total,
            0,
            "failed or canceled connection creation must release its reserved total slot"
        );
    }

    #[test]
    fn command_pool_cancelled_checkout_does_not_leak_total_slots() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let pool = Arc::new(
            PgConnectionPool::new(
                "postgres://10.255.255.1:5432/studiofs".to_string(),
                Duration::from_secs(30),
                Duration::from_secs(30),
                4,
            )
            .expect("non-zero command pool size"),
        );

        runtime.block_on(async {
            let checkout_pool = pool.clone();
            let handle = tokio::spawn(async move {
                let _ = checkout_pool.checkout("unit test canceled checkout").await;
            });

            let observed_reserved_slot = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if pool.stats().total == 1 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok();
            assert!(
                observed_reserved_slot,
                "checkout should reserve a connection slot before the test aborts it"
            );

            handle.abort();
            let _ = handle.await;
            tokio::task::yield_now().await;

            assert_eq!(
                pool.stats().total,
                0,
                "dropping a checkout future must release its reserved total slot"
            );
        });
    }

    #[test]
    fn command_pool_rejects_zero_capacity() {
        let result = PgConnectionPool::new(
            "postgres://127.0.0.1:1/studiofs".to_string(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            0,
        );

        assert!(
            result.is_err(),
            "a command pool must have a positive capacity"
        );
    }

    #[test]
    fn command_pool_rejects_capacity_above_semaphore_limit() {
        let result = PgConnectionPool::new(
            "postgres://127.0.0.1:1/studiofs".to_string(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            usize::MAX,
        );

        assert!(
            result.is_err(),
            "a command pool must reject capacities above Tokio's semaphore limit"
        );
    }
}
