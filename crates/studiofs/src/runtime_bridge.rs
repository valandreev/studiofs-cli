use std::future::Future;
use std::io;
use std::sync::{Mutex, OnceLock};

use tokio::runtime::{Runtime, RuntimeFlavor};

static CLI_RUNTIME: OnceLock<Result<Mutex<Runtime>, String>> = OnceLock::new();

fn global_runtime<E>() -> Result<&'static Mutex<Runtime>, E>
where
    E: From<io::Error>,
{
    match CLI_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map(Mutex::new)
            .map_err(|error| error.to_string())
    }) {
        Ok(runtime) => Ok(runtime),
        Err(message) => {
            Err(io::Error::other(format!("failed to build shared Tokio runtime: {message}")).into())
        }
    }
}

fn block_on_shared_runtime<T, E, F>(future: F) -> Result<T, E>
where
    T: Send,
    E: From<io::Error> + Send,
    F: Future<Output = Result<T, E>> + Send,
{
    let runtime = global_runtime::<E>()?;
    let guard = runtime
        .lock()
        .map_err(|_| io::Error::other("shared Tokio runtime mutex poisoned"))?;
    guard.block_on(future)
}

fn block_on_scoped_thread<T, E, F>(future: F) -> Result<T, E>
where
    T: Send,
    E: From<io::Error> + Send,
    F: Future<Output = Result<T, E>> + Send,
{
    std::thread::scope(|scope| {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        scope.spawn(move || {
            let _ = tx.send(block_on_shared_runtime(future));
        });
        rx.recv().map_err(|error| {
            io::Error::other(format!("failed to receive async task result: {error}"))
        })?
    })
}

pub fn block_on<T, E, F>(future: F) -> Result<T, E>
where
    T: Send,
    E: From<io::Error> + Send,
    F: Future<Output = Result<T, E>> + Send,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return match handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| handle.block_on(future)),
            RuntimeFlavor::CurrentThread => block_on_scoped_thread(future),
            _ => block_on_scoped_thread(future),
        };
    }

    block_on_shared_runtime(future)
}
