//! Mounted action and helper-process operations shared by advanced scenarios.

use super::*;

static BOUNDED_MOUNTED_IO_POOL: OnceLock<BoundedMountedIoPool> = OnceLock::new();

#[cfg(windows)]
static HOTSPOT_HELPER_TIMED_IO_ACTIVE_WORKERS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone)]
pub(in crate::scenario) struct BoundedMountedIoPool {
    active_workers: Arc<AtomicUsize>,
    max_workers: usize,
}

impl BoundedMountedIoPool {
    pub(in crate::scenario) fn new(max_workers: usize) -> Self {
        Self {
            active_workers: Arc::new(AtomicUsize::new(0)),
            max_workers,
        }
    }

    pub(in crate::scenario) fn run<T>(
        &self,
        label: impl Into<String>,
        timeout: Duration,
        action: impl FnOnce() -> io::Result<T> + Send + 'static,
    ) -> Result<T, ScenarioError>
    where
        T: Send + 'static,
    {
        let label = label.into();
        let permit = self.try_acquire(&label, timeout)?;
        let (tx, rx) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("studiofs-e2e-mounted-io".to_string())
            .spawn(move || {
                let _permit = permit;
                let _ = tx.send(action());
            })
            .map_err(ScenarioError::Io)?;

        match rx.recv_timeout(timeout) {
            Ok(result) => {
                join_bounded_mounted_io_worker(worker)?;
                result.map_err(ScenarioError::Io)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let cancel_detail = cancel_bounded_mounted_io_worker(&worker);
                Err(ScenarioError::MountedIoTimeout {
                    label,
                    timeout_ms: timeout.as_millis(),
                    cancel_detail,
                })
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                join_bounded_mounted_io_worker(worker)?;
                Err(ScenarioError::Io(io::Error::other(
                    "bounded mounted I/O worker exited without result",
                )))
            }
        }
    }

    fn try_acquire(
        &self,
        label: &str,
        timeout: Duration,
    ) -> Result<BoundedMountedIoPermit, ScenarioError> {
        let mut current = self.active_workers.load(Ordering::Acquire);
        loop {
            if current >= self.max_workers {
                return Err(ScenarioError::MountedIoWorkerLimit {
                    label: label.to_string(),
                    active_workers: current,
                    max_workers: self.max_workers,
                    timeout_ms: timeout.as_millis(),
                });
            }
            match self.active_workers.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(BoundedMountedIoPermit {
                        active_workers: Arc::clone(&self.active_workers),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
}

struct BoundedMountedIoPermit {
    active_workers: Arc<AtomicUsize>,
}

impl Drop for BoundedMountedIoPermit {
    fn drop(&mut self) {
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

fn default_bounded_mounted_io_pool() -> &'static BoundedMountedIoPool {
    BOUNDED_MOUNTED_IO_POOL
        .get_or_init(|| BoundedMountedIoPool::new(BOUNDED_MOUNTED_IO_MAX_WORKERS))
}

pub(in crate::scenario) fn run_bounded_mounted_io<T>(
    label: impl Into<String>,
    timeout: Duration,
    action: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> Result<T, ScenarioError>
where
    T: Send + 'static,
{
    default_bounded_mounted_io_pool().run(label, timeout, action)
}

pub(in crate::scenario) fn bounded_mounted_read(
    path: PathBuf,
    label: impl Into<String>,
    timeout: Duration,
) -> Result<Vec<u8>, ScenarioError> {
    let label = label.into();
    run_bounded_mounted_io(label, timeout, move || fs::read(path))
}

pub(in crate::scenario) fn bounded_mounted_write(
    path: PathBuf,
    payload: Vec<u8>,
    label: impl Into<String>,
    timeout: Duration,
) -> Result<(), ScenarioError> {
    let label = label.into();
    run_bounded_mounted_io(label, timeout, move || fs::write(path, payload))
}

pub(in crate::scenario) fn bounded_mounted_metadata(
    path: PathBuf,
    label: impl Into<String>,
    timeout: Duration,
) -> Result<fs::Metadata, ScenarioError> {
    let label = label.into();
    run_bounded_mounted_io(label, timeout, move || fs::metadata(path))
}

pub(in crate::scenario) fn bounded_mounted_read_link(
    path: PathBuf,
    label: impl Into<String>,
    timeout: Duration,
) -> Result<PathBuf, ScenarioError> {
    let label = label.into();
    run_bounded_mounted_io(label, timeout, move || fs::read_link(path))
}

pub(in crate::scenario) fn bounded_mounted_read_dir_names(
    path: PathBuf,
    label: impl Into<String>,
    timeout: Duration,
) -> Result<Vec<std::ffi::OsString>, ScenarioError> {
    let label = label.into();
    run_bounded_mounted_io(label, timeout, move || {
        let mut names = Vec::new();
        for entry in fs::read_dir(path)? {
            names.push(entry?.file_name());
        }
        Ok(names)
    })
}

fn join_bounded_mounted_io_worker(worker: thread::JoinHandle<()>) -> Result<(), ScenarioError> {
    worker
        .join()
        .map_err(|_| ScenarioError::Io(io::Error::other("bounded mounted I/O worker panicked")))
}

#[cfg(windows)]
fn cancel_bounded_mounted_io_worker(worker: &thread::JoinHandle<()>) -> String {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawHandle as _;

    unsafe extern "system" {
        fn CancelSynchronousIo(hThread: *mut c_void) -> i32;
    }

    let ok = unsafe { CancelSynchronousIo(worker.as_raw_handle().cast()) };
    if ok == 0 {
        format!("CancelSynchronousIo failed: {}", io::Error::last_os_error())
    } else {
        "CancelSynchronousIo requested".to_string()
    }
}

#[cfg(not(windows))]
fn cancel_bounded_mounted_io_worker(_worker: &thread::JoinHandle<()>) -> String {
    "thread cancellation is not available on this platform".to_string()
}

#[cfg(windows)]
struct HotspotHelperTimedIoPermit {
    active_workers: &'static AtomicUsize,
}

#[cfg(windows)]
impl HotspotHelperTimedIoPermit {
    fn try_acquire(pending_label: &str, timeout: Duration) -> Result<Self, anyhow::Error> {
        let active_workers = &HOTSPOT_HELPER_TIMED_IO_ACTIVE_WORKERS;
        let mut current = active_workers.load(Ordering::Acquire);
        loop {
            if current >= HOTSPOT_IO_MAX_ACTIVE_WORKERS {
                return Err(anyhow::anyhow!(
                    "{HOTSPOT_HELPER_INTERNAL_TIMEOUT_MARKER}: hotspot helper worker limit reached; previous timed operation may still be blocked before {pending_label}; refusing to spawn another worker after {} ms timeout budget",
                    timeout.as_millis()
                ));
            }
            match active_workers.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(Self { active_workers }),
                Err(actual) => current = actual,
            }
        }
    }
}

#[cfg(windows)]
impl Drop for HotspotHelperTimedIoPermit {
    fn drop(&mut self) {
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(in crate::scenario) fn write_mounted_file_with_readback(
    path: PathBuf,
    payload: &[u8],
    label: &str,
    create_new: bool,
) -> Result<(), ScenarioError> {
    let write_path = path.clone();
    let write_payload = payload.to_vec();
    run_bounded_mounted_io(
        format!("{label}: write {}", path.display()),
        MOUNTED_WRITE_READBACK_TIMEOUT,
        move || {
            if let Some(parent) = write_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut options = mounted_write_barrier_open_options();
            options.create(true).truncate(true);
            if create_new {
                options.create_new(true);
            }
            let mut file = options.open(&write_path)?;
            file.write_all(&write_payload)?;
            file.sync_all()
        },
    )?;
    let readback = wait_for_mounted_payload_readback(
        &path,
        payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    if readback != payload {
        return Err(ScenarioError::Io(io::Error::other(format!(
            "{label} write readback mismatch for {}: expected {} bytes, read {} bytes",
            path.display(),
            payload.len(),
            readback.len()
        ))));
    }
    Ok(())
}

pub(in crate::scenario) fn write_mounted_file_without_barrier_with_readback(
    path: PathBuf,
    payload: &[u8],
    label: &str,
) -> Result<(), ScenarioError> {
    let write_path = path.clone();
    let write_payload = payload.to_vec();
    run_bounded_mounted_io(
        format!("{label}: write {}", path.display()),
        MOUNTED_WRITE_READBACK_TIMEOUT,
        move || write_parented(write_path, &write_payload),
    )?;
    let readback = wait_for_mounted_payload_readback(
        &path,
        payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    if readback != payload {
        return Err(ScenarioError::Io(io::Error::other(format!(
            "{label} write readback mismatch for {}: expected {} bytes, read {} bytes",
            path.display(),
            payload.len(),
            readback.len()
        ))));
    }
    Ok(())
}

pub(in crate::scenario) fn remove_mounted_file(path: PathBuf) -> Result<(), ScenarioError> {
    run_bounded_mounted_io(
        format!("remove {}", path.display()),
        MOUNTED_WRITE_READBACK_TIMEOUT,
        move || fs::remove_file(path),
    )?;
    Ok(())
}

pub(in crate::scenario) fn replace_mounted_file_symlink(
    path: PathBuf,
    target: &str,
) -> Result<(), ScenarioError> {
    retry_mounted_io(
        || fs::remove_file(&path),
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    create_mounted_file_symlink(path, target)?;
    Ok(())
}

pub(in crate::scenario) fn create_mounted_file_symlink(
    path: PathBuf,
    target: &str,
) -> io::Result<()> {
    retry_mounted_io(
        || {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            create_platform_file_symlink(Path::new(target), &path)
        },
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )
}

#[cfg(windows)]
pub(in crate::scenario) fn create_platform_file_symlink(
    target: &Path,
    link: &Path,
) -> io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[cfg(unix)]
pub(in crate::scenario) fn create_platform_file_symlink(
    target: &Path,
    link: &Path,
) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

pub(in crate::scenario) fn set_mounted_readonly(path: PathBuf) -> io::Result<()> {
    retry_mounted_io(
        || {
            let mut permissions = fs::metadata(&path)?.permissions();
            permissions.set_readonly(true);
            fs::set_permissions(&path, permissions)
        },
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )
}

pub(in crate::scenario) fn retry_mounted_io(
    mut action: impl FnMut() -> io::Result<()>,
    timeout: Duration,
    poll_interval: Duration,
) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match action() {
            Ok(()) => return Ok(()),
            Err(error) if is_transient_mounted_io_error(&error) && Instant::now() < deadline => {
                thread::sleep(poll_interval);
            }
            Err(error) => return Err(error),
        }
    }
}

pub(in crate::scenario) fn is_transient_mounted_io_error(error: &io::Error) -> bool {
    #[cfg(windows)]
    {
        const ERROR_GEN_FAILURE: i32 = 31;
        error.raw_os_error() == Some(ERROR_GEN_FAILURE)
    }
    #[cfg(not(windows))]
    {
        let _ = error;
        false
    }
}

pub(in crate::scenario) fn is_supported_symlink_skip_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::PermissionDenied | io::ErrorKind::Unsupported
    ) || is_transient_mounted_io_error(error)
}

pub(in crate::scenario) fn write_parented(path: PathBuf, payload: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;
    file.write_all(payload)?;
    file.sync_all()
}

/// Executes one mounted hotspot filesystem operation in a helper process.
pub fn run_hotspot_op_helper_with_timeout(
    operation: &str,
    mount_point: PathBuf,
    path: &str,
    target: Option<&str>,
    payload: Option<&Path>,
    progress: Option<&Path>,
    timeout: Option<Duration>,
) -> Result<(), anyhow::Error> {
    #[cfg(windows)]
    if let Some(timeout) = timeout {
        return run_hotspot_op_helper_with_windows_timeout(
            operation,
            mount_point,
            path,
            target,
            payload,
            progress,
            timeout,
        );
    }

    let _ = timeout;
    run_hotspot_op_helper(operation, mount_point, path, target, payload, progress)
}

/// Executes one mounted sparse filesystem operation in a helper process.
pub fn run_sparse_op_helper_with_timeout(
    operation: &str,
    mount_point: PathBuf,
    path: &str,
    offset: Option<u64>,
    len: Option<u64>,
    payload: Option<&Path>,
    progress: Option<&Path>,
    timeout: Option<Duration>,
) -> Result<(), anyhow::Error> {
    let _ = timeout;
    run_sparse_op_helper(operation, mount_point, path, offset, len, payload, progress)
}

#[cfg(windows)]
pub(in crate::scenario) fn run_hotspot_op_helper_with_windows_timeout(
    operation: &str,
    mount_point: PathBuf,
    path: &str,
    target: Option<&str>,
    payload: Option<&Path>,
    progress: Option<&Path>,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let operation_for_worker = operation.to_string();
    let operation_for_timeout = operation.to_string();
    let path_for_worker = path.to_string();
    let path_for_timeout = path.to_string();
    let target_for_worker = target.map(str::to_string);
    let target_for_timeout = target.map(str::to_string);
    let payload_for_worker = payload.map(Path::to_path_buf);
    let progress_for_worker = progress.map(Path::to_path_buf);
    let progress_for_timeout = progress.map(Path::to_path_buf);
    let mount_for_worker = mount_point.clone();
    let mount_for_timeout = mount_point;
    let (tx, rx) = mpsc::channel();
    let pending_label = format!(
        "{operation_for_timeout} {}",
        mount_for_timeout.join(&path_for_timeout).display()
    );
    let worker_permit = HotspotHelperTimedIoPermit::try_acquire(&pending_label, timeout)?;

    let worker = thread::Builder::new()
        .name("studiofs-e2e-hotspot-helper-io".to_string())
        .spawn(move || {
            let _worker_permit = worker_permit;
            let result = run_hotspot_op_helper(
                &operation_for_worker,
                mount_for_worker,
                &path_for_worker,
                target_for_worker.as_deref(),
                payload_for_worker.as_deref(),
                progress_for_worker.as_deref(),
            );
            let _ = tx.send(result);
        })?;

    match rx.recv_timeout(timeout) {
        Ok(result) => {
            join_hotspot_helper_worker(worker)?;
            result
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            let mounted_path = mount_for_timeout.join(&path_for_timeout);
            let _ = record_hotspot_helper_progress(
                progress_for_timeout.as_deref(),
                "timeout_cancel_requested",
                &operation_for_timeout,
                &mounted_path,
                target_for_timeout.as_deref(),
                None,
            );
            let cancel_result = cancel_hotspot_helper_worker_io(&worker);
            let cancel_detail = match cancel_result {
                Ok(()) => "CancelSynchronousIo requested".to_string(),
                Err(error) => format!("CancelSynchronousIo failed: {error}"),
            };
            match rx.recv_timeout(HOTSPOT_HELPER_CANCEL_GRACE) {
                Ok(result) => {
                    join_hotspot_helper_worker(worker)?;
                    Err(anyhow::anyhow!(
                        "{HOTSPOT_HELPER_INTERNAL_TIMEOUT_MARKER}: operation timed out after {} ms; {cancel_detail}; result after cancellation: {}",
                        timeout.as_millis(),
                        match result {
                            Ok(()) => "completed".to_string(),
                            Err(error) => error.to_string(),
                        }
                    ))
                }
                Err(_) => Err(anyhow::anyhow!(
                    "{HOTSPOT_HELPER_INTERNAL_TIMEOUT_MARKER}: operation timed out after {} ms; {cancel_detail}; worker did not finish within {} ms",
                    timeout.as_millis(),
                    HOTSPOT_HELPER_CANCEL_GRACE.as_millis()
                )),
            }
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            join_hotspot_helper_worker(worker)?;
            Err(anyhow::anyhow!(
                "hotspot helper worker exited without result"
            ))
        }
    }
}

#[cfg(windows)]
pub(in crate::scenario) fn join_hotspot_helper_worker(
    worker: thread::JoinHandle<()>,
) -> Result<(), anyhow::Error> {
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("hotspot helper worker panicked"))
}

#[cfg(windows)]
pub(in crate::scenario) fn cancel_hotspot_helper_worker_io(
    worker: &thread::JoinHandle<()>,
) -> io::Result<()> {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawHandle as _;

    unsafe extern "system" {
        fn CancelSynchronousIo(hThread: *mut c_void) -> i32;
    }

    let ok = unsafe { CancelSynchronousIo(worker.as_raw_handle().cast()) };
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Executes one mounted hotspot filesystem operation without an internal timeout guard.
pub fn run_hotspot_op_helper(
    operation: &str,
    mount_point: PathBuf,
    path: &str,
    target: Option<&str>,
    payload: Option<&Path>,
    progress: Option<&Path>,
) -> Result<(), anyhow::Error> {
    let path = mount_point.join(path);
    record_hotspot_helper_progress(progress, "started", operation, &path, target, None)?;
    match operation {
        "mkdir" => {
            record_hotspot_helper_progress(
                progress,
                "before_create_dir_all",
                operation,
                &path,
                target,
                None,
            )?;
            fs::create_dir_all(&path)?;
            record_hotspot_helper_progress(
                progress,
                "after_create_dir_all",
                operation,
                &path,
                target,
                None,
            )?;
        }
        "create" => {
            let payload_path = payload.ok_or_else(|| anyhow::anyhow!("missing payload"))?;
            record_hotspot_helper_progress(
                progress,
                "before_read_payload",
                operation,
                &path,
                target,
                None,
            )?;
            let payload = fs::read(payload_path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_payload",
                operation,
                &path,
                target,
                Some(payload.len()),
            )?;
            write_parented_with_barrier_progress(
                &path,
                &payload,
                progress,
                operation,
                target,
                BarrierWriteMode::CreateNew,
            )?;
        }
        "overwrite-barrier" => {
            let payload_path = payload.ok_or_else(|| anyhow::anyhow!("missing payload"))?;
            record_hotspot_helper_progress(
                progress,
                "before_read_payload",
                operation,
                &path,
                target,
                None,
            )?;
            let payload = fs::read(payload_path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_payload",
                operation,
                &path,
                target,
                Some(payload.len()),
            )?;
            write_parented_with_barrier_progress(
                &path,
                &payload,
                progress,
                operation,
                target,
                BarrierWriteMode::Overwrite,
            )?;
        }
        "write" | "overwrite" => {
            let payload_path = payload.ok_or_else(|| anyhow::anyhow!("missing payload"))?;
            record_hotspot_helper_progress(
                progress,
                "before_read_payload",
                operation,
                &path,
                target,
                None,
            )?;
            let payload = fs::read(payload_path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_payload",
                operation,
                &path,
                target,
                Some(payload.len()),
            )?;
            write_parented_with_progress(&path, &payload, progress, operation, target)?;
        }
        "append" => {
            let payload_path = payload.ok_or_else(|| anyhow::anyhow!("missing payload"))?;
            record_hotspot_helper_progress(
                progress,
                "before_read_payload",
                operation,
                &path,
                target,
                None,
            )?;
            let payload = fs::read(payload_path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_payload",
                operation,
                &path,
                target,
                Some(payload.len()),
            )?;
            append_payload_with_progress(&path, &payload, progress, operation, target)?;
        }
        "delete" | "unlink" => {
            record_hotspot_helper_progress(
                progress,
                "before_remove_file",
                operation,
                &path,
                target,
                None,
            )?;
            fs::remove_file(&path)?;
            record_hotspot_helper_progress(
                progress,
                "after_remove_file",
                operation,
                &path,
                target,
                None,
            )?;
        }
        "rmdir" => {
            record_hotspot_helper_progress(
                progress,
                "before_remove_dir",
                operation,
                &path,
                target,
                None,
            )?;
            fs::remove_dir(&path)?;
            record_hotspot_helper_progress(
                progress,
                "after_remove_dir",
                operation,
                &path,
                target,
                None,
            )?;
        }
        "rename" => {
            let target = target.ok_or_else(|| anyhow::anyhow!("missing rename target"))?;
            record_hotspot_helper_progress(
                progress,
                "before_rename",
                operation,
                &path,
                Some(target),
                None,
            )?;
            fs::rename(&path, mount_point.join(target))?;
            record_hotspot_helper_progress(
                progress,
                "after_rename",
                operation,
                &path,
                Some(target),
                None,
            )?;
        }
        "readdir" => {
            record_hotspot_helper_progress(
                progress,
                "before_read_dir",
                operation,
                &path,
                target,
                None,
            )?;
            fs::read_dir(&path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_dir",
                operation,
                &path,
                target,
                None,
            )?;
        }
        "stat" => {
            record_hotspot_helper_progress(
                progress,
                "before_metadata",
                operation,
                &path,
                target,
                None,
            )?;
            fs::metadata(&path)?;
            record_hotspot_helper_progress(
                progress,
                "after_metadata",
                operation,
                &path,
                target,
                None,
            )?;
        }
        "read-exact" => {
            let payload_path = payload.ok_or_else(|| anyhow::anyhow!("missing payload"))?;
            record_hotspot_helper_progress(
                progress,
                "before_read_expected_payload",
                operation,
                &path,
                target,
                None,
            )?;
            let expected = fs::read(payload_path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_expected_payload",
                operation,
                &path,
                target,
                Some(expected.len()),
            )?;
            record_hotspot_helper_progress(
                progress,
                "before_read_mounted_payload",
                operation,
                &path,
                target,
                None,
            )?;
            let actual = fs::read(&path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_mounted_payload",
                operation,
                &path,
                target,
                Some(actual.len()),
            )?;
            if actual != expected {
                anyhow::bail!(
                    "read-exact mismatch for {}: expected {} bytes, read {} bytes",
                    path.display(),
                    expected.len(),
                    actual.len()
                );
            }
        }
        other => anyhow::bail!("unsupported hotspot operation: {other}"),
    }
    record_hotspot_helper_progress(progress, "completed", operation, &path, target, None)?;
    Ok(())
}

/// Executes one mounted sparse filesystem operation without an internal timeout guard.
pub fn run_sparse_op_helper(
    operation: &str,
    mount_point: PathBuf,
    path: &str,
    offset: Option<u64>,
    len: Option<u64>,
    payload: Option<&Path>,
    progress: Option<&Path>,
) -> Result<(), anyhow::Error> {
    let path = mount_point.join(path);
    record_hotspot_helper_progress(
        progress,
        "started",
        operation,
        &path,
        None,
        len.map(|v| v as usize),
    )?;
    match operation {
        "write-range" => {
            let offset = offset.ok_or_else(|| anyhow::anyhow!("missing offset"))?;
            let payload_path = payload.ok_or_else(|| anyhow::anyhow!("missing payload"))?;
            record_hotspot_helper_progress(
                progress,
                "before_read_payload",
                operation,
                &path,
                None,
                None,
            )?;
            let payload = fs::read(payload_path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_payload",
                operation,
                &path,
                None,
                Some(payload.len()),
            )?;
            write_sparse_range_with_progress(&path, offset, &payload, progress, operation)?;
        }
        "truncate" => {
            let len = len.ok_or_else(|| anyhow::anyhow!("missing len"))?;
            truncate_sparse_with_progress(&path, len, progress, operation)?;
        }
        "read-range-exact" => {
            let offset = offset.ok_or_else(|| anyhow::anyhow!("missing offset"))?;
            let payload_path = payload.ok_or_else(|| anyhow::anyhow!("missing payload"))?;
            record_hotspot_helper_progress(
                progress,
                "before_read_expected_payload",
                operation,
                &path,
                None,
                None,
            )?;
            let expected = fs::read(payload_path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_expected_payload",
                operation,
                &path,
                None,
                Some(expected.len()),
            )?;
            let actual = read_sparse_range_with_progress(
                &path,
                offset,
                expected.len(),
                progress,
                operation,
            )?;
            if actual != expected {
                anyhow::bail!(
                    "read-range-exact mismatch for {}@{}: expected {} bytes, read {} bytes",
                    path.display(),
                    offset,
                    expected.len(),
                    actual.len()
                );
            }
        }
        "read-range-zero" => {
            let offset = offset.ok_or_else(|| anyhow::anyhow!("missing offset"))?;
            let len = len.ok_or_else(|| anyhow::anyhow!("missing len"))?;
            let len = usize::try_from(len)
                .map_err(|_| anyhow::anyhow!("sparse read length is too large"))?;
            let actual = read_sparse_range_with_progress(&path, offset, len, progress, operation)?;
            if !actual.iter().all(|byte| *byte == 0) {
                anyhow::bail!(
                    "read-range-zero mismatch for {}@{}: expected {} zero bytes",
                    path.display(),
                    offset,
                    len
                );
            }
        }
        "read-all-exact" => {
            let payload_path = payload.ok_or_else(|| anyhow::anyhow!("missing payload"))?;
            record_hotspot_helper_progress(
                progress,
                "before_read_expected_payload",
                operation,
                &path,
                None,
                None,
            )?;
            let expected = fs::read(payload_path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_expected_payload",
                operation,
                &path,
                None,
                Some(expected.len()),
            )?;
            record_hotspot_helper_progress(
                progress,
                "before_read_all",
                operation,
                &path,
                None,
                None,
            )?;
            let actual = fs::read(&path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_all",
                operation,
                &path,
                None,
                Some(actual.len()),
            )?;
            if actual != expected {
                anyhow::bail!(
                    "read-all-exact mismatch for {}: expected {} bytes, read {} bytes",
                    path.display(),
                    expected.len(),
                    actual.len()
                );
            }
        }
        other => anyhow::bail!("unsupported sparse operation: {other}"),
    }
    record_hotspot_helper_progress(
        progress,
        "completed",
        operation,
        &path,
        None,
        len.map(|v| v as usize),
    )?;
    Ok(())
}

/// Executes the mounted open-handle mutation sequence in a helper process.
pub fn run_open_handle_op_helper(
    client_a_mount: PathBuf,
    client_b_mount: PathBuf,
    unlink_path: &str,
    rename_source: &str,
    rename_dest: &str,
    append_payload: &Path,
    progress: Option<&Path>,
) -> Result<(), anyhow::Error> {
    let operation = "open-handle";
    let client_a_unlink_path = client_a_mount.join(unlink_path);
    let client_b_unlink_path = client_b_mount.join(unlink_path);
    let client_b_rename_source = client_b_mount.join(rename_source);
    let client_b_rename_dest = client_b_mount.join(rename_dest);
    record_hotspot_helper_progress(
        progress,
        "started",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_read_payload",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    let append_payload = fs::read(append_payload)?;
    record_hotspot_helper_progress(
        progress,
        "after_read_payload",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        Some(append_payload.len()),
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_open_old_handle",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    let mut handle = OpenOptions::new()
        .append(true)
        .open(&client_a_unlink_path)?;
    record_hotspot_helper_progress(
        progress,
        "after_open_old_handle",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_remove_file",
        operation,
        &client_b_unlink_path,
        Some(rename_dest),
        None,
    )?;
    fs::remove_file(&client_b_unlink_path)?;
    record_hotspot_helper_progress(
        progress,
        "after_remove_file",
        operation,
        &client_b_unlink_path,
        Some(rename_dest),
        None,
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_rename",
        operation,
        &client_b_rename_source,
        Some(rename_dest),
        None,
    )?;
    fs::rename(&client_b_rename_source, &client_b_rename_dest)?;
    record_hotspot_helper_progress(
        progress,
        "after_rename",
        operation,
        &client_b_rename_source,
        Some(rename_dest),
        None,
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_write_all",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        Some(append_payload.len()),
    )?;
    handle.write_all(&append_payload)?;
    record_hotspot_helper_progress(
        progress,
        "after_write_all",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        Some(append_payload.len()),
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_sync_all",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    let sync_retries = sync_open_handle_with_retry(
        || handle.sync_all(),
        OPEN_HANDLE_SYNC_RETRY_TIMEOUT,
        OPEN_HANDLE_SYNC_RETRY_POLL_INTERVAL,
    )?;
    if sync_retries > 0 {
        record_hotspot_helper_progress(
            progress,
            "after_sync_all_retry",
            operation,
            &client_a_unlink_path,
            Some(rename_dest),
            None,
        )?;
    }
    record_hotspot_helper_progress(
        progress,
        "after_sync_all",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    drop(handle);
    record_hotspot_helper_progress(
        progress,
        "completed",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    Ok(())
}

pub(in crate::scenario) fn sync_open_handle_with_retry(
    mut sync_all: impl FnMut() -> io::Result<()>,
    timeout: Duration,
    poll_interval: Duration,
) -> io::Result<usize> {
    let deadline = Instant::now() + timeout;
    let mut retries = 0;
    loop {
        match sync_all() {
            Ok(()) => return Ok(retries),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(error);
                }
                retries += 1;
                thread::sleep(poll_interval.min(remaining));
            }
            Err(error) => return Err(error),
        }
    }
}

pub(in crate::scenario) fn write_parented_with_progress(
    path: &Path,
    payload: &[u8],
    progress: Option<&Path>,
    operation: &str,
    target: Option<&str>,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        record_hotspot_helper_progress(
            progress,
            "before_create_parent",
            operation,
            path,
            target,
            None,
        )?;
        fs::create_dir_all(parent)?;
        record_hotspot_helper_progress(
            progress,
            "after_create_parent",
            operation,
            path,
            target,
            None,
        )?;
    }
    record_hotspot_helper_progress(
        progress,
        "before_open",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    record_hotspot_helper_progress(
        progress,
        "after_open",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_write",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    file.write_all(payload)?;
    record_hotspot_helper_progress(
        progress,
        "after_write",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(progress, "before_sync_all", operation, path, target, None)?;
    file.sync_all()?;
    record_hotspot_helper_progress(progress, "after_sync_all", operation, path, target, None)
}

pub(in crate::scenario) enum BarrierWriteMode {
    CreateNew,
    Overwrite,
}

pub(in crate::scenario) fn write_parented_with_barrier_progress(
    path: &Path,
    payload: &[u8],
    progress: Option<&Path>,
    operation: &str,
    target: Option<&str>,
    mode: BarrierWriteMode,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        record_hotspot_helper_progress(
            progress,
            "before_create_parent",
            operation,
            path,
            target,
            None,
        )?;
        fs::create_dir_all(parent)?;
        record_hotspot_helper_progress(
            progress,
            "after_create_parent",
            operation,
            path,
            target,
            None,
        )?;
    }
    record_hotspot_helper_progress(
        progress,
        "before_open_barrier_write",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    let mut options = mounted_write_barrier_open_options();
    match mode {
        BarrierWriteMode::CreateNew => {
            options.create_new(true);
        }
        BarrierWriteMode::Overwrite => {
            options.create(true).truncate(true).read(true);
        }
    }
    let mut file = options.open(path)?;
    record_hotspot_helper_progress(
        progress,
        "after_open_barrier_write",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_write_all",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    file.write_all(payload)?;
    record_hotspot_helper_progress(
        progress,
        "after_write_all",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(progress, "before_sync_all", operation, path, target, None)?;
    file.sync_all()?;
    record_hotspot_helper_progress(progress, "after_sync_all", operation, path, target, None)?;
    drop(file);
    record_hotspot_helper_progress(progress, "before_readback", operation, path, target, None)?;
    let readback = wait_for_mounted_payload_readback(
        path,
        payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )
    .map_err(io::Error::other)?;
    record_hotspot_helper_progress(
        progress,
        "after_readback",
        operation,
        path,
        target,
        Some(readback.len()),
    )
}

pub(in crate::scenario) fn append_payload_with_progress(
    path: &Path,
    payload: &[u8],
    progress: Option<&Path>,
    operation: &str,
    target: Option<&str>,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        record_hotspot_helper_progress(
            progress,
            "before_create_parent",
            operation,
            path,
            target,
            None,
        )?;
        fs::create_dir_all(parent)?;
        record_hotspot_helper_progress(
            progress,
            "after_create_parent",
            operation,
            path,
            target,
            None,
        )?;
    }
    record_hotspot_helper_progress(
        progress,
        "before_open_append",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    record_hotspot_helper_progress(
        progress,
        "after_open_append",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_write_all",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    file.write_all(payload)?;
    record_hotspot_helper_progress(
        progress,
        "after_write_all",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(progress, "before_sync_all", operation, path, target, None)?;
    file.sync_all()?;
    record_hotspot_helper_progress(progress, "after_sync_all", operation, path, target, None)
}

pub(in crate::scenario) fn write_sparse_range_with_progress(
    path: &Path,
    offset: u64,
    payload: &[u8],
    progress: Option<&Path>,
    operation: &str,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        record_hotspot_helper_progress(
            progress,
            "before_create_parent",
            operation,
            path,
            None,
            None,
        )?;
        fs::create_dir_all(parent)?;
        record_hotspot_helper_progress(
            progress,
            "after_create_parent",
            operation,
            path,
            None,
            None,
        )?;
    }
    record_hotspot_helper_progress(
        progress,
        "before_open_sparse_write",
        operation,
        path,
        None,
        Some(payload.len()),
    )?;
    let mut options = mounted_write_barrier_open_options();
    let mut file = options.create(true).read(true).open(path)?;
    record_hotspot_helper_progress(
        progress,
        "after_open_sparse_write",
        operation,
        path,
        None,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(progress, "before_seek", operation, path, None, None)?;
    file.seek(SeekFrom::Start(offset))?;
    record_hotspot_helper_progress(progress, "after_seek", operation, path, None, None)?;
    record_hotspot_helper_progress(
        progress,
        "before_write_all",
        operation,
        path,
        None,
        Some(payload.len()),
    )?;
    file.write_all(payload)?;
    record_hotspot_helper_progress(
        progress,
        "after_write_all",
        operation,
        path,
        None,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(progress, "before_sync_all", operation, path, None, None)?;
    file.sync_all()?;
    record_hotspot_helper_progress(progress, "after_sync_all", operation, path, None, None)
}

pub(in crate::scenario) fn truncate_sparse_with_progress(
    path: &Path,
    len: u64,
    progress: Option<&Path>,
    operation: &str,
) -> io::Result<()> {
    record_hotspot_helper_progress(
        progress,
        "before_open_sparse_truncate",
        operation,
        path,
        None,
        None,
    )?;
    let options = mounted_write_barrier_open_options();
    let file = options.open(path)?;
    record_hotspot_helper_progress(
        progress,
        "after_open_sparse_truncate",
        operation,
        path,
        None,
        None,
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_set_len",
        operation,
        path,
        None,
        Some(len as usize),
    )?;
    file.set_len(len)?;
    record_hotspot_helper_progress(
        progress,
        "after_set_len",
        operation,
        path,
        None,
        Some(len as usize),
    )?;
    record_hotspot_helper_progress(progress, "before_sync_all", operation, path, None, None)?;
    file.sync_all()?;
    record_hotspot_helper_progress(progress, "after_sync_all", operation, path, None, None)
}

pub(in crate::scenario) fn read_sparse_range_with_progress(
    path: &Path,
    offset: u64,
    len: usize,
    progress: Option<&Path>,
    operation: &str,
) -> io::Result<Vec<u8>> {
    record_hotspot_helper_progress(
        progress,
        "before_open_sparse_read",
        operation,
        path,
        None,
        Some(len),
    )?;
    let mut file = fs::File::open(path)?;
    record_hotspot_helper_progress(
        progress,
        "after_open_sparse_read",
        operation,
        path,
        None,
        Some(len),
    )?;
    record_hotspot_helper_progress(progress, "before_seek", operation, path, None, None)?;
    file.seek(SeekFrom::Start(offset))?;
    record_hotspot_helper_progress(progress, "after_seek", operation, path, None, None)?;
    record_hotspot_helper_progress(
        progress,
        "before_read_exact",
        operation,
        path,
        None,
        Some(len),
    )?;
    let mut buffer = vec![0; len];
    file.read_exact(&mut buffer)?;
    record_hotspot_helper_progress(
        progress,
        "after_read_exact",
        operation,
        path,
        None,
        Some(len),
    )?;
    Ok(buffer)
}

pub(in crate::scenario) fn record_hotspot_helper_progress(
    progress: Option<&Path>,
    phase: &str,
    operation: &str,
    path: &Path,
    target: Option<&str>,
    payload_len: Option<usize>,
) -> io::Result<()> {
    let Some(progress) = progress else {
        return Ok(());
    };
    if let Some(parent) = progress.parent() {
        fs::create_dir_all(parent)?;
    }
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(progress)?;
    let mut line = serde_json::to_vec(&json!({
        "timestamp_ms": timestamp_ms,
        "phase": phase,
        "operation": operation,
        "path": path.display().to_string(),
        "target": target,
        "payload_len": payload_len,
    }))
    .map_err(io::Error::other)?;
    line.push(b'\n');
    file.write_all(&line)?;
    file.flush()
}

pub(in crate::scenario) fn sanitize_hotspot_progress_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    sanitized.trim_matches('_').chars().take(80).collect()
}
