//! Split from advanced mounted workload implementation.

use super::*;

pub(in crate::scenario) fn open_handle_isolated_path(
    isolation: &ScenarioIsolation,
    legacy_path: &str,
) -> String {
    isolation.path(
        legacy_path
            .strip_prefix("open-handles/")
            .unwrap_or(legacy_path),
    )
}

pub(in crate::scenario) fn wait_for_mounted_initial_payloads_until(
    clients: &[ClientSpec],
    required_payloads: &[(&str, &[u8])],
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
) -> Result<(), ScenarioError> {
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    loop {
        let last_missing =
            missing_mounted_initial_payloads(clients, required_payloads, &probe_limiter);
        if last_missing.is_empty() {
            return Ok(());
        }
        if probe_limiter.is_poisoned() {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} mounted validation probe timed out; missing: {}",
                    last_missing.join(", ")
                ),
            )));
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not become visible before timeout; missing: {}",
                    last_missing.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn wait_for_mounted_paths_absent_until(
    clients: &[ClientSpec],
    absent_paths: &[&str],
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
) -> Result<(), ScenarioError> {
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    loop {
        let present = present_mounted_paths(clients, absent_paths, &probe_limiter);
        if present.is_empty() {
            return Ok(());
        }
        if probe_limiter.is_poisoned() {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} mounted validation probe timed out; still present: {}",
                    present.join(", ")
                ),
            )));
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not disappear before timeout; still present: {}",
                    present.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn present_mounted_paths(
    clients: &[ClientSpec],
    absent_paths: &[&str],
    probe_limiter: &MountedProbeLimiter,
) -> Vec<String> {
    let mut present = Vec::new();
    for client in clients {
        for relative_path in absent_paths {
            let client = client.clone();
            let relative_path = (*relative_path).to_string();
            let client_name = client.name.clone();
            let timeout_path = relative_path.clone();
            match run_mounted_probe_with_timeout(
                probe_limiter,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                move || present_mounted_path_probe(client, relative_path),
            ) {
                Ok(Some(present_path)) => present.push(present_path),
                Ok(None) => {}
                Err(error) => present.push(format!(
                    "{}:{} ({})",
                    client_name,
                    timeout_path,
                    error.detail()
                )),
            }
        }
    }
    present
}

pub(in crate::scenario) fn present_mounted_path_probe(
    client: ClientSpec,
    relative_path: String,
) -> Option<String> {
    let _ = refresh_relative_path_parent_contains_child(&client.mount_point, &relative_path);
    match fs::metadata(client.mount_point.join(&relative_path)) {
        Ok(_) => Some(format!("{}:{relative_path}", client.name)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => Some(format!("{}:{relative_path} ({error})", client.name)),
    }
}

pub(in crate::scenario) fn missing_mounted_initial_payloads(
    clients: &[ClientSpec],
    required_payloads: &[(&str, &[u8])],
    probe_limiter: &MountedProbeLimiter,
) -> Vec<String> {
    missing_mounted_initial_payloads_with_probe(
        clients,
        required_payloads,
        probe_limiter,
        MOUNTED_VALIDATION_PROBE_TIMEOUT,
        mounted_initial_payload_probe,
    )
}

pub(in crate::scenario) fn missing_mounted_initial_payloads_with_probe<F>(
    clients: &[ClientSpec],
    required_payloads: &[(&str, &[u8])],
    probe_limiter: &MountedProbeLimiter,
    probe_timeout: Duration,
    probe: F,
) -> Vec<String>
where
    F: Fn(ClientSpec, String, Vec<u8>) -> MountedPayloadProbeStatus + Copy + Send + 'static,
{
    let mut missing = Vec::new();
    for client in clients {
        for (relative_path, expected_payload) in required_payloads {
            let client = client.clone();
            let client_name = client.name.clone();
            let relative_path = (*relative_path).to_string();
            let expected_payload = expected_payload.to_vec();
            let timeout_path = relative_path.clone();
            match run_mounted_probe_with_timeout(probe_limiter, probe_timeout, move || {
                probe(client, relative_path, expected_payload)
            }) {
                Ok(MountedPayloadProbeStatus::Present) => {}
                Ok(MountedPayloadProbeStatus::Missing(path)) => missing.push(path),
                Err(error) => {
                    missing.push(format!("{client_name}:{timeout_path} ({})", error.detail()));
                }
            }
        }
    }
    missing
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::scenario) enum MountedPayloadProbeStatus {
    Present,
    Missing(String),
}

pub(in crate::scenario) fn mounted_initial_payload_probe(
    client: ClientSpec,
    relative_path: String,
    expected_payload: Vec<u8>,
) -> MountedPayloadProbeStatus {
    let path = client.mount_point.join(&relative_path);
    let parent_contains_child =
        refresh_relative_path_parent_contains_child(&client.mount_point, &relative_path);
    if !parent_contains_child {
        let metadata_detail = match fs::metadata(&path) {
            Ok(metadata) => format!("metadata_len={}", metadata.len()),
            Err(error) => format!("metadata_error={error}"),
        };
        return MountedPayloadProbeStatus::Missing(format!(
            "{}:{relative_path} (parent_contains_child=false; {metadata_detail})",
            client.name
        ));
    }
    match fs::read(&path) {
        Ok(payload) if payload == expected_payload => MountedPayloadProbeStatus::Present,
        Ok(payload) => MountedPayloadProbeStatus::Missing(format!(
            "{}:{relative_path} (parent_contains_child=true; expected {} bytes sha256={}, read {} bytes sha256={})",
            client.name,
            expected_payload.len(),
            bytes_sha256(&expected_payload),
            payload.len(),
            bytes_sha256(&payload)
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let metadata_detail = match fs::metadata(&path) {
                Ok(metadata) => format!("metadata_len={}", metadata.len()),
                Err(metadata_error) => format!("metadata_error={metadata_error}"),
            };
            MountedPayloadProbeStatus::Missing(format!(
                "{}:{relative_path} (parent_contains_child=true; read_error={error}; {metadata_detail})",
                client.name
            ))
        }
        Err(error) => {
            let metadata_detail = match fs::metadata(&path) {
                Ok(metadata) => format!("metadata_len={}", metadata.len()),
                Err(metadata_error) => format!("metadata_error={metadata_error}"),
            };
            MountedPayloadProbeStatus::Missing(format!(
                "{}:{relative_path} (parent_contains_child=true; read_error={error}; {metadata_detail})",
                client.name
            ))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::scenario) enum MountedProbeFailure {
    TimedOut {
        timeout: Duration,
    },
    WorkerExited,
    WorkerLimitReached {
        active_workers: usize,
        max_active_workers: usize,
    },
}

impl MountedProbeFailure {
    fn detail(self) -> String {
        match self {
            Self::TimedOut { timeout } => {
                format!("probe timed out after {} ms", timeout.as_millis())
            }
            Self::WorkerExited => "probe worker exited without result".to_string(),
            Self::WorkerLimitReached {
                active_workers,
                max_active_workers,
            } => format!(
                "probe skipped because a previous mounted probe timed out; active probe workers {active_workers}/{max_active_workers}"
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::scenario) struct MountedProbeLimiter {
    pub(in crate::scenario) active_workers: Arc<AtomicUsize>,
    pub(in crate::scenario) poisoned: Arc<AtomicBool>,
    pub(in crate::scenario) max_active_workers: usize,
}

impl MountedProbeLimiter {
    pub(in crate::scenario) fn new(max_active_workers: usize) -> Self {
        Self {
            active_workers: Arc::new(AtomicUsize::new(0)),
            poisoned: Arc::new(AtomicBool::new(false)),
            max_active_workers,
        }
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    fn mark_poisoned(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    fn try_acquire(&self) -> Result<MountedProbePermit, MountedProbeFailure> {
        if self.is_poisoned() {
            return Err(self.worker_limit_reached());
        }

        loop {
            let active_workers = self.active_workers.load(Ordering::Acquire);
            if active_workers >= self.max_active_workers {
                return Err(self.worker_limit_reached());
            }
            if self
                .active_workers
                .compare_exchange(
                    active_workers,
                    active_workers.saturating_add(1),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(MountedProbePermit {
                    active_workers: Arc::clone(&self.active_workers),
                });
            }
        }
    }

    fn worker_limit_reached(&self) -> MountedProbeFailure {
        MountedProbeFailure::WorkerLimitReached {
            active_workers: self.active_workers.load(Ordering::Acquire),
            max_active_workers: self.max_active_workers,
        }
    }
}

pub(in crate::scenario) struct MountedProbePermit {
    pub(in crate::scenario) active_workers: Arc<AtomicUsize>,
}

impl Drop for MountedProbePermit {
    fn drop(&mut self) {
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(in crate::scenario) fn run_mounted_probe_with_timeout<T, F>(
    probe_limiter: &MountedProbeLimiter,
    timeout: Duration,
    probe: F,
) -> Result<T, MountedProbeFailure>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let permit = probe_limiter.try_acquire()?;
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = probe();
        drop(permit);
        let _ = tx.send(result);
    });
    match rx.recv_timeout(timeout) {
        Ok(result) => Ok(result),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            probe_limiter.mark_poisoned();
            Err(MountedProbeFailure::TimedOut { timeout })
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(MountedProbeFailure::WorkerExited),
    }
}

pub(in crate::scenario) fn refresh_relative_path_parent_contains_child(
    mount_point: &Path,
    relative_path: &str,
) -> bool {
    let relative = Path::new(relative_path);
    let Some(file_name) = relative.file_name() else {
        return true;
    };
    let mut current = mount_point.to_path_buf();
    let _ = drain_directory(&current);
    let Some(parent) = relative.parent() else {
        return directory_contains_child(&current, file_name);
    };
    let mut components = parent.components().peekable();
    while let Some(component) = components.next() {
        current.push(component.as_os_str());
        if components.peek().is_some() {
            if !drain_directory(&current) {
                return false;
            }
        } else {
            return directory_contains_child(&current, file_name);
        }
    }
    directory_contains_child(&current, file_name)
}

pub(in crate::scenario) fn directory_contains_child(
    parent: &Path,
    file_name: &std::ffi::OsStr,
) -> bool {
    let Ok(entries) = bounded_mounted_read_dir_names(
        parent.to_path_buf(),
        format!("directory contains child {}", parent.display()),
        MOUNTED_VALIDATION_PROBE_TIMEOUT,
    ) else {
        return false;
    };
    for entry_name in entries {
        if entry_name == file_name {
            return true;
        }
    }
    false
}

pub(in crate::scenario) fn drain_directory(path: &Path) -> bool {
    bounded_mounted_read_dir_names(
        path.to_path_buf(),
        format!("drain directory {}", path.display()),
        MOUNTED_VALIDATION_PROBE_TIMEOUT,
    )
    .is_ok()
}

pub(in crate::scenario) fn wait_for_required_paths_visible_until(
    clients: &[ClientSpec],
    required_paths: &[&str],
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
) -> Result<(), ScenarioError> {
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    loop {
        let last_missing = missing_required_paths(clients, required_paths, &probe_limiter);
        if last_missing.is_empty() {
            return Ok(());
        }
        if probe_limiter.is_poisoned() {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} mounted validation probe timed out; missing: {}",
                    last_missing.join(", ")
                ),
            )));
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not become visible before timeout; missing: {}",
                    last_missing.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn wait_for_client_path_visible_until(
    client: &ClientSpec,
    relative_path: &str,
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
) -> Result<(), ScenarioError> {
    let probe_limiter = MountedProbeLimiter::new(1);
    loop {
        match client_path_missing(client, relative_path, &probe_limiter) {
            Ok(None) => return Ok(()),
            Ok(Some(missing)) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(ScenarioError::Io(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "{label} did not become visible before timeout; missing: {missing}"
                        ),
                    )));
                }
                thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
            }
            Err(error) => {
                return Err(ScenarioError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "{label} mounted validation probe timed out for {}:{relative_path}: {}",
                        client.name,
                        error.detail()
                    ),
                )));
            }
        }
    }
}

pub(in crate::scenario) fn wait_for_hotspot_helper_stat_until(
    context: &ScenarioContext,
    client: &ClientSpec,
    relative_path: &str,
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
    active_workers: Arc<AtomicUsize>,
) -> Result<(), ScenarioError> {
    let mut last_error = None;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not become visible before timeout for {}:{relative_path}; last error: {}",
                    client.name,
                    last_error.unwrap_or_else(|| "none".to_string())
                ),
            )));
        }

        let remaining = deadline.saturating_duration_since(now);
        match run_hotspot_mount_operation_with_timeout(
            context,
            client,
            "stat",
            relative_path,
            remaining,
            None,
            None,
            Arc::clone(&active_workers),
        ) {
            Ok(()) => return Ok(()),
            Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) => {
                return Err(error);
            }
            Err(error) => {
                last_error = Some(error.to_string());
                thread::sleep(
                    poll_interval.min(deadline.saturating_duration_since(Instant::now())),
                );
            }
        }
    }
}

pub(in crate::scenario) fn remaining_initial_validation_timeout(
    deadline: Instant,
    label: &str,
) -> Result<Duration, ScenarioError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(ScenarioError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("{label} became visible, but no time remained for convergence validation"),
        )))
    } else {
        Ok(remaining)
    }
}

pub(in crate::scenario) fn missing_required_paths(
    clients: &[ClientSpec],
    required_paths: &[&str],
    probe_limiter: &MountedProbeLimiter,
) -> Vec<String> {
    let mut missing = Vec::new();
    for client in clients {
        for relative_path in required_paths {
            let client = client.clone();
            let relative_path = (*relative_path).to_string();
            let client_name = client.name.clone();
            let timeout_path = relative_path.clone();
            match run_mounted_probe_with_timeout(
                probe_limiter,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                move || missing_required_path_probe(client, relative_path),
            ) {
                Ok(Some(missing_path)) => missing.push(missing_path),
                Ok(None) => {}
                Err(error) => missing.push(format!(
                    "{}:{} ({})",
                    client_name,
                    timeout_path,
                    error.detail()
                )),
            }
        }
    }
    missing
}

pub(in crate::scenario) fn client_path_missing(
    client: &ClientSpec,
    relative_path: &str,
    probe_limiter: &MountedProbeLimiter,
) -> Result<Option<String>, MountedProbeFailure> {
    let client = client.clone();
    let relative_path = relative_path.to_string();
    run_mounted_probe_with_timeout(probe_limiter, MOUNTED_VALIDATION_PROBE_TIMEOUT, move || {
        missing_required_path_probe(client, relative_path)
    })
}

pub(in crate::scenario) fn missing_required_path_probe(
    client: ClientSpec,
    relative_path: String,
) -> Option<String> {
    if !refresh_relative_path_parent_contains_child(&client.mount_point, &relative_path) {
        return Some(format!("{}:{relative_path}", client.name));
    }
    let path = client.mount_point.join(&relative_path);
    match fs::metadata(&path) {
        Ok(_) => None,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Some(format!("{}:{relative_path}", client.name))
        }
        Err(error) => Some(format!("{}:{relative_path} ({error})", client.name)),
    }
}
