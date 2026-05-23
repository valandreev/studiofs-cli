//! Best-effort process diagnostics captured before forced termination.

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
#[cfg(not(test))]
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
#[cfg(not(test))]
use std::time::{Duration, Instant};

use chrono::{SecondsFormat, Utc};
use serde::Serialize;

#[cfg(not(test))]
const PROCESS_DUMP_HELPER_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
thread_local! {
    static TEST_PROCESS_DIAGNOSTICS_DIR: std::cell::RefCell<Option<PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

static PROCESS_DIAGNOSTICS_DIR: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
static PROCESS_DIAGNOSTICS_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone)]
pub(crate) struct ProcessDiagnosticArtifact {
    pub(crate) metadata_path: PathBuf,
    pub(crate) dump_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct ProcessDiagnosticPaths {
    file_stem: String,
    metadata_path: PathBuf,
    dump_path: PathBuf,
}

#[derive(Serialize)]
struct ProcessDiagnosticMetadata<'a> {
    timestamp: String,
    pid: u32,
    label: &'a str,
    status: &'a str,
    dump_path: Option<String>,
    dump_error: Option<String>,
}

pub(crate) fn set_process_diagnostics_dir(path: PathBuf) -> io::Result<()> {
    fs::create_dir_all(&path)?;
    #[cfg(test)]
    TEST_PROCESS_DIAGNOSTICS_DIR.with(|configured| {
        *configured.borrow_mut() = Some(path.clone());
    });
    let lock = PROCESS_DIAGNOSTICS_DIR.get_or_init(|| Mutex::new(None));
    let mut configured = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    *configured = Some(path);
    Ok(())
}

pub(crate) fn capture_process_diagnostics(
    pid: u32,
    label: &str,
) -> io::Result<Option<ProcessDiagnosticArtifact>> {
    let Some(output_dir) = configured_process_diagnostics_dir() else {
        return Ok(None);
    };
    fs::create_dir_all(&output_dir)?;
    let paths = process_diagnostic_paths(&output_dir, pid, label);
    let helper_result = run_process_dump_helper(pid, label, &output_dir, &paths.file_stem);
    match helper_result {
        Ok(()) if paths.metadata_path.exists() => Ok(Some(ProcessDiagnosticArtifact {
            metadata_path: paths.metadata_path,
            dump_path: paths.dump_path.exists().then_some(paths.dump_path),
        })),
        Ok(()) => {
            write_metadata(
                &paths.metadata_path,
                pid,
                label,
                "missing_metadata",
                None,
                Some("process dump helper exited without writing metadata".to_string()),
            )?;
            Ok(Some(ProcessDiagnosticArtifact {
                metadata_path: paths.metadata_path,
                dump_path: None,
            }))
        }
        Err(error) => {
            write_metadata(
                &paths.metadata_path,
                pid,
                label,
                "failed",
                None,
                Some(error.to_string()),
            )?;
            Ok(Some(ProcessDiagnosticArtifact {
                metadata_path: paths.metadata_path,
                dump_path: None,
            }))
        }
    }
}

pub(crate) fn capture_process_diagnostics_best_effort(pid: u32, label: &str) {
    match capture_process_diagnostics(pid, label) {
        Ok(Some(artifact)) => {
            let dump_suffix = artifact
                .dump_path
                .as_ref()
                .map(|path| format!(" dump={}", path.display()))
                .unwrap_or_default();
            eprintln!(
                "studiofs-e2e: captured process diagnostics for pid {pid} ({label}): {}{}",
                artifact.metadata_path.display(),
                dump_suffix
            );
        }
        Ok(None) => {}
        Err(error) => {
            eprintln!(
                "studiofs-e2e: failed to capture process diagnostics for pid {pid} ({label}): {error}"
            );
        }
    }
}

pub(crate) fn write_process_dump(
    pid: u32,
    label: &str,
    output_dir: &Path,
) -> io::Result<ProcessDiagnosticArtifact> {
    fs::create_dir_all(output_dir)?;
    let paths = process_diagnostic_paths(output_dir, pid, label);
    write_process_dump_to_paths(pid, label, paths)
}

pub(crate) fn write_process_dump_with_stem(
    pid: u32,
    label: &str,
    output_dir: &Path,
    artifact_stem: &str,
) -> io::Result<ProcessDiagnosticArtifact> {
    fs::create_dir_all(output_dir)?;
    let paths = process_diagnostic_paths_with_stem(output_dir, artifact_stem);
    write_process_dump_to_paths(pid, label, paths)
}

fn write_process_dump_to_paths(
    pid: u32,
    label: &str,
    paths: ProcessDiagnosticPaths,
) -> io::Result<ProcessDiagnosticArtifact> {
    let dump_result = write_platform_process_dump(pid, &paths.dump_path);
    match dump_result {
        Ok(()) => {
            write_metadata(
                &paths.metadata_path,
                pid,
                label,
                "captured",
                Some(&paths.dump_path),
                None,
            )?;
            Ok(ProcessDiagnosticArtifact {
                metadata_path: paths.metadata_path,
                dump_path: Some(paths.dump_path),
            })
        }
        Err(error) => {
            write_metadata(
                &paths.metadata_path,
                pid,
                label,
                "metadata_only",
                None,
                Some(error.to_string()),
            )?;
            Ok(ProcessDiagnosticArtifact {
                metadata_path: paths.metadata_path,
                dump_path: None,
            })
        }
    }
}

fn configured_process_diagnostics_dir() -> Option<PathBuf> {
    #[cfg(test)]
    if let Some(path) = TEST_PROCESS_DIAGNOSTICS_DIR.with(|configured| configured.borrow().clone())
    {
        return Some(path);
    }
    PROCESS_DIAGNOSTICS_DIR
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

#[cfg(test)]
fn run_process_dump_helper(
    pid: u32,
    label: &str,
    output_dir: &Path,
    artifact_stem: &str,
) -> io::Result<()> {
    write_process_dump_with_stem(pid, label, output_dir, artifact_stem).map(|_| ())
}

#[cfg(not(test))]
fn run_process_dump_helper(
    pid: u32,
    label: &str,
    output_dir: &Path,
    artifact_stem: &str,
) -> io::Result<()> {
    let mut child = Command::new(std::env::current_exe()?)
        .arg("process-dump")
        .arg("--pid")
        .arg(pid.to_string())
        .arg("--label")
        .arg(label)
        .arg("--output-dir")
        .arg(output_dir)
        .arg("--artifact-stem")
        .arg(artifact_stem)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let deadline = Instant::now() + PROCESS_DUMP_HELPER_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            return if status.success() {
                Ok(())
            } else {
                Err(io::Error::other(format!(
                    "process dump helper exited with status {status}"
                )))
            };
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "process dump helper timed out after {} ms",
                    PROCESS_DUMP_HELPER_TIMEOUT.as_millis()
                ),
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn process_diagnostic_paths(output_dir: &Path, pid: u32, label: &str) -> ProcessDiagnosticPaths {
    let id = PROCESS_DIAGNOSTICS_ID.fetch_add(1, Ordering::SeqCst);
    let file_stem = format!("{id:06}-pid-{pid}-{}", sanitize_label(label));
    process_diagnostic_paths_with_stem(output_dir, &file_stem)
}

fn process_diagnostic_paths_with_stem(
    output_dir: &Path,
    file_stem: &str,
) -> ProcessDiagnosticPaths {
    let file_stem = sanitize_artifact_stem(file_stem);
    ProcessDiagnosticPaths {
        file_stem: file_stem.clone(),
        metadata_path: output_dir.join(format!("{file_stem}.json")),
        dump_path: output_dir.join(format!("{file_stem}.dmp")),
    }
}

fn sanitize_artifact_stem(stem: &str) -> String {
    let sanitized = sanitize_label(stem);
    if sanitized.is_empty() {
        "process".to_string()
    } else {
        sanitized
    }
}

fn sanitize_label(label: &str) -> String {
    let mut sanitized = label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    while sanitized.contains("--") {
        sanitized = sanitized.replace("--", "-");
    }
    let sanitized = sanitized.trim_matches('-');
    if sanitized.is_empty() {
        "process".to_string()
    } else {
        sanitized.chars().take(96).collect()
    }
}

fn write_metadata(
    path: &Path,
    pid: u32,
    label: &str,
    status: &str,
    dump_path: Option<&Path>,
    dump_error: Option<String>,
) -> io::Result<()> {
    let metadata = ProcessDiagnosticMetadata {
        timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        pid,
        label,
        status,
        dump_path: dump_path.map(|path| path.display().to_string()),
        dump_error,
    };
    let file = File::create(path)?;
    serde_json::to_writer_pretty(file, &metadata)?;
    Ok(())
}

#[cfg(windows)]
fn write_platform_process_dump(pid: u32, dump_path: &Path) -> io::Result<()> {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawHandle as _;
    use std::ptr;

    const PROCESS_QUERY_INFORMATION: u32 = 0x0400;
    const PROCESS_VM_READ: u32 = 0x0010;
    const PROCESS_DUP_HANDLE: u32 = 0x0040;
    const MINI_DUMP_WITH_HANDLE_DATA: u32 = 0x0004;
    const MINI_DUMP_WITH_THREAD_INFO: u32 = 0x1000;

    #[link(name = "dbghelp")]
    unsafe extern "system" {
        fn MiniDumpWriteDump(
            hProcess: *mut c_void,
            ProcessId: u32,
            hFile: *mut c_void,
            DumpType: u32,
            ExceptionParam: *mut c_void,
            UserStreamParam: *mut c_void,
            CallbackParam: *mut c_void,
        ) -> i32;
    }

    unsafe extern "system" {
        fn OpenProcess(dwDesiredAccess: u32, bInheritHandle: i32, dwProcessId: u32) -> *mut c_void;
        fn CloseHandle(hObject: *mut c_void) -> i32;
    }

    struct ProcessHandle(*mut c_void);

    impl ProcessHandle {
        fn as_raw(&self) -> *mut c_void {
            self.0
        }
    }

    impl Drop for ProcessHandle {
        fn drop(&mut self) {
            if !self.0.is_null() {
                let _ = unsafe { CloseHandle(self.0) };
            }
        }
    }

    let process = unsafe {
        OpenProcess(
            PROCESS_QUERY_INFORMATION | PROCESS_VM_READ | PROCESS_DUP_HANDLE,
            0,
            pid,
        )
    };
    if process.is_null() {
        return Err(io::Error::last_os_error());
    }
    let process = ProcessHandle(process);

    let file = File::create(dump_path)?;
    let dump_type = MINI_DUMP_WITH_HANDLE_DATA | MINI_DUMP_WITH_THREAD_INFO;
    let ok = unsafe {
        MiniDumpWriteDump(
            process.as_raw(),
            pid,
            file.as_raw_handle().cast(),
            dump_type,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
        )
    };
    let dump_error = if ok == 0 {
        Some(io::Error::last_os_error())
    } else {
        None
    };
    if let Some(error) = dump_error {
        return Err(error);
    }
    Ok(())
}

#[cfg(not(windows))]
fn write_platform_process_dump(_pid: u32, _dump_path: &Path) -> io::Result<()> {
    Err(io::Error::other(
        "process minidumps are only supported on Windows",
    ))
}
