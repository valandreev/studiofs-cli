//! Mounted macOS page-ins for files with unpublished local state.

use super::*;

const SUMMARY: &str = "macos-mmap-exec.json";
const IO_TIMEOUT: Duration = Duration::from_secs(120);

pub(in crate::scenario) struct MacosMmapExec;

impl Scenario for MacosMmapExec {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: "mounted-macos-mmap-exec",
            description: "just-written files can be mapped and executed without a page-in fault",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        let config = context
            .client_controller_config
            .clone()
            .ok_or(ScenarioError::MissingComposeInfrastructure)?;
        let mut controller = MultiClientController::new(config, SystemClientProcessExecutor);
        let result = probe(context, &mut controller);
        let cleanup = if matches!(
            result,
            Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
        ) {
            controller.emergency_kill_all_no_wait("macos mmap exec timeout")
        } else {
            controller.unmount_all()
        };
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(()), Err(cleanup)) => Err(cleanup.into()),
            (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
                context: "macos mmap exec".into(),
                primary: Box::new(primary),
                cleanup: Box::new(cleanup.into()),
            }),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary: serde_json::Value =
            serde_json::from_slice(&fs::read(context.scenario_dir.join(SUMMARY))?)?;
        let failures = acceptance_failures(&summary);
        if !failures.is_empty() {
            return Err(io::Error::other(format!(
                "macos mmap exec acceptance failed ({}): {summary}",
                failures.join("; ")
            ))
            .into());
        }
        Ok(Vec::new())
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn acceptance_failures(summary: &serde_json::Value) -> Vec<String> {
    let mut failures = Vec::new();
    for key in [
        "map_read_after_close",
        "map_write_on_created_handle",
        "exec_just_copied",
        "exec_again",
    ] {
        if summary[key].as_str() != Some("ok") {
            failures.push(format!("{key} = {}, want ok", summary[key]));
        }
    }
    if summary["mapped_write_readback_matches"].as_bool() != Some(true) {
        failures.push("mapped write did not reach a later read".into());
    }
    failures
}

fn probe(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<(), ScenarioError> {
    controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: vec!["client-a".into()],
        clean_existing_local_metadata: true,
    })?;
    controller.mount_all()?;
    let mount = controller.clients()[0].mount_point.clone();
    let studiofs_bin = fs::canonicalize(
        &context
            .client_controller_config
            .as_ref()
            .ok_or(ScenarioError::MissingComposeInfrastructure)?
            .studiofs_bin,
    )?;
    let summary = run_bounded_mounted_io("macos mmap exec probe", IO_TIMEOUT, move || {
        mounted_probe(&mount, &studiofs_bin)
    })?;
    fs::write(
        context.scenario_dir.join(SUMMARY),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn mounted_probe(mount: &Path, studiofs_bin: &Path) -> io::Result<serde_json::Value> {
    const MAP_BYTES: usize = 64 * 1024;

    let read_path = mount.join("map-read.bin");
    fs::write(&read_path, vec![0xA5; MAP_BYTES])?;
    let reader = fs::File::open(&read_path)?;
    let map_read_after_close = page_in::read(&reader, MAP_BYTES, 0xA5)?;

    let write_path = mount.join("map-write.bin");
    let created = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&write_path)?;
    created.set_len(MAP_BYTES as u64)?;
    let map_write_on_created_handle = page_in::write(&created, MAP_BYTES, 0x5A)?;
    drop(created);
    let mapped_write_readback_matches = fs::read(&write_path)? == vec![0x5A; MAP_BYTES];

    let copied = mount.join("studiofs-copy");
    fs::copy(studiofs_bin, &copied)?;
    let exec_just_copied = exec_status(&copied)?;
    let exec_again = exec_status(&copied)?;
    Ok(json!({
        "map_read_after_close": map_read_after_close,
        "map_write_on_created_handle": map_write_on_created_handle,
        "mapped_write_readback_matches": mapped_write_readback_matches,
        "exec_just_copied": exec_just_copied,
        "exec_again": exec_again,
    }))
}

#[cfg(not(target_os = "macos"))]
fn mounted_probe(_mount: &Path, _studiofs_bin: &Path) -> io::Result<serde_json::Value> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "this scenario covers macFUSE page-in behavior and needs macOS",
    ))
}

/// `ok`, or how the executed copy failed, so a page-in fault stays in the summary.
#[cfg(target_os = "macos")]
fn exec_status(path: &Path) -> io::Result<String> {
    use std::os::unix::process::ExitStatusExt as _;

    let status = match std::process::Command::new(path)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        Ok(status) => status,
        Err(error) if error.raw_os_error().is_some() => return Ok(format!("exec: {error}")),
        Err(error) => return Err(error),
    };
    Ok(match (status.code(), status.signal()) {
        (Some(0), _) => "ok".into(),
        (Some(code), _) => format!("exit {code}"),
        (None, Some(signal)) => format!("signal {signal}"),
        (None, None) => "unknown status".into(),
    })
}

/// A failed page-in raises SIGBUS, so each mapping runs in a forked child.
#[cfg(target_os = "macos")]
mod page_in {
    use std::fs::File;
    use std::io;
    use std::os::fd::{AsRawFd as _, RawFd};

    const CONTENT_MISMATCH: libc::c_int = 2;
    const MAP_FAILED: libc::c_int = 3;
    const SYNC_FAILED: libc::c_int = 4;

    pub(super) fn read(file: &File, len: usize, expected: u8) -> io::Result<String> {
        in_child(file.as_raw_fd(), len, libc::PROT_READ, move |map| {
            for offset in 0..len {
                // SAFETY: `offset` is within the live `len`-byte mapping.
                if unsafe { map.add(offset).read_volatile() } != expected {
                    return CONTENT_MISMATCH;
                }
            }
            0
        })
    }

    pub(super) fn write(file: &File, len: usize, value: u8) -> io::Result<String> {
        in_child(
            file.as_raw_fd(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            move |map| {
                for offset in 0..len {
                    // SAFETY: `offset` is within the live, writable `len`-byte mapping.
                    unsafe { map.add(offset).write_volatile(value) };
                }
                // SAFETY: `map` and `len` describe the live mapping.
                if unsafe { libc::msync(map.cast(), len, libc::MS_SYNC) } == 0 {
                    0
                } else {
                    SYNC_FAILED
                }
            },
        )
    }

    fn in_child(
        fd: RawFd,
        len: usize,
        protection: libc::c_int,
        touch: impl FnOnce(*mut u8) -> libc::c_int,
    ) -> io::Result<String> {
        // SAFETY: the child only makes syscalls and touches the mapping, without allocating or
        // taking locks another thread could hold, and leaves through `_exit`.
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(io::Error::last_os_error());
        }
        if pid == 0 {
            // SAFETY: a null hint lets the kernel place a new mapping of the valid `fd`.
            let map = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    protection,
                    libc::MAP_SHARED,
                    fd,
                    0,
                )
            };
            let code = if map == libc::MAP_FAILED {
                MAP_FAILED
            } else {
                touch(map.cast())
            };
            // SAFETY: `_exit` skips the parent's atexit handlers and never returns.
            unsafe { libc::_exit(code) };
        }
        let mut status = 0;
        // SAFETY: `pid` is this process's unreaped child and `status` is writable.
        if unsafe { libc::waitpid(pid, &mut status, 0) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(if libc::WIFSIGNALED(status) {
            format!("signal {}", libc::WTERMSIG(status))
        } else if libc::WEXITSTATUS(status) == 0 {
            "ok".into()
        } else {
            format!("exit {}", libc::WEXITSTATUS(status))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acceptance_reports_each_page_in_failure() {
        let passing = json!({
            "map_read_after_close": "ok",
            "map_write_on_created_handle": "ok",
            "mapped_write_readback_matches": true,
            "exec_just_copied": "ok",
            "exec_again": "ok",
        });
        assert!(acceptance_failures(&passing).is_empty());

        let mut faulted = passing;
        faulted["exec_just_copied"] = json!("signal 10");
        faulted["mapped_write_readback_matches"] = json!(false);
        let failures = acceptance_failures(&faulted);
        assert_eq!(failures.len(), 2, "{failures:?}");
        assert!(failures[0].contains("exec_just_copied"), "{failures:?}");
    }

    #[test]
    fn acceptance_rejects_a_summary_without_results() {
        assert_eq!(acceptance_failures(&json!({})).len(), 5);
    }
}
