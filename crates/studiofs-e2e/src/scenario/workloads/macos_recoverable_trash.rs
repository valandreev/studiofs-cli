//! Native macFUSE trash operations across two PostgreSQL/S3 clients.

use super::*;
use crate::infra::CommandSpec;
use studiofs_vfs::{RUNTIME_STATS_FILE_NAME_STR, RuntimeStatsSnapshot};

pub(in crate::scenario) const MACOS_RECOVERABLE_TRASH: &str =
    "mounted-macos-recoverable-trash-two-client";
pub(in crate::scenario) struct MacosRecoverableTrash;

const SUMMARY: &str = "trash-summary.json";
const IO_LIMIT: Duration = Duration::from_secs(15);
const CONVERGENCE_LIMIT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(200);
const FILE: &str = "note.bin";
const DIR: &str = "album";
const CHILD: &str = "album/child.bin";
const FILE_BYTES: &[u8] = b"macOS retained file\0exact\xff\n";
const CHILD_BYTES: &[u8] = b"macOS retained directory child\0\xfe\n";
const SIDECAR: &str = ".DS_Store";

impl Scenario for MacosRecoverableTrash {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MACOS_RECOVERABLE_TRASH,
            description: "macFUSE native trash move, listing, remote read, restore, restart and Clear across two mounted clients",
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
        let binary = config.studiofs_bin.clone();
        let mut controller = MultiClientController::new(config, SystemClientProcessExecutor);
        let result = probe(&binary, &mut controller);
        let cleanup = if matches!(
            result,
            Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
        ) {
            controller.emergency_kill_all_no_wait("macOS recoverable trash timeout")
        } else {
            controller.unmount_all()
        };
        let summary = match (result, cleanup) {
            (Ok(summary), Ok(())) => summary,
            (Err(primary), Ok(())) => return Err(primary),
            (Ok(_), Err(cleanup)) => return Err(cleanup.into()),
            (Err(primary), Err(cleanup)) => {
                return Err(ScenarioError::CleanupAfterFailure {
                    context: MACOS_RECOVERABLE_TRASH.into(),
                    primary: Box::new(primary),
                    cleanup: Box::new(cleanup.into()),
                });
            }
        };
        fs::write(
            context.scenario_dir.join(SUMMARY),
            serde_json::to_vec_pretty(&summary)?,
        )?;
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary = serde_json::from_slice(&fs::read(context.scenario_dir.join(SUMMARY))?)?;
        if !summary_passed(&summary) {
            return Err(io::Error::other("macOS mounted trash acceptance incomplete").into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{MACOS_RECOVERABLE_TRASH}/{SUMMARY}"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }

    fn unsupported_platform(&self) -> Option<&'static str> {
        (!cfg!(target_os = "macos")).then_some("macOS with macFUSE required")
    }
}

fn summary_passed(summary: &serde_json::Value) -> bool {
    summary["passed"] == true
        && matches!(
            (summary["peer_s3_gets_before"].as_u64(), summary["peer_s3_gets_after"].as_u64()),
            (Some(before), Some(after)) if after > before
        )
}

fn s3_gets(mount: &Path) -> Result<u64, ScenarioError> {
    let path = mount.join(RUNTIME_STATS_FILE_NAME_STR);
    run_bounded_mounted_io("peer S3 GET counter", IO_LIMIT, move || {
        let text = fs::read_to_string(path)?;
        let stats = RuntimeStatsSnapshot::parse_text(&text).map_err(io::Error::other)?;
        stats
            .object_io
            .counters
            .get("get_calls")
            .map(|metric| metric.value)
            .ok_or_else(|| io::Error::other("client-b S3 GET telemetry missing"))
    })
}

#[derive(Debug, thiserror::Error)]
#[error("{label}: {}: {source}{hint}", path.display())]
struct MountedPathError {
    label: String,
    path: PathBuf,
    source: io::Error,
    hint: &'static str,
}

fn mounted_path_error(label: &str, path: &Path, source: io::Error) -> io::Error {
    #[cfg(unix)]
    let hint = if source.raw_os_error() == Some(libc::EPERM)
        && path.components().any(|part| part.as_os_str() == ".Trashes")
    {
        ". Grant Full Disk Access to the launching app (Terminal, iTerm, or IDE) in System Settings → Privacy & Security, then rerun"
    } else {
        ""
    };
    #[cfg(not(unix))]
    let hint = "";
    io::Error::new(
        source.kind(),
        MountedPathError {
            label: label.to_string(),
            path: path.to_path_buf(),
            source,
            hint,
        },
    )
}

fn expect_trash_payload(path: &Path, expected: &[u8]) -> Result<(), ScenarioError> {
    super::recoverable_trash::expect_payload(path, expected).map_err(|error| match error {
        ScenarioError::Io(source) => {
            mounted_path_error("read retained payload", path, source).into()
        }
        error => error,
    })
}

fn probe(
    binary: &Path,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<serde_json::Value, ScenarioError> {
    controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: vec!["client-a".into(), "client-b".into()],
        clean_existing_local_metadata: true,
    })?;
    let a = controller.client_at(0)?.clone();
    let b = controller.client_at(1)?.clone();
    let retention = CommandSpec {
        program: binary.to_string_lossy().into_owned(),
        args: vec!["trash-days".into(), a.volume_name, "7".into()],
        env: vec![
            (
                "STUDIOFS_CLIENT_ROOT".into(),
                a.client_root.to_string_lossy().into_owned(),
            ),
            (
                "STUDIOFS_DAEMON_LOG_DIR".into(),
                a.client_root.join("logs").to_string_lossy().into_owned(),
            ),
        ],
        cwd: None,
        timeout: Some(IO_LIMIT),
    };
    let status = SystemCommandExecutor.run(&retention)?.status;
    if status != 0 {
        return Err(
            io::Error::other(format!("setting trash retention exited with {status}")).into(),
        );
    }
    for client in ["client-a", "client-b"] {
        controller.set_client_env_overrides(client, [("STUDIOFS_MOUNT_BACKEND", "macfuse")])?;
    }
    controller.mount_all()?;

    let uid = std::process::Command::new("/usr/bin/id")
        .arg("-u")
        .output()?;
    if !uid.status.success() {
        return Err(io::Error::other("could not determine mount uid").into());
    }
    let uid: u32 = String::from_utf8_lossy(&uid.stdout)
        .trim()
        .parse()
        .map_err(|_| io::Error::other("mount uid is not numeric"))?;
    let trash = PathBuf::from(".Trashes").join(uid.to_string());
    let a_trash = a.mount_point.join(&trash);
    let b_trash = b.mount_point.join(&trash);

    // Positive retention exposes the containers even before the first receipt.
    wait_state("writer trash container", a_trash.clone(), &[], &[])?;
    wait_state("peer trash container", b_trash.clone(), &[], &[])?;
    let writer = a.mount_point.clone();
    let writer_trash = a_trash.clone();
    run_bounded_mounted_io("write live file and directory", IO_LIMIT, move || {
        let directory = writer.join(DIR);
        fs::create_dir(&directory)
            .map_err(|error| mounted_path_error("create live directory", &directory, error))?;
        for (name, bytes) in [(FILE, FILE_BYTES), (CHILD, CHILD_BYTES)] {
            let path = writer.join(name);
            fs::File::create(&path)
                .and_then(|mut file| {
                    file.write_all(bytes)?;
                    file.sync_all()
                })
                .map_err(|error| mounted_path_error("write live file", &path, error))?;
        }
        Ok(())
    })?;
    // The macOS trash container accepts .DS_Store as client-local metadata.
    run_bounded_mounted_io("write .Trashes/<uid>/.DS_Store", IO_LIMIT, move || {
        let path = writer_trash.join(SIDECAR);
        fs::write(&path, b"Finder metadata fixture")
            .map_err(|error| mounted_path_error("write Finder metadata", &path, error))
    })?;
    // Observe the live paths on B without warming its payload cache.
    wait_state(
        "peer live namespace publication",
        b_trash.clone(),
        &[],
        &[
            (b.mount_point.join(FILE), true),
            (b.mount_point.join(CHILD), true),
        ],
    )?;
    let writer = a.mount_point.clone();
    let writer_trash = a_trash.clone();
    run_bounded_mounted_io(
        "rename live file and directory into macOS Trash",
        IO_LIMIT,
        move || {
            for name in [FILE, DIR] {
                let path = writer_trash.join(name);
                fs::rename(writer.join(name), &path)
                    .map_err(|error| mounted_path_error("trash live entry", &path, error))?;
            }
            Ok(())
        },
    )?;
    for (label, mount, trash, sidecar_present) in [
        ("writer retained listing", &a.mount_point, &a_trash, true),
        (
            "peer retained listing and Finder metadata isolation",
            &b.mount_point,
            &b_trash,
            false,
        ),
    ] {
        wait_state(
            label,
            trash.clone(),
            &[(FILE, true), (DIR, true), (SIDECAR, sidecar_present)],
            &[
                (mount.join(FILE), false),
                (mount.join(DIR), false),
                (trash.join(CHILD), true),
            ],
        )?;
    }
    let gets_before = s3_gets(&b.mount_point)?;
    expect_trash_payload(&b_trash.join(FILE), FILE_BYTES)?;
    let gets_after = s3_gets(&b.mount_point)?;
    if gets_after <= gets_before {
        return Err(io::Error::other(format!(
            "client-b retained read did not increase S3 GETs ({gets_before} -> {gets_after})"
        ))
        .into());
    }
    let retained = b_trash.join(FILE);
    let restored = b.mount_point.join(FILE);
    run_bounded_mounted_io(
        "peer restore from macOS Trash to original path",
        IO_LIMIT,
        move || {
            fs::rename(&retained, restored)
                .map_err(|error| mounted_path_error("restore retained file", &retained, error))
        },
    )?;
    super::recoverable_trash::expect_payload(&a.mount_point.join(FILE), FILE_BYTES)?;
    wait_state(
        "writer restore receipt removal",
        a_trash.clone(),
        &[(FILE, false), (DIR, true)],
        &[],
    )?;

    // Remount B with one live receipt so an empty projection cannot pass.
    controller.restart("client-b")?;
    wait_state(
        "peer retained listing after restart",
        b_trash.clone(),
        &[(FILE, false), (DIR, true), (SIDECAR, false)],
        &[(b_trash.join(CHILD), true)],
    )?;
    expect_trash_payload(&b_trash.join(CHILD), CHILD_BYTES)?;
    let directory = b_trash.join(DIR);
    let child = b_trash.join(CHILD);
    run_bounded_mounted_io("peer Clear of retained directory", IO_LIMIT, move || {
        fs::remove_file(&child)
            .map_err(|error| mounted_path_error("unlink retained child", &child, error))?;
        fs::remove_dir(&directory)
            .map_err(|error| mounted_path_error("remove retained directory", &directory, error))
    })?;
    for (label, mount, trash) in [
        ("writer purge convergence", &a.mount_point, a_trash),
        ("peer purge convergence", &b.mount_point, b_trash),
    ] {
        wait_state(
            label,
            trash.clone(),
            &[(FILE, false), (DIR, false)],
            &[(mount.join(DIR), false), (trash.join(DIR), false)],
        )?;
    }
    Ok(json!({
        "passed": true,
        "retention_days": 7,
        "trash_path": trash,
        "retained_file_sha256": bytes_sha256(FILE_BYTES),
        "retained_child_sha256": bytes_sha256(CHILD_BYTES),
        "peer_s3_gets_before": gets_before,
        "peer_s3_gets_after": gets_after,
    }))
}

fn wait_state(
    label: &str,
    trash: PathBuf,
    expected_listing: &[(&'static str, bool)],
    expected_paths: &[(PathBuf, bool)],
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + CONVERGENCE_LIMIT;
    loop {
        let trash = trash.clone();
        let expected_listing = expected_listing.to_vec();
        let expected_paths = expected_paths.to_vec();
        let remaining = deadline.saturating_duration_since(Instant::now());
        let operation = label.to_string();
        let mismatch = run_bounded_mounted_io(label, remaining.min(IO_LIMIT), move || {
            let entries = match fs::read_dir(&trash) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Ok(Some(format!("trash container {} missing", trash.display())));
                }
                Err(error) => return Err(mounted_path_error(&operation, &trash, error)),
            };
            let names = entries
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect::<io::Result<Vec<_>>>()
                .map_err(|error| mounted_path_error(&operation, &trash, error))?;
            for (name, present) in expected_listing {
                if names.iter().any(|actual| actual == name) != present {
                    return Ok(Some(format!(
                        "{} listing expected {name} present={present}, got {names:?}",
                        trash.display()
                    )));
                }
            }
            for (path, present) in expected_paths {
                let exists = match fs::symlink_metadata(&path) {
                    Ok(_) => true,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                    Err(error) => return Err(mounted_path_error(&operation, &path, error)),
                };
                if exists != present {
                    return Ok(Some(format!(
                        "{} expected present={present}",
                        path.display()
                    )));
                }
            }
            Ok(None)
        })?;
        let Some(mismatch) = mismatch else {
            return Ok(());
        };
        if deadline.saturating_duration_since(Instant::now()) <= POLL {
            return Err(
                io::Error::new(io::ErrorKind::TimedOut, format!("{label}: {mismatch}")).into(),
            );
        }
        thread::sleep(POLL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn trash_path_error_preserves_errno_and_hints_only_for_eperm() {
        for (path, errno, hinted) in [
            ("mount/.Trashes/501", libc::EPERM, true),
            ("mount/.Trashes/501", libc::EACCES, false),
            ("mount/.Trashes/501", libc::ENOENT, false),
            ("mount/.Trashes-backup", libc::EPERM, false),
        ] {
            let source = io::Error::from_raw_os_error(errno);
            let kind = source.kind();
            let error = mounted_path_error("list", Path::new(path), source);
            assert_eq!(error.kind(), kind);
            let context = error
                .get_ref()
                .and_then(|error| error.downcast_ref::<MountedPathError>())
                .unwrap();
            assert_eq!(context.source.raw_os_error(), Some(errno));
            let message = error.to_string();
            assert!(message.contains(path), "{message}");
            assert_eq!(message.contains("Full Disk Access"), hinted, "{message}");
        }
    }

    #[test]
    fn summary_requires_explicit_pass_and_remote_fetch() {
        let mut summary =
            json!({"passed": true, "peer_s3_gets_before": 0, "peer_s3_gets_after": 1});
        assert!(summary_passed(&summary));
        summary["peer_s3_gets_after"] = json!(0);
        assert!(!summary_passed(&summary));
        assert!(!summary_passed(&json!({"passed": true})));
        assert!(!summary_passed(
            &json!({"passed": false, "peer_s3_gets_before": 0, "peer_s3_gets_after": 1})
        ));
    }
}
