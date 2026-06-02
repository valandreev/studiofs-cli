//! Sanity-level FUSE test coverage for mount lifecycle and metadata edge cases.

#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::io;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::process::ExitStatus;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use studiofs_data::store::DataStoreConfig;
use studiofs_fuse::{StudioFuseFs, StudioFuseRuntimeOptions};
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, MetaFormat, MetadataConfig, RedbMetaEngine,
};
use studiofs_vfs::{StudioVfs, VfsConfig};

const MOUNT_TESTS_ENV: &str = "STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS";
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MountedFuseMode {
    Sync,
    AsyncWrite,
    AsyncWriteDio,
}

impl MountedFuseMode {
    fn name(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::AsyncWrite => "async-write",
            Self::AsyncWriteDio => "async-write+dio",
        }
    }

    fn async_write_enabled(self) -> bool {
        matches!(self, Self::AsyncWrite | Self::AsyncWriteDio)
    }

    fn async_dio_enabled(self) -> bool {
        matches!(self, Self::AsyncWriteDio)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MountedScenarioPlatform {
    AnyUnix,
    Linux,
}

impl MountedScenarioPlatform {
    fn matches_current(self) -> bool {
        match self {
            Self::AnyUnix => true,
            Self::Linux => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MountedScenarioOperation {
    SequentialWriteReadback,
    WriteFlushReadback,
    WriteFsyncReadback,
    CloseReopenReadback,
    MultipleWriters,
    SameHandleAppends,
    FioMultiJob,
    WriteOnlyUntilUnmount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MountedAsyncScenario {
    name: &'static str,
    mode: MountedFuseMode,
    platform: MountedScenarioPlatform,
    operation: MountedScenarioOperation,
}

#[derive(Default)]
struct MountedOperationGuard {
    _open_files: Vec<std::fs::File>,
}

macro_rules! declare_mounted_scenarios {
    (
        $(
            $test_name:ident => {
                name: $name:literal,
                mode: $mode:expr,
                platform: $platform:expr,
                operation: $operation:expr
            }
        ),+ $(,)?
    ) => {
        fn mounted_async_scenarios() -> &'static [MountedAsyncScenario] {
            &[
                $(
                    MountedAsyncScenario {
                        name: $name,
                        mode: $mode,
                        platform: $platform,
                        operation: $operation,
                    },
                )+
            ]
        }

        $(
            #[test]
            #[ignore = "requires /dev/fuse and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
            fn $test_name() {
                run_mounted_scenario(MountedAsyncScenario {
                    name: $name,
                    mode: $mode,
                    platform: $platform,
                    operation: $operation,
                });
            }
        )+
    };
}

declare_mounted_scenarios! {
    async_mounted_sequential_write_readback => {
        name: "sequential-write-readback",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::AnyUnix,
        operation: MountedScenarioOperation::SequentialWriteReadback
    },
    async_mounted_write_flush_readback => {
        name: "write-flush-readback",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::AnyUnix,
        operation: MountedScenarioOperation::WriteFlushReadback
    },
    async_mounted_write_fsync_readback => {
        name: "write-fsync-readback",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::AnyUnix,
        operation: MountedScenarioOperation::WriteFsyncReadback
    },
    async_mounted_close_reopen_readback => {
        name: "close-reopen-readback",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::AnyUnix,
        operation: MountedScenarioOperation::CloseReopenReadback
    },
    async_mounted_multiple_writers => {
        name: "multiple-writers",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::AnyUnix,
        operation: MountedScenarioOperation::MultipleWriters
    },
    async_mounted_same_handle_appends => {
        name: "same-handle-appends",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::AnyUnix,
        operation: MountedScenarioOperation::SameHandleAppends
    },
    async_mounted_unmount_after_async_write => {
        name: "unmount-after-async-write",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::AnyUnix,
        operation: MountedScenarioOperation::WriteOnlyUntilUnmount
    },
    linux_async_mounted_default_sync => {
        name: "linux-default-sync",
        mode: MountedFuseMode::Sync,
        platform: MountedScenarioPlatform::Linux,
        operation: MountedScenarioOperation::SequentialWriteReadback
    },
    linux_async_mounted_async_opt_in => {
        name: "linux-async-opt-in",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::Linux,
        operation: MountedScenarioOperation::SequentialWriteReadback
    },
    linux_async_mounted_async_dio_opt_in => {
        name: "linux-async-dio-opt-in",
        mode: MountedFuseMode::AsyncWriteDio,
        platform: MountedScenarioPlatform::Linux,
        operation: MountedScenarioOperation::SequentialWriteReadback
    },
    linux_async_mounted_fio_multi_job => {
        name: "linux-fio-multi-job",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::Linux,
        operation: MountedScenarioOperation::FioMultiJob
    },
}

#[test]
fn async_mounted_test_matrix_covers_correctness_and_platform_opt_in_scenarios() {
    let scenarios = mounted_async_scenarios();
    let names: BTreeSet<_> = scenarios.iter().map(|scenario| scenario.name).collect();
    let modes: BTreeSet<_> = scenarios
        .iter()
        .map(|scenario| scenario.mode.name())
        .collect();

    for expected in [
        "sequential-write-readback",
        "write-flush-readback",
        "write-fsync-readback",
        "close-reopen-readback",
        "multiple-writers",
        "same-handle-appends",
        "linux-default-sync",
        "linux-async-opt-in",
        "linux-async-dio-opt-in",
        "linux-fio-multi-job",
        "unmount-after-async-write",
    ] {
        assert!(
            names.contains(expected),
            "mounted async FUSE scenario matrix is missing {expected}"
        );
    }

    for expected in ["sync", "async-write", "async-write+dio"] {
        assert!(
            modes.contains(expected),
            "mounted async FUSE scenario matrix is missing mode {expected}"
        );
    }
}

#[test]
fn runtime_options_from_env_is_public_for_embedder_overrides() {
    let mut options = StudioFuseRuntimeOptions::from_env();

    options.async_write_enabled = true;

    assert!(options.async_write_enabled);
}

#[test]
fn async_mounted_matrix_includes_unmount_teardown_scenario() {
    let scenarios = mounted_async_scenarios();
    let unmount = scenarios
        .iter()
        .find(|scenario| scenario.name == "unmount-after-async-write")
        .expect("unmount-after-async-write scenario should be present");

    assert_eq!(
        unmount.operation,
        MountedScenarioOperation::WriteOnlyUntilUnmount,
        "unmount-after-async-write must leave write teardown to unmount"
    );
}

#[test]
fn command_path_is_available_only_when_executable() {
    let tmp = target_tempdir("command-availability-");
    let command_path = tmp.path().join("fio");
    std::fs::write(&command_path, b"not executable").expect("write fake command");

    std::fs::set_permissions(&command_path, std::fs::Permissions::from_mode(0o644))
        .expect("set non-executable permissions");
    let mode = std::fs::metadata(&command_path)
        .expect("fake command metadata")
        .permissions()
        .mode();
    if mode & 0o111 != 0 {
        eprintln!(
            "skipping command executable-bit check because the test filesystem reports mode {mode:o} after chmod 644"
        );
        return;
    }

    assert!(
        !command_path_is_executable(&command_path),
        "plain files in PATH must not be treated as executable commands"
    );

    std::fs::set_permissions(&command_path, std::fs::Permissions::from_mode(0o755))
        .expect("set executable permissions");
    assert!(command_path_is_executable(&command_path));
}

fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn target_tempdir(prefix: &str) -> tempfile::TempDir {
    let temp_root = workspace_root()
        .join("target")
        .join("studiofs-fuse-mount-tests");
    std::fs::create_dir_all(&temp_root).expect("create target-backed test temp root");
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(temp_root)
        .expect("target-backed tempdir")
}

fn workspace_env_path() -> std::path::PathBuf {
    workspace_root().join(".env")
}

fn load_test_env() {
    static ONCE: OnceLock<()> = OnceLock::new();
    let _ = ONCE.get_or_init(|| {
        let env_path = workspace_env_path();
        if env_path.exists() {
            let _ = dotenvy::from_path(&env_path);
        } else {
            let _ = dotenvy::dotenv();
        }
    });
}

fn workspace_dotenv_vars() -> &'static std::collections::BTreeMap<String, String> {
    static VARS: OnceLock<std::collections::BTreeMap<String, String>> = OnceLock::new();
    VARS.get_or_init(|| {
        let mut vars = std::collections::BTreeMap::new();
        let env_path = workspace_env_path();
        if let Ok(iter) = dotenvy::from_path_iter(&env_path) {
            for item in iter.flatten() {
                vars.insert(item.0, item.1);
            }
        }
        vars
    })
}

fn workspace_dotenv_var(key: &str) -> Option<String> {
    workspace_dotenv_vars().get(key).cloned()
}

fn test_online_env_var(primary: &str, legacy: &str, default: &str) -> String {
    load_test_env();
    std::env::var(primary)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| workspace_dotenv_var(primary))
        .or_else(|| workspace_dotenv_var(legacy))
        .unwrap_or_else(|| default.to_string())
}

fn test_format_info(volume_suffix: &str) -> FormatInfo {
    load_test_env();
    let volume_name = format!("fuse-test-{volume_suffix}-{}", std::process::id());
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: volume_name.clone(),
        uuid: format!("fuse-test-uuid-{volume_suffix}"),
        endpoint: test_online_env_var(
            "STUDIOFS_TEST_ONLINE_S3_ENDPOINT",
            "STUDIOFS_S3_ENDPOINT",
            "http://127.0.0.1:9000",
        ),
        bucket: test_online_env_var(
            "STUDIOFS_TEST_ONLINE_S3_BUCKET",
            "STUDIOFS_S3_BUCKET",
            "studiofs-test",
        ),
        access_key: test_online_env_var(
            "STUDIOFS_TEST_ONLINE_S3_ACCESS_KEY",
            "STUDIOFS_S3_ACCESS_KEY",
            "studiofs",
        ),
        secret_key: test_online_env_var(
            "STUDIOFS_TEST_ONLINE_S3_SECRET_KEY",
            "STUDIOFS_S3_SECRET_KEY",
            "studiofs123",
        ),
        region: "us-east-1".into(),
        data_prefix: format!("{volume_name}/objects/"),
        metadata: MetadataConfig::default(),
        encryption: studiofs_meta::EncryptionConfig::new(
            [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
        ),
        block_size: 4 * 1024 * 1024,
    }
}

fn make_test_vfs(
    tmp: &tempfile::TempDir,
    volume_suffix: &str,
    rt: &tokio::runtime::Runtime,
) -> Arc<StudioVfs> {
    let db_path = tmp.path().join("meta.redb");
    let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
    meta.init_root(0, 0).expect("init root");

    let format_info = test_format_info(volume_suffix);
    meta.set_format_info(&format_info).expect("set format info");
    let data_config = DataStoreConfig::from(&format_info);

    let cache_dir = tmp.path().join("cache");
    let _guard = rt.enter();
    let vfs = StudioVfs::new(
        meta,
        &data_config,
        cache_dir.to_str().expect("cache dir str"),
        64 * 1024 * 1024,
        32 * 1024 * 1024,
        VfsConfig::default()
            .try_into()
            .expect("default VFS config should validate"),
    )
    .expect("create vfs");
    vfs.start().expect("start vfs background workers");
    vfs
}

fn mounted_tests_enabled() -> bool {
    std::env::var(MOUNT_TESTS_ENV).as_deref() == Ok("1")
}

fn finish_mounted_fuse_test(
    test_result: std::thread::Result<MountedOperationGuard>,
    unmount_result: io::Result<()>,
    join_result: Option<std::thread::Result<io::Result<()>>>,
) {
    match test_result {
        Ok(_guard) => {
            if let Some(join_result) = join_result {
                match join_result {
                    Ok(run_result) => run_result.expect("FUSE session should run until unmount"),
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            }
            unmount_result.expect("FUSE unmount should succeed");
        }
        Err(panic) => {
            if let Err(error) = unmount_result {
                eprintln!("FUSE unmount failed after scenario panic: {error}");
            }
            if let Some(join_result) = join_result {
                match join_result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        eprintln!("FUSE session failed after scenario panic: {error}");
                    }
                    Err(_) => {
                        eprintln!("FUSE session thread panicked after scenario panic");
                    }
                }
            }
            std::panic::resume_unwind(panic);
        }
    }
}

fn should_join_mounted_fuse_session(
    test_result: &std::thread::Result<MountedOperationGuard>,
    unmount_result: &io::Result<()>,
) -> bool {
    test_result.is_ok() && unmount_result.is_ok()
}

fn join_mounted_fuse_session_until(
    session_handle: std::thread::JoinHandle<io::Result<()>>,
    timeout: Duration,
) -> Option<std::thread::Result<io::Result<()>>> {
    let deadline = std::time::Instant::now() + timeout;
    while !session_handle.is_finished() {
        let now = std::time::Instant::now();
        if now >= deadline {
            eprintln!("FUSE session thread did not exit within {timeout:?}; skipping join");
            return None;
        }
        std::thread::sleep(join_poll_sleep_duration(deadline.duration_since(now)));
    }
    Some(session_handle.join())
}

fn join_poll_sleep_duration(remaining: Duration) -> Duration {
    remaining.min(Duration::from_millis(25))
}

fn fuse_mount_path_cleared(mount_path: &Path, pre_dev: u64) -> bool {
    std::fs::metadata(mount_path)
        .map(|metadata| {
            use std::os::unix::fs::MetadataExt;
            metadata.dev() == pre_dev
        })
        .unwrap_or_else(fuse_mount_metadata_error_is_cleared)
}

fn fuse_mount_metadata_error_is_cleared(error: io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound
}

fn wait_for_fuse_mount_path_to_clear(mount_path: &Path, pre_dev: u64, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while !fuse_mount_path_cleared(mount_path, pre_dev) {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    true
}

fn force_unmount_fuse_mount_path(mount_path: &Path) -> io::Result<()> {
    force_unmount_fuse_mount_path_with(mount_path, |mount_path| {
        umount_fuse_mount_path_command(mount_path).status()
    })
}

fn umount_fuse_mount_path_command(mount_path: &Path) -> Command {
    let (program, args) = forced_fuse_unmount_command_parts(std::env::consts::OS);
    let mut command = Command::new(program);
    command.args(args).arg(mount_path);
    command
}

fn forced_fuse_unmount_command_parts(target_os: &str) -> (&'static str, &'static [&'static str]) {
    if target_os == "linux" {
        ("fusermount3", &["-u", "-z"])
    } else {
        ("umount", &["-f"])
    }
}

fn force_unmount_fuse_mount_path_with(
    mount_path: &Path,
    run_umount: impl FnOnce(&Path) -> io::Result<ExitStatus>,
) -> io::Result<()> {
    let umount_failure = match run_umount(mount_path) {
        Ok(status) if status.success() => return Ok(()),
        Ok(status) => format!("system umount exited with status {status}"),
        Err(error) => format!("system umount failed to execute: {error}"),
    };

    Err(io::Error::other(umount_failure))
}

fn reconcile_fuse_unmount_result(
    unmount_result: io::Result<()>,
    force_unmount: impl FnOnce() -> io::Result<()>,
    wait_for_clear: impl FnOnce() -> bool,
) -> io::Result<()> {
    if unmount_result.is_ok() {
        return force_unmount().and_then(|()| {
            if wait_for_clear() {
                Ok(())
            } else {
                Err(io::Error::other(
                    "FUSE mount remained active after system umount",
                ))
            }
        });
    }

    match force_unmount() {
        Ok(()) => {
            if let Some(message) = forced_unmount_clearance_warning(wait_for_clear()) {
                eprintln!("{message}");
            }
        }
        Err(error) => {
            eprintln!("system umount also failed after fuser unmount failure: {error}");
        }
    }
    unmount_result
}

fn forced_unmount_clearance_warning(cleared: bool) -> Option<&'static str> {
    if cleared {
        None
    } else {
        Some("FUSE mount remained active after forced system unmount reported success")
    }
}

#[test]
fn mounted_fuse_cleanup_preserves_original_scenario_panic() {
    let scenario_panic: std::thread::Result<MountedOperationGuard> =
        Err(Box::new("scenario failed") as Box<dyn std::any::Any + Send>);
    let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        finish_mounted_fuse_test(
            scenario_panic,
            Err(io::Error::other("unmount failed")),
            None,
        );
    }))
    .expect_err("cleanup should resume the original scenario panic");

    assert_eq!(
        observed.downcast_ref::<&str>(),
        Some(&"scenario failed"),
        "cleanup errors must not replace the original scenario panic"
    );
}

#[test]
fn mounted_fuse_cleanup_reports_joined_session_panic() {
    let observed = std::panic::catch_unwind(|| {
        finish_mounted_fuse_test(
            Ok(MountedOperationGuard::default()),
            Ok(()),
            Some(Err(
                Box::new("session crashed") as Box<dyn std::any::Any + Send>
            )),
        );
    })
    .expect_err("session panic should be reported before unmount failure");

    assert_eq!(
        observed.downcast_ref::<&str>(),
        Some(&"session crashed"),
        "joined session thread panic must be reported"
    );
}

#[test]
fn mounted_fuse_cleanup_allows_skipping_join_after_cleanup_failure() {
    let observed = std::panic::catch_unwind(|| {
        finish_mounted_fuse_test(
            Ok(MountedOperationGuard::default()),
            Err(io::Error::other("unmount failed")),
            None,
        );
    })
    .expect_err("unmount failure should still be reported when join is skipped");

    let message = observed
        .downcast_ref::<String>()
        .expect("unmount failure panic should carry a string message");
    assert!(message.contains("FUSE unmount should succeed"));
    assert!(message.contains("unmount failed"));
}

#[test]
fn mounted_fuse_cleanup_skips_join_when_scenario_panicked_or_unmount_failed() {
    let scenario_panic: std::thread::Result<MountedOperationGuard> =
        Err(Box::new("scenario failed") as Box<dyn std::any::Any + Send>);
    let scenario_ok = Ok(MountedOperationGuard::default());
    let unmount_ok = Ok(());

    assert!(!should_join_mounted_fuse_session(
        &scenario_panic,
        &unmount_ok
    ));
    assert!(!should_join_mounted_fuse_session(
        &scenario_ok,
        &Err(io::Error::other("unmount failed"))
    ));
    assert!(should_join_mounted_fuse_session(&scenario_ok, &unmount_ok));
}

#[test]
fn mounted_fuse_cleanup_bounds_session_join_wait() {
    let session_handle = std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(250));
        Ok(())
    });
    let started = std::time::Instant::now();

    let join_result = join_mounted_fuse_session_until(session_handle, Duration::from_millis(10));

    assert!(join_result.is_none());
    assert!(started.elapsed() < Duration::from_millis(200));
}

#[test]
fn mounted_fuse_cleanup_caps_join_poll_sleep_to_remaining_timeout() {
    assert_eq!(
        join_poll_sleep_duration(Duration::from_millis(40)),
        Duration::from_millis(25)
    );
    assert_eq!(
        join_poll_sleep_duration(Duration::from_millis(10)),
        Duration::from_millis(10)
    );
}

#[test]
fn mounted_fuse_cleanup_detects_mount_path_device_restoration() {
    let mount_dir = target_tempdir("mount-detection-");
    use std::os::unix::fs::MetadataExt;
    let original_dev = std::fs::metadata(mount_dir.path())
        .expect("mount detection tempdir metadata")
        .dev();

    assert!(fuse_mount_path_cleared(mount_dir.path(), original_dev));
    assert!(!fuse_mount_path_cleared(
        mount_dir.path(),
        original_dev.wrapping_add(1)
    ));
}

#[test]
fn mounted_fuse_cleanup_treats_only_not_found_metadata_errors_as_cleared() {
    assert!(fuse_mount_metadata_error_is_cleared(io::Error::new(
        io::ErrorKind::NotFound,
        "mount directory removed",
    )));
    assert!(!fuse_mount_metadata_error_is_cleared(io::Error::other(
        "transport endpoint is not connected",
    )));
}

#[test]
fn mounted_fuse_cleanup_uses_forced_system_unmount_commands() {
    let mount_path = Path::new("/tmp/studiofs-test-mount");
    let (expected_program, expected_args) = forced_fuse_unmount_command_parts(std::env::consts::OS);
    let umount_command = umount_fuse_mount_path_command(mount_path);
    let umount_args = umount_command
        .get_args()
        .map(OsString::from)
        .collect::<Vec<_>>();

    let expected_args = expected_args
        .iter()
        .map(OsString::from)
        .chain(std::iter::once(mount_path.as_os_str().to_os_string()))
        .collect::<Vec<_>>();

    assert_eq!(umount_command.get_program(), expected_program);
    assert_eq!(umount_args, expected_args);
}

#[test]
fn mounted_fuse_cleanup_uses_unprivileged_linux_fuse_unmount_command() {
    let (program, args) = forced_fuse_unmount_command_parts("linux");

    assert_eq!(program, "fusermount3");
    assert_eq!(args, &["-u", "-z"]);
}

#[test]
fn mounted_fuse_cleanup_waits_after_forced_unmount_on_failed_primary_unmount() {
    let waited = std::cell::Cell::new(false);

    let result = reconcile_fuse_unmount_result(
        Err(io::Error::other("primary unmount failed")),
        || Ok(()),
        || {
            waited.set(true);
            true
        },
    );

    assert!(result.is_err());
    assert!(waited.get());
}

#[test]
fn mounted_fuse_cleanup_warns_when_forced_unmount_reports_success_but_mount_remains() {
    assert_eq!(forced_unmount_clearance_warning(true), None);
    assert_eq!(
        forced_unmount_clearance_warning(false),
        Some("FUSE mount remained active after forced system unmount reported success")
    );
}

fn with_mounted_fuse(
    mode: MountedFuseMode,
    suffix: &str,
    test_fn: impl FnOnce(&Path) -> MountedOperationGuard,
) {
    if !mounted_tests_enabled() {
        return;
    }

    let tmp = target_tempdir("vfs-");
    let mount_dir = target_tempdir("mount-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, suffix, &rt);
    let fs = StudioFuseFs::new_with_runtime_options(
        vfs,
        rt.handle().clone(),
        StudioFuseRuntimeOptions {
            async_write_enabled: mode.async_write_enabled(),
            linux_async_dio_enabled: mode.async_dio_enabled(),
            ..StudioFuseRuntimeOptions::default()
        },
    );

    let options = vec![fuser::MountOption::FSName(format!("studiofs-{suffix}"))];

    let mount_path = mount_dir.path().to_path_buf();
    use std::os::unix::fs::MetadataExt;
    let pre_dev = std::fs::metadata(&mount_path)
        .expect("mount dir should exist before session creation")
        .dev();

    let mut session = fuser::Session::new(fs, &mount_path, &options).expect("create FUSE session");
    let mut unmount = session.unmount_callable();
    let session_handle = std::thread::spawn(move || session.run());

    let test_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if std::fs::metadata(&mount_path)
                .map(|m| m.dev() != pre_dev)
                .unwrap_or(false)
            {
                break;
            }
            assert!(
                !session_handle.is_finished(),
                "FUSE session thread exited early before mount became available"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "FUSE mount did not become available within 10 s"
            );
            std::thread::sleep(Duration::from_millis(200));
        }

        test_fn(&mount_path)
    }));
    let unmount_result = unmount.unmount();
    // The unmount call above must happen while scenario guards are still alive;
    // after that, release them so fallback cleanup is not blocked by test files.
    let test_result = test_result.map(|_guard| MountedOperationGuard::default());
    let unmount_result =
        if wait_for_fuse_mount_path_to_clear(&mount_path, pre_dev, Duration::from_secs(2)) {
            unmount_result
        } else {
            eprintln!("FUSE mount remained active after fuser unmount; trying system umount");
            reconcile_fuse_unmount_result(
                unmount_result,
                || force_unmount_fuse_mount_path(&mount_path),
                || wait_for_fuse_mount_path_to_clear(&mount_path, pre_dev, Duration::from_secs(2)),
            )
        };
    let join_result = if should_join_mounted_fuse_session(&test_result, &unmount_result) {
        join_mounted_fuse_session_until(session_handle, Duration::from_secs(5))
    } else {
        None
    };

    finish_mounted_fuse_test(test_result, unmount_result, join_result);
}

fn run_mounted_scenario(scenario: MountedAsyncScenario) {
    if !scenario.platform.matches_current() {
        eprintln!(
            "skipping {} on this platform; scenario target is {:?}",
            scenario.name, scenario.platform
        );
        return;
    }
    with_mounted_fuse(scenario.mode, scenario.name, |mount_path| {
        run_mounted_operation(mount_path, scenario.operation)
    });
}

fn run_mounted_operation(
    mount_path: &Path,
    operation: MountedScenarioOperation,
) -> MountedOperationGuard {
    match operation {
        MountedScenarioOperation::SequentialWriteReadback => {
            let test_file = mount_path.join("sequential.txt");
            let payload = b"StudioFS async FUSE sequential payload";
            std::fs::write(&test_file, payload).expect("write through FUSE mount");
            let readback = std::fs::read(&test_file).expect("read through FUSE mount");
            assert_eq!(readback, payload);
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::WriteFlushReadback => {
            let test_file = mount_path.join("flush.txt");
            let payload = b"StudioFS async FUSE flush payload";
            {
                let mut file = std::fs::File::create(&test_file).expect("create file");
                file.write_all(payload).expect("write payload");
                file.flush().expect("flush file handle");
            }
            let readback = std::fs::read(&test_file).expect("read after flush and close");
            assert_eq!(readback, payload);
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::WriteFsyncReadback => {
            let test_file = mount_path.join("fsync.txt");
            let payload = b"StudioFS async FUSE fsync payload";
            let mut file = std::fs::File::create(&test_file).expect("create file");
            file.write_all(payload).expect("write payload");
            file.sync_all().expect("fsync file");
            drop(file);
            let readback = std::fs::read(&test_file).expect("read after fsync");
            assert_eq!(readback, payload);
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::CloseReopenReadback => {
            let test_file = mount_path.join("close-reopen.txt");
            let payload = b"StudioFS async FUSE close/reopen payload";
            {
                let mut file = std::fs::File::create(&test_file).expect("create file");
                file.write_all(payload).expect("write payload");
            }
            let readback = std::fs::read(&test_file).expect("read reopened file");
            assert_eq!(readback, payload);
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::MultipleWriters => {
            let test_file = mount_path.join("multiple-writers.txt");
            let mut first = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&test_file)
                .expect("open first writer");
            let mut second = std::fs::OpenOptions::new()
                .append(true)
                .open(&test_file)
                .expect("open second writer");
            first.write_all(b"writer-one\n").expect("write first chunk");
            second
                .write_all(b"writer-two\n")
                .expect("write second chunk");
            first.sync_all().expect("sync first writer");
            drop(first);
            drop(second);
            let readback = std::fs::read_to_string(&test_file).expect("read combined writes");
            assert!(readback.contains("writer-one"));
            assert!(readback.contains("writer-two"));
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::SameHandleAppends => {
            let test_file = mount_path.join("same-handle.txt");
            let mut file = std::fs::File::create(&test_file).expect("create file");
            file.write_all(b"first\n").expect("write first chunk");
            file.write_all(b"second\n").expect("write second chunk");
            file.sync_all().expect("sync appended chunks");
            drop(file);
            let readback = std::fs::read_to_string(&test_file).expect("read appended file");
            assert_eq!(readback, "first\nsecond\n");
            MountedOperationGuard::default()
        }
        MountedScenarioOperation::WriteOnlyUntilUnmount => {
            let test_file = mount_path.join("write-only-until-unmount.bin");
            let mut file = std::fs::File::create(&test_file).expect("create write-only file");
            for chunk in 0..64u8 {
                file.write_all(&[chunk; 16 * 1024])
                    .expect("write chunk that should drain during unmount");
            }
            MountedOperationGuard {
                _open_files: vec![file],
            }
        }
        MountedScenarioOperation::FioMultiJob => {
            run_fio_workload_if_available(mount_path);
            MountedOperationGuard::default()
        }
    }
}

fn command_path_is_executable(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

fn command_available(command: &str) -> bool {
    std::env::var_os("PATH").is_some_and(|paths| {
        std::env::split_paths(&paths).any(|path| command_path_is_executable(&path.join(command)))
    })
}

fn run_fio_workload_if_available(mount_path: &Path) {
    if !command_available("fio") {
        eprintln!("skipping fio mounted workload because fio is not installed");
        return;
    }

    let mut directory_arg = OsString::from("--directory=");
    directory_arg.push(mount_path);
    let status = Command::new("fio")
        .arg("--name=studiofs-fuse-async")
        .arg(directory_arg)
        .arg("--filename=fio.bin")
        .arg("--rw=write")
        .arg("--bs=64k")
        .arg("--size=1m")
        .arg("--numjobs=2")
        .arg("--ioengine=sync")
        .arg("--fsync=1")
        .arg("--group_reporting")
        .status()
        .expect("run fio workload");
    assert!(status.success(), "fio workload should exit successfully");
}

/// Verifies that creating and dropping a FUSE session does not panic or leak.
///
/// This test is intentionally ignored unless `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1`
/// is set to avoid requiring a mounted FUSE environment in normal CI runs.
#[test]
#[ignore = "requires /dev/fuse and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn fuse_session_mount_and_drop_smoke() {
    if !mounted_tests_enabled() {
        return;
    }

    let tmp = target_tempdir("vfs-");
    let mount_dir = target_tempdir("mount-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "mount_sanity", &rt);
    let fs = StudioFuseFs::new(vfs, rt.handle().clone());
    let options = vec![fuser::MountOption::FSName("studiofs-test".into())];

    let session = fuser::Session::new(fs, mount_dir.path(), &options).expect("create FUSE session");
    drop(session);
}

/// Full lifecycle smoke test: mount → write → read → readdir → delete → unmount.
///
/// Uses the low-level `fuser::Session` API directly (not `mount_and_serve`)
/// because the latter calls `std::process::exit(0)` after unmount, which would
/// kill the test harness.
///
/// Mount options intentionally omit `AllowOther` to avoid requiring
/// `user_allow_other` in `/etc/fuse.conf`, making this runnable on CI hosted
/// runners that have `/dev/fuse` available.
#[test]
#[ignore = "requires /dev/fuse and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn fuse_mount_write_read_unmount_smoke() {
    if !mounted_tests_enabled() {
        return;
    }

    let tmp = target_tempdir("vfs-");
    let mount_dir = target_tempdir("mount-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "write_read_smoke", &rt);
    let fs = StudioFuseFs::new(vfs, rt.handle().clone());

    let options = vec![fuser::MountOption::FSName("studiofs-smoke".into())];

    let mount_path = mount_dir.path().to_path_buf();

    // Capture the device ID before FUSE mounts over the directory.
    // When FUSE is active the kernel overlays a new device, so dev() changes.
    use std::os::unix::fs::MetadataExt;
    let pre_dev = std::fs::metadata(&mount_path)
        .expect("mount dir should exist before session creation")
        .dev();

    println!("Creating FUSE session at {:?}...", mount_path);
    let mut session = fuser::Session::new(fs, &mount_path, &options).expect("create FUSE session");
    let mut unmount = session.unmount_callable();

    // Run the FUSE event loop in a background thread.
    println!("Starting FUSE background loop...");
    let session_handle = std::thread::spawn(move || {
        if let Err(e) = session.run() {
            eprintln!("FUSE session error (may be normal during unmount): {e}");
        }
        println!("FUSE background loop exited.");
    });

    // Poll until the FUSE filesystem is overlaid (device ID diverges from pre-mount value).
    println!("Waiting for FUSE mount to become available...");
    // A plain is_ok() check would pass immediately because the temp dir already exists.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if std::fs::metadata(&mount_path)
            .map(|m| m.dev() != pre_dev)
            .unwrap_or(false)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "FUSE mount did not become available within 10 s"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── Smoke: create + write ───────────────────────────────────────
    let test_file = mount_path.join("smoke.txt");
    let payload = b"StudioFS FUSE smoke-test payload";
    std::fs::write(&test_file, payload).expect("write through FUSE mount");

    // ── Smoke: read-back ────────────────────────────────────────────
    let readback = std::fs::read(&test_file).expect("read through FUSE mount");
    assert_eq!(readback, payload, "read-back must match written data");

    // ── Smoke: directory listing ────────────────────────────────────
    let entries: Vec<_> = std::fs::read_dir(&mount_path)
        .expect("readdir through FUSE mount")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name())
        .collect();
    assert!(
        entries
            .iter()
            .any(|n| n == std::ffi::OsStr::new("smoke.txt")),
        "directory listing must contain the created file, got: {entries:?}"
    );

    // ── Smoke: delete ───────────────────────────────────────────────
    std::fs::remove_file(&test_file).expect("delete through FUSE mount");

    // ── Clean unmount ───────────────────────────────────────────────
    unmount.unmount().expect("FUSE unmount should succeed");
    session_handle
        .join()
        .expect("FUSE session thread should exit cleanly");
}
