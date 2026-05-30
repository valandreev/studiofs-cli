//! Sanity-level FUSE test coverage for mount lifecycle and metadata edge cases.

#![cfg(unix)]

use std::collections::BTreeSet;
use std::io;
use std::io::Write;
use std::path::Path;
use std::process::Command;
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
    Macos,
}

impl MountedScenarioPlatform {
    fn matches_current(self) -> bool {
        match self {
            Self::AnyUnix => true,
            Self::Linux => cfg!(target_os = "linux"),
            Self::Macos => cfg!(target_os = "macos"),
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
    Dd,
    FioMultiJob,
    FsyncHeavy,
    MultipleFileCopy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MountedAsyncScenario {
    name: &'static str,
    mode: MountedFuseMode,
    platform: MountedScenarioPlatform,
    operation: MountedScenarioOperation,
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
            #[ignore = "requires /dev/fuse or macFUSE and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
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
        operation: MountedScenarioOperation::CloseReopenReadback
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
    macos_async_mounted_dd => {
        name: "macos-dd",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::Macos,
        operation: MountedScenarioOperation::Dd
    },
    macos_async_mounted_fio => {
        name: "macos-fio",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::Macos,
        operation: MountedScenarioOperation::FioMultiJob
    },
    macos_async_mounted_fsync_heavy => {
        name: "macos-fsync-heavy",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::Macos,
        operation: MountedScenarioOperation::FsyncHeavy
    },
    macos_async_mounted_multiple_file_copy => {
        name: "macos-multiple-file-copy",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::Macos,
        operation: MountedScenarioOperation::MultipleFileCopy
    },
    macos_async_mounted_finder_style_copy => {
        name: "macos-finder-style-copy",
        mode: MountedFuseMode::AsyncWrite,
        platform: MountedScenarioPlatform::Macos,
        operation: MountedScenarioOperation::MultipleFileCopy
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
        "macos-dd",
        "macos-fio",
        "macos-fsync-heavy",
        "macos-multiple-file-copy",
        "macos-finder-style-copy",
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
    test_result: std::thread::Result<()>,
    unmount_result: io::Result<()>,
    join_result: std::thread::Result<io::Result<()>>,
) {
    match test_result {
        Ok(()) => {
            match join_result {
                Ok(run_result) => run_result.expect("FUSE session should run until unmount"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
            unmount_result.expect("FUSE unmount should succeed");
        }
        Err(panic) => {
            if let Err(error) = unmount_result {
                eprintln!("FUSE unmount failed after scenario panic: {error}");
            }
            match join_result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    eprintln!("FUSE session failed after scenario panic: {error}");
                }
                Err(_) => {
                    eprintln!("FUSE session thread panicked after scenario panic");
                }
            }
            std::panic::resume_unwind(panic);
        }
    }
}

#[test]
fn mounted_fuse_cleanup_preserves_original_scenario_panic() {
    let scenario_panic: std::thread::Result<()> =
        Err(Box::new("scenario failed") as Box<dyn std::any::Any + Send>);
    let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        finish_mounted_fuse_test(
            scenario_panic,
            Err(io::Error::other("unmount failed")),
            Ok(Err(io::Error::other("session failed"))),
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
fn mounted_fuse_cleanup_reports_session_panic_before_unmount_error() {
    let observed = std::panic::catch_unwind(|| {
        finish_mounted_fuse_test(
            Ok(()),
            Err(io::Error::other("unmount failed")),
            Err(Box::new("session crashed") as Box<dyn std::any::Any + Send>),
        );
    })
    .expect_err("session panic should be reported before unmount failure");

    assert_eq!(
        observed.downcast_ref::<&str>(),
        Some(&"session crashed"),
        "session thread panic must not be masked by a later unmount error"
    );
}

fn with_mounted_fuse(mode: MountedFuseMode, suffix: &str, test_fn: impl FnOnce(&Path)) {
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

    let mut options = vec![fuser::MountOption::FSName(format!("studiofs-{suffix}"))];
    if !cfg!(target_os = "linux") {
        options.push(fuser::MountOption::AutoUnmount);
    }
    if cfg!(target_os = "macos") {
        options.push(fuser::MountOption::CUSTOM("foreground".into()));
    }

    let mount_path = mount_dir.path().to_path_buf();
    use std::os::unix::fs::MetadataExt;
    let pre_dev = std::fs::metadata(&mount_path)
        .expect("mount dir should exist before session creation")
        .dev();

    let mut session = fuser::Session::new(fs, &mount_path, &options).expect("create FUSE session");
    let mut unmount = session.unmount_callable();
    let session_handle = std::thread::spawn(move || session.run());

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if std::fs::metadata(&mount_path)
            .map(|m| m.dev() != pre_dev)
            .unwrap_or(false)
        {
            break;
        }
        if session_handle.is_finished() {
            let unmount_result = unmount.unmount();
            let join_result = session_handle.join();
            finish_mounted_fuse_test(Ok(()), unmount_result, join_result);
            panic!("FUSE session thread exited early before mount became available");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "FUSE mount did not become available within 10 s"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    let test_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        test_fn(&mount_path);
    }));
    let unmount_result = unmount.unmount();
    let join_result = session_handle.join();

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
        run_mounted_operation(mount_path, scenario.operation);
    });
}

fn run_mounted_operation(mount_path: &Path, operation: MountedScenarioOperation) {
    match operation {
        MountedScenarioOperation::SequentialWriteReadback => {
            let test_file = mount_path.join("sequential.txt");
            let payload = b"StudioFS async FUSE sequential payload";
            std::fs::write(&test_file, payload).expect("write through FUSE mount");
            let readback = std::fs::read(&test_file).expect("read through FUSE mount");
            assert_eq!(readback, payload);
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
        }
        MountedScenarioOperation::Dd => run_dd_workload(mount_path),
        MountedScenarioOperation::FioMultiJob => run_fio_workload_if_available(mount_path),
        MountedScenarioOperation::FsyncHeavy => run_fsync_heavy_workload(mount_path),
        MountedScenarioOperation::MultipleFileCopy => run_multiple_file_copy_workload(mount_path),
    }
}

fn run_dd_workload(mount_path: &Path) {
    let output = mount_path.join("dd.bin");
    let status = Command::new("dd")
        .arg("if=/dev/zero")
        .arg(format!("of={}", output.display()))
        .arg("bs=64k")
        .arg("count=8")
        .arg("conv=fsync")
        .status()
        .expect("run dd workload");
    assert!(status.success(), "dd workload should exit successfully");
    let len = std::fs::metadata(output).expect("dd output metadata").len();
    assert_eq!(len, 512 * 1024);
}

fn command_available(command: &str) -> bool {
    std::env::var_os("PATH")
        .is_some_and(|paths| std::env::split_paths(&paths).any(|path| path.join(command).is_file()))
}

fn run_fio_workload_if_available(mount_path: &Path) {
    if !command_available("fio") {
        eprintln!("skipping fio mounted workload because fio is not installed");
        return;
    }

    let status = Command::new("fio")
        .arg("--name=studiofs-fuse-async")
        .arg(format!("--directory={}", mount_path.display()))
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

fn run_fsync_heavy_workload(mount_path: &Path) {
    let test_file = mount_path.join("fsync-heavy.bin");
    let mut file = std::fs::File::create(&test_file).expect("create fsync-heavy file");
    for chunk in 0..16u8 {
        file.write_all(&[chunk; 4096])
            .expect("write fsync-heavy chunk");
        file.sync_data().expect("sync fsync-heavy chunk");
    }
    drop(file);
    let len = std::fs::metadata(test_file)
        .expect("fsync-heavy output metadata")
        .len();
    assert_eq!(len, 16 * 4096);
}

fn run_multiple_file_copy_workload(mount_path: &Path) {
    let source_dir = mount_path.join("copy-source");
    let target_dir = mount_path.join("copy-target");
    std::fs::create_dir(&source_dir).expect("create copy source dir");
    std::fs::create_dir(&target_dir).expect("create copy target dir");
    for index in 0..4 {
        let source = source_dir.join(format!("file-{index}.txt"));
        let target = target_dir.join(format!("file-{index}.txt"));
        let payload = format!("StudioFS copy payload {index}\n");
        std::fs::write(&source, &payload).expect("write copy source");
        std::fs::copy(&source, &target).expect("copy file through mounted FUSE");
        let readback = std::fs::read_to_string(&target).expect("read copied file");
        assert_eq!(readback, payload);
    }
}

/// Verifies that creating and dropping a FUSE session does not panic or leak.
///
/// This test is intentionally ignored unless `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1`
/// is set to avoid requiring a mounted FUSE environment in normal CI runs.
#[test]
#[ignore = "requires /dev/fuse or macFUSE and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn fuse_session_mount_and_drop_smoke() {
    if !mounted_tests_enabled() {
        return;
    }

    let tmp = target_tempdir("vfs-");
    let mount_dir = target_tempdir("mount-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "mount_sanity", &rt);
    let fs = StudioFuseFs::new(vfs, rt.handle().clone());
    let mut options = vec![fuser::MountOption::FSName("studiofs-test".into())];
    // On Linux, AutoUnmount in fuser v0.16 can implicitly add 'allow_other',
    // which fails on CI. Since we just drop the session here (and move to next test
    // or end), we either need it or we don't. For smoke tests, let's try to be minimal.
    // Use if cfg!() instead of #[cfg] to avoid unused_mut warnings on Linux.
    if !cfg!(target_os = "linux") {
        options.push(fuser::MountOption::AutoUnmount);
    }
    if cfg!(target_os = "macos") {
        options.push(fuser::MountOption::CUSTOM("foreground".into()));
    }

    let session = fuser::Session::new(fs, mount_dir.path(), &options).expect("create FUSE session");
    drop(session);
}

/// Verifies macOS resource fork xattrs can be set, queried, listed, and removed.
///
/// This is macOS-only and validates compatibility with native xattr naming used by
/// Finder metadata paths.
#[cfg(target_os = "macos")]
#[test]
fn macos_resource_fork_roundtrip_uses_native_xattr_store() {
    let tmp = target_tempdir("vfs-");
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let vfs = make_test_vfs(&tmp, "macos_resource_fork_xattr", &rt);

    let file = vfs
        .create(1, b"fork.bin", 0, 0, 0o644)
        .expect("create file for resource fork");
    let fork_name = b"com.apple.ResourceFork";
    let fork_value = vec![0, 1, 2, 3, b'R', b'S', b'R', b'C'];

    vfs.set_xattr(file.ino, fork_name, &fork_value, 0)
        .expect("set resource fork xattr");

    let got = vfs
        .get_xattr(file.ino, fork_name)
        .expect("get resource fork xattr");
    assert_eq!(got, Some(fork_value.clone()));

    let names = vfs.list_xattrs(file.ino).expect("list xattrs");
    assert!(
        names.iter().any(|name| name.as_slice() == fork_name),
        "resource fork key should be visible in native xattr listing"
    );

    vfs.remove_xattr(file.ino, fork_name)
        .expect("remove resource fork xattr");
    let after_remove = vfs
        .get_xattr(file.ino, fork_name)
        .expect("get xattr after remove");
    assert_eq!(after_remove, None);
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
#[ignore = "requires /dev/fuse or macFUSE and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn fuse_mount_write_read_unmount_smoke() {
    if !mounted_tests_enabled() {
        return;
    }

    let tmp = target_tempdir("vfs-");
    let mount_dir = target_tempdir("mount-");
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, "write_read_smoke", &rt);
    let fs = StudioFuseFs::new(vfs, rt.handle().clone());

    let mut options = vec![fuser::MountOption::FSName("studiofs-smoke".into())];
    // Manually unmounting at the end of the test, so AutoUnmount is optional.
    // On Linux CI, AutoUnmount may trigger 'allow_other' checks in fusermount3.
    // Use if cfg!() instead of #[cfg] to avoid unused_mut warnings on Linux.
    if !cfg!(target_os = "linux") {
        options.push(fuser::MountOption::AutoUnmount);
    }
    if cfg!(target_os = "macos") {
        options.push(fuser::MountOption::CUSTOM("foreground".into()));
    }

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
