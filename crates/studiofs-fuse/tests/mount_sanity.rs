//! Sanity-level coverage for the Linux raw `/dev/fuse` backend.

#![cfg(target_os = "linux")]

use std::ffi::OsString;
use std::io;
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use studiofs_data::store::DataStoreConfig;
use studiofs_fuse::{RawFuseMountRequest, mount_raw_with_request};
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, MetaFormat, MetadataConfig, RedbMetaEngine,
};
use studiofs_vfs::{StudioVfs, VfsConfig};

const MOUNT_TESTS_ENV: &str = "STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS";

fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root")
        .to_path_buf()
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

fn fuse_mount_metadata_error_is_cleared(error: io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound
}

fn fuse_mount_path_cleared(mount_path: &Path, pre_dev: u64) -> bool {
    std::fs::metadata(mount_path)
        .map(|metadata| {
            use std::os::unix::fs::MetadataExt;
            metadata.dev() == pre_dev
        })
        .unwrap_or_else(fuse_mount_metadata_error_is_cleared)
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

fn join_mounted_fuse_session_until(
    session_handle: std::thread::JoinHandle<io::Result<()>>,
    timeout: Duration,
) -> Option<std::thread::Result<io::Result<()>>> {
    let deadline = std::time::Instant::now() + timeout;
    while !session_handle.is_finished() {
        let now = std::time::Instant::now();
        if now >= deadline {
            eprintln!("raw FUSE session thread did not exit within {timeout:?}; skipping join");
            return None;
        }
        std::thread::sleep(Duration::from_millis(25).min(deadline.duration_since(now)));
    }
    Some(session_handle.join())
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
    command.stdout(Stdio::null()).stderr(Stdio::null());
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

fn with_raw_fuse_mount(suffix: &str, read_only: bool, test_fn: impl FnOnce(&Path)) {
    if !mounted_tests_enabled() {
        return;
    }

    let tmp = studiofs_test_support::workspace_target_tempdir("vfs-");
    let mount_dir = studiofs_test_support::workspace_target_tempdir("mount-");
    let mount_path = mount_dir.path().to_path_buf();
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("runtime"));
    let vfs = make_test_vfs(&tmp, suffix, &rt);

    use std::os::unix::fs::MetadataExt;
    let pre_dev = std::fs::metadata(&mount_path)
        .expect("mount dir should exist before raw session")
        .dev();

    let mut request = RawFuseMountRequest::new(mount_path.to_string_lossy(), "studiofs-raw-smoke")
        .with_vfs(vfs, rt.handle().clone());
    request.read_only = read_only;
    let session_handle = std::thread::spawn(move || mount_raw_with_request(request));

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while fuse_mount_path_cleared(&mount_path, pre_dev) {
        assert!(
            std::time::Instant::now() < deadline,
            "raw FUSE mount did not become available within 10 s"
        );
        if session_handle.is_finished() {
            let result = session_handle
                .join()
                .expect("raw FUSE session thread should not panic");
            panic!("raw FUSE session exited before mount became available: {result:?}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let test_result = catch_unwind(AssertUnwindSafe(|| test_fn(&mount_path)));
    let unmount_result = force_unmount_fuse_mount_path(&mount_path);
    let cleared = wait_for_fuse_mount_path_to_clear(&mount_path, pre_dev, Duration::from_secs(5));
    let join_result = if test_result.is_ok() && unmount_result.is_ok() && cleared {
        join_mounted_fuse_session_until(session_handle, Duration::from_secs(5))
    } else {
        None
    };

    if let Err(panic) = test_result {
        resume_unwind(panic);
    }
    unmount_result.expect("raw FUSE unmount should succeed");
    assert!(cleared, "raw FUSE mount should clear after unmount");
    if let Some(join_result) = join_result {
        join_result
            .expect("raw FUSE session thread should not panic")
            .expect("raw FUSE session should exit cleanly");
    }
}

#[test]
fn mounted_fuse_cleanup_detects_mount_path_device_restoration() {
    let mount_dir = studiofs_test_support::workspace_target_tempdir("mount-detection-");
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
fn force_unmount_fuse_mount_path_reports_failed_system_unmount() {
    let result = force_unmount_fuse_mount_path_with(Path::new("/tmp/studiofs-test-mount"), |_| {
        Err(io::Error::other("missing command"))
    });

    assert!(result.unwrap_err().to_string().contains("missing command"));
}

#[test]
#[ignore = "requires /dev/fuse and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn raw_fuse_mount_and_unmount_smoke() {
    with_raw_fuse_mount("raw_mount_sanity", true, |mount_path| {
        for entry in std::fs::read_dir(mount_path).expect("read raw FUSE root directory") {
            entry.expect("read raw FUSE root directory entry");
        }
    });
}

#[test]
#[ignore = "requires /dev/fuse and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn raw_fuse_write_read_unmount_smoke() {
    with_raw_fuse_mount("raw_write_read", false, |mount_path| {
        let test_file = mount_path.join("write-read.txt");
        let payload = b"StudioFS raw FUSE write/read payload";

        std::fs::write(&test_file, payload).expect("write through raw FUSE mount");
        let readback = std::fs::read(&test_file).expect("read through raw FUSE mount");

        assert_eq!(readback, payload);
    });
}

#[test]
#[ignore = "requires /dev/fuse and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn raw_fuse_flush_readback_smoke() {
    with_raw_fuse_mount("raw_flush_readback", false, |mount_path| {
        let test_file = mount_path.join("flush.txt");
        let payload = b"StudioFS raw FUSE flush payload";
        let mut file = std::fs::File::create(&test_file).expect("create file");

        file.write_all(payload).expect("write payload");
        file.flush().expect("flush file handle");
        drop(file);

        let readback = std::fs::read(&test_file).expect("read after flush and close");
        assert_eq!(readback, payload);
    });
}

#[test]
#[ignore = "requires /dev/fuse and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn raw_fuse_fsync_readback_smoke() {
    with_raw_fuse_mount("raw_fsync_readback", false, |mount_path| {
        let test_file = mount_path.join("fsync.txt");
        let payload = b"StudioFS raw FUSE fsync payload";
        let mut file = std::fs::File::create(&test_file).expect("create file");

        file.write_all(payload).expect("write payload");
        file.sync_all().expect("fsync file");
        drop(file);

        let readback = std::fs::read(&test_file).expect("read after fsync");
        assert_eq!(readback, payload);
    });
}

#[test]
#[ignore = "requires /dev/fuse and STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1"]
fn raw_fuse_close_reopen_readback_smoke() {
    with_raw_fuse_mount("raw_close_reopen", false, |mount_path| {
        let test_file = mount_path.join("close-reopen.txt");
        let payload = b"StudioFS raw FUSE close/reopen payload";
        {
            let mut file = std::fs::File::create(&test_file).expect("create file");
            file.write_all(payload).expect("write payload");
        }

        let readback = std::fs::read(&test_file).expect("read reopened file");
        assert_eq!(readback, payload);
    });
}
