//! Smoke tests for the clean Linux bridge shell.

use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use studiofs_data::DataStoreConfig;
use studiofs_linux::{LinuxMountRequest, LinuxMountShell, mount_with_request};
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, FormatInfo, MetaFormat, MetadataConfig, ROOT_INO, RedbMetaEngine,
};
use studiofs_vfs::{StudioVfs, VfsConfig, VfsConfigValidated};
use tokio::runtime::Runtime;

fn make_format_info(volume_suffix: &str) -> FormatInfo {
    let volume_name = format!("linux-shell-{volume_suffix}-{}", std::process::id());
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: volume_name.clone(),
        uuid: format!("linux-shell-uuid-{volume_suffix}"),
        endpoint: "http://127.0.0.1:9000".into(),
        bucket: "studiofs-test".into(),
        access_key: "test".into(),
        secret_key: "test".into(),
        region: "us-east-1".into(),
        data_prefix: format!("{volume_name}/objects/"),
        metadata: MetadataConfig::default(),
        encryption: studiofs_meta::EncryptionConfig::new(
            [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
        ),
        block_size: 4 * 1024 * 1024,
    }
}

fn make_vfs(tmp: &tempfile::TempDir) -> (Runtime, Arc<StudioVfs>) {
    let runtime = Runtime::new().expect("runtime");
    let _guard = runtime.enter();
    let meta = Arc::new(RedbMetaEngine::open(tmp.path().join("meta.redb")).expect("open redb"));
    meta.init_root(current_uid(), current_gid())
        .expect("init root");
    let format_info = make_format_info("smoke");
    meta.set_format_info(&format_info).expect("set format info");
    let data_config = DataStoreConfig::from(&format_info);
    let cache_dir = tmp.path().join("cache");
    let vfs = StudioVfs::new(
        meta,
        &data_config,
        cache_dir.to_str().expect("cache dir utf8"),
        64 * 1024 * 1024,
        32 * 1024 * 1024,
        VfsConfigValidated::try_from(VfsConfig::default()).expect("valid vfs config"),
    )
    .expect("create vfs");
    vfs.start().expect("start vfs");
    (runtime, vfs)
}

fn current_uid() -> u32 {
    // SAFETY: getuid has no preconditions.
    unsafe { libc::getuid() }
}

fn current_gid() -> u32 {
    // SAFETY: getgid has no preconditions.
    unsafe { libc::getgid() }
}

struct MountedFuse {
    mount_point: PathBuf,
    join: Option<thread::JoinHandle<io::Result<()>>>,
}

impl Drop for MountedFuse {
    fn drop(&mut self) {
        let _ = unmount(&self.mount_point);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn mount_background(
    vfs: Arc<StudioVfs>,
    runtime: &Runtime,
    mount_point: PathBuf,
) -> io::Result<MountedFuse> {
    let request = LinuxMountRequest {
        vfs,
        runtime_handle: runtime.handle().clone(),
        mount_point: mount_point.clone(),
    };
    let join = thread::spawn(move || mount_with_request(request));
    for _ in 0..100 {
        if mountinfo_contains(&mount_point)? {
            return Ok(MountedFuse {
                mount_point,
                join: Some(join),
            });
        }
        if join.is_finished() {
            return match join.join() {
                Ok(Err(error)) => Err(error),
                Ok(Ok(())) => Err(io::Error::other("FUSE mount exited before mountinfo")),
                Err(_) => Err(io::Error::other("mount thread panicked")),
            };
        }
        thread::sleep(Duration::from_millis(50));
    }
    let mounted = MountedFuse {
        mount_point,
        join: Some(join),
    };
    drop(mounted);
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "timed out waiting for FUSE mount",
    ))
}

fn mountinfo_contains(mount_point: &Path) -> io::Result<bool> {
    let target = std::fs::canonicalize(mount_point)?;
    let target = target.to_string_lossy();
    Ok(std::fs::read_to_string("/proc/self/mountinfo")?
        .lines()
        .any(|line| line.split_whitespace().nth(4) == Some(target.as_ref())))
}

fn unmount(mount_point: &Path) -> io::Result<()> {
    let target = CString::new(mount_point.as_os_str().as_bytes())?;
    let rc = unsafe { libc::umount2(target.as_ptr(), libc::MNT_DETACH) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct TreeStats {
    files: usize,
    bytes: u64,
}

fn regular_file_stats(path: &Path) -> io::Result<TreeStats> {
    let mut stats = TreeStats::default();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.path().symlink_metadata()?;
        if metadata.is_dir() {
            let child = regular_file_stats(&entry.path())?;
            stats.files += child.files;
            stats.bytes += child.bytes;
        } else if metadata.is_file() {
            stats.files += 1;
            stats.bytes += metadata.len();
        }
    }
    Ok(stats)
}

fn zero_byte_dotfiles(path: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        let metadata = entry_path.symlink_metadata()?;
        if metadata.is_dir() {
            files.extend(zero_byte_dotfiles(&entry_path)?);
        } else if metadata.is_file()
            && metadata.len() == 0
            && entry.file_name().as_encoded_bytes().starts_with(b".")
        {
            files.push(entry_path);
        }
    }
    Ok(files)
}

fn make_repo_tree(root: &Path) -> io::Result<()> {
    std::fs::create_dir_all(root.join("src"))?;
    std::fs::create_dir_all(root.join("target/debug"))?;
    std::fs::write(root.join("Cargo.toml"), b"[package]\nname = \"sample\"\n")?;
    std::fs::write(root.join("src/lib.rs"), b"pub fn answer() -> u8 { 42 }\n")?;
    std::fs::write(root.join(".gitignore"), b"/target\n")?;
    std::fs::write(root.join("target/debug/cache.bin"), vec![7_u8; 4096])?;
    std::os::unix::fs::symlink("src/lib.rs", root.join("lib-link.rs"))?;
    std::fs::hard_link(root.join("Cargo.toml"), root.join("Cargo.lock.link"))?;
    Ok(())
}

#[test]
fn shell_answers_root_lifecycle_callbacks() {
    let tmp = studiofs_test_support::workspace_target_tempdir("linux-shell-");
    let (runtime, vfs) = make_vfs(&tmp);
    let mut shell = LinuxMountShell::new(LinuxMountRequest {
        vfs: vfs.clone(),
        runtime_handle: runtime.handle().clone(),
        mount_point: tmp.path().join("mnt"),
    });

    shell.init().expect("init");
    let root = shell.getattr(ROOT_INO).expect("root getattr");
    let entries = shell.readdir(ROOT_INO).expect("root readdir");
    let dot = shell.lookup(ROOT_INO, b".").expect("root lookup");
    shell.destroy().expect("destroy");

    assert_eq!(root.ino, ROOT_INO);
    assert_eq!(dot.ino, ROOT_INO);
    assert!(entries.is_empty());

    runtime.block_on(vfs.shutdown(Duration::ZERO));
}

#[test]
fn unsupported_requests_return_enosys() {
    let err = LinuxMountShell::unsupported().expect_err("read unsupported");

    assert_eq!(err.raw_os_error(), Some(libc::ENOSYS));
}

#[test]
#[ignore = "requires /dev/fuse and mount(2) permission"]
fn mounted_cp_a_repo_copy_preserves_regular_file_count_and_bytes() {
    let tmp = studiofs_test_support::workspace_target_tempdir("linux-mount-cp-a-");
    let source = tmp.path().join("source-repo");
    let mount_point = tmp.path().join("mnt");
    std::fs::create_dir(&mount_point).expect("create mount point");
    make_repo_tree(&source).expect("create source repo");
    let expected = regular_file_stats(&source).expect("source stats");

    let (runtime, vfs) = make_vfs(&tmp);
    let mount = mount_background(vfs.clone(), &runtime, mount_point.clone()).expect("mount FUSE");
    let copies = mount_point.join("copies");
    std::fs::create_dir(&copies).expect("mkdir through FUSE");
    let status = std::process::Command::new("cp")
        .arg("-a")
        .arg(&source)
        .arg(&copies)
        .status()
        .expect("run cp -a");
    assert!(status.success(), "cp -a exited with {status}");
    let actual = regular_file_stats(&copies.join("source-repo")).expect("copied stats");

    drop(mount);
    runtime.block_on(vfs.shutdown(Duration::ZERO));

    assert_eq!(actual, expected);
}

#[test]
#[ignore = "requires /dev/fuse and mount(2) permission"]
fn mounted_rsync_a_repo_copy_preserves_regular_files_without_zero_byte_temp_leftovers() {
    let tmp = studiofs_test_support::workspace_target_tempdir("linux-mount-rsync-a-");
    let source = tmp.path().join("source-repo");
    let mount_point = tmp.path().join("mnt");
    std::fs::create_dir(&mount_point).expect("create mount point");
    make_repo_tree(&source).expect("create source repo");
    let expected = regular_file_stats(&source).expect("source stats");

    let (runtime, vfs) = make_vfs(&tmp);
    let mount = mount_background(vfs.clone(), &runtime, mount_point.clone()).expect("mount FUSE");
    let copy = mount_point.join("rsync-copy");
    std::fs::create_dir(&copy).expect("mkdir through FUSE");
    let status = std::process::Command::new("rsync")
        .arg("-a")
        .arg(format!("{}/", source.display()))
        .arg(&copy)
        .status()
        .expect("run rsync -a");
    assert!(status.success(), "rsync -a exited with {status}");
    let actual = regular_file_stats(&copy).expect("copied stats");
    let leftovers = zero_byte_dotfiles(&copy).expect("temp leftovers");

    drop(mount);
    runtime.block_on(vfs.shutdown(Duration::ZERO));

    assert_eq!(actual, expected);
    assert!(
        leftovers.is_empty(),
        "zero-byte temp leftovers: {leftovers:?}"
    );
}
