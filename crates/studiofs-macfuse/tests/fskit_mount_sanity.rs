//! Native macOS FSKit mount sanity tests.

#![cfg(all(target_os = "macos", studiofs_fskit))]

use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use studiofs_macfuse::{
    MacFuseMountRequest, MacFuseUnmountSignal, SignalHandling, mount_with_fskit_request,
};
use studiofs_vfs::contract_harness::{CONTRACT_ROOT_INO, ContractVfsFixture};

const FS_KIT_READINESS_TIMEOUT: Duration = Duration::from_secs(5);
const FS_KIT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);

fn mount_root() -> PathBuf {
    let root = std::env::var_os("STUDIOFS_FSKIT_MOUNT_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_dir()
                .expect("resolve FSKit test checkout")
                .join("target")
        });
    assert!(
        root.is_absolute() && root.starts_with(Path::new("/Volumes")),
        "FSKit mount root must be an absolute path below /Volumes: {}",
        root.display()
    );
    root
}

fn create_mount_point(label: &str) -> PathBuf {
    let mount_point = mount_root().join(format!("{label}-{}", std::process::id()));
    if let Err(error) = std::fs::remove_dir(&mount_point)
        && error.kind() != io::ErrorKind::NotFound
    {
        panic!(
            "remove stale FSKit test mount point {}: {error}",
            mount_point.display()
        );
    }
    std::fs::create_dir_all(&mount_point).unwrap_or_else(|error| {
        panic!(
            "create FSKit test mount point {}: {error}",
            mount_point.display()
        )
    });
    mount_point
}

fn mount_worker_finished_within(worker: &JoinHandle<io::Result<()>>, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !worker.is_finished() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        thread::sleep(remaining.min(Duration::from_millis(10)));
    }
    true
}

fn join_mount_worker(worker: JoinHandle<io::Result<()>>, mount_label: &str) {
    if !mount_worker_finished_within(&worker, FS_KIT_CLEANUP_TIMEOUT) {
        eprintln!(
            "{mount_label} mount worker did not stop within {:?}; terminating the isolated native test process",
            FS_KIT_CLEANUP_TIMEOUT
        );
        std::process::exit(1);
    }
    worker
        .join()
        .unwrap_or_else(|_| panic!("{mount_label} mount worker panicked"))
        .unwrap_or_else(|error| panic!("{mount_label} mount failed: {error}"));
}

fn wait_for_fskit_mount_ready(
    mount: JoinHandle<io::Result<()>>,
    target: &Path,
    unmount_signal: &MacFuseUnmountSignal,
    timeout: Duration,
    mount_label: &str,
) -> JoinHandle<io::Result<()>> {
    let ready_deadline = Instant::now() + timeout;
    loop {
        // A kernel lookup can block while the FSKit provider is activating;
        // probe it in a child so a failed provider cannot hang this test.
        let mut probe = Command::new("/usr/bin/stat")
            .args(["-f", "%i"])
            .arg(target)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start FSKit readiness probe");
        let probe_deadline = Instant::now() + Duration::from_millis(250);
        let ready = loop {
            if let Some(status) = probe.try_wait().expect("poll FSKit readiness probe") {
                break status.success();
            }
            if Instant::now() >= probe_deadline {
                probe.kill().expect("stop hung FSKit readiness probe");
                probe.wait().expect("reap hung FSKit readiness probe");
                break false;
            }
            thread::sleep(Duration::from_millis(10));
        };
        if ready {
            return mount;
        }
        if mount.is_finished() {
            let result = mount
                .join()
                .unwrap_or_else(|_| panic!("{mount_label} mount worker panicked"));
            panic!("{mount_label} mount exited before readiness: {result:?}");
        }
        if Instant::now() >= ready_deadline {
            unmount_signal.request_unmount();
            join_mount_worker(mount, mount_label);
            panic!("{mount_label} mount did not become ready");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn mount_worker_polling_has_a_bounded_deadline() {
    let worker = thread::spawn(|| {
        thread::sleep(Duration::from_millis(100));
        Ok(())
    });
    assert!(!mount_worker_finished_within(
        &worker,
        Duration::from_millis(10)
    ));
    worker
        .join()
        .expect("join bounded polling test worker")
        .unwrap();
}

#[test]
#[ignore = "requires installed MFMount.framework and an internal /Volumes mount root"]
fn fskit_mounted_lookup_open_read_unmount_smoke() {
    let fixture =
        ContractVfsFixture::new("macfuse-fskit-mounted-read-callback").expect("create VFS fixture");
    let file_name = b"fskit-read.bin";
    let payload = b"hello from macFUSE FSKit";
    let attr = fixture
        .vfs()
        .create(CONTRACT_ROOT_INO, file_name, 501, 20, 0o644)
        .expect("create FSKit smoke file");
    fixture
        .runtime()
        .block_on(fixture.vfs().write(attr.ino, 0, payload))
        .expect("seed FSKit smoke payload");

    let mount_point = create_mount_point("studiofs-fskit-read-smoke");
    let unmount_signal = MacFuseUnmountSignal::new();
    let request = MacFuseMountRequest::new(
        fixture.vfs(),
        fixture.runtime().handle().clone(),
        mount_point.clone(),
        "StudioFS FSKit Read Smoke",
    )
    .with_read_only(true)
    .with_run_in_foreground(true)
    .with_signal_handling(SignalHandling::Disabled)
    .with_unmount_signal(unmount_signal.clone());
    let mount = thread::spawn(move || mount_with_fskit_request(request));
    let target = mount_point.join(std::str::from_utf8(file_name).unwrap());
    let mount = wait_for_fskit_mount_ready(
        mount,
        &target,
        &unmount_signal,
        FS_KIT_READINESS_TIMEOUT,
        "FSKit read smoke",
    );

    assert_eq!(
        std::fs::read(&target).expect("read through FSKit mount"),
        payload
    );
    unmount_signal.request_unmount();
    join_mount_worker(mount, "FSKit read smoke");
    std::fs::remove_dir(&mount_point).expect("remove FSKit smoke mount point");

    fixture
        .runtime()
        .block_on(fixture.vfs().shutdown(Duration::ZERO))
        .expect("FSKit mounted VFS shutdown should succeed");
    fixture
        .runtime()
        .block_on(fixture.vfs().shutdown(Duration::ZERO))
        .expect("repeated FSKit mounted VFS shutdown should succeed");
}

#[test]
#[ignore = "requires installed MFMount.framework and an internal /Volumes mount root"]
fn fskit_cancelled_mount_leaves_no_volume() {
    let fixture = ContractVfsFixture::new("macfuse-fskit-cancelled-mount").expect("create fixture");
    let mount_point = create_mount_point("studiofs-fskit-cancel");
    let signal = MacFuseUnmountSignal::new();
    signal.request_unmount();
    let request = MacFuseMountRequest::new(
        fixture.vfs(),
        fixture.runtime().handle().clone(),
        mount_point.clone(),
        "StudioFS FSKit Cancel Smoke",
    )
    .with_read_only(true)
    .with_run_in_foreground(true)
    .with_signal_handling(SignalHandling::Disabled)
    .with_unmount_signal(signal);
    mount_with_fskit_request(request).expect("complete cancelled mount");
    std::fs::remove_dir(&mount_point).expect("cancelled mount must leave no mounted volume");
}

#[test]
#[ignore = "requires installed MFMount.framework and an internal /Volumes mount root"]
fn fskit_mounted_hardlink_unlink_smoke() {
    let fixture =
        ContractVfsFixture::new("macfuse-fskit-mounted-hardlink").expect("create VFS fixture");
    let source_name = b"fskit-hardlink-source.bin";
    let source_attr = fixture
        .vfs()
        .create(CONTRACT_ROOT_INO, source_name, 501, 20, 0o644)
        .expect("create FSKit hardlink source");
    fixture
        .runtime()
        .block_on(fixture.vfs().write(source_attr.ino, 0, b"before"))
        .expect("seed FSKit hardlink source");

    let mount_point = create_mount_point("studiofs-fskit-hardlink-smoke");
    let unmount_signal = MacFuseUnmountSignal::new();
    let request = MacFuseMountRequest::new(
        fixture.vfs(),
        fixture.runtime().handle().clone(),
        mount_point.clone(),
        "StudioFS FSKit Hardlink Smoke",
    )
    .with_run_in_foreground(true)
    .with_signal_handling(SignalHandling::Disabled)
    .with_unmount_signal(unmount_signal.clone());
    let mount = thread::spawn(move || mount_with_fskit_request(request));
    let source = mount_point.join(std::str::from_utf8(source_name).unwrap());
    let alias = mount_point.join("fskit-hardlink-alias.bin");
    let mount = wait_for_fskit_mount_ready(
        mount,
        &source,
        &unmount_signal,
        FS_KIT_READINESS_TIMEOUT,
        "FSKit hardlink",
    );

    std::fs::hard_link(&source, &alias).expect("create hardlink through FSKit mount");
    assert_eq!(
        std::fs::metadata(&source)
            .expect("stat FSKit hardlink source")
            .ino(),
        std::fs::metadata(&alias)
            .expect("stat FSKit hardlink alias")
            .ino()
    );
    std::fs::write(&source, b"after").expect("write through FSKit mount");
    assert_eq!(
        std::fs::read(&alias).expect("read updated FSKit hardlink alias"),
        b"after"
    );

    std::fs::remove_file(&source).expect("remove one FSKit hardlink name");
    assert!(
        matches!(
            std::fs::read(&source),
            Err(error) if error.kind() == io::ErrorKind::NotFound
        ),
        "deleted FSKit hardlink name remains visible"
    );
    assert_eq!(
        std::fs::read(&alias).expect("remaining FSKit hardlink must stay readable"),
        b"after"
    );

    unmount_signal.request_unmount();
    join_mount_worker(mount, "FSKit hardlink");
    std::fs::remove_dir(&mount_point).expect("remove FSKit hardlink mount point");

    fixture
        .runtime()
        .block_on(fixture.vfs().shutdown(Duration::ZERO))
        .expect("FSKit hardlink VFS shutdown should succeed");
    fixture
        .runtime()
        .block_on(fixture.vfs().shutdown(Duration::ZERO))
        .expect("repeated FSKit hardlink VFS shutdown should succeed");
}
