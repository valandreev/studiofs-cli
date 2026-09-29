use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::time::Duration;
use studiofs_vfs::contract_harness::{CONTRACT_ROOT_INO, ContractVfsFixture};

#[test]
#[ignore = "requires a native FUSE mount and a non-root caller with supplementary groups"]
fn mounted_access_selects_the_callers_permission_class() {
    run_access_checks(false, false);
}

#[test]
#[ignore = "requires a native FUSE mount"]
fn mounted_read_only_access_rejects_write() {
    run_access_checks(true, false);
}

#[cfg(target_os = "macos")]
mod mfmount_acceptance {
    #[cfg(feature = "fskit-mount-sanity")]
    #[test]
    #[ignore = "requires the native MFMount backend"]
    fn mounted_mfmount_access_selects_the_callers_permission_class() {
        super::run_access_checks(false, true);
    }

    #[cfg(feature = "fskit-mount-sanity")]
    #[test]
    #[ignore = "requires the native MFMount backend"]
    fn mounted_mfmount_read_only_access_rejects_write() {
        super::run_access_checks(true, true);
    }
}

fn run_access_checks(read_only: bool, mfmount: bool) {
    // SAFETY: These credential queries have no pointer arguments.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    assert_ne!(uid, 0, "root bypasses ordinary permission classes");
    // SAFETY: A zero length queries the required buffer size.
    let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
    assert!(count >= 0, "query supplementary groups");
    let mut groups = vec![0; count as usize];
    // SAFETY: The buffer has space for count gid_t entries.
    let count = unsafe { libc::getgroups(count, groups.as_mut_ptr()) };
    assert!(count >= 0, "read supplementary groups");
    groups.truncate(count as usize);
    let supplementary = supplementary_gid(&groups, gid, read_only);
    let foreign_uid = unrelated_uid(uid);
    let foreign_gid = (1..65535)
        .find(|&group| group != gid && !groups.contains(&group))
        .expect("find unrelated group");
    let cases = [
        (
            "supplementary-no-fallthrough",
            foreign_uid,
            supplementary,
            0o004,
            false,
            libc::R_OK,
            false,
        ),
        (
            "owner-write",
            uid,
            foreign_gid,
            0o200,
            false,
            libc::W_OK,
            true,
        ),
        (
            "group-write",
            foreign_uid,
            gid,
            0o020,
            false,
            libc::W_OK,
            true,
        ),
        (
            "owner-read",
            uid,
            foreign_gid,
            if read_only { 0o600 } else { 0o400 },
            false,
            libc::R_OK,
            true,
        ),
        (
            "owner-no-fallthrough",
            uid,
            gid,
            0o044,
            false,
            libc::R_OK,
            false,
        ),
        (
            "primary-group-read",
            foreign_uid,
            gid,
            0o040,
            false,
            libc::R_OK,
            true,
        ),
        (
            "group-no-fallthrough",
            foreign_uid,
            gid,
            0o004,
            false,
            libc::R_OK,
            false,
        ),
        (
            "supplementary-group-read",
            foreign_uid,
            supplementary,
            0o040,
            false,
            libc::R_OK,
            true,
        ),
        (
            "other-read",
            foreign_uid,
            foreign_gid,
            0o004,
            false,
            libc::R_OK,
            true,
        ),
        (
            "other-cannot-use-owner",
            foreign_uid,
            foreign_gid,
            0o400,
            false,
            libc::R_OK,
            false,
        ),
        (
            "other-cannot-use-group",
            foreign_uid,
            foreign_gid,
            0o040,
            false,
            libc::R_OK,
            false,
        ),
        (
            "combined-mask",
            uid,
            foreign_gid,
            0o400,
            false,
            libc::R_OK | libc::W_OK,
            false,
        ),
        ("file-no-execute", uid, gid, 0o666, false, libc::X_OK, false),
        (
            "directory-no-search",
            uid,
            gid,
            0o644,
            true,
            libc::X_OK,
            false,
        ),
        (
            "directory-owner-no-fallthrough",
            uid,
            gid,
            0o011,
            true,
            libc::X_OK,
            false,
        ),
        ("directory-search", uid, gid, 0o100, true, libc::X_OK, true),
        (
            "existence-without-rights",
            uid,
            gid,
            0,
            false,
            libc::F_OK,
            true,
        ),
    ];
    let fixture = ContractVfsFixture::new("native-posix-access").expect("create fixture");
    for &(name, owner, group, mode, directory, _, _) in &cases {
        if directory {
            fixture
                .vfs()
                .mkdir(CONTRACT_ROOT_INO, name.as_bytes(), owner, group, mode)
                .expect("create access directory");
        } else {
            fixture
                .vfs()
                .create(CONTRACT_ROOT_INO, name.as_bytes(), owner, group, mode)
                .expect("create access file");
        }
    }
    let mount_point = native_mount_point();
    std::fs::create_dir_all(&mount_point).expect("create mount point");
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    #[cfg(target_os = "macos")]
    let signal = studiofs_macfuse::MacFuseUnmountSignal::new();
    #[cfg(target_os = "macos")]
    let request = studiofs_macfuse::MacFuseMountRequest::new(
        fixture.vfs(),
        fixture.runtime().handle().clone(),
        &mount_point,
        "POSIX Access",
    )
    .with_read_only(read_only)
    .with_signal_handling(studiofs_macfuse::SignalHandling::Disabled)
    .with_unmount_signal(signal.clone())
    .with_mount_ready_callback(move || {
        ready_tx.send(()).expect("report ready mount");
    });
    #[cfg(target_os = "linux")]
    let request = studiofs_linux_fuse::LinuxFuseMountRequest::new(
        fixture.vfs(),
        fixture.runtime().handle().clone(),
        &mount_point,
        "posix-access",
        read_only,
    )
    .with_mount_ready_callback(move || {
        ready_tx.send(()).expect("report ready mount");
    });
    #[cfg(target_os = "macos")]
    let worker = std::thread::spawn(move || {
        if mfmount {
            studiofs_macfuse::mount_with_fskit_request(request)
        } else {
            studiofs_macfuse::mount_with_request(request)
        }
    });
    #[cfg(target_os = "linux")]
    let _ = mfmount;
    #[cfg(target_os = "linux")]
    let worker = std::thread::spawn(move || studiofs_linux_fuse::mount_with_request(request));
    let result = std::panic::catch_unwind(|| {
        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("native mount ready");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::fs::metadata(mount_point.join("owner-read")).is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "mounted lookup did not become ready"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        if read_only {
            let path =
                CString::new(mount_point.join("owner-read").as_os_str().as_bytes()).expect("path");
            // SAFETY: path is a live NUL-terminated string for each synchronous call.
            assert_eq!(unsafe { libc::access(path.as_ptr(), libc::R_OK) }, 0);
            // SAFETY: same live path.
            assert_eq!(unsafe { libc::access(path.as_ptr(), libc::F_OK) }, 0);
            // SAFETY: same live path.
            let write_result = unsafe { libc::access(path.as_ptr(), libc::W_OK) };
            let write_error = (write_result != 0).then(std::io::Error::last_os_error);
            assert_eq!(write_result, -1, "read-only mount must deny W_OK");
            assert_eq!(
                write_error.expect("failed W_OK errno").raw_os_error(),
                Some(libc::EROFS)
            );
            return;
        }
        for &(name, _, _, _, _, mask, allowed) in &cases {
            let path = CString::new(mount_point.join(name).as_os_str().as_bytes()).expect("path");
            // SAFETY: path is a live NUL-terminated string for this synchronous call.
            let result = unsafe { libc::access(path.as_ptr(), mask) };
            let error = (result != 0).then(std::io::Error::last_os_error);
            assert_eq!(
                result == 0,
                allowed,
                "{name}: access returned {result}, error: {error:?}"
            );
            if !allowed {
                assert_eq!(
                    error.expect("failed access errno").raw_os_error(),
                    Some(libc::EACCES),
                    "{name}"
                );
            }
        }
    });
    #[cfg(target_os = "macos")]
    signal.request_unmount();
    #[cfg(target_os = "macos")]
    let unmount_result = Ok(());
    #[cfg(target_os = "linux")]
    let unmount_result = std::process::Command::new("fusermount3")
        .arg("-u")
        .arg(&mount_point)
        .status()
        .and_then(|status| {
            if status.success() {
                Ok(())
            } else {
                Err(std::io::Error::other("fusermount3 unmount failed"))
            }
        });
    // Always join before interpreting unmount errors, including failed startup.
    let mount_result = worker.join();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let directory_result = loop {
        match std::fs::remove_dir(&mount_point) {
            Ok(()) => break Ok(()),
            Err(error)
                if error.raw_os_error() == Some(libc::EBUSY)
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => break Err(error),
        }
    };
    finish_access_test(result, mount_result, unmount_result.and(directory_result));
}

fn unrelated_uid(uid: u32) -> u32 {
    if uid == 1 { 2 } else { 1 }
}

fn native_mount_point() -> std::path::PathBuf {
    studiofs_test_support::workspace_target_temp_path("native-posix-access-")
}

#[test]
fn unrelated_uid_is_nonzero_and_different_at_boundaries() {
    for uid in [1, 501, u32::MAX] {
        let other = unrelated_uid(uid);
        assert_ne!(other, uid);
        assert_ne!(other, 0);
    }
}

#[test]
fn native_mount_point_is_under_workspace_target() {
    let reference = studiofs_test_support::workspace_target_temp_path("reference-");
    assert_eq!(native_mount_point().parent(), reference.parent());
}

fn finish_access_test(
    probe: std::thread::Result<()>,
    mount: std::thread::Result<std::io::Result<()>>,
    cleanup: std::io::Result<()>,
) {
    if let Err(panic) = probe {
        if let Ok(Err(error)) = &mount {
            eprintln!("mount worker failed: {error}");
        }
        if let Err(error) = cleanup {
            eprintln!("mount cleanup failed: {error}");
        }
        std::panic::resume_unwind(panic);
    }
    cleanup.expect("unmount and remove mount point");
    mount
        .expect("join mount worker")
        .expect("native mount lifecycle");
}

#[test]
fn failed_mount_cleanup_preserves_initial_failure() {
    let result = std::panic::catch_unwind(|| {
        let probe = std::panic::catch_unwind(|| panic!("initial mount failure"));
        let worker = std::thread::spawn(|| Err(std::io::Error::other("startup failure")));
        finish_access_test(
            probe,
            worker.join(),
            Err(std::io::Error::other("not mounted")),
        );
    });
    let panic = result.expect_err("initial failure must propagate");
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"initial mount failure"));
}

#[test]
fn mount_failure_does_not_mask_cleanup_failure() {
    let worker = std::thread::spawn(|| -> std::io::Result<()> {
        panic!("worker failure");
    });
    for mount in [
        Ok(Err(std::io::Error::other("lifecycle failure"))),
        worker.join(),
    ] {
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            finish_access_test(Ok(()), mount, Err(std::io::Error::other("cleanup failure")));
        }))
        .expect_err("cleanup failure must propagate");
        let message = panic
            .downcast_ref::<String>()
            .expect("expect panic message");
        assert!(
            message.contains("unmount and remove mount point"),
            "{message}"
        );
    }
}

fn supplementary_gid(groups: &[u32], gid: u32, read_only: bool) -> u32 {
    if read_only {
        return gid;
    }
    *groups
        .iter()
        .find(|&&group| group != gid && group != 0)
        .expect("native acceptance requires a supplementary group")
}

#[test]
fn read_only_acceptance_does_not_require_supplementary_groups() {
    assert_eq!(supplementary_gid(&[], 20, true), 20);
}

#[test]
fn worker_panic_preserves_initial_probe_failure() {
    let result = std::panic::catch_unwind(|| {
        let probe = std::panic::catch_unwind(|| panic!("initial probe failure"));
        let worker = std::thread::spawn(|| -> std::io::Result<()> {
            panic!("worker root cause");
        });
        finish_access_test(probe, worker.join(), Ok(()));
    });
    assert_eq!(
        result.expect_err("probe must fail").downcast_ref::<&str>(),
        Some(&"initial probe failure")
    );
}
