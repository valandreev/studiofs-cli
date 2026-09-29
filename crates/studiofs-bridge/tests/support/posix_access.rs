use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::time::Duration;
use studiofs_vfs::contract_harness::{CONTRACT_ROOT_INO, ContractVfsFixture};

#[test]
#[ignore = "requires a native FUSE mount and a non-root caller with supplementary groups"]
fn mounted_access_selects_the_callers_permission_class() {
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
    let supplementary = *groups
        .iter()
        .find(|&&group| group != gid && group != 0)
        .expect("native acceptance requires a supplementary group");
    let foreign_uid = uid + 1;
    let foreign_gid = (1..65535)
        .find(|&group| group != gid && !groups.contains(&group))
        .expect("find unrelated group");
    let cases = [
        (
            "owner-read",
            uid,
            foreign_gid,
            0o400,
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
    let mount_point = std::env::current_dir()
        .expect("current directory")
        .join("target")
        .join(format!("native-posix-access-{}", std::process::id()));
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
        false,
    )
    .with_mount_ready_callback(move || {
        ready_tx.send(()).expect("report ready mount");
    });
    #[cfg(target_os = "macos")]
    let worker = std::thread::spawn(move || studiofs_macfuse::mount_with_request(request));
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
        for &(name, _, _, _, _, mask, allowed) in &cases {
            let path = CString::new(mount_point.join(name).as_os_str().as_bytes()).expect("path");
            // SAFETY: path is a live NUL-terminated string for this synchronous call.
            let result = unsafe { libc::access(path.as_ptr(), mask) };
            assert_eq!(result == 0, allowed, "{name}: access returned {result}");
            if !allowed {
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EACCES),
                    "{name}"
                );
            }
        }
    });
    #[cfg(target_os = "macos")]
    signal.request_unmount();
    #[cfg(target_os = "linux")]
    assert!(
        std::process::Command::new("fusermount3")
            .arg("-u")
            .arg(&mount_point)
            .status()
            .expect("unmount")
            .success()
    );
    worker
        .join()
        .expect("join mount worker")
        .expect("native mount lifecycle");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        match std::fs::remove_dir(&mount_point) {
            Ok(()) => break,
            Err(error)
                if error.raw_os_error() == Some(libc::EBUSY)
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("remove mount point: {error}"),
        }
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
