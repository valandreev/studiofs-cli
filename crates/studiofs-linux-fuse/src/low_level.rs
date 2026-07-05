use super::*;

pub(crate) mod raw {
    use super::*;

    pub(crate) enum FuseSession {}

    #[repr(C)]
    pub(crate) struct FuseArgs {
        pub(crate) argc: c_int,
        pub(crate) argv: *mut *mut c_char,
        pub(crate) allocated: c_int,
    }

    #[repr(C)]
    pub(crate) struct FuseLowLevelOps {
        _private: [u8; 0],
    }

    #[link(name = "fuse3")]
    unsafe extern "C" {
        pub(crate) fn fuse_opt_free_args(args: *mut FuseArgs);
        pub(crate) fn fuse_session_mount(
            session: *mut FuseSession,
            mountpoint: *const c_char,
        ) -> c_int;
        pub(crate) fn fuse_session_loop(session: *mut FuseSession) -> c_int;
        pub(crate) fn fuse_session_unmount(session: *mut FuseSession);
        pub(crate) fn fuse_session_destroy(session: *mut FuseSession);
        pub(crate) fn fuse_set_signal_handlers(session: *mut FuseSession) -> c_int;
        pub(crate) fn fuse_remove_signal_handlers(session: *mut FuseSession);
    }
}

unsafe extern "C" {
    fn studiofs_linux_fuse_empty_low_level_ops() -> *const raw::FuseLowLevelOps;
    fn studiofs_linux_fuse_low_level_ops_size() -> libc::size_t;
    fn studiofs_linux_fuse_session_new(
        args: *mut raw::FuseArgs,
        ops: *const raw::FuseLowLevelOps,
        ops_size: libc::size_t,
        userdata: *mut c_void,
    ) -> *mut raw::FuseSession;
}

fn argv_strings(volume_name: &str) -> Vec<String> {
    vec![
        "studiofs-linux-fuse".to_owned(),
        "-o".to_owned(),
        format!("fsname={volume_name}"),
        "-o".to_owned(),
        "subtype=studiofs".to_owned(),
        "-o".to_owned(),
        "auto_unmount".to_owned(),
    ]
}

fn path_to_cstring(path: &std::path::Path) -> io::Result<CString> {
    use std::os::unix::ffi::OsStrExt;

    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Linux FUSE mount point contains an interior NUL byte",
        )
    })
}

struct OwnedFuseArgs {
    _strings: Vec<CString>,
    _argv: Vec<*mut c_char>,
    raw: raw::FuseArgs,
}

impl OwnedFuseArgs {
    fn new(args: Vec<String>) -> io::Result<Self> {
        let strings = args
            .into_iter()
            .map(|arg| {
                CString::new(arg).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Linux FUSE argument contains an interior NUL byte",
                    )
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut argv = strings
            .iter()
            .map(|arg| arg.as_ptr().cast_mut())
            .collect::<Vec<_>>();
        let raw = raw::FuseArgs {
            argc: argv.len().try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "too many Linux FUSE arguments")
            })?,
            argv: argv.as_mut_ptr(),
            allocated: 0,
        };
        Ok(Self {
            _strings: strings,
            _argv: argv,
            raw,
        })
    }

    fn as_mut_ptr(&mut self) -> *mut raw::FuseArgs {
        &mut self.raw
    }
}

impl Drop for OwnedFuseArgs {
    fn drop(&mut self) {
        // SAFETY: libfuse accepts a stack-owned fuse_args with allocated == 0;
        // this frees only libfuse-owned replacements if option parsing made any.
        unsafe { raw::fuse_opt_free_args(&mut self.raw) };
    }
}

pub(crate) struct Session {
    raw: NonNull<raw::FuseSession>,
    signal_handlers_installed: bool,
}

impl Session {
    pub(crate) fn mount(
        mount_point: impl Into<PathBuf>,
        volume_name: impl AsRef<str>,
    ) -> io::Result<Self> {
        let mount_point = path_to_cstring(&mount_point.into())?;
        let mut args = OwnedFuseArgs::new(argv_strings(volume_name.as_ref()))?;
        // SAFETY: the C shim expands libfuse's versioned session constructor.
        // The empty ops table is static, and this skeleton uses null userdata.
        let raw = NonNull::new(unsafe {
            studiofs_linux_fuse_session_new(
                args.as_mut_ptr(),
                studiofs_linux_fuse_empty_low_level_ops(),
                studiofs_linux_fuse_low_level_ops_size(),
                std::ptr::null_mut(),
            )
        })
        .ok_or_else(|| io::Error::other("libfuse3 failed to create low-level session"))?;
        // SAFETY: `raw` is a live libfuse session and `mount_point` is a valid
        // NUL-terminated path for this call.
        let mount_result = unsafe { raw::fuse_session_mount(raw.as_ptr(), mount_point.as_ptr()) };
        if mount_result == 0 {
            Ok(Self {
                raw,
                signal_handlers_installed: false,
            })
        } else {
            // SAFETY: `raw` has not been returned and must be destroyed here.
            unsafe { raw::fuse_session_destroy(raw.as_ptr()) };
            Err(io::Error::last_os_error())
        }
    }

    pub(crate) fn install_signal_handlers(&mut self) -> io::Result<()> {
        // SAFETY: `self.raw` is a live libfuse session.
        let result = unsafe { raw::fuse_set_signal_handlers(self.raw.as_ptr()) };
        if result == 0 {
            self.signal_handlers_installed = true;
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(crate) fn run(&mut self) -> io::Result<()> {
        // SAFETY: `self.raw` is a live libfuse session owned by this wrapper.
        let result = unsafe { raw::fuse_session_loop(self.raw.as_ptr()) };
        if result == 0 || result == libc::SIGINT || result == libc::SIGTERM {
            Ok(())
        } else if result < 0 {
            Err(io::Error::from_raw_os_error(-result))
        } else {
            Err(io::Error::from_raw_os_error(result))
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if self.signal_handlers_installed {
            // SAFETY: handlers were installed for this live session.
            unsafe { raw::fuse_remove_signal_handlers(self.raw.as_ptr()) };
        }
        // SAFETY: libfuse tolerates unmount after regular kernel disconnects;
        // it is needed after signal-triggered exits to avoid a stuck mount.
        unsafe { raw::fuse_session_unmount(self.raw.as_ptr()) };
        // SAFETY: `self.raw` is owned by this wrapper and destroyed once.
        unsafe { raw::fuse_session_destroy(self.raw.as_ptr()) };
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use std::time::Duration;

    #[test]
    fn argv_uses_system_libfuse_mount_options() {
        let args = super::argv_strings("demo");

        assert!(args.iter().any(|arg| arg == "studiofs-linux-fuse"));
        assert!(args.iter().any(|arg| arg == "fsname=demo"));
        assert!(args.iter().any(|arg| arg == "subtype=studiofs"));
        assert!(args.iter().any(|arg| arg == "auto_unmount"));
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_smoke_mounts_and_unmounts_cleanly() {
        let mount_point =
            std::env::temp_dir().join(format!("studiofs-linux-fuse-smoke-{}", std::process::id()));
        std::fs::create_dir_all(&mount_point).expect("create smoke mount point");

        let mut session =
            super::Session::mount(&mount_point, "studiofs-smoke").expect("mount libfuse3 session");
        let mount_point_for_unmount = mount_point.clone();
        let unmount = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(250));
            Command::new("fusermount3")
                .arg("-u")
                .arg(&mount_point_for_unmount)
                .status()
                .expect("run fusermount3 -u")
        });

        session
            .install_signal_handlers()
            .expect("install libfuse3 signal handlers");
        session.run().expect("run libfuse3 session");
        assert!(unmount.join().expect("join unmount thread").success());
        drop(session);
        std::fs::remove_dir_all(&mount_point).expect("remove smoke mount point");
    }
}
