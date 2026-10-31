#![doc = "Linux libfuse3 bridge boundary for StudioFS."]
#![cfg(target_os = "linux")]
#![deny(missing_docs)]

//! Linux libfuse3 bridge boundary for StudioFS.

use std::ffi::CString;
use std::fmt;
use std::io;
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::Arc;
use studiofs_vfs::StudioVfs;
use tokio::runtime::Handle;
mod async_writes;
mod cache_policy;
mod directory_cache;
mod invalidation;
mod low_level;
mod open_directories;

/// Linux libfuse3 queue bounds and callback worker policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinuxFuseConnectionConfig {
    /// Maximum number of background kernel requests queued to the daemon.
    pub max_background: u32,
    /// Kernel congestion threshold for applying request backpressure.
    pub congestion_threshold: u32,
    /// Requested callback workers; `None` uses `max(16, 2 * CPUs)` on libfuse 3.12+.
    /// Positive values are capped at `max_background` and 128 workers.
    /// Explicit overrides require libfuse 3.12+; older builds keep CPU-based idle retention.
    pub worker_threads: Option<u32>,
}

impl Default for LinuxFuseConnectionConfig {
    fn default() -> Self {
        Self {
            max_background: 128,
            congestion_threshold: 96,
            worker_threads: None,
        }
    }
}

impl LinuxFuseConnectionConfig {
    fn validate(self, hard_worker_limit: bool) -> io::Result<Self> {
        if self.worker_threads == Some(0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Linux FUSE worker_threads must be greater than 0",
            ));
        }
        if self.worker_threads.is_some() && !hard_worker_limit {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Linux FUSE worker_threads requires libfuse 3.12+ for a hard worker limit",
            ));
        }
        if self.max_background == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Linux FUSE max_background must be greater than 0",
            ));
        }
        if self.congestion_threshold == 0 || self.congestion_threshold > self.max_background {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Linux FUSE congestion_threshold must be in 1..=max_background",
            ));
        }
        Ok(self)
    }

    pub(crate) fn worker_thread_count(self, hard_worker_limit: bool) -> u32 {
        let parallelism = std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1);
        self.worker_thread_count_for_parallelism(hard_worker_limit, parallelism)
    }

    fn worker_thread_count_for_parallelism(
        self,
        hard_worker_limit: bool,
        parallelism: usize,
    ) -> u32 {
        let cpu_workers = u32::try_from(parallelism)
            .unwrap_or(u32::MAX)
            .saturating_mul(2);
        if !hard_worker_limit {
            return cpu_workers.clamp(1, self.max_background.max(1));
        }
        // Small hosts get a 16-worker floor for blocking I/O; large hosts keep the old 2x CPU count.
        self.worker_threads
            .unwrap_or(cpu_workers.max(16))
            .clamp(1, self.max_background.clamp(1, 128))
    }
}

fn parse_worker_threads(value: Option<std::ffi::OsString>) -> io::Result<Option<u32>> {
    value
        .map(|value| {
            value
                .to_str()
                .and_then(|value| value.parse::<u32>().ok())
                .filter(|count| *count > 0)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "STUDIOFS_LINUX_FUSE_WORKER_THREADS must be a positive u32",
                    )
                })
        })
        .transpose()
}

/// A request-level worker count takes precedence over the environment override.
fn resolve_worker_threads(
    mut config: LinuxFuseConnectionConfig,
    env_value: Option<std::ffi::OsString>,
) -> io::Result<LinuxFuseConnectionConfig> {
    if config.worker_threads.is_none() {
        config.worker_threads = parse_worker_threads(env_value)?;
    }
    Ok(config)
}

/// Request used by the CLI backend router to launch the Linux libfuse3 bridge.
pub struct LinuxFuseMountRequest {
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    mount_point: PathBuf,
    volume_name: String,
    read_only: bool,
    allow_other: bool,
    connection_config: LinuxFuseConnectionConfig,
    mount_ready_callback: Option<Box<dyn FnOnce() + Send>>,
}

impl LinuxFuseMountRequest {
    /// Create a Linux libfuse3 mount request.
    pub fn new(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        mount_point: impl Into<PathBuf>,
        volume_name: impl Into<String>,
        read_only: bool,
    ) -> Self {
        Self {
            vfs,
            runtime_handle,
            mount_point: mount_point.into(),
            volume_name: volume_name.into(),
            read_only,
            allow_other: false,
            connection_config: LinuxFuseConnectionConfig::default(),
            mount_ready_callback: None,
        }
    }

    /// Let other local users reach the mount through `-o allow_other`.
    ///
    /// `default_permissions` stays enabled, so the kernel still enforces mode
    /// bits and ownership for foreign callers. Non-root mounters need
    /// `user_allow_other` in `/etc/fuse.conf`; [`mount_with_request`] checks it
    /// before calling `fusermount3`.
    pub fn with_allow_other(mut self, allow_other: bool) -> Self {
        self.allow_other = allow_other;
        self
    }

    /// Set a callback invoked after libfuse has attached the mount point.
    pub fn with_mount_ready_callback<F>(mut self, callback: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        self.mount_ready_callback = Some(Box::new(callback));
        self
    }

    /// Override Linux libfuse3 connection-level queue bounds.
    pub fn with_connection_config(mut self, config: LinuxFuseConnectionConfig) -> Self {
        self.connection_config = config;
        self
    }
}

/// Start serving the Linux libfuse3 bridge.
pub fn mount_with_request(request: LinuxFuseMountRequest) -> io::Result<()> {
    ensure_fusermount3_available()?;
    let LinuxFuseMountRequest {
        vfs,
        runtime_handle,
        mount_point,
        volume_name,
        read_only,
        allow_other,
        connection_config,
        mount_ready_callback,
    } = request;
    if allow_other {
        check_allow_other_permitted()?;
    }
    let connection_config = resolve_worker_threads(
        connection_config,
        std::env::var_os("STUDIOFS_LINUX_FUSE_WORKER_THREADS"),
    )?
    .validate(low_level::has_worker_limit())?;
    let mut session = low_level::Session::mount(
        low_level::SessionState::new(vfs, runtime_handle, read_only, connection_config)
            .with_allow_other(allow_other),
        mount_point,
        volume_name,
    )?;
    session.install_signal_handlers()?;
    if let Some(callback) = mount_ready_callback {
        callback();
    }
    session.run()
}

fn ensure_fusermount3_available() -> io::Result<()> {
    check_fusermount3_in_path(std::env::var_os("PATH"))
}

fn check_fusermount3_in_path(path_var: Option<std::ffi::OsString>) -> io::Result<()> {
    let path_var = path_var.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "fusermount3 is required for Linux StudioFS mounts; PATH is not set",
        )
    })?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join("fusermount3");
        if candidate.is_file() {
            return Ok(());
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "fusermount3 is required for Linux StudioFS mounts; install the libfuse3 runtime package",
    ))
}

/// System FUSE configuration that `fusermount3` reads for `user_allow_other`.
pub const FUSE_CONF_PATH: &str = "/etc/fuse.conf";

/// Why a non-root `allow_other` mount cannot be attempted.
#[derive(Debug)]
pub enum AllowOtherPreflightError {
    /// The FUSE configuration is absent or has no `user_allow_other` line.
    MissingUserAllowOther {
        /// Configuration file that was checked.
        path: PathBuf,
    },
    /// The FUSE configuration exists but could not be read.
    UnreadableConfig {
        /// Configuration file that was checked.
        path: PathBuf,
        /// Underlying read failure.
        source: io::Error,
    },
}

impl fmt::Display for AllowOtherPreflightError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingUserAllowOther { path } => write!(
                f,
                "allow_other mounts by a non-root user require the line `user_allow_other` in {}; \
                 add it (as root), or disable allow_other for this mount \
                 (`--allow-other=false` also overrides a saved `studiofs settings` default)",
                path.display()
            ),
            Self::UnreadableConfig { path, source } => write!(
                f,
                "allow_other mounts by a non-root user require `user_allow_other` in {}, \
                 but the file could not be read: {source}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for AllowOtherPreflightError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingUserAllowOther { .. } => None,
            Self::UnreadableConfig { source, .. } => Some(source),
        }
    }
}

impl From<AllowOtherPreflightError> for io::Error {
    fn from(error: AllowOtherPreflightError) -> Self {
        io::Error::new(io::ErrorKind::PermissionDenied, error)
    }
}

/// Check that the effective user may request `allow_other`.
///
/// Root may always request it. Other users need `user_allow_other` in
/// [`FUSE_CONF_PATH`]; without it `fusermount3` rejects the mount with a
/// generic message, so callers should run this before mounting.
pub fn check_allow_other_permitted() -> Result<(), AllowOtherPreflightError> {
    // SAFETY: `geteuid` has no preconditions.
    let euid = unsafe { libc::geteuid() };
    check_allow_other_config(euid, Path::new(FUSE_CONF_PATH))
}

fn check_allow_other_config(euid: u32, path: &Path) -> Result<(), AllowOtherPreflightError> {
    if euid == 0 {
        return Ok(());
    }
    let contents = match std::fs::read(path) {
        // fusermount3 parses bytes; non-UTF-8 comments must not hide the keyword.
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            // Setuid-root fusermount3 can read a config this user cannot
            // (for example 0640 root:fuse), so let it make the decision.
            tracing::warn!(
                path = %path.display(),
                "cannot read FUSE configuration to preflight allow_other; deferring to fusermount3"
            );
            return Ok(());
        }
        Err(source) => {
            return Err(AllowOtherPreflightError::UnreadableConfig {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if fuse_conf_allows_other(&contents) {
        Ok(())
    } else {
        Err(AllowOtherPreflightError::MissingUserAllowOther {
            path: path.to_path_buf(),
        })
    }
}

/// Mirrors `fusermount3`: `#` starts a comment and the keyword is case-sensitive.
fn fuse_conf_allows_other(contents: &str) -> bool {
    contents.lines().any(|line| {
        let line = line.split_once('#').map_or(line, |(before, _)| before);
        line.trim() == "user_allow_other"
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    #[test]
    fn fuse_conf_parser_accepts_user_allow_other_with_surrounding_whitespace() {
        for contents in [
            "user_allow_other",
            "user_allow_other\n",
            "  user_allow_other\t\n",
            "# comment\nmount_max = 1000\n user_allow_other # enabled for render nodes\n",
        ] {
            assert!(super::fuse_conf_allows_other(contents), "{contents:?}");
        }
    }

    #[test]
    fn fuse_conf_parser_ignores_comments_and_near_misses() {
        for contents in [
            "",
            "#user_allow_other\n",
            "  # user_allow_other\n",
            "mount_max = 1000\n",
            "User_allow_other\n",
            "user_allow_others\n",
            "user_allow_other = 1\n",
        ] {
            assert!(!super::fuse_conf_allows_other(contents), "{contents:?}");
        }
    }

    #[test]
    fn allow_other_preflight_names_the_missing_line_for_non_root() {
        let temp = studiofs_test_support::workspace_target_tempdir("fuse-conf-");
        let path = temp.path().join("fuse.conf");
        std::fs::write(&path, "#user_allow_other\n").expect("write fuse.conf");

        let error = super::check_allow_other_config(1000, &path).expect_err("must reject");

        assert!(matches!(
            error,
            super::AllowOtherPreflightError::MissingUserAllowOther { .. }
        ));
        let message = error.to_string();
        assert!(message.contains("`user_allow_other`"), "{message}");
        assert!(message.contains(&path.display().to_string()), "{message}");
        assert_eq!(
            std::io::Error::from(error).kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn allow_other_preflight_reads_non_utf8_config() {
        let temp = studiofs_test_support::workspace_target_tempdir("fuse-conf-");
        let path = temp.path().join("fuse.conf");
        std::fs::write(&path, b"# caf\xe9 render nodes\nuser_allow_other\n").expect("write");

        super::check_allow_other_config(1000, &path).expect("non-UTF-8 comment is ignored");
    }

    #[test]
    fn allow_other_preflight_defers_unreadable_config_to_fusermount3() {
        use std::os::unix::fs::PermissionsExt;

        // SAFETY: `geteuid` has no preconditions.
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipping: root bypasses file mode checks");
            return;
        }
        let temp = studiofs_test_support::workspace_target_tempdir("fuse-conf-");
        let path = temp.path().join("fuse.conf");
        std::fs::write(&path, "").expect("write fuse.conf");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000))
            .expect("make fuse.conf unreadable");

        super::check_allow_other_config(1000, &path)
            .expect("unreadable config defers to setuid fusermount3");
    }

    #[test]
    fn allow_other_preflight_reports_other_read_failures() {
        let temp = studiofs_test_support::workspace_target_tempdir("fuse-conf-");

        let error = super::check_allow_other_config(1000, temp.path())
            .expect_err("a directory is not a readable config");

        assert!(matches!(
            error,
            super::AllowOtherPreflightError::UnreadableConfig { .. }
        ));
    }

    #[test]
    fn allow_other_preflight_treats_missing_config_as_not_allowed() {
        let temp = studiofs_test_support::workspace_target_tempdir("fuse-conf-");

        let error = super::check_allow_other_config(1000, &temp.path().join("absent.conf"))
            .expect_err("missing fuse.conf must reject");

        assert!(matches!(
            error,
            super::AllowOtherPreflightError::MissingUserAllowOther { .. }
        ));
    }

    #[test]
    fn allow_other_preflight_accepts_configured_user_and_root() {
        let temp = studiofs_test_support::workspace_target_tempdir("fuse-conf-");
        let path = temp.path().join("fuse.conf");
        std::fs::write(&path, "user_allow_other\n").expect("write fuse.conf");

        super::check_allow_other_config(1000, &path).expect("configured user");
        super::check_allow_other_config(0, &temp.path().join("absent.conf"))
            .expect("root does not need user_allow_other");
    }

    #[test]
    fn fusermount3_check_reports_missing_path() {
        let err = super::check_fusermount3_in_path(None).expect_err("missing PATH should fail");

        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        assert!(err.to_string().contains("fusermount3"));
    }

    #[test]
    fn fusermount3_check_reports_path_without_binary() {
        let err = super::check_fusermount3_in_path(Some(OsString::from("/definitely/missing")))
            .expect_err("missing fusermount3 should fail");

        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        assert!(err.to_string().contains("fusermount3"));
    }

    #[test]
    fn connection_config_rejects_zero_max_background() {
        let config = super::LinuxFuseConnectionConfig {
            max_background: 0,
            congestion_threshold: 1,
            worker_threads: None,
        };

        assert_eq!(
            config.validate(true).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn connection_config_rejects_threshold_above_max_background() {
        let config = super::LinuxFuseConnectionConfig {
            max_background: 8,
            congestion_threshold: 9,
            worker_threads: None,
        };

        assert_eq!(
            config.validate(true).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn worker_threads_rejects_invalid_environment_values() {
        for value in ["", "0", "-1", "threads", "4294967296"] {
            assert_eq!(
                super::parse_worker_threads(Some(std::ffi::OsString::from(value)))
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::InvalidInput
            );
        }
    }

    #[test]
    fn worker_threads_accepts_positive_override_and_absent_default() {
        assert_eq!(super::parse_worker_threads(None).unwrap(), None);
        assert_eq!(
            super::parse_worker_threads(Some(std::ffi::OsString::from("48"))).unwrap(),
            Some(48)
        );
    }

    #[test]
    fn connection_config_rejects_zero_worker_threads() {
        let config = super::LinuxFuseConnectionConfig {
            worker_threads: Some(0),
            ..Default::default()
        };
        assert_eq!(
            config.validate(true).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn legacy_libfuse_rejects_explicit_worker_limit_but_accepts_default() {
        let config = super::LinuxFuseConnectionConfig {
            worker_threads: Some(16),
            ..Default::default()
        };
        assert_eq!(
            config.validate(false).unwrap_err().kind(),
            std::io::ErrorKind::Unsupported
        );
        assert!(
            super::LinuxFuseConnectionConfig::default()
                .validate(false)
                .is_ok()
        );
    }

    #[test]
    fn worker_threads_caps_oversized_configuration_and_preserves_small_override() {
        for (requested, expected) in [(1, 1), (48, 48), (u32::MAX, 128)] {
            let config = super::LinuxFuseConnectionConfig {
                max_background: 1024,
                worker_threads: Some(requested),
                ..Default::default()
            };
            assert_eq!(
                config.validate(true).unwrap().worker_thread_count(true),
                expected
            );
        }
    }

    #[test]
    fn modern_default_worker_count_uses_floor_or_cpu_surplus_within_queue() {
        let config = super::LinuxFuseConnectionConfig::default();
        for (parallelism, expected) in [(1, 16), (4, 16), (8, 16), (32, 64), (64, 128), (512, 128)]
        {
            assert_eq!(
                config.worker_thread_count_for_parallelism(true, parallelism),
                expected,
                "parallelism {parallelism}"
            );
        }
        let large_queue = super::LinuxFuseConnectionConfig {
            max_background: 1024,
            ..Default::default()
        };
        assert_eq!(
            large_queue.worker_thread_count_for_parallelism(true, 512),
            128
        );
    }

    #[test]
    fn request_worker_threads_take_precedence_over_environment() {
        let request = super::LinuxFuseConnectionConfig {
            worker_threads: Some(24),
            ..Default::default()
        };
        let resolved =
            super::resolve_worker_threads(request, Some(std::ffi::OsString::from("48"))).unwrap();
        assert_eq!(resolved.worker_threads, Some(24));

        let resolved = super::resolve_worker_threads(
            super::LinuxFuseConnectionConfig::default(),
            Some(std::ffi::OsString::from("48")),
        )
        .unwrap();
        assert_eq!(resolved.worker_threads, Some(48));
    }
}
