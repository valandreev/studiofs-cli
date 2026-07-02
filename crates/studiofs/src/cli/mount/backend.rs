use std::io;
use std::sync::Arc;

use studiofs_vfs::StudioVfs;
use tokio::runtime::Handle;

#[cfg(unix)]
pub(crate) const MOUNT_BACKEND_ENV: &str = "STUDIOFS_MOUNT_BACKEND";

#[cfg_attr(all(unix, not(target_os = "linux")), allow(dead_code))]
pub(crate) struct BackendLaunch {
    pub(crate) vfs: Arc<StudioVfs>,
    pub(crate) runtime_handle: Handle,
    pub(crate) mount_point: String,
    pub(crate) volume_name: String,
    pub(crate) read_only: bool,
    #[cfg(all(unix, not(target_os = "linux")))]
    pub(crate) run_in_foreground: bool,
}

pub(crate) trait MountBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn serve(&self, launch: BackendLaunch) -> io::Result<()>;
}

#[cfg(target_os = "linux")]
pub(crate) fn default_mount_backend() -> Box<dyn MountBackend> {
    mount_backend_for_selector(std::env::var(MOUNT_BACKEND_ENV).ok().as_deref())
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) fn default_mount_backend() -> Box<dyn MountBackend> {
    mount_backend_for_selector(std::env::var(MOUNT_BACKEND_ENV).ok().as_deref())
}

#[cfg(windows)]
pub(crate) fn default_mount_backend() -> Box<dyn MountBackend> {
    Box::new(WinfspMountBackend)
}

#[cfg(target_os = "linux")]
pub(crate) fn mount_backend_for_selector(selector: Option<&str>) -> Box<dyn MountBackend> {
    match selector.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) if value.eq_ignore_ascii_case("raw") => Box::new(RawFuseMountBackend),
        Some(value) if value.eq_ignore_ascii_case("fuser") => Box::new(FuseMountBackend),
        Some(value) => {
            tracing::warn!(
                backend = value,
                "unknown Linux mount backend; falling back to fuser"
            );
            Box::new(FuseMountBackend)
        }
        None => Box::new(RawFuseMountBackend),
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn mount_backend_for_selector(selector: Option<&str>) -> Box<dyn MountBackend> {
    match selector.map(str::trim).filter(|value| !value.is_empty()) {
        None => Box::new(MacFuseMountBackend),
        Some(value) if value.eq_ignore_ascii_case("macfuse") => Box::new(MacFuseMountBackend),
        _ => Box::new(UnsupportedUnixMountBackend),
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
pub(crate) fn mount_backend_for_selector(_selector: Option<&str>) -> Box<dyn MountBackend> {
    Box::new(UnsupportedUnixMountBackend)
}

#[cfg(target_os = "linux")]
struct FuseMountBackend;

#[cfg(target_os = "linux")]
impl MountBackend for FuseMountBackend {
    fn name(&self) -> &'static str {
        "FUSE"
    }

    fn serve(&self, launch: BackendLaunch) -> io::Result<()> {
        let mut request = studiofs_fuse::FuseMountRequest::new(
            launch.vfs,
            launch.runtime_handle,
            launch.mount_point,
            launch.volume_name,
        );
        request.read_only = launch.read_only;
        request.signal_handling = studiofs_fuse::SignalHandling::InstallForCli;
        studiofs_fuse::mount_with_request(request)
    }
}

#[cfg(target_os = "linux")]
struct RawFuseMountBackend;

#[cfg(target_os = "linux")]
impl MountBackend for RawFuseMountBackend {
    fn name(&self) -> &'static str {
        "raw-fuse"
    }

    fn serve(&self, launch: BackendLaunch) -> io::Result<()> {
        let mut request =
            studiofs_fuse::RawFuseMountRequest::new(launch.mount_point, launch.volume_name)
                .with_vfs(launch.vfs, launch.runtime_handle);
        request.read_only = launch.read_only;
        studiofs_fuse::mount_raw_with_request(request)
    }
}

#[cfg(target_os = "macos")]
struct MacFuseMountBackend;

#[cfg(target_os = "macos")]
impl MountBackend for MacFuseMountBackend {
    fn name(&self) -> &'static str {
        "macFUSE"
    }

    fn serve(&self, launch: BackendLaunch) -> io::Result<()> {
        let request = studiofs_macfuse::MacFuseMountRequest::new(
            launch.vfs,
            launch.runtime_handle,
            launch.mount_point,
            launch.volume_name,
        )
        .with_read_only(launch.read_only)
        .with_run_in_foreground(launch.run_in_foreground)
        .with_signal_handling(studiofs_macfuse::SignalHandling::InstallForCli);
        studiofs_macfuse::mount_with_request(request)
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
struct UnsupportedUnixMountBackend;

#[cfg(all(unix, not(target_os = "linux")))]
fn unsupported_unix_backend_error_message() -> &'static str {
    if cfg!(target_os = "macos") {
        "StudioFS FUSE backend is Linux-only; macOS uses the native macFUSE bridge by default; clear or set STUDIOFS_MOUNT_BACKEND=macfuse to use it"
    } else {
        "StudioFS FUSE backend is Linux-only; macOS and Windows are supported via their respective native bridges"
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
impl MountBackend for UnsupportedUnixMountBackend {
    fn name(&self) -> &'static str {
        "unsupported-unix"
    }

    fn serve(&self, _launch: BackendLaunch) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            unsupported_unix_backend_error_message(),
        ))
    }
}

#[cfg(windows)]
struct WinfspMountBackend;

#[cfg(windows)]
impl MountBackend for WinfspMountBackend {
    fn name(&self) -> &'static str {
        "WinFSP"
    }

    fn serve(&self, launch: BackendLaunch) -> io::Result<()> {
        let shutdown_event_name = crate::cli::utils::shutdown_event_name(&launch.mount_point);
        studiofs_winfsp::mount_and_serve(studiofs_winfsp::WinfspMountRequest {
            vfs: launch.vfs,
            runtime_handle: launch.runtime_handle,
            mount_point: launch.mount_point,
            volume_label: launch.volume_name,
            read_only: launch.read_only,
            shutdown_event_name: Some(shutdown_event_name),
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_native_selector_routes_to_macfuse_read_only_mvp() {
        let backend = super::mount_backend_for_selector(Some("macfuse"));

        assert_eq!(backend.name(), "macFUSE");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_default_selector_routes_to_macfuse() {
        for selector in [None, Some("")] {
            let backend = super::mount_backend_for_selector(selector);

            assert_eq!(backend.name(), "macFUSE");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_unknown_selector_is_explicitly_unsupported() {
        let backend = super::mount_backend_for_selector(Some("other"));

        assert_eq!(backend.name(), "unsupported-unix");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_unsupported_backend_error_mentions_default_native_bridge() {
        let message = super::unsupported_unix_backend_error_message();

        assert!(message.contains(super::MOUNT_BACKEND_ENV));
        assert!(message.contains("macFUSE"));
    }

    #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
    #[test]
    fn non_linux_unix_default_backend_is_explicitly_unsupported() {
        let backend = super::mount_backend_for_selector(None);

        assert_eq!(backend.name(), "unsupported-unix");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_raw_selector_routes_to_raw_fuse_backend() {
        let backend = super::mount_backend_for_selector(Some("raw"));

        assert_eq!(backend.name(), "raw-fuse");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_default_selector_routes_to_raw_fuse_backend() {
        for selector in [None, Some("")] {
            let backend = super::mount_backend_for_selector(selector);

            assert_eq!(backend.name(), "raw-fuse");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_fuser_selector_remains_explicit_fallback_backend() {
        for selector in [Some("fuser"), Some("unknown")] {
            let backend = super::mount_backend_for_selector(selector);

            assert_eq!(backend.name(), "FUSE");
        }
    }
}
