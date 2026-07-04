use std::io;
use std::sync::Arc;

use studiofs_vfs::StudioVfs;
use tokio::runtime::Handle;

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) const MOUNT_BACKEND_ENV: &str = "STUDIOFS_MOUNT_BACKEND";

#[cfg_attr(unix, allow(dead_code))]
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

#[cfg(unix)]
pub(crate) fn default_mount_backend() -> Box<dyn MountBackend> {
    #[cfg(target_os = "linux")]
    {
        return Box::new(UnsupportedUnixMountBackend);
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    {
        mount_backend_for_selector(std::env::var(MOUNT_BACKEND_ENV).ok().as_deref())
    }
}

#[cfg(windows)]
pub(crate) fn default_mount_backend() -> Box<dyn MountBackend> {
    Box::new(WinfspMountBackend)
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

#[cfg(unix)]
struct UnsupportedUnixMountBackend;

#[cfg(unix)]
fn unsupported_unix_backend_error_message() -> &'static str {
    if cfg!(target_os = "linux") {
        "StudioFS Linux mount is not implemented; the legacy raw FUSE backend was removed in SFS-600"
    } else if cfg!(target_os = "macos") {
        "StudioFS FUSE backend is Linux-only; macOS uses the native macFUSE bridge by default; clear or set STUDIOFS_MOUNT_BACKEND=macfuse to use it"
    } else {
        "StudioFS FUSE backend is Linux-only; macOS and Windows are supported via their respective native bridges"
    }
}

#[cfg(unix)]
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
    fn linux_default_backend_is_explicitly_unsupported() {
        let backend = super::default_mount_backend();

        assert_eq!(backend.name(), "unsupported-unix");
    }
}
