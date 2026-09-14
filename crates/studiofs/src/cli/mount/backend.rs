use std::io;
use std::sync::Arc;

use studiofs_vfs::StudioVfs;
use tokio::runtime::Handle;

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) const MOUNT_BACKEND_ENV: &str = "STUDIOFS_MOUNT_BACKEND";

pub(crate) struct BackendLaunch {
    pub(crate) vfs: Arc<StudioVfs>,
    pub(crate) runtime_handle: Handle,
    pub(crate) mount_point: String,
    pub(crate) volume_name: String,
    pub(crate) read_only: bool,
    pub(crate) mount_ready_callback: Box<dyn FnOnce() + Send>,
    #[cfg(all(unix, not(target_os = "linux")))]
    pub(crate) run_in_foreground: bool,
}

pub(crate) trait MountBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn serve(&self, launch: BackendLaunch) -> io::Result<()>;
}

#[cfg(target_os = "linux")]
pub(crate) fn default_mount_backend() -> Box<dyn MountBackend> {
    Box::new(LinuxFuseMountBackend)
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) fn default_mount_backend() -> Box<dyn MountBackend> {
    mount_backend_for_selector(std::env::var(MOUNT_BACKEND_ENV).ok().as_deref())
}

#[cfg(windows)]
pub(crate) fn default_mount_backend() -> Box<dyn MountBackend> {
    Box::new(WinfspMountBackend)
}

#[cfg(target_os = "macos")]
pub(crate) fn mount_backend_for_selector(selector: Option<&str>) -> Box<dyn MountBackend> {
    match selector.map(str::trim).filter(|value| !value.is_empty()) {
        None => Box::new(MacFuseMountBackend),
        Some(value)
            if value.eq_ignore_ascii_case("legacy") || value.eq_ignore_ascii_case("macfuse") =>
        {
            Box::new(MacFuseMountBackend)
        }
        Some(value) if value.eq_ignore_ascii_case("fskit") => Box::new(MacFuseFsKitMountBackend),
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
        .with_mount_ready_callback(launch.mount_ready_callback)
        .with_signal_handling(studiofs_macfuse::SignalHandling::InstallForCli);
        studiofs_macfuse::mount_with_request(request)
    }
}

#[cfg(target_os = "macos")]
struct MacFuseFsKitMountBackend;

#[cfg(target_os = "macos")]
impl MountBackend for MacFuseFsKitMountBackend {
    fn name(&self) -> &'static str {
        "macFUSE-FSKit"
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
        .with_mount_ready_callback(launch.mount_ready_callback)
        .with_signal_handling(studiofs_macfuse::SignalHandling::InstallForCli);
        studiofs_macfuse::mount_with_fskit_request(request)
    }
}

#[cfg(target_os = "linux")]
struct LinuxFuseMountBackend;

#[cfg(target_os = "linux")]
impl MountBackend for LinuxFuseMountBackend {
    fn name(&self) -> &'static str {
        "libfuse3"
    }

    fn serve(&self, launch: BackendLaunch) -> io::Result<()> {
        let BackendLaunch {
            vfs,
            runtime_handle,
            mount_point,
            volume_name,
            read_only,
            mount_ready_callback,
        } = launch;
        let request = studiofs_linux_fuse::LinuxFuseMountRequest::new(
            vfs,
            runtime_handle,
            mount_point,
            volume_name,
            read_only,
        )
        .with_mount_ready_callback(mount_ready_callback);
        studiofs_linux_fuse::mount_with_request(request)
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
struct UnsupportedUnixMountBackend;

#[cfg(all(unix, not(target_os = "linux")))]
fn unsupported_unix_backend_error_message() -> &'static str {
    if cfg!(target_os = "macos") {
        "StudioFS mount backend selector is unsupported; macOS uses the native macFUSE bridge by default; clear or set STUDIOFS_MOUNT_BACKEND=macfuse or STUDIOFS_MOUNT_BACKEND=fskit"
    } else {
        "StudioFS mount is not implemented on this Unix platform; macOS and Windows are supported via their native bridges"
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
        let request = studiofs_winfsp::WinfspMountRequest {
            vfs: launch.vfs,
            runtime_handle: launch.runtime_handle,
            mount_point: launch.mount_point,
            volume_label: launch.volume_name,
            read_only: launch.read_only,
            shutdown_event_name: Some(shutdown_event_name),
            mount_ready_callback: None,
        }
        .with_mount_ready_callback(launch.mount_ready_callback);
        studiofs_winfsp::mount_and_serve(request)
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
    fn macos_legacy_selector_keeps_the_legacy_backend_explicit() {
        let backend = super::mount_backend_for_selector(Some("legacy"));

        assert_eq!(backend.name(), "macFUSE");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_fskit_selector_routes_to_experimental_backend() {
        let backend = super::mount_backend_for_selector(Some("fskit"));

        assert_eq!(backend.name(), "macFUSE-FSKit");
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
    fn linux_default_backend_routes_to_libfuse3() {
        let backend = super::default_mount_backend();

        assert_eq!(backend.name(), "libfuse3");
    }
}
