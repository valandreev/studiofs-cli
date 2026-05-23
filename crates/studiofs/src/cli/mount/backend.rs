use std::io;
use std::sync::Arc;

use studiofs_vfs::StudioVfs;
use tokio::runtime::Handle;

pub(crate) struct BackendLaunch {
    pub(crate) vfs: Arc<StudioVfs>,
    pub(crate) runtime_handle: Handle,
    pub(crate) mount_point: String,
    pub(crate) volume_name: String,
    pub(crate) read_only: bool,
    #[cfg(unix)]
    pub(crate) run_in_foreground: bool,
}

pub(crate) trait MountBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn serve(&self, launch: BackendLaunch) -> io::Result<()>;
}

#[cfg(unix)]
pub(crate) fn default_mount_backend() -> Box<dyn MountBackend> {
    Box::new(FuseMountBackend)
}

#[cfg(windows)]
pub(crate) fn default_mount_backend() -> Box<dyn MountBackend> {
    Box::new(WinfspMountBackend)
}

#[cfg(unix)]
struct FuseMountBackend;

#[cfg(unix)]
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
        request.foreground = launch.run_in_foreground;
        request.read_only = launch.read_only;
        request.signal_handling = studiofs_fuse::SignalHandling::InstallForCli;
        studiofs_fuse::mount_with_request(request)
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
