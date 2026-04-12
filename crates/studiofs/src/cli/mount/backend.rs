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
        studiofs_fuse::mount_and_serve(
            launch.vfs,
            launch.runtime_handle,
            &launch.mount_point,
            &launch.volume_name,
            launch.run_in_foreground,
            launch.read_only,
        )
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
