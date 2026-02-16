#![cfg(unix)]

//! StudioFS FUSE backend for Linux and macOS using `fuser`.
//!
//! Implements `fuser::Filesystem` to expose StudioVfs via FUSE.

mod fs;

pub use fs::StudioFuseFs;

/// Mount and serve the FUSE filesystem (blocks until unmounted).
pub fn mount_and_serve(
    vfs: std::sync::Arc<studiofs_vfs::StudioVfs>,
    rt: std::sync::Arc<tokio::runtime::Runtime>,
    mount_point: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let fs = StudioFuseFs::new(vfs, rt);
    let options = vec![
        fuser::MountOption::FSName("studiofs".into()),
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::AllowOther,
    ];
    tracing::info!("mounting StudioFS at {mount_point}");
    fuser::mount2(fs, mount_point, &options)?;
    Ok(())
}
