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
    foreground: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Keep a reference for shutdown() after fuser::mount2() returns
    // (vfs is moved into StudioFuseFs below).
    let vfs_ref = vfs.clone();
    let fs = StudioFuseFs::new(vfs, rt);
    let mut options = vec![
        fuser::MountOption::FSName("studiofs".into()),
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::AllowOther,
        // Do NOT add DefaultPermissions here: it would make the kernel enforce
        // POSIX mode/uid/gid from getattr() in-kernel.  Old volumes have the
        // root inode stored with uid=0/gid=0 (historic default), so the kernel
        // would see the mounting user as "other" (r-x only) and block writes.
        // Instead we implement access() → ok() so the FUSE daemon decides.
    ];
    // macOS-specific options:
    //  noappledouble – suppress creation of ._  AppleDouble resource files
    //  volname       – display name in Finder
    #[cfg(target_os = "macos")]
    {
        options.push(fuser::MountOption::CUSTOM("noappledouble".into()));
        options.push(fuser::MountOption::CUSTOM("volname=StudioFS".into()));
    }
    if foreground {
        options.push(fuser::MountOption::CUSTOM("foreground".into()));
    }
    tracing::info!("mounting StudioFS at {mount_point}");
    fuser::mount2(fs, mount_point, &options)?;
    // fuser::mount2 returns when the filesystem is unmounted.
    // Drain uploads and persist cache checkpoint before process exit.
    rt.block_on(vfs_ref.shutdown(std::time::Duration::from_secs(30)));
    Ok(())
}
