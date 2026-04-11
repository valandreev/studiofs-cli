//! CLI command modules.

pub(crate) const WRITE_PATH_OBSERVABILITY_FILE: &str = "write_path_observability.json";

pub mod cache_layout;
pub mod destroy;
pub mod discovery;
pub mod dump_load;
pub mod fsck;
pub mod init;
pub mod janitor;
pub mod mount;
#[cfg(unix)]
pub mod mount_profile;
#[cfg(not(unix))]
pub mod mount_profile_stub;
#[cfg(not(unix))]
pub use self::mount_profile_stub as mount_profile;
pub mod mount_profile_types;
pub mod settings;
pub mod snapshot;
pub mod status;
pub mod test;
pub mod test_cache;
#[cfg(unix)]
pub mod test_cache_profile;
#[cfg(not(unix))]
#[path = "test_cache_profile_stub.rs"]
pub mod test_cache_profile;
pub mod test_cache_profile_common;
pub mod test_cache_profile_types;
pub mod umount;
pub mod utils;
#[cfg(windows)]
pub mod winfsp_bootstrap;
