//! CLI command modules.

pub mod destroy;
pub mod discovery;
pub mod dump_load;
pub mod fsck;
pub mod init;
pub mod janitor;
pub mod mount;
pub mod mount_profile;
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
pub mod umount;
pub mod utils;
