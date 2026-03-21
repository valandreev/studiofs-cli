use crate::cli::mount_profile_types::MountProfileMode;
use std::path::Path;

pub struct MountProfileSession;

impl MountProfileSession {
    pub fn start(
        _mode: Option<MountProfileMode>,
        _volume_name: &str,
        _platform: &str,
        _bridge: &str,
        _mount_mode: &str,
        _cache_mode: &str,
        _cache_path: &Path,
    ) -> Option<Self> {
        None
    }

    pub fn finish(self) {}
}
