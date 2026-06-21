//! Shared test helpers for StudioFS workspace crates.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const TARGET_TEST_DIR: &str = "studiofs-tests";

/// Returns the StudioFS workspace root for helper calls made from this crate.
#[must_use]
pub fn workspace_root() -> &'static Path {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir
            .parent()
            .and_then(Path::parent)
            .expect("studiofs-test-support is nested under workspace crates/")
            .to_path_buf()
    })
    .as_path()
}

/// Creates a temporary directory under the workspace `target/` directory.
///
/// The returned [`tempfile::TempDir`] removes its directory when dropped.
pub fn workspace_target_tempdir(prefix: &str) -> tempfile::TempDir {
    let temp_root = workspace_root().join("target").join(TARGET_TEST_DIR);
    std::fs::create_dir_all(&temp_root).expect("create workspace target test dir");
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(temp_root)
        .expect("create workspace target tempdir")
}

/// Returns a unique path under the workspace `target/` directory.
///
/// This is for tests that need to hand an artifact root path to code that
/// creates the directory itself instead of owning a [`tempfile::TempDir`].
#[must_use]
pub fn workspace_target_temp_path(prefix: &str) -> PathBuf {
    let temp_root = workspace_root().join("target").join(TARGET_TEST_DIR);
    std::fs::create_dir_all(&temp_root).expect("create workspace target test dir");
    temp_root.join(format!(
        "{prefix}{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after unix epoch")
            .as_nanos()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_target_tempdir_creates_dir_under_workspace_target() {
        let temp = workspace_target_tempdir("helper-");

        assert!(temp.path().starts_with(workspace_root().join("target")));
    }

    #[test]
    fn workspace_target_temp_path_returns_path_under_workspace_target() {
        let path = workspace_target_temp_path("helper-");

        assert!(path.starts_with(workspace_root().join("target")));
    }
}
