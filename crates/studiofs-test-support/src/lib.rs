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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_target_tempdir_creates_dir_under_workspace_target() {
        let temp = workspace_target_tempdir("helper-");

        assert!(temp.path().starts_with(workspace_root().join("target")));
    }
}
