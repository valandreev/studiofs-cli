//! Shared test helpers for StudioFS workspace crates.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const TARGET_TEST_DIR: &str = "studiofs-tests";

/// Returns the StudioFS workspace root for helper calls made from this crate.
#[must_use]
pub fn workspace_root() -> &'static Path {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(resolve_workspace_root).as_path()
}

/// Creates a temporary directory under the Cargo target directory.
///
/// `CARGO_TARGET_DIR` is honored when set; otherwise the workspace `target/`
/// directory is used.
///
/// The returned [`tempfile::TempDir`] removes its directory when dropped.
pub fn workspace_target_tempdir(prefix: &str) -> tempfile::TempDir {
    let temp_root = workspace_target_test_root();
    std::fs::create_dir_all(&temp_root).expect("create workspace target test dir");
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(temp_root)
        .expect("create workspace target tempdir")
}

/// Returns a unique path under the Cargo target directory.
///
/// `CARGO_TARGET_DIR` is honored when set; otherwise the workspace `target/`
/// directory is used.
///
/// This is for tests that need to hand an artifact root path to code that
/// creates the directory itself instead of owning a [`tempfile::TempDir`].
#[must_use]
pub fn workspace_target_temp_path(prefix: &str) -> PathBuf {
    let temp_root = workspace_target_test_root();
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

fn resolve_workspace_root() -> PathBuf {
    if let Some(root) = std::env::var_os("CARGO_WORKSPACE_DIR") {
        return PathBuf::from(root);
    }

    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    if manifest_dir.exists() {
        return manifest_dir
            .parent()
            .and_then(Path::parent)
            .expect("studiofs-test-support is nested under workspace crates/")
            .to_path_buf();
    }

    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn workspace_target_test_root() -> PathBuf {
    cargo_target_dir(std::env::var_os("CARGO_TARGET_DIR")).join(TARGET_TEST_DIR)
}

fn cargo_target_dir(cargo_target_dir: Option<std::ffi::OsString>) -> PathBuf {
    cargo_target_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_target_tempdir_creates_dir_under_workspace_target() {
        let temp = workspace_target_tempdir("helper-");

        assert!(temp.path().starts_with(workspace_target_test_root()));
    }

    #[test]
    fn workspace_target_temp_path_returns_path_under_workspace_target() {
        let path = workspace_target_temp_path("helper-");

        assert!(path.starts_with(workspace_target_test_root()));
    }

    #[test]
    fn cargo_target_dir_prefers_cargo_target_dir_override() {
        let override_dir = PathBuf::from("custom-target");

        assert_eq!(
            cargo_target_dir(Some(override_dir.clone().into_os_string())),
            override_dir
        );
    }

    #[test]
    fn cargo_target_dir_defaults_to_workspace_target() {
        assert_eq!(cargo_target_dir(None), workspace_root().join("target"));
    }
}
