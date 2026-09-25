//! Shared test helpers for StudioFS workspace crates.
#![forbid(unsafe_code)]

use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

const TARGET_TEST_DIR: &str = "studiofs-tests";

/// Copies a file's contents to a new destination without copying its permissions.
pub fn copy_file_contents(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> io::Result<u64> {
    let mut source = std::fs::File::open(source)?;
    let mut destination = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    io::copy(&mut source, &mut destination)
}

/// Marks a cleanly closed redb file as requiring recovery on its next open.
pub fn mark_redb_repair_required(path: impl AsRef<Path>) -> io::Result<()> {
    const REDB_GOD_BYTE_OFFSET: u64 = 9;
    const REDB_RECOVERY_REQUIRED: u8 = 2;
    const REDB_TWO_PHASE_COMMIT: u8 = 4;

    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(REDB_GOD_BYTE_OFFSET))?;
    let mut flags = [0u8; 1];
    file.read_exact(&mut flags)?;
    flags[0] |= REDB_RECOVERY_REQUIRED;
    flags[0] &= !REDB_TWO_PHASE_COMMIT;
    file.seek(SeekFrom::Start(REDB_GOD_BYTE_OFFSET))?;
    file.write_all(&flags)?;
    file.sync_all()
}

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
    std::fs::create_dir_all(temp_root).expect("create workspace target test dir");
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
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    let temp_root = workspace_target_test_root();
    std::fs::create_dir_all(temp_root).expect("create workspace target test dir");
    temp_root.join(format!(
        "{prefix}{}-{}-{count}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
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

    std::env::current_dir()
        .ok()
        .and_then(discover_workspace_root_from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn discover_workspace_root_from(mut current: PathBuf) -> Option<PathBuf> {
    loop {
        let manifest = current.join("Cargo.toml");
        match std::fs::read_to_string(&manifest) {
            Ok(content) if content.contains("[workspace]") => return Some(current),
            _ => {}
        }

        if !current.pop() {
            return None;
        }
    }
}

fn workspace_target_test_root() -> &'static Path {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        cargo_target_dir(std::env::var_os("CARGO_TARGET_DIR")).join(TARGET_TEST_DIR)
    })
    .as_path()
}

fn cargo_target_dir(cargo_target_dir: Option<std::ffi::OsString>) -> PathBuf {
    let path = cargo_target_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target"));
    if path.is_relative() {
        workspace_root().join(path)
    } else {
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn copy_file_contents_keeps_destination_writable_for_read_only_source() {
        let temp = workspace_target_tempdir("helper-copy-");
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        std::fs::write(&source, b"fixture").unwrap();
        let original_permissions = std::fs::metadata(&source).unwrap().permissions();
        let mut read_only_permissions = original_permissions.clone();
        read_only_permissions.set_readonly(true);
        std::fs::set_permissions(&source, read_only_permissions).unwrap();

        assert_eq!(copy_file_contents(&source, &destination).unwrap(), 7);
        std::fs::set_permissions(&source, original_permissions).unwrap();
        assert_eq!(std::fs::read(&destination).unwrap(), b"fixture");

        let mut copied_file = std::fs::OpenOptions::new()
            .append(true)
            .open(&destination)
            .unwrap();
        copied_file.write_all(b"!").unwrap();
        assert_eq!(std::fs::read(&destination).unwrap(), b"fixture!");
    }

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
    fn workspace_target_temp_path_returns_unique_paths() {
        let first = workspace_target_temp_path("helper-");
        let second = workspace_target_temp_path("helper-");

        assert_ne!(first, second);
    }

    #[test]
    fn cargo_target_dir_prefers_absolute_cargo_target_dir_override() {
        let override_dir = workspace_root().join("custom-target");

        assert_eq!(
            cargo_target_dir(Some(override_dir.clone().into_os_string())),
            override_dir
        );
    }

    #[test]
    fn cargo_target_dir_resolves_relative_override_from_workspace_root() {
        let override_dir = PathBuf::from("custom-target");

        assert_eq!(
            cargo_target_dir(Some(override_dir.clone().into_os_string())),
            workspace_root().join(override_dir)
        );
    }

    #[test]
    fn cargo_target_dir_defaults_to_workspace_target() {
        assert_eq!(cargo_target_dir(None), workspace_root().join("target"));
    }

    #[test]
    fn discover_workspace_root_walks_up_from_crate_directory() {
        let temp = workspace_target_tempdir("workspace-discovery-");
        let workspace = temp.path();
        let crate_dir = workspace.join("crates").join("member");
        std::fs::create_dir_all(&crate_dir).expect("create fake crate dir");
        std::fs::write(workspace.join("Cargo.toml"), "[workspace]\n").expect("write manifest");
        std::fs::write(
            crate_dir.join("Cargo.toml"),
            "[package]\nname = \"member\"\n",
        )
        .expect("write crate manifest");

        assert_eq!(
            discover_workspace_root_from(crate_dir).as_deref(),
            Some(workspace)
        );
    }
}
