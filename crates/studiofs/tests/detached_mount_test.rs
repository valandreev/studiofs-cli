//! Mounted regression tests for `studiofs mount --detach` readiness.
//!
//! Run infra-backed tests (S3 + PostgreSQL + Linux FUSE):
//!   cargo test -p studiofs --test detached_mount_test -- --ignored --nocapture --test-threads=1

#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;
use uuid::Uuid;

fn workspace_dotenv_vars() -> &'static BTreeMap<String, String> {
    static VARS: OnceLock<BTreeMap<String, String>> = OnceLock::new();
    VARS.get_or_init(|| {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let env_path = manifest_dir
            .parent()
            .and_then(Path::parent)
            .unwrap_or(manifest_dir)
            .join(".env");
        dotenvy::from_path_iter(env_path)
            .map(|iter| iter.flatten().collect())
            .unwrap_or_default()
    })
}

fn required_env(key: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| workspace_dotenv_vars().get(key).cloned())
        .unwrap_or_else(|| panic!("{key} is required for the detached mount test"))
}

fn studiofs(client_root: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_studiofs"));
    command.env("STUDIOFS_CLIENT_ROOT", client_root);
    command
}

fn output_text(output: &Output) -> String {
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    text
}

/// Unmounts and destroys the test volume even when an assertion fails.
struct DetachedVolume {
    client_root: PathBuf,
    name: String,
    mount_root: Option<PathBuf>,
}

impl Drop for DetachedVolume {
    fn drop(&mut self) {
        if let Some(mount_root) = self.mount_root.take() {
            let _ = studiofs(&self.client_root)
                .arg("umount")
                .arg(&mount_root)
                .output();
        }
        let Ok(mut child) = studiofs(&self.client_root)
            .arg("destroy")
            .arg(&self.name)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        else {
            return;
        };
        if let Some(mut stdin) = child.stdin.take() {
            let _ = write!(stdin, "y\n{}\n", self.name);
        }
        let _ = child.wait();
    }
}

#[test]
#[ignore = "requires S3 + PostgreSQL test env (STUDIOFS_S3_*, STUDIOFS_TEST_PG_DSN) and fusermount3"]
fn detached_mount_accepts_first_root_create_on_return() {
    let temp = studiofs_test_support::workspace_target_tempdir("detached-mount-");
    let client_root = temp.path().join("client");
    let mount_parent = temp.path().join("mnt");
    let cache_dir = temp.path().join("cache");
    std::fs::create_dir_all(&mount_parent).expect("create mount parent");
    let name = format!("sfs902-{}", Uuid::new_v4().simple());

    let init = studiofs(&client_root)
        .arg("init")
        .arg(&name)
        .arg("--endpoint")
        .arg(required_env("STUDIOFS_S3_ENDPOINT"))
        .arg("--bucket")
        .arg(required_env("STUDIOFS_S3_BUCKET"))
        .arg("--access-key")
        .arg(required_env("STUDIOFS_S3_ACCESS_KEY"))
        .arg("--secret-key")
        .arg(required_env("STUDIOFS_S3_SECRET_KEY"))
        .arg("--meta")
        .arg(required_env("STUDIOFS_TEST_PG_DSN"))
        .output()
        .expect("run studiofs init");
    let mut volume = DetachedVolume {
        client_root: client_root.clone(),
        name: name.clone(),
        mount_root: None,
    };
    assert!(init.status.success(), "init failed: {}", output_text(&init));

    let mount = studiofs(&client_root)
        .arg("mount")
        .arg(&name)
        .arg(&mount_parent)
        .arg("--detach")
        .arg("--cache-dir")
        .arg(&cache_dir)
        .output()
        .expect("run studiofs mount --detach");
    let mount_root = mount_parent.join(&name);
    volume.mount_root = Some(mount_root.clone());
    let mount_output = output_text(&mount);
    assert!(mount.status.success(), "mount failed: {mount_output}");
    assert!(
        mount_output.contains("status: live"),
        "detached mount returned before write authority: {mount_output}"
    );

    // No polling: the first create must succeed as soon as `--detach` returns.
    std::fs::write(mount_root.join("first-create"), b"sfs-902")
        .expect("first create in mount root after detached mount returned");
    std::fs::create_dir(mount_root.join("first-dir"))
        .expect("first mkdir in mount root after detached mount returned");
    assert_eq!(
        std::fs::read(mount_root.join("first-create")).expect("read first create"),
        b"sfs-902"
    );
}
