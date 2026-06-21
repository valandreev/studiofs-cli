//! Build script for `studiofs`.
//!
//! Ensures WinFSP delayed-loading linker flags are applied at the binary
//! link stage so `/DELAYLOAD:winfsp-x64.dll` affects the final executable.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Entry point for the build script.
fn main() {
    emit_git_metadata();

    // Required for WinFSP delayed loading — must be in the final binary's build.rs
    // so the linker flag /DELAYLOAD:winfsp-x64.dll applies to the exe, not just the lib.
    #[cfg(windows)]
    winfsp::build::winfsp_link_delayload();
}

fn emit_git_metadata() {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    if let Some(git_dir) = resolve_git_dir(&manifest_dir) {
        let git_head = git_dir.join("HEAD");
        if git_head.exists() {
            println!("cargo:rerun-if-changed={}", git_head.display());
        }
        let reference = std::fs::read_to_string(&git_head)
            .ok()
            .and_then(|head| head.trim().strip_prefix("ref: ").map(str::to_owned));
        if let Some(reference) = reference {
            let ref_path = git_dir.join(reference);
            if ref_path.exists() {
                println!("cargo:rerun-if-changed={}", ref_path.display());
            } else if let Some(parent) = nearest_existing_parent(&ref_path, &git_dir) {
                println!("cargo:rerun-if-changed={}", parent.display());
            }
            let packed_refs = git_dir.join("packed-refs");
            if packed_refs.exists() {
                println!("cargo:rerun-if-changed={}", packed_refs.display());
            }
        }
    }

    emit_git_env(&manifest_dir, "GIT_SHA", &["rev-parse", "--verify", "HEAD"]);
    emit_git_env(
        &manifest_dir,
        "GIT_BRANCH",
        &["rev-parse", "--abbrev-ref", "HEAD"],
    );
}

fn nearest_existing_parent(path: &Path, boundary: &Path) -> Option<PathBuf> {
    path.ancestors()
        .skip(1)
        .take_while(|ancestor| ancestor.starts_with(boundary))
        .find(|ancestor| ancestor.exists())
        .map(Path::to_path_buf)
}

fn resolve_git_dir(manifest_dir: &Path) -> Option<PathBuf> {
    let git_dir = run_git(manifest_dir, &["rev-parse", "--git-dir"])?;
    let git_dir = PathBuf::from(git_dir);
    Some(if git_dir.is_absolute() {
        git_dir
    } else {
        manifest_dir.join(git_dir)
    })
}

fn emit_git_env(manifest_dir: &Path, name: &str, args: &[&str]) {
    let Some(value) = run_git(manifest_dir, args) else {
        return;
    };
    if !value.is_empty() {
        println!("cargo:rustc-env={name}={value}");
    }
}

fn run_git(manifest_dir: &Path, args: &[&str]) -> Option<String> {
    let Ok(output) = Command::new("git")
        .current_dir(manifest_dir)
        .args(args)
        .output()
    else {
        return None;
    };
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&output.stdout);
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}
