// Build script for `studiofs`.
// Ensures WinFSP delayed-loading linker flags are applied at the binary
// link stage so `/DELAYLOAD:winfsp-x64.dll` affects the final executable.

use std::path::{Path, PathBuf};
use std::process::Command;

#[cfg(test)]
#[path = "tests/build_version/cases.rs"]
mod tests;

fn build_version(repo: &Path, base: &str) -> String {
    let sha = run_git(repo, &["rev-parse", "--short=8", "HEAD"]);
    let dirty = run_git(repo, &["status", "--porcelain", "--untracked-files=no"])
        .is_some_and(|status| !status.is_empty());
    let head = run_git(repo, &["rev-parse", "--verify", "HEAD"]);
    let tag = run_git(
        repo,
        &[
            "rev-parse",
            "--verify",
            &format!("refs/tags/v{base}^{{commit}}"),
        ],
    );
    if !dirty && head.is_some() && head == tag {
        return base.to_owned();
    }
    let count = development_count(repo, base).unwrap_or_else(|| "unknown".to_owned());
    let dirty = if dirty { "-dirty" } else { "" };
    format!(
        "{base}-dev.{count}{dirty} ({})",
        sha.as_deref().unwrap_or("unknown")
    )
}

fn development_count(repo: &Path, base: &str) -> Option<String> {
    if run_git(repo, &["rev-parse", "--is-shallow-repository"])?.trim() != "false" {
        return None;
    }
    let changes = run_git(
        repo,
        &[
            "log",
            "--first-parent",
            "--format=%H",
            "-G",
            "^[[:space:]]*version[[:space:]]*=",
            "--",
            "Cargo.toml",
        ],
    )?;
    let mut anchor = None;
    for commit in changes.lines() {
        let manifest = run_git(repo, &["show", &format!("{commit}:Cargo.toml")])?;
        let manifest: toml::Value = toml::from_str(&manifest).ok()?;
        let version = manifest
            .get("workspace")?
            .get("package")?
            .get("version")?
            .as_str()?;
        if version != base {
            break;
        }
        anchor = Some(commit);
    }
    run_git(
        repo,
        &[
            "rev-list",
            "--first-parent",
            "--count",
            &format!("{}..HEAD", anchor?),
        ],
    )
}

/// Entry point for the build script.
fn main() {
    emit_git_metadata();

    #[cfg(windows)]
    emit_winfsp_delayload_for_studiofs_bin();
}

#[cfg(windows)]
fn emit_winfsp_delayload_for_studiofs_bin() {
    let dll = match std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() {
        Ok("x86_64") => "winfsp-x64.dll",
        Ok("x86") => "winfsp-x86.dll",
        Ok("aarch64") => "winfsp-a64.dll",
        _ => panic!("unsupported WinFSP target architecture"),
    };

    println!("cargo:rustc-link-lib=dylib=delayimp");
    println!("cargo:rustc-link-arg-bin=studiofs=/DELAYLOAD:{dll}");
}

fn emit_git_metadata() {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let repo = run_git(&manifest_dir, &["rev-parse", "--show-toplevel"])
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.clone());
    for name in ["HEAD", "index", "refs", "packed-refs", "shallow"] {
        if let Some(path) = run_git(&manifest_dir, &["rev-parse", "--git-path", name]) {
            let path = manifest_dir.join(path);
            if path.exists() {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }
    // Track source edits as well as commits so cached builds cannot retain a clean identity.
    if let Some(files) = run_git(&repo, &["ls-files"]) {
        for file in files.lines() {
            println!("cargo:rerun-if-changed={}", repo.join(file).display());
        }
    }

    let version = build_version(
        &repo,
        &std::env::var("CARGO_PKG_VERSION").unwrap_or_default(),
    );
    println!("cargo:rustc-env=STUDIOFS_BUILD_VERSION={version}");

    emit_git_env(&manifest_dir, "GIT_SHA", &["rev-parse", "--verify", "HEAD"]);
    emit_git_env(
        &manifest_dir,
        "GIT_BRANCH",
        &["rev-parse", "--abbrev-ref", "HEAD"],
    );
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
    Some(value.to_string())
}
