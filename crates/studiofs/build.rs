//! Build script for `studiofs`.
//!
//! Ensures WinFSP delayed-loading linker flags are applied at the binary
//! link stage so `/DELAYLOAD:winfsp-x64.dll` affects the final executable.

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
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    if let Ok(head) = std::fs::read_to_string("../../.git/HEAD")
        && let Some(reference) = head.trim().strip_prefix("ref: ")
    {
        println!("cargo:rerun-if-changed=../../.git/{reference}");
    }

    emit_git_env("GIT_SHA", &["rev-parse", "--verify", "HEAD"]);
    emit_git_env("GIT_BRANCH", &["rev-parse", "--abbrev-ref", "HEAD"]);
}

fn emit_git_env(name: &str, args: &[&str]) {
    let Ok(output) = Command::new("git").args(args).output() else {
        return;
    };
    if !output.status.success() {
        return;
    }
    let value = String::from_utf8_lossy(&output.stdout);
    let value = value.trim();
    if !value.is_empty() {
        println!("cargo:rustc-env={name}={value}");
    }
}
