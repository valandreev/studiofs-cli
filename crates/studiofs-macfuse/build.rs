//! Build script for the macOS low-level macFUSE bridge boundary.

use std::process::Command;

fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }

    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");
    println!("cargo:rerun-if-changed=csrc/low_level_ops.c");
    let mut build = cc::Build::new();
    build.file("csrc/low_level_ops.c");
    build.define("_FILE_OFFSET_BITS", "64");
    build.define("FUSE_USE_VERSION", "26");
    let Ok(output) = Command::new("pkg-config")
        .args(["--cflags", "--libs-only-L", "fuse"])
        .output()
    else {
        build.compile("studiofs_macfuse_low_level_ops");
        return;
    };
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        for flag in stdout.split_whitespace() {
            if let Some(path) = flag.strip_prefix("-L") {
                println!("cargo:rustc-link-search=native={path}");
            } else if let Some(path) = flag.strip_prefix("-I") {
                build.include(path);
            }
        }
    }
    build.compile("studiofs_macfuse_low_level_ops");
}
