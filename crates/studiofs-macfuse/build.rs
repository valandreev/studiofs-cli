//! Build script for the macOS low-level macFUSE bridge boundary.

#[cfg(target_os = "macos")]
use std::process::Command;

#[cfg(target_os = "macos")]
use std::path::Path;

#[cfg(target_os = "macos")]
const MACFUSE_FALLBACK_INCLUDE_DIRS: &[&str] = &[
    "/opt/homebrew/include/osxfuse",
    "/usr/local/include/osxfuse",
];

#[cfg(target_os = "macos")]
const MACFUSE_FALLBACK_LIB_DIRS: &[&str] = &["/opt/homebrew/lib", "/usr/local/lib"];
#[cfg(target_os = "macos")]
const MFMOUNT_FRAMEWORK_ENV: &str = "STUDIOFS_MFMOUNT_FRAMEWORK_DIR";
#[cfg(target_os = "macos")]
const DEFAULT_MFMOUNT_FRAMEWORK_DIR: &str = "/Library/Filesystems/macfuse.fs/Contents/Frameworks";

#[cfg(not(target_os = "macos"))]
fn main() {}

#[cfg(target_os = "macos")]
fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }

    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");
    println!("cargo:rerun-if-env-changed={MFMOUNT_FRAMEWORK_ENV}");
    println!("cargo:rerun-if-changed=csrc/low_level_ops.c");
    println!("cargo:rustc-check-cfg=cfg(studiofs_fskit)");
    if link_mfmount_framework() {
        println!("cargo:rustc-cfg=studiofs_fskit");
    }
    let mut build = cc::Build::new();
    build.file("csrc/low_level_ops.c");
    build.define("_FILE_OFFSET_BITS", "64");
    build.define("FUSE_USE_VERSION", "26");
    build.define("STUDIOFS_REAL_MACFUSE_HEADERS_REQUIRED", "1");
    let Ok(output) = Command::new("pkg-config")
        .args(["--cflags", "--libs-only-L", "fuse"])
        .output()
    else {
        add_macfuse_fallback_include_dirs(&mut build);
        add_macfuse_fallback_link_search_dirs();
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
    } else {
        add_macfuse_fallback_include_dirs(&mut build);
        add_macfuse_fallback_link_search_dirs();
    }
    build.compile("studiofs_macfuse_low_level_ops");
}

#[cfg(target_os = "macos")]
fn link_mfmount_framework() -> bool {
    let framework_dir = std::env::var(MFMOUNT_FRAMEWORK_ENV)
        .unwrap_or_else(|_| DEFAULT_MFMOUNT_FRAMEWORK_DIR.to_string());
    let framework_dir = Path::new(&framework_dir);
    let header = framework_dir.join("MFMount.framework/Headers/MFMount.h");
    let binary = framework_dir.join("MFMount.framework/MFMount");
    if !header.is_file() || !binary.is_file() {
        println!(
            "cargo:warning=MFMount.framework not found at '{}'; experimental FSKit backend is disabled; set {} to its parent directory to enable it",
            framework_dir.display(),
            MFMOUNT_FRAMEWORK_ENV
        );
        return false;
    }
    println!("cargo:rerun-if-changed={}", header.display());
    println!("cargo:rerun-if-changed={}", binary.display());
    println!(
        "cargo:rustc-link-search=framework={}",
        framework_dir.display()
    );
    println!("cargo:rustc-link-lib=framework=MFMount");
    true
}

#[cfg(target_os = "macos")]
fn add_macfuse_fallback_include_dirs(build: &mut cc::Build) {
    for path in macfuse_fallback_include_dirs() {
        build.include(path);
    }
}

#[cfg(target_os = "macos")]
fn add_macfuse_fallback_link_search_dirs() {
    for path in macfuse_fallback_lib_dirs() {
        println!("cargo:rustc-link-search=native={path}");
    }
}

#[cfg(target_os = "macos")]
fn macfuse_fallback_include_dirs() -> &'static [&'static str] {
    MACFUSE_FALLBACK_INCLUDE_DIRS
}

#[cfg(target_os = "macos")]
fn macfuse_fallback_lib_dirs() -> &'static [&'static str] {
    MACFUSE_FALLBACK_LIB_DIRS
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    #[test]
    fn macfuse_fallback_include_dirs_cover_homebrew_prefixes() {
        let dirs = super::macfuse_fallback_include_dirs();

        assert!(dirs.contains(&"/opt/homebrew/include/osxfuse"));
        assert!(dirs.contains(&"/usr/local/include/osxfuse"));
    }

    #[test]
    fn macfuse_fallback_lib_dirs_cover_homebrew_prefixes() {
        let dirs = super::macfuse_fallback_lib_dirs();

        assert!(dirs.contains(&"/opt/homebrew/lib"));
        assert!(dirs.contains(&"/usr/local/lib"));
    }
}
