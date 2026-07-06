//! Build script for the Linux libfuse3 C shim.

#[cfg(not(target_os = "linux"))]
fn main() {}

#[cfg(target_os = "linux")]
fn main() {
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");
    println!("cargo:rerun-if-changed=csrc/low_level_ops.c");

    let fuse3 = pkg_config::Config::new()
        .probe("fuse3")
        .unwrap_or_else(|error| {
            panic!("pkg-config could not find fuse3; install libfuse3-dev: {error}")
        });
    let mut version_parts = fuse3.version.split('.').map(|part| {
        part.split(|ch: char| !ch.is_ascii_digit())
            .next()
            .unwrap_or("0")
            .parse::<u32>()
            .unwrap_or(0)
    });
    let fuse_version = (
        version_parts.next().unwrap_or(0),
        version_parts.next().unwrap_or(0),
    );
    if fuse_version < (3, 2) {
        panic!(
            "libfuse3 >= 3.2 is required for StudioFS Linux mounts; found {}",
            fuse3.version
        );
    }
    let fuse_api_version = if fuse_version >= (3, 12) { "312" } else { "32" };

    let mut build = cc::Build::new();
    build.file("csrc/low_level_ops.c");
    build.define("_FILE_OFFSET_BITS", "64");
    build.define("FUSE_USE_VERSION", fuse_api_version);
    for include_path in fuse3.include_paths {
        build.include(include_path);
    }
    build.compile("studiofs_linux_fuse_low_level_ops");
}
