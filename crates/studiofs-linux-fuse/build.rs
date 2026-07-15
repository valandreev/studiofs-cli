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
        version_parts.next().unwrap_or(0),
    );
    if fuse_version < (3, 2, 0) {
        panic!(
            "libfuse3 >= 3.2 is required for StudioFS Linux mounts; found {}",
            fuse3.version
        );
    }
    let fuse_api_version = if fuse_version >= (3, 12, 0) {
        "312"
    } else {
        "32"
    };
    println!(
        "cargo:rustc-env=STUDIOFS_LIBFUSE_VERSION_MAJOR={}",
        fuse_version.0
    );
    println!(
        "cargo:rustc-env=STUDIOFS_LIBFUSE_VERSION_MINOR={}",
        fuse_version.1
    );
    println!(
        "cargo:rustc-env=STUDIOFS_LIBFUSE_VERSION_PATCH={}",
        fuse_version.2
    );

    let mut build = cc::Build::new();
    build.file("csrc/low_level_ops.c");
    build.define("_FILE_OFFSET_BITS", "64");
    build.define("FUSE_USE_VERSION", fuse_api_version);
    if fuse_version >= (3, 12, 0) {
        build.define("STUDIOFS_FUSE_HAS_BOUNDED_LOOP_CONFIG", None);
    }
    if fuse_version >= (3, 14, 1) {
        build.define("STUDIOFS_FUSE_HAS_PARALLEL_DIRECT_WRITES", None);
    }
    if fuse_version >= (3, 17, 1) {
        build.define("STUDIOFS_FUSE_HAS_EXTENDED_CONN_CAPS", None);
    }
    for include_path in fuse3.include_paths {
        build.include(include_path);
    }
    build.compile("studiofs_linux_fuse_low_level_ops");
}
