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
    let mut build = cc::Build::new();
    build.file("csrc/low_level_ops.c");
    build.define("_FILE_OFFSET_BITS", "64");
    build.define("FUSE_USE_VERSION", "31");
    for include_path in fuse3.include_paths {
        build.include(include_path);
    }
    build.compile("studiofs_linux_fuse_low_level_ops");
}
