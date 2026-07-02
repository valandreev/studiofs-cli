//! Contract tests that keep `studiofs-fuse` scoped to the Linux bridge.

#![cfg(unix)]

use std::path::Path;

#[test]
fn studiofs_fuse_sources_do_not_own_macos_bridge_behavior() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let checked_files = [
        "Cargo.toml",
        "src/lib.rs",
        "src/raw.rs",
        "tests/mount_sanity.rs",
    ];

    let forbidden = [
        "target_os = \"macos\"",
        "Macos",
        "macos",
        "Darwin",
        "darwin",
        "unicode-normalization",
        "libfuse",
    ];

    for relative_path in checked_files {
        let path = crate_root.join(relative_path);
        let contents = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));

        for needle in forbidden {
            assert!(
                !contents.contains(needle),
                "{} still contains `{needle}`; studiofs-fuse must remain Linux-only",
                path.display()
            );
        }
    }
}
