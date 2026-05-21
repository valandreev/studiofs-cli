//! Structural checks for the `pg_engine` regression test module layout.

use std::fs;
use std::path::{Path, PathBuf};

fn pg_engine_test_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("pg_engine")
        .join("tests")
}

#[test]
fn pg_engine_regression_tests_are_split_by_runtime_boundary() {
    let test_dir = pg_engine_test_dir();
    let root_tests = fs::read_to_string(test_dir.join("mod.rs")).expect("read pg_engine tests");
    assert!(
        !root_tests.contains("#[test]"),
        "pg_engine/tests/mod.rs should only wire modules and shared support; move tests into focused modules"
    );

    for module in [
        "remote_apply.rs",
        "lifecycle.rs",
        "lock_leases.rs",
        "lock_workers.rs",
        "janitor.rs",
        "metadata_notify.rs",
        "replication_backlog.rs",
        "inode_allocator.rs",
    ] {
        assert!(
            test_dir.join(module).is_file(),
            "missing pg_engine test module {module}"
        );
    }

    let inode_allocator = fs::read_to_string(test_dir.join("inode_allocator.rs"))
        .expect("read inode allocator tests");
    for remote_apply_name in [
        "sparse_ready_foreign_publish_preserves_disjoint_base_ranges_when_not_complete",
        "sparse_ready_self_batch_preserves_imported_disjoint_base_ranges_when_not_complete",
        "ready_complete_publish_preserved_base_ranges_sets_remote_serving_size",
        "unit_remote_projection_converges_nested_rename_overwrite_to_source_inode",
    ] {
        assert!(
            !inode_allocator.contains(remote_apply_name),
            "remote-apply test {remote_apply_name} should not live in inode_allocator.rs"
        );
    }
}
