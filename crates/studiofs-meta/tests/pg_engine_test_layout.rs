//! Structural checks for the `pg_engine` regression test module layout.

use std::fs;
use std::path::{Path, PathBuf};

fn pg_engine_test_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("pg_engine")
        .join("tests")
}

fn pg_engine_src_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("pg_engine")
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

#[test]
fn pg_engine_data_path_modules_document_private_ownership_boundaries() {
    let src_dir = pg_engine_src_dir();
    for (module, ownership) in [
        ("engine_core.rs", "//! Owns trait delegation"),
        ("metadata_io.rs", "//! Owns remote metadata snapshot"),
        ("replication_flush.rs", "//! Owns local event replication"),
        (
            "inode_allocator.rs",
            "//! Owns PG-backed inode and client-slot allocation",
        ),
        (
            "metadata_sync.rs",
            "//! Owns foreign metadata catch-up orchestration",
        ),
    ] {
        let source = fs::read_to_string(src_dir.join(module)).expect("read pg_engine module");
        assert!(
            source.starts_with(ownership),
            "{module} should start with an ownership sentence: {ownership}"
        );
    }
}

#[test]
fn metadata_sync_uses_one_local_replay_position_helper() {
    let src_dir = pg_engine_src_dir();
    let local_state =
        fs::read_to_string(src_dir.join("local_state.rs")).expect("read local_state module");
    assert!(
        local_state.contains("fn persist_replay_position("),
        "local_state.rs should own replay version/counter advancement as one helper"
    );

    let metadata_sync =
        fs::read_to_string(src_dir.join("metadata_sync.rs")).expect("read metadata_sync module");
    assert!(
        metadata_sync.contains("self.persist_replay_position(&outcome)?"),
        "metadata_sync.rs should delegate replay version/counter advancement to local_state.rs"
    );
    for direct_call in [
        "self.persist_local_version(",
        "self.persist_namespace_version(",
        "self.persist_flushed_event_seq(",
    ] {
        assert!(
            !metadata_sync.contains(direct_call),
            "metadata_sync.rs should not call {direct_call} directly"
        );
    }
}
