use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

fn read_workspace_file(path: &str) -> String {
    fs::read_to_string(repo_root().join(path)).unwrap_or_else(|error| {
        panic!("failed to read {path}: {error}");
    })
}

fn collect_term_hits(path: &str, terms: &[&str]) -> Vec<String> {
    let contents = read_workspace_file(path);
    let mut hits = Vec::new();
    let lowered_terms: Vec<_> = terms.iter().map(|term| term.to_lowercase()).collect();

    for (line_number, line) in contents.lines().enumerate() {
        let lowered_line = line.to_lowercase();
        for (term, lowered_term) in terms.iter().zip(lowered_terms.iter()) {
            if lowered_line.contains(lowered_term) {
                hits.push(format!("{path}:{}: found `{term}`", line_number + 1));
            }
        }
    }

    hits
}

#[test]
fn active_docs_drop_superseded_io_architecture_terms() {
    let files = [
        "README.md",
        "AGENTS.md",
        "docs/architecture.md",
        "docs/cache.md",
        "docs/local-block-runtime-layout.md",
        "docs/configuration.md",
        "docs/examples.md",
        "docs/vfs.md",
    ];
    let forbidden_terms = [
        "buffered-first",
        "Cache 3.0",
        "cache-v3",
        "CacheV3",
        "WriteBuffer",
        "write-buffer",
        "UploadCoordinator",
        "upload coordinator",
        "ReadPipeline",
        "access classifier",
        "planner",
    ];

    let hits: Vec<_> = files
        .iter()
        .flat_map(|path| collect_term_hits(path, &forbidden_terms))
        .collect();

    assert!(
        hits.is_empty(),
        "active docs still teach superseded IO vocabulary:\n{}",
        hits.join("\n")
    );
}

#[test]
fn cli_surface_avoids_legacy_runtime_product_wording() {
    let files = [
        "crates/studiofs/src/cli/cache_layout.rs",
        "crates/studiofs/src/cli/destroy.rs",
        "crates/studiofs/src/cli/status.rs",
        "crates/studiofs/src/cli/test_cache_profile_common.rs",
        "crates/studiofs/src/cli/test_cache_profile_stub.rs",
    ];

    let hits: Vec<_> = files
        .iter()
        .flat_map(|path| collect_term_hits(path, &["Cache 3.0", "cache-v3", "CacheV3"]))
        .collect();

    assert!(
        hits.is_empty(),
        "CLI surface still uses legacy runtime product wording:\n{}",
        hits.join("\n")
    );
}

#[test]
fn cli_and_regression_surfaces_drop_buffered_first_wording() {
    let files = [
        "crates/studiofs/src/cli/test_cache/reporting/human.rs",
        "crates/studiofs-vfs/tests/vfs_test.rs",
    ];

    let hits: Vec<_> = files
        .iter()
        .flat_map(|path| collect_term_hits(path, &["Buffered-first", "buffered-first"]))
        .collect();

    assert!(
        hits.is_empty(),
        "CLI and regression surfaces still use buffered-first wording:\n{}",
        hits.join("\n")
    );
}

#[test]
fn runtime_metric_names_drop_superseded_write_buffer_and_flush_ordered_terms() {
    let files = [
        "crates/studiofs-vfs/src/vfs.rs",
        "crates/studiofs-vfs/src/vfs/metrics.rs",
        "crates/studiofs-vfs/src/vfs/upload_lifecycle.rs",
        "crates/studiofs/src/cli/status.rs",
    ];
    let forbidden_terms = [
        "accepted_write_buffer_bytes_before",
        "accepted_write_buffer_bytes_after",
        "flush_ordered_calls",
        "flush_ordered_calls_per_sec",
    ];

    let hits: Vec<_> = files
        .iter()
        .flat_map(|path| collect_term_hits(path, &forbidden_terms))
        .collect();

    assert!(
        hits.is_empty(),
        "runtime metrics still expose superseded write-buffer/flush-ordered names:\n{}",
        hits.join("\n")
    );
}

#[test]
fn runtime_and_regression_surfaces_drop_lowercase_transitional_terms() {
    let files = [
        "crates/studiofs-vfs/src/vfs/upload_lifecycle.rs",
        "crates/studiofs-vfs/tests/vfs_test.rs",
    ];
    let forbidden_terms = ["write-buffer", "upload coordinator"];

    let hits: Vec<_> = files
        .iter()
        .flat_map(|path| collect_term_hits(path, &forbidden_terms))
        .collect();

    assert!(
        hits.is_empty(),
        "runtime and regression surfaces still use transitional lowercase vocabulary:\n{}",
        hits.join("\n")
    );
}

#[test]
fn internal_vfs_types_and_helpers_drop_write_buffer_wording() {
    let files = [
        "AGENTS.md",
        "crates/studiofs-vfs/src/lock_order.rs",
        "crates/studiofs-vfs/src/vfs.rs",
        "crates/studiofs-vfs/src/local_dirty.rs",
        "crates/studiofs-vfs/src/vfs/namespace_ops.rs",
        "crates/studiofs-vfs/src/vfs/sync_ops.rs",
    ];
    let forbidden_terms = [
        "WriteBufferState",
        "WriteBufferDrain",
        "OrderedWriteSession",
        "flush_ordered",
        "LockTier::WriteBuffer",
        "write_buffer:",
    ];

    let hits: Vec<_> = files
        .iter()
        .flat_map(|path| collect_term_hits(path, &forbidden_terms))
        .collect();

    assert!(
        hits.is_empty(),
        "internal VFS surfaces still expose write-buffer vocabulary:\n{}",
        hits.join("\n")
    );
}

#[test]
fn prefetch_helper_name_drops_planner_wording() {
    let hits = collect_term_hits(
        "crates/studiofs-vfs/src/vfs/prefetch.rs",
        &["PrefetchHintPlanner"],
    );

    assert!(
        hits.is_empty(),
        "prefetch helper still uses planner wording:\n{}",
        hits.join("\n")
    );
}

#[test]
fn access_tracking_surface_drops_classifier_wording() {
    let files = [
        "crates/studiofs-vfs/src/access.rs",
        "crates/studiofs-vfs/src/vfs.rs",
        "crates/studiofs-vfs/src/vfs/bootstrap.rs",
        "crates/studiofs-vfs/src/vfs/file_session.rs",
        "crates/studiofs-vfs/src/vfs/io_ops.rs",
        "crates/studiofs-vfs/src/vfs/prefetch.rs",
        "crates/studiofs-vfs/src/vfs/read_path.rs",
        "crates/studiofs-vfs/src/vfs/sync_ops.rs",
        "crates/studiofs-vfs/src/vfs/tests.rs",
    ];

    let hits: Vec<_> = files
        .iter()
        .flat_map(|path| collect_term_hits(path, &["AccessClassifierConfig", "access_classifier"]))
        .collect();

    assert!(
        hits.is_empty(),
        "access tracking surface still uses classifier wording:\n{}",
        hits.join("\n")
    );
}
