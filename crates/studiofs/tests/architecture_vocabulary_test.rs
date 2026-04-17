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

    for (line_number, line) in contents.lines().enumerate() {
        for term in terms {
            if line.contains(term) {
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
        "docs/cache-v3-contract.md",
        "docs/configuration.md",
        "docs/vfs.md",
    ];
    let forbidden_terms = [
        "buffered-first",
        "Cache 3.0",
        "WriteBuffer",
        "UploadCoordinator",
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
fn cli_surface_avoids_cache_30_product_wording() {
    let files = [
        "crates/studiofs/src/cli/cache_layout.rs",
        "crates/studiofs/src/cli/destroy.rs",
        "crates/studiofs/src/cli/status.rs",
    ];

    let hits: Vec<_> = files
        .iter()
        .flat_map(|path| collect_term_hits(path, &["Cache 3.0"]))
        .collect();

    assert!(
        hits.is_empty(),
        "CLI surface still uses Cache 3.0 product wording:\n{}",
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
