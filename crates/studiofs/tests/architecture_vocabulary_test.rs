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
    collect_term_hits_from_contents(path, &contents, terms)
}

fn collect_term_hits_outside_test_modules(path: &str, terms: &[&str]) -> Vec<String> {
    let contents = read_workspace_file(path);
    let filtered = strip_cfg_test_items(&contents);

    collect_term_hits_from_contents(path, &filtered, terms)
}

fn strip_cfg_test_items(contents: &str) -> String {
    let mut filtered = String::new();
    let mut pending_cfg_test = false;
    let mut skipping_test_module = false;
    let mut test_module_brace_depth = 0usize;

    for line in contents.lines() {
        let trimmed = line.trim();

        if skipping_test_module {
            test_module_brace_depth = test_module_brace_depth
                .saturating_add(line.matches('{').count())
                .saturating_sub(line.matches('}').count());
            if test_module_brace_depth == 0 {
                skipping_test_module = false;
            }
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("#[cfg(test)]") {
            let rest = rest.trim();
            if rest.is_empty() {
                pending_cfg_test = true;
                continue;
            }
            if is_module_declaration(rest) {
                skipping_test_module = true;
                test_module_brace_depth = rest
                    .matches('{')
                    .count()
                    .saturating_sub(rest.matches('}').count());
                if test_module_brace_depth == 0 {
                    skipping_test_module = false;
                }
            }
            continue;
        }

        if pending_cfg_test {
            if trimmed.is_empty() || trimmed.starts_with("#[") {
                continue;
            }
            if is_module_declaration(trimmed) {
                skipping_test_module = true;
                test_module_brace_depth = line
                    .matches('{')
                    .count()
                    .saturating_sub(line.matches('}').count());
                if test_module_brace_depth == 0 {
                    skipping_test_module = false;
                }
            }
            pending_cfg_test = false;
            continue;
        }

        filtered.push_str(line);
        filtered.push('\n');
    }

    filtered
}

fn is_module_declaration(line: &str) -> bool {
    let line = line.trim();
    (line.starts_with("mod ")
        || line.starts_with("pub mod ")
        || line.starts_with("pub(crate) mod ")
        || line.starts_with("pub(super) mod ")
        || line.starts_with("pub(self) mod "))
        && line.ends_with('{')
}

fn write_temp_source(contents: &str) -> PathBuf {
    let unique = format!(
        "studiofs-architecture-vocabulary-test-{}-{}.rs",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("current time")
            .as_nanos()
    );
    let path = std::env::temp_dir().join(unique);
    fs::write(&path, contents).unwrap_or_else(|error| {
        panic!("failed to write temp source '{}': {error}", path.display());
    });
    path
}

fn collect_term_hits_from_contents(path: &str, contents: &str, terms: &[&str]) -> Vec<String> {
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
fn helper_keeps_scanning_past_cfg_test_imports() {
    let path = write_temp_source(
        "use std::fmt;\n\
         #[cfg(test)]\n\
         use std::collections::BTreeMap;\n\
         fn production_surface() { let _ = \"cache mode\"; }\n",
    );
    let path_str = path.to_string_lossy().into_owned();

    let hits = collect_term_hits_outside_test_modules(&path_str, &["cache mode"]);

    fs::remove_file(&path).ok();
    assert_eq!(
        hits.len(),
        1,
        "helper should keep scanning production lines"
    );
}

#[test]
fn helper_skips_cfg_test_modules_but_keeps_following_production_code() {
    let path = write_temp_source(
        "fn before() {}\n\
         #[cfg(test)]\n\
         mod tests {\n\
             fn helper_surface() { let _ = \"cache mode\"; }\n\
         }\n\
         fn after() { let _ = \"writeback-delay\"; }\n",
    );
    let path_str = path.to_string_lossy().into_owned();

    let test_hits = collect_term_hits_outside_test_modules(&path_str, &["cache mode"]);
    let production_hits = collect_term_hits_outside_test_modules(&path_str, &["writeback-delay"]);

    fs::remove_file(&path).ok();
    assert!(
        test_hits.is_empty(),
        "helper should ignore cfg(test) module contents"
    );
    assert_eq!(
        production_hits.len(),
        1,
        "helper should resume scanning after cfg(test) modules"
    );
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
        "crates/studiofs/src/cli/mod.rs",
        "crates/studiofs/src/cli/mount/mod.rs",
        "crates/studiofs/src/cli/runtime_tuning.rs",
        "crates/studiofs/src/cli/settings.rs",
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
fn active_docs_and_cli_drop_writeback_delay_surface() {
    let files = [
        "crates/studiofs/src/cli/mod.rs",
        "crates/studiofs/src/cli/mount/mod.rs",
        "crates/studiofs/src/cli/runtime_tuning.rs",
        "crates/studiofs/src/cli/settings.rs",
        "docs/configuration.md",
        "docs/examples.md",
    ];

    let hits: Vec<_> = files
        .iter()
        .flat_map(|path| {
            if path.starts_with("docs/") {
                collect_term_hits(path, &["writeback-delay", "writeback_delay"])
            } else {
                collect_term_hits_outside_test_modules(
                    path,
                    &["writeback-delay", "writeback_delay"],
                )
            }
        })
        .collect();

    assert!(
        hits.is_empty(),
        "active docs and CLI still expose writeback-delay surface:\n{}",
        hits.join("\n")
    );
}

#[test]
fn active_docs_and_mount_profiling_drop_cache_mode_surface() {
    let files = [
        "crates/studiofs/src/cli/mount_profile.rs",
        "crates/studiofs/src/cli/mount_profile_stub.rs",
        "crates/studiofs/src/cli/mount/session.rs",
        "docs/configuration.md",
    ];

    let hits: Vec<_> = files
        .iter()
        .flat_map(|path| {
            if path.starts_with("docs/") {
                collect_term_hits(path, &["cache_mode", "cache mode"])
            } else {
                collect_term_hits_outside_test_modules(path, &["cache_mode", "cache mode"])
            }
        })
        .collect();

    assert!(
        hits.is_empty(),
        "active docs and mount profiling still expose cache-mode wording:\n{}",
        hits.join("\n")
    );
}

#[test]
fn validation_docs_drop_durable_mode_callback_wording() {
    let hits = collect_term_hits("docs/local-first-validation-matrix.md", &["durable mode"]);

    assert!(
        hits.is_empty(),
        "validation docs still describe callback semantics with durable-mode wording:\n{}",
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
