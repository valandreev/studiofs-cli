use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use proc_macro2::LineColumn;
use syn::spanned::Spanned;

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

fn collect_term_hits_recursively(path: &str, terms: &[&str]) -> Vec<String> {
    let root = repo_root().join(path);
    let mut stack = vec![root];
    let mut hits = Vec::new();

    while let Some(current) = stack.pop() {
        let metadata = fs::metadata(&current).unwrap_or_else(|error| {
            panic!("failed to stat '{}': {error}", current.display());
        });
        if metadata.is_dir() {
            for entry in fs::read_dir(&current).unwrap_or_else(|error| {
                panic!("failed to read dir '{}': {error}", current.display());
            }) {
                let entry = entry.unwrap_or_else(|error| {
                    panic!(
                        "failed to read dir entry in '{}': {error}",
                        current.display()
                    );
                });
                stack.push(entry.path());
            }
            continue;
        }
        if current.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let relative = current
            .strip_prefix(repo_root())
            .unwrap_or(current.as_path())
            .to_string_lossy()
            .replace('\\', "/");
        if relative == "crates/studiofs/tests/architecture_vocabulary_test.rs" {
            continue;
        }
        hits.extend(collect_term_hits(&relative, terms));
    }

    hits
}

fn collect_term_hits_outside_test_modules(path: &str, terms: &[&str]) -> Vec<String> {
    let contents = read_workspace_file(path);
    let filtered = strip_cfg_test_items(&contents);

    collect_term_hits_from_contents(path, &filtered, terms)
}

fn strip_cfg_test_items(contents: &str) -> String {
    let Ok(file) = syn::parse_file(contents) else {
        return contents.to_string();
    };
    let line_starts = line_start_offsets(contents);
    let mut excluded_ranges: Vec<_> = file
        .items
        .iter()
        .filter(|item| item_has_cfg_test(item))
        .filter_map(|item| item_source_range(contents, &line_starts, item))
        .collect();

    if excluded_ranges.is_empty() {
        return contents.to_string();
    }

    excluded_ranges.sort_unstable_by_key(|(start, _)| *start);
    let mut filtered = String::new();
    let mut cursor = 0usize;
    for (start, end) in excluded_ranges {
        let start = start.min(contents.len());
        let end = end.min(contents.len());
        if start > cursor {
            filtered.push_str(&contents[cursor..start]);
        }
        cursor = cursor.max(end);
    }
    filtered.push_str(&contents[cursor..]);
    filtered
}

fn item_has_cfg_test(item: &syn::Item) -> bool {
    item_attrs(item).iter().any(attr_is_cfg_test)
}

fn item_attrs(item: &syn::Item) -> &[syn::Attribute] {
    match item {
        syn::Item::Const(item) => &item.attrs,
        syn::Item::Enum(item) => &item.attrs,
        syn::Item::ExternCrate(item) => &item.attrs,
        syn::Item::Fn(item) => &item.attrs,
        syn::Item::ForeignMod(item) => &item.attrs,
        syn::Item::Impl(item) => &item.attrs,
        syn::Item::Macro(item) => &item.attrs,
        syn::Item::Mod(item) => &item.attrs,
        syn::Item::Static(item) => &item.attrs,
        syn::Item::Struct(item) => &item.attrs,
        syn::Item::Trait(item) => &item.attrs,
        syn::Item::TraitAlias(item) => &item.attrs,
        syn::Item::Type(item) => &item.attrs,
        syn::Item::Union(item) => &item.attrs,
        syn::Item::Use(item) => &item.attrs,
        syn::Item::Verbatim(_) => &[],
        _ => &[],
    }
}

fn attr_is_cfg_test(attr: &syn::Attribute) -> bool {
    if !attr.path().is_ident("cfg") {
        return false;
    }

    attr.parse_args_with(syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated)
        .map(|args| {
            args.into_iter().any(|meta| match meta {
                syn::Meta::Path(path) => path.is_ident("test"),
                _ => false,
            })
        })
        .unwrap_or(false)
}

fn item_source_range(
    contents: &str,
    line_starts: &[usize],
    item: &syn::Item,
) -> Option<(usize, usize)> {
    let attrs = item_attrs(item);
    let start = attrs.first()?.span().start();
    let end = item.span().end();
    Some((
        line_column_to_offset(contents, line_starts, start),
        line_column_to_offset(contents, line_starts, end),
    ))
}

fn line_start_offsets(contents: &str) -> Vec<usize> {
    let mut starts = vec![0];
    for (index, byte) in contents.bytes().enumerate() {
        if byte == b'\n' {
            starts.push(index + 1);
        }
    }
    starts
}

fn line_column_to_offset(contents: &str, line_starts: &[usize], location: LineColumn) -> usize {
    let Some(&line_start) = line_starts.get(location.line.saturating_sub(1)) else {
        return contents.len();
    };
    let line_end = line_starts
        .get(location.line)
        .copied()
        .unwrap_or(contents.len());
    let line = &contents[line_start..line_end];
    let byte_in_line = line
        .char_indices()
        .nth(location.column)
        .map(|(index, _)| index)
        .unwrap_or(line.len());
    (line_start + byte_in_line).min(contents.len())
}

fn write_temp_source(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap_or_else(|error| {
        panic!("failed to create temp source file: {error}");
    });
    file.write_all(contents.as_bytes()).unwrap_or_else(|error| {
        panic!(
            "failed to write temp source '{}': {error}",
            file.path().display()
        );
    });
    file
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
    let path_str = path.path().to_string_lossy().into_owned();

    let hits = collect_term_hits_outside_test_modules(&path_str, &["cache mode"]);

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
    let path_str = path.path().to_string_lossy().into_owned();

    let test_hits = collect_term_hits_outside_test_modules(&path_str, &["cache mode"]);
    let production_hits = collect_term_hits_outside_test_modules(&path_str, &["writeback-delay"]);

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
fn helper_skips_cfg_test_module_with_trailing_comment_and_next_line_brace() {
    let path = write_temp_source(
        "fn before() {}\n\
         #[cfg(test)]\n\
         pub(in crate::tests) mod tests // comment\n\
         {\n\
             fn helper_surface() { let _ = \"cache mode\"; }\n\
         }\n\
         fn after() { let _ = \"writeback-delay\"; }\n",
    );
    let path_str = path.path().to_string_lossy().into_owned();

    let test_hits = collect_term_hits_outside_test_modules(&path_str, &["cache mode"]);
    let production_hits = collect_term_hits_outside_test_modules(&path_str, &["writeback-delay"]);

    assert!(
        test_hits.is_empty(),
        "helper should ignore cfg(test) module contents even with comments and split braces"
    );
    assert_eq!(
        production_hits.len(),
        1,
        "helper should keep scanning production code after cfg(test) module"
    );
}

#[test]
fn helper_ignores_braces_inside_cfg_test_module_strings() {
    let path = write_temp_source(
        "fn before() {}\n\
         #[cfg(test)]\n\
         mod tests {\n\
             fn helper_surface() { let _ = \"{\"; }\n\
         }\n\
         fn after() { let _ = \"writeback-delay\"; }\n",
    );
    let path_str = path.path().to_string_lossy().into_owned();

    let production_hits = collect_term_hits_outside_test_modules(&path_str, &["writeback-delay"]);

    assert_eq!(
        production_hits.len(),
        1,
        "helper should not let braces in test-only strings hide following production code"
    );
}

#[test]
fn active_docs_drop_superseded_io_architecture_terms() {
    let files = [
        "README.md",
        "AGENTS.md",
        "docs/architecture.md",
        "docs/cache.md",
        "docs/local-payload-runtime-layout.md",
        "docs/juicefs-local-first-rust-contract.md",
        "docs/platform-bridges.md",
        "docs/superpowers/plans/2026-04-18-sfs-261-local-payload-runtime-cleanup.md",
        "docs/superpowers/specs/2026-04-18-sfs-261-local-payload-runtime-cleanup-design.md",
        "docs/configuration.md",
        "docs/data-layer.md",
        "docs/examples.md",
        "docs/local-first-validation-matrix.md",
        "docs/metadata.md",
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
        "local-block-runtime",
        "local-block-runtime-layout",
        "block-level I/O",
        "Block Cache logo",
        "hot block",
        "block-ID ranges",
        "block ID ranges",
        "block state",
        "block preallocation",
        "block allocation",
        "dirty blocks",
        "pending blocks",
        "block-store entries",
        "local dirty block",
        "block-id count",
        "block-shaped",
        "payload blocks",
        "block payload",
        "local-block runtime",
        "block eviction",
        "internal block eviction",
        "file-per-block",
        "block_*.rs",
        "block objects",
        "block caching",
        "block lifecycle",
        "block-store",
        "block store",
        "dirty block",
        "pending block",
        "local block",
        "regular-file block",
        "file block",
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

#[test]
fn active_code_drops_flat_block_contract_vocabulary() {
    let files = [
        "crates/studiofs-cache/src",
        "crates/studiofs-data/src",
        "crates/studiofs-meta/src",
        "crates/studiofs-vfs/src",
        "crates/studiofs-vfs/tests",
        "crates/studiofs/src",
        "crates/studiofs/tests",
        "crates/studiofs-winfsp/src",
        "crates/studiofs-fuse/src",
    ];
    let forbidden_terms = [
        "block_projection",
        "block projection",
        "BlockProjection",
        "MetaBlocks",
        "redb_engine/blocks.rs",
        "redb_engine\\blocks.rs",
        "local-block-runtime",
        "local-block-runtime-layout",
        "PendingBlock",
        "pending_blocks",
        "orphan_dirty_blocks",
        "copy_block_payload_into_range",
        "clear_pending_blocks_for_inode",
        "flush_pending_blocks_for_shutdown",
        "snapshot_dirty_local_dirty_blocks",
        "commit_persisted_local_dirty_blocks",
        "replace_pending_local_dirty_block",
        "copy_committed_block_payload_into_range",
        "remove_file_blocks",
        "FragmentedBlock",
        "set_file_block",
        "commit_uploaded_block",
        "publish_file_manifest",
        "stage_block_slices",
        "stage_block_tombstone_slices",
        "stage_logical_payload_tombstone",
        "lookup_file_block_slice",
        "lookup_file_block_slices",
        "FileSliceMetaFixture",
        "PgBlockSliceFixture",
        "BlockSliceFixture",
        "fixture_block_slices",
        "raw data blocks",
        "block-based VFS",
        "block_store",
        "block store",
        "next_block_id",
        "next_block_ids",
        "put_block",
        "get_block",
        "delete_block",
        "block_exists",
        "block_key",
        "nextBlockId",
        "pgBlock",
        "dirty blocks",
        "pending blocks",
        "block-store entries",
        "local dirty block",
        "block-id count",
        "block-shaped",
        "payload block",
        "payload blocks",
        "block payload",
        "block objects",
        "block lifecycle",
        "block-store",
        "dirty block",
        "pending block",
        "local block",
        "avg_blocks_per_burst",
        "max_blocks_per_burst",
        "payload_burst_total_blocks",
        "payload_burst_max_blocks",
        "chunk/slice/block",
        "block graph",
        "graph-backed",
    ];

    let hits: Vec<_> = files
        .iter()
        .flat_map(|path| {
            if repo_root().join(path).is_dir() {
                collect_term_hits_recursively(path, &forbidden_terms)
            } else {
                collect_term_hits(path, &forbidden_terms)
            }
        })
        .collect();

    assert!(
        hits.is_empty(),
        "active code still exposes flat-block contract vocabulary:\n{}",
        hits.join("\n")
    );
}
