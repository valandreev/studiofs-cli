//! Tests that validate repository-wide architecture vocabulary and product-contract terms.
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

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

fn workspace_line_count(path: &str) -> usize {
    read_workspace_file(path).lines().count()
}

fn workspace_path_exists<P: AsRef<Path>>(path: P) -> bool {
    repo_root().join(path).exists()
}

fn workspace_member_manifests(root_manifest: &str) -> Vec<String> {
    let manifest =
        toml::from_str::<toml::Table>(root_manifest).expect("root Cargo.toml should be valid TOML");
    let members = manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("members"))
        .and_then(toml::Value::as_array)
        .expect("root Cargo.toml should define [workspace].members");

    members
        .iter()
        .map(|member| {
            let member = member
                .as_str()
                .expect("workspace member entries should be strings");
            format!("{member}/Cargo.toml")
        })
        .collect()
}

#[test]
fn workspace_members_inherit_workspace_lints() {
    let root_manifest = read_workspace_file("Cargo.toml");
    assert!(
        root_manifest.contains("[workspace.lints.rust]"),
        "root Cargo.toml should define a conservative Rust lint baseline"
    );
    assert!(
        root_manifest.contains("[workspace.lints.clippy]"),
        "root Cargo.toml should define a conservative Clippy lint baseline"
    );

    for manifest in workspace_member_manifests(&root_manifest) {
        let contents = read_workspace_file(&manifest);
        assert!(
            contents.contains("[lints]\nworkspace = true")
                || contents.contains("[lints]\r\nworkspace = true"),
            "{manifest} should opt into workspace lint inheritance with [lints] workspace = true"
        );
    }
}

fn normalize_doc_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn contains_doc_term(doc: &str, normalized_doc: &str, term: &str) -> bool {
    if doc.contains(term) {
        return true;
    }
    if is_normalized_doc_term(term) {
        return normalized_doc.contains(term);
    }

    normalized_doc.contains(&normalize_doc_whitespace(term))
}

fn is_normalized_doc_term(term: &str) -> bool {
    let mut previous_was_space = true;
    for ch in term.chars() {
        let is_space = ch == ' ';
        if ch.is_whitespace() && !is_space {
            return false;
        }
        if is_space && previous_was_space {
            return false;
        }
        previous_was_space = is_space;
    }

    !previous_was_space
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

#[test]
fn diagnostics_public_module_stays_a_facade() {
    let facade = read_workspace_file("crates/studiofs/src/diagnostics.rs");
    let line_count = workspace_line_count("crates/studiofs/src/diagnostics.rs");
    assert!(
        line_count <= 400,
        "diagnostics.rs should stay a facade; found {line_count} lines"
    );

    let inline_internals = [
        "struct DiagnosticsProfilingCoordinator",
        "struct LocalDiagnosticsHost",
        "struct LocalDiagnosticsClient",
        "struct ControllerLeaseManager",
        "enum DiagnosticsRequest",
        "enum DiagnosticsResponse",
    ];
    let unexpected = inline_internals
        .iter()
        .filter(|needle| facade.contains(**needle))
        .copied()
        .collect::<Vec<_>>();
    assert!(
        unexpected.is_empty(),
        "diagnostics.rs should re-export focused diagnostics submodules instead of owning internals inline: {}",
        unexpected.join(", ")
    );
}

#[test]
fn e2e_scenario_module_stays_split_by_responsibility() {
    let repo = repo_root();
    let legacy_monolith = repo.join("crates/studiofs-e2e/src/scenario.rs");
    let module_dir = repo.join("crates/studiofs-e2e/src/scenario");

    assert!(
        !legacy_monolith.exists(),
        "studiofs-e2e scenario code should live in focused scenario/ modules, not scenario.rs"
    );
    assert!(
        module_dir.join("mod.rs").exists(),
        "scenario module facade should live at crates/studiofs-e2e/src/scenario/mod.rs"
    );

    for module in ["mod.rs", "registry.rs", "types.rs", "workloads.rs"] {
        assert!(
            module_dir.join(module).exists(),
            "missing focused scenario submodule {module}"
        );
    }

    let workloads_dir = module_dir.join("workloads");
    for module in [
        "actions.rs",
        "advanced_mounted.rs",
        "assertions.rs",
        "faults.rs",
        "namespace_collision.rs",
        "smoke_outage.rs",
        "test_fixtures.rs",
        "tests.rs",
    ] {
        assert!(
            workloads_dir.join(module).exists(),
            "missing focused scenario workloads submodule {module}"
        );
    }

    let workloads_lines = workspace_line_count("crates/studiofs-e2e/src/scenario/workloads.rs");
    assert!(
        workloads_lines <= 300,
        "scenario/workloads.rs should only wire shared constants and focused modules; found {workloads_lines} lines"
    );

    let workloads = read_workspace_file("crates/studiofs-e2e/src/scenario/workloads.rs");
    assert!(
        !workloads.contains("#[path"),
        "scenario/workloads.rs should use Rust's file layout instead of #[path] indirection"
    );
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

struct TempSource {
    _dir: tempfile::TempDir,
    file: tempfile::NamedTempFile,
}

impl TempSource {
    fn path(&self) -> &std::path::Path {
        self.file.path()
    }
}

fn write_temp_source(contents: &str) -> TempSource {
    let dir = studiofs_test_support::workspace_target_tempdir("architecture-source-");
    let mut file = tempfile::NamedTempFile::new_in(dir.path()).unwrap_or_else(|error| {
        panic!("failed to create temp source file: {error}");
    });
    file.write_all(contents.as_bytes()).unwrap_or_else(|error| {
        panic!(
            "failed to write temp source '{}': {error}",
            file.path().display()
        );
    });
    TempSource { _dir: dir, file }
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
fn pg_sync_contract_contains_required_invariants() {
    // CONTEXT: This is a documentation contract guard for SFS-695. Runtime
    // startup behavior is implemented by the follow-up PG sync issues.
    let contract = read_workspace_file("docs/pg-sync-contract.md");
    let normalized_contract = normalize_doc_whitespace(&contract);
    let required_terms = [
        "# PostgreSQL Sync Contract",
        "SFS-695",
        "breaking on-disk/protocol change",
        "Migration compatibility is not required for existing test disks",
        "Serving-First Mount Lifecycle",
        "checkpoint-first bootstrap",
        "Bounded, Paged, and Resumable Catch-Up",
        "frozen startup target",
        "local-ready",
        "catching-up",
        "target-reached",
        "healthy",
        "degraded(reason)",
        "read-only authority",
        "read/write authority",
        "replay cursor",
        "checkpoint lag",
        "degraded read-only",
        "page boundary must never split",
        "last fully applied batch",
        "producer must enforce the replay envelope",
        "replay row limit",
        "replay byte limit",
        "SQL insert chunking inside one transaction is not a batch bound",
        "max_checkpoint_lag_versions",
        "single-flight",
        "coordination lease",
        "permanently unmountable",
        "`destroy --local`",
        "SFS-696",
        "SFS-697",
        "SFS-698",
        "SFS-721",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&contract, &normalized_contract, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-695 PostgreSQL sync contract is missing required invariants:\n{}",
        missing.join("\n")
    );
}

#[test]
fn related_docs_link_to_canonical_pg_sync_contract() {
    let linked_docs = [
        "docs/architecture.md",
        "docs/metadata.md",
        "docs/configuration.md",
        "docs/juicefs-local-first-rust-contract.md",
        "docs/local-first-validation-matrix.md",
        "docs/tests.md",
        "README.md",
    ];
    let missing_links: Vec<_> = linked_docs
        .into_iter()
        .filter(|path| !read_workspace_file(path).contains("pg-sync-contract.md"))
        .collect();

    assert!(
        missing_links.is_empty(),
        "SFS-695 canonical PostgreSQL sync contract links are missing from:\n{}",
        missing_links.join("\n")
    );
}

#[test]
fn configuration_docs_label_pg_sync_target_behavior() {
    let configuration = read_workspace_file("docs/configuration.md");
    let normalized_configuration = normalize_doc_whitespace(&configuration);
    let required_terms = [
        "Target behavior (SFS-695",
        "runtime follow-ups SFS-696 through SFS-698",
        "Current transitional behavior",
        "does not yet return checkpoint/head versions",
        "fresh-client authority state",
    ];
    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&configuration, &normalized_configuration, term))
        .collect();

    assert!(
        missing.is_empty(),
        "configuration docs must label SFS-695 behavior as transitional until runtime follow-ups land:\n{}",
        missing.join("\n")
    );
}

#[test]
fn active_docs_drop_superseded_pg_startup_wording() {
    let active_docs = [
        (
            "docs/architecture.md",
            read_workspace_file("docs/architecture.md"),
        ),
        ("docs/metadata.md", read_workspace_file("docs/metadata.md")),
        (
            "docs/configuration.md",
            read_workspace_file("docs/configuration.md"),
        ),
    ];
    let forbidden_legacy_statements = [
        "Before starting the filesystem, the client connects to the remote PostgreSQL database and compares the local UUID with the remote one.",
        "The operation is blocked because the identity of the disk cannot be verified.",
        "startup catch-up did not reach the current remote head",
    ];
    let legacy_hits: Vec<String> = active_docs
        .iter()
        .flat_map(|(path, doc)| {
            forbidden_legacy_statements
                .iter()
                .filter(move |term| doc.contains(*term))
                .map(move |term| format!("{path}: found `{term}`"))
        })
        .collect();

    assert!(
        legacy_hits.is_empty(),
        "active docs still contain superseded PostgreSQL startup wording:\n{}",
        legacy_hits.join("\n")
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
        "crates/studiofs/src/cli/benchmark_profile_common.rs",
        "crates/studiofs/src/cli/benchmark_profile_stub.rs",
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
fn e2e_runner_contract_covers_required_sfs341_surfaces() {
    let doc = read_workspace_file("docs/e2e-runner-contract.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "# Real E2E Runner Contract",
        "## Runner Boundaries And CLI",
        "target/e2e-runs/<run-id>",
        "## Artifact Layout And Retention",
        "report.json",
        "stdout.log",
        "stderr.log",
        "events.ndjson",
        "## JSON Report Shape",
        "## Status And Exit-Code Semantics",
        "passed",
        "failed",
        "skipped",
        "preflight_failed",
        "cleanup_failed",
        "## Lifecycle Hooks",
        "setup",
        "run",
        "validate",
        "cleanup",
        "## Preflight Gates",
        "Docker",
        "Linux mounted runs require `/dev/fuse`",
        "missing mount prerequisites fail preflight explicitly",
        "WinFSP",
        "macFUSE",
        "floating RustFS tag",
        "port allocation",
        "stale mounts",
        "## Cleanup Safety",
        "must never delete outside",
        "explicit client roots",
        "## Initial Scenario Gating",
        "smoke",
        "outage",
        "collision",
    ];

    let missing: Vec<_> = required_terms
        .iter()
        .copied()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-341 E2E runner contract is missing required surfaces:\n{}",
        missing.join("\n")
    );
}

#[test]
fn encryption_at_rest_contract_covers_required_sfs346_boundaries() {
    let doc = read_workspace_file("docs/encryption-at-rest-contract.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "Encryption-At-Rest Contract",
        "XChaCha20-Poly1305 is the only supported algorithm",
        "remote payload fragment",
        "4 MiB",
        "local payload cache stays plaintext",
        "object storage stores only ciphertext",
        "there is no legacy plaintext volume mode",
        "all current and newly formatted volumes are encrypted at rest by default",
        "protects against object storage and bucket compromise",
        "does not encrypt filenames",
        "does not encrypt file sizes",
        "does not encrypt inode metadata",
        "does not encrypt ACL metadata",
        "does not encrypt PostgreSQL metadata",
        "does not encrypt client process memory",
        "does not protect against an attacker who can read PostgreSQL or local metadata",
        "AES-256-GCM",
        "RSA wrapping",
        "user-facing encryption flags",
        "passphrase prompts",
        "KMS configuration",
        "cryptographically secure random 256-bit volume key",
        "cryptographically secure random 192-bit nonce",
        "24-byte nonce",
        "nonce prefix",
        "replay of an older valid ciphertext for the same remote-object identity",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-346 encryption-at-rest contract is missing required boundaries:\n{}",
        missing.join("\n")
    );
}

#[test]
fn transport_encryption_contract_covers_required_sfs448_boundaries() {
    let contract = read_workspace_file("docs/transport-encryption-contract.md");
    let normalized_contract = normalize_doc_whitespace(&contract);
    let required_terms = [
        "SFS-448",
        "Transport Encryption Contract",
        "transport-only",
        "must not reopen at-rest SFEC implementation decisions",
        "S3-compatible object storage endpoints honor the operator-selected URL scheme",
        "Plain HTTP S3 is accepted when the endpoint URL explicitly uses",
        "localhost",
        "127.0.0.0/8",
        "::1",
        "IPv4-mapped IPv6 loopback addresses",
        "IPv4-compatible IPv6 loopback",
        "must be normalized",
        "memory:// remains a test-only operator path",
        "init/configuration layer rejects memory:// for production user-supplied endpoints",
        "TLS is preferred for non-local PostgreSQL DSNs",
        "If sslmode is omitted for a non-local DSN",
        "TLS-preferred behavior",
        "fallback is allowed for operator compatibility",
        "info level, not warning level",
        "sslmode=require",
        "TLS setup failures remain fatal",
        "`sslmode=disable` is accepted as an explicit cleartext operator choice",
        "Unix domain sockets",
        "no noisy downgrade warning",
        "PostgreSQL carries shared metadata and active volume key material",
        "volume_crypto_keys.key_material",
        "reject policy-invalid transports early",
        "sanitize DSNs and endpoints",
        "must not log credentials or key material",
        "Existing local development and CI setups",
        "loopback RustFS/PostgreSQL",
        "remain supported",
        "SFS-449",
        "SFS-450",
        "SFS-452",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&contract, &normalized_contract, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-448 transport encryption contract is missing required boundaries:\n{}",
        missing.join("\n")
    );
}

#[test]
fn tests_guide_covers_sfs451_transport_tls_fixtures() {
    let doc = read_workspace_file("docs/tests.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "SFS-451",
        "STUDIOFS_TEST_PG_TLS_DSN",
        "STUDIOFS_S3_HTTPS_ENDPOINT",
        "optional external TLS fixtures",
        "skip cleanly when unset",
        "Loopback RustFS/PostgreSQL fixtures remain valid",
        "cargo test -p studiofs-meta pg_dsn -- --nocapture",
        "cargo test -p studiofs-data endpoint -- --nocapture",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-451 transport TLS fixture docs are missing required surfaces:\n{}",
        missing.join("\n")
    );
}

#[test]
fn docs_cover_sfs452_final_transport_acceptance_gate() {
    let docs = [
        read_workspace_file("docs/transport-encryption-contract.md"),
        read_workspace_file("docs/configuration.md"),
        read_workspace_file("docs/architecture.md"),
        read_workspace_file("docs/data-layer.md"),
        read_workspace_file("docs/encryption-at-rest-contract.md"),
        read_workspace_file("docs/tests.md"),
        read_workspace_file("docs/examples.md"),
    ];
    let searchable_docs: Vec<_> = docs
        .iter()
        .map(|doc| {
            let clean_doc = doc.replace('`', "");
            let normalized_doc = normalize_doc_whitespace(&clean_doc);
            (clean_doc, normalized_doc)
        })
        .collect();
    let required_terms = [
        "SFS-452",
        "final transport encryption acceptance gate",
        "S3-compatible endpoints honor the operator-selected https:// or http:// scheme",
        "Plain HTTP S3 is accepted when the endpoint explicitly uses http://",
        "non-local PostgreSQL DSNs use TLS-preferred or TLS-required transport",
        "sslmode=disable is accepted as an explicit cleartext operator choice",
        "must also be logged at info level for non-local hosts",
        "PostgreSQL carries shared metadata and active volume key material",
        "SFEC protects remote payload confidentiality and integrity at rest",
        "TLS protects credentials, metadata/key-material links, and network transport",
        "TLS does not replace SFEC and SFEC does not replace TLS",
        "malformed, credential-bearing, memory://, unsupported-scheme, and bare non-loopback S3 endpoints are rejected before init, persisted mount, or endpoint override",
        "production PostgreSQL call sites use connect_studiofs_postgres instead of tokio_postgres::NoTls",
        "remaining tokio_postgres::NoTls call sites are local fixture setup",
        "Local loopback examples remain available as development/test examples",
        "https://s3.example.com",
        "postgresql://user:pass@db.example.com/studiofs?sslmode=require",
        "http://127.0.0.1:9000",
        "sslmode=disable",
        "no user passphrases, KMS, S3 SSE/KMS, AES/RSA modes, legacy plaintext fallback, or local-cache encryption",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| {
            let normalized_term = normalize_doc_whitespace(term);
            !searchable_docs.iter().any(|(doc, normalized_doc)| {
                doc.contains(term) || normalized_doc.contains(&normalized_term)
            })
        })
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-452 final transport acceptance docs are missing required terms:\n{}",
        missing.join("\n")
    );
}

#[test]
fn local_first_validation_matrix_covers_sfs356_encrypted_regression_gates() {
    let doc = read_workspace_file("docs/local-first-validation-matrix.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "SFS-356",
        "Encrypted remote object regression gate",
        "encrypted_remote_object_storage_keeps_remote_ciphertext_and_local_plaintext_cache",
        "cache_hit_read_does_not_touch_encrypted_remote_object",
        "foreground_fetch_decrypts_one_remote_object_and_caches_plaintext_payload",
        "prefetch_uses_same_decrypt_path_and_caches_plaintext_payload",
        "random_block_fetch_only_decrypts_requested_remote_object",
        "corrupt_or_swapped_encrypted_objects_fail_before_cache_population",
        "crypto_fetch_failures_are_observed_without_retrying",
        "successful_fetch_records_encrypted_remote_read_counters",
        "test_write_read_large_file",
        "foreground_read_baseline_during_and_post_upload_are_comparable",
        "warm_cache_sequential_read_bounded_latency_after_write",
        "mounted_regular_file_io_is_visible_across_reopen",
        "mounted_namespace_mutations_are_visible_through_readdir",
        "winfsp_mount_write_read_unmount_smoke",
        "SFEC",
        "whole-file decrypt",
        "warm cache hits",
        "foreground read while background upload is active",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-356 encrypted remote regression matrix is missing required gates:\n{}",
        missing.join("\n")
    );
}

#[test]
fn local_first_validation_matrix_covers_sfs610_linux_release_gate() {
    let matrix = read_workspace_file("docs/local-first-validation-matrix.md");
    let tests = read_workspace_file("docs/tests.md");
    let bridge = read_workspace_file("docs/platform-bridges.md");

    let normalized_matrix = normalize_doc_whitespace(&matrix);
    let normalized_tests = normalize_doc_whitespace(&tests);
    let normalized_bridge = normalize_doc_whitespace(&bridge);

    let matrix_terms = [
        "SFS-610",
        "Ubuntu 24.04+",
        "Rocky Linux 9.4+",
        "Rocky Linux 8.6+",
        "best-effort only",
        // Exact Linux smoke test names keep the documented gate tied to cargo.
        "mounted_regular_file_io_is_visible_across_reopen",
        "mounted_namespace_mutations_are_visible_through_readdir",
        "create/write/read/close/reopen",
        "xattr get/set/list/remove",
        "flush/fsync/release local-only semantics",
        "kernel invalidation",
        "large sequential write",
        "upload/backpressure observability",
        "Unity/Unreal-style project tree workloads",
        "`direct` is the tested operational rollback",
        "/dev/fuse",
        "no longer a separate runtime profile",
        "validate-bench-linux.sh",
        "studiofs devbench",
        "target/perf",
    ];
    let tests_terms = [
        "Hosted Linux real mounted E2E is operator-run",
        "local-first-validation-matrix.md#sfs-610-linux-platform-release-gate",
    ];
    let bridge_terms = ["First validated target: Ubuntu 24.04+"];

    let missing: Vec<String> = matrix_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&matrix, &normalized_matrix, term))
        .map(|term| format!("docs/local-first-validation-matrix.md: {term}"))
        .chain(
            tests_terms
                .into_iter()
                .filter(|term| !contains_doc_term(&tests, &normalized_tests, term))
                .map(|term| format!("docs/tests.md: {term}")),
        )
        .chain(
            bridge_terms
                .into_iter()
                .filter(|term| !contains_doc_term(&bridge, &normalized_bridge, term))
                .map(|term| format!("docs/platform-bridges.md: {term}")),
        )
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-610 Linux platform gate docs are missing required surfaces:\n{}",
        missing.join("\n")
    );
}

#[test]
fn docs_cover_sfs357_final_encryption_acceptance_gate() {
    let contract = read_workspace_file("docs/encryption-at-rest-contract.md");
    let config = read_workspace_file("docs/configuration.md");
    let matrix = read_workspace_file("docs/local-first-validation-matrix.md");
    let tests = read_workspace_file("docs/tests.md");
    let combined = format!("{contract}\n{config}\n{matrix}\n{tests}");
    let combined_clean = combined.replace('`', "");
    let normalized_doc = normalize_doc_whitespace(&combined_clean);
    let required_terms = [
        "SFS-357",
        "Final encrypted-at-rest acceptance gate",
        "encrypts remote payload objects before object-storage upload",
        "users do not configure encryption",
        "local cache remains plaintext intentionally for performance",
        "object storage contains encrypted SFEC objects, not plaintext STFS objects",
        "only XChaCha20-Poly1305 is supported",
        "no AES-256-GCM",
        "RSA",
        "user passphrase",
        "KMS",
        "legacy plaintext volume compatibility",
        "object storage and bucket compromise",
        "payload confidentiality in remote object storage",
        "remote payload tampering detection",
        "object-key or object-fragment swap detection",
        "local payload cache files",
        "process memory",
        "PostgreSQL metadata",
        "attackers who can read both metadata/key material",
        "encrypted_remote_object_storage_keeps_remote_ciphertext_and_local_plaintext_cache",
        "cargo test --workspace --all-targets -- --nocapture",
        "no user-facing encryption CLI or mount options were added",
        "no legacy plaintext fallback branch was added",
        "no AES/RSA implementation was added",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&combined_clean, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-357 final encrypted-at-rest acceptance docs are missing required terms:\n{}",
        missing.join("\n")
    );
}

#[test]
fn local_first_e2e_baseline_audit_covers_sfs402_surfaces() {
    let doc = read_workspace_file("docs/local-first-e2e-baseline-audit.md");
    let required_terms = [
        "# Local First E2E Baseline Audit",
        "f4e5e831252f265b16bba569dc7b8f274645d26e",
        "SFS-309 through SFS-316",
        "SFS-317 through SFS-329",
        "Local First contract conflicts",
        "Product bugs",
        "Harness bugs",
        "Environment drift",
        "Incorrect test expectations",
        "retained lessons",
        "PostgreSQL 18",
        "rustfs/rustfs:latest",
        "Windows process cleanup",
        "zombie",
        "landing-zone",
        "stale resource recovery",
        "Follow-up rewrites",
    ];

    let missing: Vec<_> = required_terms
        .iter()
        .copied()
        .filter(|term| !doc.contains(term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-402 baseline audit is missing required surfaces:\n{}",
        missing.join("\n")
    );
}

#[test]
fn linux_adaptive_perf_gate_covers_mandatory_workloads_and_phase_checks() {
    let script = read_workspace_file("tools/perf/linux-juicefs-parity-gate.sh");
    let required_terms = [
        "cold-sequential-read",
        "repeated-warm-read",
        "random-read",
        "larger-than-memory-read",
        "inode-barrier-isolation",
        "small-files-4kib",
        "small-files-16kib",
        "small-files-64kib",
        "write_ms",
        "read_stat_ms",
        "cleanup_ms",
        "phase_checks",
    ];

    let missing: Vec<_> = required_terms
        .iter()
        .copied()
        .filter(|term| !script.contains(term))
        .collect();

    assert!(
        missing.is_empty(),
        "Linux adaptive performance gate is missing mandatory coverage:\n{}",
        missing.join("\n")
    );

    if !cfg!(target_os = "linux") {
        return;
    }

    let fixture =
        studiofs_test_support::workspace_target_tempdir("linux-cache-gate-report-fixture-");
    let targets = [
        "local",
        "studiofs_feature_direct",
        "studiofs_main_direct",
        "studiofs_feature_adaptive",
        "juicefs",
    ];
    let workloads = [
        "repeated-warm-read",
        "inode-barrier-isolation",
        "small-files-4kib",
    ];
    for target in targets {
        for workload in workloads {
            let run_dir = fixture.path().join(target).join(workload).join("run-001");
            fs::create_dir_all(&run_dir).expect("create performance gate fixture directory");

            let (wall_ms, fsync_p95_ms) = match (target, workload) {
                ("studiofs_feature_adaptive", "repeated-warm-read") => (90.0, None),
                ("studiofs_feature_direct", "inode-barrier-isolation") => (100.0, Some(20.0)),
                (_, "inode-barrier-isolation") => (100.0, Some(10.0)),
                _ => (100.0, None),
            };
            fs::write(
                run_dir.join("metrics.json"),
                serde_json::to_vec(&serde_json::json!({
                    "status": 0,
                    "wall_ms": wall_ms,
                    "fsync_p95_ms": fsync_p95_ms,
                }))
                .expect("serialize performance gate metrics"),
            )
            .expect("write performance gate metrics");

            let read_stat_ms =
                if target == "studiofs_feature_adaptive" && workload == "small-files-4kib" {
                    200.0
                } else {
                    100.0
                };
            fs::write(
                run_dir.join("stdout.txt"),
                format!(
                    "{}\n",
                    serde_json::json!({
                        "write_ms": 100.0,
                        "read_stat_ms": read_stat_ms,
                        "cleanup_ms": 100.0,
                    })
                ),
            )
            .expect("write performance gate stdout fixture");
        }
    }

    let script_path = repo_root().join("tools/perf/linux-juicefs-parity-gate.sh");
    let output = Command::new("bash")
        .arg(script_path)
        .args([
            "--local-target",
            "/tmp",
            "--studiofs-new-target",
            "/tmp",
            "--studiofs-old-target",
            "/tmp",
            "--studiofs-adaptive-target",
            "/tmp",
            "--juicefs-target",
            "/tmp",
        ])
        .env("STUDIOFS_PERF_REPORT_ONLY_DIR", fixture.path())
        .env("STUDIOFS_PERF_REPORT_WORKLOADS", workloads.join(" "))
        .output()
        .expect("run fixture-backed performance report");
    assert!(
        !output.status.success(),
        "fixture contains barrier-tail and small-file phase regressions"
    );

    let gate: serde_json::Value = serde_json::from_slice(
        &fs::read(fixture.path().join("gate.json")).expect("read fixture gate"),
    )
    .expect("parse fixture gate");
    let comparisons = gate["comparisons"]
        .as_array()
        .expect("gate comparisons should be an array");
    let barrier = comparisons
        .iter()
        .find(|item| item["workload"] == "inode-barrier-isolation")
        .expect("barrier comparison");
    assert_eq!(
        barrier["checks"]["fsync_p95_direct_median_vs_main"], false,
        "barrier fsync p95 regression must fail the gate"
    );
    let small_files = comparisons
        .iter()
        .find(|item| item["workload"] == "small-files-4kib")
        .expect("small-file comparison");
    assert_eq!(
        small_files["phase_checks"]["read_stat_ms"]["adaptive_median_vs_direct"], false,
        "a small-file phase regression must fail the gate"
    );
}

#[test]
fn linux_async_fuse_benchmark_report_covers_sfs473_decision() {
    let doc = read_workspace_file("docs/perf/sfs-473-linux-async-fuse-benchmark.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "SFS-473",
        "Linux async FUSE opt-in benchmark",
        "default synchronous mode",
        "async write opt-in",
        "async write plus background tuning",
        "64 KiB writer pages",
        "STUDIOFS_FUSE_ASYNC_DIO=1",
        "sequential write",
        "multi-job write",
        "dirty-pressure workload",
        "fsync-heavy workload",
        "CPU",
        "memory",
        "pending async write bytes",
        "barrier-heavy workloads",
        "Windows WinFSP fio comparison",
        "winfsp_mount_fio_multi_job_smoke",
        "fio-3.42",
        "Linux remains synchronous by default",
        "cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_default_sync -- --ignored --exact --nocapture",
        "cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_opt_in -- --ignored --exact --nocapture",
        "cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_dio_opt_in -- --ignored --exact --nocapture",
        "cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_fio_multi_job -- --ignored --exact --nocapture",
        "cargo test -p studiofs-winfsp --test mount_sanity winfsp_mount_fio_multi_job_smoke -- --ignored --exact --nocapture",
    ];

    let missing: Vec<_> = required_terms
        .iter()
        .copied()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-473 Linux async FUSE benchmark report is missing required evidence:\n{}",
        missing.join("\n")
    );
}

#[test]
fn benchmark_methodology_covers_sfs558_operator_contract() {
    let doc = read_workspace_file("docs/perf/benchmark-methodology.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    // CONTEXT: This is a documentation contract guard for the SFS-558 public
    // methodology. Keep the list to stable concepts, command names, JSON field
    // names, and platform names so normal editorial rewrites do not break CI.
    let required_terms = [
        "SFS-558",
        "StudioFS Benchmark And Stats Methodology",
        "studiofs perf",
        "observer only",
        ".studiofs-runtime-stats",
        "studiofs bench",
        "studiofs devbench",
        "active StudioFS mount",
        "developer workload",
        "target/perf",
        "runtime_stats.available: false",
        "local_first_read_comparison",
        "Windows WinFSP",
        "macOS macFUSE",
        "Linux FUSE",
        "JuiceFS",
        "same workload shape",
        "cold pass",
        "warm pass",
        "at least three runs",
        "p95/p99 latency",
        "smoothness indicators",
        "known limits",
        "not a correctness substitute",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-558 benchmark methodology doc is missing required operator contract terms:\n{}",
        missing.join("\n")
    );
}

fn sfs567_benchmark_docs() -> &'static str {
    static DOCS: OnceLock<String> = OnceLock::new();
    DOCS.get_or_init(|| {
        [
            read_workspace_file("docs/configuration.md"),
            read_workspace_file("docs/perf/benchmark-methodology.md"),
            read_workspace_file("docs/perf/benchmark-validation.md"),
            read_workspace_file("tools/perf/README.md"),
        ]
        .join("\n")
    })
}

#[test]
fn benchmark_docs_cover_sfs567_split_terms() {
    let docs = sfs567_benchmark_docs();
    let normalized_docs = normalize_doc_whitespace(docs);
    let required_terms = [
        "SFS-567",
        "studiofs bench <mountpoint> --report <path>",
        "fixed pseudo-random workload",
        "repeatable across machines",
        "optional `--report`",
        "studiofs devbench",
        "--json-output",
        "--block-size",
        "--big-file-size",
        "--small-file-count",
        "--threads",
        "first-read",
        "not a guaranteed fully cold OS-cache measurement",
        "tools/perf",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(docs, &normalized_docs, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-567 benchmark docs are missing bench/devbench split terms:\n{}",
        missing.join("\n")
    );
}

#[test]
fn benchmark_docs_do_not_show_devbench_flags_on_public_bench_examples() {
    let docs = sfs567_benchmark_docs();
    let normalized_docs = normalize_doc_whitespace(docs);
    let forbidden_public_examples = [
        "studiofs bench <mountpoint> --block-size",
        "studiofs bench <mountpoint> --big-file-size",
        "studiofs bench <mountpoint> --small-file-count",
        "studiofs bench <mountpoint> --threads",
        "studiofs bench /mnt/studio/myvolume --block-size",
        "studiofs bench /Volumes/studio/myvolume --block-size",
        "studiofs bench Z:\\ --block-size",
    ];
    let leaked: Vec<_> = forbidden_public_examples
        .into_iter()
        .filter(|term| contains_doc_term(docs, &normalized_docs, term))
        .collect();

    assert!(
        leaked.is_empty(),
        "SFS-567 docs must not show developer flags on public studiofs bench examples:\n{}",
        leaked.join("\n")
    );
}

#[test]
fn macos_native_bridge_split_note_covers_sfs475_boundary() {
    let doc = read_workspace_file("docs/macos-native-bridge-split.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "SFS-475",
        "https://linear.app/valandreev/issue/SFS-475/define-macos-native-bridge-split-from-main",
        "SFS-474",
        "https://linear.app/valandreev/issue/SFS-474/probe-fuser-notifierstore-for-macos-repeated-read-cache-behavior",
        "fuser::Notifier::store",
        "EINVAL",
        "did not produce macOS kernel-cache hits",
        "studiofs-fuse becomes Linux-only",
        "studiofs-macfuse",
        "pub async fn mount_and_serve",
        "Result<(), MacFuseMountError>",
        "thiserror",
        "StudioVfs",
        "CLI/Backend Routing",
        "Fallback Behavior",
        "Historical Validation Sequence",
        "Current mounted and unit checks are post-rollout regression validation",
        "Validation Sequence",
        "First Implementation Issues",
    ];

    let missing: Vec<_> = required_terms
        .iter()
        .copied()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-475 macOS native bridge split note is missing required surfaces:\n{}",
        missing.join("\n")
    );
}

#[test]
fn configuration_contracts_match_executable_defaults() {
    let mount_backend = read_workspace_file("crates/studiofs/src/cli/mount/backend.rs");
    let winfsp = read_workspace_file("crates/studiofs-winfsp/src/lib.rs");
    let studiofs_manifest = read_workspace_file("crates/studiofs/Cargo.toml");
    let cli_help = read_workspace_file("crates/studiofs/src/cli/mod.rs");
    let configuration = read_workspace_file("docs/configuration.md");
    let coding_guide = read_workspace_file("docs/agents-coding-guide.md");
    let platform_bridges = read_workspace_file("docs/platform-bridges.md");
    let macos_split = read_workspace_file("docs/macos-native-bridge-split.md");
    let tests_guide = read_workspace_file("docs/tests.md");
    let workflow = read_workspace_file(".github/workflows/rust.yml");
    let normalized_mount_backend = normalize_doc_whitespace(&mount_backend);
    let normalized_winfsp = normalize_doc_whitespace(&winfsp);
    let normalized_studiofs_manifest = normalize_doc_whitespace(&studiofs_manifest);
    let normalized_configuration = normalize_doc_whitespace(&configuration);
    let normalized_coding_guide = normalize_doc_whitespace(&coding_guide);
    let normalized_platform_bridges = normalize_doc_whitespace(&platform_bridges);
    let normalized_macos_split = normalize_doc_whitespace(&macos_split);
    let normalized_tests_guide = normalize_doc_whitespace(&tests_guide);
    let normalized_workflow = normalize_doc_whitespace(&workflow);

    let required_contracts = [
        (
            "macOS executable default",
            contains_doc_term(
                &mount_backend,
                &normalized_mount_backend,
                "None => Box::new(MacFuseMountBackend)",
            ),
        ),
        (
            "macOS selector compatibility",
            contains_doc_term(
                &mount_backend,
                &normalized_mount_backend,
                "Some(value) if value.eq_ignore_ascii_case(\"legacy\") || value.eq_ignore_ascii_case(\"macfuse\") => { Box::new(MacFuseMountBackend) }",
            ),
        ),
        (
            "macOS configuration default",
            contains_doc_term(
                &configuration,
                &normalized_configuration,
                "On macOS, the legacy native macFUSE bridge is the default mount backend.",
            ),
        ),
        (
            "macOS split default",
            contains_doc_term(
                &macos_split,
                &normalized_macos_split,
                "The CLI now routes macOS mounts to `studiofs_macfuse` by default.",
            ),
        ),
        (
            "Linux executable default",
            contains_doc_term(
                &mount_backend,
                &normalized_mount_backend,
                "pub(crate) fn default_mount_backend() -> Box<dyn MountBackend> { Box::new(LinuxFuseMountBackend) }",
            ),
        ),
        (
            "Linux configuration default",
            contains_doc_term(
                &configuration,
                &normalized_configuration,
                "Linux mounts use the libfuse3 low-level bridge.",
            ),
        ),
        (
            "Linux bridge default",
            contains_doc_term(
                &platform_bridges,
                &normalized_platform_bridges,
                "Linux mount starts through a thin libfuse3 low-level bridge.",
            ),
        ),
        (
            "Windows executable default",
            contains_doc_term(
                &mount_backend,
                &normalized_mount_backend,
                "pub(crate) fn default_mount_backend() -> Box<dyn MountBackend> { Box::new(WinfspMountBackend) }",
            ),
        ),
        (
            "macOS target dependency",
            contains_doc_term(
                &studiofs_manifest,
                &normalized_studiofs_manifest,
                "[target.'cfg(target_os = \"macos\")'.dependencies] studiofs-macfuse.workspace = true",
            ),
        ),
        (
            "Linux target dependency",
            contains_doc_term(
                &studiofs_manifest,
                &normalized_studiofs_manifest,
                "[target.'cfg(target_os = \"linux\")'.dependencies] studiofs-linux-fuse.workspace = true",
            ),
        ),
        (
            "Windows target dependency",
            contains_doc_term(
                &studiofs_manifest,
                &normalized_studiofs_manifest,
                "[target.'cfg(windows)'.dependencies] studiofs-winfsp.workspace = true",
            ),
        ),
        (
            "WinFSP executable default",
            contains_doc_term(
                &winfsp,
                &normalized_winfsp,
                "const DEFAULT_FLUSH_AND_PURGE_ON_CLEANUP: bool = false;",
            ),
        ),
        (
            "WinFSP cleanup override",
            contains_doc_term(
                &winfsp,
                &normalized_winfsp,
                "let flush_and_purge_on_cleanup = parse_bool_env( \"STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP\", DEFAULT_FLUSH_AND_PURGE_ON_CLEANUP, );",
            ),
        ),
        (
            "WinFSP coding-guide default",
            contains_doc_term(
                &coding_guide,
                &normalized_coding_guide,
                "WinFSP `FlushAndPurgeOnCleanup` is disabled by default",
            ),
        ),
        (
            "WinFSP bridge default",
            contains_doc_term(
                &platform_bridges,
                &normalized_platform_bridges,
                "Flush/purge on cleanup | Disabled by default",
            ),
        ),
        (
            "WinFSP configuration default",
            contains_doc_term(
                &configuration,
                &normalized_configuration,
                "On Windows, WinFSP `FlushAndPurgeOnCleanup` is disabled by default",
            ),
        ),
        (
            "CLI Linux platform help",
            cli_help.contains("• Linux    (via libfuse3)"),
        ),
        (
            "platform test guide Linux bridge",
            contains_doc_term(
                &tests_guide,
                &normalized_tests_guide,
                "Linux libfuse3 bridge tests belong to `studiofs-linux-fuse`.",
            ),
        ),
        (
            "workflow macOS default",
            contains_doc_term(
                &workflow,
                &normalized_workflow,
                "macFUSE is the default macOS mount backend",
            ),
        ),
    ];

    let missing: Vec<_> = required_contracts
        .into_iter()
        .filter_map(|(contract, present)| (!present).then_some(contract))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-648 executable/default documentation contracts are missing:\n{}",
        missing.join("\n")
    );
}

#[test]
fn removed_configuration_knobs_have_no_active_surface() {
    let active_paths = [
        "crates/studiofs/src/cli/mount/session.rs",
        "crates/studiofs-vfs/src/vfs/config.rs",
        "crates/studiofs-vfs/src/vfs.rs",
        "crates/studiofs-vfs/src/vfs/tests.rs",
        "crates/studiofs-vfs/tests/vfs_test.rs",
        "crates/studiofs/src/cli/settings.rs",
        "docs/agents-coding-guide.md",
        "docs/configuration.md",
        "docs/superpowers/specs/2026-07-02-sfs-595-juicefs-writer-pipeline-design.md",
    ];
    let forbidden_terms = [
        "readahead_seq_step",
        "STUDIOFS_READAHEAD_SEQ_STEP",
        "readahead_random_penalty",
        "STUDIOFS_READAHEAD_RANDOM_PENALTY",
        "prefetch_max_per_cycle",
        "STUDIOFS_PREFETCH_MAX_PER_CYCLE",
        "max_drain_batch",
        "STUDIOFS_MAX_DRAIN_BATCH",
        "slab_flush_interval_ms",
        "STUDIOFS_SLAB_FLUSH_INTERVAL_MS",
    ];
    let leaked: Vec<_> = active_paths
        .iter()
        .flat_map(|path| collect_term_hits(path, &forbidden_terms))
        .collect();

    assert!(
        leaked.is_empty(),
        "SFS-648 removed configuration knobs still have active references:\n{}",
        leaked.join("\n")
    );
}

#[test]
fn macos_native_bridge_readiness_report_covers_sfs485_decision_gate() {
    let doc = read_workspace_file("docs/perf/sfs-485-macos-native-bridge-readiness.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "SFS-485",
        "macOS native bridge readiness and default rollout",
        "STUDIOFS_MOUNT_BACKEND=macfuse",
        "The current decision is default: macFUSE is the normal macOS mount path",
        "Recorded Decision",
        "post-rollout regression validation",
        "these reports do not gate whether macFUSE is the default",
        "read/write/truncate/xattr/Finder-focused scenarios",
        "current fuser baseline",
        "callback count",
        "throughput",
        "Linux FUSE and Windows WinFSP focused validations show no regression",
        "default",
        "opt-in",
        "reject",
        "operator should run, but the agent should not run locally",
        "cargo test -p studiofs --test architecture_vocabulary_test macos_native_bridge_readiness_report_covers_sfs485_decision_gate -- --nocapture",
        "cargo test -p studiofs-macfuse",
        "STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-linux-fuse mounted_ -- --ignored --nocapture --test-threads=1",
        "cargo test -p studiofs-winfsp --test mount_sanity",
        "Capture mounted benchmark reports for the macFUSE candidate and fuser baseline",
        "docs/configuration.md",
        "docs/platform-bridges.md",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-485 macOS native bridge readiness report is missing required decision-gate surfaces:\n{}",
        missing.join("\n")
    );
}

fn benchmark_validation_required_files() -> [&'static str; 5] {
    [
        "tools/perf/validate-bench-windows.ps1",
        "tools/perf/validate-bench-unix.sh",
        "tools/perf/validate-bench-macos.sh",
        "tools/perf/validate-bench-linux.sh",
        "docs/perf/benchmark-validation.md",
    ]
}

fn benchmark_validation_combined_text() -> &'static str {
    static CACHE: OnceLock<String> = OnceLock::new();
    CACHE.get_or_init(|| {
        let windows_script = read_workspace_file("tools/perf/validate-bench-windows.ps1");
        let unix_script = read_workspace_file("tools/perf/validate-bench-unix.sh");
        let macos_script = read_workspace_file("tools/perf/validate-bench-macos.sh");
        let linux_script = read_workspace_file("tools/perf/validate-bench-linux.sh");
        let docs = read_workspace_file("docs/perf/benchmark-validation.md");

        format!("{windows_script}\n{unix_script}\n{macos_script}\n{linux_script}\n{docs}")
    })
}

fn sfs549_write_path_gate_docs() -> &'static str {
    static DOCS: OnceLock<String> = OnceLock::new();
    DOCS.get_or_init(|| {
        [
            read_workspace_file("docs/perf/benchmark-validation.md"),
            read_workspace_file("docs/perf/benchmark-methodology.md"),
            read_workspace_file("docs/local-first-validation-matrix.md"),
            read_workspace_file("tools/perf/README.md"),
            read_workspace_file("tools/perf/validate-bench-unix.sh"),
            read_workspace_file("tools/perf/validate-bench-windows.ps1"),
        ]
        .join("\n")
    })
}

#[test]
fn benchmark_validation_files_exist() {
    let missing_files: Vec<_> = benchmark_validation_required_files()
        .iter()
        .copied()
        .filter(|path| !workspace_path_exists(path))
        .collect();
    assert!(
        missing_files.is_empty(),
        "SFS-557 benchmark validation is missing files:\n{}",
        missing_files.join("\n")
    );
}

#[test]
fn benchmark_docs_cover_sfs549_write_path_gate() {
    let docs = sfs549_write_path_gate_docs();
    let normalized_docs = normalize_doc_whitespace(docs);
    let required_terms = [
        "SFS-549",
        "studiofs devbench --profile write-path-gate",
        "--profile write-path-gate",
        "write_path_gate",
        "single_file_sequential",
        "payload_file_count",
        "write_mib_per_sec",
        "repeated_read_mib_per_sec",
        "post_drain_warm_read_mib_per_sec",
        "cpu_percent",
        "dirty_backlog",
        "top_wait_counter_deltas",
        "fio is not required for the StudioFS side",
        "default to `1`",
        "threads_was_set",
        "$PSBoundParameters.ContainsKey(\"Threads\")",
        "validate-bench-linux.sh --mount-point /mnt/studio/myvolume --profile write-path-gate",
    ];
    let missing_terms: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(docs, &normalized_docs, term))
        .collect();

    assert!(
        missing_terms.is_empty(),
        "SFS-549 write-path gate docs are missing terms:\n{}",
        missing_terms.join("\n")
    );
}

#[test]
fn benchmark_validation_required_terms_cover_sfs557_cross_platform_contract() {
    let combined = benchmark_validation_combined_text();
    let normalized_combined = normalize_doc_whitespace(combined);
    let required_terms = [
        "target/perf",
        "studiofs devbench",
        "--json-output",
        "validate-bench-unix.sh",
        "studiofs devbench failed",
        "target\\release\\studiofs.exe",
        "target\\debug\\studiofs.exe",
        "target/release/studiofs",
        "target/debug/studiofs",
        "--runs",
        "--threads",
        "--small-file-count",
        "must be a positive integer",
        "normalize_positive_int",
        "10#$value",
        "is_executable_command",
        "$command_path\" == */* && -x \"$command_path",
        "a path value",
        "Starting benchmark run",
        "command.txt",
        "stdout.txt",
        "environment.json",
        "summary.md",
        "report.json",
        "run-001",
        "Do not overwrite previous benchmark artifacts",
        "recommended run count is 5",
        "compare medians",
        "p95",
        "p99",
        "mount point",
        "git commit",
        "OS",
        "benchmark settings",
        "Windows",
        "macOS",
        "Linux",
    ];
    let missing_terms: Vec<_> = required_terms
        .iter()
        .copied()
        .filter(|term| !contains_doc_term(combined, &normalized_combined, term))
        .collect();

    assert!(
        missing_terms.is_empty(),
        "SFS-557 benchmark validation contract is missing terms:\n{}",
        missing_terms.join("\n")
    );
}

#[test]
fn benchmark_validation_scripts_avoid_review_blocked_terms() {
    let combined = benchmark_validation_combined_text();
    let normalized_combined = normalize_doc_whitespace(combined);

    let forbidden_terms = [
        "$args = @(",
        "for run in $(seq",
        "while [[ -e \"$validation_dir\"",
        "repo_root_for_fallback",
        "${2:?}",
        "\"$runs\" -lt 1",
        "\"$small_file_count\" -lt 1",
        "\"$threads\" -lt 1",
        "> $stdoutPath 2>&1",
        "$ErrorActionPreference = \"Continue\"",
        "value=\"${value//$'\\n'/\\\\n}\"",
        "value=\"${value//$'\\r'/\\\\r}\"",
        "value=\"${value//$'\\t'/\\\\t}\"",
        "if (-not (Test-Path -LiteralPath $candidate))",
        "``$MountPoint``",
        "``$gitCommit``",
        "``$runDir``",
    ];
    let forbidden_hits: Vec<_> = forbidden_terms
        .iter()
        .copied()
        .filter(|term| contains_doc_term(combined, &normalized_combined, term))
        .collect();

    assert!(
        forbidden_hits.is_empty(),
        "SFS-557 benchmark validation scripts still contain review-blocked terms:\n{}",
        forbidden_hits.join("\n")
    );
}

#[test]
fn fuse3_macos_candidate_report_covers_sfs487_proof_gate() {
    let doc = read_workspace_file("docs/perf/sfs-487-fuse3-macos-candidate.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "SFS-487",
        "Sherlock-Holo/fuse3",
        "https://github.com/Sherlock-Holo/fuse3/pull/108",
        "https://github.com/Sherlock-Holo/fuse3/pull/109",
        "target/sfs-487-fuse3-proof",
        "custom_options(...)",
        "mount_options.rs",
        "viable migration candidate",
        "reference only",
        "rejected for StudioFS macOS",
        "current decision: reference only",
        "crates/studiofs/src/cli/mount/backend.rs",
        "crates/studiofs-macfuse/src/lib.rs",
        "crates/studiofs-macfuse/src/low_level.rs",
        "crates/studiofs-macfuse/src/adapter.rs",
        "crates/studiofs-fuse/Cargo.toml",
        "low-level reply ownership",
        "xattr",
        "setattr",
        "kernel invalidation",
        "large read",
        "No production dependency or routing change is allowed by this proof",
        "cargo test -p studiofs --test architecture_vocabulary_test fuse3_macos_candidate_report_covers_sfs487_proof_gate -- --nocapture",
        "cargo test -p studiofs-macfuse",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-487 fuse3 macOS candidate report is missing required proof-gate surfaces:\n{}",
        missing.join("\n")
    );
}

#[test]
fn macfuse_fskit_contract_and_legacy_baseline_covers_sfs668_gate() {
    let doc = read_workspace_file(
        "docs/exec-plans/SFS-668-macfuse-fskit-contract-and-legacy-baseline.md",
    );
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "SFS-668",
        "Define the macFUSE FSKit backend contract and record the legacy macOS performance baseline",
        "macFUSE 5.3.3",
        "macOS 15.4",
        "MFMount.framework",
        "MFChannelCreate",
        "MFMount",
        "MFChannelCopyNextMessage",
        "MFMessageGetBodyBuffers",
        "MFMessageGetReplyBuffer",
        "MFChannelSendMessage",
        "MFChannelInterrupt",
        "FUSE_DARWIN_REPLY_BUF",
        "FUSE_DARWIN_PAYLOAD_BUF",
        "fuse_in_header",
        "fuse_out_header",
        "fuse_reply_buf_out",
        "FUSE_INIT",
        "fuse_lowlevel_notify_inval_inode",
        "fuse_lowlevel_notify_inval_entry",
        "notification API is not supported",
        "mount points outside /Volumes",
        "files are always opened read/write",
        "fuse_context_t",
        "local-first write visibility",
        "flush/fsync/release",
        "ResourceFork",
        "kernel invalidation",
        "STUDIOFS_MOUNT_BACKEND=macfuse",
        "studiofs devbench",
        "studiofs bench",
        "validate-bench-macos.sh",
        "--runs 5",
        "1GB",
        "4MB",
        "128KB",
        "1000",
        "target/perf",
        "runtime_stats",
        "fuse_callbacks",
        "cache_pipeline",
        "mounted_memory",
        "sample variance",
        "cold sequential read",
        "warm repeated sequential read",
        "random read",
        "large-file read throughput",
        "write throughput",
        "flush/fsync latency",
        "metadata-heavy directory traversal/file-open",
        "median",
        "variance",
        "same host",
        "no production backend removal",
        "SFS-669",
        "SFS-671",
        "SFS-672",
        "cargo test -p studiofs --test architecture_vocabulary_test macfuse_fskit_contract_and_legacy_baseline_covers_sfs668_gate -- --nocapture",
        "https://github.com/macfuse/macfuse/wiki/FUSE-Backends",
        "https://github.com/macfuse/macfuse/releases",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-668 macFUSE FSKit contract and baseline is missing required decision-gate surfaces:\n{}",
        missing.join("\n")
    );
}

#[test]
fn deterministic_simulation_architecture_covers_required_sfs330_surfaces() {
    let doc = read_workspace_file("docs/deterministic-simulation-architecture.md");
    let required_terms = [
        "# Deterministic Simulation Architecture",
        "## Scope And Non-Goals",
        "core distributed state-machine behavior",
        "FUSE",
        "WinFSP",
        "OS page cache",
        "performance",
        "## Simulation Boundaries",
        "core logic",
        "metadata replication",
        "object upload",
        "cache/replay",
        "real OS mount",
        "## Required Abstractions",
        "virtual time",
        "deterministic scheduler",
        "simulated metadata bus",
        "simulated object store",
        "logical clients",
        "invariant validation",
        "## Test-Layer Relationship",
        "Contract/Core",
        "Real E2E",
        "Multi-client",
        "## Phase 1/2 Reuse",
        "operation model",
        "invariants",
        "artifacts",
        "## Counterexample Capture And Replay",
        "seed",
        "trace",
        "operation log",
        "replay",
        "## Initial Module Plan",
        "SFS-331",
        "SFS-332",
        "SFS-333",
        "SFS-334",
        "SFS-335",
        "SFS-336",
    ];

    let missing: Vec<_> = required_terms
        .iter()
        .copied()
        .filter(|term| !doc.contains(term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-330 deterministic simulation architecture is missing required surfaces:\n{}",
        missing.join("\n")
    );
}

#[test]
fn deterministic_simulation_architecture_covers_sfs407_replan() {
    let doc = read_workspace_file("docs/deterministic-simulation-architecture.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "SFS-407 Replan From Accepted Local First Contracts",
        "SFS-330",
        "SFS-402",
        "SFS-405",
        "accepted E2E baseline",
        "corrected Local First contracts",
        "Scenario 4",
        "Scenario 5",
        "deterministic simulation owns",
        "real mounted E2E owns",
        "OS cache behavior",
        "WinFSP callback timing",
        "kernel notification delivery",
        "SFS-395 through SFS-400",
        "clear product invariant",
    ];

    let missing: Vec<_> = required_terms
        .iter()
        .copied()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-407 deterministic simulation replan is missing required surfaces:\n{}",
        missing.join("\n")
    );
}

#[test]
fn deterministic_simulation_architecture_covers_sfs395_baseline_rebase() {
    let doc = read_workspace_file("docs/deterministic-simulation-architecture.md");
    let overview = read_workspace_file("docs/filesystem-testing-overview.md");
    let combined = format!("{doc}\n{overview}");
    let normalized_combined = normalize_doc_whitespace(&combined);
    let required_terms = [
        "SFS-395 Rebase On Accepted Windows E2E 1-16 Baseline",
        "accepted mounted Windows E2E 1-16 baseline",
        "deterministic backstop",
        "no longer a blocker for accepting scenarios 1-16",
        "WinFSP callback behavior",
        "OS cache behavior",
        "actual kernel notification delivery",
        "real process teardown",
        "SFS-396",
        "sparse-file truncate/write/read-range",
        "SFS-397",
        "cold-observer long-history convergence",
        "SFS-398",
        "invalidation dropout and fallback sync",
        "SFS-399",
        "symlink and metadata-attribute convergence",
        "SFS-400",
        "adversarial runner cleanup",
    ];

    let missing: Vec<_> = required_terms
        .iter()
        .copied()
        .filter(|term| !contains_doc_term(&combined, &normalized_combined, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-395 deterministic simulation baseline rebase is missing required surfaces:\n{}",
        missing.join("\n")
    );
    assert!(
        !doc.contains("SFS-395 through SFS-400 must not be resurrected unchanged"),
        "SFS-395 baseline rebase should replace stale canceled-chain wording"
    );
}

#[test]
fn cli_and_regression_surfaces_drop_buffered_first_wording() {
    let files = [
        "crates/studiofs/src/cli/benchmark_engine/reporting/human.rs",
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
fn vfs_facade_does_not_inline_startup_replay_and_read_context_internals() {
    let hits = collect_term_hits(
        "crates/studiofs-vfs/src/vfs.rs",
        &[
            "struct ReadRequestContext",
            "struct ForegroundReadScope",
            "struct StartupReplayCoordinator",
        ],
    );

    assert!(
        hits.is_empty(),
        "vfs.rs should stay a facade and keep startup replay / read-context internals in focused modules:\n{}",
        hits.join("\n")
    );
}

#[test]
fn local_dirty_facade_splits_ports_adapters_core_and_test_support() {
    for path in [
        "crates/studiofs-vfs/src/local_dirty/ports.rs",
        "crates/studiofs-vfs/src/local_dirty/core.rs",
        "crates/studiofs-vfs/src/local_dirty/adapters.rs",
        "crates/studiofs-vfs/src/local_dirty/test_support.rs",
    ] {
        assert!(
            repo_root().join(path).is_file(),
            "local_dirty should keep focused implementation surface in {path}"
        );
    }

    let hits = collect_term_hits(
        "crates/studiofs-vfs/src/local_dirty.rs",
        &[
            "trait BlockIdAllocatorPort",
            "trait LocalPayloadWriterPort",
            "trait OverlayCommitPort",
            "trait UploadSubmissionPort",
            "trait DrainCommitObserverPort",
            "struct TestLocalPayloadWriter",
            "struct TestOverlayCommitter",
            "struct TestUploadSubmitter",
            "struct TestDrainObserver",
        ],
    );

    assert!(
        hits.is_empty(),
        "local_dirty.rs should stay a facade and keep ports/adapters/test helpers in focused modules:\n{}",
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
        "crates/studiofs-linux-fuse/src",
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

#[test]
fn active_vfs_code_drops_legacy_handle_writer_data_plane() {
    let files = [
        "crates/studiofs-vfs/src/local_dirty.rs",
        "crates/studiofs-vfs/src/vfs.rs",
        "crates/studiofs-vfs/src/vfs",
        "crates/studiofs-vfs/tests",
    ];
    let forbidden_terms = [
        "HandleWriterRegistry",
        "handle_writers",
        "handle writer registry",
        "snapshot_handle_writer",
        "record_handle_writer",
        "materialize_handle_writer",
        "spill_handle_writer",
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
        "active VFS still exposes the removed handle-writer mounted data plane:\n{}",
        hits.join("\n")
    );
}
