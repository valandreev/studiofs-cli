//! Tests that validate repository-wide architecture vocabulary and product-contract terms.
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

fn workspace_line_count(path: &str) -> usize {
    read_workspace_file(path).lines().count()
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
fn e2e_runner_contract_covers_required_sfs341_surfaces() {
    let doc = read_workspace_file("docs/e2e-runner-contract.md");
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
        "/dev/fuse",
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
        .filter(|term| !doc.contains(term))
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
        "fuse_mount_write_read_unmount_smoke",
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
fn linux_fuse_ext4_throughput_baseline_report_covers_sfs509_decision_gate() {
    let doc = read_workspace_file("docs/perf/sfs-509-linux-fuse-ext4-throughput-baseline.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "SFS-509",
        "native Linux 6.x",
        "ext4",
        "not WSL drvfs",
        "sequential read/write",
        "random read/write",
        "mixed read/write",
        "multi-job",
        "fsync-heavy",
        "metadata-heavy readdir/stat",
        "cold-cache",
        "warm-cache",
        "dirty-pressure",
        "large-file",
        "StudioFS current defaults",
        "async write opt-in",
        "async_dio opt-in",
        "FUSE background queue profiles",
        "async-read prototype",
        "JuiceFS",
        "StudioFS status snapshots",
        "pending async write bytes",
        "FUSE callback latency histograms",
        "CPU profiles",
        "flamegraphs",
        "RSS",
        "syscall rates",
        "context switches",
        "bottleneck ranking",
        "default-on/default-off recommendations",
        "target/sfs-509-linux-fuse-ext4-throughput",
        "fio",
        "perf record",
        "perf stat",
        "cargo test -p studiofs --test architecture_vocabulary_test linux_fuse_ext4_throughput_baseline_report_covers_sfs509_decision_gate -- --nocapture",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-509 Linux FUSE ext4 throughput baseline report is missing required decision-gate surfaces:\n{}",
        missing.join("\n")
    );
}

#[test]
fn linux_fuse_concurrent_dispatch_audit_covers_sfs514_decision() {
    let doc = read_workspace_file("docs/perf/sfs-514-linux-fuse-concurrent-dispatch-audit.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "SFS-514",
        "fuser 0.16",
        "fuser 0.17.0",
        "Config::n_threads",
        "FUSE_DEV_IOC_CLONE",
        "Filesystem trait methods use &self",
        "Send + Sync + 'static",
        "AsyncFilesystem",
        "breaking API migration",
        "owned async replies",
        "STUDIOFS_FUSE_ASYNC_METADATA",
        "slow read",
        "slow getattr",
        "slow readdir",
        "metadata operations during slow reads",
        "release/flush ordering",
        "mount shutdown with pending replies",
        "deterministic shutdown",
        "notifier registration",
        "mount error behavior",
        "existing async write barriers",
        "selected approach",
        "migration risks",
        "callback-level async is the safer route",
        "fuse3",
        "Rust 1.91",
        "async filesystem traits",
        "async handler dispatch",
        "inode-based APIs",
        "readdirplus",
        "notify support",
        "init replies",
        "max_write",
        "max_background",
        "congestion_threshold",
        "fuser-iouring",
        "single io_uring queue",
        "normal /dev/fuse read/write operations",
        "not the newer FUSE-over-io_uring kernel transport",
        "fuse-mt",
        "wrapper over fuser 0.16",
        "path-oriented",
        "subset of operations",
        "two tracks",
        "prototype fuse3 behind a Linux-only adapter",
        "measure whether upgrading to fuser 0.17 is enough",
        "SFS-515",
    ];

    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !contains_doc_term(&doc, &normalized_doc, term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-514 Linux FUSE concurrent dispatch audit is missing required decision-gate surfaces:\n{}",
        missing.join("\n")
    );
}

#[test]
fn linux_fuse_concurrent_dispatch_prototype_exposes_sfs514_opt_in() {
    let required_terms = [
        "STUDIOFS_FUSE_ASYNC_METADATA",
        "async_metadata_enabled",
        "should_enable_async_fuse_metadata_from_env",
        "opendir_with_reply",
        "FuseOpenReplyGuard",
        "getattr_with_reply",
        "readdir_with_reply",
        "FuseAttrReplyGuard",
        "FuseDirectoryReplyGuard",
        "DirectoryEntriesForReply",
        "async metadata disables readdirplus auto",
        "self.rt.spawn_blocking(move ||",
        "async_metadata_blocking_directory_callbacks_avoid_double_spawn",
        "FuseAttrReplyGuard dropped without explicit reply",
        "FuseOpenReplyGuard dropped without explicit reply",
        "FuseDirectoryReplyGuard dropped without explicit reply",
    ];

    let mut source = String::new();
    let mut stack = vec![repo_root().join("crates/studiofs-fuse/src")];
    while let Some(current) = stack.pop() {
        let metadata = fs::metadata(&current).unwrap_or_else(|error| {
            panic!("failed to stat '{}': {error}", current.display());
        });
        if metadata.is_dir() {
            for entry in fs::read_dir(&current).unwrap_or_else(|error| {
                panic!("failed to read dir '{}': {error}", current.display());
            }) {
                source.push('\n');
                stack.push(entry.expect("read dir entry").path());
            }
            continue;
        }
        if current.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            source.push_str(&fs::read_to_string(&current).unwrap_or_else(|error| {
                panic!("failed to read '{}': {error}", current.display());
            }));
            source.push('\n');
        }
    }
    let missing: Vec<_> = required_terms
        .into_iter()
        .filter(|term| !source.contains(term))
        .collect();

    assert!(
        missing.is_empty(),
        "SFS-514 async metadata prototype terms are missing from studiofs-fuse sources:\n{}",
        missing.join("\n")
    );
    assert!(
        !source.contains("Ok(Arc::new(entries))"),
        "SFS-514 async readdir cache miss should not allocate an Arc just for reply completion"
    );
    assert!(
        !source.contains("async metadata readdir task failed"),
        "SFS-514 async readdir should load entries and complete the reply in one blocking task"
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
fn macos_native_bridge_readiness_report_covers_sfs485_decision_gate() {
    let doc = read_workspace_file("docs/perf/sfs-485-macos-native-bridge-readiness.md");
    let normalized_doc = normalize_doc_whitespace(&doc);
    let required_terms = [
        "SFS-485",
        "macOS native bridge readiness and default rollout",
        "STUDIOFS_MOUNT_BACKEND=macfuse",
        "Do not make the native bridge default before this issue reaches a decision",
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
        "cargo test -p studiofs-fuse --test mount_sanity",
        "cargo test -p studiofs-winfsp --test mount_sanity",
        "studiofs test-cache --mounted",
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
