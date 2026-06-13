//! Repository-level production lint policy checks.

use std::fs;
use std::path::{Path, PathBuf};

use syn::spanned::Spanned as _;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

#[test]
fn production_rust_sources_do_not_use_unscoped_panic_or_unwrap() {
    let mut violations = Vec::new();
    for source in production_rust_sources() {
        let contents = fs::read_to_string(&source)
            .unwrap_or_else(|error| panic!("failed to read '{}': {error}", source.display()));
        let filtered = strip_cfg_test_items(&contents);
        let relative = source
            .strip_prefix(repo_root())
            .unwrap_or(source.as_path())
            .to_string_lossy()
            .replace('\\', "/");

        violations.extend(collect_panic_policy_violations(&relative, &filtered));
    }

    assert!(
        violations.is_empty(),
        "production code must not use unwrap/expect/panic without a local #[expect(clippy::<lint>, reason = \"...\")] rationale:\n{}",
        violations.join("\n")
    );
}

fn production_rust_sources() -> Vec<PathBuf> {
    [
        "crates/studiofs-cache/src/cached_store.rs",
        "crates/studiofs-cache/src/payload_index.rs",
        "crates/studiofs-fuse/src/fs/pending_write_tracker.rs",
        "crates/studiofs-fuse/src/lib.rs",
        "crates/studiofs-winfsp/src/lib.rs",
    ]
    .into_iter()
    .map(|path| repo_root().join(path))
    .filter(|path| path_is_production_source(path))
    .collect()
}

fn path_is_production_source(path: &Path) -> bool {
    let components: Vec<_> = path
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .collect();
    let Some(src_index) = components.iter().position(|component| *component == "src") else {
        return false;
    };
    if components
        .get(src_index + 1..)
        .unwrap_or_default()
        .iter()
        .any(|component| *component == "tests" || *component == "test_support")
    {
        return false;
    }

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    file_name != "tests.rs" && !file_name.starts_with("test_")
}

fn collect_panic_policy_violations(path: &str, contents: &str) -> Vec<String> {
    // This is intentionally a narrow policy tripwire for the current production
    // allowlist. A broader repository policy should move to AST-based matching
    // instead of extending these textual needles.
    let mut violations = Vec::new();
    for (line_index, line) in contents.lines().enumerate() {
        if line.trim_start().starts_with("#[expect(") {
            continue;
        }
        for (needle, lint) in [
            (".unwrap(", "unwrap_used"),
            (".expect(", "expect_used"),
            ("panic!", "panic"),
        ] {
            if line.contains(needle) && !has_local_expect_reason(contents, line_index, lint) {
                violations.push(format!(
                    "{path}:{}: `{needle}` requires #[expect(clippy::{lint}, reason = \"...\")]",
                    line_index + 1
                ));
            }
        }
    }
    violations
}

fn has_local_expect_reason(contents: &str, line_index: usize, lint: &str) -> bool {
    let lines = contents.lines().collect::<Vec<_>>();
    let mut start = line_index;
    while start > 0 {
        let previous = lines[start - 1].trim();
        if previous.ends_with(';') || previous == "}" || previous == "}," {
            break;
        }
        start -= 1;
    }
    let window = lines[start..=line_index].join(" ");
    window.contains("#[expect(")
        && window.contains(&format!("clippy::{lint}"))
        && window.contains("reason =")
}

fn strip_cfg_test_items(contents: &str) -> String {
    let Ok(file) = syn::parse_file(contents) else {
        return contents.to_string();
    };
    let mut excluded_ranges: Vec<_> = file
        .items
        .iter()
        .filter(|item| item_has_cfg_test(item))
        .filter_map(item_source_range)
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

    // Keep this intentionally narrow: only exact test-only cfg predicates are
    // stripped. Broader cfg expressions such as `any(test, feature = "...")`
    // may also compile in production feature sets and must remain visible to
    // this policy check.
    attr.parse_args_with(syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated)
        .map(|args| {
            args.into_iter().any(|meta| match meta {
                syn::Meta::Path(path) => path.is_ident("test"),
                syn::Meta::List(list) => list.path.is_ident("test"),
                _ => false,
            })
        })
        .unwrap_or(false)
}

fn item_source_range(item: &syn::Item) -> Option<(usize, usize)> {
    let attrs = item_attrs(item);
    let start = attrs.first()?.span().byte_range().start;
    let end = item.span().byte_range().end;
    Some((start, end))
}

#[test]
fn strip_cfg_test_items_handles_utf8_before_test_item() {
    let contents =
        "const LABEL: &str = \"café\";\n#[cfg(test)]\nfn test_only() {}\nfn production() {}\n";
    let filtered = strip_cfg_test_items(contents);

    assert!(filtered.contains("const LABEL"));
    assert!(!filtered.contains("test_only"));
    assert!(filtered.contains("fn production"));
}

#[test]
fn expect_reason_only_covers_the_immediately_annotated_violation() {
    let contents = r#"
#[expect(clippy::unwrap_used, reason = "checked in test")]
let first = value.unwrap();
let second = other.unwrap();
"#;

    let violations = collect_panic_policy_violations("sample.rs", contents);

    assert_eq!(violations.len(), 1);
    assert!(violations[0].contains("sample.rs:4"));
}

#[test]
fn expect_reason_covers_multiline_statement_chain() {
    let contents = r#"
#[expect(clippy::expect_used, reason = "poisoned mutex is unrecoverable")]
self.state
    .lock()
    .expect("state poisoned")
"#;

    let violations = collect_panic_policy_violations("sample.rs", contents);

    assert!(violations.is_empty(), "{violations:?}");
}
