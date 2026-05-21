//! Regression guard for unsafe process environment mutation in Rust code.

use std::fs;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn collect_rust_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}

fn mutation_patterns() -> [String; 4] {
    [
        ["std", "::", "env", "::", "set_var", "("].concat(),
        ["std", "::", "env", "::", "remove_var", "("].concat(),
        ["env", "::", "set_var", "("].concat(),
        ["env", "::", "remove_var", "("].concat(),
    ]
}

fn normalize_for_call_scan(content: &str) -> String {
    content.chars().filter(|ch| !ch.is_whitespace()).collect()
}

#[test]
fn rust_code_does_not_mutate_process_environment() {
    let root = workspace_root();
    let mut files = Vec::new();
    collect_rust_files(&root.join("crates"), &mut files);
    let patterns = mutation_patterns();

    let offenders = files
        .into_iter()
        .filter_map(|path| {
            let content = fs::read_to_string(&path).expect("read rust file");
            let normalized_content = normalize_for_call_scan(&content);
            if !patterns
                .iter()
                .any(|pattern| normalized_content.contains(pattern))
            {
                return None;
            }

            let matches = content
                .lines()
                .enumerate()
                .filter(|(_, line)| {
                    let normalized_line = normalize_for_call_scan(line);
                    patterns
                        .iter()
                        .any(|pattern| normalized_line.contains(pattern))
                })
                .map(|(idx, line)| {
                    format!(
                        "{}:{}: {}",
                        path.strip_prefix(&root).unwrap_or(&path).display(),
                        idx + 1,
                        line.trim()
                    )
                })
                .collect::<Vec<_>>();

            let matches = if matches.is_empty() {
                vec![format!(
                    "{}: contains process environment mutation call",
                    path.strip_prefix(&root).unwrap_or(&path).display()
                )]
            } else {
                matches
            };
            (!matches.is_empty()).then_some(matches)
        })
        .flatten()
        .collect::<Vec<_>>();

    assert!(
        offenders.is_empty(),
        "Rust code must not mutate the process environment; use injected config, thread-local test overrides, or child-process Command::env instead:\n{}",
        offenders.join("\n")
    );
}
