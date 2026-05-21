//! Regression guard for unsafe process environment mutation in test code.

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

#[test]
fn test_code_does_not_mutate_process_environment() {
    let root = workspace_root();
    let mut files = Vec::new();
    collect_rust_files(&root.join("crates"), &mut files);

    let offenders = files
        .into_iter()
        .filter(|path| {
            path.components().any(|component| {
                component.as_os_str() == "tests"
                    || component.as_os_str() == "benches"
                    || component.as_os_str() == "examples"
            }) || path.file_name().is_some_and(|name| name == "lib.rs")
        })
        .filter_map(|path| {
            let content = fs::read_to_string(&path).expect("read rust file");
            let matches = content
                .lines()
                .enumerate()
                .filter(|(_, line)| {
                    let set_var = ["std::env::", "set_var("].concat();
                    let remove_var = ["std::env::", "remove_var("].concat();
                    line.contains(&set_var) || line.contains(&remove_var)
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
            (!matches.is_empty()).then_some(matches)
        })
        .flatten()
        .collect::<Vec<_>>();

    assert!(
        offenders.is_empty(),
        "test code must not mutate the process environment; use injected test config or child-process Command::env instead:\n{}",
        offenders.join("\n")
    );
}
