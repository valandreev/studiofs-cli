//! SFS-1002 drift gate: every pinned persisted-format fixture must match the
//! SHA-256 recorded in `tests/fixtures/baseline-0.2.5/manifest.json`, and every
//! file in the pinned fixture directories must be listed there.

use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const MANIFEST: &str = "tests/fixtures/baseline-0.2.5/manifest.json";
const PINNED_DIRS: [&str; 3] = [
    "crates/studiofs-meta/tests/fixtures/baseline-0.2.5",
    "crates/studiofs-meta/tests/fixtures/published-v0.2.2",
    "crates/studiofs-meta/tests/fixtures/published-v0.2.4",
];
const REQUIRED_FIELDS: [&str; 6] = [
    "path",
    "sha256",
    "source",
    "generator",
    "identity",
    "expected",
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn manifest() -> serde_json::Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(MANIFEST);
    serde_json::from_slice(&std::fs::read(&path).expect("read manifest")).expect("parse manifest")
}

fn files_under(dir: &Path, root: &Path, out: &mut BTreeSet<String>) {
    for entry in std::fs::read_dir(dir).expect("read fixture dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            files_under(&path, root, out);
        } else {
            let relative = path.strip_prefix(root).expect("under workspace root");
            out.insert(relative.to_string_lossy().replace('\\', "/"));
        }
    }
}

#[test]
fn pinned_fixtures_match_manifest_checksums() {
    let root = workspace_root();
    let manifest = manifest();
    let fixtures = manifest["fixtures"].as_array().expect("fixtures array");
    assert!(!fixtures.is_empty());
    let mut drift = Vec::new();
    for fixture in fixtures {
        for field in REQUIRED_FIELDS {
            assert!(
                fixture[field]
                    .as_str()
                    .is_some_and(|value| !value.is_empty()),
                "manifest entry {fixture} lacks {field}"
            );
        }
        let path = fixture["path"].as_str().unwrap();
        let bytes = std::fs::read(root.join(path))
            .unwrap_or_else(|err| panic!("read pinned fixture {path}: {err}"));
        let actual = format!("{:x}", Sha256::digest(&bytes));
        if actual != fixture["sha256"].as_str().unwrap() {
            drift.push(format!(
                "{path}: manifest {} actual {actual}",
                fixture["sha256"]
            ));
        }
    }
    assert!(
        drift.is_empty(),
        "pinned fixtures drifted; regenerate only with the baseline encoder and record the reason in the manifest:\n{}",
        drift.join("\n")
    );
}

#[test]
fn pinned_fixture_directories_are_fully_listed() {
    let root = workspace_root();
    let listed: BTreeSet<String> = manifest()["fixtures"]
        .as_array()
        .expect("fixtures array")
        .iter()
        .map(|fixture| fixture["path"].as_str().expect("path").to_string())
        .collect();
    let mut present = BTreeSet::new();
    for dir in PINNED_DIRS {
        files_under(&root.join(dir), &root, &mut present);
    }
    present.remove(&format!("crates/studiofs-meta/{MANIFEST}"));
    let unlisted: Vec<_> = present.difference(&listed).collect();
    assert!(
        unlisted.is_empty(),
        "fixtures missing from manifest: {unlisted:?}"
    );
}
