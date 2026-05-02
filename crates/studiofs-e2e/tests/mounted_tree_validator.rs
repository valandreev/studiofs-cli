use std::fs;
use std::thread;
use std::time::Duration;

use studiofs_e2e::mounted_tree::{
    ClientMount, TreeComparison, TreeEntryKind, compare_snapshots, snapshot_client_tree,
    wait_for_convergence, write_divergence_artifacts,
};
use tempfile::tempdir;

#[test]
fn snapshot_records_sorted_paths_file_metadata_and_sha256() {
    let temp = tempdir().unwrap();
    let mount = temp.path().join("client-a");
    fs::create_dir_all(mount.join("dir")).unwrap();
    fs::write(mount.join("dir").join("a.txt"), b"hello").unwrap();
    fs::write(mount.join("z.txt"), b"world").unwrap();

    let snapshot = snapshot_client_tree("client-a", &mount).unwrap();

    let paths: Vec<_> = snapshot
        .entries
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    assert_eq!(paths, ["dir", "dir/a.txt", "z.txt"]);

    let file = snapshot
        .entries
        .iter()
        .find(|entry| entry.path == "dir/a.txt")
        .unwrap();
    assert_eq!(file.entry_type, TreeEntryKind::File);
    assert_eq!(file.size, Some(5));
    assert_eq!(
        file.checksum_sha256.as_deref(),
        Some("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
    );

    let encoded = serde_json::to_string_pretty(&snapshot).unwrap();
    assert!(encoded.contains("\"client\": \"client-a\""));
    assert!(encoded.contains("\"checksum_sha256\""));
}

#[test]
fn comparison_requires_file_content_checksum_not_only_size() {
    let temp = tempdir().unwrap();
    let client_a = temp.path().join("client-a");
    let client_b = temp.path().join("client-b");
    fs::create_dir_all(&client_a).unwrap();
    fs::create_dir_all(&client_b).unwrap();
    fs::write(client_a.join("same-size.bin"), b"abc").unwrap();
    fs::write(client_b.join("same-size.bin"), b"xyz").unwrap();

    let snapshots = vec![
        snapshot_client_tree("client-a", &client_a).unwrap(),
        snapshot_client_tree("client-b", &client_b).unwrap(),
    ];
    let comparison = compare_snapshots(&snapshots);

    assert!(!comparison.converged);
    assert!(
        comparison
            .differences
            .iter()
            .any(|difference| difference.path == "same-size.bin"
                && difference.message.contains("checksum differs"))
    );
}

#[test]
fn divergence_artifacts_include_each_client_snapshot_and_readable_diff() {
    let temp = tempdir().unwrap();
    let client_a = temp.path().join("client-a");
    let client_b = temp.path().join("client-b");
    let client_c = temp.path().join("client-c");
    fs::create_dir_all(&client_a).unwrap();
    fs::create_dir_all(&client_b).unwrap();
    fs::create_dir_all(&client_c).unwrap();
    fs::write(client_a.join("shared.txt"), b"same").unwrap();
    fs::write(client_b.join("shared.txt"), b"same").unwrap();
    fs::write(client_c.join("shared.txt"), b"different").unwrap();

    let snapshots = vec![
        snapshot_client_tree("client-a", &client_a).unwrap(),
        snapshot_client_tree("client-b", &client_b).unwrap(),
        snapshot_client_tree("client-c", &client_c).unwrap(),
    ];
    let comparison = TreeComparison::from_snapshots(&snapshots);
    let artifact_dir = temp.path().join("artifacts");

    let artifacts = write_divergence_artifacts(&artifact_dir, &snapshots, &comparison).unwrap();

    assert_eq!(
        artifacts,
        [
            "tree-client-1.json",
            "tree-client-2.json",
            "tree-client-3.json",
            "diff.json"
        ]
    );
    assert!(artifact_dir.join("tree-client-1.json").exists());
    assert!(artifact_dir.join("tree-client-2.json").exists());
    assert!(artifact_dir.join("tree-client-3.json").exists());
    let diff = fs::read_to_string(artifact_dir.join("diff.json")).unwrap();
    assert!(diff.contains("\"converged\": false"));
    assert!(diff.contains("client-c"));
    assert!(diff.contains("checksum differs"));
}

#[test]
fn wait_for_convergence_retries_until_trees_match_without_failure_artifacts() {
    let temp = tempdir().unwrap();
    let client_a = temp.path().join("client-a");
    let client_b = temp.path().join("client-b");
    fs::create_dir_all(&client_a).unwrap();
    fs::create_dir_all(&client_b).unwrap();
    fs::write(client_a.join("eventual.txt"), b"settled").unwrap();

    let delayed_client = client_b.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        fs::write(delayed_client.join("eventual.txt"), b"settled").unwrap();
    });

    let artifact_dir = temp.path().join("validation");
    let outcome = wait_for_convergence(
        &[
            ClientMount::new("client-a", client_a),
            ClientMount::new("client-b", client_b),
        ],
        &artifact_dir,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert!(outcome.comparison.converged);
    assert!(outcome.attempts > 1);
    assert!(!artifact_dir.join("diff.json").exists());
}
