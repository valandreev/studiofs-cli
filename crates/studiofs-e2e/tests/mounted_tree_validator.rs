//! Crate documentation for lint completion.
use std::fs;
use std::thread;
use std::time::Duration;

use studiofs_e2e::mounted_tree::{
    ClientMount, MountedTreeError, MountedTreeSnapshot, TreeEntry, TreeEntryKind,
    compare_snapshots, mounted_snapshots_to_logical_snapshot, snapshot_client_tree,
    validate_namespace_invariants, wait_for_convergence, write_divergence_artifacts,
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
fn namespace_invariants_detect_duplicate_paths_missing_parents_and_resurrection() {
    let snapshot = MountedTreeSnapshot {
        schema_version: 1,
        client: "client-a".to_string(),
        mount_point: "/mnt/client-a".to_string(),
        entries: vec![
            TreeEntry {
                path: "dir/file.txt".to_string(),
                entry_type: TreeEntryKind::File,
                size: Some(5),
                modified_unix_ms: None,
                checksum_sha256: Some("checksum-a".to_string()),
                symlink_target: None,
            },
            TreeEntry {
                path: "dup.txt".to_string(),
                entry_type: TreeEntryKind::File,
                size: Some(1),
                modified_unix_ms: None,
                checksum_sha256: Some("checksum-b".to_string()),
                symlink_target: None,
            },
            TreeEntry {
                path: "dup.txt".to_string(),
                entry_type: TreeEntryKind::File,
                size: Some(1),
                modified_unix_ms: None,
                checksum_sha256: Some("checksum-b".to_string()),
                symlink_target: None,
            },
            TreeEntry {
                path: "deleted.txt".to_string(),
                entry_type: TreeEntryKind::File,
                size: Some(7),
                modified_unix_ms: None,
                checksum_sha256: Some("checksum-c".to_string()),
                symlink_target: None,
            },
        ],
    };

    let violations = validate_namespace_invariants(&snapshot, &["deleted.txt"]);
    let messages = violations
        .iter()
        .map(|violation| violation.message.as_str())
        .collect::<Vec<_>>();

    assert!(messages.contains(&"duplicate canonical path"));
    assert!(messages.contains(&"missing parent directory"));
    assert!(messages.contains(&"unexpected resurrection"));
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
    let comparison = compare_snapshots(&snapshots);
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

#[test]
fn wait_for_convergence_treats_transient_snapshot_io_errors_as_not_yet_converged() {
    let temp = tempdir().unwrap();
    let client_a = temp.path().join("client-a");
    let client_b = temp.path().join("client-b");
    fs::create_dir_all(&client_a).unwrap();
    fs::write(client_a.join("eventual.txt"), b"settled").unwrap();

    let delayed_client = client_b.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        fs::create_dir_all(&delayed_client).unwrap();
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

#[test]
fn wait_for_convergence_reports_timeout_when_snapshots_never_succeed() {
    let temp = tempdir().unwrap();
    let missing_client = temp.path().join("missing-client");

    let err = wait_for_convergence(
        &[ClientMount::new("client-a", missing_client)],
        temp.path().join("validation"),
        Duration::from_millis(30),
        Duration::from_millis(5),
    )
    .unwrap_err();

    match err {
        MountedTreeError::SnapshotUnavailableTimedOut {
            attempts, source, ..
        } => {
            assert!(attempts > 1);
            assert!(matches!(*source, MountedTreeError::Io { .. }));
        }
        other => panic!("expected snapshot-unavailable timeout, got {other:?}"),
    }
}

#[test]
fn comparison_detects_symlink_target_divergence() {
    let snapshots = vec![
        symlink_snapshot("client-a", "target-a"),
        symlink_snapshot("client-b", "target-b"),
    ];

    let comparison = compare_snapshots(&snapshots);

    assert!(!comparison.converged);
    assert!(
        comparison
            .differences
            .iter()
            .any(|difference| difference.path == "link"
                && difference.message.contains("symlink target differs"))
    );
}

#[test]
fn mounted_snapshots_map_to_shared_logical_validator_contract() {
    let snapshots = vec![
        MountedTreeSnapshot {
            schema_version: 1,
            client: "client-a".to_string(),
            mount_point: "/mnt/client-a".to_string(),
            entries: vec![TreeEntry {
                path: "dir/file.txt".to_string(),
                entry_type: TreeEntryKind::File,
                size: Some(5),
                modified_unix_ms: Some(123),
                checksum_sha256: Some("checksum-a".to_string()),
                symlink_target: None,
            }],
        },
        MountedTreeSnapshot {
            schema_version: 1,
            client: "client-b".to_string(),
            mount_point: "/mnt/client-b".to_string(),
            entries: vec![TreeEntry {
                path: "dir/file.txt".to_string(),
                entry_type: TreeEntryKind::File,
                size: Some(7),
                modified_unix_ms: Some(456),
                checksum_sha256: Some("checksum-b".to_string()),
                symlink_target: None,
            }],
        },
    ];

    let logical = mounted_snapshots_to_logical_snapshot(&snapshots, &[]);
    let report = studiofs_scenario::invariant::validate_logical_invariants(&logical);

    assert!(!report.passed);
    assert_eq!(logical.clients[0].client, "client-a");
    assert_eq!(logical.clients[0].entries[0].path, "dir/file.txt");
    assert_eq!(logical.clients[0].entries[0].logical_id.as_deref(), None);
    assert_eq!(logical.clients[0].entries[0].object, None);
    assert!(logical.clients[0].entries[0].byte_ranges.is_empty());
    assert_eq!(logical.clients[0].entries[0].symlink_target, None);
    assert!(
        report
            .violations
            .iter()
            .any(|violation| violation.invariant == "eventual_convergence"
                && violation.path.as_deref() == Some("dir/file.txt"))
    );
}

#[test]
fn mounted_logical_mapping_does_not_require_object_lifecycle_facts() {
    let snapshots = vec![
        mounted_file_snapshot("client-a", "shared.txt", 4, "checksum-a"),
        mounted_file_snapshot("client-b", "shared.txt", 4, "checksum-a"),
    ];

    let logical = mounted_snapshots_to_logical_snapshot(&snapshots, &[]);
    let report = studiofs_scenario::invariant::validate_logical_invariants(&logical);

    assert!(report.passed, "{:?}", report.violations);
    assert!(logical.objects.is_empty());
    assert!(logical.clients[0].entries[0].object.is_none());
}

#[cfg(unix)]
#[test]
fn snapshot_records_symlink_target() {
    use std::os::unix::fs::symlink;

    let temp = tempdir().unwrap();
    let client_a = temp.path().join("client-a");
    fs::create_dir_all(&client_a).unwrap();
    symlink("target-a", client_a.join("link")).unwrap();

    let snapshot = snapshot_client_tree("client-a", &client_a).unwrap();

    let link = snapshot
        .entries
        .iter()
        .find(|entry| entry.path == "link")
        .unwrap();
    assert_eq!(link.entry_type, TreeEntryKind::Symlink);
    assert_eq!(link.symlink_target.as_deref(), Some("target-a"));
}

#[cfg(all(unix, not(target_os = "macos")))]
#[test]
fn snapshot_rejects_non_utf8_relative_paths_instead_of_lossy_encoding() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt as _;

    let temp = tempdir().unwrap();
    let mount = temp.path().join("client-a");
    fs::create_dir_all(&mount).unwrap();
    fs::write(
        mount.join(OsString::from_vec(vec![b'f', 0x80, b'e'])),
        b"data",
    )
    .unwrap();

    let err = snapshot_client_tree("client-a", &mount).unwrap_err();

    assert!(matches!(err, MountedTreeError::NonUtf8Path { .. }));
}

fn symlink_snapshot(client: &str, target: &str) -> MountedTreeSnapshot {
    MountedTreeSnapshot {
        schema_version: 1,
        client: client.to_string(),
        mount_point: format!("/mnt/{client}"),
        entries: vec![TreeEntry {
            path: "link".to_string(),
            entry_type: TreeEntryKind::Symlink,
            size: None,
            modified_unix_ms: None,
            checksum_sha256: None,
            symlink_target: Some(target.to_string()),
        }],
    }
}

fn mounted_file_snapshot(
    client: &str,
    path: &str,
    size: u64,
    checksum_sha256: &str,
) -> MountedTreeSnapshot {
    MountedTreeSnapshot {
        schema_version: 1,
        client: client.to_string(),
        mount_point: format!("/mnt/{client}"),
        entries: vec![TreeEntry {
            path: path.to_string(),
            entry_type: TreeEntryKind::File,
            size: Some(size),
            modified_unix_ms: None,
            checksum_sha256: Some(checksum_sha256.to_string()),
            symlink_target: None,
        }],
    }
}
