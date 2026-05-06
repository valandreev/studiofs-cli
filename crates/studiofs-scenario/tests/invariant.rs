#![allow(missing_docs)]
use studiofs_scenario::invariant::{
    ClientLogicalSnapshot, DurableWriteExpectation, LogicalEntry, LogicalEntryKind,
    LogicalSnapshot, ObjectLifecycle, ObjectRef, validate_logical_invariants,
};

#[test]
fn detects_divergence_lost_durable_write_resurrection_and_tree_errors() {
    let writer = ClientLogicalSnapshot {
        client: "client-a".to_string(),
        entries: vec![
            LogicalEntry::directory("dir", "inode-dir"),
            LogicalEntry::file(
                "dir/durable.txt",
                "inode-file",
                Some(ObjectRef::remote("object-a")),
                Some("checksum-a".to_string()),
                Some(12),
            ),
            LogicalEntry::file("dup.txt", "inode-dup-a", None, None, None),
            LogicalEntry::file("dup.txt", "inode-dup-b", None, None, None),
            LogicalEntry::file("deleted.txt", "inode-deleted", None, None, None),
        ],
    };
    let peer = ClientLogicalSnapshot {
        client: "client-b".to_string(),
        entries: vec![LogicalEntry::directory("dir", "inode-dir")],
    };
    let snapshot = LogicalSnapshot {
        schema_version: 1,
        clients: vec![writer, peer],
        expected_absent_paths: vec!["deleted.txt".to_string()],
        durable_writes: vec![DurableWriteExpectation {
            client: "client-a".to_string(),
            path: "dir/durable.txt".to_string(),
            checksum_sha256: Some("checksum-a".to_string()),
            size: Some(13),
        }],
        objects: vec![ObjectLifecycle {
            object_id: "object-a".to_string(),
            present_remote: false,
            protected: false,
        }],
        allow_hard_links: false,
        replay_idempotent: Some(false),
    };

    let report = validate_logical_invariants(&snapshot);

    assert!(!report.passed);
    let invariants = report
        .violations
        .iter()
        .map(|violation| violation.invariant.as_str())
        .collect::<Vec<_>>();
    assert!(invariants.contains(&"eventual_convergence"));
    assert!(invariants.contains(&"no_lost_locally_durable_writes"));
    assert!(invariants.contains(&"no_unexpected_resurrection"));
    assert!(invariants.contains(&"canonical_path_ownership"));
    assert!(invariants.contains(&"valid_object_reachability"));
    assert!(invariants.contains(&"idempotent_replication_replay"));
}

#[test]
fn allows_explicit_hard_link_model_to_share_an_inode_across_paths() {
    let snapshot = LogicalSnapshot {
        schema_version: 1,
        clients: vec![ClientLogicalSnapshot {
            client: "client-a".to_string(),
            entries: vec![
                LogicalEntry::file("a.txt", "shared-inode", None, None, None),
                LogicalEntry::file("b.txt", "shared-inode", None, None, None),
            ],
        }],
        expected_absent_paths: Vec::new(),
        durable_writes: Vec::new(),
        objects: Vec::new(),
        allow_hard_links: true,
        replay_idempotent: None,
    };

    let report = validate_logical_invariants(&snapshot);

    assert!(report.passed);
}

#[test]
fn convergence_detects_logical_id_divergence_for_simulation_snapshots() {
    let snapshot = LogicalSnapshot {
        schema_version: 1,
        clients: vec![
            ClientLogicalSnapshot {
                client: "client-a".to_string(),
                entries: vec![LogicalEntry::file(
                    "same.txt",
                    "inode-a",
                    None,
                    Some("checksum-a".to_string()),
                    Some(5),
                )],
            },
            ClientLogicalSnapshot {
                client: "client-b".to_string(),
                entries: vec![LogicalEntry::file(
                    "same.txt",
                    "inode-b",
                    None,
                    Some("checksum-a".to_string()),
                    Some(5),
                )],
            },
        ],
        expected_absent_paths: Vec::new(),
        durable_writes: Vec::new(),
        objects: Vec::new(),
        allow_hard_links: false,
        replay_idempotent: None,
    };

    let report = validate_logical_invariants(&snapshot);

    assert!(!report.passed);
    assert!(
        report
            .violations
            .iter()
            .any(|violation| violation.invariant == "eventual_convergence"
                && violation.path.as_deref() == Some("same.txt"))
    );
}

#[test]
fn convergence_detects_symlink_target_divergence() {
    let snapshot = LogicalSnapshot {
        schema_version: 1,
        clients: vec![
            ClientLogicalSnapshot {
                client: "client-a".to_string(),
                entries: vec![LogicalEntry {
                    path: "link".to_string(),
                    kind: LogicalEntryKind::Symlink,
                    logical_id: None,
                    object: None,
                    checksum_sha256: None,
                    size: None,
                    symlink_target: Some("target-a".to_string()),
                }],
            },
            ClientLogicalSnapshot {
                client: "client-b".to_string(),
                entries: vec![LogicalEntry {
                    path: "link".to_string(),
                    kind: LogicalEntryKind::Symlink,
                    logical_id: None,
                    object: None,
                    checksum_sha256: None,
                    size: None,
                    symlink_target: Some("target-b".to_string()),
                }],
            },
        ],
        expected_absent_paths: Vec::new(),
        durable_writes: Vec::new(),
        objects: Vec::new(),
        allow_hard_links: false,
        replay_idempotent: None,
    };

    let report = validate_logical_invariants(&snapshot);

    assert!(!report.passed);
    assert!(
        report
            .violations
            .iter()
            .any(|violation| violation.invariant == "eventual_convergence"
                && violation.path.as_deref() == Some("link"))
    );
}

#[test]
fn logical_snapshot_serializes_stable_schema_for_simulation_artifacts() {
    let snapshot = LogicalSnapshot {
        schema_version: 1,
        clients: vec![ClientLogicalSnapshot {
            client: "client-a".to_string(),
            entries: vec![LogicalEntry {
                path: "dir/file.txt".to_string(),
                kind: LogicalEntryKind::File,
                logical_id: Some("inode-1".to_string()),
                object: Some(ObjectRef::local("pending-object")),
                checksum_sha256: Some("checksum-a".to_string()),
                size: Some(5),
                symlink_target: None,
            }],
        }],
        expected_absent_paths: Vec::new(),
        durable_writes: Vec::new(),
        objects: Vec::new(),
        allow_hard_links: false,
        replay_idempotent: None,
    };

    let encoded = serde_json::to_string_pretty(&snapshot).unwrap();

    assert!(encoded.contains("\"schema_version\": 1"));
    assert!(encoded.contains("\"logical_id\": \"inode-1\""));
    assert!(encoded.contains("\"location\": \"local\""));
}
