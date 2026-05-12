//! Crate documentation for lint completion.
use studiofs_scenario::client::SimulatedRuntime;
use studiofs_scenario::invariant::validate_logical_invariants;

#[test]
fn two_clients_converge_after_upload_publish_and_foreign_apply() {
    let mut runtime = SimulatedRuntime::new(42);
    runtime.add_client("writer");
    runtime.add_client("reader");

    runtime.create_file("writer", "dir/file.txt").unwrap();
    runtime
        .write_file("writer", "dir/file.txt", 11, "checksum-v1")
        .unwrap();
    runtime.flush("writer", "dir/file.txt").unwrap();

    assert_eq!(
        runtime
            .read_file("writer", "dir/file.txt")
            .unwrap()
            .checksum_sha256,
        "checksum-v1"
    );
    assert!(runtime.read_file("reader", "dir/file.txt").is_err());

    let upload = runtime.schedule_upload("writer", "dir/file.txt").unwrap();
    runtime.complete_upload(upload.upload_id).unwrap();

    assert!(runtime.read_file("reader", "dir/file.txt").is_err());

    runtime.publish_file("writer", "dir/file.txt").unwrap();
    runtime.deliver_next_metadata("reader").unwrap();

    assert_eq!(
        runtime
            .read_file("reader", "dir/file.txt")
            .unwrap()
            .checksum_sha256,
        "checksum-v1"
    );

    let snapshot = runtime.logical_snapshot();
    let report = validate_logical_invariants(&snapshot);
    assert!(report.passed, "{:#?}", report.violations);
    assert!(
        runtime
            .trace()
            .iter()
            .any(|record| record.client.as_deref() == Some("writer")
                && record.kind == "write_file")
    );
}

#[test]
fn metadata_outage_and_restart_preserve_local_durable_write_until_convergence() {
    let mut runtime = SimulatedRuntime::new(7);
    runtime.add_client("writer");
    runtime.add_client("reader");

    runtime.set_metadata_available(false);
    runtime.create_file("writer", "outage.txt").unwrap();
    runtime
        .write_file("writer", "outage.txt", 5, "checksum-outage")
        .unwrap();
    runtime.flush("writer", "outage.txt").unwrap();

    let upload = runtime.schedule_upload("writer", "outage.txt").unwrap();
    runtime.complete_upload(upload.upload_id).unwrap();
    runtime.publish_file("writer", "outage.txt").unwrap();
    runtime.restart_client("writer").unwrap();

    assert_eq!(
        runtime
            .read_file("writer", "outage.txt")
            .unwrap()
            .checksum_sha256,
        "checksum-outage"
    );
    assert!(runtime.deliver_next_metadata("reader").is_err());

    runtime.set_metadata_available(true);
    runtime.deliver_next_metadata("reader").unwrap();

    assert_eq!(
        runtime
            .read_file("reader", "outage.txt")
            .unwrap()
            .checksum_sha256,
        "checksum-outage"
    );
    assert!(
        runtime
            .trace()
            .iter()
            .any(|record| record.client.as_deref() == Some("writer")
                && record.kind == "client_restarted")
    );
}

#[test]
fn client_core_namespace_operations_update_isolated_local_state() {
    let mut runtime = SimulatedRuntime::new(11);
    runtime.add_client("client-a");
    runtime.add_client("client-b");

    runtime.mkdir("client-a", "dir").unwrap();
    runtime.create_file("client-a", "dir/file.txt").unwrap();
    runtime
        .write_file("client-a", "dir/file.txt", 4, "checksum-a")
        .unwrap();
    runtime
        .append_file("client-a", "dir/file.txt", 3, "checksum-b")
        .unwrap();
    runtime
        .truncate_file("client-a", "dir/file.txt", 5, "checksum-c")
        .unwrap();
    runtime
        .rename("client-a", "dir/file.txt", "dir/renamed.txt")
        .unwrap();

    let stat = runtime.stat("client-a", "dir/renamed.txt").unwrap();
    assert_eq!(stat.size, Some(5));
    assert_eq!(stat.checksum_sha256.as_deref(), Some("checksum-c"));
    assert_eq!(
        runtime.readdir("client-a", "dir").unwrap(),
        vec!["renamed.txt"]
    );
    assert!(runtime.stat("client-b", "dir/renamed.txt").is_err());

    runtime.unlink("client-a", "dir/renamed.txt").unwrap();
    assert!(runtime.readdir("client-a", "dir").unwrap().is_empty());
    runtime.rmdir("client-a", "dir").unwrap();
    assert!(runtime.stat("client-a", "dir").is_err());
}

#[test]
fn restart_replays_only_durable_local_state() {
    let mut runtime = SimulatedRuntime::new(13);
    runtime.add_client("writer");

    runtime.create_file("writer", "file.txt").unwrap();
    runtime
        .write_file("writer", "file.txt", 4, "checksum-v1")
        .unwrap();
    runtime.flush("writer", "file.txt").unwrap();
    runtime
        .write_file("writer", "file.txt", 9, "checksum-dirty")
        .unwrap();

    runtime.restart_client("writer").unwrap();

    let file = runtime.read_file("writer", "file.txt").unwrap();
    assert_eq!(file.size, 4);
    assert_eq!(file.checksum_sha256, "checksum-v1");
}

#[test]
fn stale_foreign_publish_for_same_logical_file_does_not_override_newer_epoch() {
    let mut runtime = SimulatedRuntime::new(37);
    runtime.add_client("writer");
    runtime.add_client("reader");

    runtime.create_file("writer", "file.txt").unwrap();
    runtime
        .write_file("writer", "file.txt", 4, "checksum-v1")
        .unwrap();
    runtime.flush("writer", "file.txt").unwrap();
    let stale_publish = runtime
        .published_file_version("writer", "file.txt")
        .unwrap();

    runtime
        .write_file("writer", "file.txt", 7, "checksum-v2")
        .unwrap();
    runtime.flush("writer", "file.txt").unwrap();
    let newer_upload = runtime.schedule_upload("writer", "file.txt").unwrap();
    runtime.complete_upload(newer_upload.upload_id).unwrap();
    runtime.publish_file("writer", "file.txt").unwrap();
    runtime.deliver_next_metadata("reader").unwrap();
    assert_eq!(
        runtime
            .read_file("reader", "file.txt")
            .unwrap()
            .checksum_sha256,
        "checksum-v2"
    );

    runtime
        .enqueue_published_file_version("writer", "file.txt", stale_publish)
        .unwrap();
    runtime.deliver_next_metadata("reader").unwrap();

    let file = runtime.read_file("reader", "file.txt").unwrap();
    assert_eq!(file.size, 7);
    assert_eq!(file.checksum_sha256, "checksum-v2");
}

#[test]
fn recreated_file_gets_a_new_logical_id_after_rename() {
    let mut runtime = SimulatedRuntime::new(17);
    runtime.add_client("client-a");

    runtime.create_file("client-a", "a.txt").unwrap();
    runtime.rename("client-a", "a.txt", "b.txt").unwrap();
    runtime.create_file("client-a", "a.txt").unwrap();

    let snapshot = runtime.logical_snapshot();
    let client = snapshot
        .clients
        .iter()
        .find(|client| client.client == "client-a")
        .unwrap();
    let a_id = client
        .entries
        .iter()
        .find(|entry| entry.path == "a.txt")
        .and_then(|entry| entry.logical_id.as_deref())
        .unwrap();
    let b_id = client
        .entries
        .iter()
        .find(|entry| entry.path == "b.txt")
        .and_then(|entry| entry.logical_id.as_deref())
        .unwrap();

    assert_ne!(a_id, b_id);
}

#[test]
fn root_directory_supports_stat_and_readdir() {
    let mut runtime = SimulatedRuntime::new(19);
    runtime.add_client("client-a");

    runtime.mkdir("client-a", "dir").unwrap();
    runtime.create_file("client-a", "top.txt").unwrap();

    assert!(runtime.stat("client-a", "").is_ok());
    assert_eq!(
        runtime.readdir("client-a", "").unwrap(),
        vec!["dir", "top.txt"]
    );
}

#[test]
fn file_cannot_be_used_as_a_parent_directory() {
    let mut runtime = SimulatedRuntime::new(23);
    runtime.add_client("client-a");

    runtime.create_file("client-a", "file").unwrap();

    assert!(runtime.create_file("client-a", "file/child.txt").is_err());
}

#[test]
fn directory_rename_moves_descendants_and_rejects_root_or_traversal_paths() {
    let mut runtime = SimulatedRuntime::new(29);
    runtime.add_client("client-a");

    runtime.mkdir("client-a", "dir").unwrap();
    runtime.create_file("client-a", "dir/file.txt").unwrap();
    runtime.rename("client-a", "dir", "newdir").unwrap();

    assert!(runtime.stat("client-a", "dir/file.txt").is_err());
    assert!(runtime.stat("client-a", "newdir/file.txt").is_ok());
    assert_eq!(
        runtime.readdir("client-a", "newdir").unwrap(),
        vec!["file.txt"]
    );

    assert!(runtime.rename("client-a", "", "root").is_err());
    assert!(runtime.rename("client-a", "newdir", "../escape").is_err());
    assert!(runtime.rename("client-a", "newdir", "/absolute").is_err());
    assert!(runtime.rename("client-a", "newdir", "bad\\path").is_err());
}

#[test]
fn rmdir_rejects_root_and_traversal_paths() {
    let mut runtime = SimulatedRuntime::new(31);
    runtime.add_client("client-a");

    assert!(runtime.rmdir("client-a", "").is_err());
    assert!(runtime.rmdir("client-a", "../escape").is_err());
    assert!(runtime.rmdir("client-a", "/absolute").is_err());
    assert!(runtime.rmdir("client-a", "bad\\path").is_err());
}

#[test]
fn create_and_mkdir_reject_root_absolute_traversal_and_windows_paths() {
    let mut runtime = SimulatedRuntime::new(33);
    runtime.add_client("client-a");

    for invalid_path in [
        "",
        "../escape",
        "dir/../escape",
        "./relative",
        "dir//file",
        "/absolute",
        "bad\\path",
    ] {
        assert!(runtime.create_file("client-a", invalid_path).is_err());
        assert!(runtime.mkdir("client-a", invalid_path).is_err());
    }
}

#[test]
fn directory_rename_rejects_self_descendant_target() {
    let mut runtime = SimulatedRuntime::new(35);
    runtime.add_client("client-a");

    runtime.mkdir("client-a", "dir").unwrap();
    runtime.create_file("client-a", "dir/file.txt").unwrap();

    assert!(runtime.rename("client-a", "dir", "dir/subdir").is_err());
    assert!(runtime.stat("client-a", "dir/file.txt").is_ok());
}

#[test]
fn durable_write_expectations_follow_renamed_paths() {
    let mut runtime = SimulatedRuntime::new(39);
    runtime.add_client("client-a");

    runtime.create_file("client-a", "old.txt").unwrap();
    runtime
        .write_file("client-a", "old.txt", 5, "checksum-v1")
        .unwrap();
    runtime.flush("client-a", "old.txt").unwrap();
    runtime.rename("client-a", "old.txt", "new.txt").unwrap();

    let report = validate_logical_invariants(&runtime.logical_snapshot());
    assert!(report.passed, "{:#?}", report.violations);
}

#[test]
fn namespace_mutations_are_replayed_after_restart() {
    let mut runtime = SimulatedRuntime::new(41);
    runtime.add_client("client-a");

    runtime.mkdir("client-a", "dir").unwrap();
    runtime.restart_client("client-a").unwrap();
    assert!(runtime.stat("client-a", "dir").is_ok());

    runtime.create_file("client-a", "dir/file.txt").unwrap();
    runtime
        .write_file("client-a", "dir/file.txt", 4, "checksum-v1")
        .unwrap();
    runtime.flush("client-a", "dir/file.txt").unwrap();
    runtime.unlink("client-a", "dir/file.txt").unwrap();
    runtime.restart_client("client-a").unwrap();
    assert!(runtime.stat("client-a", "dir/file.txt").is_err());

    runtime.rmdir("client-a", "dir").unwrap();
    runtime.restart_client("client-a").unwrap();
    assert!(runtime.stat("client-a", "dir").is_err());
}

#[test]
fn metadata_history_replay_drains_only_the_current_queue_snapshot() {
    let mut runtime = SimulatedRuntime::new(45);
    runtime.add_client("writer");
    runtime.add_client("reader");

    runtime.create_file("writer", "file.txt").unwrap();
    runtime
        .write_file("writer", "file.txt", 4, "checksum-v1")
        .unwrap();
    runtime.flush("writer", "file.txt").unwrap();
    let upload = runtime.schedule_upload("writer", "file.txt").unwrap();
    runtime.complete_upload(upload.upload_id).unwrap();
    runtime.publish_file("writer", "file.txt").unwrap();

    assert_eq!(runtime.deliver_all_metadata("reader").unwrap(), 1);
    assert_eq!(runtime.import_metadata_history("reader").unwrap(), 0);
    assert_eq!(runtime.replay_metadata_history("reader").unwrap(), 1);
    assert_eq!(runtime.deliver_all_metadata("reader").unwrap(), 1);
    assert_eq!(runtime.deliver_all_metadata("reader").unwrap(), 0);

    let file = runtime.read_file("reader", "file.txt").unwrap();
    assert_eq!(file.checksum_sha256, "checksum-v1");
}

#[test]
fn directory_rename_preserves_logical_id_after_restart() {
    let mut runtime = SimulatedRuntime::new(43);
    runtime.add_client("client-a");

    runtime.mkdir("client-a", "dir").unwrap();
    let before = directory_logical_id(&runtime, "dir");

    runtime.rename("client-a", "dir", "renamed").unwrap();
    runtime.restart_client("client-a").unwrap();

    let after = directory_logical_id(&runtime, "renamed");
    assert_eq!(before, after);
}

#[test]
fn symlink_and_metadata_attributes_converge_through_metadata_delivery() {
    let mut runtime = SimulatedRuntime::new(47);
    runtime.add_client("writer");
    runtime.add_client("reader");

    runtime.mkdir("writer", "aux").unwrap();
    runtime
        .create_symlink("writer", "aux/current", "target-a.txt")
        .unwrap();
    runtime
        .set_metadata_attribute(
            "writer",
            "aux/current",
            "user.studiofs.label",
            "published-link",
        )
        .unwrap();
    runtime.publish_symlink("writer", "aux/current").unwrap();
    runtime
        .publish_metadata_attribute("writer", "aux/current", "user.studiofs.label")
        .unwrap();
    runtime.deliver_all_metadata("reader").unwrap();

    assert_eq!(
        runtime.read_symlink("reader", "aux/current").unwrap(),
        "target-a.txt"
    );
    assert_eq!(
        runtime
            .read_metadata_attribute("reader", "aux/current", "user.studiofs.label")
            .unwrap(),
        Some("published-link".to_string())
    );

    runtime
        .rename("writer", "aux/current", "aux/renamed")
        .unwrap();
    runtime
        .set_symlink_target("writer", "aux/renamed", "target-b.txt")
        .unwrap();
    runtime
        .set_metadata_attribute(
            "writer",
            "aux/renamed",
            "user.studiofs.label",
            "renamed-link",
        )
        .unwrap();
    runtime.publish_symlink("writer", "aux/renamed").unwrap();
    runtime
        .publish_metadata_attribute("writer", "aux/renamed", "user.studiofs.label")
        .unwrap();
    runtime.deliver_all_metadata("reader").unwrap();

    assert!(runtime.read_symlink("reader", "aux/current").is_err());
    assert_eq!(
        runtime.read_symlink("reader", "aux/renamed").unwrap(),
        "target-b.txt"
    );
    assert_eq!(
        runtime
            .read_metadata_attribute("reader", "aux/renamed", "user.studiofs.label")
            .unwrap(),
        Some("renamed-link".to_string())
    );

    let report = validate_logical_invariants(&runtime.logical_snapshot());
    assert!(report.passed, "{:#?}", report.violations);
}

fn directory_logical_id(runtime: &SimulatedRuntime, path: &str) -> String {
    runtime
        .logical_snapshot()
        .clients
        .iter()
        .flat_map(|client| &client.entries)
        .find(|entry| entry.path == path)
        .and_then(|entry| entry.logical_id.clone())
        .expect("directory logical id exists")
}
