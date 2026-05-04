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
