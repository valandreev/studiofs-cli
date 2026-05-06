#![allow(missing_docs)]
use studiofs_scenario::invariant::ObjectLifecycle;
use studiofs_scenario::object_store::{
    ObjectStoreFault, ObjectStoreTraceKind, SimulatedObjectStore, UploadOutcome,
};

#[test]
fn upload_completion_makes_object_remote_without_publishing_metadata() {
    let mut store = SimulatedObjectStore::new();
    store.stage_local_payload("writer", "object-a", "checksum-a", 12);

    assert!(store.has_local_payload("writer", "object-a"));
    assert!(!store.has_remote_object("object-a"));
    assert!(!store.is_published("object-a"));

    let ticket = store.schedule_upload("writer", "object-a").unwrap();
    assert_eq!(
        store.complete_upload(ticket.upload_id),
        Some(UploadOutcome::Completed {
            upload_id: ticket.upload_id,
            object_id: "object-a".to_string()
        })
    );

    assert!(store.has_remote_object("object-a"));
    assert!(!store.is_published("object-a"));
    assert_eq!(
        store.read_remote("object-a").unwrap().checksum_sha256,
        "checksum-a"
    );

    store.mark_published("object-a");
    assert!(store.is_published("object-a"));
}

#[test]
fn completed_upload_is_terminal_and_idempotent() {
    let mut store = SimulatedObjectStore::new();
    store.stage_local_payload("writer", "object-a", "checksum-a", 12);
    let ticket = store.schedule_upload("writer", "object-a").unwrap();

    assert_eq!(
        store.complete_upload(ticket.upload_id),
        Some(UploadOutcome::Completed {
            upload_id: ticket.upload_id,
            object_id: "object-a".to_string()
        })
    );
    assert_eq!(
        store.complete_upload(ticket.upload_id),
        Some(UploadOutcome::AlreadyCompleted {
            upload_id: ticket.upload_id,
            object_id: "object-a".to_string()
        })
    );
}

#[test]
fn completed_upload_cannot_be_marked_failed() {
    let mut store = SimulatedObjectStore::new();
    store.stage_local_payload("writer", "object-a", "checksum-a", 12);
    let ticket = store.schedule_upload("writer", "object-a").unwrap();

    assert!(matches!(
        store.complete_upload(ticket.upload_id),
        Some(UploadOutcome::Completed { .. })
    ));
    assert_eq!(
        store.fail_upload(ticket.upload_id, "late failure", Some(10)),
        None
    );
    assert_eq!(
        store.complete_upload(ticket.upload_id),
        Some(UploadOutcome::AlreadyCompleted {
            upload_id: ticket.upload_id,
            object_id: "object-a".to_string()
        })
    );
}

#[test]
fn outage_blocks_upload_completion_and_remote_reads_until_recovery() {
    let mut store = SimulatedObjectStore::new();
    store.stage_local_payload("writer", "object-a", "checksum-a", 12);
    let ticket = store.schedule_upload("writer", "object-a").unwrap();

    store.set_available(false);

    assert_eq!(
        store.complete_upload(ticket.upload_id),
        Some(UploadOutcome::Blocked {
            upload_id: ticket.upload_id,
            object_id: "object-a".to_string(),
            reason: ObjectStoreFault::ObjectStoreOutage
        })
    );
    assert_eq!(
        store.read_remote("object-a").unwrap_err(),
        ObjectStoreFault::ObjectStoreOutage
    );
    assert!(!store.has_remote_object("object-a"));

    store.set_available(true);
    assert!(matches!(
        store.complete_upload(ticket.upload_id),
        Some(UploadOutcome::Completed { .. })
    ));
    assert_eq!(store.read_remote("object-a").unwrap().size, 12);
}

#[test]
fn upload_with_missing_local_payload_is_blocked_not_ambiguous() {
    let mut store = SimulatedObjectStore::new();
    store.stage_local_payload("writer", "object-a", "checksum-a", 12);
    let ticket = store.schedule_upload("writer", "object-a").unwrap();
    store.remove_local_payload("writer", "object-a");

    assert_eq!(store.complete_upload(999), None);
    assert_eq!(
        store.complete_upload(ticket.upload_id),
        Some(UploadOutcome::Blocked {
            upload_id: ticket.upload_id,
            object_id: "object-a".to_string(),
            reason: ObjectStoreFault::LocalPayloadMissing
        })
    );
    assert!(
        store
            .trace()
            .iter()
            .any(|record| record.kind == ObjectStoreTraceKind::UploadBlocked
                && record.fault == Some(ObjectStoreFault::LocalPayloadMissing))
    );
}

#[test]
fn lifecycle_includes_pending_upload_after_local_payload_is_removed() {
    let mut store = SimulatedObjectStore::new();
    store.stage_local_payload("writer", "object-a", "checksum-a", 12);
    store.schedule_upload("writer", "object-a").unwrap();
    store.remove_local_payload("writer", "object-a");

    assert_eq!(
        store.object_lifecycle(),
        vec![ObjectLifecycle {
            object_id: "object-a".to_string(),
            present_remote: false,
            protected: true,
        }]
    );
}

#[test]
fn failed_upload_can_be_retried_at_an_explicit_scheduler_tick() {
    let mut store = SimulatedObjectStore::new();
    store.stage_local_payload("writer", "object-a", "checksum-a", 12);
    let ticket = store.schedule_upload("writer", "object-a").unwrap();

    assert_eq!(
        store.fail_upload(ticket.upload_id, "injected failure", Some(7)),
        Some(UploadOutcome::Failed {
            upload_id: ticket.upload_id,
            object_id: "object-a".to_string(),
            error: "injected failure".to_string(),
            retry_at_tick: Some(7)
        })
    );
    assert_eq!(store.ready_retries(6), Vec::<u64>::new());
    assert_eq!(store.ready_retries(7), vec![ticket.upload_id]);

    assert!(matches!(
        store.complete_upload(ticket.upload_id),
        Some(UploadOutcome::Completed { .. })
    ));
    assert!(store.ready_retries(8).is_empty());
}

#[test]
fn reachability_export_marks_active_pending_and_deleted_objects() {
    let mut store = SimulatedObjectStore::new();
    store.stage_local_payload("writer", "active-object", "checksum-a", 1);
    store.stage_local_payload("writer", "pending-object", "checksum-b", 2);
    store.stage_local_payload("writer", "deleted-object", "checksum-c", 3);

    let pending = store.schedule_upload("writer", "pending-object").unwrap();
    let deleted = store.schedule_upload("writer", "deleted-object").unwrap();
    store.complete_upload(deleted.upload_id);
    store.mark_deleted("deleted-object");

    assert_eq!(store.ready_retries(10), Vec::<u64>::new());
    assert_eq!(
        store.object_lifecycle(),
        vec![
            ObjectLifecycle {
                object_id: "active-object".to_string(),
                present_remote: false,
                protected: true,
            },
            ObjectLifecycle {
                object_id: "deleted-object".to_string(),
                present_remote: true,
                protected: false,
            },
            ObjectLifecycle {
                object_id: "pending-object".to_string(),
                present_remote: false,
                protected: true,
            },
        ]
    );
    assert!(store.trace().iter().any(|record| record.kind
        == ObjectStoreTraceKind::UploadScheduled
        && record.upload_id == Some(pending.upload_id)));
}
