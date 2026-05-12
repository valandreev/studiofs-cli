//! Crate documentation for lint completion.
use studiofs_scenario::metadata_bus::{
    DeliveryFault, DeliveryOutcome, MetadataBatch, MetadataBus, MetadataEvent, MetadataTraceKind,
};

fn batch(client: &str, payload: &str) -> MetadataBatch {
    MetadataBatch::new(
        client,
        vec![MetadataEvent::new("file_published").with_field("path", payload)],
    )
}

#[test]
fn delivers_foreign_batches_in_version_order_at_explicit_scheduler_points() {
    let mut bus = MetadataBus::new();
    bus.connect_client("client-a");
    bus.connect_client("client-b");

    let first = bus.enqueue_local(batch("client-a", "one.txt"));
    let second = bus.enqueue_local(batch("client-a", "two.txt"));

    assert_eq!(first.version, 1);
    assert_eq!(second.version, 2);
    assert_eq!(
        bus.pending_for("client-b")
            .iter()
            .map(|batch| batch.version)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let delivered = bus.deliver_next("client-b");

    assert_eq!(
        delivered,
        Some(DeliveryOutcome::Applied {
            batch_id: first.batch_id,
            version: 1,
            source_client: "client-a".to_string()
        })
    );
    assert_eq!(
        bus.pending_for("client-b")
            .iter()
            .map(|batch| batch.version)
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(bus.applied_versions("client-b"), vec![1]);
    assert!(
        bus.trace()
            .iter()
            .any(|record| record.kind == MetadataTraceKind::Delivered
                && record.target_client.as_deref() == Some("client-b")
                && record.version == Some(1))
    );
}

#[test]
fn outage_and_disconnect_block_delivery_until_explicit_recovery() {
    let mut bus = MetadataBus::new();
    bus.connect_client("writer");
    bus.connect_client("reader");

    bus.enqueue_local(batch("writer", "during-outage.txt"));
    bus.set_metadata_outage(true);
    assert_eq!(
        bus.deliver_next("reader"),
        Some(DeliveryOutcome::Blocked {
            reason: DeliveryFault::MetadataOutage
        })
    );
    assert_eq!(bus.applied_versions("reader"), Vec::<u64>::new());

    bus.set_metadata_outage(false);
    bus.disconnect_client("reader");
    assert_eq!(
        bus.deliver_next("reader"),
        Some(DeliveryOutcome::Blocked {
            reason: DeliveryFault::ClientDisconnected
        })
    );

    bus.reconnect_client("reader");
    assert!(matches!(
        bus.deliver_next("reader"),
        Some(DeliveryOutcome::Applied { version: 1, .. })
    ));
    assert_eq!(bus.applied_versions("reader"), vec![1]);
}

#[test]
fn duplicate_delivery_is_recorded_but_idempotent() {
    let mut bus = MetadataBus::new();
    bus.connect_client("writer");
    bus.connect_client("reader");

    let publication = bus.enqueue_local(batch("writer", "idempotent.txt"));
    assert!(matches!(
        bus.deliver_next("reader"),
        Some(DeliveryOutcome::Applied { version: 1, .. })
    ));

    bus.inject_duplicate("reader", publication.batch_id);
    assert_eq!(
        bus.deliver_next("reader"),
        Some(DeliveryOutcome::DuplicateIgnored {
            batch_id: publication.batch_id,
            version: 1,
            source_client: "writer".to_string()
        })
    );
    assert_eq!(bus.applied_versions("reader"), vec![1]);
}

#[test]
fn delayed_delivery_waits_for_scheduler_release() {
    let mut bus = MetadataBus::new();
    bus.connect_client("writer");
    bus.connect_client("reader");

    let publication = bus.enqueue_local(batch("writer", "delayed.txt"));
    bus.delay_for("reader", publication.batch_id);

    assert_eq!(bus.deliver_next("reader"), None);
    bus.release_delay("reader", publication.batch_id);

    assert!(matches!(
        bus.deliver_next("reader"),
        Some(DeliveryOutcome::Applied { version: 1, .. })
    ));
}

#[test]
fn delayed_head_blocks_later_batches_for_strict_version_ordering() {
    let mut bus = MetadataBus::new();
    bus.connect_client("writer");
    bus.connect_client("reader");

    let first = bus.enqueue_local(batch("writer", "first.txt"));
    let second = bus.enqueue_local(batch("writer", "second.txt"));
    bus.delay_for("reader", first.batch_id);

    assert_eq!(bus.deliver_next("reader"), None);
    assert_eq!(
        bus.pending_for("reader")
            .iter()
            .map(|batch| batch.version)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    bus.release_delay("reader", first.batch_id);

    assert_eq!(
        bus.deliver_next("reader"),
        Some(DeliveryOutcome::Applied {
            batch_id: first.batch_id,
            version: 1,
            source_client: "writer".to_string()
        })
    );
    assert_eq!(
        bus.deliver_next("reader"),
        Some(DeliveryOutcome::Applied {
            batch_id: second.batch_id,
            version: 2,
            source_client: "writer".to_string()
        })
    );
}

#[test]
fn trace_can_be_drained_between_long_simulation_windows() {
    let mut bus = MetadataBus::new();
    bus.connect_client("writer");
    bus.connect_client("reader");
    bus.enqueue_local(batch("writer", "traced.txt"));

    let drained = bus.drain_trace();

    assert!(
        drained
            .iter()
            .any(|record| record.kind == MetadataTraceKind::Enqueued)
    );
    assert!(bus.trace().is_empty());

    bus.deliver_next("reader");

    assert!(
        bus.trace()
            .iter()
            .any(|record| record.kind == MetadataTraceKind::Delivered)
    );
}

#[test]
fn unknown_batch_ids_are_rejected_without_mutating_delivery_state() {
    let mut bus = MetadataBus::new();
    bus.connect_client("writer");
    bus.connect_client("reader");
    let publication = bus.enqueue_local(batch("writer", "known.txt"));
    bus.drain_trace();

    assert!(!bus.delay_for("reader", 999));
    assert!(!bus.release_delay("reader", 999));
    assert!(!bus.inject_duplicate("reader", 999));
    assert!(bus.trace().is_empty());
    assert_eq!(
        bus.pending_for("reader")
            .iter()
            .map(|batch| batch.batch_id)
            .collect::<Vec<_>>(),
        vec![publication.batch_id]
    );

    assert_eq!(
        bus.deliver_next("reader"),
        Some(DeliveryOutcome::Applied {
            batch_id: publication.batch_id,
            version: 1,
            source_client: "writer".to_string()
        })
    );
}
