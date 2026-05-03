use studiofs_scenario::scheduler::{
    ScheduledEvent, SchedulerEventType, SchedulerRunOutcome, SimulationScheduler,
};

fn client_event(client: &str, event_type: SchedulerEventType) -> ScheduledEvent {
    ScheduledEvent::new(event_type).with_client(client)
}

#[test]
fn fixed_seed_replays_the_same_interleaving_and_trace() {
    let first = run_seeded_upload_and_publish(42);
    let second = run_seeded_upload_and_publish(42);
    let different = run_seeded_upload_and_publish(43);

    assert_eq!(first, second);
    assert_ne!(first, different);
    assert_eq!(
        first.iter().map(|record| record.seed).collect::<Vec<_>>(),
        vec![42, 42, 42]
    );
    assert!(
        first
            .iter()
            .any(|record| record.client.as_deref() == Some("writer")
                && record.event_type == SchedulerEventType::UploadCompletion)
    );
}

#[test]
fn scheduler_advances_virtual_ticks_without_wall_clock_sleep() {
    let mut scheduler = SimulationScheduler::new(7);
    scheduler.schedule_at(
        5,
        client_event("writer", SchedulerEventType::UploadCompletion),
    );
    scheduler.schedule_at(
        2,
        client_event("reader", SchedulerEventType::MetadataDelivery),
    );

    assert_eq!(scheduler.current_tick(), 0);

    assert_eq!(
        scheduler.step().unwrap().event.event_type,
        SchedulerEventType::MetadataDelivery
    );
    assert_eq!(scheduler.current_tick(), 2);

    assert_eq!(
        scheduler.step().unwrap().event.event_type,
        SchedulerEventType::UploadCompletion
    );
    assert_eq!(scheduler.current_tick(), 5);
}

#[test]
fn run_until_idle_executes_all_ready_events() {
    let mut scheduler = SimulationScheduler::new(99);
    scheduler.schedule_now(client_event("a", SchedulerEventType::ClientOperation));
    scheduler.schedule_after(1, client_event("b", SchedulerEventType::PublishAttempt));

    assert_eq!(scheduler.run_until_idle(), SchedulerRunOutcome::Idle);

    let trace = scheduler.trace();
    assert_eq!(trace.len(), 2);
    assert_eq!(scheduler.pending_len(), 0);
    assert_eq!(scheduler.current_tick(), 1);
}

#[test]
fn run_until_invariant_failure_stops_before_draining_later_events() {
    let mut scheduler = SimulationScheduler::new(11);
    scheduler.schedule_now(client_event("a", SchedulerEventType::ClientOperation));
    scheduler.schedule_after(1, client_event("b", SchedulerEventType::CrashRestart));
    scheduler.schedule_after(2, client_event("c", SchedulerEventType::MetadataDelivery));

    let outcome = scheduler.run_until(|selected| {
        if selected.event.client.as_deref() == Some("b") {
            Err("client b invariant failed".to_string())
        } else {
            Ok(())
        }
    });

    assert_eq!(
        outcome,
        SchedulerRunOutcome::InvariantFailed {
            message: "client b invariant failed".to_string()
        }
    );
    assert_eq!(scheduler.trace().len(), 2);
    assert_eq!(scheduler.pending_len(), 1);
}

#[test]
fn trace_can_be_drained_between_simulation_windows() {
    let mut scheduler = SimulationScheduler::new(3);
    scheduler.schedule_now(client_event("a", SchedulerEventType::ObjectStoreFault));
    scheduler.step().unwrap();

    let drained = scheduler.drain_trace();
    assert_eq!(drained.len(), 1);
    assert!(scheduler.trace().is_empty());

    scheduler.schedule_now(client_event("b", SchedulerEventType::MetadataDelivery));
    scheduler.step().unwrap();

    assert_eq!(scheduler.trace().len(), 1);
    assert_eq!(
        scheduler.trace()[0].event_type,
        SchedulerEventType::MetadataDelivery
    );
}

fn run_seeded_upload_and_publish(
    seed: u64,
) -> Vec<studiofs_scenario::scheduler::SchedulerTraceRecord> {
    let mut scheduler = SimulationScheduler::new(seed);
    scheduler.schedule_now(client_event("writer", SchedulerEventType::ClientOperation));
    scheduler.schedule_now(client_event("writer", SchedulerEventType::UploadCompletion));
    scheduler.schedule_now(client_event("reader", SchedulerEventType::MetadataDelivery));

    assert_eq!(scheduler.run_until_idle(), SchedulerRunOutcome::Idle);
    scheduler.trace().to_vec()
}
