//! Virtual-time deterministic scheduler for simulation tests.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledEvent {
    pub event_type: SchedulerEventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl ScheduledEvent {
    pub fn new(event_type: SchedulerEventType) -> Self {
        Self {
            event_type,
            client: None,
            description: None,
        }
    }

    pub fn with_client(mut self, client: impl Into<String>) -> Self {
        self.client = Some(client.into());
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerEventType {
    ClientOperation,
    MetadataDelivery,
    UploadCompletion,
    PublishAttempt,
    CrashRestart,
    ObjectStoreFault,
    MetadataFault,
    Checkpoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedEvent {
    pub event_id: u64,
    pub scheduled_tick: u64,
    pub event: ScheduledEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerTraceRecord {
    pub step: u64,
    pub seed: u64,
    pub virtual_tick: u64,
    pub event_id: u64,
    pub event_type: SchedulerEventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub interleaving_choice: usize,
    pub ready_event_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerRunOutcome {
    Idle,
    InvariantFailed { message: String },
}

#[derive(Debug)]
pub struct SimulationScheduler {
    seed: u64,
    rng: DeterministicRng,
    current_tick: u64,
    next_event_id: u64,
    next_step: u64,
    pending: Vec<QueuedEvent>,
    trace: Vec<SchedulerTraceRecord>,
}

impl SimulationScheduler {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            rng: DeterministicRng::new(seed),
            current_tick: 0,
            next_event_id: 0,
            next_step: 0,
            pending: Vec::new(),
            trace: Vec::new(),
        }
    }

    pub fn current_tick(&self) -> u64 {
        self.current_tick
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn schedule_now(&mut self, event: ScheduledEvent) -> u64 {
        self.schedule_at(self.current_tick, event)
    }

    pub fn schedule_after(&mut self, ticks: u64, event: ScheduledEvent) -> u64 {
        self.schedule_at(self.current_tick.saturating_add(ticks), event)
    }

    pub fn schedule_at(&mut self, tick: u64, event: ScheduledEvent) -> u64 {
        self.next_event_id += 1;
        let event_id = self.next_event_id;
        self.pending.push(QueuedEvent {
            event_id,
            tick,
            event,
        });
        event_id
    }

    pub fn step(&mut self) -> Option<SelectedEvent> {
        let (ready_indexes, tick) = self.ready_indexes()?;
        self.current_tick = tick;
        let choice = self.rng.choose(ready_indexes.len());
        let pending_index = ready_indexes[choice];
        let queued = self.pending.remove(pending_index);
        let selected = SelectedEvent {
            event_id: queued.event_id,
            scheduled_tick: queued.tick,
            event: queued.event,
        };
        self.record(&selected, choice, ready_indexes.len());
        Some(selected)
    }

    pub fn run_until_idle(&mut self) -> SchedulerRunOutcome {
        self.run_until(|_| Ok(()))
    }

    pub fn run_until(
        &mut self,
        mut check_invariant: impl FnMut(&SelectedEvent) -> Result<(), String>,
    ) -> SchedulerRunOutcome {
        while let Some(selected) = self.step() {
            if let Err(message) = check_invariant(&selected) {
                return SchedulerRunOutcome::InvariantFailed { message };
            }
        }
        SchedulerRunOutcome::Idle
    }

    pub fn trace(&self) -> &[SchedulerTraceRecord] {
        &self.trace
    }

    pub fn drain_trace(&mut self) -> Vec<SchedulerTraceRecord> {
        std::mem::take(&mut self.trace)
    }

    fn ready_indexes(&self) -> Option<(Vec<usize>, u64)> {
        let tick = self.pending.iter().map(|event| event.tick).min()?;
        let ready_indexes = self
            .pending
            .iter()
            .enumerate()
            .filter_map(|(index, event)| (event.tick == tick).then_some(index))
            .collect::<Vec<_>>();
        Some((ready_indexes, tick))
    }

    fn record(&mut self, selected: &SelectedEvent, choice: usize, ready_event_count: usize) {
        self.next_step += 1;
        self.trace.push(SchedulerTraceRecord {
            step: self.next_step,
            seed: self.seed,
            virtual_tick: self.current_tick,
            event_id: selected.event_id,
            event_type: selected.event.event_type,
            client: selected.event.client.clone(),
            description: selected.event.description.clone(),
            interleaving_choice: choice,
            ready_event_count,
        });
    }
}

#[derive(Debug)]
struct QueuedEvent {
    event_id: u64,
    tick: u64,
    event: ScheduledEvent,
}

#[derive(Debug)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn choose(&mut self, len: usize) -> usize {
        debug_assert!(len > 0);
        let choice = (self.state as usize) % len;
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        choice
    }
}
