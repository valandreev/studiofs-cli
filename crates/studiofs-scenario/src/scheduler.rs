//! Virtual-time deterministic scheduler for simulation tests.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

const SEED_MIX_GOLDEN_RATIO: u64 = 0x9e37_79b9_7f4a_7c15;

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
    pub ready_event_ids: Vec<u64>,
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
    pending: BTreeMap<u64, Vec<QueuedEvent>>,
    trace: Vec<SchedulerTraceRecord>,
}

impl SimulationScheduler {
    pub fn new(seed: u64) -> Self {
        Self::new_with_stream_seed(seed, seed)
    }

    pub fn new_with_stream_seed(seed: u64, stream_seed: u64) -> Self {
        Self {
            seed,
            rng: DeterministicRng::new(stream_seed),
            current_tick: 0,
            next_event_id: 0,
            next_step: 0,
            pending: BTreeMap::new(),
            trace: Vec::new(),
        }
    }

    pub fn current_tick(&self) -> u64 {
        self.current_tick
    }

    pub fn pending_len(&self) -> usize {
        self.pending.values().map(Vec::len).sum()
    }

    pub fn schedule_now(&mut self, event: ScheduledEvent) -> u64 {
        self.schedule_at(self.current_tick, event)
    }

    pub fn schedule_after(&mut self, ticks: u64, event: ScheduledEvent) -> u64 {
        self.schedule_at(self.current_tick.saturating_add(ticks), event)
    }

    pub fn schedule_at(&mut self, tick: u64, event: ScheduledEvent) -> u64 {
        debug_assert!(tick >= self.current_tick, "cannot schedule in the past");
        self.next_event_id += 1;
        let event_id = self.next_event_id;
        self.pending
            .entry(tick)
            .or_default()
            .push(QueuedEvent { event_id, event });
        event_id
    }

    pub fn step(&mut self) -> Option<SelectedEvent> {
        let tick = self.next_ready_tick()?;
        self.current_tick = tick;
        let ready_events = self.pending.get_mut(&tick)?;
        let ready_event_ids = ready_events
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>();
        let choice = self.rng.choose(ready_events.len());
        let queued = ready_events.swap_remove(choice);
        if ready_events.is_empty() {
            self.pending.remove(&tick);
        }
        let selected = SelectedEvent {
            event_id: queued.event_id,
            scheduled_tick: tick,
            event: queued.event,
        };
        self.record(&selected, choice, ready_event_ids);
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

    fn next_ready_tick(&self) -> Option<u64> {
        self.pending.first_key_value().map(|(tick, _)| *tick)
    }

    fn record(&mut self, selected: &SelectedEvent, choice: usize, ready_event_ids: Vec<u64>) {
        self.next_step += 1;
        let ready_event_count = ready_event_ids.len();
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
            ready_event_ids,
        });
    }
}

pub fn simulation_stream_seed(seed: u64, stream_index: u64) -> u64 {
    seed ^ stream_index.wrapping_mul(SEED_MIX_GOLDEN_RATIO)
}

#[derive(Debug)]
struct QueuedEvent {
    event_id: u64,
    event: ScheduledEvent,
}

#[derive(Debug)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self {
            state: mix_seed(seed),
        }
    }

    fn choose(&mut self, len: usize) -> usize {
        debug_assert!(len > 0);
        loop {
            if let Some(choice) = Self::choice_from_u32(self.next_u32(), len) {
                return choice;
            }
        }
    }

    fn choice_from_u32(value: u32, len: usize) -> Option<usize> {
        let len = u64::try_from(len).expect("ready event count must fit in u64");
        let sample_space = u64::from(u32::MAX) + 1;
        let limit = sample_space - (sample_space % len);
        let value = u64::from(value);
        (value < limit).then_some((value % len) as usize)
    }

    fn next_u32(&mut self) -> u32 {
        let old_state = self.state;
        self.state = old_state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let xorshifted = (((old_state >> 18) ^ old_state) >> 27) as u32;
        let rotate = (old_state >> 59) as u32;
        xorshifted.rotate_right(rotate)
    }
}

fn mix_seed(mut value: u64) -> u64 {
    value = value.wrapping_add(SEED_MIX_GOLDEN_RATIO);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::DeterministicRng;

    #[test]
    fn choice_rejects_values_outside_the_even_selection_window() {
        assert_eq!(DeterministicRng::choice_from_u32(5, 10), Some(5));
        assert_eq!(DeterministicRng::choice_from_u32(u32::MAX - 4, 10), None);
        assert_eq!(DeterministicRng::choice_from_u32(u32::MAX, 10), None);
    }
}
