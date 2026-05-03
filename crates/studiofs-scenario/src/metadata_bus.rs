//! Deterministic metadata replication bus for simulation tests.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataEvent {
    pub kind: String,
    pub fields: BTreeMap<String, String>,
}

impl MetadataEvent {
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            fields: BTreeMap::new(),
        }
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataBatch {
    pub batch_id: u64,
    pub version: u64,
    pub source_client: String,
    pub events: Vec<MetadataEvent>,
}

impl MetadataBatch {
    pub fn new(source_client: impl Into<String>, events: Vec<MetadataEvent>) -> Self {
        Self {
            batch_id: 0,
            version: 0,
            source_client: source_client.into(),
            events,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryOutcome {
    Applied {
        batch_id: u64,
        version: u64,
        source_client: String,
    },
    DuplicateIgnored {
        batch_id: u64,
        version: u64,
        source_client: String,
    },
    Blocked {
        reason: DeliveryFault,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryFault {
    MetadataOutage,
    ClientDisconnected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataTraceRecord {
    pub step: u64,
    pub kind: MetadataTraceKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fault: Option<DeliveryFault>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTraceKind {
    ClientConnected,
    ClientDisconnected,
    MetadataOutageStarted,
    MetadataOutageEnded,
    Enqueued,
    Delayed,
    DelayReleased,
    Delivered,
    DuplicateIgnored,
    DeliveryBlocked,
}

#[derive(Debug, Default)]
pub struct MetadataBus {
    next_batch_id: u64,
    next_version: u64,
    next_step: u64,
    metadata_outage: bool,
    batches: BTreeMap<u64, MetadataBatch>,
    connected_clients: BTreeSet<String>,
    client_queues: BTreeMap<String, VecDeque<u64>>,
    applied_batches: BTreeMap<String, BTreeSet<u64>>,
    applied_versions: BTreeMap<String, Vec<u64>>,
    delayed: BTreeMap<String, BTreeSet<u64>>,
    trace: Vec<MetadataTraceRecord>,
}

impl MetadataBus {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn connect_client(&mut self, client: impl Into<String>) {
        let client = client.into();
        self.client_queues.entry(client.clone()).or_default();
        self.connected_clients.insert(client.clone());
        self.record(MetadataTraceRecord {
            step: 0,
            kind: MetadataTraceKind::ClientConnected,
            batch_id: None,
            version: None,
            source_client: None,
            target_client: Some(client),
            fault: None,
        });
    }

    pub fn disconnect_client(&mut self, client: &str) {
        self.connected_clients.remove(client);
        self.record(MetadataTraceRecord {
            step: 0,
            kind: MetadataTraceKind::ClientDisconnected,
            batch_id: None,
            version: None,
            source_client: None,
            target_client: Some(client.to_string()),
            fault: None,
        });
    }

    pub fn reconnect_client(&mut self, client: impl Into<String>) {
        self.connect_client(client);
    }

    pub fn set_metadata_outage(&mut self, outage: bool) {
        if self.metadata_outage == outage {
            return;
        }
        self.metadata_outage = outage;
        self.record(MetadataTraceRecord {
            step: 0,
            kind: if outage {
                MetadataTraceKind::MetadataOutageStarted
            } else {
                MetadataTraceKind::MetadataOutageEnded
            },
            batch_id: None,
            version: None,
            source_client: None,
            target_client: None,
            fault: None,
        });
    }

    pub fn enqueue_local(&mut self, mut batch: MetadataBatch) -> MetadataBatch {
        self.next_batch_id += 1;
        self.next_version += 1;
        batch.batch_id = self.next_batch_id;
        batch.version = self.next_version;

        for client in self.client_queues.keys().cloned().collect::<Vec<_>>() {
            if client != batch.source_client {
                self.client_queues
                    .entry(client)
                    .or_default()
                    .push_back(batch.batch_id);
            }
        }
        self.batches.insert(batch.batch_id, batch.clone());
        self.record(batch_trace(&batch, MetadataTraceKind::Enqueued, None));
        batch
    }

    pub fn pending_for(&self, client: &str) -> Vec<MetadataBatch> {
        self.client_queues
            .get(client)
            .into_iter()
            .flat_map(|queue| queue.iter())
            .filter(|batch_id| !self.is_delayed(client, **batch_id))
            .filter_map(|batch_id| self.batches.get(batch_id).cloned())
            .collect()
    }

    pub fn deliver_next(&mut self, client: &str) -> Option<DeliveryOutcome> {
        if self.metadata_outage {
            return Some(self.blocked(client, DeliveryFault::MetadataOutage));
        }
        if !self.connected_clients.contains(client) {
            return Some(self.blocked(client, DeliveryFault::ClientDisconnected));
        }

        let batch_id = self.next_ready_batch_id(client)?;
        let queue = self.client_queues.entry(client.to_string()).or_default();
        if let Some(position) = queue.iter().position(|candidate| *candidate == batch_id) {
            queue.remove(position);
        }

        let batch = self.batches.get(&batch_id).cloned()?;
        let already_applied = !self
            .applied_batches
            .entry(client.to_string())
            .or_default()
            .insert(batch_id);

        if already_applied {
            self.record(batch_trace(
                &batch,
                MetadataTraceKind::DuplicateIgnored,
                Some(client),
            ));
            return Some(DeliveryOutcome::DuplicateIgnored {
                batch_id,
                version: batch.version,
                source_client: batch.source_client,
            });
        }

        self.applied_versions
            .entry(client.to_string())
            .or_default()
            .push(batch.version);
        self.record(batch_trace(
            &batch,
            MetadataTraceKind::Delivered,
            Some(client),
        ));
        Some(DeliveryOutcome::Applied {
            batch_id,
            version: batch.version,
            source_client: batch.source_client,
        })
    }

    pub fn delay_for(&mut self, client: &str, batch_id: u64) {
        self.delayed
            .entry(client.to_string())
            .or_default()
            .insert(batch_id);
        if let Some(batch) = self.batches.get(&batch_id).cloned() {
            self.record(batch_trace(
                &batch,
                MetadataTraceKind::Delayed,
                Some(client),
            ));
        }
    }

    pub fn release_delay(&mut self, client: &str, batch_id: u64) {
        if let Some(delayed) = self.delayed.get_mut(client) {
            delayed.remove(&batch_id);
        }
        if let Some(batch) = self.batches.get(&batch_id).cloned() {
            self.record(batch_trace(
                &batch,
                MetadataTraceKind::DelayReleased,
                Some(client),
            ));
        }
    }

    pub fn inject_duplicate(&mut self, client: &str, batch_id: u64) {
        self.client_queues
            .entry(client.to_string())
            .or_default()
            .push_back(batch_id);
    }

    pub fn applied_versions(&self, client: &str) -> Vec<u64> {
        self.applied_versions
            .get(client)
            .cloned()
            .unwrap_or_default()
    }

    pub fn trace(&self) -> &[MetadataTraceRecord] {
        &self.trace
    }

    fn next_ready_batch_id(&self, client: &str) -> Option<u64> {
        self.client_queues
            .get(client)?
            .iter()
            .copied()
            .find(|batch_id| !self.is_delayed(client, *batch_id))
    }

    fn is_delayed(&self, client: &str, batch_id: u64) -> bool {
        self.delayed
            .get(client)
            .is_some_and(|delayed| delayed.contains(&batch_id))
    }

    fn blocked(&mut self, client: &str, fault: DeliveryFault) -> DeliveryOutcome {
        self.record(MetadataTraceRecord {
            step: 0,
            kind: MetadataTraceKind::DeliveryBlocked,
            batch_id: None,
            version: None,
            source_client: None,
            target_client: Some(client.to_string()),
            fault: Some(fault),
        });
        DeliveryOutcome::Blocked { reason: fault }
    }

    fn record(&mut self, mut record: MetadataTraceRecord) {
        self.next_step += 1;
        record.step = self.next_step;
        self.trace.push(record);
    }
}

fn batch_trace(
    batch: &MetadataBatch,
    kind: MetadataTraceKind,
    target_client: Option<&str>,
) -> MetadataTraceRecord {
    MetadataTraceRecord {
        step: 0,
        kind,
        batch_id: Some(batch.batch_id),
        version: Some(batch.version),
        source_client: Some(batch.source_client.clone()),
        target_client: target_client.map(str::to_string),
        fault: None,
    }
}
