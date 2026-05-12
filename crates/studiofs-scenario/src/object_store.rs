//! Deterministic object-store and upload model for simulation tests.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::invariant::ObjectLifecycle;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulatedObject {
    /// Field $name.
    pub object_id: String,
    /// Field $name.
    pub checksum_sha256: String,
    /// Field $name.
    pub size: u64,
}

impl SimulatedObject {
    /// Function $name.
    pub fn new(
        object_id: impl Into<String>,
        checksum_sha256: impl Into<String>,
        size: u64,
    ) -> Self {
        Self {
            object_id: object_id.into(),
            checksum_sha256: checksum_sha256.into(),
            size,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct UploadTicket {
    /// Field $name.
    pub upload_id: u64,
    /// Field $name.
    pub client: String,
    /// Field $name.
    pub object_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Enum $name.
pub enum UploadOutcome {
    /// Variant $name.
    Completed {
        /// Field $name.
        upload_id: u64,
        /// Field $name.
        object_id: String,
    },
    /// Variant $name.
    AlreadyCompleted {
        /// Field $name.
        upload_id: u64,
        /// Field $name.
        object_id: String,
    },
    /// Variant $name.
    Failed {
        /// Field $name.
        upload_id: u64,
        /// Field $name.
        object_id: String,
        /// Field $name.
        error: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        /// Field $name.
        retry_at_tick: Option<u64>,
    },
    /// Variant $name.
    Blocked {
        /// Field $name.
        upload_id: u64,
        /// Field $name.
        object_id: String,
        /// Field $name.
        reason: ObjectStoreFault,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Enum $name.
pub enum ObjectStoreFault {
    /// Variant $name.
    ObjectStoreOutage,
    /// Variant $name.
    ObjectNotFound,
    /// Variant $name.
    LocalPayloadMissing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct ObjectStoreTraceRecord {
    /// Field $name.
    pub step: u64,
    /// Field $name.
    pub kind: ObjectStoreTraceKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub object_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub upload_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub fault: Option<ObjectStoreFault>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub retry_at_tick: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Enum $name.
pub enum ObjectStoreTraceKind {
    /// Variant $name.
    LocalPayloadStaged,
    /// Variant $name.
    UploadScheduled,
    /// Variant $name.
    UploadCompleted,
    /// Variant $name.
    UploadFailed,
    /// Variant $name.
    UploadBlocked,
    /// Variant $name.
    ObjectStoreOutageStarted,
    /// Variant $name.
    ObjectStoreOutageEnded,
    /// Variant $name.
    RemoteRead,
    /// Variant $name.
    RemoteReadFailed,
    /// Variant $name.
    MetadataPublished,
    /// Variant $name.
    ObjectDeleted,
}

#[derive(Debug, Default)]
/// Struct $name.
pub struct SimulatedObjectStore {
    available: bool,
    next_upload_id: u64,
    next_step: u64,
    local_payloads: BTreeMap<String, BTreeMap<String, SimulatedObject>>,
    remote_objects: BTreeMap<String, SimulatedObject>,
    uploads: BTreeMap<u64, UploadState>,
    published: BTreeSet<String>,
    deleted: BTreeSet<String>,
    trace: Vec<ObjectStoreTraceRecord>,
}

impl SimulatedObjectStore {
    /// Function $name.
    pub fn new() -> Self {
        Self {
            available: true,
            ..Self::default()
        }
    }

    /// Function $name.
    pub fn stage_local_payload(
        &mut self,
        client: impl Into<String>,
        object_id: impl Into<String>,
        checksum_sha256: impl Into<String>,
        size: u64,
    ) {
        let client = client.into();
        let object = SimulatedObject::new(object_id, checksum_sha256, size);
        self.deleted.remove(&object.object_id);
        self.local_payloads
            .entry(client.clone())
            .or_default()
            .insert(object.object_id.clone(), object.clone());
        self.record(trace_record(
            ObjectStoreTraceKind::LocalPayloadStaged,
            Some(client),
            Some(object.object_id),
            None,
            None,
            None,
        ));
    }

    /// Function $name.
    pub fn has_local_payload(&self, client: &str, object_id: &str) -> bool {
        self.local_payloads
            .get(client)
            .is_some_and(|payloads| payloads.contains_key(object_id))
    }

    /// Function $name.
    pub fn remove_local_payload(&mut self, client: &str, object_id: &str) -> bool {
        self.local_payloads
            .get_mut(client)
            .and_then(|payloads| payloads.remove(object_id))
            .is_some()
    }

    /// Function $name.
    pub fn has_remote_object(&self, object_id: &str) -> bool {
        self.remote_objects.contains_key(object_id)
    }

    /// Function $name.
    pub fn schedule_upload(
        &mut self,
        client: impl Into<String>,
        object_id: impl Into<String>,
    ) -> Result<UploadTicket, ObjectStoreFault> {
        let client = client.into();
        let object_id = object_id.into();
        if !self.has_local_payload(&client, &object_id) {
            return Err(ObjectStoreFault::LocalPayloadMissing);
        }
        self.next_upload_id += 1;
        let ticket = UploadTicket {
            upload_id: self.next_upload_id,
            client,
            object_id,
        };
        self.uploads.insert(
            ticket.upload_id,
            UploadState {
                ticket: ticket.clone(),
                retry_at_tick: None,
                terminal: false,
            },
        );
        self.record(upload_trace(
            &ticket,
            ObjectStoreTraceKind::UploadScheduled,
            None,
            None,
        ));
        Ok(ticket)
    }

    /// Function $name.
    pub fn upload_ticket(&self, upload_id: u64) -> Option<UploadTicket> {
        self.uploads
            .get(&upload_id)
            .map(|state| state.ticket.clone())
    }

    /// Function $name.
    pub fn complete_upload(&mut self, upload_id: u64) -> Option<UploadOutcome> {
        let state = self.uploads.get(&upload_id)?.clone();
        if state.terminal {
            return Some(UploadOutcome::AlreadyCompleted {
                upload_id,
                object_id: state.ticket.object_id,
            });
        }
        if !self.is_available() {
            let outcome = UploadOutcome::Blocked {
                upload_id,
                object_id: state.ticket.object_id.clone(),
                reason: ObjectStoreFault::ObjectStoreOutage,
            };
            self.record(upload_trace(
                &state.ticket,
                ObjectStoreTraceKind::UploadBlocked,
                Some(ObjectStoreFault::ObjectStoreOutage),
                state.retry_at_tick,
            ));
            return Some(outcome);
        }

        let Some(object) = self
            .local_payloads
            .get(&state.ticket.client)
            .and_then(|payloads| payloads.get(&state.ticket.object_id))
            .cloned()
        else {
            let outcome = UploadOutcome::Blocked {
                upload_id,
                object_id: state.ticket.object_id.clone(),
                reason: ObjectStoreFault::LocalPayloadMissing,
            };
            self.record(upload_trace(
                &state.ticket,
                ObjectStoreTraceKind::UploadBlocked,
                Some(ObjectStoreFault::LocalPayloadMissing),
                state.retry_at_tick,
            ));
            return Some(outcome);
        };
        self.remote_objects.insert(object.object_id.clone(), object);
        if let Some(state) = self.uploads.get_mut(&upload_id) {
            state.retry_at_tick = None;
            state.terminal = true;
        }
        self.record(upload_trace(
            &state.ticket,
            ObjectStoreTraceKind::UploadCompleted,
            None,
            None,
        ));
        Some(UploadOutcome::Completed {
            upload_id,
            object_id: state.ticket.object_id,
        })
    }

    /// Function $name.
    pub fn fail_upload(
        &mut self,
        upload_id: u64,
        error: impl Into<String>,
        retry_at_tick: Option<u64>,
    ) -> Option<UploadOutcome> {
        let error = error.into();
        let state = self.uploads.get_mut(&upload_id)?;
        if state.terminal {
            return None;
        }
        state.retry_at_tick = retry_at_tick;
        state.terminal = false;
        let ticket = state.ticket.clone();
        self.record(upload_trace(
            &ticket,
            ObjectStoreTraceKind::UploadFailed,
            None,
            retry_at_tick,
        ));
        Some(UploadOutcome::Failed {
            upload_id,
            object_id: ticket.object_id,
            error,
            retry_at_tick,
        })
    }

    /// Function $name.
    pub fn ready_retries(&self, tick: u64) -> Vec<u64> {
        self.uploads
            .iter()
            .filter(|(_, state)| {
                !state.terminal && state.retry_at_tick.is_some_and(|retry| retry <= tick)
            })
            .map(|(upload_id, _)| *upload_id)
            .collect()
    }

    /// Function $name.
    pub fn set_available(&mut self, available: bool) {
        if self.available == available {
            return;
        }
        self.available = available;
        self.record(trace_record(
            if available {
                ObjectStoreTraceKind::ObjectStoreOutageEnded
            } else {
                ObjectStoreTraceKind::ObjectStoreOutageStarted
            },
            None,
            None,
            None,
            None,
            None,
        ));
    }

    /// Function $name.
    pub fn is_available(&self) -> bool {
        self.available
    }

    /// Function $name.
    pub fn read_remote(&mut self, object_id: &str) -> Result<SimulatedObject, ObjectStoreFault> {
        if !self.is_available() {
            self.record(trace_record(
                ObjectStoreTraceKind::RemoteReadFailed,
                None,
                Some(object_id.to_string()),
                None,
                Some(ObjectStoreFault::ObjectStoreOutage),
                None,
            ));
            return Err(ObjectStoreFault::ObjectStoreOutage);
        }
        let Some(object) = self.remote_objects.get(object_id).cloned() else {
            self.record(trace_record(
                ObjectStoreTraceKind::RemoteReadFailed,
                None,
                Some(object_id.to_string()),
                None,
                Some(ObjectStoreFault::ObjectNotFound),
                None,
            ));
            return Err(ObjectStoreFault::ObjectNotFound);
        };
        self.record(trace_record(
            ObjectStoreTraceKind::RemoteRead,
            None,
            Some(object_id.to_string()),
            None,
            None,
            None,
        ));
        Ok(object)
    }

    /// Function $name.
    pub fn mark_published(&mut self, object_id: &str) {
        self.published.insert(object_id.to_string());
        self.record(trace_record(
            ObjectStoreTraceKind::MetadataPublished,
            None,
            Some(object_id.to_string()),
            None,
            None,
            None,
        ));
    }

    /// Function $name.
    pub fn is_published(&self, object_id: &str) -> bool {
        self.published.contains(object_id)
    }

    /// Function $name.
    pub fn mark_deleted(&mut self, object_id: &str) {
        self.deleted.insert(object_id.to_string());
        self.published.remove(object_id);
        self.record(trace_record(
            ObjectStoreTraceKind::ObjectDeleted,
            None,
            Some(object_id.to_string()),
            None,
            None,
            None,
        ));
    }

    /// Function $name.
    pub fn object_lifecycle(&self) -> Vec<ObjectLifecycle> {
        let local_payload_ids = self
            .local_payloads
            .values()
            .flat_map(|payloads| payloads.keys().cloned())
            .collect::<BTreeSet<_>>();
        let pending_upload_ids = self
            .uploads
            .values()
            .filter(|state| !state.terminal)
            .map(|state| state.ticket.object_id.clone())
            .collect::<BTreeSet<_>>();

        self.known_object_ids()
            .into_iter()
            .map(|object_id| ObjectLifecycle {
                present_remote: self.remote_objects.contains_key(&object_id),
                protected: !self.deleted.contains(&object_id)
                    && (local_payload_ids.contains(&object_id)
                        || pending_upload_ids.contains(&object_id)
                        || self.published.contains(&object_id)),
                object_id,
            })
            .collect()
    }

    /// Function $name.
    pub fn trace(&self) -> &[ObjectStoreTraceRecord] {
        &self.trace
    }

    /// Function $name.
    pub fn drain_trace(&mut self) -> Vec<ObjectStoreTraceRecord> {
        std::mem::take(&mut self.trace)
    }

    fn known_object_ids(&self) -> BTreeSet<String> {
        let mut ids = BTreeSet::new();
        for payloads in self.local_payloads.values() {
            ids.extend(payloads.keys().cloned());
        }
        ids.extend(self.remote_objects.keys().cloned());
        ids.extend(
            self.uploads
                .values()
                .map(|state| state.ticket.object_id.clone()),
        );
        ids.extend(self.published.iter().cloned());
        ids.extend(self.deleted.iter().cloned());
        ids
    }

    fn record(&mut self, mut record: ObjectStoreTraceRecord) {
        self.next_step += 1;
        record.step = self.next_step;
        self.trace.push(record);
    }
}

#[derive(Debug, Clone)]
struct UploadState {
    ticket: UploadTicket,
    retry_at_tick: Option<u64>,
    terminal: bool,
}

fn upload_trace(
    ticket: &UploadTicket,
    kind: ObjectStoreTraceKind,
    fault: Option<ObjectStoreFault>,
    retry_at_tick: Option<u64>,
) -> ObjectStoreTraceRecord {
    trace_record(
        kind,
        Some(ticket.client.clone()),
        Some(ticket.object_id.clone()),
        Some(ticket.upload_id),
        fault,
        retry_at_tick,
    )
}

fn trace_record(
    kind: ObjectStoreTraceKind,
    client: Option<String>,
    object_id: Option<String>,
    upload_id: Option<u64>,
    fault: Option<ObjectStoreFault>,
    retry_at_tick: Option<u64>,
) -> ObjectStoreTraceRecord {
    ObjectStoreTraceRecord {
        step: 0,
        kind,
        client,
        object_id,
        upload_id,
        fault,
        retry_at_tick,
    }
}
