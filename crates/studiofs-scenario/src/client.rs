//! Simulated logical multi-client runtime for deterministic scenario tests.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::invariant::{
    ClientLogicalSnapshot, DurableWriteExpectation, LogicalByteRange, LogicalByteRangeKind,
    LogicalEntry, LogicalSnapshot, ObjectRef,
};
use crate::metadata_bus::{
    DeliveryFault, DeliveryOutcome, MetadataBatch, MetadataBus, MetadataEvent,
};
use crate::object_store::{ObjectStoreFault, SimulatedObjectStore, UploadOutcome, UploadTicket};
use crate::scheduler::{ScheduledEvent, SchedulerEventType, SimulationScheduler};

#[derive(Debug)]
/// Struct $name.
pub struct SimulatedRuntime {
    next_logical_id: u64,
    clients: BTreeMap<String, SimulatedClient>,
    metadata_bus: MetadataBus,
    object_store: SimulatedObjectStore,
    scheduler: SimulationScheduler,
    trace: Vec<ClientTraceRecord>,
}

impl SimulatedRuntime {
    /// Function $name.
    pub fn new(seed: u64) -> Self {
        Self {
            next_logical_id: 0,
            clients: BTreeMap::new(),
            metadata_bus: MetadataBus::new(),
            object_store: SimulatedObjectStore::new(),
            scheduler: SimulationScheduler::new(seed),
            trace: Vec::new(),
        }
    }

    /// Function $name.
    pub fn add_client(&mut self, client: impl Into<String>) {
        let client = client.into();
        self.clients
            .entry(client.clone())
            .or_insert_with(|| SimulatedClient::new(client.clone()));
        self.metadata_bus.connect_client(client.clone());
        self.record(client, "client_connected", None);
    }

    /// Function $name.
    pub fn create_file(&mut self, client: &str, path: &str) -> Result<(), ClientRuntimeError> {
        validate_mutation_path(client, path)?;
        self.ensure_client(client)?;
        self.ensure_parent_dirs(client, path)?;
        if self.client(client)?.entries.contains_key(path) {
            return Err(ClientRuntimeError::PathExists {
                client: client.to_string(),
                path: path.to_string(),
            });
        }
        let logical_id = self.next_file_logical_id();
        self.client_mut(client)?.entries.insert(
            path.to_string(),
            LocalEntry::file(logical_id, None, None, Some(ObjectRef::local("")), 0, None),
        );
        self.record(client, "create_file", Some(path));
        self.schedule_client_event(client, "create_file");
        Ok(())
    }

    /// Function $name.
    pub fn mkdir(&mut self, client: &str, path: &str) -> Result<(), ClientRuntimeError> {
        validate_mutation_path(client, path)?;
        self.ensure_client(client)?;
        self.ensure_parent_dirs(client, path)?;
        if self.client(client)?.entries.contains_key(path) {
            return Err(ClientRuntimeError::PathExists {
                client: client.to_string(),
                path: path.to_string(),
            });
        }
        let logical_id = self.directory_logical_id(path);
        self.client_mut(client)?
            .entries
            .insert(path.to_string(), LocalEntry::directory(logical_id));
        self.persist_current_entry(client, path)?;
        self.record(client, "mkdir", Some(path));
        self.schedule_client_event(client, "mkdir");
        Ok(())
    }

    /// Function $name.
    pub fn write_file(
        &mut self,
        client: &str,
        path: &str,
        size: u64,
        checksum_sha256: impl Into<String>,
    ) -> Result<(), ClientRuntimeError> {
        let checksum_sha256 = checksum_sha256.into();
        let logical_id = self.entry(client, path)?.logical_id.clone();
        let object_id = Self::object_id(client, &logical_id, &checksum_sha256);
        let next_epoch = self
            .entry(client, path)?
            .epoch
            .checked_add(1)
            .expect("simulated file epoch overflowed");
        let entry = self.entry_mut(client, path)?;
        entry.size = Some(size);
        entry.checksum_sha256 = Some(checksum_sha256.clone());
        entry.object = Some(ObjectRef::local(object_id.clone()));
        entry.payload = None;
        entry.epoch = next_epoch;
        self.object_store
            .stage_local_payload(client, object_id, checksum_sha256, size);
        self.record(client, "write_file", Some(path));
        self.schedule_client_event(client, "write_file");
        Ok(())
    }

    /// Function $name.
    pub fn write_sparse_range(
        &mut self,
        client: &str,
        path: &str,
        offset: u64,
        bytes: &[u8],
    ) -> Result<(), ClientRuntimeError> {
        let next_epoch = self
            .entry(client, path)?
            .epoch
            .checked_add(1)
            .expect("simulated file epoch overflowed");
        let (size, checksum_sha256, object_id) = {
            let entry = self.entry_mut(client, path)?;
            if entry.kind != LocalEntryKind::File {
                return Err(ClientRuntimeError::NotFile {
                    client: client.to_string(),
                    path: path.to_string(),
                });
            }
            let payload = entry
                .payload
                .get_or_insert_with(|| SparsePayload::new(entry.size.unwrap_or_default()));
            payload.write(offset, bytes)?;
            let size = payload.size;
            let checksum_sha256 = payload.file_checksum();
            let object_id = Self::object_id(client, &entry.logical_id, &checksum_sha256);
            entry.size = Some(size);
            entry.checksum_sha256 = Some(checksum_sha256.clone());
            entry.object = Some(ObjectRef::local(object_id.clone()));
            entry.epoch = next_epoch;
            (size, checksum_sha256, object_id)
        };
        self.object_store
            .stage_local_payload(client, object_id, checksum_sha256, size);
        self.record_with_metadata(
            client,
            "write_sparse_range",
            Some(path),
            [
                ("offset".to_string(), offset.to_string()),
                ("bytes".to_string(), bytes.len().to_string()),
                ("checksum_sha256".to_string(), payload_checksum(bytes)),
            ],
        );
        self.schedule_client_event(client, "write_sparse_range");
        Ok(())
    }

    /// Function $name.
    pub fn append_file(
        &mut self,
        client: &str,
        path: &str,
        bytes: u64,
        checksum_sha256: impl Into<String>,
    ) -> Result<(), ClientRuntimeError> {
        let current_size = self.entry(client, path)?.size.unwrap_or_default();
        self.write_file(
            client,
            path,
            current_size.saturating_add(bytes),
            checksum_sha256,
        )?;
        self.record(client, "append_file", Some(path));
        Ok(())
    }

    /// Function $name.
    pub fn truncate_file(
        &mut self,
        client: &str,
        path: &str,
        size: u64,
        checksum_sha256: impl Into<String>,
    ) -> Result<(), ClientRuntimeError> {
        self.write_file(client, path, size, checksum_sha256)?;
        self.record(client, "truncate_file", Some(path));
        Ok(())
    }

    /// Function $name.
    pub fn truncate_sparse_file(
        &mut self,
        client: &str,
        path: &str,
        size: u64,
    ) -> Result<(), ClientRuntimeError> {
        let next_epoch = self
            .entry(client, path)?
            .epoch
            .checked_add(1)
            .expect("simulated file epoch overflowed");
        let (checksum_sha256, object_id) = {
            let entry = self.entry_mut(client, path)?;
            if entry.kind != LocalEntryKind::File {
                return Err(ClientRuntimeError::NotFile {
                    client: client.to_string(),
                    path: path.to_string(),
                });
            }
            let payload = entry
                .payload
                .get_or_insert_with(|| SparsePayload::new(entry.size.unwrap_or_default()));
            payload.truncate(size)?;
            let checksum_sha256 = payload.file_checksum();
            let object_id = Self::object_id(client, &entry.logical_id, &checksum_sha256);
            entry.size = Some(size);
            entry.checksum_sha256 = Some(checksum_sha256.clone());
            entry.object = Some(ObjectRef::local(object_id.clone()));
            entry.epoch = next_epoch;
            (checksum_sha256, object_id)
        };
        self.object_store
            .stage_local_payload(client, object_id, checksum_sha256, size);
        self.record_with_metadata(
            client,
            "truncate_sparse_file",
            Some(path),
            [("size".to_string(), size.to_string())],
        );
        self.schedule_client_event(client, "truncate_sparse_file");
        Ok(())
    }

    /// Function $name.
    pub fn rename(&mut self, client: &str, from: &str, to: &str) -> Result<(), ClientRuntimeError> {
        validate_mutation_path(client, from)?;
        validate_mutation_path(client, to)?;
        let source_entry = self.entry(client, from)?;
        let is_directory = source_entry.kind == LocalEntryKind::Directory;
        if is_directory && to.starts_with(&format!("{from}/")) {
            return Err(ClientRuntimeError::InvalidPath {
                client: client.to_string(),
                path: to.to_string(),
            });
        }
        self.ensure_parent_dirs(client, to)?;
        if self.client(client)?.entries.contains_key(to) {
            return Err(ClientRuntimeError::PathExists {
                client: client.to_string(),
                path: to.to_string(),
            });
        }
        let client_state = self.client_mut(client)?;
        let entry =
            client_state
                .entries
                .remove(from)
                .ok_or_else(|| ClientRuntimeError::PathNotFound {
                    client: client.to_string(),
                    path: from.to_string(),
                })?;
        client_state.entries.insert(to.to_string(), entry);

        if is_directory {
            let from_prefix = format!("{from}/");
            let to_prefix = format!("{to}/");
            let child_paths = client_state
                .entries
                .keys()
                .filter(|path| path.starts_with(&from_prefix))
                .cloned()
                .collect::<Vec<_>>();
            for old_path in child_paths {
                if let Some(child) = client_state.entries.remove(&old_path) {
                    let suffix = &old_path[from_prefix.len()..];
                    client_state
                        .entries
                        .insert(format!("{to_prefix}{suffix}"), child);
                }
            }
        }
        self.rename_durable_entries(client, from, to)?;
        self.rename_durable_write_expectations(client, from, to)?;
        self.record(client, "rename", Some(to));
        self.schedule_client_event(client, "rename");
        Ok(())
    }

    /// Function $name.
    pub fn unlink(&mut self, client: &str, path: &str) -> Result<(), ClientRuntimeError> {
        let entry = self.entry(client, path)?;
        if entry.kind != LocalEntryKind::File {
            return Err(ClientRuntimeError::NotFile {
                client: client.to_string(),
                path: path.to_string(),
            });
        }
        let object_id = entry.object.as_ref().map(|object| object.object_id.clone());
        if let Some(object_id) = object_id {
            if !object_id.is_empty() {
                self.object_store.mark_deleted(&object_id);
            }
        }
        self.client_mut(client)?.entries.remove(path);
        self.client_mut(client)?.durable_entries.remove(path);
        self.client_mut(client)?.durable_writes.remove(path);
        self.record(client, "unlink", Some(path));
        self.schedule_client_event(client, "unlink");
        Ok(())
    }

    /// Function $name.
    pub fn publish_unlink(&mut self, client: &str, path: &str) -> Result<(), ClientRuntimeError> {
        let entry = self.entry(client, path)?;
        if entry.kind != LocalEntryKind::File {
            return Err(ClientRuntimeError::NotFile {
                client: client.to_string(),
                path: path.to_string(),
            });
        }
        let logical_id = entry.logical_id.clone();
        let epoch = entry.epoch;
        self.unlink(client, path)?;
        let event = MetadataEvent::new("file_deleted")
            .with_field("path", path)
            .with_field("logical_id", logical_id)
            .with_field("epoch", epoch.to_string());
        self.metadata_bus
            .enqueue_local(MetadataBatch::new(client, vec![event]));
        self.record(client, "publish_unlink", Some(path));
        self.scheduler.schedule_now(
            ScheduledEvent::new(SchedulerEventType::PublishAttempt)
                .with_client(client)
                .with_description(path),
        );
        Ok(())
    }

    /// Function $name.
    pub fn rmdir(&mut self, client: &str, path: &str) -> Result<(), ClientRuntimeError> {
        validate_mutation_path(client, path)?;
        let entry = self.entry(client, path)?;
        if entry.kind != LocalEntryKind::Directory {
            return Err(ClientRuntimeError::NotDirectory {
                client: client.to_string(),
                path: path.to_string(),
            });
        }
        if !self.readdir(client, path)?.is_empty() {
            return Err(ClientRuntimeError::DirectoryNotEmpty {
                client: client.to_string(),
                path: path.to_string(),
            });
        }
        self.client_mut(client)?.entries.remove(path);
        self.client_mut(client)?.durable_entries.remove(path);
        self.record(client, "rmdir", Some(path));
        self.schedule_client_event(client, "rmdir");
        Ok(())
    }

    /// Function $name.
    pub fn flush(&mut self, client: &str, path: &str) -> Result<(), ClientRuntimeError> {
        let entry = self.entry(client, path)?;
        if entry.kind != LocalEntryKind::File {
            return Err(ClientRuntimeError::NotFile {
                client: client.to_string(),
                path: path.to_string(),
            });
        }
        let expectation = DurableWriteExpectation {
            client: client.to_string(),
            path: path.to_string(),
            checksum_sha256: entry.checksum_sha256.clone(),
            size: entry.size,
        };
        let durable_entries = self.durable_entries_for_path(client, path)?;
        let client_state = self.client_mut(client)?;
        client_state
            .durable_writes
            .insert(path.to_string(), expectation);
        for (path, entry) in durable_entries {
            client_state.durable_entries.insert(path, entry);
        }
        self.record(client, "flush", Some(path));
        Ok(())
    }

    /// Function $name.
    pub fn read_file(
        &self,
        client: &str,
        path: &str,
    ) -> Result<SimulatedFileView, ClientRuntimeError> {
        let entry = self.entry(client, path)?;
        if entry.kind != LocalEntryKind::File {
            return Err(ClientRuntimeError::NotFile {
                client: client.to_string(),
                path: path.to_string(),
            });
        }
        Ok(SimulatedFileView {
            path: path.to_string(),
            checksum_sha256: entry.checksum_sha256.clone().unwrap_or_default(),
            size: entry.size.unwrap_or_default(),
        })
    }

    /// Function $name.
    pub fn read_file_range(
        &mut self,
        client: &str,
        path: &str,
        offset: u64,
        bytes: u64,
    ) -> Result<SimulatedReadRange, ClientRuntimeError> {
        let entry = self.entry(client, path)?;
        if entry.kind != LocalEntryKind::File {
            return Err(ClientRuntimeError::NotFile {
                client: client.to_string(),
                path: path.to_string(),
            });
        }
        let mut read = match &entry.payload {
            Some(payload) => payload.read(offset, bytes)?,
            None => SparsePayload::read_empty(entry.size.unwrap_or_default(), offset, bytes)?,
        };
        read.path = path.to_string();
        self.record_with_metadata(
            client,
            "read_file_range",
            Some(path),
            [
                ("offset".to_string(), offset.to_string()),
                ("bytes".to_string(), bytes.to_string()),
                ("checksum_sha256".to_string(), read.checksum_sha256.clone()),
            ],
        );
        Ok(read)
    }

    /// Function $name.
    pub fn stat(&self, client: &str, path: &str) -> Result<SimulatedStat, ClientRuntimeError> {
        let entry = self.entry(client, path)?;
        Ok(SimulatedStat {
            path: path.to_string(),
            kind: entry.kind,
            checksum_sha256: entry.checksum_sha256.clone(),
            size: entry.size,
        })
    }

    /// Function $name.
    pub fn readdir(&self, client: &str, path: &str) -> Result<Vec<String>, ClientRuntimeError> {
        let entry = self.entry(client, path)?;
        if entry.kind != LocalEntryKind::Directory {
            return Err(ClientRuntimeError::NotDirectory {
                client: client.to_string(),
                path: path.to_string(),
            });
        }
        let prefix = format!("{path}/");
        let mut names = self
            .client(client)?
            .entries
            .keys()
            .filter_map(|candidate| {
                if path.is_empty() {
                    (!candidate.is_empty()).then_some(candidate.as_str())
                } else {
                    candidate.strip_prefix(&prefix)
                }
            })
            .filter(|suffix| !suffix.is_empty() && !suffix.contains('/'))
            .map(str::to_string)
            .collect::<Vec<_>>();
        names.sort();
        Ok(names)
    }

    /// Function $name.
    pub fn schedule_upload(
        &mut self,
        client: &str,
        path: &str,
    ) -> Result<UploadTicket, ClientRuntimeError> {
        let object_id = self
            .entry(client, path)?
            .object
            .as_ref()
            .map(|object| object.object_id.clone())
            .ok_or_else(|| ClientRuntimeError::MissingObjectRef {
                client: client.to_string(),
                path: path.to_string(),
            })?;
        let ticket = self.object_store.schedule_upload(client, object_id)?;
        self.record(client, "upload_scheduled", Some(path));
        self.scheduler.schedule_now(
            ScheduledEvent::new(SchedulerEventType::UploadCompletion)
                .with_client(client)
                .with_description(path),
        );
        Ok(ticket)
    }

    /// Function $name.
    pub fn complete_upload(&mut self, upload_id: u64) -> Result<UploadOutcome, ClientRuntimeError> {
        let ticket = self
            .object_store
            .upload_ticket(upload_id)
            .ok_or(ClientRuntimeError::UnknownUpload { upload_id })?;
        let outcome = self
            .object_store
            .complete_upload(upload_id)
            .ok_or(ClientRuntimeError::UnknownUpload { upload_id })?;
        if let UploadOutcome::Completed { object_id, .. }
        | UploadOutcome::AlreadyCompleted { object_id, .. } = &outcome
        {
            self.promote_uploaded_object(&ticket.client, object_id);
        }
        Ok(outcome)
    }

    /// Function $name.
    pub fn publish_file(&mut self, client: &str, path: &str) -> Result<(), ClientRuntimeError> {
        let entry = self.entry(client, path)?;
        let object = entry
            .object
            .as_ref()
            .ok_or_else(|| ClientRuntimeError::MissingObjectRef {
                client: client.to_string(),
                path: path.to_string(),
            })?;
        if !self.object_store.has_remote_object(&object.object_id) {
            return Err(ClientRuntimeError::ObjectNotUploaded {
                object_id: object.object_id.clone(),
            });
        }
        let logical_id = entry.logical_id.clone();
        let object_id = object.object_id.clone();
        let checksum_sha256 = entry.checksum_sha256.clone().unwrap_or_default();
        let size = entry.size.unwrap_or_default();
        let epoch = entry.epoch;
        let payload = entry.payload.clone();

        self.publish_file_version(
            client,
            path,
            PublishedFileVersion {
                logical_id,
                object_id,
                checksum_sha256,
                size,
                epoch,
                payload,
            },
        )
    }

    /// Function $name.
    pub fn published_file_version(
        &self,
        client: &str,
        path: &str,
    ) -> Result<PublishedFileVersion, ClientRuntimeError> {
        let entry = self.entry(client, path)?;
        let object = entry
            .object
            .as_ref()
            .ok_or_else(|| ClientRuntimeError::MissingObjectRef {
                client: client.to_string(),
                path: path.to_string(),
            })?;
        Ok(PublishedFileVersion {
            logical_id: entry.logical_id.clone(),
            object_id: object.object_id.clone(),
            checksum_sha256: entry.checksum_sha256.clone().unwrap_or_default(),
            size: entry.size.unwrap_or_default(),
            epoch: entry.epoch,
            payload: entry.payload.clone(),
        })
    }

    /// Function $name.
    pub fn publish_file_version(
        &mut self,
        client: &str,
        path: &str,
        version: PublishedFileVersion,
    ) -> Result<(), ClientRuntimeError> {
        let current = self.entry(client, path)?;
        if current.epoch != version.epoch {
            return Err(ClientRuntimeError::StalePublish {
                client: client.to_string(),
                path: path.to_string(),
                attempted_epoch: version.epoch,
                current_epoch: current.epoch,
            });
        }
        if !self.object_store.has_remote_object(&version.object_id) {
            return Err(ClientRuntimeError::ObjectNotUploaded {
                object_id: version.object_id,
            });
        }

        self.object_store.mark_published(&version.object_id);
        self.enqueue_published_file_version(client, path, version)
    }

    /// Function $name.
    pub fn enqueue_published_file_version(
        &mut self,
        client: &str,
        path: &str,
        version: PublishedFileVersion,
    ) -> Result<(), ClientRuntimeError> {
        let event = {
            let mut event = MetadataEvent::new("file_published")
                .with_field("path", path)
                .with_field("logical_id", version.logical_id)
                .with_field("object_id", version.object_id)
                .with_field("checksum_sha256", version.checksum_sha256)
                .with_field("size", version.size.to_string())
                .with_field("epoch", version.epoch.to_string());
            if let Some(payload) = version.payload {
                event = event.with_field("sparse_payload", payload.to_metadata_json());
            }
            event
        };
        let batch = MetadataBatch::new(client, vec![event]);
        self.metadata_bus.enqueue_local(batch);
        self.record(client, "publish_file", Some(path));
        self.scheduler.schedule_now(
            ScheduledEvent::new(SchedulerEventType::PublishAttempt)
                .with_client(client)
                .with_description(path),
        );
        Ok(())
    }

    /// Function $name.
    pub fn deliver_next_metadata(
        &mut self,
        client: &str,
    ) -> Result<DeliveryOutcome, ClientRuntimeError> {
        let outcome = self.metadata_bus.deliver_next(client).ok_or_else(|| {
            ClientRuntimeError::NoPendingMetadata {
                client: client.to_string(),
            }
        })?;
        match outcome {
            DeliveryOutcome::Applied { batch_id, .. } => {
                self.apply_batch_by_id(client, batch_id)?;
            }
            DeliveryOutcome::Blocked { reason } => {
                return Err(ClientRuntimeError::MetadataBlocked {
                    client: client.to_string(),
                    reason,
                });
            }
            DeliveryOutcome::DuplicateIgnored { .. } => {}
        }
        self.record(client, "metadata_delivered", None);
        self.scheduler.schedule_now(
            ScheduledEvent::new(SchedulerEventType::MetadataDelivery).with_client(client),
        );
        Ok(outcome)
    }

    /// Function $name.
    pub fn deliver_all_metadata(&mut self, client: &str) -> Result<usize, ClientRuntimeError> {
        let mut delivered = 0;
        let pending = self.metadata_bus.pending_count_for(client);
        for _ in 0..pending {
            match self.deliver_next_metadata(client) {
                Ok(_) => delivered += 1,
                Err(ClientRuntimeError::NoPendingMetadata { .. }) => return Ok(delivered),
                Err(error) => return Err(error),
            }
        }
        Ok(delivered)
    }

    /// Function $name.
    pub fn import_metadata_history(&mut self, client: &str) -> Result<usize, ClientRuntimeError> {
        self.ensure_client(client)?;
        let enqueued = self.metadata_bus.enqueue_history_for(client, false);
        self.record_with_metadata(
            client,
            "metadata_history_imported",
            None,
            [("enqueued_batches".to_string(), enqueued.to_string())],
        );
        Ok(enqueued)
    }

    /// Function $name.
    pub fn replay_metadata_history(&mut self, client: &str) -> Result<usize, ClientRuntimeError> {
        self.ensure_client(client)?;
        let enqueued = self.metadata_bus.enqueue_history_for(client, true);
        self.record_with_metadata(
            client,
            "metadata_history_replayed",
            None,
            [("enqueued_batches".to_string(), enqueued.to_string())],
        );
        Ok(enqueued)
    }

    /// Function $name.
    pub fn set_metadata_available(&mut self, available: bool) {
        self.metadata_bus.set_metadata_outage(!available);
        self.trace.push(ClientTraceRecord {
            step: self.trace.len() as u64 + 1,
            client: None,
            kind: if available {
                "metadata_recovered".to_string()
            } else {
                "metadata_outage".to_string()
            },
            path: None,
            metadata: BTreeMap::new(),
        });
        self.scheduler
            .schedule_now(ScheduledEvent::new(SchedulerEventType::MetadataFault));
    }

    /// Function $name.
    pub fn restart_client(&mut self, client: &str) -> Result<(), ClientRuntimeError> {
        self.ensure_client(client)?;
        self.metadata_bus.disconnect_client(client);
        self.metadata_bus.reconnect_client(client);
        let durable_entries = self.client(client)?.durable_entries.clone();
        self.client_mut(client)?.entries = durable_entries;
        self.record(client, "client_restarted", None);
        self.scheduler.schedule_now(
            ScheduledEvent::new(SchedulerEventType::CrashRestart).with_client(client),
        );
        Ok(())
    }

    /// Function $name.
    pub fn logical_snapshot(&self) -> LogicalSnapshot {
        let mut snapshot = LogicalSnapshot::new(
            self.clients
                .values()
                .map(SimulatedClient::logical_snapshot)
                .collect(),
        );
        snapshot.durable_writes = self
            .clients
            .values()
            .flat_map(|client| client.durable_writes.values().cloned())
            .collect();
        snapshot.objects = self.object_store.object_lifecycle();
        snapshot
    }

    /// Function $name.
    pub fn trace(&self) -> &[ClientTraceRecord] {
        &self.trace
    }

    fn apply_batch_by_id(&mut self, client: &str, batch_id: u64) -> Result<(), ClientRuntimeError> {
        let batch = self
            .metadata_bus
            .batch(batch_id)
            .ok_or(ClientRuntimeError::MissingDeliveredBatch { batch_id })?;
        self.apply_batch(client, &batch)
    }

    fn apply_batch(
        &mut self,
        client: &str,
        batch: &MetadataBatch,
    ) -> Result<(), ClientRuntimeError> {
        for event in &batch.events {
            if event.kind == "file_published" {
                let path = field(event, "path")?;
                let logical_id = field(event, "logical_id")?;
                let object_id = field(event, "object_id")?;
                let checksum_sha256 = field(event, "checksum_sha256")?;
                let size = field(event, "size")?.parse().map_err(|_| {
                    ClientRuntimeError::InvalidMetadataField {
                        field: "size".to_string(),
                    }
                })?;
                let epoch = optional_field(event, "epoch")
                    .map(|epoch| {
                        epoch
                            .parse()
                            .map_err(|_| ClientRuntimeError::InvalidMetadataField {
                                field: "epoch".to_string(),
                            })
                    })
                    .transpose()?
                    .unwrap_or_default();
                let payload = optional_field(event, "sparse_payload")
                    .map(SparsePayload::from_metadata_json)
                    .transpose()?;
                let is_stale_same_logical_file = self
                    .client(client)?
                    .entries
                    .get(path)
                    .is_some_and(|current| {
                        current.logical_id == logical_id && current.epoch > epoch
                    });
                if is_stale_same_logical_file {
                    continue;
                }
                self.ensure_parent_dirs(client, path)?;
                self.client_mut(client)?.entries.insert(
                    path.to_string(),
                    LocalEntry::file(
                        logical_id.to_string(),
                        Some(size),
                        Some(checksum_sha256.to_string()),
                        Some(ObjectRef::remote(object_id)),
                        epoch,
                        payload,
                    ),
                );
            } else if event.kind == "file_deleted" {
                let path = field(event, "path")?;
                let logical_id = field(event, "logical_id")?;
                let epoch = optional_field(event, "epoch")
                    .map(|epoch| {
                        epoch
                            .parse()
                            .map_err(|_| ClientRuntimeError::InvalidMetadataField {
                                field: "epoch".to_string(),
                            })
                    })
                    .transpose()?
                    .unwrap_or_default();
                let remove = self
                    .client(client)?
                    .entries
                    .get(path)
                    .is_some_and(|current| {
                        current.logical_id == logical_id && current.epoch <= epoch
                    });
                if remove {
                    let client_state = self.client_mut(client)?;
                    client_state.entries.remove(path);
                    client_state.durable_entries.remove(path);
                    client_state.durable_writes.remove(path);
                }
            }
        }
        Ok(())
    }

    fn promote_uploaded_object(&mut self, client: &str, object_id: &str) {
        if let Some(client) = self.clients.get_mut(client) {
            for entry in client.entries.values_mut() {
                if entry
                    .object
                    .as_ref()
                    .is_some_and(|object| object.object_id == object_id)
                {
                    entry.object = Some(ObjectRef::remote(object_id.to_string()));
                }
            }
        }
    }

    fn ensure_parent_dirs(&mut self, client: &str, path: &str) -> Result<(), ClientRuntimeError> {
        self.ensure_client(client)?;
        let mut current = String::new();
        for component in path
            .split('/')
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .skip(1)
            .rev()
        {
            if component.is_empty() {
                continue;
            }
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(component);
            if let Some(entry) = self.client(client)?.entries.get(&current) {
                if entry.kind != LocalEntryKind::Directory {
                    return Err(ClientRuntimeError::NotDirectory {
                        client: client.to_string(),
                        path: current.clone(),
                    });
                }
            } else {
                let logical_id = self.directory_logical_id(&current);
                self.client_mut(client)?
                    .entries
                    .insert(current.clone(), LocalEntry::directory(logical_id));
            }
        }
        Ok(())
    }

    fn entry(&self, client: &str, path: &str) -> Result<&LocalEntry, ClientRuntimeError> {
        self.client(client)?
            .entries
            .get(path)
            .ok_or_else(|| ClientRuntimeError::PathNotFound {
                client: client.to_string(),
                path: path.to_string(),
            })
    }

    fn entry_mut(
        &mut self,
        client: &str,
        path: &str,
    ) -> Result<&mut LocalEntry, ClientRuntimeError> {
        self.client_mut(client)?
            .entries
            .get_mut(path)
            .ok_or_else(|| ClientRuntimeError::PathNotFound {
                client: client.to_string(),
                path: path.to_string(),
            })
    }

    fn client(&self, client: &str) -> Result<&SimulatedClient, ClientRuntimeError> {
        self.clients
            .get(client)
            .ok_or_else(|| ClientRuntimeError::UnknownClient {
                client: client.to_string(),
            })
    }

    fn client_mut(&mut self, client: &str) -> Result<&mut SimulatedClient, ClientRuntimeError> {
        self.clients
            .get_mut(client)
            .ok_or_else(|| ClientRuntimeError::UnknownClient {
                client: client.to_string(),
            })
    }

    fn ensure_client(&self, client: &str) -> Result<(), ClientRuntimeError> {
        self.client(client).map(|_| ())
    }

    fn durable_entries_for_path(
        &self,
        client: &str,
        path: &str,
    ) -> Result<BTreeMap<String, LocalEntry>, ClientRuntimeError> {
        let client_state = self.client(client)?;
        let mut durable = BTreeMap::new();
        durable.insert(
            String::new(),
            client_state.entries.get("").cloned().ok_or_else(|| {
                ClientRuntimeError::PathNotFound {
                    client: client.to_string(),
                    path: String::new(),
                }
            })?,
        );
        let mut current = String::new();
        for component in path
            .split('/')
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .skip(1)
            .rev()
        {
            if component.is_empty() {
                continue;
            }
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(component);
            let entry = client_state.entries.get(&current).cloned().ok_or_else(|| {
                ClientRuntimeError::PathNotFound {
                    client: client.to_string(),
                    path: current.clone(),
                }
            })?;
            durable.insert(current.clone(), entry);
        }
        durable.insert(path.to_string(), self.entry(client, path)?.clone());
        Ok(durable)
    }

    fn next_file_logical_id(&mut self) -> String {
        self.next_logical_id += 1;
        format!("inode-{}", self.next_logical_id)
    }

    fn directory_logical_id(&self, path: &str) -> String {
        if path.is_empty() {
            "root".to_string()
        } else {
            format!("dir:{path}")
        }
    }

    fn object_id(client: &str, logical_id: &str, checksum_sha256: &str) -> String {
        format!("{client}:{logical_id}:{checksum_sha256}")
    }

    fn schedule_client_event(&mut self, client: &str, description: &str) {
        self.scheduler.schedule_now(
            ScheduledEvent::new(SchedulerEventType::ClientOperation)
                .with_client(client)
                .with_description(description),
        );
    }

    fn persist_current_entry(
        &mut self,
        client: &str,
        path: &str,
    ) -> Result<(), ClientRuntimeError> {
        let entry = self.entry(client, path)?.clone();
        self.client_mut(client)?
            .durable_entries
            .insert(path.to_string(), entry);
        Ok(())
    }

    fn rename_durable_entries(
        &mut self,
        client: &str,
        from: &str,
        to: &str,
    ) -> Result<(), ClientRuntimeError> {
        let from_prefix = format!("{from}/");
        let to_prefix = format!("{to}/");
        let paths = self
            .client(client)?
            .durable_entries
            .range(from.to_string()..)
            .map(|(path, _)| path)
            .take_while(|path| *path == from || path.starts_with(&from_prefix))
            .cloned()
            .collect::<Vec<_>>();
        let client_state = self.client_mut(client)?;
        for old_path in paths {
            if let Some(entry) = client_state.durable_entries.remove(&old_path) {
                let new_path = if old_path == from {
                    to.to_string()
                } else {
                    let suffix = &old_path[from_prefix.len()..];
                    format!("{to_prefix}{suffix}")
                };
                client_state.durable_entries.insert(new_path, entry);
            }
        }
        Ok(())
    }

    fn rename_durable_write_expectations(
        &mut self,
        client: &str,
        from: &str,
        to: &str,
    ) -> Result<(), ClientRuntimeError> {
        let from_prefix = format!("{from}/");
        let to_prefix = format!("{to}/");
        let paths = self
            .client(client)?
            .durable_writes
            .range(from.to_string()..)
            .map(|(path, _)| path)
            .take_while(|path| *path == from || path.starts_with(&from_prefix))
            .cloned()
            .collect::<Vec<_>>();
        let client_state = self.client_mut(client)?;
        for old_path in paths {
            if let Some(mut expectation) = client_state.durable_writes.remove(&old_path) {
                let new_path = if old_path == from {
                    to.to_string()
                } else {
                    let suffix = &old_path[from_prefix.len()..];
                    format!("{to_prefix}{suffix}")
                };
                expectation.path = new_path.clone();
                client_state.durable_writes.insert(new_path, expectation);
            }
        }
        Ok(())
    }

    fn record(&mut self, client: impl Into<String>, kind: impl Into<String>, path: Option<&str>) {
        self.record_with_metadata(client, kind, path, std::iter::empty::<(String, String)>());
    }

    fn record_with_metadata(
        &mut self,
        client: impl Into<String>,
        kind: impl Into<String>,
        path: Option<&str>,
        metadata: impl IntoIterator<Item = (String, String)>,
    ) {
        self.trace.push(ClientTraceRecord {
            step: self.trace.len() as u64 + 1,
            client: Some(client.into()),
            kind: kind.into(),
            path: path.map(str::to_string),
            metadata: metadata.into_iter().collect(),
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulatedFileView {
    /// Field $name.
    pub path: String,
    /// Field $name.
    pub checksum_sha256: String,
    /// Field $name.
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulatedReadRange {
    /// Field $name.
    pub path: String,
    /// Field $name.
    pub offset: u64,
    /// Field $name.
    pub bytes: Vec<u8>,
    /// Field $name.
    pub checksum_sha256: String,
    /// Field $name.
    pub holes: Vec<SimulatedRange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulatedRange {
    /// Field $name.
    pub offset: u64,
    /// Field $name.
    pub len: u64,
}

impl From<(u64, u64)> for SimulatedRange {
    fn from((offset, len): (u64, u64)) -> Self {
        Self { offset, len }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SparsePayload {
    size: u64,
    extents: BTreeMap<u64, Vec<u8>>,
}

impl SparsePayload {
    fn new(size: u64) -> Self {
        Self {
            size,
            extents: BTreeMap::new(),
        }
    }

    fn write(&mut self, offset: u64, bytes: &[u8]) -> Result<(), ClientRuntimeError> {
        if bytes.is_empty() {
            return Ok(());
        }
        let end = Self::extent_end(offset, bytes.len())?;
        let mut right = self.replace_extents_in_range(offset, end)?;
        self.extents.insert(offset, bytes.to_vec());
        self.extents.append(&mut right);
        self.size = self.size.max(end);
        Ok(())
    }

    fn replace_extents_in_range(
        &mut self,
        offset: u64,
        end: u64,
    ) -> Result<BTreeMap<u64, Vec<u8>>, ClientRuntimeError> {
        let mut overlapping = self.extents.split_off(&offset);
        self.preserve_left_overlap(offset, end, &mut overlapping)?;
        let mut right = overlapping.split_off(&end);
        Self::preserve_right_overlap(end, &overlapping, &mut right)?;
        Ok(right)
    }

    fn preserve_left_overlap(
        &mut self,
        offset: u64,
        end: u64,
        overlapping: &mut BTreeMap<u64, Vec<u8>>,
    ) -> Result<(), ClientRuntimeError> {
        if let Some((extent_offset, extent_bytes)) = self
            .extents
            .last_key_value()
            .map(|(&extent_offset, extent_bytes)| (extent_offset, extent_bytes.clone()))
        {
            let extent_end = Self::extent_end(extent_offset, extent_bytes.len())?;
            if extent_end <= offset {
                return Ok(());
            }

            let keep_len = (offset - extent_offset) as usize;
            self.extents
                .insert(extent_offset, extent_bytes[..keep_len].to_vec());
            if extent_end > end {
                let keep_start = (end - extent_offset) as usize;
                debug_assert!(
                    !overlapping.contains_key(&end),
                    "sparse extents must not overlap at the replacement end"
                );
                overlapping.insert(end, extent_bytes[keep_start..].to_vec());
            }
        }
        Ok(())
    }

    fn preserve_right_overlap(
        end: u64,
        overlapping: &BTreeMap<u64, Vec<u8>>,
        right: &mut BTreeMap<u64, Vec<u8>>,
    ) -> Result<(), ClientRuntimeError> {
        for (&extent_offset, extent_bytes) in overlapping {
            let extent_end = Self::extent_end(extent_offset, extent_bytes.len())?;
            if extent_end > end {
                let keep_start = (end - extent_offset) as usize;
                debug_assert!(
                    !right.contains_key(&end),
                    "sparse extents must not overlap at the replacement end"
                );
                right.insert(end, extent_bytes[keep_start..].to_vec());
                break;
            }
        }
        Ok(())
    }

    fn extent_end(offset: u64, len: usize) -> Result<u64, ClientRuntimeError> {
        offset
            .checked_add(len as u64)
            .ok_or(ClientRuntimeError::SparseRangeOverflow)
    }

    fn truncate(&mut self, size: u64) -> Result<(), ClientRuntimeError> {
        self.size = size;
        let _dropped_tail = self.extents.split_off(&size);
        if let Some((extent_offset, extent_bytes)) = self
            .extents
            .range(..size)
            .next_back()
            .map(|(&extent_offset, extent_bytes)| (extent_offset, extent_bytes.clone()))
        {
            let extent_end = Self::extent_end(extent_offset, extent_bytes.len())?;
            if extent_end > size {
                let keep_len = (size - extent_offset) as usize;
                self.extents
                    .insert(extent_offset, extent_bytes[..keep_len].to_vec());
            }
        }
        Ok(())
    }

    fn overlapping_extent_offsets(
        &self,
        offset: u64,
        end: u64,
    ) -> Result<Vec<u64>, ClientRuntimeError> {
        let mut offsets = Vec::new();
        if let Some((&extent_offset, extent_bytes)) = self.extents.range(..offset).next_back() {
            let extent_end = Self::extent_end(extent_offset, extent_bytes.len())?;
            if extent_end > offset {
                offsets.push(extent_offset);
            }
        }
        offsets.extend(
            self.extents
                .range(offset..end)
                .map(|(&extent_offset, _)| extent_offset),
        );
        Ok(offsets)
    }

    fn overlapping_extents(
        &self,
        offset: u64,
        end: u64,
    ) -> Result<Vec<(u64, &[u8])>, ClientRuntimeError> {
        self.overlapping_extent_offsets(offset, end)?
            .into_iter()
            .map(|extent_offset| {
                let extent_bytes = self.extents.get(&extent_offset).ok_or(
                    ClientRuntimeError::MissingSparseExtent {
                        offset: extent_offset,
                    },
                )?;
                Ok((extent_offset, extent_bytes.as_slice()))
            })
            .collect()
    }

    fn read(&self, offset: u64, bytes: u64) -> Result<SimulatedReadRange, ClientRuntimeError> {
        self.read_with_extents(offset, bytes)
    }

    fn read_empty(
        size: u64,
        offset: u64,
        bytes: u64,
    ) -> Result<SimulatedReadRange, ClientRuntimeError> {
        Self::new(size).read_with_extents(offset, bytes)
    }

    fn read_with_extents(
        &self,
        offset: u64,
        bytes: u64,
    ) -> Result<SimulatedReadRange, ClientRuntimeError> {
        let requested_end = offset
            .checked_add(bytes)
            .ok_or(ClientRuntimeError::SparseRangeOverflow)?;
        let end = requested_end.min(self.size);
        if offset >= end {
            return Ok(SimulatedReadRange {
                path: String::new(),
                offset,
                bytes: Vec::new(),
                checksum_sha256: payload_checksum(&[]),
                holes: Vec::new(),
            });
        }
        let mut materialized = vec![0; (end - offset) as usize];
        for (extent_offset, extent_bytes) in self.overlapping_extents(offset, end)? {
            let extent_end = Self::extent_end(extent_offset, extent_bytes.len())?;
            let copy_start = extent_offset.max(offset);
            let copy_end = extent_end.min(end);
            let src_start = (copy_start - extent_offset) as usize;
            let dst_start = (copy_start - offset) as usize;
            let len = (copy_end - copy_start) as usize;
            materialized[dst_start..dst_start + len]
                .copy_from_slice(&extent_bytes[src_start..src_start + len]);
        }
        Ok(SimulatedReadRange {
            path: String::new(),
            offset,
            checksum_sha256: payload_checksum(&materialized),
            holes: self.holes_in(offset, end)?,
            bytes: materialized,
        })
    }

    fn holes_in(&self, offset: u64, end: u64) -> Result<Vec<SimulatedRange>, ClientRuntimeError> {
        let mut holes = Vec::new();
        let mut cursor = offset;
        for (extent_offset, extent_bytes) in self.overlapping_extents(offset, end)? {
            let extent_end = Self::extent_end(extent_offset, extent_bytes.len())?;
            let data_start = extent_offset.max(offset);
            if cursor < data_start {
                holes.push((cursor, data_start - cursor).into());
            }
            cursor = cursor.max(extent_end.min(end));
        }
        if cursor < end {
            holes.push((cursor, end - cursor).into());
        }
        Ok(holes)
    }

    fn file_checksum(&self) -> String {
        let mut checksum = PayloadChecksum::new();
        let mut cursor = 0;
        for (&offset, bytes) in &self.extents {
            if cursor < offset {
                checksum.update_zeroes(offset - cursor);
            }
            checksum.update(bytes);
            cursor = offset.saturating_add(bytes.len() as u64).min(self.size);
        }
        if cursor < self.size {
            checksum.update_zeroes(self.size - cursor);
        }
        checksum.finish()
    }

    fn logical_byte_ranges(&self) -> Vec<LogicalByteRange> {
        let mut ranges = Vec::new();
        let mut cursor = 0;
        for (&offset, bytes) in &self.extents {
            if cursor < offset {
                ranges.push(LogicalByteRange {
                    offset: cursor,
                    len: offset - cursor,
                    kind: LogicalByteRangeKind::Hole,
                    checksum_sha256: None,
                });
            }
            let len = bytes.len() as u64;
            ranges.push(LogicalByteRange {
                offset,
                len,
                kind: LogicalByteRangeKind::Data,
                checksum_sha256: Some(payload_checksum(bytes)),
            });
            cursor = offset.saturating_add(len).min(self.size);
        }
        if cursor < self.size {
            ranges.push(LogicalByteRange {
                offset: cursor,
                len: self.size - cursor,
                kind: LogicalByteRangeKind::Hole,
                checksum_sha256: None,
            });
        }
        ranges
    }

    fn to_metadata_json(&self) -> String {
        serde_json::to_string(self).expect("sparse payload metadata serializes")
    }

    fn from_metadata_json(value: &str) -> Result<Self, ClientRuntimeError> {
        serde_json::from_str(value).map_err(|_| ClientRuntimeError::InvalidMetadataField {
            field: "sparse_payload".to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulatedStat {
    /// Field $name.
    pub path: String,
    /// Field $name.
    pub kind: LocalEntryKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub checksum_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct PublishedFileVersion {
    /// Field $name.
    pub logical_id: String,
    /// Field $name.
    pub object_id: String,
    /// Field $name.
    pub checksum_sha256: String,
    /// Field $name.
    pub size: u64,
    /// Field $name.
    pub epoch: u64,
    /// Field $name.
    pub payload: Option<SparsePayload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct ClientTraceRecord {
    /// Field $name.
    pub step: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub client: Option<String>,
    /// Field $name.
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    /// Field $name.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
/// Enum $name.
pub enum ClientRuntimeError {
    #[error("unknown simulated client {client}")]
    /// Variant $name.
    UnknownClient {
        /// The client value.
        client: String,
    },
    #[error("path {path} not found for simulated client {client}")]
    /// Field $name.
    PathNotFound {
        /// The client value.
        client: String,
        /// The path value.
        path: String,
    },
    #[error("path {path} already exists for simulated client {client}")]
    /// Field $name.
    PathExists {
        /// The client value.
        client: String,
        /// The path value.
        path: String,
    },
    #[error("path {path} is invalid for simulated client {client}")]
    /// Variant $name.
    InvalidPath {
        /// The client value.
        client: String,
        /// The path value.
        path: String,
    },
    #[error("path {path} for simulated client {client} is not a file")]
    /// Variant $name.
    NotFile {
        /// The client value.
        client: String,
        /// The path value.
        path: String,
    },
    #[error("path {path} for simulated client {client} is not a directory")]
    /// Field $name.
    NotDirectory {
        /// The client value.
        client: String,
        /// The path value.
        path: String,
    },
    #[error("directory {path} for simulated client {client} is not empty")]
    /// Variant $name.
    DirectoryNotEmpty {
        /// The client value.
        client: String,
        /// The path value.
        path: String,
    },
    #[error("path {path} for simulated client {client} has no object reference")]
    /// Field $name.
    MissingObjectRef {
        /// The client value.
        client: String,
        /// The path value.
        path: String,
    },
    #[error("object {object_id} has not been uploaded")]
    /// Field $name.
    ObjectNotUploaded {
        /// The object_id value.
        object_id: String,
    },
    #[error(
        "stale publish for simulated client {client} path {path}: attempted epoch {attempted_epoch}, current epoch {current_epoch}"
    )]
    /// Variant $name.
    StalePublish {
        /// Field $name.
        client: String,
        /// Field $name.
        path: String,
        /// Field $name.
        attempted_epoch: u64,
        /// Field $name.
        current_epoch: u64,
    },
    #[error("unknown upload {upload_id}")]
    /// Variant $name.
    UnknownUpload {
        /// The upload_id value.
        upload_id: u64,
    },
    #[error("no pending metadata for simulated client {client}")]
    /// Field $name.
    NoPendingMetadata {
        /// The client value.
        client: String,
    },
    #[error("metadata delivery to simulated client {client} is blocked: {reason:?}")]
    /// Variant $name.
    MetadataBlocked {
        /// Field $name.
        client: String,
        /// Field $name.
        reason: DeliveryFault,
    },
    #[error("missing delivered metadata batch {batch_id}")]
    /// Variant $name.
    MissingDeliveredBatch {
        /// The batch_id value.
        batch_id: u64,
    },
    #[error("metadata event is missing field {field}")]
    /// Variant $name.
    MissingMetadataField {
        /// The field value.
        field: String,
    },
    #[error("metadata event has invalid field {field}")]
    /// Variant $name.
    InvalidMetadataField {
        /// The field value.
        field: String,
    },
    #[error("simulated sparse byte range overflows u64")]
    /// Variant $name.
    SparseRangeOverflow,
    #[error("simulated sparse extent at offset {offset} disappeared during range iteration")]
    /// Variant $name.
    MissingSparseExtent {
        /// The offset value.
        offset: u64,
    },
    #[error("object store error: {0:?}")]
    /// Variant $name.
    ObjectStore(ObjectStoreFault),
}

impl From<ObjectStoreFault> for ClientRuntimeError {
    fn from(error: ObjectStoreFault) -> Self {
        Self::ObjectStore(error)
    }
}

#[derive(Debug)]
struct SimulatedClient {
    client: String,
    entries: BTreeMap<String, LocalEntry>,
    durable_entries: BTreeMap<String, LocalEntry>,
    durable_writes: BTreeMap<String, DurableWriteExpectation>,
}

impl SimulatedClient {
    fn new(client: String) -> Self {
        let mut entries = BTreeMap::new();
        entries.insert(String::new(), LocalEntry::directory("root".to_string()));
        Self {
            client,
            durable_entries: entries.clone(),
            entries,
            durable_writes: BTreeMap::new(),
        }
    }

    fn logical_snapshot(&self) -> ClientLogicalSnapshot {
        ClientLogicalSnapshot {
            client: self.client.clone(),
            entries: self
                .entries
                .iter()
                .map(|(path, entry)| entry.logical_entry(path))
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
struct LocalEntry {
    kind: LocalEntryKind,
    logical_id: String,
    object: Option<ObjectRef>,
    checksum_sha256: Option<String>,
    size: Option<u64>,
    payload: Option<SparsePayload>,
    epoch: u64,
}

impl LocalEntry {
    fn directory(logical_id: String) -> Self {
        Self {
            kind: LocalEntryKind::Directory,
            logical_id,
            object: None,
            checksum_sha256: None,
            size: None,
            payload: None,
            epoch: 0,
        }
    }

    fn file(
        logical_id: String,
        size: Option<u64>,
        checksum_sha256: Option<String>,
        object: Option<ObjectRef>,
        epoch: u64,
        payload: Option<SparsePayload>,
    ) -> Self {
        Self {
            kind: LocalEntryKind::File,
            logical_id,
            object,
            checksum_sha256,
            size,
            payload,
            epoch,
        }
    }

    fn logical_entry(&self, path: &str) -> LogicalEntry {
        match self.kind {
            LocalEntryKind::Directory => LogicalEntry::directory(path, self.logical_id.clone()),
            LocalEntryKind::File => {
                let mut entry = LogicalEntry::file(
                    path,
                    self.logical_id.clone(),
                    self.object
                        .as_ref()
                        .filter(|object| !object.object_id.is_empty())
                        .cloned(),
                    self.checksum_sha256.clone(),
                    self.size,
                );
                if let Some(payload) = &self.payload {
                    entry.byte_ranges = payload.logical_byte_ranges();
                }
                entry
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Enum $name.
pub enum LocalEntryKind {
    /// Variant $name.
    Directory,
    /// Variant $name.
    File,
}

fn field<'a>(event: &'a MetadataEvent, name: &str) -> Result<&'a str, ClientRuntimeError> {
    event.fields.get(name).map(String::as_str).ok_or_else(|| {
        ClientRuntimeError::MissingMetadataField {
            field: name.to_string(),
        }
    })
}

fn optional_field<'a>(event: &'a MetadataEvent, name: &str) -> Option<&'a str> {
    event.fields.get(name).map(String::as_str)
}

fn payload_checksum(bytes: &[u8]) -> String {
    let mut checksum = PayloadChecksum::new();
    checksum.update(bytes);
    checksum.finish()
}

struct PayloadChecksum {
    hash: u64,
}

impl PayloadChecksum {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    fn new() -> Self {
        Self {
            hash: Self::OFFSET_BASIS,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.hash ^= u64::from(*byte);
            self.hash = self.hash.wrapping_mul(Self::PRIME);
        }
    }

    fn update_zeroes(&mut self, len: u64) {
        self.hash = self.hash.wrapping_mul(wrapping_pow(Self::PRIME, len));
    }

    fn finish(self) -> String {
        format!("simulated-fnv64-{:016x}", self.hash)
    }
}

fn wrapping_pow(mut base: u64, mut exp: u64) -> u64 {
    let mut result = 1_u64;
    while exp > 0 {
        if exp & 1 == 1 {
            result = result.wrapping_mul(base);
        }
        base = base.wrapping_mul(base);
        exp >>= 1;
    }
    result
}

fn validate_mutation_path(client: &str, path: &str) -> Result<(), ClientRuntimeError> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(ClientRuntimeError::InvalidPath {
            client: client.to_string(),
            path: path.to_string(),
        });
    }
    Ok(())
}
