//! Simulated logical multi-client runtime for deterministic scenario tests.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::invariant::{
    ClientLogicalSnapshot, DurableWriteExpectation, LogicalEntry, LogicalSnapshot, ObjectRef,
};
use crate::metadata_bus::{
    DeliveryFault, DeliveryOutcome, MetadataBatch, MetadataBus, MetadataEvent,
};
use crate::object_store::{ObjectStoreFault, SimulatedObjectStore, UploadOutcome, UploadTicket};
use crate::scheduler::{ScheduledEvent, SchedulerEventType, SimulationScheduler};

#[derive(Debug)]
pub struct SimulatedRuntime {
    next_logical_id: u64,
    clients: BTreeMap<String, SimulatedClient>,
    metadata_bus: MetadataBus,
    object_store: SimulatedObjectStore,
    scheduler: SimulationScheduler,
    trace: Vec<ClientTraceRecord>,
}

impl SimulatedRuntime {
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

    pub fn add_client(&mut self, client: impl Into<String>) {
        let client = client.into();
        self.clients
            .entry(client.clone())
            .or_insert_with(|| SimulatedClient::new(client.clone()));
        self.metadata_bus.connect_client(client.clone());
        self.record(client, "client_connected", None);
    }

    pub fn create_file(&mut self, client: &str, path: &str) -> Result<(), ClientRuntimeError> {
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
            LocalEntry::file(logical_id, None, None, Some(ObjectRef::local("")), 0),
        );
        self.record(client, "create_file", Some(path));
        self.schedule_client_event(client, "create_file");
        Ok(())
    }

    pub fn mkdir(&mut self, client: &str, path: &str) -> Result<(), ClientRuntimeError> {
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

    pub fn write_file(
        &mut self,
        client: &str,
        path: &str,
        size: u64,
        checksum_sha256: impl Into<String>,
    ) -> Result<(), ClientRuntimeError> {
        let checksum_sha256 = checksum_sha256.into();
        let object_id = self.object_id(client, path, &checksum_sha256);
        let next_epoch = self
            .entry(client, path)?
            .epoch
            .checked_add(1)
            .expect("simulated file epoch overflowed");
        let entry = self.entry_mut(client, path)?;
        entry.size = Some(size);
        entry.checksum_sha256 = Some(checksum_sha256.clone());
        entry.object = Some(ObjectRef::local(object_id.clone()));
        entry.epoch = next_epoch;
        self.object_store
            .stage_local_payload(client, object_id, checksum_sha256, size);
        self.record(client, "write_file", Some(path));
        self.schedule_client_event(client, "write_file");
        Ok(())
    }

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

    pub fn rename(&mut self, client: &str, from: &str, to: &str) -> Result<(), ClientRuntimeError> {
        validate_mutation_path(client, from)?;
        validate_mutation_path(client, to)?;
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
        let is_directory = entry.kind == LocalEntryKind::Directory;
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
        self.record(client, "rename", Some(to));
        self.schedule_client_event(client, "rename");
        Ok(())
    }

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
        self.record(client, "unlink", Some(path));
        self.schedule_client_event(client, "unlink");
        Ok(())
    }

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

    pub fn stat(&self, client: &str, path: &str) -> Result<SimulatedStat, ClientRuntimeError> {
        let entry = self.entry(client, path)?;
        Ok(SimulatedStat {
            path: path.to_string(),
            kind: entry.kind,
            checksum_sha256: entry.checksum_sha256.clone(),
            size: entry.size,
        })
    }

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

        self.publish_file_version(
            client,
            path,
            PublishedFileVersion {
                logical_id,
                object_id,
                checksum_sha256,
                size,
                epoch,
            },
        )
    }

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
        })
    }

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

    pub fn enqueue_published_file_version(
        &mut self,
        client: &str,
        path: &str,
        version: PublishedFileVersion,
    ) -> Result<(), ClientRuntimeError> {
        let batch = MetadataBatch::new(
            client,
            vec![
                MetadataEvent::new("file_published")
                    .with_field("path", path)
                    .with_field("logical_id", version.logical_id)
                    .with_field("object_id", version.object_id)
                    .with_field("checksum_sha256", version.checksum_sha256)
                    .with_field("size", version.size.to_string())
                    .with_field("epoch", version.epoch.to_string()),
            ],
        );
        self.metadata_bus.enqueue_local(batch);
        self.record(client, "publish_file", Some(path));
        self.scheduler.schedule_now(
            ScheduledEvent::new(SchedulerEventType::PublishAttempt)
                .with_client(client)
                .with_description(path),
        );
        Ok(())
    }

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
        });
        self.scheduler
            .schedule_now(ScheduledEvent::new(SchedulerEventType::MetadataFault));
    }

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
                    ),
                );
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

    fn object_id(&self, client: &str, path: &str, checksum_sha256: &str) -> String {
        format!("{client}:{path}:{checksum_sha256}")
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
            .keys()
            .filter(|path| *path == from || path.starts_with(&from_prefix))
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

    fn record(&mut self, client: impl Into<String>, kind: impl Into<String>, path: Option<&str>) {
        self.trace.push(ClientTraceRecord {
            step: self.trace.len() as u64 + 1,
            client: Some(client.into()),
            kind: kind.into(),
            path: path.map(str::to_string),
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulatedFileView {
    pub path: String,
    pub checksum_sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulatedStat {
    pub path: String,
    pub kind: LocalEntryKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedFileVersion {
    pub logical_id: String,
    pub object_id: String,
    pub checksum_sha256: String,
    pub size: u64,
    pub epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTraceRecord {
    pub step: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ClientRuntimeError {
    #[error("unknown simulated client {client}")]
    UnknownClient { client: String },
    #[error("path {path} not found for simulated client {client}")]
    PathNotFound { client: String, path: String },
    #[error("path {path} already exists for simulated client {client}")]
    PathExists { client: String, path: String },
    #[error("path {path} is invalid for simulated client {client}")]
    InvalidPath { client: String, path: String },
    #[error("path {path} for simulated client {client} is not a file")]
    NotFile { client: String, path: String },
    #[error("path {path} for simulated client {client} is not a directory")]
    NotDirectory { client: String, path: String },
    #[error("directory {path} for simulated client {client} is not empty")]
    DirectoryNotEmpty { client: String, path: String },
    #[error("path {path} for simulated client {client} has no object reference")]
    MissingObjectRef { client: String, path: String },
    #[error("object {object_id} has not been uploaded")]
    ObjectNotUploaded { object_id: String },
    #[error(
        "stale publish for simulated client {client} path {path}: attempted epoch {attempted_epoch}, current epoch {current_epoch}"
    )]
    StalePublish {
        client: String,
        path: String,
        attempted_epoch: u64,
        current_epoch: u64,
    },
    #[error("unknown upload {upload_id}")]
    UnknownUpload { upload_id: u64 },
    #[error("no pending metadata for simulated client {client}")]
    NoPendingMetadata { client: String },
    #[error("metadata delivery to simulated client {client} is blocked: {reason:?}")]
    MetadataBlocked {
        client: String,
        reason: DeliveryFault,
    },
    #[error("missing delivered metadata batch {batch_id}")]
    MissingDeliveredBatch { batch_id: u64 },
    #[error("metadata event is missing field {field}")]
    MissingMetadataField { field: String },
    #[error("metadata event has invalid field {field}")]
    InvalidMetadataField { field: String },
    #[error("object store error: {0:?}")]
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
            epoch: 0,
        }
    }

    fn file(
        logical_id: String,
        size: Option<u64>,
        checksum_sha256: Option<String>,
        object: Option<ObjectRef>,
        epoch: u64,
    ) -> Self {
        Self {
            kind: LocalEntryKind::File,
            logical_id,
            object,
            checksum_sha256,
            size,
            epoch,
        }
    }

    fn logical_entry(&self, path: &str) -> LogicalEntry {
        match self.kind {
            LocalEntryKind::Directory => LogicalEntry::directory(path, self.logical_id.clone()),
            LocalEntryKind::File => LogicalEntry::file(
                path,
                self.logical_id.clone(),
                self.object
                    .as_ref()
                    .filter(|object| !object.object_id.is_empty())
                    .cloned(),
                self.checksum_sha256.clone(),
                self.size,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalEntryKind {
    Directory,
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

fn validate_mutation_path(client: &str, path: &str) -> Result<(), ClientRuntimeError> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path.split('/').any(|component| component == "..")
    {
        return Err(ClientRuntimeError::InvalidPath {
            client: client.to_string(),
            path: path.to_string(),
        });
    }
    Ok(())
}
