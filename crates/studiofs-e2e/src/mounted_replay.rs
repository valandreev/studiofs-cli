//! Replay shared logical operation records against real mounted clients.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek as _, SeekFrom, Write as _};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use studiofs_scenario::{OperationOutcome, ScenarioOperation, ScenarioOperationRecord};

use crate::client::{
    ClientProcessExecutor, ClientSpec, MultiClientController, SystemClientProcessExecutor,
};
use crate::infra::{
    CommandExecutor, ComposeServiceControl, POSTGRES_SERVICE_NAME, RUSTFS_SERVICE_NAME,
};
use crate::scenario::ScenarioError;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountedReplayReport {
    pub applied: Vec<MountedReplayAppliedRecord>,
    pub skipped: Vec<MountedReplaySkippedRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountedReplayAppliedRecord {
    pub sequence: u64,
    pub client: Option<String>,
    pub operation: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountedReplaySkippedRecord {
    pub sequence: u64,
    pub operation: String,
    pub reason: String,
}

pub trait MountedReplayDriver {
    fn clients(&self) -> Vec<ClientSpec>;

    fn crash_client(&mut self, client: &str) -> Result<(), ScenarioError> {
        Err(ScenarioError::Io(std::io::Error::other(format!(
            "mounted replay driver does not support client crash for {client}"
        ))))
    }

    fn restart_client(&mut self, client: &str) -> Result<(), ScenarioError> {
        Err(ScenarioError::Io(std::io::Error::other(format!(
            "mounted replay driver does not support client restart for {client}"
        ))))
    }

    fn postgres_outage(&mut self) -> Result<(), ScenarioError> {
        Err(ScenarioError::Io(std::io::Error::other(
            "mounted replay driver does not support PostgreSQL outage",
        )))
    }

    fn postgres_recovery(&mut self) -> Result<(), ScenarioError> {
        Err(ScenarioError::Io(std::io::Error::other(
            "mounted replay driver does not support PostgreSQL recovery",
        )))
    }

    fn object_store_outage(&mut self) -> Result<(), ScenarioError> {
        Err(ScenarioError::Io(std::io::Error::other(
            "mounted replay driver does not support object-store outage",
        )))
    }

    fn object_store_recovery(&mut self) -> Result<(), ScenarioError> {
        Err(ScenarioError::Io(std::io::Error::other(
            "mounted replay driver does not support object-store recovery",
        )))
    }

    fn mounted_only(
        &mut self,
        name: &str,
        reason: &str,
        _record: &ScenarioOperationRecord,
    ) -> Result<(), ScenarioError> {
        Err(ScenarioError::Io(std::io::Error::other(format!(
            "mounted replay driver does not support mounted-only operation {name}: {reason}"
        ))))
    }
}

pub struct MountedReplayControllerDriver<'a, E, C> {
    pub controller: &'a mut MultiClientController<E>,
    pub compose_control: Option<&'a mut ComposeServiceControl>,
    pub infra_executor: Option<&'a C>,
}

impl<'a, E, C> MountedReplayControllerDriver<'a, E, C> {
    pub fn new(
        controller: &'a mut MultiClientController<E>,
        compose_control: Option<&'a mut ComposeServiceControl>,
        infra_executor: Option<&'a C>,
    ) -> Self {
        Self {
            controller,
            compose_control,
            infra_executor,
        }
    }
}

impl<E, C> MountedReplayDriver for MountedReplayControllerDriver<'_, E, C>
where
    E: ClientProcessExecutor,
    C: CommandExecutor,
{
    fn clients(&self) -> Vec<ClientSpec> {
        self.controller
            .clients()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>()
    }

    fn crash_client(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.controller.force_kill(client)?)
    }

    fn restart_client(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.controller.remount(client)?)
    }

    fn postgres_outage(&mut self) -> Result<(), ScenarioError> {
        self.stop_service(POSTGRES_SERVICE_NAME, "PostgreSQL outage")
    }

    fn postgres_recovery(&mut self) -> Result<(), ScenarioError> {
        self.start_service(POSTGRES_SERVICE_NAME, "PostgreSQL recovery")
    }

    fn object_store_outage(&mut self) -> Result<(), ScenarioError> {
        self.stop_service(RUSTFS_SERVICE_NAME, "object-store outage")
    }

    fn object_store_recovery(&mut self) -> Result<(), ScenarioError> {
        self.start_service(RUSTFS_SERVICE_NAME, "object-store recovery")
    }
}

impl<E, C> MountedReplayControllerDriver<'_, E, C>
where
    E: ClientProcessExecutor,
    C: CommandExecutor,
{
    fn stop_service(&mut self, service: &str, action: &str) -> Result<(), ScenarioError> {
        let compose_control = self.compose_control.as_deref_mut().ok_or_else(|| {
            ScenarioError::Io(std::io::Error::other(format!(
                "mounted replay {action} requires compose infrastructure"
            )))
        })?;
        let executor = self.infra_executor.ok_or_else(|| {
            ScenarioError::Io(std::io::Error::other(format!(
                "mounted replay {action} requires an infrastructure executor"
            )))
        })?;
        Ok(compose_control.stop_service(service, executor)?)
    }

    fn start_service(&mut self, service: &str, action: &str) -> Result<(), ScenarioError> {
        let compose_control = self.compose_control.as_deref_mut().ok_or_else(|| {
            ScenarioError::Io(std::io::Error::other(format!(
                "mounted replay {action} requires compose infrastructure"
            )))
        })?;
        let executor = self.infra_executor.ok_or_else(|| {
            ScenarioError::Io(std::io::Error::other(format!(
                "mounted replay {action} requires an infrastructure executor"
            )))
        })?;
        Ok(compose_control.start_service(service, executor)?)
    }
}

pub type SystemMountedReplayControllerDriver<'a, C> =
    MountedReplayControllerDriver<'a, SystemClientProcessExecutor, C>;

pub fn replay_operation_records(
    driver: &mut dyn MountedReplayDriver,
    records: &[ScenarioOperationRecord],
) -> Result<MountedReplayReport, ScenarioError> {
    MountedReplaySession::new(driver).replay(records)
}

struct MountedReplaySession<'a> {
    driver: &'a mut dyn MountedReplayDriver,
    clients: Vec<ClientSpec>,
    report: MountedReplayReport,
}

impl<'a> MountedReplaySession<'a> {
    fn new(driver: &'a mut dyn MountedReplayDriver) -> Self {
        let clients = driver.clients();
        Self {
            driver,
            clients,
            report: MountedReplayReport::default(),
        }
    }

    fn replay(
        mut self,
        records: &[ScenarioOperationRecord],
    ) -> Result<MountedReplayReport, ScenarioError> {
        for record in records {
            if !self.record_should_apply(record) {
                continue;
            }
            self.apply_record(record)?;
        }
        Ok(self.report)
    }

    fn record_should_apply(&mut self, record: &ScenarioOperationRecord) -> bool {
        match &record.outcome {
            OperationOutcome::Accepted | OperationOutcome::Observed => true,
            OperationOutcome::Planned => {
                self.skip(record, "planned operation is not applied");
                false
            }
            OperationOutcome::Rejected { error } => {
                self.skip(
                    record,
                    format!("rejected operation is not applied: {error}"),
                );
                false
            }
        }
    }

    fn apply_record(&mut self, record: &ScenarioOperationRecord) -> Result<(), ScenarioError> {
        match &record.operation {
            ScenarioOperation::CreateDirectory { path } => self.create_dir(record, path),
            ScenarioOperation::CreateFile { path, bytes } => {
                self.create_file(record, path, bytes.unwrap_or_default())
            }
            ScenarioOperation::Write {
                path,
                offset,
                bytes,
                ..
            } => self.write(record, path, offset.unwrap_or(0), *bytes, false),
            ScenarioOperation::Append { path, bytes, .. } => {
                self.write(record, path, 0, *bytes, true)
            }
            ScenarioOperation::Truncate { path, size } => self.truncate(record, path, *size),
            ScenarioOperation::Rename { from, to } => self.rename(record, from, to),
            ScenarioOperation::Unlink { path } => self.unlink(record, path),
            ScenarioOperation::Rmdir { path } => self.rmdir(record, path),
            ScenarioOperation::Read { path, bytes, .. } => self.read(record, path, *bytes),
            ScenarioOperation::Stat { path } => self.stat(record, path),
            ScenarioOperation::Readdir { path } => self.readdir(record, path),
            ScenarioOperation::Flush { path } | ScenarioOperation::Close { path } => {
                self.flush_or_close(record, path.as_deref())
            }
            ScenarioOperation::Fsync { path } => self.fsync(record, path),
            ScenarioOperation::Release { path } => self.release(record, path.as_deref()),
            ScenarioOperation::ClientCrash => self.client_crash(record),
            ScenarioOperation::ClientRestart => self.client_restart(record),
            ScenarioOperation::PostgresOutage => {
                self.infra_event(record, InfraReplayEvent::PostgresOutage)
            }
            ScenarioOperation::PostgresRecovery => {
                self.infra_event(record, InfraReplayEvent::PostgresRecovery)
            }
            ScenarioOperation::ObjectStoreOutage => {
                self.infra_event(record, InfraReplayEvent::ObjectStoreOutage)
            }
            ScenarioOperation::ObjectStoreRecovery => {
                self.infra_event(record, InfraReplayEvent::ObjectStoreRecovery)
            }
            ScenarioOperation::Wait { .. }
            | ScenarioOperation::Barrier { .. }
            | ScenarioOperation::SchedulerCheckpoint { .. } => {
                self.skip(record, "checkpoint/barrier is not a mounted mutation");
                Ok(())
            }
            ScenarioOperation::MountedOnly { name, reason } => {
                self.driver.mounted_only(name, reason, record)?;
                self.applied(record, record.client.clone(), None);
                Ok(())
            }
        }
    }

    fn create_dir(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
    ) -> Result<(), ScenarioError> {
        let target = self.record_path(record, path)?;
        fs::create_dir(&target)?;
        self.applied(record, record.client.clone(), Some(path.to_string()));
        Ok(())
    }

    fn create_file(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
        bytes: u64,
    ) -> Result<(), ScenarioError> {
        let target = self.record_path(record, path)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&target)?;
        if bytes > 0 {
            let len = usize::try_from(bytes).map_err(|_| {
                replay_io_error(format!("create file length {bytes} does not fit usize"))
            })?;
            file.write_all(&deterministic_payload(record.sequence, len))?;
        }
        self.applied(record, record.client.clone(), Some(path.to_string()));
        Ok(())
    }

    fn write(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
        offset: u64,
        bytes: u64,
        append: bool,
    ) -> Result<(), ScenarioError> {
        let target = self.record_path(record, path)?;
        let len = usize::try_from(bytes)
            .map_err(|_| replay_io_error(format!("write length {bytes} does not fit usize")))?;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&target)?;
        if append {
            file.seek(SeekFrom::End(0))?;
        } else {
            file.seek(SeekFrom::Start(offset))?;
        }
        file.write_all(&deterministic_payload(record.sequence, len))?;
        self.applied(record, record.client.clone(), Some(path.to_string()));
        Ok(())
    }

    fn truncate(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
        size: u64,
    ) -> Result<(), ScenarioError> {
        let target = self.record_path(record, path)?;
        OpenOptions::new()
            .create(false)
            .write(true)
            .open(&target)?
            .set_len(size)?;
        self.applied(record, record.client.clone(), Some(path.to_string()));
        Ok(())
    }

    fn rename(
        &mut self,
        record: &ScenarioOperationRecord,
        from: &str,
        to: &str,
    ) -> Result<(), ScenarioError> {
        let source = self.record_path(record, from)?;
        let target = self.record_path(record, to)?;
        fs::rename(source, target)?;
        self.applied(record, record.client.clone(), Some(to.to_string()));
        Ok(())
    }

    fn unlink(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
    ) -> Result<(), ScenarioError> {
        let target = self.record_path(record, path)?;
        fs::remove_file(target)?;
        self.applied(record, record.client.clone(), Some(path.to_string()));
        Ok(())
    }

    fn rmdir(&mut self, record: &ScenarioOperationRecord, path: &str) -> Result<(), ScenarioError> {
        let target = self.record_path(record, path)?;
        fs::remove_dir(target)?;
        self.applied(record, record.client.clone(), Some(path.to_string()));
        Ok(())
    }

    fn read(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
        bytes: Option<u64>,
    ) -> Result<(), ScenarioError> {
        let target = self.record_path(record, path)?;
        let payload = fs::read(target)?;
        if let Some(bytes) = bytes {
            let actual = u64::try_from(payload.len()).unwrap_or(u64::MAX);
            if actual < bytes {
                return Err(replay_io_error(format!(
                    "read observed {actual} bytes but record expected at least {bytes} bytes for {path}"
                ))
                .into());
            }
        }
        self.applied(record, record.client.clone(), Some(path.to_string()));
        Ok(())
    }

    fn stat(&mut self, record: &ScenarioOperationRecord, path: &str) -> Result<(), ScenarioError> {
        let target = self.record_path(record, path)?;
        fs::metadata(target)?;
        self.applied(record, record.client.clone(), Some(path.to_string()));
        Ok(())
    }

    fn readdir(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
    ) -> Result<(), ScenarioError> {
        let target = self.record_path(record, path)?;
        fs::read_dir(target)?.collect::<Result<Vec<_>, _>>()?;
        self.applied(record, record.client.clone(), Some(path.to_string()));
        Ok(())
    }

    fn flush_or_close(
        &mut self,
        record: &ScenarioOperationRecord,
        path: Option<&str>,
    ) -> Result<(), ScenarioError> {
        let Some(path) = path else {
            self.applied(record, record.client.clone(), None);
            return Ok(());
        };
        self.fsync(record, path)
    }

    fn fsync(&mut self, record: &ScenarioOperationRecord, path: &str) -> Result<(), ScenarioError> {
        let target = self.record_path(record, path)?;
        File::open(target)?.sync_all()?;
        self.applied(record, record.client.clone(), Some(path.to_string()));
        Ok(())
    }

    fn release(
        &mut self,
        record: &ScenarioOperationRecord,
        path: Option<&str>,
    ) -> Result<(), ScenarioError> {
        if let Some(path) = path {
            let target = self.record_path(record, path)?;
            fs::metadata(target)?;
            self.applied(record, record.client.clone(), Some(path.to_string()));
        } else {
            self.applied(record, record.client.clone(), None);
        }
        Ok(())
    }

    fn client_crash(&mut self, record: &ScenarioOperationRecord) -> Result<(), ScenarioError> {
        let client = self.record_client_name(record)?;
        self.driver.crash_client(&client)?;
        self.applied(record, Some(client), None);
        Ok(())
    }

    fn client_restart(&mut self, record: &ScenarioOperationRecord) -> Result<(), ScenarioError> {
        let client = self.record_client_name(record)?;
        self.driver.restart_client(&client)?;
        self.refresh_clients();
        self.applied(record, Some(client), None);
        Ok(())
    }

    fn infra_event(
        &mut self,
        record: &ScenarioOperationRecord,
        event: InfraReplayEvent,
    ) -> Result<(), ScenarioError> {
        match event {
            InfraReplayEvent::PostgresOutage => self.driver.postgres_outage()?,
            InfraReplayEvent::PostgresRecovery => self.driver.postgres_recovery()?,
            InfraReplayEvent::ObjectStoreOutage => self.driver.object_store_outage()?,
            InfraReplayEvent::ObjectStoreRecovery => self.driver.object_store_recovery()?,
        }
        self.applied(record, None, None);
        Ok(())
    }

    fn record_path(
        &self,
        record: &ScenarioOperationRecord,
        path: &str,
    ) -> Result<PathBuf, ScenarioError> {
        let client = self.record_client(record)?;
        Ok(client.mount_point.join(valid_relative_path(path)?))
    }

    fn record_client(
        &self,
        record: &ScenarioOperationRecord,
    ) -> Result<&ClientSpec, ScenarioError> {
        let name = self.record_client_name(record)?;
        self.clients
            .iter()
            .find(|client| client.name == name)
            .ok_or_else(|| replay_io_error(format!("unknown mounted replay client {name}")).into())
    }

    fn record_client_name(
        &self,
        record: &ScenarioOperationRecord,
    ) -> Result<String, ScenarioError> {
        if let Some(client) = &record.client {
            return Ok(client.clone());
        }
        self.clients
            .first()
            .map(|client| client.name.clone())
            .ok_or_else(|| replay_io_error("mounted replay requires at least one client").into())
    }

    fn refresh_clients(&mut self) {
        self.clients = self.driver.clients();
    }

    fn applied(
        &mut self,
        record: &ScenarioOperationRecord,
        client: Option<String>,
        path: Option<String>,
    ) {
        self.report.applied.push(MountedReplayAppliedRecord {
            sequence: record.sequence,
            client,
            operation: operation_name(&record.operation).to_string(),
            path,
        });
    }

    fn skip(&mut self, record: &ScenarioOperationRecord, reason: impl Into<String>) {
        self.report.skipped.push(MountedReplaySkippedRecord {
            sequence: record.sequence,
            operation: operation_name(&record.operation).to_string(),
            reason: reason.into(),
        });
    }
}

#[derive(Debug, Clone, Copy)]
enum InfraReplayEvent {
    PostgresOutage,
    PostgresRecovery,
    ObjectStoreOutage,
    ObjectStoreRecovery,
}

fn valid_relative_path(path: &str) -> Result<&Path, ScenarioError> {
    if path.is_empty() || path.contains('\\') {
        return Err(replay_io_error(format!("invalid mounted replay path {path:?}")).into());
    }
    let path = Path::new(path);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::Prefix(_)
                    | Component::RootDir
                    | Component::CurDir
                    | Component::ParentDir
            )
        })
    {
        return Err(replay_io_error(format!("invalid mounted replay path {path:?}")).into());
    }
    Ok(path)
}

fn replay_io_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}

fn deterministic_payload(sequence: u64, len: usize) -> Vec<u8> {
    let byte = (sequence as u8).wrapping_mul(31).wrapping_add(17);
    vec![byte; len]
}

fn operation_name(operation: &ScenarioOperation) -> &'static str {
    match operation {
        ScenarioOperation::CreateFile { .. } => "create_file",
        ScenarioOperation::CreateDirectory { .. } => "create_directory",
        ScenarioOperation::Write { .. } => "write",
        ScenarioOperation::Append { .. } => "append",
        ScenarioOperation::Truncate { .. } => "truncate",
        ScenarioOperation::Rename { .. } => "rename",
        ScenarioOperation::Unlink { .. } => "unlink",
        ScenarioOperation::Rmdir { .. } => "rmdir",
        ScenarioOperation::Read { .. } => "read",
        ScenarioOperation::Stat { .. } => "stat",
        ScenarioOperation::Readdir { .. } => "readdir",
        ScenarioOperation::Flush { .. } => "flush",
        ScenarioOperation::Fsync { .. } => "fsync",
        ScenarioOperation::Close { .. } => "close",
        ScenarioOperation::Release { .. } => "release",
        ScenarioOperation::ClientRestart => "client_restart",
        ScenarioOperation::ClientCrash => "client_crash",
        ScenarioOperation::PostgresOutage => "postgres_outage",
        ScenarioOperation::PostgresRecovery => "postgres_recovery",
        ScenarioOperation::ObjectStoreOutage => "object_store_outage",
        ScenarioOperation::ObjectStoreRecovery => "object_store_recovery",
        ScenarioOperation::Wait { .. } => "wait",
        ScenarioOperation::Barrier { .. } => "barrier",
        ScenarioOperation::SchedulerCheckpoint { .. } => "scheduler_checkpoint",
        ScenarioOperation::MountedOnly { .. } => "mounted_only",
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::path::Path;

    use studiofs_scenario::{OperationOutcome, ScenarioOperation, ScenarioOperationRecord};
    use tempfile::tempdir;

    use super::*;
    use crate::client::client_spec_for;

    struct RecordingMountedReplayDriver {
        clients: Vec<ClientSpec>,
        actions: RefCell<Vec<String>>,
    }

    impl RecordingMountedReplayDriver {
        fn new(clients: Vec<ClientSpec>) -> Self {
            Self {
                clients,
                actions: RefCell::new(Vec::new()),
            }
        }

        fn actions(&self) -> Vec<String> {
            self.actions.borrow().clone()
        }
    }

    impl MountedReplayDriver for RecordingMountedReplayDriver {
        fn clients(&self) -> Vec<ClientSpec> {
            self.clients.clone()
        }

        fn crash_client(&mut self, client: &str) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push(format!("crash:{client}"));
            Ok(())
        }

        fn restart_client(&mut self, client: &str) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push(format!("restart:{client}"));
            Ok(())
        }

        fn postgres_outage(&mut self) -> Result<(), ScenarioError> {
            self.actions
                .borrow_mut()
                .push("postgres_outage".to_string());
            Ok(())
        }

        fn postgres_recovery(&mut self) -> Result<(), ScenarioError> {
            self.actions
                .borrow_mut()
                .push("postgres_recovery".to_string());
            Ok(())
        }

        fn object_store_outage(&mut self) -> Result<(), ScenarioError> {
            self.actions
                .borrow_mut()
                .push("object_store_outage".to_string());
            Ok(())
        }

        fn object_store_recovery(&mut self) -> Result<(), ScenarioError> {
            self.actions
                .borrow_mut()
                .push("object_store_recovery".to_string());
            Ok(())
        }

        fn mounted_only(
            &mut self,
            name: &str,
            reason: &str,
            _record: &ScenarioOperationRecord,
        ) -> Result<(), ScenarioError> {
            self.actions
                .borrow_mut()
                .push(format!("mounted_only:{name}:{reason}"));
            Ok(())
        }
    }

    #[test]
    fn mounted_replay_applies_real_mount_file_operations_by_client() {
        let temp = tempdir().unwrap();
        let clients = mounted_replay_clients(temp.path(), &["client-a", "client-b"]);
        let client_a_mount = clients[0].mount_point.clone();
        let client_b_mount = clients[1].mount_point.clone();
        let mut driver = RecordingMountedReplayDriver::new(clients);
        let records = vec![
            record(
                1,
                Some("client-a"),
                ScenarioOperation::CreateDirectory {
                    path: "shared".to_string(),
                },
            ),
            record(
                2,
                Some("client-a"),
                ScenarioOperation::CreateFile {
                    path: "shared/file.txt".to_string(),
                    bytes: Some(3),
                },
            ),
            record(
                3,
                Some("client-a"),
                ScenarioOperation::Write {
                    path: "shared/file.txt".to_string(),
                    offset: Some(1),
                    bytes: 2,
                    checksum_sha256: None,
                },
            ),
            record(
                4,
                Some("client-a"),
                ScenarioOperation::Append {
                    path: "shared/file.txt".to_string(),
                    bytes: 1,
                    checksum_sha256: None,
                },
            ),
            record(
                5,
                Some("client-a"),
                ScenarioOperation::Read {
                    path: "shared/file.txt".to_string(),
                    bytes: Some(4),
                    checksum_sha256: None,
                },
            ),
            record(
                6,
                Some("client-a"),
                ScenarioOperation::Rename {
                    from: "shared/file.txt".to_string(),
                    to: "shared/renamed.txt".to_string(),
                },
            ),
            record(
                7,
                Some("client-b"),
                ScenarioOperation::Write {
                    path: "peer.txt".to_string(),
                    offset: Some(0),
                    bytes: 4,
                    checksum_sha256: None,
                },
            ),
            record(
                8,
                Some("client-a"),
                ScenarioOperation::Unlink {
                    path: "shared/renamed.txt".to_string(),
                },
            ),
            record(
                9,
                Some("client-a"),
                ScenarioOperation::Rmdir {
                    path: "shared".to_string(),
                },
            ),
        ];

        let report = replay_operation_records(&mut driver, &records).unwrap();

        assert_eq!(report.applied.len(), 9);
        assert!(!client_a_mount.join("shared").exists());
        assert_eq!(fs::read(client_b_mount.join("peer.txt")).unwrap().len(), 4);
    }

    #[test]
    fn mounted_replay_applies_crash_outage_and_mounted_only_hooks() {
        let temp = tempdir().unwrap();
        let clients = mounted_replay_clients(temp.path(), &["client-a"]);
        let mut driver = RecordingMountedReplayDriver::new(clients);
        let records = vec![
            record(1, Some("client-a"), ScenarioOperation::ClientCrash),
            record(2, Some("client-a"), ScenarioOperation::ClientRestart),
            record(3, None, ScenarioOperation::PostgresOutage),
            record(4, None, ScenarioOperation::PostgresRecovery),
            record(5, None, ScenarioOperation::ObjectStoreOutage),
            record(6, None, ScenarioOperation::ObjectStoreRecovery),
            ScenarioOperationRecord::new(
                "mounted-replay-test",
                7,
                ScenarioOperation::MountedOnly {
                    name: "winfsp_drive_letter_probe".to_string(),
                    reason: "requires real mount".to_string(),
                },
            )
            .with_outcome(OperationOutcome::Observed),
            record(
                8,
                None,
                ScenarioOperation::SchedulerCheckpoint {
                    name: "ignored".to_string(),
                },
            )
            .with_outcome(OperationOutcome::Observed),
            record(
                9,
                Some("client-a"),
                ScenarioOperation::Write {
                    path: "rejected.txt".to_string(),
                    offset: Some(0),
                    bytes: 1,
                    checksum_sha256: None,
                },
            )
            .with_outcome(OperationOutcome::Rejected {
                error: "injected".to_string(),
            }),
        ];

        let report = replay_operation_records(&mut driver, &records).unwrap();

        assert_eq!(
            driver.actions(),
            [
                "crash:client-a",
                "restart:client-a",
                "postgres_outage",
                "postgres_recovery",
                "object_store_outage",
                "object_store_recovery",
                "mounted_only:winfsp_drive_letter_probe:requires real mount",
            ]
        );
        assert_eq!(report.applied.len(), 7);
        assert_eq!(report.skipped.len(), 2);
        assert_eq!(report.skipped[0].operation, "scheduler_checkpoint");
        assert_eq!(report.skipped[1].operation, "write");
    }

    fn mounted_replay_clients(root: &Path, names: &[&str]) -> Vec<ClientSpec> {
        names
            .iter()
            .map(|name| {
                let mount = root.join("mounts").join(name);
                fs::create_dir_all(&mount).unwrap();
                client_spec_for(root, "projectfs", name, mount)
            })
            .collect()
    }

    fn record(
        sequence: u64,
        client: Option<&str>,
        operation: ScenarioOperation,
    ) -> ScenarioOperationRecord {
        let mut record = ScenarioOperationRecord::new("mounted-replay-test", sequence, operation);
        if let Some(client) = client {
            record = record.with_client(client);
        }
        record
    }
}
