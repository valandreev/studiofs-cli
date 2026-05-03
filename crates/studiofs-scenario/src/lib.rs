//! Shared logical scenario operation model for E2E artifacts and simulation.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead as _, BufReader};
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const SCENARIO_OPERATION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioOperationRecord {
    pub schema_version: u32,
    pub scenario: String,
    pub sequence: u64,
    pub client: Option<String>,
    pub timestamp_ms: Option<u64>,
    pub operation: ScenarioOperation,
    pub outcome: OperationOutcome,
    pub simulation_support: SimulationSupport,
    pub metadata: BTreeMap<String, String>,
}

impl ScenarioOperationRecord {
    pub fn new(scenario: impl Into<String>, sequence: u64, operation: ScenarioOperation) -> Self {
        let simulation_support = operation.default_simulation_support();
        Self {
            schema_version: SCENARIO_OPERATION_SCHEMA_VERSION,
            scenario: scenario.into(),
            sequence,
            client: None,
            timestamp_ms: None,
            operation,
            outcome: OperationOutcome::Accepted,
            simulation_support,
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_client(mut self, client: impl Into<String>) -> Self {
        self.client = Some(client.into());
        self
    }

    pub fn with_timestamp_ms(mut self, timestamp_ms: u64) -> Self {
        self.timestamp_ms = Some(timestamp_ms);
        self
    }

    pub fn with_outcome(mut self, outcome: OperationOutcome) -> Self {
        self.outcome = outcome;
        self
    }

    pub fn with_simulation_support(mut self, simulation_support: SimulationSupport) -> Self {
        self.simulation_support = simulation_support;
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn is_simulation_supported(&self) -> bool {
        self.simulation_support.is_supported()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScenarioOperation {
    CreateFile {
        path: String,
        bytes: Option<u64>,
    },
    CreateDirectory {
        path: String,
    },
    Write {
        path: String,
        offset: Option<u64>,
        bytes: u64,
        checksum_sha256: Option<String>,
    },
    Append {
        path: String,
        bytes: u64,
        checksum_sha256: Option<String>,
    },
    Truncate {
        path: String,
        size: u64,
    },
    Rename {
        from: String,
        to: String,
    },
    Unlink {
        path: String,
    },
    Rmdir {
        path: String,
    },
    Read {
        path: String,
        bytes: Option<u64>,
        checksum_sha256: Option<String>,
    },
    Stat {
        path: String,
    },
    Readdir {
        path: String,
    },
    Flush {
        path: Option<String>,
    },
    Fsync {
        path: String,
    },
    Close {
        path: Option<String>,
    },
    Release {
        path: Option<String>,
    },
    ClientRestart,
    ClientCrash,
    PostgresOutage,
    PostgresRecovery,
    ObjectStoreOutage,
    ObjectStoreRecovery,
    Wait {
        checkpoint: String,
    },
    Barrier {
        name: String,
    },
    SchedulerCheckpoint {
        name: String,
    },
    MountedOnly {
        name: String,
        reason: String,
    },
}

impl ScenarioOperation {
    pub fn default_simulation_support(&self) -> SimulationSupport {
        match self {
            Self::MountedOnly { reason, .. } => SimulationSupport::Unsupported {
                reason: reason.clone(),
            },
            _ => SimulationSupport::Supported,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum OperationOutcome {
    Planned,
    Accepted,
    Rejected { error: String },
    Observed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SimulationSupport {
    Supported,
    Unsupported { reason: String },
}

impl SimulationSupport {
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::Supported)
    }
}

#[derive(Debug, Error)]
pub enum OperationLogError {
    #[error("operation log I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("operation log JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("operation log JSON failed on line {line}: {source}")]
    JsonLine {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("unsupported operation schema version {version} on line {line}")]
    UnsupportedSchemaVersion { line: usize, version: u32 },
}

pub fn write_operation_log(
    path: impl AsRef<Path>,
    records: &[ScenarioOperationRecord],
) -> Result<(), OperationLogError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    for record in records {
        write_operation_record(&mut file, record)?;
    }
    Ok(())
}

pub fn append_operation_record(
    path: impl AsRef<Path>,
    record: &ScenarioOperationRecord,
) -> Result<(), OperationLogError> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    write_operation_record(&mut file, record)
}

pub struct OperationLogWriter {
    file: fs::File,
    next_sequence: u64,
}

impl OperationLogWriter {
    pub fn open_append(path: impl AsRef<Path>) -> Result<Self, OperationLogError> {
        let path = path.as_ref();
        let next_sequence = next_operation_sequence(path)?;
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file,
            next_sequence,
        })
    }

    pub fn record(
        &self,
        scenario: impl Into<String>,
        operation: ScenarioOperation,
    ) -> ScenarioOperationRecord {
        ScenarioOperationRecord::new(scenario, self.next_sequence, operation)
    }

    pub fn append_record(
        &mut self,
        record: &ScenarioOperationRecord,
    ) -> Result<(), OperationLogError> {
        write_operation_record(&mut self.file, record)?;
        self.next_sequence = record.sequence.saturating_add(1).max(self.next_sequence);
        Ok(())
    }
}

pub fn next_operation_sequence(path: impl AsRef<Path>) -> Result<u64, OperationLogError> {
    match read_operation_log(path) {
        Ok(records) => Ok(records
            .last()
            .map_or(1, |record| record.sequence.saturating_add(1))),
        Err(OperationLogError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(1),
        Err(error) => Err(error),
    }
}

pub fn write_operation_record(
    writer: &mut impl std::io::Write,
    record: &ScenarioOperationRecord,
) -> Result<(), OperationLogError> {
    serde_json::to_writer(&mut *writer, record)?;
    writer.write_all(b"\n")?;
    Ok(())
}

pub fn read_operation_log(
    path: impl AsRef<Path>,
) -> Result<Vec<ScenarioOperationRecord>, OperationLogError> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    read_operation_log_from_reader(reader)
}

pub fn read_operation_log_from_reader(
    reader: impl std::io::Read,
) -> Result<Vec<ScenarioOperationRecord>, OperationLogError> {
    let reader = BufReader::new(reader);
    let mut records = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: ScenarioOperationRecord =
            serde_json::from_str(&line).map_err(|source| OperationLogError::JsonLine {
                line: line_number,
                source,
            })?;
        if record.schema_version != SCENARIO_OPERATION_SCHEMA_VERSION {
            return Err(OperationLogError::UnsupportedSchemaVersion {
                line: line_number,
                version: record.schema_version,
            });
        }
        records.push(record);
    }
    Ok(records)
}

pub fn simulation_supported_records(
    records: &[ScenarioOperationRecord],
) -> impl Iterator<Item = &ScenarioOperationRecord> {
    records
        .iter()
        .filter(|record| record.is_simulation_supported())
}
