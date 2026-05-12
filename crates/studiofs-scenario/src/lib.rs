//! Shared logical scenario operation model for E2E artifacts and simulation.

pub mod client;
pub mod collision;
pub mod crash_replay;
pub mod invariant;
pub mod metadata_bus;
pub mod object_store;
pub mod scheduler;
pub mod seed_corpus;

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead as _, BufReader, Read as _, Seek as _, SeekFrom};
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Constant $name.
pub const SCENARIO_OPERATION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct ScenarioOperationRecord {
    /// Field $name.
    pub schema_version: u32,
    /// Field $name.
    pub scenario: String,
    /// Field $name.
    pub sequence: u64,
    /// Field $name.
    pub client: Option<String>,
    /// Field $name.
    pub timestamp_ms: Option<u64>,
    /// Field $name.
    pub operation: ScenarioOperation,
    /// Field $name.
    pub outcome: OperationOutcome,
    /// Field $name.
    pub simulation_support: SimulationSupport,
    /// Field $name.
    pub metadata: BTreeMap<String, String>,
}

impl ScenarioOperationRecord {
    /// Function $name.
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

    /// Function $name.
    pub fn with_client(mut self, client: impl Into<String>) -> Self {
        self.client = Some(client.into());
        self
    }

    /// Function $name.
    pub fn with_timestamp_ms(mut self, timestamp_ms: u64) -> Self {
        self.timestamp_ms = Some(timestamp_ms);
        self
    }

    /// Function $name.
    pub fn with_outcome(mut self, outcome: OperationOutcome) -> Self {
        self.outcome = outcome;
        self
    }

    /// Function $name.
    pub fn with_simulation_support(mut self, simulation_support: SimulationSupport) -> Self {
        self.simulation_support = simulation_support;
        self
    }

    /// Function $name.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Function $name.
    pub fn is_simulation_supported(&self) -> bool {
        self.simulation_support.is_supported()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Enum $name.
pub enum ScenarioOperation {
    /// Variant $name.
    CreateFile {
        /// Field $name.
        path: String,
        /// Field $name.
        bytes: Option<u64>,
    },
    /// Variant $name.
    CreateDirectory {
        /// Field $name.
        path: String,
    },
    /// Variant $name.
    CreateSymlink {
        /// Field $name.
        path: String,
        /// Field $name.
        target: String,
    },
    /// Variant $name.
    Write {
        /// Field $name.
        path: String,
        /// Field $name.
        offset: Option<u64>,
        /// Field $name.
        bytes: u64,
        /// Field $name.
        checksum_sha256: Option<String>,
    },
    /// Variant $name.
    Append {
        /// Field $name.
        path: String,
        /// Field $name.
        bytes: u64,
        /// Field $name.
        checksum_sha256: Option<String>,
    },
    /// Variant $name.
    Truncate {
        /// Field $name.
        path: String,
        /// Field $name.
        size: u64,
    },
    /// Variant $name.
    Rename {
        /// Field $name.
        from: String,
        /// Field $name.
        to: String,
    },
    /// Variant $name.
    Unlink {
        /// Field $name.
        path: String,
    },
    /// Variant $name.
    Rmdir {
        /// Field $name.
        path: String,
    },
    /// Variant $name.
    Read {
        /// Field $name.
        path: String,
        /// Field $name.
        bytes: Option<u64>,
        /// Field $name.
        checksum_sha256: Option<String>,
    },
    /// Variant $name.
    ReadSymlink {
        /// Field $name.
        path: String,
        /// Field $name.
        target: Option<String>,
    },
    /// Variant $name.
    SetMetadataAttribute {
        /// Field $name.
        path: String,
        /// Field $name.
        name: String,
        /// Field $name.
        value: String,
    },
    /// Variant $name.
    PublishMetadataAttribute {
        /// Field $name.
        path: String,
        /// Field $name.
        name: String,
    },
    /// Variant $name.
    GetMetadataAttribute {
        /// Field $name.
        path: String,
        /// Field $name.
        name: String,
        /// Field $name.
        value: Option<String>,
    },
    /// Variant $name.
    Stat {
        /// Field $name.
        path: String,
    },
    /// Variant $name.
    Readdir {
        /// Field $name.
        path: String,
    },
    /// Variant $name.
    Flush {
        /// Field $name.
        path: Option<String>,
    },
    /// Variant $name.
    Fsync {
        /// Field $name.
        path: String,
    },
    /// Variant $name.
    Close {
        /// Field $name.
        path: Option<String>,
    },
    /// Variant $name.
    Release {
        /// Field $name.
        path: Option<String>,
    },
    /// Variant $name.
    ClientRestart,
    /// Variant $name.
    ClientCrash,
    /// Variant $name.
    PostgresOutage,
    /// Variant $name.
    PostgresRecovery,
    /// Variant $name.
    ObjectStoreOutage,
    /// Variant $name.
    ObjectStoreRecovery,
    /// Variant $name.
    Wait {
        /// Field $name.
        checkpoint: String,
    },
    /// Variant $name.
    Barrier {
        /// Field $name.
        name: String,
    },
    /// Variant $name.
    SchedulerCheckpoint {
        /// Field $name.
        name: String,
    },
    /// Variant $name.
    MountedOnly {
        /// Field $name.
        name: String,
        /// Field $name.
        reason: String,
    },
}

impl ScenarioOperation {
    /// Function $name.
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
/// Enum $name.
pub enum OperationOutcome {
    /// Variant $name.
    Planned,
    /// Variant $name.
    Accepted,
    /// Variant $name.
    Rejected {
        /// The error value.
        error: String,
    },
    /// Variant $name.
    Observed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
/// Enum $name.
pub enum SimulationSupport {
    /// Variant $name.
    Supported,
    /// Variant $name.
    Unsupported {
        /// The reason value.
        reason: String,
    },
}

impl SimulationSupport {
    /// Function $name.
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::Supported)
    }
}

#[derive(Debug, Error)]
/// Enum $name.
pub enum OperationLogError {
    #[error("operation log I/O failed: {0}")]
    /// Variant $name.
    Io(#[from] std::io::Error),
    #[error("operation log JSON failed: {0}")]
    /// Variant $name.
    Json(#[from] serde_json::Error),
    #[error("operation log JSON failed on line {line}: {source}")]
    /// Variant $name.
    JsonLine {
        /// Field $name.
        line: usize,
        #[source]
        /// Field $name.
        source: serde_json::Error,
    },
    #[error("unsupported operation schema version {version} on line {line}")]
    /// Field $name.
    UnsupportedSchemaVersion {
        /// The line value.
        line: usize,
        /// The version value.
        version: u32,
    },
}

/// Function $name.
pub fn write_operation_log(
    path: impl AsRef<Path>,
    records: &[ScenarioOperationRecord],
) -> Result<(), OperationLogError> {
    let mut file = open_truncate(path.as_ref())?;
    for record in records {
        write_operation_record(&mut file, record)?;
    }
    Ok(())
}

/// Function $name.
pub fn append_operation_record(
    path: impl AsRef<Path>,
    record: &ScenarioOperationRecord,
) -> Result<(), OperationLogError> {
    let mut file = open_append(path.as_ref())?;
    write_operation_record(&mut file, record)
}

/// Struct $name.
pub struct OperationLogWriter {
    file: fs::File,
    next_sequence: u64,
}

impl OperationLogWriter {
    /// Function $name.
    pub fn open_append(path: impl AsRef<Path>) -> Result<Self, OperationLogError> {
        let path = path.as_ref();
        let next_sequence = next_operation_sequence(path)?;
        let file = open_append(path)?;
        Ok(Self {
            file,
            next_sequence,
        })
    }

    /// Function $name.
    pub fn record(
        &self,
        scenario: impl Into<String>,
        operation: ScenarioOperation,
    ) -> ScenarioOperationRecord {
        ScenarioOperationRecord::new(scenario, self.next_sequence, operation)
    }

    /// Function $name.
    pub fn append_record(
        &mut self,
        record: &ScenarioOperationRecord,
    ) -> Result<(), OperationLogError> {
        write_operation_record(&mut self.file, record)?;
        self.next_sequence = record.sequence.saturating_add(1).max(self.next_sequence);
        Ok(())
    }

    /// Function $name.
    pub fn refresh_next_sequence(&mut self) -> Result<(), OperationLogError> {
        self.next_sequence = next_operation_sequence_from_file(&mut self.file)?;
        Ok(())
    }
}

/// Function $name.
pub fn next_operation_sequence(path: impl AsRef<Path>) -> Result<u64, OperationLogError> {
    let path = path.as_ref();
    match open_read(path) {
        Ok(mut file) => next_operation_sequence_from_file(&mut file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(1),
        Err(error) => Err(OperationLogError::Io(error)),
    }
}

fn next_operation_sequence_from_file(file: &mut fs::File) -> Result<u64, OperationLogError> {
    let Some(line) = last_non_empty_line(file)? else {
        return Ok(1);
    };
    let record: ScenarioOperationRecord = serde_json::from_str(&line)
        .map_err(|source| OperationLogError::JsonLine { line: 0, source })?;
    if record.schema_version != SCENARIO_OPERATION_SCHEMA_VERSION {
        return Err(OperationLogError::UnsupportedSchemaVersion {
            line: 0,
            version: record.schema_version,
        });
    }
    Ok(record.sequence.saturating_add(1))
}

fn last_non_empty_line(file: &mut fs::File) -> Result<Option<String>, OperationLogError> {
    const CHUNK_SIZE: u64 = 8192;

    let mut position = file.seek(SeekFrom::End(0))?;
    let mut line = Vec::new();
    let mut seen_non_whitespace = false;
    while position > 0 {
        let chunk_len = position.min(CHUNK_SIZE);
        position -= chunk_len;
        file.seek(SeekFrom::Start(position))?;
        let mut chunk = vec![0; chunk_len as usize];
        file.read_exact(&mut chunk)?;

        for byte in chunk.into_iter().rev() {
            if !seen_non_whitespace {
                if byte.is_ascii_whitespace() {
                    continue;
                }
                seen_non_whitespace = true;
            }
            if byte == b'\n' {
                line.reverse();
                return Ok(Some(String::from_utf8_lossy(&line).into_owned()));
            }
            line.push(byte);
        }
    }

    if seen_non_whitespace {
        line.reverse();
        Ok(Some(String::from_utf8_lossy(&line).into_owned()))
    } else {
        Ok(None)
    }
}

/// Function $name.
pub fn write_operation_record(
    writer: &mut impl std::io::Write,
    record: &ScenarioOperationRecord,
) -> Result<(), OperationLogError> {
    serde_json::to_writer(&mut *writer, record)?;
    writer.write_all(b"\n")?;
    Ok(())
}

/// Function $name.
pub fn read_operation_log(
    path: impl AsRef<Path>,
) -> Result<Vec<ScenarioOperationRecord>, OperationLogError> {
    let file = open_read(path.as_ref())?;
    read_operation_log_from_reader(file)
}

/// Function $name.
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

/// Function $name.
pub fn simulation_supported_records(
    records: &[ScenarioOperationRecord],
) -> impl Iterator<Item = &ScenarioOperationRecord> {
    records
        .iter()
        .filter(|record| record.is_simulation_supported())
}

fn open_read(path: &Path) -> std::io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options.read(true);
    allow_parallel_log_access(&mut options);
    options.open(path)
}

fn open_append(path: &Path) -> std::io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options.create(true).append(true).read(true);
    allow_parallel_log_access(&mut options);
    options.open(path)
}

fn open_truncate(path: &Path) -> std::io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    allow_parallel_log_access(&mut options);
    options.open(path)
}

#[cfg(windows)]
fn allow_parallel_log_access(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;

    options.share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE);
}

#[cfg(not(windows))]
fn allow_parallel_log_access(_options: &mut OpenOptions) {}
