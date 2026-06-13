//! Split from advanced mounted workload implementation.

use super::*;

pub(in crate::scenario) fn sparse_expected_range_specs(
    isolation: Option<(&ScenarioIsolation, &str)>,
) -> Vec<SparseRangeSpec> {
    [
        SparseRangeSpec {
            label: "prefix",
            offset: 0,
            payload: sparse_range_payload(isolation, "prefix", SPARSE_PREFIX_PAYLOAD),
        },
        SparseRangeSpec {
            label: "first_chunk_boundary",
            offset: SPARSE_FIRST_BOUNDARY_OFFSET,
            payload: sparse_range_payload(
                isolation,
                "first_chunk_boundary",
                SPARSE_FIRST_BOUNDARY_PAYLOAD,
            ),
        },
        SparseRangeSpec {
            label: "second_chunk_boundary",
            offset: SPARSE_SECOND_BOUNDARY_OFFSET,
            payload: sparse_range_payload(
                isolation,
                "second_chunk_boundary",
                SPARSE_SECOND_BOUNDARY_PAYLOAD,
            ),
        },
        SparseRangeSpec {
            label: "final_tail",
            offset: SPARSE_FINAL_TAIL_OFFSET,
            payload: sparse_range_payload(isolation, "final_tail", SPARSE_FINAL_TAIL_PAYLOAD),
        },
    ]
    .into_iter()
    .collect()
}

pub(in crate::scenario) fn sparse_range_payload(
    isolation: Option<(&ScenarioIsolation, &str)>,
    label: &str,
    payload: &[u8],
) -> Vec<u8> {
    match isolation {
        Some((isolation, logical_path)) => {
            isolation.payload(label, logical_path, &String::from_utf8_lossy(payload))
        }
        None => payload.to_vec(),
    }
}

pub(in crate::scenario) fn sparse_hole_specs() -> &'static [SparseHoleSpec] {
    &[
        SparseHoleSpec {
            offset: 65_536,
            len: 4_096,
        },
        SparseHoleSpec {
            offset: 1_572_864,
            len: 4_096,
        },
    ]
}

pub(in crate::scenario) fn sparse_logical_size(
    base_logical_size: u64,
    ranges: &[SparseRangeSpec],
) -> u64 {
    ranges.iter().fold(base_logical_size, |size, range| {
        size.max(range.offset + range.payload.len() as u64)
    })
}

pub(in crate::scenario) fn sparse_final_logical_size(ranges: &[SparseRangeSpec]) -> u64 {
    sparse_logical_size(SPARSE_FINAL_LOGICAL_SIZE, ranges)
}

pub(in crate::scenario) fn run_sparse_range_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(Vec<NamespaceCollisionOperation>, SparseRangeSummary), ScenarioError> {
    if clients.len() < 3 {
        return Err(ScenarioError::InsufficientMountedClients {
            expected: 3,
            actual: clients.len(),
        });
    }

    let started = Instant::now();
    let writer = &clients[0];
    let reader = &clients[1];
    let extender = &clients[2];
    let mut operations = Vec::new();
    let sparse_path = isolation.path(
        SPARSE_FILE_PATH
            .strip_prefix("sparse/")
            .unwrap_or(SPARSE_FILE_PATH),
    );
    let sparse_ranges = sparse_expected_range_specs(Some((isolation, &sparse_path)));
    let initial_logical_size =
        sparse_logical_size(SPARSE_INITIAL_LOGICAL_SIZE, &sparse_ranges[..2]);
    let shrunk_logical_size = sparse_logical_size(SPARSE_SHRUNK_LOGICAL_SIZE, &sparse_ranges[..2]);
    let final_logical_size = sparse_final_logical_size(&sparse_ranges);

    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            1,
            writer,
            "write_range",
            &format!("{sparse_path}@0"),
            run_sparse_helper_process(
                context,
                MOUNTED_SPARSE_WRITE_TIMEOUT,
                writer,
                "write-range",
                &sparse_path,
                Some(0),
                None,
                Some(&sparse_ranges[0].payload),
            ),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            2,
            writer,
            "write_range",
            &format!("{sparse_path}@1048569"),
            run_sparse_helper_process(
                context,
                MOUNTED_SPARSE_WRITE_TIMEOUT,
                writer,
                "write-range",
                &sparse_path,
                Some(SPARSE_FIRST_BOUNDARY_OFFSET),
                None,
                Some(&sparse_ranges[1].payload),
            ),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            3,
            writer,
            "truncate_extend",
            &format!("{sparse_path}@{initial_logical_size}"),
            run_sparse_helper_process(
                context,
                MOUNTED_SPARSE_WRITE_TIMEOUT,
                writer,
                "truncate",
                &sparse_path,
                None,
                Some(initial_logical_size),
                None,
            ),
        )?,
    )?;
    wait_for_sparse_expected_until(SparseExpectedWait {
        context,
        clients,
        sparse_path: &sparse_path,
        logical_size: initial_logical_size,
        expected_ranges: &sparse_ranges[..2],
        hole_specs: &[],
        deadline: Instant::now() + timeout,
        poll_interval,
        label: "initial sparse ranges",
    })?;

    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            4,
            reader,
            "read_probe",
            &format!("{sparse_path}@65536"),
            run_sparse_helper_process(
                context,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                reader,
                "read-range-zero",
                &sparse_path,
                Some(65_536),
                Some(4_096),
                None,
            ),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            5,
            reader,
            "truncate_shrink",
            &format!("{sparse_path}@{shrunk_logical_size}"),
            run_sparse_helper_process(
                context,
                MOUNTED_SPARSE_WRITE_TIMEOUT,
                reader,
                "truncate",
                &sparse_path,
                None,
                Some(shrunk_logical_size),
                None,
            ),
        )?,
    )?;
    wait_for_sparse_expected_until(SparseExpectedWait {
        context,
        clients,
        sparse_path: &sparse_path,
        logical_size: shrunk_logical_size,
        expected_ranges: &sparse_ranges[..2],
        hole_specs: &[],
        deadline: Instant::now() + timeout,
        poll_interval,
        label: "shrunk sparse ranges",
    })?;

    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            6,
            extender,
            "truncate_extend",
            &format!("{sparse_path}@{final_logical_size}"),
            run_sparse_helper_process(
                context,
                MOUNTED_SPARSE_WRITE_TIMEOUT,
                extender,
                "truncate",
                &sparse_path,
                None,
                Some(final_logical_size),
                None,
            ),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            7,
            extender,
            "write_range",
            &format!("{sparse_path}@2097147"),
            run_sparse_helper_process(
                context,
                MOUNTED_SPARSE_WRITE_TIMEOUT,
                extender,
                "write-range",
                &sparse_path,
                Some(SPARSE_SECOND_BOUNDARY_OFFSET),
                None,
                Some(&sparse_ranges[2].payload),
            ),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            8,
            writer,
            "write_range",
            &format!("{sparse_path}@3145900"),
            run_sparse_helper_process(
                context,
                MOUNTED_SPARSE_WRITE_TIMEOUT,
                writer,
                "write-range",
                &sparse_path,
                Some(SPARSE_FINAL_TAIL_OFFSET),
                None,
                Some(&sparse_ranges[3].payload),
            ),
        )?,
    )?;

    let (read_probes, zero_filled_holes) = wait_for_sparse_expected_until(SparseExpectedWait {
        context,
        clients,
        sparse_path: &sparse_path,
        logical_size: final_logical_size,
        expected_ranges: &sparse_ranges,
        hole_specs: sparse_hole_specs(),
        deadline: Instant::now() + timeout,
        poll_interval,
        label: "final sparse ranges",
    })?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            9,
            writer,
            "read_probe",
            &format!("{sparse_path}@3145900"),
            run_sparse_helper_process(
                context,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                writer,
                "read-range-exact",
                &sparse_path,
                Some(SPARSE_FINAL_TAIL_OFFSET),
                None,
                Some(&sparse_ranges[3].payload),
            ),
        )?,
    )?;

    let expected_payload = expected_sparse_payload(&sparse_ranges, final_logical_size);
    let summary = SparseRangeSummary {
        relative_path: sparse_path,
        logical_size: final_logical_size,
        final_checksum_sha256: bytes_sha256(&expected_payload),
        expected_byte_ranges: sparse_expected_ranges(&sparse_ranges),
        zero_filled_holes,
        read_probes,
    };
    Ok((operations, summary))
}

pub(in crate::scenario) struct SparseExpectedWait<'a> {
    pub(in crate::scenario) context: &'a ScenarioContext,
    pub(in crate::scenario) clients: &'a [ClientSpec],
    pub(in crate::scenario) sparse_path: &'a str,
    pub(in crate::scenario) logical_size: u64,
    pub(in crate::scenario) expected_ranges: &'a [SparseRangeSpec],
    pub(in crate::scenario) hole_specs: &'a [SparseHoleSpec],
    pub(in crate::scenario) deadline: Instant,
    pub(in crate::scenario) poll_interval: Duration,
    pub(in crate::scenario) label: &'a str,
}

pub(in crate::scenario) fn wait_for_sparse_expected_until(
    wait: SparseExpectedWait<'_>,
) -> Result<(Vec<SparseReadProbe>, Vec<SparseHoleProbe>), ScenarioError> {
    let wait_started = Instant::now();
    let SparseExpectedWait {
        context,
        clients,
        sparse_path,
        logical_size,
        expected_ranges,
        hole_specs,
        deadline,
        poll_interval,
        label,
    } = wait;
    loop {
        let observation = observe_sparse_ranges(
            context,
            clients,
            sparse_path,
            logical_size,
            expected_ranges,
            hole_specs,
        )?;
        if observation.missing.is_empty() {
            return Ok((observation.read_probes, observation.zero_filled_holes));
        }

        let now = Instant::now();
        if now >= deadline {
            let artifact = write_sparse_timeout_diagnostics(
                context,
                SparseTimeoutDiagnosticsInput {
                    label,
                    sparse_path,
                    logical_size,
                    expected_ranges,
                    hole_specs,
                    poll_interval,
                    elapsed: wait_started.elapsed(),
                    observation: &observation,
                },
            );
            let artifact_note = match artifact {
                Ok(artifact) => format!(
                    "; diagnostics: artifacts/scenarios/{}/{}",
                    AdvancedMountedKind::SparseRange.name(),
                    artifact
                ),
                Err(error) => format!("; diagnostics write failed: {error}"),
            };
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not become visible before timeout; missing: {}{}",
                    observation.missing.join(", "),
                    artifact_note
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

#[derive(Debug, Serialize)]
pub(in crate::scenario) struct SparseTimeoutDiagnostics {
    pub(in crate::scenario) schema_version: u32,
    pub(in crate::scenario) run_id: String,
    pub(in crate::scenario) scenario: String,
    pub(in crate::scenario) label: String,
    pub(in crate::scenario) sparse_path: String,
    pub(in crate::scenario) logical_size: u64,
    pub(in crate::scenario) elapsed_ms: u64,
    pub(in crate::scenario) poll_interval_ms: u64,
    pub(in crate::scenario) missing: Vec<String>,
    pub(in crate::scenario) expected_ranges: Vec<SparseTimeoutRangeSpec>,
    pub(in crate::scenario) expected_holes: Vec<SparseTimeoutHoleSpec>,
    pub(in crate::scenario) environment: Vec<SparseEnvironmentValue>,
    pub(in crate::scenario) clients: Vec<SparseClientObservation>,
}

#[derive(Debug, Serialize)]
pub(in crate::scenario) struct SparseTimeoutRangeSpec {
    pub(in crate::scenario) label: String,
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
    pub(in crate::scenario) expected_checksum_sha256: String,
}

#[derive(Debug, Serialize)]
pub(in crate::scenario) struct SparseTimeoutHoleSpec {
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
}

#[derive(Debug, Serialize)]
pub(in crate::scenario) struct SparseEnvironmentValue {
    pub(in crate::scenario) name: &'static str,
    pub(in crate::scenario) value: Option<String>,
}

pub(in crate::scenario) struct SparseTimeoutDiagnosticsInput<'a> {
    pub(in crate::scenario) label: &'a str,
    pub(in crate::scenario) sparse_path: &'a str,
    pub(in crate::scenario) logical_size: u64,
    pub(in crate::scenario) expected_ranges: &'a [SparseRangeSpec],
    pub(in crate::scenario) hole_specs: &'a [SparseHoleSpec],
    pub(in crate::scenario) poll_interval: Duration,
    pub(in crate::scenario) elapsed: Duration,
    pub(in crate::scenario) observation: &'a SparseObservation,
}

pub(in crate::scenario) struct SparseObservation {
    pub(in crate::scenario) read_probes: Vec<SparseReadProbe>,
    pub(in crate::scenario) zero_filled_holes: Vec<SparseHoleProbe>,
    pub(in crate::scenario) missing: Vec<String>,
    pub(in crate::scenario) clients: Vec<SparseClientObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub(in crate::scenario) struct SparseClientObservation {
    pub(in crate::scenario) client: String,
    pub(in crate::scenario) mount_point: String,
    pub(in crate::scenario) path: String,
    pub(in crate::scenario) parent_visible: bool,
    pub(in crate::scenario) metadata: SparseMetadataObservation,
    pub(in crate::scenario) open: SparseOpenObservation,
    pub(in crate::scenario) ranges: Vec<SparseRangeObservation>,
    pub(in crate::scenario) holes: Vec<SparseHoleObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub(in crate::scenario) struct SparseMetadataObservation {
    pub(in crate::scenario) ok: bool,
    pub(in crate::scenario) len: Option<u64>,
    pub(in crate::scenario) is_file: Option<bool>,
    pub(in crate::scenario) error: Option<SparseIoErrorObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub(in crate::scenario) struct SparseOpenObservation {
    pub(in crate::scenario) ok: bool,
    pub(in crate::scenario) error: Option<SparseIoErrorObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub(in crate::scenario) struct SparseRangeObservation {
    pub(in crate::scenario) label: String,
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
    pub(in crate::scenario) expected_checksum_sha256: String,
    pub(in crate::scenario) actual_checksum_sha256: Option<String>,
    pub(in crate::scenario) matched: Option<bool>,
    pub(in crate::scenario) error: Option<SparseIoErrorObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub(in crate::scenario) struct SparseHoleObservation {
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
    pub(in crate::scenario) all_zero: Option<bool>,
    pub(in crate::scenario) error: Option<SparseIoErrorObservation>,
}

#[derive(Debug, Clone, Serialize)]
pub(in crate::scenario) struct SparseIoErrorObservation {
    pub(in crate::scenario) kind: String,
    pub(in crate::scenario) message: String,
}

pub(in crate::scenario) fn observe_sparse_ranges(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    sparse_path: &str,
    logical_size: u64,
    expected_ranges: &[SparseRangeSpec],
    hole_specs: &[SparseHoleSpec],
) -> Result<SparseObservation, ScenarioError> {
    let mut read_probes = Vec::new();
    let mut hole_results = Vec::new();
    let mut missing = Vec::new();
    let mut client_observations = Vec::new();
    let expected_payload = expected_sparse_payload(expected_ranges, logical_size);

    for client in clients {
        let path = client.mount_point.join(sparse_path);
        let mut client_observation = SparseClientObservation {
            client: client.name.clone(),
            mount_point: client.mount_point.display().to_string(),
            path: path.display().to_string(),
            parent_visible: false,
            metadata: SparseMetadataObservation {
                ok: false,
                len: None,
                is_file: None,
                error: None,
            },
            open: SparseOpenObservation {
                ok: false,
                error: None,
            },
            ranges: Vec::new(),
            holes: Vec::new(),
        };

        match run_sparse_helper_process(
            context,
            MOUNTED_VALIDATION_PROBE_TIMEOUT,
            client,
            "read-all-exact",
            sparse_path,
            None,
            None,
            Some(&expected_payload),
        ) {
            Ok(()) => {
                client_observation.parent_visible = true;
                client_observation.metadata = SparseMetadataObservation {
                    ok: true,
                    len: Some(logical_size),
                    is_file: Some(true),
                    error: None,
                };
                client_observation.open = SparseOpenObservation {
                    ok: true,
                    error: None,
                };
                for range in expected_ranges {
                    let expected_checksum = bytes_sha256(&range.payload);
                    client_observation.ranges.push(SparseRangeObservation {
                        label: range.label.to_string(),
                        offset: range.offset,
                        len: range.payload.len(),
                        expected_checksum_sha256: expected_checksum.clone(),
                        actual_checksum_sha256: Some(expected_checksum.clone()),
                        matched: Some(true),
                        error: None,
                    });
                    read_probes.push(SparseReadProbe {
                        client: client.name.clone(),
                        offset: range.offset,
                        len: range.payload.len(),
                        actual_checksum_sha256: expected_checksum,
                        matched: true,
                    });
                }
                for hole in hole_specs {
                    client_observation.holes.push(SparseHoleObservation {
                        offset: hole.offset,
                        len: hole.len,
                        all_zero: Some(true),
                        error: None,
                    });
                    hole_results.push(SparseHoleProbe {
                        client: client.name.clone(),
                        offset: hole.offset,
                        len: hole.len,
                        all_zero: true,
                    });
                }
            }
            Err(error) if hotspot_helper_error_is_fatal(&error) => return Err(error),
            Err(error) => {
                let observation = sparse_scenario_error_observation(&error);
                client_observation.metadata.error = Some(observation.clone());
                client_observation.open.error = Some(observation);
                missing.push(format!("{}:{sparse_path} ({error})", client.name));
            }
        }
        client_observations.push(client_observation);
    }

    Ok(SparseObservation {
        read_probes,
        zero_filled_holes: hole_results,
        missing,
        clients: client_observations,
    })
}

pub(in crate::scenario) fn write_sparse_timeout_diagnostics(
    context: &ScenarioContext,
    input: SparseTimeoutDiagnosticsInput<'_>,
) -> Result<String, ScenarioError> {
    let artifact = sparse_timeout_diagnostics_file_name(input.label);
    let diagnostics = SparseTimeoutDiagnostics {
        schema_version: 1,
        run_id: context.run_id.clone(),
        scenario: AdvancedMountedKind::SparseRange.name().to_string(),
        label: input.label.to_string(),
        sparse_path: input.sparse_path.to_string(),
        logical_size: input.logical_size,
        elapsed_ms: duration_millis_u64(input.elapsed),
        poll_interval_ms: duration_millis_u64(input.poll_interval),
        missing: input.observation.missing.clone(),
        expected_ranges: input
            .expected_ranges
            .iter()
            .map(|range| SparseTimeoutRangeSpec {
                label: range.label.to_string(),
                offset: range.offset,
                len: range.payload.len(),
                expected_checksum_sha256: bytes_sha256(&range.payload),
            })
            .collect(),
        expected_holes: input
            .hole_specs
            .iter()
            .map(|hole| SparseTimeoutHoleSpec {
                offset: hole.offset,
                len: hole.len,
            })
            .collect(),
        environment: sparse_timeout_environment(),
        clients: input.observation.clients.clone(),
    };
    fs::write(
        context.scenario_dir.join(&artifact),
        serde_json::to_vec_pretty(&diagnostics)?,
    )?;
    append_sparse_timeout_diagnostics_event(context, input.label, &artifact, &diagnostics)?;
    Ok(artifact)
}

pub(in crate::scenario) fn append_sparse_timeout_diagnostics_event(
    context: &ScenarioContext,
    label: &str,
    artifact: &str,
    diagnostics: &SparseTimeoutDiagnostics,
) -> Result<(), ScenarioError> {
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(context.scenario_dir.join("operations.ndjson"))?;
    let mut buffer = serde_json::to_vec(&json!({
        "run_id": context.run_id,
        "scenario": AdvancedMountedKind::SparseRange.name(),
        "operation": "sparse_timeout_diagnostics",
        "label": label,
        "path": diagnostics.sparse_path,
        "logical_size": diagnostics.logical_size,
        "artifact": format!(
            "artifacts/scenarios/{}/{}",
            AdvancedMountedKind::SparseRange.name(),
            artifact
        ),
        "missing": diagnostics.missing,
        "clients": diagnostics.clients,
    }))?;
    buffer.push(b'\n');
    log.write_all(&buffer)?;
    Ok(())
}

pub(in crate::scenario) fn sparse_timeout_diagnostics_file_name(label: &str) -> String {
    let slug = label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    format!("sparse-{slug}-timeout-diagnostics.json")
}

pub(in crate::scenario) fn sparse_timeout_environment() -> Vec<SparseEnvironmentValue> {
    [
        "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS",
        "STUDIOFS_PG_SYNC_POLL_SECS",
        "STUDIOFS_E2E_DISABLE_METADATA_BACKGROUND_POLL",
    ]
    .into_iter()
    .map(|name| SparseEnvironmentValue {
        name,
        value: std::env::var(name).ok(),
    })
    .collect()
}

pub(in crate::scenario) fn sparse_io_error_observation(
    error: &io::Error,
) -> SparseIoErrorObservation {
    SparseIoErrorObservation {
        kind: format!("{:?}", error.kind()),
        message: error.to_string(),
    }
}

pub(in crate::scenario) fn sparse_scenario_error_observation(
    error: &ScenarioError,
) -> SparseIoErrorObservation {
    match error {
        ScenarioError::Io(error) => sparse_io_error_observation(error),
        error => SparseIoErrorObservation {
            kind: "ScenarioError".to_string(),
            message: error.to_string(),
        },
    }
}

pub(in crate::scenario) fn duration_millis_u64(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    millis.min(u128::from(u64::MAX)) as u64
}

pub(in crate::scenario) fn expected_sparse_payload(
    ranges: &[SparseRangeSpec],
    logical_size: u64,
) -> Vec<u8> {
    let mut payload = vec![0; logical_size as usize];
    for range in ranges {
        let start = range.offset as usize;
        let end = start + range.payload.len();
        payload[start..end].copy_from_slice(&range.payload);
    }
    payload
}

pub(in crate::scenario) fn sparse_expected_ranges(
    ranges: &[SparseRangeSpec],
) -> Vec<SparseExpectedRange> {
    ranges
        .iter()
        .map(|range| SparseExpectedRange {
            label: range.label.to_string(),
            offset: range.offset,
            len: range.payload.len(),
            expected_checksum_sha256: bytes_sha256(&range.payload),
        })
        .collect()
}

pub(in crate::scenario) fn sparse_hole_summaries(
    clients: &[&str],
    hole_specs: &[SparseHoleSpec],
    all_zero: bool,
) -> Vec<SparseHoleProbe> {
    clients
        .iter()
        .flat_map(|client| {
            hole_specs.iter().map(move |hole| SparseHoleProbe {
                client: (*client).to_string(),
                offset: hole.offset,
                len: hole.len,
                all_zero,
            })
        })
        .collect()
}

pub(in crate::scenario) fn planned_sparse_range_summary() -> SparseRangeSummary {
    let ranges = sparse_expected_range_specs(None);
    let final_logical_size = sparse_final_logical_size(&ranges);
    let expected_payload = expected_sparse_payload(&ranges, final_logical_size);
    SparseRangeSummary {
        relative_path: SPARSE_FILE_PATH.to_string(),
        logical_size: final_logical_size,
        final_checksum_sha256: bytes_sha256(&expected_payload),
        expected_byte_ranges: sparse_expected_ranges(&ranges),
        zero_filled_holes: sparse_hole_summaries(
            ADVANCED_MOUNTED_CLIENT_NAMES,
            sparse_hole_specs(),
            true,
        ),
        read_probes: ADVANCED_MOUNTED_CLIENT_NAMES
            .iter()
            .flat_map(|client| {
                ranges.iter().map(move |range| SparseReadProbe {
                    client: (*client).to_string(),
                    offset: range.offset,
                    len: range.payload.len(),
                    actual_checksum_sha256: bytes_sha256(&range.payload),
                    matched: true,
                })
            })
            .collect(),
    }
}
