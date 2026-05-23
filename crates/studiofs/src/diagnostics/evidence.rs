use super::*;

pub(super) fn clamp_evidence_budget(
    requested: &DiagnosticsEvidenceBudget,
) -> DiagnosticsEvidenceBudget {
    fn clamp_limit(requested: usize, default: usize, max: usize) -> usize {
        if requested == 0 {
            default
        } else {
            requested.clamp(1, max)
        }
    }

    DiagnosticsEvidenceBudget {
        max_recent_events: clamp_limit(
            requested.max_recent_events,
            DEFAULT_EVIDENCE_RECENT_EVENTS_LIMIT,
            MAX_EVIDENCE_RECENT_EVENTS_LIMIT,
        ),
        max_timeline_entries: clamp_limit(
            requested.max_timeline_entries,
            DEFAULT_EVIDENCE_TIMELINE_ENTRIES_LIMIT,
            MAX_EVIDENCE_TIMELINE_ENTRIES_LIMIT,
        ),
        max_profiling_windows: clamp_limit(
            requested.max_profiling_windows,
            DEFAULT_EVIDENCE_PROFILING_WINDOWS_LIMIT,
            MAX_EVIDENCE_PROFILING_WINDOWS_LIMIT,
        ),
        max_derived_items_per_kind: clamp_limit(
            requested.max_derived_items_per_kind,
            DEFAULT_EVIDENCE_DERIVED_ITEMS_PER_KIND,
            MAX_EVIDENCE_DERIVED_ITEMS_PER_KIND,
        ),
    }
}

pub(super) fn canonical_subsystem(value: &str) -> &str {
    match value {
        "upload" | "upload_pipeline" => "upload",
        other => other,
    }
}

pub(super) fn subsystem_matches(filter: Option<&str>, candidate: &str) -> bool {
    filter.is_none_or(|filter| canonical_subsystem(filter) == canonical_subsystem(candidate))
}

pub(super) fn operation_matches(filter: Option<&str>, candidate: &str) -> bool {
    filter.is_none_or(|filter| filter == candidate)
}

pub(super) fn json_contains_u64(value: &serde_json::Value, keys: &[&str], expected: u64) -> bool {
    match value {
        serde_json::Value::Object(map) => map.iter().any(|(key, value)| {
            (keys
                .iter()
                .any(|candidate| key.eq_ignore_ascii_case(candidate))
                && value.as_u64() == Some(expected))
                || json_contains_u64(value, keys, expected)
        }),
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| json_contains_u64(value, keys, expected)),
        _ => false,
    }
}

pub(super) fn json_contains_string(
    value: &serde_json::Value,
    keys: &[&str],
    expected: &str,
) -> bool {
    match value {
        serde_json::Value::Object(map) => map.iter().any(|(key, value)| {
            (keys
                .iter()
                .any(|candidate| key.eq_ignore_ascii_case(candidate))
                && value.as_str() == Some(expected))
                || json_contains_string(value, keys, expected)
        }),
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string(value, keys, expected)),
        _ => false,
    }
}

pub(super) fn object_identity_matches_fields(
    fields: &serde_json::Value,
    identity: &DiagnosticsObjectIdentity,
) -> bool {
    if let Some(inode) = identity.inode
        && !json_contains_u64(fields, &["ino", "inode"], inode)
    {
        return false;
    }
    if let Some(path) = identity.path.as_deref()
        && !json_contains_string(fields, &["path"], path)
    {
        return false;
    }
    if let Some(file_handle) = identity.file_handle.as_deref()
        && !json_contains_string(fields, &["file_handle", "fh", "handle_id"], file_handle)
    {
        return false;
    }
    true
}

pub(super) fn event_matches_scope(
    event: &DiagnosticsRecentEvent,
    scope: &DiagnosticsEvidenceScope,
    marker_span: Option<&DiagnosticsResolvedMarkerSpan>,
) -> bool {
    if !subsystem_matches(scope.subsystem.as_deref(), &event.subsystem) {
        return false;
    }
    if !operation_matches(scope.operation_type.as_deref(), &event.kind) {
        return false;
    }
    if let Some(identity) = scope.object_identity.as_ref()
        && !object_identity_matches_fields(&event.fields, identity)
    {
        return false;
    }
    if let Some(marker_span) = marker_span
        && (event.generated_unix_ms < marker_span.start_unix_ms
            || event.generated_unix_ms > marker_span.end_unix_ms)
    {
        return false;
    }
    true
}

pub(super) fn artifact_matches_scope(
    artifact: &WallBlockingProfileArtifact,
    scope: &DiagnosticsEvidenceScope,
) -> bool {
    match scope.object_identity.as_ref() {
        Some(identity) => artifact
            .start_events
            .iter()
            .chain(artifact.end_events.iter())
            .any(|event| object_identity_matches_fields(&event.fields, identity)),
        None => true,
    }
}

pub(super) fn snapshot_sections_for_scope(
    snapshot: &DiagnosticsRuntimeSnapshot,
    scope: &DiagnosticsEvidenceScope,
    marker_span: Option<&DiagnosticsResolvedMarkerSpan>,
) -> serde_json::Map<String, serde_json::Value> {
    if marker_span.is_some() {
        return serde_json::Map::new();
    }

    let mut sections = serde_json::Map::new();
    let filter = scope.subsystem.as_deref();
    if subsystem_matches(filter, "ingress") {
        sections.insert("ingress".to_string(), serde_json::json!(snapshot.ingress));
    }
    if subsystem_matches(filter, "write_path") {
        sections.insert(
            "write_path".to_string(),
            serde_json::json!(snapshot.write_path),
        );
    }
    if subsystem_matches(filter, "upload_pipeline") {
        sections.insert("upload".to_string(), serde_json::json!(snapshot.upload));
    }
    if subsystem_matches(filter, "zeroio") {
        sections.insert("zeroio".to_string(), serde_json::json!(snapshot.zeroio));
    }
    if subsystem_matches(filter, "cache") {
        sections.insert("cache".to_string(), serde_json::json!(snapshot.cache));
    }
    if subsystem_matches(filter, "synchronization") {
        sections.insert(
            "synchronization".to_string(),
            serde_json::json!(snapshot.synchronization),
        );
    }
    sections
}

pub(super) fn resolve_marker_span(
    run: &InvestigationRun,
    span: &DiagnosticsMarkerSpan,
) -> Result<DiagnosticsResolvedMarkerSpan> {
    let start = run
        .timeline
        .iter()
        .find(|entry| entry.sequence == span.start_sequence)
        .ok_or_else(|| anyhow!("marker {} not found", span.start_sequence))?;
    if start.kind != TimelineEntryKind::Marker {
        return Err(anyhow!("timeline entry {} is not a marker", start.sequence));
    }
    let end_sequence = span.end_sequence.unwrap_or(span.start_sequence);
    let end = run
        .timeline
        .iter()
        .find(|entry| entry.sequence == end_sequence)
        .ok_or_else(|| anyhow!("marker {} not found", end_sequence))?;
    if end.kind != TimelineEntryKind::Marker {
        return Err(anyhow!("timeline entry {} is not a marker", end.sequence));
    }
    if end.sequence < start.sequence {
        return Err(anyhow!(
            "marker span end {} precedes start {}",
            end.sequence,
            start.sequence
        ));
    }
    Ok(DiagnosticsResolvedMarkerSpan {
        start_sequence: start.sequence,
        end_sequence: end.sequence,
        start_unix_ms: start.occurred_unix_ms,
        end_unix_ms: end.occurred_unix_ms,
    })
}

pub(super) fn load_wall_blocking_profile_artifact(
    window: &ProfilingWindowRecord,
) -> Result<Option<(serde_json::Value, WallBlockingProfileArtifact)>> {
    let Some(artifact) = window.artifact.as_ref() else {
        return Ok(None);
    };
    if artifact.kind != "wall_blocking_summary" {
        return Ok(None);
    }
    let bytes = fs::read(&artifact.path)
        .with_context(|| format!("read profiling artifact '{}'", artifact.path))?;
    let summary = serde_json::from_slice::<serde_json::Value>(&bytes)
        .with_context(|| format!("parse profiling artifact '{}'", artifact.path))?;
    let typed = serde_json::from_slice::<WallBlockingProfileArtifact>(&bytes)
        .with_context(|| format!("decode profiling artifact '{}'", artifact.path))?;
    Ok(Some((summary, typed)))
}

pub(super) fn truncate_to_limit<T>(mut items: Vec<T>, limit: usize) -> (Vec<T>, bool) {
    let truncated = items.len() > limit;
    items.truncate(limit);
    (items, truncated)
}

pub(super) fn queue_growth_episode(
    start_event: &DiagnosticsRecentEvent,
    start_pending_uploads: u64,
    end_event: &DiagnosticsRecentEvent,
    end_pending_uploads: u64,
) -> DiagnosticsQueueGrowthEpisode {
    DiagnosticsQueueGrowthEpisode {
        subsystem: SUBSYSTEM_UPLOAD_PIPELINE.to_string(),
        operation_type: OPERATION_QUEUE_GROWTH.to_string(),
        start_event_sequence: start_event.sequence,
        end_event_sequence: end_event.sequence,
        start_pending_uploads,
        end_pending_uploads,
        delta_pending_uploads: end_pending_uploads as i64 - start_pending_uploads as i64,
        supporting_evidence: vec![
            DiagnosticsEvidenceReference {
                source: DiagnosticsEvidenceReferenceSource::RecentEvent,
                id: start_event.sequence.to_string(),
                field: Some("fields.pending_uploads".to_string()),
            },
            DiagnosticsEvidenceReference {
                source: DiagnosticsEvidenceReferenceSource::RecentEvent,
                id: end_event.sequence.to_string(),
                field: Some("fields.pending_uploads".to_string()),
            },
        ],
    }
}

pub(super) fn derive_queue_growth_episodes(
    scope: &DiagnosticsEvidenceScope,
    events: &[DiagnosticsRecentEvent],
    limit: usize,
) -> (Vec<DiagnosticsQueueGrowthEpisode>, bool) {
    if !subsystem_matches(scope.subsystem.as_deref(), SUBSYSTEM_UPLOAD_PIPELINE)
        || !operation_matches(scope.operation_type.as_deref(), OPERATION_QUEUE_GROWTH)
    {
        return (Vec::new(), false);
    }

    let mut episodes = Vec::new();
    let mut current: Option<(&DiagnosticsRecentEvent, u64, &DiagnosticsRecentEvent, u64)> = None;

    for event in events {
        let Some(pending_uploads) = json_pending_uploads(&event.fields) else {
            current = None;
            continue;
        };

        match current {
            Some((start_event, start_pending, _, last_pending))
                if pending_uploads > last_pending =>
            {
                current = Some((start_event, start_pending, event, pending_uploads));
            }
            Some((start_event, start_pending, end_event, end_pending))
                if end_pending > start_pending =>
            {
                episodes.push(queue_growth_episode(
                    start_event,
                    start_pending,
                    end_event,
                    end_pending,
                ));
                current = Some((event, pending_uploads, event, pending_uploads));
            }
            _ => {
                current = Some((event, pending_uploads, event, pending_uploads));
            }
        }
    }

    if let Some((start_event, start_pending, end_event, end_pending)) = current
        && end_pending > start_pending
    {
        episodes.push(queue_growth_episode(
            start_event,
            start_pending,
            end_event,
            end_pending,
        ));
    }

    truncate_to_limit(episodes, limit)
}

pub(super) fn json_pending_uploads(fields: &serde_json::Value) -> Option<u64> {
    match fields {
        serde_json::Value::Object(map) => {
            if let Some(value) = map
                .get("pending_uploads")
                .and_then(serde_json::Value::as_u64)
            {
                return Some(value);
            }
            map.values().find_map(json_pending_uploads)
        }
        serde_json::Value::Array(values) => values.iter().find_map(json_pending_uploads),
        _ => None,
    }
}

pub(super) fn profile_window_reference(
    window: &ProfilingWindowRecord,
) -> DiagnosticsEvidenceReference {
    DiagnosticsEvidenceReference {
        source: DiagnosticsEvidenceReferenceSource::ProfilingWindow,
        id: window.window_id.clone(),
        field: None,
    }
}

pub(super) fn profile_artifact_reference(path: &str, field: &str) -> DiagnosticsEvidenceReference {
    DiagnosticsEvidenceReference {
        source: DiagnosticsEvidenceReferenceSource::ProfilingArtifact,
        id: path.to_string(),
        field: Some(field.to_string()),
    }
}

pub(super) fn runtime_snapshot_reference(field: &str) -> DiagnosticsEvidenceReference {
    DiagnosticsEvidenceReference {
        source: DiagnosticsEvidenceReferenceSource::RuntimeSnapshot,
        id: "runtime_snapshot".to_string(),
        field: Some(field.to_string()),
    }
}

pub(super) fn marker_snapshot_reference(
    sequence: u64,
    field: &str,
) -> DiagnosticsEvidenceReference {
    DiagnosticsEvidenceReference {
        source: DiagnosticsEvidenceReferenceSource::MarkerSnapshot,
        id: sequence.to_string(),
        field: Some(field.to_string()),
    }
}

pub(super) fn qualify_comparison_evidence(
    run_id: &str,
    reference: DiagnosticsEvidenceReference,
) -> DiagnosticsEvidenceReference {
    let needs_run_scope = matches!(
        reference.source,
        DiagnosticsEvidenceReferenceSource::RuntimeSnapshot
            | DiagnosticsEvidenceReferenceSource::RecentEvent
            | DiagnosticsEvidenceReferenceSource::TimelineEntry
            | DiagnosticsEvidenceReferenceSource::ProfilingWindow
            | DiagnosticsEvidenceReferenceSource::ProfilingArtifact
            | DiagnosticsEvidenceReferenceSource::MarkerSnapshot
    );
    if !needs_run_scope {
        return reference;
    }

    DiagnosticsEvidenceReference {
        id: format!("{run_id}:{}", reference.id),
        ..reference
    }
}

pub(super) fn push_blocking_interval(
    output: &mut Vec<DiagnosticsBlockingInterval>,
    scope: &DiagnosticsEvidenceScope,
    subsystem: &str,
    operation_type: &str,
    total_blocked_ms: f64,
    max_blocked_ms: f64,
    supporting_evidence: Vec<DiagnosticsEvidenceReference>,
) {
    if !total_blocked_ms.is_finite()
        || !max_blocked_ms.is_finite()
        || total_blocked_ms <= 0.0
        || !subsystem_matches(scope.subsystem.as_deref(), subsystem)
        || !operation_matches(scope.operation_type.as_deref(), operation_type)
    {
        return;
    }

    output.push(DiagnosticsBlockingInterval {
        subsystem: subsystem.to_string(),
        operation_type: operation_type.to_string(),
        total_blocked_ms,
        max_blocked_ms,
        supporting_evidence,
    });
}

pub(super) fn push_hotspot(
    output: &mut Vec<DiagnosticsHotspotSummary>,
    scope: &DiagnosticsEvidenceScope,
    subsystem: &str,
    operation_type: &str,
    metric: &str,
    score: f64,
    supporting_evidence: Vec<DiagnosticsEvidenceReference>,
) {
    if !score.is_finite()
        || score <= 0.0
        || !subsystem_matches(scope.subsystem.as_deref(), subsystem)
        || !operation_matches(scope.operation_type.as_deref(), operation_type)
    {
        return;
    }

    output.push(DiagnosticsHotspotSummary {
        subsystem: subsystem.to_string(),
        operation_type: operation_type.to_string(),
        metric: metric.to_string(),
        score,
        supporting_evidence,
    });
}

pub(super) fn derive_blocking_intervals(
    scope: &DiagnosticsEvidenceScope,
    snapshot: Option<&DiagnosticsRuntimeSnapshot>,
    profiling_windows: &[(ProfilingWindowRecord, Option<WallBlockingProfileArtifact>)],
    limit: usize,
) -> (Vec<DiagnosticsBlockingInterval>, bool) {
    let mut intervals = Vec::new();
    if scope.object_identity.is_none() {
        if let Some(snapshot) = snapshot {
            push_blocking_interval(
                &mut intervals,
                scope,
                SUBSYSTEM_WRITE_PATH,
                OPERATION_SYNC_DATA_QUEUE_WAIT,
                snapshot
                    .write_path
                    .payload_store_sync_data
                    .queue_wait_total_ms,
                snapshot
                    .write_path
                    .payload_store_sync_data
                    .queue_wait_max_ms,
                vec![runtime_snapshot_reference(
                    "write_path.payload_store_sync_data.queue_wait_total_ms",
                )],
            );
            push_blocking_interval(
                &mut intervals,
                scope,
                SUBSYSTEM_WRITE_PATH,
                OPERATION_WRITE_BACKPRESSURE,
                snapshot.write_path.write_backpressure.wait_total_ms as f64,
                snapshot.write_path.write_backpressure.stall_max_ms as f64,
                vec![runtime_snapshot_reference(
                    "write_path.write_backpressure.wait_total_ms",
                )],
            );
            push_blocking_interval(
                &mut intervals,
                scope,
                SUBSYSTEM_SYNCHRONIZATION,
                OPERATION_UPLOAD_FALLBACK_WAIT,
                snapshot
                    .synchronization
                    .state
                    .foreground_reads
                    .upload_fallback_wait_ns as f64
                    / 1_000_000.0,
                snapshot
                    .synchronization
                    .state
                    .foreground_reads
                    .upload_fallback_wait_max_ns as f64
                    / 1_000_000.0,
                vec![runtime_snapshot_reference(
                    "synchronization.state.foreground_reads.upload_fallback_wait_ns",
                )],
            );
        }
    }

    for (window, artifact) in profiling_windows {
        let Some(artifact) = artifact else {
            continue;
        };
        if !artifact_matches_scope(artifact, scope) {
            continue;
        }
        let artifact_path = window
            .artifact
            .as_ref()
            .map(|artifact| artifact.path.as_str())
            .unwrap_or_default();
        let queue_wait_delta = artifact
            .end_snapshot
            .write_path
            .payload_store_sync_data
            .queue_wait_total_ms
            - artifact
                .start_snapshot
                .write_path
                .payload_store_sync_data
                .queue_wait_total_ms;
        push_blocking_interval(
            &mut intervals,
            scope,
            SUBSYSTEM_WRITE_PATH,
            OPERATION_SYNC_DATA_QUEUE_WAIT,
            queue_wait_delta,
            artifact
                .end_snapshot
                .write_path
                .payload_store_sync_data
                .queue_wait_max_ms,
            vec![
                profile_window_reference(window),
                profile_artifact_reference(
                    artifact_path,
                    "end_snapshot.write_path.payload_store_sync_data.queue_wait_total_ms",
                ),
            ],
        );
        let backpressure_delta = artifact
            .end_snapshot
            .write_path
            .write_backpressure
            .wait_total_ms as f64
            - artifact
                .start_snapshot
                .write_path
                .write_backpressure
                .wait_total_ms as f64;
        push_blocking_interval(
            &mut intervals,
            scope,
            SUBSYSTEM_WRITE_PATH,
            OPERATION_WRITE_BACKPRESSURE,
            backpressure_delta,
            artifact
                .end_snapshot
                .write_path
                .write_backpressure
                .stall_max_ms as f64,
            vec![
                profile_window_reference(window),
                profile_artifact_reference(
                    artifact_path,
                    "end_snapshot.write_path.write_backpressure.wait_total_ms",
                ),
            ],
        );
        let fallback_delta = artifact
            .end_snapshot
            .synchronization
            .state
            .foreground_reads
            .upload_fallback_wait_ns as f64
            / 1_000_000.0
            - artifact
                .start_snapshot
                .synchronization
                .state
                .foreground_reads
                .upload_fallback_wait_ns as f64
                / 1_000_000.0;
        push_blocking_interval(
            &mut intervals,
            scope,
            SUBSYSTEM_SYNCHRONIZATION,
            OPERATION_UPLOAD_FALLBACK_WAIT,
            fallback_delta,
            artifact
                .end_snapshot
                .synchronization
                .state
                .foreground_reads
                .upload_fallback_wait_max_ns as f64
                / 1_000_000.0,
            vec![
                profile_window_reference(window),
                profile_artifact_reference(
                    artifact_path,
                    "end_snapshot.synchronization.state.foreground_reads.upload_fallback_wait_ns",
                ),
            ],
        );
    }

    truncate_to_limit(intervals, limit)
}

pub(super) fn derive_hotspots(
    scope: &DiagnosticsEvidenceScope,
    snapshot: Option<&DiagnosticsRuntimeSnapshot>,
    profiling_windows: &[(ProfilingWindowRecord, Option<WallBlockingProfileArtifact>)],
    limit: usize,
) -> (Vec<DiagnosticsHotspotSummary>, bool) {
    let mut hotspots = Vec::new();
    if scope.object_identity.is_none() {
        if let Some(snapshot) = snapshot {
            push_hotspot(
                &mut hotspots,
                scope,
                SUBSYSTEM_UPLOAD_PIPELINE,
                OPERATION_QUEUE_GROWTH,
                "pending_uploads",
                snapshot.upload.pending_uploads as f64,
                vec![runtime_snapshot_reference("upload.pending_uploads")],
            );
            push_hotspot(
                &mut hotspots,
                scope,
                SUBSYSTEM_WRITE_PATH,
                OPERATION_SYNC_DATA_QUEUE_WAIT,
                "payload_store_sync_data.queue_wait_total_ms",
                snapshot
                    .write_path
                    .payload_store_sync_data
                    .queue_wait_total_ms,
                vec![runtime_snapshot_reference(
                    "write_path.payload_store_sync_data.queue_wait_total_ms",
                )],
            );
            push_hotspot(
                &mut hotspots,
                scope,
                SUBSYSTEM_WRITE_PATH,
                OPERATION_WRITE_BACKPRESSURE,
                "write_backpressure.wait_total_ms",
                snapshot.write_path.write_backpressure.wait_total_ms as f64,
                vec![runtime_snapshot_reference(
                    "write_path.write_backpressure.wait_total_ms",
                )],
            );
            push_hotspot(
                &mut hotspots,
                scope,
                SUBSYSTEM_CACHE,
                OPERATION_PREFETCH_QUEUE,
                "prefetch_queue_depth",
                snapshot.cache.state.prefetch_queue_depth as f64,
                vec![runtime_snapshot_reference(
                    "cache.state.prefetch_queue_depth",
                )],
            );
            push_hotspot(
                &mut hotspots,
                scope,
                SUBSYSTEM_SYNCHRONIZATION,
                OPERATION_METADATA_BACKLOG,
                "pending_metadata_ops",
                snapshot.synchronization.state.pending_metadata_ops as f64,
                vec![runtime_snapshot_reference(
                    "synchronization.state.pending_metadata_ops",
                )],
            );
        }
    }

    for (window, artifact) in profiling_windows {
        let Some(artifact) = artifact else {
            continue;
        };
        if !artifact_matches_scope(artifact, scope) {
            continue;
        }
        let artifact_path = window
            .artifact
            .as_ref()
            .map(|artifact| artifact.path.as_str())
            .unwrap_or_default();
        push_hotspot(
            &mut hotspots,
            scope,
            SUBSYSTEM_WRITE_PATH,
            OPERATION_SYNC_DATA_QUEUE_WAIT,
            "payload_store_sync_data.queue_wait_total_ms_delta",
            artifact
                .end_snapshot
                .write_path
                .payload_store_sync_data
                .queue_wait_total_ms
                - artifact
                    .start_snapshot
                    .write_path
                    .payload_store_sync_data
                    .queue_wait_total_ms,
            vec![
                profile_window_reference(window),
                profile_artifact_reference(
                    artifact_path,
                    "end_snapshot.write_path.payload_store_sync_data.queue_wait_total_ms",
                ),
            ],
        );
        push_hotspot(
            &mut hotspots,
            scope,
            SUBSYSTEM_WRITE_PATH,
            OPERATION_WRITE_BACKPRESSURE,
            "write_backpressure.wait_total_ms_delta",
            artifact
                .end_snapshot
                .write_path
                .write_backpressure
                .wait_total_ms as f64
                - artifact
                    .start_snapshot
                    .write_path
                    .write_backpressure
                    .wait_total_ms as f64,
            vec![
                profile_window_reference(window),
                profile_artifact_reference(
                    artifact_path,
                    "end_snapshot.write_path.write_backpressure.wait_total_ms",
                ),
            ],
        );
        push_hotspot(
            &mut hotspots,
            scope,
            SUBSYSTEM_UPLOAD_PIPELINE,
            OPERATION_QUEUE_GROWTH,
            "pending_uploads_delta",
            artifact.end_snapshot.upload.pending_uploads as f64
                - artifact.start_snapshot.upload.pending_uploads as f64,
            vec![
                profile_window_reference(window),
                profile_artifact_reference(artifact_path, "end_snapshot.upload.pending_uploads"),
            ],
        );
    }

    hotspots.sort_by(|left, right| right.score.total_cmp(&left.score));
    truncate_to_limit(hotspots, limit)
}

pub(super) fn build_evidence_view(
    run_id: &str,
    request: DiagnosticsEvidenceRequest,
    source: &dyn DiagnosticsDataSource,
    store: &InvestigationStore,
) -> Result<DiagnosticsEvidenceView> {
    build_evidence_view_with_snapshot(run_id, request, source.runtime_snapshot(), source, store)
}

pub(super) fn build_evidence_view_with_snapshot(
    run_id: &str,
    request: DiagnosticsEvidenceRequest,
    snapshot: DiagnosticsRuntimeSnapshot,
    source: &dyn DiagnosticsDataSource,
    store: &InvestigationStore,
) -> Result<DiagnosticsEvidenceView> {
    let run = store.load_run(run_id)?;
    let applied_budget = clamp_evidence_budget(&request.budget);
    let resolved_marker_span = request
        .scope
        .marker_span
        .as_ref()
        .map(|span| resolve_marker_span(&run, span))
        .transpose()?;
    let scope_resolution = DiagnosticsEvidenceScopeResolution {
        subsystem: request.scope.subsystem.clone(),
        operation_type: request.scope.operation_type.clone(),
        object_identity: request.scope.object_identity.clone(),
        marker_span: resolved_marker_span.clone(),
    };

    let recent_event_candidates = source.recent_events(MAX_EVIDENCE_RECENT_EVENTS_LIMIT);
    let filtered_recent_events = recent_event_candidates
        .into_iter()
        .filter(|event| event_matches_scope(event, &request.scope, resolved_marker_span.as_ref()))
        .collect::<Vec<_>>();
    let (recent_events, recent_events_truncated) =
        truncate_to_limit(filtered_recent_events, applied_budget.max_recent_events);

    let filtered_timeline_entries = run
        .timeline
        .iter()
        .filter(|entry| {
            resolved_marker_span.as_ref().is_none_or(|span| {
                entry.sequence >= span.start_sequence && entry.sequence <= span.end_sequence
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let (timeline_entries, timeline_entries_truncated) = truncate_to_limit(
        filtered_timeline_entries,
        applied_budget.max_timeline_entries,
    );

    let filtered_profiling_windows = run
        .profiling_windows
        .iter()
        .filter(|window| {
            resolved_marker_span.as_ref().is_none_or(|span| {
                window.occurred_unix_ms >= span.start_unix_ms
                    && window.occurred_unix_ms <= span.end_unix_ms
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let profiling_windows_requested_too_large =
        request.budget.max_profiling_windows > applied_budget.max_profiling_windows;
    let derived_items_requested_too_large =
        request.budget.max_derived_items_per_kind > applied_budget.max_derived_items_per_kind;
    let (selected_windows, profiling_windows_truncated_by_count) = truncate_to_limit(
        filtered_profiling_windows,
        applied_budget.max_profiling_windows,
    );
    let mut profiling_windows = Vec::with_capacity(selected_windows.len());
    let mut typed_profiling_windows = Vec::with_capacity(selected_windows.len());
    for window in selected_windows {
        let (artifact, load_error) = match load_wall_blocking_profile_artifact(&window) {
            Ok(artifact) => (artifact, None),
            Err(error) => {
                warn!(
                    window_id = %window.window_id,
                    artifact_path = ?window.artifact.as_ref().map(|artifact| artifact.path.as_str()),
                    error = %error,
                    "failed to load wall-blocking profiling artifact for evidence view"
                );
                (None, Some(error.to_string()))
            }
        };
        let summary = artifact.as_ref().map(|(summary, _)| summary.clone());
        let typed = artifact.map(|(_, typed)| typed);
        typed_profiling_windows.push((window.clone(), typed));
        profiling_windows.push(DiagnosticsEvidenceProfilingWindow {
            window,
            summary,
            load_error,
        });
    }

    let snapshot_in_view = resolved_marker_span.is_none();
    let snapshot_sections =
        snapshot_sections_for_scope(&snapshot, &request.scope, resolved_marker_span.as_ref());

    let (queue_growth, queue_growth_truncated) = derive_queue_growth_episodes(
        &request.scope,
        &recent_events,
        applied_budget.max_derived_items_per_kind,
    );
    let (blocking_intervals, blocking_intervals_truncated) = derive_blocking_intervals(
        &request.scope,
        snapshot_in_view.then_some(&snapshot),
        &typed_profiling_windows,
        applied_budget.max_derived_items_per_kind,
    );
    let (hotspots, hotspots_truncated) = derive_hotspots(
        &request.scope,
        snapshot_in_view.then_some(&snapshot),
        &typed_profiling_windows,
        applied_budget.max_derived_items_per_kind,
    );

    Ok(DiagnosticsEvidenceView {
        generated_unix_ms: now_unix_millis(),
        run_id: run_id.to_string(),
        scope: scope_resolution,
        budget: DiagnosticsEvidenceBudgetReport {
            requested: request.budget.clone(),
            applied: applied_budget.clone(),
            truncation: DiagnosticsEvidenceTruncation {
                recent_events: recent_events_truncated
                    || request.budget.max_recent_events > applied_budget.max_recent_events,
                timeline_entries: timeline_entries_truncated
                    || request.budget.max_timeline_entries > applied_budget.max_timeline_entries,
                profiling_windows: profiling_windows_truncated_by_count
                    || profiling_windows_requested_too_large,
                queue_growth: queue_growth_truncated || derived_items_requested_too_large,
                blocking_intervals: blocking_intervals_truncated
                    || derived_items_requested_too_large,
                hotspots: hotspots_truncated || derived_items_requested_too_large,
            },
        },
        raw: DiagnosticsEvidenceRawView {
            snapshot_sections,
            recent_events,
            timeline_entries,
            profiling_windows,
        },
        derived: DiagnosticsDerivedEvidenceView {
            queue_growth,
            blocking_intervals,
            hotspots,
        },
    })
}

pub(super) fn truncate_report_items<T>(
    items: Vec<T>,
    limit: usize,
) -> (Vec<T>, InvestigationReportSectionSummary) {
    let total_count = items.len();
    let included_count = total_count.min(limit);
    let truncated_count = total_count.saturating_sub(included_count);
    (
        items.into_iter().take(limit).collect(),
        InvestigationReportSectionSummary {
            included_count,
            total_count,
            truncated_count,
        },
    )
}

pub(super) fn timeline_entry_reference(entry: &TimelineEntry) -> DiagnosticsEvidenceReference {
    DiagnosticsEvidenceReference {
        source: DiagnosticsEvidenceReferenceSource::TimelineEntry,
        id: entry.sequence.to_string(),
        field: None,
    }
}

pub(super) fn is_legacy_anomaly_reason(reason: Option<&str>) -> bool {
    matches!(reason, Some("annotate anomaly" | "annotate_anomaly"))
}

pub(super) fn is_anomaly_timeline_entry(entry: &TimelineEntry) -> bool {
    entry.kind == TimelineEntryKind::Anomaly
        || (entry.kind == TimelineEntryKind::Note
            && is_legacy_anomaly_reason(entry.reason.as_deref()))
}

pub(super) fn build_report_anomaly_candidates(
    run: &InvestigationRun,
    derived_view: &DiagnosticsEvidenceView,
) -> Vec<InvestigationReportFinding> {
    let mut anomalies = derived_view
        .derived
        .queue_growth
        .iter()
        .map(|episode| InvestigationReportFinding {
            title: format!(
                "{} {}",
                episode.subsystem.replace('_', " "),
                episode.operation_type.replace('_', " ")
            ),
            summary: format!(
                "Pending uploads moved from {} to {} across events {}-{}.",
                episode.start_pending_uploads,
                episode.end_pending_uploads,
                episode.start_event_sequence,
                episode.end_event_sequence
            ),
            supporting_evidence: episode.supporting_evidence.clone(),
        })
        .collect::<Vec<_>>();
    anomalies.extend(
        run.timeline
            .iter()
            .filter(|entry| is_anomaly_timeline_entry(entry))
            .map(|entry| InvestigationReportFinding {
                title: entry.title.clone(),
                summary: entry
                    .body
                    .clone()
                    .unwrap_or_else(|| "Operator annotated a runtime anomaly.".to_string()),
                supporting_evidence: vec![timeline_entry_reference(entry)],
            }),
    );
    anomalies.extend(
        derived_view
            .raw
            .profiling_windows
            .iter()
            .filter_map(|window| {
                window
                    .load_error
                    .as_ref()
                    .map(|error| InvestigationReportFinding {
                        title: format!("profiling window {}", window.window.window_id),
                        summary: format!("Profiling artifact could not be reloaded: {error}"),
                        supporting_evidence: vec![DiagnosticsEvidenceReference {
                            source: DiagnosticsEvidenceReferenceSource::ProfilingWindow,
                            id: window.window.window_id.clone(),
                            field: None,
                        }],
                    })
            }),
    );
    anomalies
}

pub(super) fn build_report_bottleneck_candidates(
    derived_view: &DiagnosticsEvidenceView,
) -> Vec<InvestigationReportFinding> {
    let mut candidate_bottlenecks = derived_view
        .derived
        .blocking_intervals
        .iter()
        .map(|interval| InvestigationReportFinding {
            title: format!(
                "{} {}",
                interval.subsystem.replace('_', " "),
                interval.operation_type.replace('_', " ")
            ),
            summary: format!(
                "Observed {:.1} ms blocked with a {:.1} ms max interval.",
                interval.total_blocked_ms, interval.max_blocked_ms
            ),
            supporting_evidence: interval.supporting_evidence.clone(),
        })
        .collect::<Vec<_>>();
    candidate_bottlenecks.extend(derived_view.derived.hotspots.iter().map(|hotspot| {
        InvestigationReportFinding {
            title: format!(
                "{} {}",
                hotspot.subsystem.replace('_', " "),
                hotspot.operation_type.replace('_', " ")
            ),
            summary: format!(
                "Hotspot metric '{}' scored {:.1}.",
                hotspot.metric, hotspot.score
            ),
            supporting_evidence: hotspot.supporting_evidence.clone(),
        }
    }));
    candidate_bottlenecks
}

pub(super) fn build_report_selected_evidence_candidates(
    run: &InvestigationRun,
    derived_view: &DiagnosticsEvidenceView,
) -> Vec<InvestigationReportSelectedEvidence> {
    let mut selected_evidence = Vec::new();
    if !derived_view.raw.snapshot_sections.is_empty() {
        let sections = derived_view
            .raw
            .snapshot_sections
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        selected_evidence.push(InvestigationReportSelectedEvidence {
            label: "runtime snapshot".to_string(),
            summary: format!("Captured snapshot sections: {sections}."),
            supporting_evidence: vec![DiagnosticsEvidenceReference {
                source: DiagnosticsEvidenceReferenceSource::RuntimeSnapshot,
                id: "runtime_snapshot".to_string(),
                field: None,
            }],
        });
    }
    if let Some(event) = derived_view.raw.recent_events.last() {
        selected_evidence.push(InvestigationReportSelectedEvidence {
            label: format!("recent event {}", event.sequence),
            summary: format!("{} / {}: {}", event.subsystem, event.kind, event.title),
            supporting_evidence: vec![DiagnosticsEvidenceReference {
                source: DiagnosticsEvidenceReferenceSource::RecentEvent,
                id: event.sequence.to_string(),
                field: None,
            }],
        });
    }
    selected_evidence.extend(run.profiling_windows.iter().map(|window| {
        InvestigationReportSelectedEvidence {
            label: format!("profiling window {}", window.window_id),
            summary: format!(
                "{:?} {:?} status {:?}",
                window.capture_mode, window.preset, window.status
            ),
            supporting_evidence: vec![DiagnosticsEvidenceReference {
                source: DiagnosticsEvidenceReferenceSource::ProfilingWindow,
                id: window.window_id.clone(),
                field: None,
            }],
        }
    }));
    selected_evidence
}

pub(super) fn build_report_next_action_candidates(
    run: &InvestigationRun,
    marker_timeline: &[TimelineEntry],
    derived_view: &DiagnosticsEvidenceView,
) -> Vec<InvestigationRecommendedNextAction> {
    let mut recommended_next_actions = Vec::new();
    if run.profiling_windows.is_empty() {
        recommended_next_actions.push(InvestigationRecommendedNextAction {
            title: "capture a balanced wall-blocking profile".to_string(),
            rationale:
                "The run has no profiling captures yet, so a follow-up capture would strengthen the report."
                    .to_string(),
            supporting_evidence: Vec::new(),
        });
    }
    if let Some(interval) = derived_view
        .derived
        .blocking_intervals
        .iter()
        .max_by(|left, right| left.total_blocked_ms.total_cmp(&right.total_blocked_ms))
    {
        recommended_next_actions.push(InvestigationRecommendedNextAction {
            title: format!(
                "drill into {} {}",
                interval.subsystem.replace('_', " "),
                interval.operation_type.replace('_', " ")
            ),
            rationale: format!(
                "This interval accumulated {:.1} ms of blocked time and is the strongest current bottleneck candidate.",
                interval.total_blocked_ms
            ),
            supporting_evidence: interval.supporting_evidence.clone(),
        });
    }
    if marker_timeline.is_empty() {
        recommended_next_actions.push(InvestigationRecommendedNextAction {
            title: "add workload markers on the next run".to_string(),
            rationale:
                "Marker spans will make the next report easier to compare and easier to scope for focused evidence views."
                    .to_string(),
            supporting_evidence: Vec::new(),
        });
    }
    if recommended_next_actions.is_empty() {
        recommended_next_actions.push(InvestigationRecommendedNextAction {
            title: "review the selected evidence with engineering".to_string(),
            rationale:
                "The report already contains enough concrete evidence to drive a focused follow-up discussion."
                    .to_string(),
            supporting_evidence: Vec::new(),
        });
    }
    recommended_next_actions
}

/// Build the first durable single-run report from persisted run data and the
/// current diagnostics-derived evidence view, keeping the artifact concise while
/// recording when any section had to be truncated for readability.
pub(super) fn materialize_report(
    run_id: &str,
    source: &dyn DiagnosticsDataSource,
    store: &InvestigationStore,
) -> Result<InvestigationReport> {
    let run = store.load_run(run_id)?;
    let runtime_snapshot = source.runtime_snapshot();
    let derived_view = build_evidence_view_with_snapshot(
        run_id,
        DiagnosticsEvidenceRequest {
            scope: DiagnosticsEvidenceScope::default(),
            budget: DiagnosticsEvidenceBudget::default(),
        },
        runtime_snapshot.clone(),
        source,
        store,
    )?;
    let marker_timeline = run
        .timeline
        .iter()
        .filter(|entry| entry.kind == TimelineEntryKind::Marker)
        .cloned()
        .collect::<Vec<_>>();
    let evidence_summary = InvestigationReportEvidenceSummary {
        recent_event_count: derived_view.raw.recent_events.len(),
        timeline_entry_count: derived_view.raw.timeline_entries.len(),
        profiling_window_count: derived_view.raw.profiling_windows.len(),
        queue_growth_count: derived_view.derived.queue_growth.len(),
        blocking_interval_count: derived_view.derived.blocking_intervals.len(),
        hotspot_count: derived_view.derived.hotspots.len(),
    };
    let (anomalies, anomaly_summary) = truncate_report_items(
        build_report_anomaly_candidates(&run, &derived_view),
        MAX_REPORT_ANOMALIES,
    );
    let (candidate_bottlenecks, candidate_bottleneck_summary) = truncate_report_items(
        build_report_bottleneck_candidates(&derived_view),
        MAX_REPORT_CANDIDATE_BOTTLENECKS,
    );
    let (selected_evidence, selected_evidence_summary) = truncate_report_items(
        build_report_selected_evidence_candidates(&run, &derived_view),
        MAX_REPORT_SELECTED_EVIDENCE,
    );
    let (recommended_next_actions, next_action_summary) = truncate_report_items(
        build_report_next_action_candidates(&run, &marker_timeline, &derived_view),
        MAX_REPORT_RECOMMENDED_NEXT_ACTIONS,
    );

    store.append_report(
        run_id,
        NewInvestigationReport {
            environment: run.fingerprint.clone(),
            marker_timeline,
            evidence_summary,
            truncation: InvestigationReportTruncationSummary {
                anomalies: anomaly_summary,
                candidate_bottlenecks: candidate_bottleneck_summary,
                selected_evidence: selected_evidence_summary,
                recommended_next_actions: next_action_summary,
            },
            anomalies,
            candidate_bottlenecks,
            selected_evidence,
            recommended_next_actions,
            references: InvestigationReportReferences {
                runtime_snapshot,
                derived_view,
                profiling_windows: run.profiling_windows.clone(),
            },
        },
    )
}

pub(super) fn marker_snapshot_by_title<'a>(
    run: &'a InvestigationRun,
    title: &str,
) -> Result<&'a crate::investigation::InvestigationMarkerSnapshot> {
    let mut matches = run
        .marker_snapshots
        .iter()
        .filter(|snapshot| snapshot.marker_title == title);
    let Some(snapshot) = matches.next() else {
        return Err(anyhow!(
            "marker snapshot '{}' not found for run {}",
            title,
            run.id
        ));
    };
    if matches.next().is_some() {
        return Err(anyhow!(
            "marker snapshot '{}' is ambiguous for run {}",
            title,
            run.id
        ));
    }
    Ok(snapshot)
}

pub(super) fn count_anomalies_in_span(
    run: &InvestigationRun,
    start_sequence: u64,
    end_sequence: u64,
) -> Vec<(String, DiagnosticsEvidenceReference)> {
    run.timeline
        .iter()
        .filter(|entry| {
            is_anomaly_timeline_entry(entry)
                && entry.sequence >= start_sequence
                && entry.sequence <= end_sequence
        })
        .map(|entry| {
            (
                entry.title.clone(),
                qualify_comparison_evidence(&run.id, timeline_entry_reference(entry)),
            )
        })
        .collect()
}

pub(super) fn profiling_windows_in_span(
    run: &InvestigationRun,
    start_sequence: u64,
    end_sequence: u64,
) -> Vec<&ProfilingWindowRecord> {
    let Some(start) = run
        .timeline
        .iter()
        .find(|entry| entry.sequence == start_sequence)
    else {
        return Vec::new();
    };
    let Some(end) = run
        .timeline
        .iter()
        .find(|entry| entry.sequence == end_sequence)
    else {
        return Vec::new();
    };
    run.profiling_windows
        .iter()
        .filter(|window| {
            window.occurred_unix_ms >= start.occurred_unix_ms
                && window.occurred_unix_ms <= end.occurred_unix_ms
        })
        .collect()
}

pub(super) fn collect_hotspots_for_windows(
    windows: &[&ProfilingWindowRecord],
) -> Result<Vec<DiagnosticsHotspotSummary>> {
    let mut loaded = Vec::new();
    for window in windows {
        match load_wall_blocking_profile_artifact(window) {
            Ok(Some((_summary, artifact))) => loaded.push(((*window).clone(), Some(artifact))),
            Ok(None) => {}
            Err(error) => {
                warn!(
                    window_id = %window.window_id,
                    error = %error,
                    "failed to load wall-blocking profile artifact while collecting comparison hotspots"
                );
                return Err(error);
            }
        }
    }
    Ok(derive_hotspots(
        &DiagnosticsEvidenceScope::default(),
        None,
        &loaded,
        MAX_EVIDENCE_DERIVED_ITEMS_PER_KIND,
    )
    .0)
}

pub(super) fn materialize_run_span_comparison(
    request: DiagnosticsCompareRunSpanRequest,
    store: &InvestigationStore,
) -> Result<InvestigationSpanComparison> {
    let baseline = store.load_run(&request.baseline_run_id)?;
    let candidate = store.load_run(&request.candidate_run_id)?;
    if baseline.case_id != candidate.case_id {
        return Err(anyhow!(
            "runs {} and {} belong to different investigation cases",
            baseline.id,
            candidate.id
        ));
    }
    let baseline_start = marker_snapshot_by_title(&baseline, &request.start_marker_title)?;
    let candidate_start = marker_snapshot_by_title(&candidate, &request.start_marker_title)?;
    let baseline_end = marker_snapshot_by_title(&baseline, &request.end_marker_title)?;
    let candidate_end = marker_snapshot_by_title(&candidate, &request.end_marker_title)?;
    if baseline_end.marker_sequence < baseline_start.marker_sequence {
        return Err(anyhow!("baseline marker span ordering is invalid"));
    }
    if candidate_end.marker_sequence < candidate_start.marker_sequence {
        return Err(anyhow!("candidate marker span ordering is invalid"));
    }

    let metric_deltas = [
        (
            "upload.pending_uploads_delta",
            baseline_end.runtime_snapshot.upload.pending_uploads as f64
                - baseline_start.runtime_snapshot.upload.pending_uploads as f64,
            candidate_end.runtime_snapshot.upload.pending_uploads as f64
                - candidate_start.runtime_snapshot.upload.pending_uploads as f64,
            "upload.pending_uploads",
        ),
        (
            "write_path.payload_store_sync_data.queue_wait_total_ms_delta",
            baseline_end
                .runtime_snapshot
                .write_path
                .payload_store_sync_data
                .queue_wait_total_ms
                - baseline_start
                    .runtime_snapshot
                    .write_path
                    .payload_store_sync_data
                    .queue_wait_total_ms,
            candidate_end
                .runtime_snapshot
                .write_path
                .payload_store_sync_data
                .queue_wait_total_ms
                - candidate_start
                    .runtime_snapshot
                    .write_path
                    .payload_store_sync_data
                    .queue_wait_total_ms,
            "write_path.payload_store_sync_data.queue_wait_total_ms",
        ),
        (
            "write_path.write_backpressure.wait_total_ms_delta",
            baseline_end
                .runtime_snapshot
                .write_path
                .write_backpressure
                .wait_total_ms as f64
                - baseline_start
                    .runtime_snapshot
                    .write_path
                    .write_backpressure
                    .wait_total_ms as f64,
            candidate_end
                .runtime_snapshot
                .write_path
                .write_backpressure
                .wait_total_ms as f64
                - candidate_start
                    .runtime_snapshot
                    .write_path
                    .write_backpressure
                    .wait_total_ms as f64,
            "write_path.write_backpressure.wait_total_ms",
        ),
    ]
    .into_iter()
    .map(
        |(metric, baseline_value, candidate_value, field)| InvestigationComparisonMetricDelta {
            metric: metric.to_string(),
            baseline_value,
            candidate_value,
            delta: candidate_value - baseline_value,
            supporting_evidence: vec![
                qualify_comparison_evidence(
                    &baseline.id,
                    marker_snapshot_reference(baseline_start.marker_sequence, field),
                ),
                qualify_comparison_evidence(
                    &baseline.id,
                    marker_snapshot_reference(baseline_end.marker_sequence, field),
                ),
                qualify_comparison_evidence(
                    &candidate.id,
                    marker_snapshot_reference(candidate_start.marker_sequence, field),
                ),
                qualify_comparison_evidence(
                    &candidate.id,
                    marker_snapshot_reference(candidate_end.marker_sequence, field),
                ),
            ],
        },
    )
    .collect::<Vec<_>>();

    let baseline_anomalies = count_anomalies_in_span(
        &baseline,
        baseline_start.marker_sequence,
        baseline_end.marker_sequence,
    );
    let candidate_anomalies = count_anomalies_in_span(
        &candidate,
        candidate_start.marker_sequence,
        candidate_end.marker_sequence,
    );
    let mut anomaly_titles = baseline_anomalies
        .iter()
        .map(|(title, _)| title.clone())
        .collect::<Vec<_>>();
    anomaly_titles.extend(candidate_anomalies.iter().map(|(title, _)| title.clone()));
    anomaly_titles.sort();
    anomaly_titles.dedup();
    let anomaly_deltas = anomaly_titles
        .into_iter()
        .filter_map(|title| {
            let baseline_refs = baseline_anomalies
                .iter()
                .filter(|(candidate_title, _)| *candidate_title == title)
                .map(|(_, evidence)| evidence.clone())
                .collect::<Vec<_>>();
            let candidate_refs = candidate_anomalies
                .iter()
                .filter(|(candidate_title, _)| *candidate_title == title)
                .map(|(_, evidence)| evidence.clone())
                .collect::<Vec<_>>();
            let baseline_count = baseline_refs.len();
            let candidate_count = candidate_refs.len();
            let delta = candidate_count as i64 - baseline_count as i64;
            (baseline_count > 0 || candidate_count > 0).then_some(
                InvestigationComparisonAnomalyDelta {
                    title,
                    baseline_count,
                    candidate_count,
                    delta,
                    supporting_evidence: baseline_refs.into_iter().chain(candidate_refs).collect(),
                },
            )
        })
        .collect::<Vec<_>>();

    let baseline_hotspots = collect_hotspots_for_windows(&profiling_windows_in_span(
        &baseline,
        baseline_start.marker_sequence,
        baseline_end.marker_sequence,
    ))?;
    let candidate_hotspots = collect_hotspots_for_windows(&profiling_windows_in_span(
        &candidate,
        candidate_start.marker_sequence,
        candidate_end.marker_sequence,
    ))?;
    let mut hotspot_keys = baseline_hotspots
        .iter()
        .map(|hotspot| {
            (
                hotspot.subsystem.clone(),
                hotspot.operation_type.clone(),
                hotspot.metric.clone(),
            )
        })
        .collect::<Vec<_>>();
    hotspot_keys.extend(candidate_hotspots.iter().map(|hotspot| {
        (
            hotspot.subsystem.clone(),
            hotspot.operation_type.clone(),
            hotspot.metric.clone(),
        )
    }));
    hotspot_keys.sort();
    hotspot_keys.dedup();
    let hotspot_deltas = hotspot_keys
        .into_iter()
        .map(|(subsystem, operation_type, metric)| {
            let baseline_match = baseline_hotspots.iter().find(|hotspot| {
                hotspot.subsystem == subsystem
                    && hotspot.operation_type == operation_type
                    && hotspot.metric == metric
            });
            let candidate_match = candidate_hotspots.iter().find(|hotspot| {
                hotspot.subsystem == subsystem
                    && hotspot.operation_type == operation_type
                    && hotspot.metric == metric
            });
            let baseline_score = baseline_match.map(|hotspot| hotspot.score).unwrap_or(0.0);
            let candidate_score = candidate_match.map(|hotspot| hotspot.score).unwrap_or(0.0);
            InvestigationComparisonHotspotDelta {
                subsystem,
                operation_type,
                metric,
                baseline_score,
                candidate_score,
                delta: candidate_score - baseline_score,
                supporting_evidence: baseline_match
                    .into_iter()
                    .flat_map(|hotspot| {
                        hotspot
                            .supporting_evidence
                            .clone()
                            .into_iter()
                            .map(|reference| qualify_comparison_evidence(&baseline.id, reference))
                    })
                    .chain(candidate_match.into_iter().flat_map(|hotspot| {
                        hotspot
                            .supporting_evidence
                            .clone()
                            .into_iter()
                            .map(|reference| qualify_comparison_evidence(&candidate.id, reference))
                    }))
                    .collect(),
            }
        })
        .collect::<Vec<_>>();

    let summary = InvestigationSpanComparisonSummary {
        metric_deltas,
        anomaly_deltas,
        hotspot_deltas,
    };
    let span = InvestigationMatchedMarkerSpan {
        start_marker_title: request.start_marker_title,
        end_marker_title: request.end_marker_title,
        baseline_start_sequence: baseline_start.marker_sequence,
        baseline_end_sequence: baseline_end.marker_sequence,
        candidate_start_sequence: candidate_start.marker_sequence,
        candidate_end_sequence: candidate_end.marker_sequence,
    };
    let supporting_evidence = summary
        .metric_deltas
        .iter()
        .flat_map(|item| item.supporting_evidence.clone())
        .chain(
            summary
                .anomaly_deltas
                .iter()
                .flat_map(|item| item.supporting_evidence.clone()),
        )
        .chain(
            summary
                .hotspot_deltas
                .iter()
                .flat_map(|item| item.supporting_evidence.clone()),
        )
        .collect::<Vec<_>>();

    store.append_span_comparison(
        &baseline.case_id,
        NewInvestigationSpanComparison {
            baseline_run_id: baseline.id,
            candidate_run_id: candidate.id,
            span,
            summary,
            supporting_evidence,
        },
    )
}

pub(super) fn append_safe_action_audit(
    store: &InvestigationStore,
    run_id: &str,
    action: SafeDebugActionKind,
    actor: Option<&str>,
    reason: Option<&str>,
    outcome: &SafeDebugActionOutcome,
) -> Result<()> {
    #[cfg(test)]
    if let Some(message) = test_safe_action_audit_failure_message() {
        return Err(anyhow!(message));
    }

    let body = SafeActionAuditBody {
        status: outcome.status,
        summary: outcome.summary.clone(),
        artifact: outcome.artifact.clone(),
        error: outcome.error.clone(),
        audit_error: outcome.audit_error.clone(),
    };
    store.append_timeline_entry(
        run_id,
        NewTimelineEntry {
            source: TimelineEntrySource::Mcp,
            kind: TimelineEntryKind::Action,
            title: match action {
                SafeDebugActionKind::SetLogVerbosity => "set_log_verbosity".to_string(),
                SafeDebugActionKind::CaptureBundle => "capture_bundle".to_string(),
                SafeDebugActionKind::ResetObservabilityCounters => {
                    "reset_observability_counters".to_string()
                }
                SafeDebugActionKind::LightweightSelfCheck => "lightweight_self_check".to_string(),
            },
            body: Some(serde_json::to_string(&body)?),
            actor: actor.map(str::to_string),
            reason: reason.map(str::to_string),
        },
    )?;
    Ok(())
}

pub(super) fn capture_bundle_artifact(
    session: &DiagnosticsSessionInfo,
    snapshot: &DiagnosticsRuntimeSnapshot,
    recent_events: &[DiagnosticsRecentEvent],
) -> Result<ProfileArtifactRecord> {
    let output_dir = PathBuf::from(&session.cache_path).join("diagnostics-bundles");
    fs::create_dir_all(&output_dir).with_context(|| {
        format!(
            "create diagnostics bundle directory '{}'",
            output_dir.display()
        )
    })?;
    let path = output_dir.join(format!(
        "bundle-{}-{}.json",
        now_unix_millis(),
        Uuid::new_v4()
    ));
    let payload = serde_json::json!({
        "schema_version": 1,
        "session": session,
        "runtime_snapshot": snapshot,
        "recent_events": recent_events,
    });
    fs::write(&path, serde_json::to_vec_pretty(&payload)?)
        .with_context(|| format!("write diagnostics bundle '{}'", path.display()))?;
    let bytes = fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
    Ok(ProfileArtifactRecord {
        kind: "diagnostics_bundle".to_string(),
        path: path.to_string_lossy().into_owned(),
        bytes,
    })
}
