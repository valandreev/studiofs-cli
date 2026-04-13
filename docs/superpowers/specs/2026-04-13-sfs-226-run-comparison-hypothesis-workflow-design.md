# SFS-226 Run Comparison And Hypothesis Workflow Design

## Goal

Turn isolated investigation runs into a repeatable before-and-after workflow by
adding durable run comparison artifacts and structured hypotheses that can be
updated through the existing controller-gated diagnostics surface.

## Existing Base

- `crates/studiofs/src/investigation.rs` already persists investigation cases,
  runs, timeline entries, profiling windows, and single-run reports.
- `crates/studiofs/src/diagnostics.rs` already enforces controller leases and
  materializes evidence views and reports from runtime snapshots, timeline
  markers, recent events, and profiling artifacts.
- Marker spans already exist as a scoped evidence concept through
  `DiagnosticsMarkerSpan` and `resolve_marker_span`, but the output is limited to
  single-run evidence and reports.

## Chosen Approach

Use marker-boundary snapshots as the durable anchor for cross-run comparison.
When a marker is appended to a run, the diagnostics host captures a lightweight
runtime snapshot and stores it alongside the marker sequence. A later comparison
request matches equivalent spans between two runs by marker title and sequence
ordering, then computes before-and-after deltas using the persisted boundary
snapshots plus the existing evidence/report pipeline for the content inside the
span.

Everything remains controller-gated. There is no second offline write API for
comparisons or hypotheses in this slice.

## Data Model

### Marker Snapshots

Each run gains durable marker-snapshot records with:

- marker timeline sequence
- marker title
- captured unix timestamp
- runtime snapshot at the marker boundary

This makes marker spans durable and comparable after the live mount exits.

### Span Comparisons

Add a durable comparison artifact at the case level that stores:

- baseline run id
- candidate run id
- matched marker-span descriptor
- metric deltas derived from boundary snapshots
- anomaly deltas based on the single-run reports/evidence views for the span
- hotspot deltas based on the derived hotspot summaries for the span
- supporting evidence references

The comparison is stored so later hypothesis updates can link back to exactly
which before-and-after view motivated the change.

### Hypotheses

Add case-level hypotheses with:

- stable id
- statement
- expected signal
- current status
- linked evidence references
- status history entries that record actor, reason, old status, new status, and
  supporting evidence

The hypothesis belongs to the case, not a single run, because it must survive
multiple reruns and comparisons.

## Matching Equivalent Marker Spans

Equivalent spans are matched conservatively:

1. Find the start marker in each run by exact marker title.
2. If an end marker is provided, find the matching end marker by exact title.
3. Require the start and end ordering to be valid in both runs.
4. Use the stored marker-boundary snapshots for start/end deltas.
5. Use the existing evidence-view/report machinery scoped to those marker spans
   to summarize anomalies and hotspots inside the window.

This slice intentionally avoids fuzzy matching across renamed markers or
cross-case comparisons.

## Diagnostics Surface

Extend the controller-only diagnostics protocol with:

- create/list/update hypothesis operations
- compare-runs operation for one matched marker span

The hidden internal CLI should expose matching helper commands for local
development and tests, following the existing `diagnostics-*` pattern.

## Testing Strategy

- `crates/studiofs/src/investigation.rs`
  - case persistence for marker snapshots, span comparisons, and hypotheses
  - hypothesis status history and evidence linkage
- `crates/studiofs/src/diagnostics.rs`
  - span matching success and rejection cases
  - comparison deltas for metrics, anomalies, and hotspots
  - hypothesis lifecycle mutations through the diagnostics protocol
- `crates/studiofs/src/cli/internal.rs`
  - hidden CLI parsing for comparison and hypothesis commands

## Non-Goals

- No fuzzy marker matching or cross-case comparison.
- No standalone offline mutation API outside the diagnostics controller flow.
- No automatic Linear issue creation in this slice.
