# SFS-221 Runtime Snapshot Surface Design

## Goal

Expose a stable daemon-side diagnostics surface that lets an attached client read
structured runtime snapshots for the StudioFS subsystems involved in cache and
sync investigations, plus a bounded recent-events window that does not require
parsing unbounded logs.

## Existing Base

- `crates/studiofs/src/diagnostics.rs` already publishes local attach discovery,
  session info, controller lease management, and a lightweight `Health` RPC.
- `crates/studiofs/src/cli/mount/session.rs` already verifies a mounted session
  can be discovered and queried by an attached client.
- `crates/studiofs-vfs/src/vfs/workers.rs` already computes a rich periodic
  upload/cache/sync summary for logs.
- `crates/studiofs-vfs` already exposes reusable structured snapshots for
  ingress, write path, upload state, foreground read protection, and file
  session state.

## Design

### Diagnostics Protocol

Keep the existing `Health` request/response unchanged for compatibility.

Add two new RPCs:

- `RuntimeSnapshot`
- `RecentEvents { limit }`

The diagnostics client will gain matching helpers so the MCP adapter can target
these stable protocol operations directly.

### Stable Snapshot Schema

Add a new top-level diagnostics envelope in `crates/studiofs/src/diagnostics.rs`
with these sections:

- `ingress`
- `write_path`
- `upload`
- `cache`
- `synchronization`

`cache` will aggregate existing cache and queue counters that are already
sampled in `background_upload_stats`, including upload queue metrics, namespace
cache occupancy and hit/miss counters, local backend dirty object count, and
transit-memory usage.

`synchronization` will aggregate existing synchronization-adjacent state:
foreground read protection, file-session state, and metadata backlog counters.

The schema should be additive-friendly and serializable so later comparison and
reporting work can build on it without reshaping the first client contract.

### Recent Events

Introduce a small bounded in-memory recent-event buffer on the daemon side.

The first event source will be the existing `background_upload_stats` worker in
`crates/studiofs-vfs/src/vfs/workers.rs`. It already centralizes the highest
value cache/upload/sync observability pass, so it can emit compact structured
events without spreading new instrumentation across unrelated hot paths.

The worker should record:

- activation of upload-pipeline activity
- periodic active samples
- the first idle transition after active work

Events remain bounded by count, are cheap to skip when no client asks for them,
and avoid adding unbounded log parsing to the attach workflow.

## Testing Strategy

- Extend diagnostics unit tests in `crates/studiofs/src/diagnostics.rs` to
  verify runtime snapshot availability and schema shape.
- Add bounded recent-event retrieval tests in the same module.
- Extend the mount attach integration in
  `crates/studiofs/src/cli/mount/session.rs` so a live mounted daemon can serve
  the new snapshot/event surfaces.
- Keep test scope focused to the touched crates; do not run the full workspace
  suite in this task.

## Non-Goals For This Slice

- No broad instrumentation of every `debug!` or `warn!` site.
- No log-file tailing protocol.
- No comparison or reporting logic yet.
- No controller-only mutating diagnostics actions beyond what already exists.
