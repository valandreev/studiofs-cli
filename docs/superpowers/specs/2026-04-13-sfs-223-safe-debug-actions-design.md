# SFS-223 Safe Debug Actions Design

## Goal

Add the first controller-safe debug action surface to the live diagnostics host
so an attached controller can trigger approved bounded actions while preserving
trust in the investigation evidence trail.

## Existing Base

- `crates/studiofs/src/diagnostics.rs` already provides attach discovery,
  controller lease enforcement, run creation, profiling control, and timeline
  append RPCs.
- `crates/studiofs/src/investigation.rs` already persists append-only timeline
  entries with actor and reason metadata.
- `crates/studiofs/src/cli/mount/session.rs` already covers live attach flows
  end to end against a mounted daemon.
- `crates/studiofs/src/main.rs` already initializes tracing once at process
  startup, which is the right place to add bounded runtime log-filter control.

## Design

### Diagnostics Protocol

Add a new typed controller-only RPC to `crates/studiofs/src/diagnostics.rs`:

- `InvokeSafeAction { lease_id, run_id, action }`

The response should return a structured action outcome with:

- action kind
- loud vs silent classification
- success or fail-open failure result
- optional artifact metadata
- optional summary payload suitable for the MCP adapter

This keeps action authorization, fail-open handling, and persistence inside the
existing diagnostics host instead of splitting behavior across multiple clients.

### Approved Action Set

The first slice should support these bounded actions:

- silent: set log verbosity to an allowlisted level (`info`, `debug`, `trace`)
- loud: capture bundle
- loud: reset observability counters
- loud: lightweight self-check

Silent actions still require the current controller lease, but they do not add
extra confirmation or mandatory timeline noise once control is established.

Loud actions are still controller-safe, but they must always leave a visible
audit record in the run timeline.

### Action Execution Model

Add a small diagnostics action executor in `crates/studiofs/src/diagnostics.rs`
that validates the request, performs the action, and emits a structured result.

The implementation should stay intentionally narrow:

- log verbosity uses a reloadable tracing filter handle created during process
  startup and rejects values outside the explicit allowlist
- capture bundle writes a bounded JSON artifact under the diagnostics cache area
  containing session info, runtime snapshot, and a limited recent-events window
- counter reset calls explicit reset hooks on observability counters already
  exposed by the diagnostics runtime surface
- self-check is read-only and lightweight, producing a small summary verdict
  from already-available runtime state rather than running any repair path

### Timeline Audit

Extend the investigation timeline model with an explicit action-oriented entry
kind so loud actions are visibly distinct from markers and notes.

For every loud action, append a timeline entry that records:

- occurred time
- actor
- reason
- action name
- outcome summary

The entry must be recorded even when the action fails in fail-open mode so the
evidence trail shows both the attempt and the outcome.

Silent actions should not append timeline entries by default.

### Fail-Open Behavior

Safe-action failures must not break the live diagnostics host or mounted
filesystem.

If a safe action cannot complete, the diagnostics protocol should return a
structured failure outcome and preserve any audit entry required for loud
actions. This matches the existing fail-open behavior used by unsupported or
failed profiling windows.

## Testing Strategy

- Extend `crates/studiofs/src/diagnostics.rs` unit tests to cover controller
  authorization for safe actions.
- Add tests for loud-action audit persistence and silent-action non-audit
  behavior.
- Add representative action tests for bounded log verbosity, capture bundle,
  counter reset, and lightweight self-check behavior.
- Add failure-path tests to verify loud actions remain audited and that action
  failures stay fail-open.
- Extend the live attach integration in
  `crates/studiofs/src/cli/mount/session.rs` to prove a mounted daemon can run
  a representative safe action end to end.

## Non-Goals For This Slice

- No unrestricted mutation of cache policy, sync behavior, throttles, or mount
  semantics.
- No broad arbitrary command-execution surface.
- No unbounded log-tail or artifact-export protocol.
- No multi-step diagnostics workflows beyond one approved action per request.
