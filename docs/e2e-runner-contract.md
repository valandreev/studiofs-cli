# Real E2E Runner Contract

This document defines the implementation contract for the real mounted E2E
runner before scenario-specific logic is added. It is the shared contract for
the runner skeleton, artifact collection, and CI upload/reporting work tracked
by SFS-318, SFS-323, and SFS-329.

The runner is not part of the default fast test suite. It is an explicit,
operator-started workflow for tests that need Docker, PostgreSQL, S3-compatible
object storage, and a real FUSE, macFUSE, or WinFSP mount.

## Runner Boundaries And CLI

The runner belongs in a dedicated workspace crate, expected to be named
`studiofs-e2e-runner`, with a binary named `studiofs-e2e`. The crate owns
orchestration only:

- preflight checks and run planning;
- per-run sandbox creation under `target/e2e-runs/<run-id>`;
- child-process lifecycle management for Docker, `studiofs`, and scenario
  probes;
- JSON report and artifact emission;
- cleanup and retention decisions.

The runner must treat `studiofs` as an external product binary. It must not
reach into VFS, metadata, cache, FUSE, or WinFSP internals to make a scenario
pass. Shared helpers are acceptable only for stable test utilities that do not
change mounted-runtime semantics.

Expected CLI shape:

```text
studiofs-e2e run \
  --scenario smoke \
  --studiofs-bin <path-to-studiofs> \
  --root target/e2e-runs \
  --run-id <run-id> \
  --platform auto \
  --retain on-failure \
  --report target/e2e-runs/<run-id>/report.json
```

Required initial flags:

| Flag | Contract |
|---|---|
| `--scenario <name>` | Repeatable. Initial accepted value is `smoke`; later gates add convergence, outage, and collision names. |
| `--studiofs-bin <path>` | Path to the product CLI under test. The runner may build it only when an explicit build flag is added. |
| `--root <path>` | Parent run directory. Defaults to `target/e2e-runs`. |
| `--run-id <id>` | Optional stable run ID. If omitted, use an ISO-like UTC timestamp plus short random suffix. |
| `--platform auto|linux-fuse|macos-fuse|windows-winfsp` | Mount backend selection. `auto` resolves from host OS and installed driver checks. |
| `--retain always|on-failure|never` | Sandbox retention policy. Default is `on-failure`. |
| `--report <path>` | Optional copy target for the final `report.json`. The canonical report remains in the run sandbox while the sandbox is retained. |
| `--port-range <start-end>` | Optional deterministic host port pool for Docker services. |
| `--timeout <duration>` | Whole-run timeout; per-hook timeouts are derived from it unless overridden later. |

The initial runner should implement `run` only. Future subcommands may add
`list-scenarios`, `preflight`, and `cleanup`, but those must reuse the same
report schema and cleanup safety checks.

## Sandbox Layout

Every run creates exactly one canonical sandbox:

```text
target/e2e-runs/<run-id>/
  run.json
  report.json
  events.ndjson
  stdout.log
  stderr.log
  preflight.json
  ports.json
  cleanup.json
  artifacts/
    docker/
    commands/
    scenarios/
      <scenario-name>/
    clients/
      client-a/
        client-root/
        mount/
        studiofs.stdout.log
        studiofs.stderr.log
      client-b/
        client-root/
        mount/
        studiofs.stdout.log
        studiofs.stderr.log
```

On Windows, `artifacts/clients/<client>/mount/` is a metadata directory for the
drive-letter mount. It must contain the chosen mount point in a small text or
JSON artifact, because `Z:` is not a directory inside the sandbox.

`run.json` is the immutable run manifest written before setup starts. It must
include the run ID, absolute sandbox path, requested scenarios, retention
policy, selected platform, explicit client roots, allocated ports, child
process IDs once known, and cleanup allowlist.

## Artifact Layout And Retention

The following artifact names are required and stable:

| Artifact | Purpose |
|---|---|
| `report.json` | Final machine-readable report. |
| `events.ndjson` | Append-only event stream for progress and post-mortem timeline. |
| `stdout.log` | Runner stdout mirror. |
| `stderr.log` | Runner stderr mirror. |
| `preflight.json` | Full preflight check results. |
| `ports.json` | Allocated port pool and service bindings. |
| `cleanup.json` | Cleanup actions attempted, skipped, and failed. |
| `artifacts/docker/` | Docker compose file, service logs, and container inspect output when Docker is used. |
| `artifacts/commands/` | Per-command stdout/stderr/status records for child commands. |
| `artifacts/scenarios/<scenario-name>/` | Scenario-specific inputs, probes, and validation output. |
| `artifacts/clients/<client>/` | Client root, mount metadata, mount logs, and post-run status snapshots. |

Retention behavior:

- `always` keeps the full sandbox after cleanup.
- `on-failure` keeps the full sandbox for `failed`, `preflight_failed`,
  `cleanup_failed`, interrupted, or timed-out runs. On success it keeps the
  required top-level artifacts and may delete bulky service/client directories
  after summarizing them in `cleanup.json`.
- `never` may remove the sandbox only after `report.json`, `events.ndjson`,
  `stdout.log`, `stderr.log`, `preflight.json`, and `cleanup.json` are copied
  to the explicit `--report` destination or CI artifact staging directory.

CI-oriented callers should use `on-failure` or `always` until SFS-323 artifact
collection is stable.

## JSON Report Shape

`report.json` is UTF-8 pretty JSON. Unknown fields are allowed, but existing
field names and enum values are append-only once SFS-318 depends on them.

Minimum shape:

```json
{
  "schema_version": 1,
  "run_id": "20260501T181530Z-ab12cd",
  "status": "passed",
  "exit_code": 0,
  "started_at": "2026-05-01T18:15:30Z",
  "finished_at": "2026-05-01T18:16:12Z",
  "duration_ms": 42000,
  "runner": {
    "crate": "studiofs-e2e-runner",
    "version": "0.1.0",
    "git_sha": "unknown"
  },
  "host": {
    "os": "windows",
    "arch": "x86_64",
    "platform": "windows-winfsp"
  },
  "sandbox": {
    "root": "target/e2e-runs/20260501T181530Z-ab12cd",
    "retention": "on-failure"
  },
  "preflight": {
    "status": "passed",
    "checks": [
      {
        "name": "Docker",
        "status": "passed",
        "detail": "docker version succeeded"
      }
    ]
  },
  "scenarios": [
    {
      "name": "smoke",
      "status": "passed",
      "started_at": "2026-05-01T18:15:42Z",
      "finished_at": "2026-05-01T18:16:02Z",
      "duration_ms": 20000,
      "artifacts": [
        "artifacts/scenarios/smoke/validation.json"
      ],
      "error": null
    }
  ],
  "artifacts": [
    {
      "name": "report.json",
      "path": "report.json",
      "required": true
    }
  ],
  "cleanup": {
    "status": "passed",
    "actions": [],
    "errors": []
  }
}
```

Reports should use forward slashes in relative artifact paths even on Windows.
Absolute paths are allowed only in `sandbox.root`, explicit client root fields,
and diagnostic details where the operator needs to locate a host resource.

## Status And Exit-Code Semantics

Run and scenario status values:

| Status | Meaning |
|---|---|
| `passed` | The hook or scenario completed and validation passed. |
| `failed` | Scenario validation or an expected child command failed. |
| `skipped` | Scenario did not run because its gate or platform requirement was not selected. |
| `preflight_failed` | Required host, Docker, mount, command, port allocation, or stale mounts check failed before scenario setup. |
| `cleanup_failed` | Cleanup could not return the host to a safe state or could not prove that retained resources are recorded. |
| `timed_out` | A hook or whole run exceeded its timeout and cleanup was attempted. |
| `interrupted` | The runner received Ctrl+C or a termination signal and cleanup was attempted. |

Exit codes:

| Exit code | Contract |
|---|---|
| `0` | All selected scenarios passed and cleanup passed. |
| `1` | At least one selected scenario failed or timed out. |
| `2` | Preflight failed and no scenario ran. |
| `3` | Runner usage/configuration error, such as invalid flags or unsupported scenario name. |
| `4` | Scenarios passed, but cleanup failed. If scenarios also failed, use `1` and record `cleanup.status = "cleanup_failed"`. |
| `130` | Interrupted by Ctrl+C after writing the best-effort report. |

The runner must write a best-effort `report.json` for every non-usage failure.
Usage errors may print help without creating a sandbox.

## Lifecycle Hooks

Each scenario runs through the same hook model:

1. `setup`
2. `run`
3. `validate`
4. `cleanup`

Hook rules:

- `setup` creates only resources listed in `run.json`: Docker services,
  per-client `STUDIOFS_CLIENT_ROOT` directories, mount points, and child
  processes. It must fail fast if any required path or port is unsafe.
- `run` performs the scenario workload through mounted filesystem paths and
  product CLI commands. It must not inspect private VFS state to decide pass or
  fail.
- `validate` reads mounted paths, command outputs, and public artifacts. It may
  call `studiofs status` or other product CLI surfaces, but it must not mutate
  scenario state except for validation artifacts.
- `cleanup` always runs after `setup` starts, even when `run` or `validate`
  fails. It unmounts, stops child processes, tears down Docker resources, and
  applies the retention policy.

Every hook writes a start and finish event to `events.ndjson`. Hook failures
must include a category, human-readable message, and artifact references in the
report.

## Preflight Gates

Preflight must run before setup and must produce `preflight.json`.

Required checks:

- Docker: `docker version` succeeds, Docker compose is available, and the
  daemon can start required PostgreSQL and S3-compatible services.
- Mount support:
  - Linux requires a usable `/dev/fuse`, mountpoint creation rights, and the
    FUSE backend expected by `studiofs-fuse`.
  - Windows requires WinFSP installed, `winfsp-x64.dll` loadable by the
    product CLI, a free drive letter, and either elevation or a configured
    WinFSP registry setting such as `MountUseMountmgrFromFSD=1`.
  - macOS requires macFUSE or the configured FUSE provider installed and loaded,
    plus a writable mount directory.
- Required commands: `studiofs` binary, `cargo` only when an explicit build mode
  exists, `docker`, Docker compose, and platform unmount/status commands needed
  by cleanup.
- Port allocation: all service ports come from the configured range or an
  explicit allocator. The runner writes `ports.json` before starting Docker and
  must fail preflight when a selected port is already bound.
- stale mounts: requested mount points or drive letters must be unused, or must
  match a prior runner manifest that explicitly authorizes cleanup. Unknown
  mounted paths are a `preflight_failed` result, not an implicit force-unmount.
- Filesystem safety: sandbox and explicit client roots canonicalize
  successfully, are not drive roots, home roots, repository roots, or temp roots,
  and do not cross symlinks or junctions in a way that would make cleanup
  ambiguous.

Preflight may emit warnings for optional diagnostics, but a missing required
check must prevent setup.

## Cleanup Safety

Cleanup is a safety boundary, not a convenience script. The runner must never delete outside
the run sandbox or explicit client roots recorded in `run.json`.

Rules:

- Canonicalize every path before deletion.
- Refuse to delete empty paths, drive roots, filesystem roots, the repository
  root, user home directories, and broad temp roots.
- Delete a path only when it is inside `target/e2e-runs/<run-id>` or exactly
  matches an explicit client root recorded in `run.json`.
- Treat symlinks and junctions as leaf entries. Do not recursively follow them
  during cleanup.
- Kill only child process IDs recorded in the manifest or discovered as direct
  descendants of those PIDs.
- Unmount before deleting mount directories or explicit client roots.
- If unmount fails, retain the affected path and record the failure in
  `cleanup.json`.
- Docker cleanup must target run-labeled containers, networks, and volumes
  only. Labels must include the run ID.

When cleanup cannot prove ownership, it must retain the resource and mark the
run `cleanup_failed`.

## Initial Scenario Gating

Scenario growth is deliberately staged:

1. `smoke` first: one StudioFS client, isolated client root, real mount,
   create/write/read/readdir/delete/unmount, and required artifacts.
2. Basic convergence next: two clients with separate explicit client roots,
   PostgreSQL and S3-compatible services, publish/apply visibility, and stable
   report fields.
3. Outage scenarios only after smoke and convergence are stable: stop/restart
   PostgreSQL or S3-compatible services and validate local-first behavior,
   replay, and failure reporting.
4. Collision scenarios only after outage reporting is stable: concurrent
   writes, renames, deletes, locks, and conflict cases.

Each new scenario must keep the same hook model, report schema, artifact names,
and cleanup safety rules. Adding outage or collision coverage before the smoke
runner, convergence gate, and artifact collection are stable is out of scope.
