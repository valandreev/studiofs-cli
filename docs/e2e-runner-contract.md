# Real E2E Runner Contract

This document defines the implementation contract for the real mounted E2E
runner before scenario-specific logic is added. It is the shared contract for
the runner skeleton, artifact collection, and CI upload/reporting work.

The runner is not part of the default fast test suite. It is an explicit,
operator-started workflow for tests that need Docker, PostgreSQL, S3-compatible
object storage, and a real FUSE, macFUSE, or WinFSP mount.

## Runner Boundaries And CLI

The runner belongs in a dedicated workspace crate named `studiofs-e2e`, with a
binary named `studiofs-e2e`. The crate owns
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

Initial operator commands:

```text
cargo run -p studiofs-e2e -- list
cargo run -p studiofs-e2e -- run basic-multi-client-smoke
cargo run -p studiofs-e2e -- run postgres-outage-multi-client-recovery --studiofs-bin <path-to-studiofs>
cargo run -p studiofs-e2e -- run rustfs-outage-multi-client-recovery --studiofs-bin <path-to-studiofs>
cargo run -p studiofs-e2e -- run mounted-client-crash-restart-recovery --studiofs-bin <path-to-studiofs>
cargo run -p studiofs-e2e -- run collision-rename-vs-unlink --studiofs-bin <path-to-studiofs>
cargo run -p studiofs-e2e -- local-first-baseline-gate --studiofs-bin <path-to-studiofs> --run-id-prefix <prefix> --seed 405 --port-range <start-end>
cargo run -p studiofs-e2e -- clean
```

Expanded run CLI shape:

```text
studiofs-e2e run \
  --scenario smoke \
  --studiofs-bin <path-to-studiofs> \
  --root target/e2e-runs \
  --run-id <run-id> \
  --platform auto \
  --infra compose \
  --retain on-failure \
  --report target/e2e-runs/<run-id>/report.json
```

Required initial run flags and environment:

| Flag | Contract |
|---|---|
| `basic-multi-client-smoke` | Positional shorthand for the mounted three-client happy-path smoke scenario. |
| `--scenario <name|number|range|list>` | Repeatable. Accepted values include scenario names/aliases, one-based numbers from `studiofs-e2e list`, numeric ranges such as `1-8`, and comma lists such as `2,5,10`. Names include `basic-multi-client-smoke` (`smoke`), `postgres-outage-multi-client-recovery` (`postgres-outage`), `rustfs-outage-multi-client-recovery` (`rustfs-outage`, `s3-outage`), `mounted-client-crash-restart-recovery` (`client-crash-restart`, `crash-restart`), `mounted-publication-boundary-crash-matrix` (`publication-boundary-crash`, `publication-boundary`), `mounted-chaos-convergence` (`mounted-chaos`, `chaos`), `mounted-hotspot-churn-convergence` (`hotspot-churn`, `hot-dir-churn`), `mounted-fault-chaos-convergence` (`fault-chaos`, `chaos-faults`), `mounted-lww-reconnect-chaos-convergence` (`lww-chaos`, `reconnect-lww`), `mounted-open-handle-convergence` (`open-handle`, `open-unlink-rename`), `mounted-sparse-file-range-convergence` (`sparse-range`, `sparse-file-range`), `mounted-cold-observer-long-history-convergence` (`cold-observer`, `cold-observer-long-history`), and the mounted namespace collision scenarios `collision-rename-vs-rename`, `collision-rename-vs-unlink`, `collision-unlink-vs-append`, `collision-overwrite-vs-read`, `collision-mkdir-rmdir-vs-create`, and `collision-readdir-stat-during-mutation`. |
| `--studiofs-bin <path>` | Path to the product CLI under test. Real mounted scenarios require it when `--infra compose` is used. The runner may build it only when an explicit build flag is added. |
| `--root <path>` | Parent run directory. Defaults to `target/e2e-runs`. |
| `--run-id <id>` | Optional stable run ID. If omitted, use an ISO-like UTC timestamp plus short random suffix. Explicit IDs must be safe single directory names: no separators, drive prefixes, traversal segments, leading dots, or other shell/path metacharacters. |
| `--platform auto|linux-fuse|macos-fuse|windows-winfsp` | Mount backend selection. `auto` resolves from host OS and installed driver checks. |
| `--infra compose|disabled` | External infrastructure mode. `compose` starts `docker/compose.e2e.yml`, waits for healthy required services, collects Docker logs, and tears the stack down. `disabled` is reserved for runner self-tests and skeleton-only validation. |
| `--retain always|on-failure|never` | Sandbox retention policy. Default is `on-failure`. |
| `--report <path>` | Optional copy target for the final `report.json`. The canonical report remains in the run sandbox while the sandbox is retained. |
| `--port-range <start-end>` | Optional deterministic host port pool for Docker services. The compose runner allocates PostgreSQL, RustFS S3, RustFS console, and Toxiproxy API ports in that order. |
| `STUDIOFS_E2E_COMPOSE_FILE` | Optional environment override for the compose file path. Defaults to `docker/compose.e2e.yml` discovered from the current directory or executable location. |
| `--timeout <duration>` | Whole-run timeout; per-hook timeouts are derived from it unless overridden later. |

The initial runner implements `list`, `run`, and `clean`. Future subcommands
may add dedicated `preflight` or artifact commands, but those must reuse the
same report schema and cleanup safety checks.

## Scenario Supervision And Cleanup Gates

Single-scenario `run` invocations execute the scenario lifecycle directly so
the operator-facing workflow stays unchanged. Multi-scenario sweeps supervise
each selected scenario in a hidden `run-one` child process owned by the parent
runner. The parent remains responsible for preflight, compose lifecycle,
top-level progress, report aggregation, and final cleanup; the child owns
exactly one scenario lifecycle and writes
`artifacts/scenarios/<scenario>/scenario-child-report.json` for aggregation.

The parent forwards the requested timeout as the child process deadline. If
the deadline expires, the parent reports the scenario as `timed_out`, records a
`scenario_child` timeout event, and the command executor kills the child
process boundary. On Windows the timeout path uses `taskkill /PID <pid> /T /F`
as the process-tree fallback before waiting for exit.

After every mounted scenario, the parent runs the runner-owned stale-process
cleanup helper in a post-scenario verification phase. Any runner-owned
`studiofs.exe` / `studiofs-e2e.exe` process found or cleaned in that phase is a
scenario failure, because later mounted scenarios would be running on
contaminated host state. Once this gate fails, later mounted scenarios are
recorded as skipped instead of being started on potentially occupied default
mount letters. External-env and non-runner-owned processes remain outside the
cleanup allowlist.

## Local First Baseline Gate

`local-first-baseline-gate` is the explicit SFS-405 gate for rerunning the
accepted mounted scenario 1-8 baseline after the Local First contract rewrites.
It is operator-started, uses the same per-run sandbox/report schema as `run`,
and does not become part of the default fast suite.

Recommended Windows/WinFSP command:

```powershell
cargo build -p studiofs
cargo run -p studiofs-e2e -- local-first-baseline-gate `
  --studiofs-bin target\debug\studiofs.exe `
  --run-id-prefix sfs405-baseline `
  --iterations 3 `
  --seed 405 `
  --port-range 29700-29800
```

Each iteration records a stable run ID (`sfs405-baseline-01`,
`sfs405-baseline-02`, and so on), writes the normal `report.json`,
`preflight.json`, `cleanup.json`, mounted-tree validation artifacts, scenario
summaries, and Docker diagnostics. Failures should be classified from those
artifacts as product bug, harness bug, environment drift, or invalid test
expectation before changing product semantics.

The gate runs these accepted baseline scenarios in order:

1. `basic-multi-client-smoke`
2. `postgres-outage-multi-client-recovery`
3. `rustfs-outage-multi-client-recovery`
4. `mounted-client-crash-restart-recovery`
5. `mounted-publication-boundary-crash-matrix`
6. `collision-rename-vs-rename`
7. `collision-rename-vs-unlink`
8. `collision-unlink-vs-append`

The same numbering is used by `studiofs-e2e list` and by `run --scenario`, so
`--scenario 1-8` runs this baseline set and `--scenario 2,5,10` runs only those
numbered scenarios when they are registered.

Scenario 2 follows the rewritten PostgreSQL outage contract: writer recovery
comes first, then normal publication, then peer convergence. Scenario 4 follows
the rewritten crash/restart contract: a readable recovered writer checksum is
the convergence oracle, while unrecoverable local loss is reported separately
from corrupt advertised files, publication failure, and peer convergence
failure.

For Windows/WinFSP runs, the gate checks for stale runner-owned
`studiofs.exe` / `studiofs-e2e.exe` processes before starting the first
iteration. Compose preflight also rejects environment drift before scenario
execution: PostgreSQL must use `postgres:18-alpine`, RustFS must use
`rustfs/rustfs:1.0.0-alpha.94`, and accidental RustFS `latest` usage is a
preflight failure, not a product failure.

## Mounted Levels 9+ Promotion Gate

Mounted levels 9+ are blocked until scenarios 1-8 pass with Local First
semantics through the `local-first-baseline-gate`. A deeper mounted level may
be added or trusted only after the baseline artifacts show that failures are no
longer coming from unresolved Local First contract rewrites, runner cleanup, or
environment drift in the accepted baseline.

Every new mounted scenario issue after level 8 must state:

- its Local First invariant and publication boundary;
- whether it validates product-invariant, harness-invariant, or platform-only
  behavior;
- why the behavior belongs in real mounted E2E instead of a lower test layer;
- whether deterministic simulation first gives useful signal before WinFSP,
  FUSE, or macFUSE is involved.

Use deterministic simulation first when the behavior is a logical state-machine
or convergence invariant that can be modeled without the OS mount stack. Keep
real mounted E2E for bridge, OS cache, shutdown, and kernel notification
behavior, and for final public-path validation of scenarios whose lower-layer
models have already passed.

The old post-baseline scenario chain is not continued automatically. Any
scenario that was planned beyond the accepted baseline must be reintroduced
through this gate with its invariant class, publication boundary, and required
lower-layer signal named explicitly.

## Sandbox Layout

Every run creates exactly one canonical sandbox:

```text
target/e2e-runs/<run-id>/
  .studiofs-e2e-runner.json
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
        cache/
        mount/
        studiofs.stdout.log
        studiofs.stderr.log
        studiofs-tracing.log
      client-b/
        client-root/
        cache/
        mount/
        studiofs.stdout.log
        studiofs.stderr.log
        studiofs-tracing.log
      client-c/
        client-root/
        cache/
        mount/
        studiofs.stdout.log
        studiofs.stderr.log
        studiofs-tracing.log
      client-d/
        client-root/
        cache/
        mount/
        studiofs.stdout.log
        studiofs.stderr.log
        studiofs-tracing.log
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
| `.studiofs-e2e-runner.json` | Hidden ownership marker used by safe cleanup before deleting a sandbox. |
| `report.json` | Final machine-readable report. |
| `events.ndjson` | Append-only event stream for progress and post-mortem timeline. |
| `stdout.log` | Runner stdout mirror. |
| `stderr.log` | Runner stderr mirror. |
| `preflight.json` | Full preflight check results. |
| `ports.json` | Allocated port pool and service bindings. |
| `cleanup.json` | Cleanup actions attempted, skipped, and failed. |
| `artifacts/docker/` | Docker compose file, service logs, and container inspect output when Docker is used. |
| `artifacts/commands/` | Per-command stdout/stderr/status records for child commands. |
| `artifacts/scenarios/<scenario-name>/` | Scenario-specific inputs, probes, `operations.ndjson`, and validation output. |
| `artifacts/clients/<client>/` | Client root, mount metadata, mount stdout/stderr logs, structured tracing logs when emitted, and post-run status snapshots. |

The final report lists all required top-level artifacts and recursively
discovers files under `artifacts/` before writing `report.json`. This makes
client logs, Docker logs, mounted tree snapshots, validator diffs, and
scenario-specific diagnostics visible to CI uploaders without every scenario
having to duplicate report-linking code.

When Docker Compose infrastructure has started, cleanup collects best-effort
diagnostics before teardown:

| Artifact | Source |
|---|---|
| `artifacts/docker/compose.log` | `docker compose logs --no-color --timestamps` stdout for all services. |
| `artifacts/docker/compose.stderr.log` | stderr from Docker Compose log collection. |
| `artifacts/docker/postgres-schema.sql` | Optional PostgreSQL schema dump from the running `postgres` service. |
| `artifacts/docker/postgres-schema.stderr.log` | stderr from optional PostgreSQL dump collection. |
| `artifacts/docker/postgres-schema.status.json` | Exit status and collected/skipped marker for optional PostgreSQL dump collection. |
| `artifacts/docker/rustfs-object-listing.txt` | Optional RustFS `/data` object listing from the running `rustfs` service. |
| `artifacts/docker/rustfs-object-listing.stderr.log` | stderr from optional RustFS listing collection. |
| `artifacts/docker/rustfs-object-listing.status.json` | Exit status and collected/skipped marker for optional RustFS listing collection. |

Optional diagnostic stdout artifacts are written even when their command exits
non-zero; the status JSON records whether the artifact was complete. Artifact
discovery includes regular files and symlink entries under `artifacts/`, but it
does not recurse through symlinked directories.

## Multi-Client Process Controller

Mounted multi-client scenarios use the `studiofs-e2e::client` controller to
create isolated logical clients in `artifacts/clients/<client>/`. Each client
gets a dedicated `STUDIOFS_CLIENT_ROOT`, local metadata path
`client-root/meta/<volume>/<volume>.db`, cache directory, mount point,
stdout/stderr logs, tracing log stem, and lifecycle state.

The controller bootstraps the first client with `studiofs init`, discovers the
same volume for additional clients with `studiofs discover`, spawns foreground
`studiofs mount` processes with per-client environment variables, waits for
mount readiness, and supports graceful `studiofs umount`, forced child-process
kill, and restart/remount. Scenarios must use this controller rather than
sharing one local metadata DB or falling back to direct VFS calls.
Crash-style remounts do not run `studiofs umount` for the killed client. On
Linux, before spawning the replacement mount process, the controller attempts a
lazy stale-mount cleanup for leftover FUSE mountpoints with `fusermount3 -u -z`,
`fusermount -u -z`, or `umount -l`. Other platforms keep the existing remount
path unless a platform-specific stale-mount cleanup is added.

`basic-multi-client-smoke` uses the first three default clients. It initializes
three peer remote volumes, one from each client, then discovers each peer-owned
volume from the other two clients. To keep Windows drive-letter use bounded, it
validates those volumes sequentially: select one volume, mount it from all three
clients, write the smoke file through that volume's init client, wait for the
mounted tree validator to observe convergence from all three mount points, then
unmount and move to the next peer volume. The scenario writes
`peer-bootstrap-summary.json`, `smoke-summary.json`, and `validation.json`.
The `--infra disabled` path remains limited to runner self-tests and writes only
the skeleton validation artifact.

`postgres-outage-multi-client-recovery` uses the same three-client mounted
bootstrap, writes an initial file while PostgreSQL is healthy, stops only the
PostgreSQL compose service, accepts a writer-local outage write through
`client-a`, force-kills that writer, starts PostgreSQL again with compose health
waiting, and remounts `client-a`. If the writer recovers the accepted local
payload, the scenario uses the mounted tree validator to check peer observation
from `client-b` and `client-c`. If the unclean writer shutdown recovers zero
bytes, the scenario records `lost_after_unclean_shutdown` and treats the write
as having no payload to publish; peer convergence for that lost write is skipped
because every peer remaining empty is a valid outcome. The scenario does not
require peers to observe the writer's outage payload before writer recovery and
publication after PostgreSQL returns. The scenario writes the accepted writer
outage operation to `operations.ndjson` and `outage-operations.ndjson`, and
writes `postgres-outage-recovery-summary.json` with outage duration,
accepted-operation count, writer recovery client/status/checksum/time, peer
observation time, recovery time, and the final validator result.
`outage_duration_ms` covers the stopped
PostgreSQL window through the `docker compose up --wait` healthcheck readiness
gate. `recovery_time_ms` starts before the PostgreSQL compose service is brought
back, so it includes PostgreSQL healthcheck readiness, writer remount/recovery,
and mounted tree convergence time.

`rustfs-outage-multi-client-recovery` uses a four-client mounted bootstrap,
writes and converges an initial file while RustFS/S3 is healthy, stops only the
RustFS compose service, writes a new complete version through `client-a`,
verifies that `client-a` can read the accepted local version, and checks that
every peer client does not observe that new checksum before RustFS/S3 is
restored. After `docker compose up --wait rustfs`, the scenario waits for
mounted tree convergence and records the local read checksum, per-peer
pre-recovery visibility checksums, per-client final checksums, upload recovery
time, and final validator result in `rustfs-outage-recovery-summary.json`.
`outage_duration_ms` covers the RustFS stopped window through the accepted
local write/read boundary; the peer visibility probe runs after that boundary
so peer I/O latency does not inflate the outage-duration metric.
Mounted E2E clients set `STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS=250`
so peers that already cached the healthy payload refresh metadata inside the
scenario convergence window after RustFS/S3 recovers.

`mounted-client-crash-restart-recovery` uses at least two mounted clients with
separate `STUDIOFS_CLIENT_ROOT` values. It writes accepted data through
`client-a`, crosses an explicit local durability boundary with `fsync`, kills
only the `client-a` mount process without running `studiofs umount`, remounts
`client-a` with the same client root, and probes the writer-visible file after
restart. If the writer can read recovered bytes, that recovered checksum becomes
the convergence oracle even when it differs from the pre-crash intended
checksum. If the writer cannot recover the file from local log/cache, the
scenario reports unrecoverable loss instead of requiring volatile OS-cache data
to survive an unclean crash. The scenario runs two modes in one invocation:

- `after_local_durability_boundary`, which crashes immediately after the
  documented local durability boundary is crossed. Recovered writer state is
  expected to publish and converge across peers.
- `before_remote_publication_completes`, which crashes after local durability
  but before the scenario waits for remote publication and peer convergence.
  The writer's recovered local read is pass/fail; peer visibility is recorded
  as diagnostics only.

The scenario writes `operations.ndjson`,
`crash-restart-recovery-summary.json`, and `validation.json`. The summary
records the restarted client, reused client root, per-mode intended, recovered
local, and final checksums, whether a peer saw any pre-convergence checksum in
the `before_remote_publication_completes` mode, restart timing, convergence
timing, recovery outcome, and mounted tree validator result. Diagnostics must
distinguish unrecoverable local loss, publication failure, peer convergence
failure in the post-durability mode, and a corrupt advertised file whose final
peer checksum differs from the writer's recovered checksum. Publication waiting
is intentionally the public mounted-tree convergence loop; the scenario must not
inspect private S3, PostgreSQL, VFS, cache, or redb state to decide success. It
must not delete local metadata, rerun `discover`, or share one local metadata
database between clients during recovery.

Mounted namespace collision scenarios use the first three default clients
against one shared remote volume. Each scenario establishes a bounded initial
tree, starts conflicting operations from multiple clients through a shared
barrier, writes every attempted operation to `operations.ndjson`, waits for
mounted tree convergence, and writes `namespace-collision-summary.json` plus
`validation.json`. Operation records include client, operation, path, result,
error, timestamp, and deterministic order marker. The suite currently includes:

- `collision-rename-vs-rename`
- `collision-rename-vs-unlink`
- `collision-unlink-vs-append`
- `collision-overwrite-vs-read`
- `collision-mkdir-rmdir-vs-create`
- `collision-readdir-stat-during-mutation`

Collision validation does not assume a specific winning operation unless a
scenario explicitly defines an absent path, such as the original source path
after rename collisions. Final validation requires all mounted clients to
converge on the same public tree and rejects duplicate canonical paths, missing
parent directories, unexpected resurrection, and checksum mismatch. The
overwrite/read scenario records repeated read loops while another client
overwrites the committed file version.

The restored advanced mounted scenarios keep the same artifact shape:
`operations.ndjson`, a scenario-specific summary JSON file, and
`validation.json`. `mounted-publication-boundary-crash-matrix` records the
flush, fsync, release/close, and shutdown-drain boundaries. `mounted-chaos-*`
scenarios run bounded mounted mutations and converge through the public mounted
tree validator. `mounted-lww-reconnect-chaos-convergence` also writes
`lww-oracle.json`, and validation requires the observed winner checksum to
match that oracle. `mounted-open-handle-convergence` includes the old-path
absence check for `open-handles/unlink-target.txt`.
`mounted-sparse-file-range-convergence` appends after the accepted 1-16 list
and writes `sparse-range-summary.json`; it validates sparse holes,
shrink/extend truncation, chunk-boundary writes, range read probes, logical
size, and final checksums through public mounted reads.
`mounted-cold-observer-long-history-convergence` appends after the sparse
promotion scenario and writes `cold-observer-long-history-summary.json`,
`cold-observer-diagnostics.json`, `operations.ndjson`, and `validation.json`.
Warm clients build a bounded history with create, delete, rename, overwrite,
append, and nested directory mutations before `client-d` is discovered and
mounted with a fresh local root/cache. Validation uses public mounted reads and
tree convergence to reject resurrected tombstones, stale source paths,
checksum mismatches, and non-canonical namespace results.

## Mounted Tree Convergence Validation

Mounted scenarios validate client-visible invariants through the
`studiofs-e2e::mounted_tree` validator. The validator snapshots public
mountpoints only; it must not inspect private VFS, metadata, cache, PostgreSQL,
or object-store state to decide whether a scenario passed.

The portable equality contract is intentionally narrow:

- required comparison fields: relative path, entry type, regular-file size, and
  regular-file SHA-256 content checksum;
- required symlink comparison field: symlink target path, when symlink entries
  are present in a mounted tree;
- diagnostic-only fields: mtimes and other platform-derived metadata;
- out-of-initial-scope metadata: xattrs, hard links, and platform-specific file
  attributes unless a later StudioFS contract explicitly makes them mandatory.

Snapshot JSON must be deterministic across repeated runs and operating systems:
entries are sorted by forward-slash relative path, and per-client snapshots are
written as `tree-client-N.json`. When convergence times out, validation writes
one tree snapshot per client plus `diff.json` in the scenario validation
artifact directory. Temporary divergence during the convergence window is
recorded by repeated polling but is not a failure until the timeout expires.
Transient snapshot I/O failures, such as files being renamed during traversal or
temporary Windows sharing violations, are treated the same way: retry until the
convergence deadline, then report the last stable divergence or snapshot error.
Snapshot paths and symlink targets must be valid UTF-8 because artifacts are
JSON; non-UTF-8 paths fail validation instead of using lossy replacement that
could hide collisions. The validator hashes every regular file on each
successful poll, so mounted convergence scenarios should keep trees bounded
until a separate checksum cache/invalidation contract exists.

Retention behavior:

- `always` keeps the full sandbox after cleanup.
- `on-failure` keeps the full sandbox for `failed`, `preflight_failed`,
  `cleanup_failed`, interrupted, or timed-out runs. On success it keeps the
  required top-level artifacts and may delete bulky service/client directories
  after summarizing them in `cleanup.json`.
- `never` may remove the sandbox only after `report.json`, `events.ndjson`,
  `stdout.log`, `stderr.log`, `preflight.json`, and `cleanup.json` are copied
  to the explicit `--report` destination or CI artifact staging directory.

CI-oriented callers should use `on-failure` or `always` until artifact
collection is stable.

## JSON Report Shape

`report.json` is UTF-8 pretty JSON. Unknown fields are allowed, but existing
field names and enum values are append-only once downstream runner tooling
depends on them.

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
    "crate": "studiofs-e2e",
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
- Compose stack: `docker/compose.e2e.yml` defines PostgreSQL 18
  (`postgres:18-alpine`), a floating RustFS tag (`rustfs/rustfs:latest`), and
  a Toxiproxy placeholder. Published ports bind to `127.0.0.1` only. The runner invokes
  `docker compose up --detach --wait` with a project-scoped name and records the
  copied compose file plus logs under `artifacts/docker/`.
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
  must fail preflight when a selected port is already bound. This check is early
  conflict detection; Docker's bind during `compose up` remains the final
  authority because host ports can change state after preflight.
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
- Delete a sandbox only when both `run.json` and `.studiofs-e2e-runner.json`
  identify `studiofs-e2e`, the run ID matches the directory name, the manifest
  sandbox path canonicalizes back to that directory, and the required sandbox
  layout is present.
- A malformed or undeletable candidate inside the cleanup root must be retained
  and recorded as skipped; it must not prevent cleanup from attempting later
  valid runner-owned sandboxes in the same root.
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
  only. Labels must include the run ID. Compose teardown uses
  `docker compose down --volumes --remove-orphans` so PostgreSQL and RustFS
  named volumes do not persist after cleanup.

When cleanup cannot prove ownership, it must retain the resource and mark the
run `cleanup_failed`.

## Initial Scenario Gating

Scenario growth is deliberately staged:

1. `smoke` first: one StudioFS client, isolated client root, real mount,
   create/write/read/readdir/delete/unmount, and required artifacts.
2. Basic convergence next: two clients with separate explicit client roots,
   PostgreSQL and S3-compatible services, publish/apply visibility, and stable
   report fields.
3. PostgreSQL outage recovery: stop/restart PostgreSQL and validate
   local-first writes, replay, convergence, and failure reporting.
4. Collision scenarios only after outage reporting is stable: concurrent
   writes, renames, deletes, locks, and conflict cases.

Each new scenario must keep the same hook model, report schema, artifact names,
and cleanup safety rules. Adding outage or collision coverage before the smoke
runner, convergence gate, and artifact collection are stable is out of scope.
