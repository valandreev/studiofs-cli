# Real E2E Runner Contract

This document defines the implementation contract for the real mounted E2E
runner before scenario-specific logic is added. It is the shared contract for
the runner skeleton, artifact collection, and CI upload/reporting work.

The runner is not part of the default fast test suite. It is an explicit,
operator-started workflow for tests that need Docker, PostgreSQL, S3-compatible
object storage, and a real FUSE, macFUSE, or WinFSP mount.

## RWX Linux/FUSE Hosted Sweep

RWX is the primary hosted environment for product-level real mounted E2E
coverage while the shared filesystem behavior is still being expanded. The
manual workflow at `.rwx/e2e.yml` runs the `e2e-linux-full` target on
Ubuntu with Docker support and `/dev/fuse`, builds the `studiofs` binary, and
executes:

```text
studiofs-e2e run --all \
  --seed 360 \
  --timeout 90m \
  --studiofs-bin target/debug/studiofs \
  --root target/e2e-runs \
  --run-id rwx-full-<RWX_RUN_ID> \
  --platform linux-fuse \
  --infra compose \
  --retain always
```

This target is the first completion bar for the shared FS logic: every
registered scenario must pass on RWX Linux/FUSE before the same scenario set is
treated as ready for Windows/WinFSP-specific hang and shutdown work. New
implemented scenarios should join the registry and then become part of the RWX
full sweep through `--all`; planned-but-unimplemented scenario families should
not be represented by passing placeholders.

The RWX run owns exactly one sandbox rooted at
`target/e2e-runs/rwx-full-<RWX_RUN_ID>`. The run ID must contain only ASCII
letters, digits, underscores, and hyphens before it is used in any artifact
path. RWX uploads `target/e2e-runs` as the `e2e-run` artifact so the canonical
run report, Docker diagnostics, per-scenario operation logs, mounted-tree
snapshots, and per-client logs stay available after both passes and failures.
The RWX task timeout is `120m`, while the runner watchdog is `90m`; the task
timeout must remain higher so the runner can write its own report and cleanup
artifacts if the mounted sweep stalls. During bring-up the workflow retains
sandboxes on success as well as failure; that can be tightened after the full
sweep is stable.

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
cargo run -p studiofs-e2e -- run --all --seed 360 --timeout 4h --studiofs-bin <path-to-studiofs>
cargo run -p studiofs-e2e -- run basic-multi-client-smoke
cargo run -p studiofs-e2e -- run postgres-outage-multi-client-recovery --studiofs-bin <path-to-studiofs>
cargo run -p studiofs-e2e -- run rustfs-outage-multi-client-recovery --studiofs-bin <path-to-studiofs>
cargo run -p studiofs-e2e -- run mounted-client-crash-restart-recovery --studiofs-bin <path-to-studiofs>
cargo run -p studiofs-e2e -- run collision-rename-vs-unlink --studiofs-bin <path-to-studiofs>
cargo run -p studiofs-e2e -- run mounted-open-handle-convergence --studiofs-bin <path-to-studiofs>
cargo run -p studiofs-e2e -- run mounted-fault-chaos-convergence --seed 360 --timeout 30m --studiofs-bin <path-to-studiofs>
cargo run -p studiofs-e2e -- run mounted-lww-reconnect-chaos-convergence --seed 360 --timeout 30m --studiofs-bin <path-to-studiofs>
cargo run -p studiofs-e2e -- run mounted-publication-boundary-crash-matrix --studiofs-bin <path-to-studiofs>
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
| `--all` | Run every registered scenario in one supervised sweep. Compose infrastructure is started once for the run; each scenario child gets a fresh StudioFS volume and scenario-scoped client roots/cache/logs. |
| `--scenario <name>` | Repeatable. Accepted values include `basic-multi-client-smoke` (`smoke`), `postgres-outage-multi-client-recovery` (`postgres-outage`), `rustfs-outage-multi-client-recovery` (`rustfs-outage`, `s3-outage`), `mounted-client-crash-restart-recovery` (`client-crash-restart`, `crash-restart`), `mounted-publication-boundary-crash-matrix` (`publication-boundary-crash`, `publication-boundary`), `mounted-open-handle-convergence` (`open-handle`, `open-unlink-rename`), `mounted-chaos-convergence` (`mounted-chaos`, `chaos`), `mounted-hotspot-churn-convergence` (`hotspot-churn`, `hot-dir-churn`), `mounted-fault-chaos-convergence` (`fault-chaos`, `chaos-faults`), `mounted-lww-reconnect-chaos-convergence` (`lww-chaos`, `reconnect-lww`), and the mounted namespace collision scenarios `collision-rename-vs-rename`, `collision-rename-vs-unlink`, `collision-unlink-vs-append`, `collision-overwrite-vs-read`, `collision-mkdir-rmdir-vs-create`, and `collision-readdir-stat-during-mutation`. |
| `--studiofs-bin <path>` | Path to the product CLI under test. Real mounted scenarios require it when `--infra compose` or `--infra external-env` is used. The runner may build it only when an explicit build flag is added. |
| `--root <path>` | Parent run directory. Defaults to `target/e2e-runs`. |
| `--run-id <id>` | Optional stable run ID. If omitted, use an ISO-like UTC timestamp plus short random suffix. Explicit IDs must be safe single directory names: no separators, drive prefixes, traversal segments, leading dots, or other shell/path metacharacters. |
| `--platform auto|linux-fuse|macos-fuse|windows-winfsp` | Mount backend selection. `auto` resolves from host OS and installed driver checks. |
| `--infra compose|external-env|disabled` | External infrastructure mode. `compose` starts `docker/compose.e2e.yml`, waits for healthy required services, collects Docker logs, and tears the stack down. `external-env` uses caller-provided PostgreSQL and S3/RustFS settings from process environment or `.env` and never starts, stops, or cleans those services. `disabled` is reserved for runner self-tests and skeleton-only validation. |
| `--retain always|on-failure|never` | Sandbox retention policy. Default is `on-failure`. |
| `--report <path>` | Optional copy target for the final `report.json`. The canonical report remains in the run sandbox while the sandbox is retained. |
| `--port-range <start-end>` | Optional deterministic host port pool for Docker services. The compose runner allocates PostgreSQL, RustFS S3, RustFS console, and Toxiproxy API ports in that order. Use this when a default host port is occupied or Docker reports it healthy but host-side clients cannot reach it. |
| `STUDIOFS_E2E_COMPOSE_FILE` | Optional environment override for the compose file path. Defaults to `docker/compose.e2e.yml` discovered from the current directory or executable location. |
| `STUDIOFS_E2E_EXTERNAL_PG_DSN` / `STUDIOFS_TEST_PG_DSN` | PostgreSQL DSN for `--infra external-env`. The E2E-specific variable wins when both are set. |
| `STUDIOFS_E2E_EXTERNAL_S3_ENDPOINT` / `STUDIOFS_TEST_ONLINE_S3_ENDPOINT` / `STUDIOFS_S3_ENDPOINT` | S3/RustFS endpoint for `--infra external-env`. |
| `STUDIOFS_E2E_EXTERNAL_S3_BUCKET` / `STUDIOFS_TEST_ONLINE_S3_BUCKET` / `STUDIOFS_S3_BUCKET` | S3/RustFS bucket for `--infra external-env`. The runner uses a UUID StudioFS volume name for each scenario bootstrap inside this bucket. |
| `STUDIOFS_E2E_EXTERNAL_S3_ACCESS_KEY` / `STUDIOFS_TEST_ONLINE_S3_ACCESS_KEY` / `STUDIOFS_S3_ACCESS_KEY` | S3/RustFS access key for `--infra external-env`. |
| `STUDIOFS_E2E_EXTERNAL_S3_SECRET_KEY` / `STUDIOFS_TEST_ONLINE_S3_SECRET_KEY` / `STUDIOFS_S3_SECRET_KEY` | S3/RustFS secret key for `--infra external-env`. |
| `STUDIOFS_E2E_EXTERNAL_S3_REGION` / `STUDIOFS_TEST_ONLINE_S3_REGION` / `STUDIOFS_S3_REGION` | Optional S3/RustFS region for `--infra external-env`; defaults to `us-east-1`. |
| `--timeout <duration>` | Enforced whole-run watchdog. Supported units are `ms`, `s`, `m`, and `h`, for example `500ms`, `30s`, `10m`, or `1h`. When a multi-scenario run is selected, the parent runner executes each scenario in a supervised child process and passes the remaining deadline as the child command timeout. If that timeout expires, the parent kills the child, marks the active scenario `timed_out`, records the active hook when known, writes best-effort `report.json`, `events.ndjson`, and `cleanup.json`, and does not start later scenarios. Single-scenario runs still use the direct lifecycle path for focused local debugging. |

The public runner implements `list`, `run`, and `clean`. Hidden helper
subcommands such as `__run-one` are parent-owned implementation details; they
must reuse the same report schema, sandbox layout, and cleanup safety checks.

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
        scenario-report.json
        child.stdout.log
        child.stderr.log
        operations.ndjson
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

For RWX, `<run-id>` is `rwx-full-<RWX_RUN_ID>` and the whole
`target/e2e-runs` tree is uploaded as the hosted artifact. The runner still
owns the nested sandbox and report schema; RWX only provides the Linux/FUSE
host, Docker daemon, and artifact uploader. A full RWX sweep must start compose
once for the run, then give each scenario a fresh StudioFS volume and
scenario-scoped client runtime under `artifacts/scenarios/<scenario-name>/`.

On Windows, `artifacts/clients/<client>/mount/` is a metadata directory for the
drive-letter mount. It must contain the chosen mount point in a small text or
JSON artifact, because `Z:` is not a directory inside the sandbox.

`run.json` is the immutable run manifest written before setup starts. It must
include the run ID, absolute sandbox path, requested scenarios, retention
policy, selected platform, explicit client roots, allocated ports, child
process IDs once known, and cleanup allowlist.

## Interactive Progress

Real E2E runs print stable line-oriented progress to stdout and flush after
each line so PowerShell and CI logs show the active operation immediately. The
runner prints the run ID and sandbox path before preflight so operators can
inspect `events.ndjson`, child logs, and scenario artifacts while the run is
still active.

Progress is emitted as stable key-value lines for run start, preflight start
and result, scenario start and result, hook start and result, and cleanup start
and result. While a hook is active, the runner emits a heartbeat every 30
seconds. Multi-scenario `run --all` executions redirect each child scenario's
stdout and stderr to `child.stdout.log` and `child.stderr.log`; the parent
supervisor still prints heartbeat lines to visible stdout while it waits for
the child. Heartbeats are plain log lines, not terminal-control updates:

```text
[studiofs-e2e][2026-05-06T17:30:00.000Z] run=20260506T173000Z-a1b2c3 scenario=1/1 scenario_name=mounted-fault-chaos-convergence hook=run phase=heartbeat elapsed=90s last_event=run.started sandbox=E:\dev\studiofs-cli\target\e2e-runs\20260506T173000Z-a1b2c3
```

Use `run`, `scenario_name`, `hook`, `elapsed`, and `last_event` from the latest
heartbeat to identify the active hook without waiting for post-mortem artifact
inspection. `last_event` is sampled from the scenario operation log when a
scenario has written operations, and falls back to the current hook boundary
when no later operation is available. Values containing whitespace are emitted
as JSON strings so paths remain unambiguous for log parsers.

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

`operations.ndjson` uses the shared `studiofs-scenario` operation schema. Each
line is one `ScenarioOperationRecord` with `schema_version = 1`, scenario name,
monotonic per-file `sequence`, optional client, logical operation, outcome,
simulation support marker, and string metadata. The log is append-only within a
scenario run. Runner hook boundaries are recorded as `scheduler_checkpoint`
operations so the deterministic simulator can ignore or replay them without
knowing about the real mounted runner internals.

Simulation-replayable operations keep `simulation_support.status = "supported"`.
Mounted-only or platform-specific boundaries must stay in the log only when
they are explicitly marked as unsupported with a reason. Parsing or replaying an
operation log must not require Docker, PostgreSQL, RustFS, real mount points, or
platform bridge crates.

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

If a run is interrupted before normal cleanup, `studiofs-e2e clean` must recover
stale compose infrastructure before removing the sandbox. Recovery is allowed
only when both `.studiofs-e2e-runner.json` and `run.json` prove the candidate is
runner-owned, `run.json` records `infra: "compose"` and a `compose_project`, and
the sandbox path in the manifest canonicalizes back to the candidate directory.
The recovery command is the same bounded compose teardown used by normal
cleanup: `docker compose down --volumes --remove-orphans` for the recorded
project. Ambiguous candidates are retained and reported as skipped. A retained
sandbox with `cleanup.json` status `passed` is treated as having completed the
cleanup lifecycle and must not block later compose preflight.

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
Runner-owned foreground CLI commands must be bounded. The controller applies
per-command timeouts to `studiofs init`, `studiofs discover`, and
`studiofs umount`, appends captured stdout/stderr to the per-client logs even
when a command times out, kills the timed-out command process, and reports the
client name, command action, timeout, executable path, and stdout/stderr log
paths. If `studiofs umount` itself times out, the controller must still continue
to the tracked mount-process fallback so cleanup can force-kill the foreground
mount process and return control to scenario reporting.
When the compose RustFS endpoint is still settling after Docker reports the
service healthy, the first `studiofs init` may retry narrowly-classified
object-store transport failures during bucket creation. The retry window is
bounded and must not hide credential, permission, bucket naming, or endpoint
configuration errors.
Each real E2E bootstrap uses a UUID as the StudioFS volume name. This keeps
multi-scenario runs from colliding with a volume created by an earlier scenario
in the same PostgreSQL metadata database, while preserving the readable
run-scoped bucket name for Docker/RustFS diagnostics.
Crash-style remounts do not run `studiofs umount` for the killed client. On
Linux, before spawning the replacement mount process, the controller attempts a
lazy stale-mount cleanup for leftover FUSE mount points with `fusermount3 -u -z`,
`fusermount -u -z`, or `umount -l`. Other platforms keep the existing remount
path unless a platform-specific stale-mount cleanup is added.

`basic-multi-client-smoke` uses the first three default clients. It initializes
one shared remote volume from `client-a`, discovers it from `client-b` and
`client-c`, mounts all three clients, writes `studiofs-e2e-smoke.txt` through
`client-a`'s mount point, waits for the mounted tree validator to observe the
same file from all three mount points, writes `bootstrap-summary.json`,
`smoke-summary.json`, and `validation.json`, and then unmounts the clients.
The `--infra disabled` path remains limited to runner self-tests and writes only
the skeleton validation artifact.

`postgres-outage-multi-client-recovery` uses the same three-client mounted
bootstrap, writes an initial file while PostgreSQL is healthy, stops only the
PostgreSQL compose service, performs accepted local-first writes from multiple
mounted clients, starts PostgreSQL again with compose health waiting, and then
uses the mounted tree validator for final convergence. Temporary peer
invisibility while PostgreSQL is stopped is not a failure; only local write
failures and post-recovery convergence failure fail the scenario. The scenario
publishes and converges the healthy initial file before stopping PostgreSQL,
recording that gate under `healthy-validation/` and
`initial_publication_gate`. It writes PostgreSQL outage/recovery events and
local write operations to `operations.ndjson` and `outage-operations.ndjson`
using the shared operation schema, and writes
`postgres-outage-recovery-summary.json` with the initial publication gate,
outage duration, accepted-operation count, recovery time, and the final
validator result.
`recovery_time_ms` starts before the PostgreSQL compose service is brought back
with `docker compose up --wait`, so it includes the PostgreSQL healthcheck
readiness gate as well as mounted tree convergence time.

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
`client-a` with the same client root, verifies the durable data is still
readable after restart, waits for upload/publication and peer convergence, and
then validates matching mounted tree checksums from all clients. The scenario
runs two modes in one invocation:

- `after_local_durability_boundary`, which crashes immediately after the
  documented local durability boundary is crossed.
- `before_remote_publication_completes`, which crashes after local durability
  but before the scenario waits for remote publication and peer convergence.

The scenario writes `operations.ndjson`,
`crash-restart-recovery-summary.json`, and `validation.json`. The summary
records the restarted client, reused client root, per-mode local and final
checksums, whether a peer saw any pre-convergence checksum in the
`before_remote_publication_completes` mode, restart timing, convergence timing,
and mounted tree validator result. Publication waiting is intentionally the
public mounted-tree convergence loop; the scenario must not inspect private S3,
PostgreSQL, VFS, cache, or redb state to decide success. It must not delete
local metadata, rerun `discover`, or share one local metadata database between
clients during recovery.

`mounted-open-handle-convergence` uses three mounted clients against one shared
remote volume. `client-a` opens a file handle and keeps it live while
`client-b` unlinks that path; the open handle then appends, flushes, fsyncs,
reads back through the handle, and releases. A second case keeps another handle
open while `client-b` renames the namespace entry, then repeats append, flush,
fsync, read, and release through the still-open handle. `client-c` is the
observer for namespace convergence. The pass condition is purely public
mounted behavior: all clients converge to the same final tree, the unlinked
path does not resurrect after flush/fsync/release, the old rename source stays
absent, and the rename destination is visible. The scenario writes
`operations.ndjson`, `mounted-open-handle-summary.json`, and `validation.json`;
open-handle lifecycle records use `mounted_only` operations because the
simulation layer does not model OS file descriptor state.

`mounted-publication-boundary-crash-matrix` uses three mounted clients against
one shared remote volume. `client-a` writes payloads and is force-killed then
remounted at four local-first publication boundaries: flush, fsync,
release/close, and graceful shutdown drain. Each case reuses the same client
root, verifies the payload is locally durable after remount, opens the
scenario-defined public convergence gate by refreshing observer peers, records
peer mounted-tree checksums at that gate, and then waits for final mounted-tree
convergence across all clients. The scenario writes `operations.ndjson`,
`publication-boundary-summary.json`, and `validation.json`; boundary checkpoints
and crash/restart operations use the shared `ScenarioOperationRecord` schema,
while the shutdown-drain marker is explicitly recorded as mounted-only.

Mounted namespace collision scenarios use the first three default clients
against one shared remote volume. Each scenario establishes a bounded initial
tree, starts conflicting operations from multiple clients through a shared
barrier, writes every attempted operation to `operations.ndjson`, waits for
mounted tree convergence, and writes `namespace-collision-summary.json` plus
`validation.json`. Operation log records use the shared schema; sequence replaces
the old ad hoc deterministic order marker, outcome replaces the old string
result field, and operation variants carry paths such as rename source/target,
unlink path, read path, or create path. The suite currently includes:

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

`mounted-chaos-convergence` is the seeded product chaos scenario. It uses the
first three default clients, creates a bounded hot working set, then executes
multiple concurrent rounds chosen from the same conflict families as the
focused collision suite: rename/rename, rename/unlink, unlink/append,
overwrite/read, mkdir/rmdir/create, and readdir/stat during mutation. The
scenario records each round's planned and started operations in
`operations.ndjson` before launching mounted operation helpers, then records
accepted or rejected outcomes after helpers return. Each mounted filesystem
operation runs in a short-lived helper process rather than an in-runner thread,
so a blocked platform syscall cannot keep the runner's main process from
writing the failure report. A mounted-chaos round has a bounded timeout; on
timeout the scenario force-terminates pending helper processes, reports their
operation labels, and still enters mounted cleanup. The scenario writes
`mounted-chaos-summary.json` and `validation.json`, and uses only mounted tree
convergence plus namespace invariants as the pass condition. A failure should
be reproducible with the same `--seed`.

`mounted-hotspot-churn-convergence` focuses the mounted product chaos layer on
one bounded hot directory. It uses the first three default mounted clients: two
clients repeatedly create, unlink, rename, overwrite, and append files with the
same `slot-*` prefix inside `hotspot/`, while the third client concurrently
performs readdir, stat, and open/read probes against the same directory. The
scenario records planned, started, accepted, rejected, and probe operations in
`operations.ndjson`, writes `mounted-hotspot-churn-summary.json` and
`validation.json`, and validates only the final mounted tree plus namespace
invariants. Temporary divergence during the churn window is allowed; duplicate
canonical paths, missing parents, resurrected children, and checksum mismatches
are final validation failures.

Mounted cleanup must not rely only on graceful unmount. If `studiofs umount`
returns but the mounted process does not exit within the configured process-exit
timeout, the client controller force-kills the mount process, clears the
recorded PID, marks the client as killed, and returns the unmount timeout as the
cleanup error. If the `studiofs umount` command process itself times out, the
same mount-process force-kill fallback is required and the cleanup error must
identify the timed-out command and its captured log paths.
On Windows, `studiofs umount` itself must not report success after requesting
forced termination unless the mount process is actually signaled as exited.

`mounted-fault-chaos-convergence` is the deeper product fault-chaos scenario.
It uses four clients against one shared remote volume. The first three clients
mount as warm mutating clients, run a seeded conflict burst, accept local work
during a PostgreSQL outage, force-kill and remount one seeded client after a
local durability boundary, accept payload-bearing work during a RustFS/S3
outage, and finally mount the fourth client as a cold observer with a fresh
local root/cache. Final validation requires mounted tree convergence across all
four clients, including the restarted client and the cold observer. The
scenario writes `operations.ndjson`, `mounted-fault-chaos-summary.json`, and
`validation.json`; outage/crash/restart/cold-observer boundaries are recorded
with the shared `ScenarioOperationRecord` schema.

`mounted-lww-reconnect-chaos-convergence` is the oracle-backed LWW reconnect
scenario. It uses five clients against one shared remote volume. Four clients
start warm; `client-c` is force-killed as a stale writer, `client-a` and
`client-b` publish ordered writes to the same path while `client-c` is offline,
then `client-c` remounts, writes the third ordered version, is crash/restarted
before final convergence, and `client-e` mounts late as a cold observer. Final
validation still requires mounted tree convergence across all five clients, but
it also validates `lww-chaos/winner.txt` against an explicit LWW oracle derived
from the planned operation order. This catches failures where every mounted
client converges to the same tree but the winner is not the last expected
writer. The scenario writes `operations.ndjson`,
`mounted-lww-reconnect-chaos-summary.json`, `lww-oracle.json`, and
`validation.json`.

## Mounted Tree Convergence Validation

Mounted scenarios validate client-visible invariants through the
`studiofs-e2e::mounted_tree` validator. The validator snapshots public
mount points only; it must not inspect private VFS, metadata, cache, PostgreSQL,
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

The mounted validator can translate mounted snapshots into
`studiofs-scenario::invariant::LogicalSnapshot` for shared logical invariant
checks where the public mounted surface has enough information. The translation
intentionally carries only relative path, entry type, regular-file size, and
regular-file checksum, plus symlink target when present. It does not infer inode
IDs, object lifecycle state, durable-write expectations, hard-link
relationships, or replay outcomes from a mounted tree. Scenarios that need
those invariants must provide the missing logical facts explicitly or keep using
scenario-specific validation.

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
- Compose stack: `docker/compose.e2e.yml` defines PostgreSQL 18, RustFS, and a
  Toxiproxy placeholder using version-pinned service images for reproducible
  runs. Published ports bind to `127.0.0.1` only. The runner invokes
  `docker compose up --detach --wait` with a project-scoped name, verifies the
  host-mapped RustFS S3 health endpoint with bounded retries, and records the
  copied compose file plus logs under `artifacts/docker/`. If Docker reports the
  stack healthy but the host-mapped S3 endpoint remains unreachable and the
  caller supplied spare ports in `--port-range`, the runner may recreate the
  stack on a higher port block within that range before scenarios start.
- Mount support:
  - Linux requires a usable `/dev/fuse`, mount point creation rights, and the
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
- stale runner-owned infrastructure: compose preflight scans the run root for
  prior runner-owned compose manifests that do not have a passed `cleanup.json`
  before starting Docker. If stale or ambiguous candidates are present,
  preflight fails with a diagnostic that names the affected run IDs and points
  to `studiofs-e2e clean --root <run-root>`.
- stale mounts: requested mount points or drive letters must be unused, or must
  match a prior runner manifest that explicitly authorizes cleanup. Unknown
  mounted paths are a `preflight_failed` result, not an implicit force-unmount.
- default Windows mounted-run lock: until per-run drive-letter allocation is
  implemented, Windows WinFSP runs using compose infrastructure and at least one
  mounted scenario acquire a machine-level lock before Docker or clients start.
  The lock is stored under the machine-wide StudioFS E2E state directory on
  Windows. A second default mounted run fails preflight with the lock path,
  current run ID, and recorded holder diagnostics instead of reusing `Z:\`,
  `Y:\`, or other default drive letters.
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
- Unmount only clients that reached the mounted state or still have a tracked
  mount process, then delete mount directories or explicit client roots.
- If unmount fails, retain the affected path and record the failure in
  `cleanup.json`.
- Docker cleanup must target runner-owned compose projects recorded in
  `run.json`. The recorded project name is required for interrupted-run
  recovery, and compose teardown uses
  `docker compose down --volumes --remove-orphans` so PostgreSQL and RustFS
  named volumes do not persist after cleanup. Unknown or ambiguous Docker
  resources must be retained and reported instead of removed. The current
  cleanup command intentionally has no force mode; unproven resources require
  manual operator action.

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
