# How We Test StudioFS As A Real Filesystem

This is a friendly, not-so-technical overview of how we test StudioFS as a
filesystem. It does not replace the technical runner contract or the detailed
test matrix. Think of it as the story of the test setup: what kind of safe
playground we create, what we do inside it, and what kind of confidence each
layer gives us.

## The Short Version

StudioFS is not tested only as a Rust library. We also mount it as a real
filesystem, start several independent clients, connect them to shared
PostgreSQL metadata and S3-compatible object storage, and make them do the
kinds of things real users and real machines do: write files, rename paths,
lose services, crash clients, come back online, and check whether everyone sees
the same final workspace.

The goal is not just "does one command work?" The goal is: after stress,
conflicts, outages, and restarts, does StudioFS still behave like one reliable
shared filesystem?

## The Sandbox

Every real E2E run creates its own sandbox under `target/e2e-runs/<run-id>`.
That sandbox is the run's little laboratory. It contains the run manifest,
reports, logs, Docker diagnostics, per-scenario artifacts, and one isolated
client area for each mounted StudioFS client.

The runner starts a private Docker Compose stack for the run. Today that stack
includes:

- PostgreSQL 18, which stores shared metadata and replication history.
- RustFS, an S3-compatible object store used for file payloads.
- Toxiproxy, which is available for network-fault-oriented scenarios.

Each mounted client gets its own local root, local metadata, cache, and mount
point. That matters because it prevents the test from accidentally passing by
sharing private local state. If three clients converge, they converged through
the same public mechanisms that real StudioFS clients use.

Cleanup is part of the test. The runner records what it started, tears down
mounts, child processes, containers, volumes, and locks, and writes cleanup
artifacts. If cleanup cannot prove ownership of something, it keeps the item and
reports the problem instead of deleting aggressively.

## What We Record

The runner writes a machine-readable `report.json` for every meaningful run.
It also writes `events.ndjson`, `preflight.json`, `cleanup.json`, stdout/stderr
logs, Docker diagnostics, and scenario artifacts under
`artifacts/scenarios/<scenario-name>/`.

Many scenarios also write `operations.ndjson`. This is a timeline of what the
scenario planned, accepted, rejected, or observed. It makes failures easier to
explain: instead of "something went wrong", we can say which operation happened,
in which order, and what each mounted client saw.

## The Active Testing Layers

The current workspace has two active filesystem-testing layers.

First, focused Rust tests check runner contracts, mounted-tree validation,
client process control, metadata behavior, cache behavior, and VFS invariants.
These are fast enough to run while developing and are good at catching narrow
regressions.

Second, `studiofs-e2e` runs StudioFS through real mounts. This layer uses
Docker services and OS filesystem bridges such as WinFSP on Windows or FUSE on
Linux/macOS. It is slower, but it proves behavior through the actual filesystem
interface instead of through private internals.

Older documentation mentioned a separate deterministic simulation crate. That
crate is not part of the current workspace, so this overview describes the
testing surface that exists in the codebase now.

## Real Mounted E2E Scenarios We Have Today

`studiofs-e2e list` currently exposes these mounted scenarios in this order.
The same numbers are used by `run --scenario`, so `--scenario 1-8` runs the
baseline gate set and `--scenario 12-16` runs the deeper mounted scenario group.

### 1. Basic Multi-Client Smoke

This is the simplest real mounted proof. We start multiple clients on one
shared volume, write through one mounted client, and wait until the others see
the same file through their own mounts.

It proves the basic chain works: initialize, discover, mount, write, publish,
sync peers, validate the mounted tree, and clean up.

### 2. PostgreSQL Outage Recovery

Here we publish a healthy baseline, stop PostgreSQL while clients are mounted,
accept local-first work where the contract allows it, then bring PostgreSQL
back. The writer must recover first, then normal publication and peer
convergence must finish.

This tests that temporary metadata-service loss does not automatically mean
accepted local work is lost, and that recovery does not corrupt the shared
namespace.

### 3. RustFS / S3 Outage Recovery

This scenario stops the object store while a mounted client writes new data.
The writer must keep its accepted local data readable. Peers must not pretend
to see the new payload before it can actually be published. After RustFS comes
back, everyone must converge.

This tests the split between local durability and remote publication.

### 4. Mounted Client Crash / Restart Recovery

We kill a mounted client after it has accepted local work, then remount it with
the same client root. The restarted client must recover readable local state,
and the run records whether peer convergence succeeds after that recovery.

This tests whether accepted local work survives process death.

### 5. Publication Boundary Crash Matrix

This scenario writes data and crashes or remounts around important publication
boundaries: flush, fsync, release/close, and shutdown drain. The writer must be
able to read the exact payload after remount.

This tests the line between local durability and public visibility.

### 6-11. Focused Namespace Collisions

These are smaller conflict scenarios that target specific filesystem races:

- rename versus rename
- rename versus unlink
- unlink versus append/write
- overwrite versus repeated reads
- mkdir/rmdir versus create
- readdir/stat while mutations are in flight

They test whether clients converge on one valid namespace without duplicate
paths, missing parents, unexpected resurrection, or checksum mismatch.

### 12. Seeded Mounted Chaos

The mounted chaos scenario combines several namespace collision families across
seeded rounds. It is still bounded and explainable, but it mixes enough
operations to catch bugs that focused one-race tests can miss.

### 13. Mounted Hot-Spot Churn

Hot-spot churn keeps mutating a small set of `slot-*` paths while another
client probes the directory. Temporary divergence is allowed during the run,
but the final mounted tree must converge and remain structurally valid.

This tests busy real-world directories where the same names are created,
deleted, renamed, overwritten, and observed repeatedly.

### 14. Mounted Fault Chaos

Fault chaos combines mounted namespace churn with service and process failures:
PostgreSQL outage, client crash/remount, RustFS outage, and a cold observer.

This tests whether StudioFS still converges when several realistic bad things
happen in one run.

### 15. LWW Reconnect Chaos

This scenario focuses on last-writer-wins behavior during reconnect. A stale
writer goes offline, online peers write ordered versions, then the stale writer
returns and writes the final expected version. A late observer joins at the end.

The test does not only check that clients converge. It also checks that they
converge to the expected winner recorded in the LWW oracle artifact.

### 16. Open-Handle Convergence

This scenario keeps an old file handle open while another client unlinks or
renames paths. The old deleted path must not come back, rename destinations must
be visible, and the final mounted tree must converge.

This tests filesystem behavior that users commonly hit through editors,
renderers, and other long-running processes.

## What The Baseline Gate Means

`local-first-baseline-gate` reruns scenarios 1 through 8 as the accepted
Local-First baseline. That gate is intentionally operator-started rather than
part of the default fast suite. It is meant to answer a practical question:
"Do the core mounted Local-First scenarios still work on real infrastructure?"

Deeper scenarios after level 8 are still part of the real E2E runner, but they
are heavier and should be run deliberately when validating mounted chaos,
bridge behavior, or a specific bug fix.

## What A Passing Mounted Sweep Means

A passing mounted sweep means the current implementation survived the selected
real-mount scenarios: basic sharing, service outages, crash/restart,
publication boundaries, focused namespace collisions, seeded chaos, hot-spot
churn, fault chaos, LWW reconnect, and open-handle behavior.

It does not mean the filesystem is perfect. It means the current safety net is
working and gives us a strong baseline before we add deeper scenarios.

## How We Use Failures

When a scenario fails, we keep the sandbox and inspect the report, event stream,
operation log, client logs, Docker logs, and validation artifacts. The failure
then becomes one of three things:

- a product bug in StudioFS,
- a gap or bug in the test harness,
- or a scenario contract that needs to be made more precise.

The important part is that we do not treat chaos output as noise. A useful
failure should become a smaller focused regression before we move on.

## What Comes Next

The workflow is intentionally staged. We add or restore one level, run it, fix
the bugs it finds, rerun the focused slice to make sure nothing regressed, and
only then move to the next level.

Future filesystem testing can go deeper into sparse-file truncate/write races,
long-history cold observers, metadata invalidation dropouts, symlink and
attribute convergence, and adversarial runner cleanup.
