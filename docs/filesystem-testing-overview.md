# How We Test StudioFS As A Real Filesystem

This document is a friendly, not-so-technical overview of how we currently test
StudioFS. It does not replace the technical runner contract or the detailed
test matrix. Think of it as the story of our testing approach: what kind of
safe playground we create, what we do inside it, and what confidence we get
from each layer.

## The Short Version

StudioFS is not tested only as a library. We also mount it as a real filesystem,
start several independent clients, give them shared PostgreSQL metadata and
S3-compatible object storage, then make them do the kinds of things real users
and real machines do: write files, rename paths, lose services, crash clients,
come back online, and check whether everyone sees the same final workspace.

The goal is not just "does one command work?" The goal is: after stress,
conflict, outages, and restarts, does StudioFS still behave like one reliable
shared filesystem?

## The Sandbox

Every real E2E run creates its own sandbox under `target/e2e-runs/<run-id>`.
That sandbox is the run's little laboratory. It contains the run manifest,
reports, logs, Docker diagnostics, per-scenario artifacts, and one isolated
client area for each mounted StudioFS client.

The runner also starts a private Docker Compose stack for the run. Today that
stack includes:

- PostgreSQL, which stores shared metadata and replication history.
- RustFS, an S3-compatible object store used for file payloads.
- Toxiproxy, which is available for future network fault scenarios.

Each mounted client gets its own local root and cache. That matters because it
prevents the test from accidentally passing by sharing local state. If three
clients converge, they converged through the same public mechanisms that real
StudioFS clients use.

Cleanup is part of the test. The runner records what it started, tears down
mounts, child processes, containers, volumes, and locks, and writes cleanup
artifacts. If cleanup cannot prove ownership of something, it keeps the item and
reports the problem instead of deleting aggressively.

## What We Record

The runner writes a machine-readable `report.json` for every meaningful run.
Scenario details live under `artifacts/scenarios/<scenario-name>/`, and client
logs live under `artifacts/clients/<client-name>/`.

For conflict and chaos tests, we also write `operations.ndjson`. This is an
append-only timeline of what the scenario planned, started, accepted, rejected,
or observed. It makes failures much easier to replay and explain: instead of
"something went wrong", we can say which operation happened, in which order, and
what each mounted client saw.

## The Logical Simulation Layer

Before we spend time on real mounts, we use deterministic simulation in the
`studiofs-scenario` crate. This layer does not start Docker, PostgreSQL, RustFS,
FUSE, or WinFSP. It models clients, operation ordering, metadata delivery, and
object-store behavior in a controlled way.

This is where we search for tricky interleavings cheaply. When the simulator
finds a counterexample, it saves a replay artifact. That artifact can then be
used to understand the exact ordering and, where appropriate, promote the case
into a core contract test or a real mounted E2E test.

Simulation is the searchlight. Real mounted E2E is the proof that the product
works through the actual operating-system filesystem interface.

## Real Mounted E2E Levels We Have Today

### 1. Basic Multi-Client Smoke

This is the simplest real mounted proof. We start multiple clients on one
shared volume, write through one mounted client, and wait until the others see
the same file through their own mounts.

It proves the basic chain works: initialize, discover, mount, write, upload,
publish metadata, sync peers, validate the mounted tree, and clean up.

### 2. PostgreSQL Outage Recovery

Here we stop PostgreSQL while clients are mounted. Clients should still accept
local-first work where the contract allows it. After PostgreSQL returns, the
clients must converge again.

This tests that temporary metadata-service loss does not automatically mean
local work is lost, and that recovery does not corrupt the shared namespace.

### 3. RustFS / S3 Outage Recovery

This scenario stops the object store while a mounted client writes new data.
The writer must keep its accepted local data readable. Peers must not pretend
to see the new payload before it can actually be published. After RustFS comes
back, everyone must converge.

This tests the split between local durability and remote publication.

### 4. Mounted Client Crash / Restart Recovery

We kill a mounted client after it has accepted local work, then remount it with
the same client root. The restarted client must recover its local state, and
the other clients must eventually converge with it.

This tests whether accepted local work survives process death.

### 5. Focused Namespace Collisions

These are smaller conflict scenarios that target specific filesystem races:

- rename versus rename
- rename versus unlink
- unlink versus append/write
- overwrite versus repeated reads
- mkdir/rmdir versus create
- readdir/stat while mutations are in flight

They test whether clients converge on one valid namespace without duplicate
paths, missing parents, unexpected resurrection, or checksum mismatch.

### 6. Seeded Mounted Chaos

The mounted chaos scenario combines several namespace collision families across
multiple rounds. It uses a deterministic seed, so a failure can be reproduced
with the same seed.

This is the first broader chaos layer. It is still bounded and explainable, but
it mixes enough operations to catch bugs that focused one-race tests can miss.

### 7. Mounted Fault Chaos

Fault chaos adds service and process failures to mounted namespace chaos. It
mixes a conflict burst, PostgreSQL outage, client force-kill and remount,
RustFS outage, and a late cold observer client with a fresh local root/cache.

This tests whether StudioFS still converges when several realistic bad things
happen in one run.

### 8. LWW Reconnect Chaos

This scenario focuses on last-writer-wins behavior during reconnect. A stale
writer goes offline, online peers publish ordered writes, then the stale writer
returns and writes the final expected version. A cold observer joins at the end.

The test does not only check that clients converge. It also checks that they
converge to the right winner.

### 9. Mounted Open-Handle Convergence

This scenario keeps real operating-system file handles open while another
mounted client changes the namespace underneath them. One case unlinks the path
while the handle owner keeps reading and writing through the open handle. The
other case renames the path while the handle owner appends, flushes, fsyncs,
reads back, and releases the same open handle.

The pass condition has two sides. The open handle must keep behaving like a
real local file handle until release, but the mounted namespace must still
converge to the public winner. Deleted paths must not come back after
flush/fsync/release, the old rename source must stay absent, and the renamed
destination must remain visible to every mounted client.

The focused regression and the real Windows/WinFSP mounted scenario now pass.
The final validation shows all three clients converge,
`open-handles/unlink-target.txt` stayed absent, `open-handles/rename-target.txt`
stayed absent, `open-handles/renamed-target.txt` stayed visible, and both open
handles read back the expected payload and checksum.

### 10. Mounted Publication-Boundary Crash Matrix

This scenario targets the boundary between local durability and public
publication. A mounted writer creates a file, crosses one explicit boundary,
then the scenario kills or gracefully shuts down that writer and remounts it
with the same client root.

The matrix covers four boundaries:

- flush
- fsync
- release/close
- graceful shutdown drain

For each boundary, the restarted writer must read back the exact payload from
its recovered local state. Peers are checked at the scenario's publication gate,
then the runner waits for normal mounted-tree convergence across all clients.

This level is important because local-first filesystems have two different
promises that are easy to mix up. First, data accepted by the writer must become
locally durable at the right boundary and survive process death. Second, peers
must converge through the public publication path rather than by sharing private
writer state.

The scenario writes `operations.ndjson`,
`publication-boundary-summary.json`, and `validation.json`. The successful
Windows/WinFSP run covered all four boundaries, remounted `client-a` with the
same client root each time, matched local and final checksums, and converged
`client-a`, `client-b`, and `client-c` with no mounted-tree differences.

### 11. Mounted Hot-Spot Directory Churn

This scenario focuses product chaos on one busy mounted directory. Two mounted
writer clients repeatedly mutate the same bounded `hotspot/` working set with
creates, writes, renames, and unlinks. A third mounted observer concurrently
probes that same directory with readdir, stat, and read operations.

The test intentionally allows temporary divergence while the churn window is
active. The pass condition is about the final filesystem contract: every
mounted client must converge to the same valid tree, the hot directory must not
end with duplicate logical slots, and observer reads must only see valid
StudioFS hotspot payloads rather than empty or corrupted results.

The scenario writes `operations.ndjson`,
`mounted-hotspot-churn-summary.json`, and `validation.json`. This gives us a
bounded timeline of every planned and accepted mutation, plus a compact summary
of the final namespace and observer activity.

## What A Passing Full Sweep Means

A passing full mounted E2E sweep means the current implementation survived all
implemented real-mount scenarios in one run: basic sharing, service outages,
client crash/restart, focused namespace collisions, seeded mounted chaos,
fault chaos, LWW reconnect chaos, open-handle namespace convergence, and
publication-boundary crash recovery, and mounted hot-spot directory churn.

It does not mean the filesystem is perfect. It means the current safety net is
working and gives us a strong baseline before we add deeper scenarios.

## How We Use Failures

When a scenario fails, we keep the sandbox and inspect the report, event stream,
operation log, client logs, Docker logs, and validation artifacts. The failure
then becomes one of three things:

- a product bug in StudioFS,
- a gap or bug in the test harness,
- or a scenario contract that needs to be made more precise.

The important part is that we do not treat chaos output as noise. A good failure
should become a smaller focused regression before we move on.

## What Comes Next

The next planned testing levels go deeper into filesystem semantics that are
easy to get wrong: sparse-file truncate/write races, long-history cold
observers, metadata invalidation dropouts, symlink/attribute convergence, and
adversarial runner cleanup.

The workflow is intentionally staged. We add one level, run it, fix the bugs it
finds, rerun the existing sweep to make sure nothing regressed, and only then
move to the next level.
