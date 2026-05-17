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

Simulation v2 now serves as the deterministic backstop for this accepted
mounted Windows baseline. It catches logical metadata, payload, publication,
replay, and convergence regressions cheaply, but it is not a replacement for
real mounted E2E and it is no longer a blocker for accepting scenarios 1-18.

## Real Mounted E2E Scenarios We Have Today

`studiofs-e2e list` currently exposes these mounted scenarios in this order.
The same numbers are used by `run --scenario`, so `--scenario 1-8` runs the
baseline gate set and `--scenario 14-18` runs the accepted deeper mounted
scenario group. The old publication-boundary matrix was split into three
separate crash-acceptance scenarios, so later numbers moved by two positions.
The current appended promotion scenarios are 19-23: sparse-file range convergence,
cold-observer long-history convergence,
stale-remount plus cold-discover catch-up, invalidation-dropout fallback
convergence, and symlink plus metadata-attribute convergence.

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
and the run records whether peers can see the work after that recovery. For the
pre-publication crash mode, peer visibility is diagnostic rather than pass/fail.

This tests whether accepted local work survives process death.

### 5. Crash Before Write Acceptance

This scenario attempts to create the target file, but the client crashes before
that create crosses an acceptance boundary in StudioFS. After remount, the
writer and peers must not see the file at all.

This tests that we do not turn a pre-acceptance crash into fake local recovery
or fake publication.

### 6. Crash After Partial Write Acceptance

This scenario writes and locally syncs only part of the intended payload, then
kills the mounted client. After remount, the writer must recover exactly the
accepted prefix, not the full payload and not unrelated bytes.

This tests that partial accepted local work is recoverable without pretending
the user finished the whole file.

### 7. Crash After Full Write Acceptance

This scenario writes the full payload through a mounted client and crosses a
real local sync boundary before the process is killed. After remount, the
writer must read back the exact full payload.

This tests the boundary where local durability is real, while peer visibility
can still remain eventual.

### 8-13. Focused Namespace Collisions

These are smaller conflict scenarios that target specific filesystem races:

- rename versus rename
- rename versus unlink
- unlink versus append/write
- overwrite versus repeated reads
- mkdir/rmdir versus create
- readdir/stat while mutations are in flight

They test whether clients converge on one valid namespace without duplicate
paths, missing parents, unexpected resurrection, or checksum mismatch.

### 14. Seeded Mounted Chaos

The mounted chaos scenario combines several namespace collision families across
seeded rounds. It is still bounded and explainable, but it mixes enough
operations to catch bugs that focused one-race tests can miss.

### 15. Mounted Hot-Spot Churn

Hot-spot churn keeps mutating a small set of `slot-*` paths while another
client probes the directory. Temporary divergence is allowed during the run,
but the final mounted tree must converge and remain structurally valid.

This tests busy real-world directories where the same names are created,
deleted, renamed, overwritten, and observed repeatedly.

### 16. Mounted Fault Chaos

Fault chaos combines mounted namespace churn with service and process failures:
PostgreSQL outage, client crash/remount, RustFS outage, and a cold observer.

This tests whether StudioFS still converges when several realistic bad things
happen in one run.

### 17. LWW Reconnect Chaos

This scenario focuses on last-writer-wins behavior during reconnect. A stale
writer goes offline, online peers write ordered versions, then the stale writer
returns and writes the final expected version. A late observer joins at the end.

The test does not only check that clients converge. It also checks that they
converge to the expected winner recorded in the LWW oracle artifact.

### 18. Open-Handle Convergence

This scenario keeps an old file handle open while another client unlinks or
renames paths. The old deleted path must not come back, rename destinations must
be visible, and the final mounted tree must converge.

This tests filesystem behavior that users commonly hit through editors,
renderers, and other long-running processes.

### 19. Sparse-File Range Convergence

This scenario creates a bounded sparse file through public mounts, writes byte
ranges around chunk boundaries, verifies zero-filled holes with range reads,
shrinks and extends the logical size, then validates final checksums through
mounted reads.

### 20. Cold-Observer Long-History Convergence

This scenario lets three mounted clients build a small but meaningful history:
they create files, delete one path, rename another, overwrite shared content,
append data, and create a nested child file. A fourth client joins only after
that history already exists.

This tests whether a late observer can discover the volume after prior
activity and still converge to the same final workspace: deleted paths stay
gone, renamed paths appear under the final name, overwritten content has the
expected winner, and nested content is visible.

### 21. Stale-Remount And Cold-Discover Catch-Up

This scenario starts three mounted clients together. One of them, `client-c`,
sees an initial baseline and then goes away while keeping its local StudioFS
state on disk. While it is offline, `client-a` and `client-b` keep changing the
workspace: they create, delete, rename, overwrite, append, and add nested
content.

After that history exists, two different kinds of clients join close together.
`client-c` comes back with old local knowledge and must catch up from where it
left off. `client-d` joins as a brand-new observer and must discover the
already-active volume from scratch. The test passes only if both end up seeing
the same final mounted workspace as the warm writers.

This protects a common real-world shape: one machine sleeps or disconnects,
another new machine joins later, and both must still see the current filesystem
without resurrecting deleted files, missing renamed paths, or reading stale
payloads.

### 22. Invalidation-Dropout Fallback Convergence

This scenario checks what happens when one mounted observer temporarily misses
the normal "something changed" signals. While `client-c` is in that quiet
window, other clients keep publishing ordinary filesystem activity: new files,
overwritten content, and deleted paths.

The important part is that the observer is allowed to be stale for a while, but
not forever. Its slower fallback sync must eventually notice the missed changes
through the public filesystem path. The final workspace must converge, updated
payloads must become readable, and deleted paths must stay deleted instead of
reappearing from old local knowledge.

This protects machines that miss live notifications because of a dropped event,
a sleeping process, or a short local scheduling gap. They may lag behind for a
bounded period, but they still need a reliable way back to the current shared
filesystem state.

### 23. Symlink And Metadata-Attribute Convergence

This scenario checks filesystem details that are easy to forget when we only
look at regular files and directories. It creates normal target files, tries to
create a symlink to one of them, renames the symlink, and, on platforms where
the bridge exposes it, checks that the symlink target is still the same through
the public mounted filesystem.

It also checks a simple metadata attribute that users can observe without
looking inside StudioFS internals: the read-only flag on a file. One mounted
client marks a file as read-only, and the others must eventually see that same
public attribute through their own mounts.

Not every platform exposes every detail the same way. On Windows/WinFSP, for
example, symlink creation can be unavailable in the current bridge path. That
is not treated as a mysterious failure. The scenario records an explicit
platform skip for the unsupported symlink behavior, then still validates the
supported parts of the scenario. This keeps the test honest: we do not fake
features a bridge cannot expose, but we do require supported public filesystem
state to converge.

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
write-acceptance crash boundaries, focused namespace collisions, seeded chaos, hot-spot
churn, fault chaos, LWW reconnect, open-handle behavior, sparse files, late
observers, stale remounts, cold discovery, invalidation dropout/fallback sync,
symlink and metadata-attribute behavior, and any later appended promotion
scenarios selected for that run.

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

Future deterministic backstops can go deeper into sparse-file
truncate/write/read-range races, cold-observer long-history convergence,
stale-remount catch-up, and adversarial runner cleanup. Symlink and
metadata-attribute convergence now has a real mounted promotion scenario,
`mounted-symlink-attribute-convergence`, with platform-gated public filesystem
checks.
