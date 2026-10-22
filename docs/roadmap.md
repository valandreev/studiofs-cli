# StudioFS Roadmap

**0.2.0 is the first public release.** Its focus is stabilization: filesystem
correctness, consistency, recovery, regressions, and public distribution readiness.
New product features belong to later planning.

This is a roadmap, not a fixed implementation schedule. Workstreams may proceed
in parallel, move between releases, or be reordered freely as evidence and user
needs change. Suggested version groups are planning labels, not release
commitments or dependency order. Safety and compatibility gates still apply to
the change they protect. Linear holds current scope and status; this page is a
high-level map rather than a checklist of every issue.

## Existing foundation

StudioFS already has a local-first VFS, local `redb` metadata with PostgreSQL
bootstrap and replication, persistent payload caching and background upload,
Snapshot v2, remote encryption, platform bridges, runtime telemetry, and mounted
benchmark/E2E tooling. These are foundations to improve, not features to rebuild.
Frame-aligned encrypted cold reads are also implemented (SFS-787); further
optimization requires new measurements.

See the [architecture](architecture.md),
[mounted I/O contract](juicefs-local-first-rust-contract.md),
[encryption contract](encryption-at-rest-contract.md), and
[metadata and snapshots](metadata.md).

## 0.2.0 — first public release and stabilization

| Workstream | Focus | Canonical references and tracking |
| --- | --- | --- |
| Local-first correctness and recovery | Preserve same-mount visibility, local durability, background publication, and recovery under crash, disk-full, remote outages, and multi-client activity. | [I/O contract](juicefs-local-first-rust-contract.md), [validation matrix](local-first-validation-matrix.md), [PG sync contract](pg-sync-contract.md); established validation families SFS-270 and SFS-402. |
| Platform bridges | Stabilize Windows WinFSP, legacy macFUSE, and Linux libfuse3 using native mounted evidence and application workloads. FSKit remains experimental and deferred. | [Platform bridges](platform-bridges.md), [Linux parity ADR](adr/0004-linux-mount-targets-platform-bridge-parity.md), [Linux release gate](local-first-validation-matrix.md#sfs-610-linux-platform-release-gate); completed baselines SFS-598 and SFS-610. |
| Performance and responsiveness | Measure latency, throughput, CPU, RSS, cache behavior, and upload backlog on comparable hosts and real media workloads. | [Benchmark methodology](perf/benchmark-methodology.md), [benchmark validation](perf/benchmark-validation.md); baseline families SFS-301, SFS-495, and SFS-612. |
| Public distribution | Complete licensing, provenance, dependency notices, and native driver distribution checks before public release. | [Contributing](contributing.md); active release family SFS-848 through SFS-854. |

The validation families above include completed work. Their contracts and evidence
remain the stabilization baseline; linking them does not reopen those issues or
imply that every platform gate has been rerun for the current revision.

### Known limitations and validation gaps

- Local acceptance, local durability, and peer availability are distinct states.
  Snapshots alone are not a complete metadata-plus-payload backup strategy.
- Platform and application compatibility require native workload evidence. The
  Linux release policy prioritizes Ubuntu 24.04+ and Rocky Linux 9.4+; Rocky 8.6+
  is best-effort. Windows per-file ACL persistence/SetSecurity is unsupported;
  existing volume roles should not be mistaken for per-file Windows ACLs.
- Historical parity results do not guarantee current cross-platform performance.
  The documented SFS-612 metadata-create baseline is Windows-only and a manual
  sample; comparable Linux/macOS measurements remain a documentation evidence
  gap. The PG sync contract also retains high-RTT and sustained-write catch-up
  validation gates. Focused/unit success does not replace service or mounted runs.

## Migration and compatibility gates

- **Current metadata boundary:** the [PG sync contract](pg-sync-contract.md)
  (SFS-695 / SFS-696) establishes format v5.
  Legacy test-disk state is rejected; there is no automatic migration, dual-read,
  or dual-write path across that boundary. Follow documented recreate or discover
  guidance rather than assuming an in-place upgrade.
- **Post-public-release upgrades:** define a bounded client support window and
  test mixed-version operation before promising rolling upgrades. Evolve schemas
  with expand/migrate/contract changes and track schema, event, object-format,
  and capability compatibility explicitly. Versioned events
  (SFS-639) are an existing building
  block, not proof that arbitrary old clients can safely write new formats.
  Preserve formats understood by supported clients; incompatible mutations need
  explicit read-only or fail-closed behavior. New negotiation cannot retrofit
  capabilities into older binaries.
- **FSKit:** work is deferred because of the upstream macFUSE crash/handoff bug.
  The [bridge document](platform-bridges.md#macos-backend-selection) records the
  stale-mount/reboot limitation: crash/restart acceptance and full mounted parity
  require a clean-host rerun after a fix or upgrade. After that fix, revalidate
  mounted semantics and lifecycle
  (SFS-670), evaluate the planned zero-copy path (SFS-671), and run
  controlled benchmarks with an explicit GO/NO-GO decision
  (SFS-672). Default promotion and legacy removal (SFS-673 / SFS-674) remain
  gated follow-ups.
  Legacy macFUSE stays the default meanwhile.

## Proposed directions after 0.2.0

These groups summarize the product discussion in
SFS-508. They are proposals,
not approved feature scope. Their order and version assignment may change freely.
Detailed design, user validation, and acceptance criteria belong in dedicated
issues when a direction is selected.

### Suggested 0.3.x — user and operator experience

- A read-only localhost dashboard using existing
  [runtime telemetry](perf/studiofs-perf-command.md), plus StudioFS Doctor for
  actionable storage, network, driver, configuration, and recovery diagnostics.
- Clear sync/activity states across CLI, dashboard, and tray: local acceptance,
  local durability, upload, publication, catch-up, and degraded operation.
  Improve shutdown guidance around pending work while retaining crash recovery;
  extend the existing tray (SFS-86).
- Online cache resize that preserves dirty, pinned, and unpublished data and
  reports when the requested capacity cannot safely be reached.
- Friendly installers/updates, saved mounts, optional auto-mount, and client
  compatibility checks. Establish the cross-release upgrade policy above early.

### Suggested 0.4.x — creative workflows and cache management

- Resumable online cache relocation with safe cutover and recovery of upload
  backlog; user-directed warmup/pinning with visible capacity costs. Build on
  the [existing cache](cache.md).
- Project-aware prefetch through optional DCC helpers, using existing sequence
  prefetch (SFS-19); portable volume
  and path identities without promising automatic rewriting of application links.
- Snapshot browse/diff and individual file restore; lock-owner/status UX,
  read-only workspace profiles, and measured creative I/O presets. Extend existing
  snapshots, locking, and ACLs.
- An experimental LAN read-through S3 cache proxy for verified immutable objects.
  Validate range reads, authentication, encrypted object identity, cleanup, and
  failure behavior before adoption; keep writes and PostgreSQL independent.

### Suggested 0.5.x — studio operations and external access

- Browser access for external collaborators and evaluated WebDAV/SFTP gateways;
  optionally a separate S3-compatible API for logical files. Internal encrypted
  bucket fragments are not a public logical-file API.
- A reusable headless VFS entry point that preserves namespace and publication
  semantics across portals and gateways.
- An optional management hub, identity/device provisioning and revocation, and
  fleet health/version visibility. Keep direct self-hosted PostgreSQL + S3
  deployments viable; a hub must not become a foreground I/O dependency.
- Verified metadata-plus-payload backup/restore drills, storage health and
  provider-dependent cost visibility, and resumable S3 provider migration with
  integrity checks and explicit cutover/rollback gates.

### Suggested 0.6+ — performance and scale research

- Benchmark-driven WAN upload concurrency/batching, encrypted range-read
  improvements beyond the implemented baseline, and adaptive RAM/SSD placement.
- Small-object packing only if measurements justify read, encryption, metadata,
  and garbage-collection complexity. The canceled
  SFS-109 is not a commitment to
  revive chunk grouping.
- Multi-volume runtime sharing where deployments demonstrate a need; a native
  LAN peer cache only after the simpler proxy experiment establishes value.

### Long-term research

Offline project preparation and safe reconciliation, explicit recoverable
conflict versions, multi-region storage/failover, storage tiering, content
deduplication, and pipeline hooks/events remain exploratory. Define concurrent
writer, integrity, availability, encryption, and authorization limits before
committing to these features. FSKit stays deferred under the gate above.

## 1.0 readiness

Readiness depends on demonstrated data safety and recovery under crash, network
loss, disk pressure, and multi-client workloads; a tested upgrade/support policy;
verified PostgreSQL + S3 disaster recovery; current cross-platform performance
and application compatibility evidence; and clear operational/security guidance
backed by long-running production-like validation. It is a quality milestone,
not a bucket that commits every proposed feature to 1.0.
