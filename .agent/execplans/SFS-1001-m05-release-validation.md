# Milestone 05: Forward Compatibility Release Validation

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan is maintained in accordance with `.agent/PLANS.md` at the repository root. It depends on milestones 01–04 (`.agent/execplans/SFS-1001-m01-contracts-baseline.md` through `.agent/execplans/SFS-1001-m04-write-safety-activation.md`).

## Purpose / Big Picture

After this milestone the 0.2.5 release candidate has evidence, not only design, that it behaves as the support matrix in `docs/persisted-format-compatibility.md` says when it shares a volume with a newer release. A compiled test successor activates a new capability on a live volume while 0.2.5 clients are mounted, crashes are injected at every step, and rollback is exercised within its documented limits. Release documentation states the guarantees and their limits.

## Progress

- [ ] SFS-1019: supported-release forward-compatibility integration and mounted E2E matrix.
- [ ] SFS-1020: validate crash and rollback safety and establish the 0.2.5 forward-compatibility baseline in release documentation.

## Surprises & Discoveries

None yet.

## Decision Log

- Decision: the successor in this matrix is the compiled test successor defined by SFS-1131, not a released 0.2.7.
  Rationale: 0.2.7 does not exist yet; the test successor exercises the same fencing paths and is excluded from shipped binaries by the SFS-1131 release guard.
  Date/Author: 2026-11-25, SFS-1001 audit.

## Outcomes & Retrospective

Not started.

## Context and Orientation

Mounted end-to-end scenarios run through the registered `studiofs-e2e` runner described in `docs/e2e-runner-contract.md`; scenarios use bounded polling of observable state and retain diagnostics, never fixed sleeps. After any product source change, the binary must be rebuilt before a mounted run counts. The support matrix rows to cover are: 0.2.5 with 0.2.5; successor state with a 0.2.5 binary before activation, after a read-compatible activation, and after a read-incompatible activation; 0.2.5 state with the successor; unknown future state; and pre-0.2.5 state, which must be adopted through `studiofs upgrade`. The columns are read, write and publish, maintenance, snapshot, dump and load, offline client with pending local writes, and rollback.

## Plan of Work

SFS-1019 builds one scenario per matrix row on Linux FUSE and WinFSP (and macFUSE when available), asserting the typed outcome named in each cell and that unpublished local work survives every refusal. SFS-1020 adds crash injection at Expand, Migrate, Activate and Contract, a rollback before Contract, a refused rollback after Contract, and the format 5 and 6 upgrade chain under crash; it then writes the release notes section that states the baseline guarantee.

## Concrete Steps

From the repository root, after building both binaries:

    studiofs-e2e run --scenario <forward-compat scenario> --studiofs-bin <rebuilt-binary> --infra compose --retain always

Scenario names are defined by SFS-1019; record them here and in `docs/local-first-validation-matrix.md` when they exist.

## Validation and Acceptance

Every row of the support matrix has a passing scenario or a documented environment gate. Retained reports show typed refusals and unchanged pending local data. The rows under "Planned: forward-compatibility gate" in `docs/local-first-validation-matrix.md` name real evidence.

## Idempotence and Recovery

Scenarios create fresh volumes and retain artifacts under `target/e2e-runs/`. Re-running a scenario does not depend on earlier runs.

## Artifacts and Notes

Issues: https://linear.app/valandreev/issue/SFS-1019, https://linear.app/valandreev/issue/SFS-1020.

## Interfaces and Dependencies

Uses the test-successor build and manifest from SFS-1131 and the fixtures from SFS-1002.
