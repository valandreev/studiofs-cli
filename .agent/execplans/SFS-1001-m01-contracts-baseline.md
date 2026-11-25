# Milestone 01: Contracts And Baseline For 0.2.5 Forward Compatibility

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan is maintained in accordance with `.agent/PLANS.md` at the repository root. It is the first of five milestone plans for the Linear project "[0.2.5] Metadata Compatibility Layer"; the others are `.agent/execplans/SFS-1001-m02-pg-compat-foundation.md`, `.agent/execplans/SFS-1001-m03-data-format-evolution.md`, `.agent/execplans/SFS-1001-m04-write-safety-activation.md`, and `.agent/execplans/SFS-1001-m05-release-validation.md`.

## Purpose / Big Picture

StudioFS stores bytes in many places that a later binary reads back: a local metadata database, a shared PostgreSQL database, encrypted objects in S3, a local payload cache index, snapshot artifacts and dump files. Today one number, `CURRENT_FORMAT_VERSION` in `crates/studiofs-meta/src/schema.rs`, gates most of them by exact equality. Any change, even a harmless new optional table, either stays invisible or locks out every older binary at once.

After this milestone, the repository has a written contract that splits that number into separate version identities, defines capabilities that a volume can require, and specifies a single evaluator that turns those inputs into a typed allow-or-refuse decision. It also has pinned baseline fixtures and a compiled "test successor" binary definition, so later milestones can prove that 0.2.5 refuses a newer volume cleanly instead of corrupting it. A reader can see the result by opening `docs/persisted-format-compatibility.md` and `docs/adr/0018-version-identities-and-capability-model.md`, and, once SFS-1004 lands, by running the evaluator's unit tests.

## Progress

- [x] (2026-11-25) SFS-1001: wrote the persisted-format audit `docs/persisted-format-compatibility.md`, ADR 0018, the forward-compatibility rows in `docs/local-first-validation-matrix.md`, and these five milestone plans.
- [x] (2026-11-25) SFS-1002: pinned the provisional 0.2.5 baseline fixtures (FormatInfo JSON, `VolumeSettings` with compression, serving checkpoint bytes, one event per tag, STFS v2 in 1 MiB SFEC frames, the reused STFS v3 fixture, payload index, snapshot artifact, clean and dirty local caches, version-4 dump) and captured format 5 (v0.2.2) and format 6 (v0.2.4) volumes from the published binaries. Manifest and drift test: `crates/studiofs-meta/tests/fixtures/baseline-0.2.5/manifest.json`, `crates/studiofs-meta/tests/baseline_fixture_manifest.rs`. Successor fixtures moved to SFS-1131 (scope decision of 2026-11-25). The published volumes are captured by the opt-in `studiofs-e2e` scenario `published-volume-fixture-capture` on the runner-owned Compose stack.
- [ ] SFS-1131: define the test-successor build, its manifest of declared capabilities, and the release guard that keeps it out of shipped artifacts.
- [x] (2026-11-25) SFS-1003: added `crates/studiofs-meta/src/version.rs` with typed identities (informative client release, shared volume format, local redb layout, event codec wire, serving checkpoint format and schema) and a pointer table to the identities owned by other crates; routed every existing strict check to the identity it guards without bumping any value; made the checkpoint schema a supported range; added `MetaError::UpgradeRequired` for unknown `FormatInfo` fields and enum variants, kept through the bootstrap descriptor (`PgSyncFailure::UpgradeRequired`); documented the version-bump rules in `docs/persisted-format-compatibility.md`.
- [x] (2026-11-25) SFS-1003 review: wired the existing PostgreSQL `payload_gc` DDL identity and the local redb header checks in volume settings, retained entries, and redb snapshots. Snapshot source errors now preserve the upgrade-required outcome. Three focused regressions failed on the original paths before the correction. The subsequent Windows metadata selection passed 48 tests, with two ignored.
- [x] (2026-11-25) SFS-1003 follow-up review: guard the shared/redb version pair at compile time until SFS-1012, document the coordination required for SFS-1132, and exercise the redb read, PostgreSQL sync gate, and serving checkpoint version checks directly. Cover unknown fields in `RemoteStoreConfig` as well. A temporary local-only bump failed at the assertion. A temporary checkpoint-format bump from 4 to 5 passed the version-gate regression without changing the volume or schema constants. Both temporary edits were reverted.
- [ ] SFS-1004: implement the central capability evaluator with deterministic decisions.

## Surprises & Discoveries

- Observation: an unknown `FormatInfo` field or backend variant is reported as the generic `MetaError::Serialization`, and the bootstrap path converts it to `InvalidCheckpointDescriptor`, so the operator sees "invalid checkpoint" instead of "upgrade required".
  Evidence: `crates/studiofs-meta/src/lib.rs:103` and `crates/studiofs-meta/src/engine.rs:162`.
- Observation: the serving checkpoint stored in `metadata_state` has no format column; only snapshot source checkpoints record `checkpoint_format_version`.
  Evidence: `crates/studiofs-meta/src/pg_engine/schema.rs:327` versus `crates/studiofs-meta/src/engine.rs:141`.
- Observation: a v0.2.4 `init` publishes the serving checkpoint at version 0 with default settings and the volume's compression policy as event batch 1 (tag 21); a v0.2.2 `init` writes no event rows.
  Evidence: `crates/studiofs-meta/tests/fixtures/published-v0.2.4/event-log.json` and `metadata-state.json`.
- Observation: an active lease cannot be pinned as offline bytes; lease liveness is a PostgreSQL row evaluated against the server clock. The manifest records it under `not_representable`.
  Evidence: `crates/studiofs-meta/src/pg_engine/sql.rs:158`.

- Observation: the local redb header has no version field of its own; it is the `FormatInfo` JSON in `SETTINGS_TABLE["format"]`, so the local layout and shared format identities read the same field until SFS-1012 adds a local marker.
  Evidence: `crates/studiofs-meta/src/redb_engine/format.rs:70`.
- Observation: `EncryptionConfig` also uses `deny_unknown_fields`, so an unknown field there is reported as `UpgradeRequired` like the other nested configs.
  Evidence: `format_info_decode_reports_unknown_field_as_upgrade_required`.

## Decision Log

- Decision: keep `CURRENT_FORMAT_VERSION` as the value `FormatInfo` writers stamp, defined as `SHARED_VOLUME_FORMAT.current`, and route checks through `version::SHARED_VOLUME_FORMAT` or `version::LOCAL_METADATA_LAYOUT`.
  Rationale: about sixty call sites only construct `FormatInfo` (the shared format); renaming them adds churn without changing behavior. Every check now names its own identity.
  Date/Author: 2026-11-25, SFS-1003 implementer.
- Decision: the local redb header stays the `FormatInfo` copy; local opens judge it by `LOCAL_METADATA_LAYOUT`, shared reads by `SHARED_VOLUME_FORMAT`. A dedicated local marker is left to SFS-1012.
  Rationale: adding a new redb field would change the persisted layout, which this issue must not do.
  Date/Author: 2026-11-25, SFS-1003 implementer.
- Decision: `UpgradeRequired` is derived from Serde's unknown-field and unknown-variant data diagnostics. Only short ASCII identifiers can be rendered as names. Other JSON errors report the input position.
  Rationale: `serde_json` exposes both unknown-name errors under the same data category. Focused regression tests exercise that diagnostic boundary and the redaction rules.
  Date/Author: 2026-11-25, SFS-1003 implementer.

- Decision: the audit and ADR are the single source of truth; milestone plans reference them instead of copying the tables.
  Rationale: the tables cite exact source lines and will be updated as gaps close; duplicated copies would drift.
  Date/Author: 2026-11-25, SFS-1001 implementer.
- Decision: format 7 is the last bump of the shared format number; later changes move separate identities.
  Rationale: preflight decision of 2026-11-25; 0.2.x volumes join the baseline through `studiofs upgrade` chaining to format 7 (SFS-1132).
  Date/Author: 2026-11-25, project preflight.
- Decision: SFS-1002 pins baseline fixtures only; successor fixtures come from the SFS-1131 test-successor build. One JSON manifest next to the studiofs-meta fixtures lists every pinned file by workspace-relative path, so fixtures stay in their owning crate and the existing STFS v3 fixture is reused.
  Rationale: Linear scope decision of 2026-11-25; extends the existing `include_bytes!` fixture convention instead of adding a parallel mechanism.
  Date/Author: 2026-11-25, SFS-1002 implementer.
- Decision: published v0.2.2/v0.2.4 volumes are captured by the opt-in `studiofs-e2e` scenario `published-volume-fixture-capture` with `--infra compose`, not by a private Docker stack in the PowerShell script, and not against the shared `.env` services.
  Rationale: the project does not allow private Docker stacks started by tools or tests (precedent: `.agent/execplans/janitor-docker-tests-to-env-and-e2e.md`). `FormatInfo` embeds the S3 keys and the PostgreSQL DSN, so capturing against `.env` services would commit real dev credentials; the Compose runner uses fixed test-only credentials, so the captured bytes stay raw product output. The script only downloads and verifies the binary, runs the scenario and copies artifacts.
  Date/Author: 2026-11-25, user decision, implemented by SFS-1002 implementer.

## Outcomes & Retrospective

SFS-1001 is complete: the audit lists twelve persisted boundaries, the existing mechanisms that must be extended, the baseline obligations with their current state in code, and the support matrix. The remaining issues in this milestone turn the contract into fixtures and code.

## Context and Orientation

A "persisted boundary" is any place where bytes outlive the process that wrote them. The audit `docs/persisted-format-compatibility.md` lists all of them with source references; read its "Boundary audit" and "Baseline obligations" sections before starting any issue here.

A "version identity" is the version number of one boundary only, for example the event codec or the payload index. A "capability" is a named, versioned feature that a volume can require for reading, writing or maintenance (deleting data). The "evaluator" is one pure function that takes the binary's declared capabilities, the volume's policy record, the shared schema components and the local identities, and returns, per operation, `Allowed`, `UpgradeRequired`, `MaintenanceForbidden` or `LocalUpgradeRequired`. All of this is defined in ADR 0018 (`docs/adr/0018-version-identities-and-capability-model.md`).

The "test successor" is a build of the same source with an extra declared capability, used only by tests, that plays the role of a newer release such as 0.2.7. Its manifest and the release guard are SFS-1131.

Key code: `crates/studiofs-meta/src/schema.rs` (`FormatInfo`, `CURRENT_FORMAT_VERSION`), `crates/studiofs-meta/src/lib.rs` (`decode_format_info`, `ensure_supported_format_version`), `crates/studiofs-meta/src/engine.rs` (`MetaError`, bootstrap descriptor), and the existing baseline fixture `crates/studiofs-data/tests/fixtures/stfs-v3-mixed.bin`.

## Plan of Work

First, SFS-1002 stores fixture files under each crate's `tests/fixtures/` directory, following `stfs-v3-mixed.bin`, and adds decode tests that read them byte for byte. Second, SFS-1131 adds the test-successor manifest and a guard that fails the release build if the successor capability is compiled into a shipped binary. Third, SFS-1003 introduces the identity types and a new `MetaError::UpgradeRequired` variant with a reason, mapped in `decode_format_info` before the generic serialization error. Fourth, SFS-1004 adds the evaluator as a pure module in `studiofs-meta` with table-driven tests, without wiring it into mounts yet (wiring is milestone 04).

## Concrete Steps

From the repository root:

    cargo test -p studiofs-meta --lib format_info
    cargo test -p studiofs-data --lib stfs_v3
    cargo clippy -p studiofs-meta --all-targets -- -D warnings
    git diff --check

Each command must report at least one test run; `running 0 tests` is invalid evidence.

## Validation and Acceptance

Acceptance is behavioral: feeding a `FormatInfo` JSON with an extra field to `decode_format_info` returns `UpgradeRequired` naming the field; the evaluator returns `UpgradeRequired` for a volume requiring a capability the binary does not declare and never returns `Allowed` for an unknown name; every pinned fixture decodes with the current binary. Rows in `docs/local-first-validation-matrix.md` under "Planned: forward-compatibility gate" are updated with the real test names when they exist.

## Idempotence and Recovery

This milestone changes no persisted state. Fixture tests are read-only. Re-running any step is safe.

## Artifacts and Notes

Issues: https://linear.app/valandreev/issue/SFS-1001, https://linear.app/valandreev/issue/SFS-1002, https://linear.app/valandreev/issue/SFS-1003, https://linear.app/valandreev/issue/SFS-1004, https://linear.app/valandreev/issue/SFS-1131.

## Interfaces and Dependencies

The evaluator must not depend on PostgreSQL or redb types; it takes plain data so the 0.2.7 Redis-compatible adapter can reuse it. `MetaError::UpgradeRequired` must render without secrets, following the redaction rules in `AGENTS.md`.
