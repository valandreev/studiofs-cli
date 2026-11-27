# redb compatibility fixtures

The `redb-4.1-*` fixtures here and in `studiofs-cache/tests/fixtures/` are the
original 4.1 baselines. The 4.3 fixtures were produced with `redb = 4.3.0`:

- `redb-4.3-metadata.redb` starts from the valid 4.1 format-v5 fixture and
  records a committed `redb_compatibility_fixture_writer = 4.3.0` settings row.
- `../../../studiofs-cache/tests/fixtures/redb-4.3-cache-index.redb` was
  created with the raw v2 cache-index record and its payload, order, and
  accounting tables, then compacted with redb 4.3.0 to 64 KiB.

These fixtures verify upgrades to the current redb version, including writes
and reopen. They do not establish downgrade compatibility. The metadata fixture
covers a database updated by 4.3.0, rather than initialized by it.

SHA-256:

- `redb-4.3-metadata.redb`: `9aab7b0324b8e77ecadd9b4a8b54017a300b141f89009ef21a9948c80c840bd9`
- `../../../studiofs-cache/tests/fixtures/redb-4.3-cache-index.redb`: `3b885fd940e31d35be531da4bdecb42c9055f2c14e8d47b28635453645b8c225`

# 0.2.5 baseline persisted-format fixtures (SFS-1002)

`baseline-0.2.5/manifest.json` lists every pinned 0.2.5 baseline fixture by
workspace-relative path: the files in `baseline-0.2.5/` and
`published-v0.2.2/` / `published-v0.2.4/` here, plus fixtures kept in their
owning crate (`studiofs-data` STFS objects, the `studiofs-cache` payload
index, the `studiofs` version-4 and version-5 dumps). Each entry records the source commit or
release, the generator, the SHA-256, the format identity, and the expected
behavior. `tests/baseline_fixture_manifest.rs` fails when a file drifts or an
unlisted file appears. Decode and restore checks use the current binary.

SFS-1020 sealed the candidate fixtures from the working tree based on
`7fe972358f9e48655249739ae7c8415bcac5504d` with uncommitted changes. The
manifest records the generator and checksum for each fixture and the
compiled baseline and successor binary hashes. Final release evidence comes
from the clean release candidate.

- Baseline generators require both `STUDIOFS_REGENERATE_BASELINE_FIXTURES=1`
  and `--ignored`. Use the exact commands in the fixture entries. The
  `FormatInfo` fixture uses shared format 7. The published format-5 and
  format-6 captures cover adoption.
- Dump v5 comes from the candidate's `studiofs dump` command after
  `studiofs init` on disposable local PostgreSQL and RustFS. The candidate's
  `studiofs load` restored the format-7 volume and its published checkpoint
  into a second disposable PostgreSQL. The v4 dump still covers load refusal.
  See the [SFS-1020 ExecPlan](../../../../docs/exec-plans/SFS-1020-crash-rollback-validation.md)
  for the commands.
- Test successor: `python3 tools/test_successor/build.py --require-complete`.
  The generated manifest records the source and binary digests. The successor
  event and checkpoint golden tests verify their bytes in source.
- Published v0.2.2 / v0.2.4 volumes: `pwsh
  tools/fixtures/capture-published-volume-fixtures.ps1`. It downloads and
  verifies the release binary, runs the opt-in `studiofs-e2e` scenario
  `published-volume-fixture-capture` with `--infra compose` (the runner owns
  PostgreSQL and RustFS and their fixed test-only credentials), and copies the
  run artifacts here with LF line endings. Needs `gh`, Docker and network. No
  tool or test starts its own Docker stack.

Update the digests and append the reason to `regenerations`. Run
`cargo test --locked -p studiofs-meta --test baseline_fixture_manifest`
and the fixture decode tests. All keys are throwaway test values. See
`test_only_secrets` in the manifest.
