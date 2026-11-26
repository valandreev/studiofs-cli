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
index, the `studiofs` version-4 dump). Each entry records the source commit or
release, the generator, the SHA-256, the format identity, and the expected
behavior. `tests/baseline_fixture_manifest.rs` fails when a file drifts or an
unlisted file appears; decode tests read each file with the current binary.

The fixtures are provisional until SFS-1020 seals them from the release
candidate. Regenerate only with the baseline encoder, never a successor build:

- Baseline: `STUDIOFS_REGENERATE_BASELINE_FIXTURES=1 cargo test -p <crate>
  <generator> -- --ignored`. Generators are `#[ignore]` and also skip without
  the variable, so normal and `--ignored` runs never rewrite fixtures. The
  pinned `studiofs` version 4 dump (`dump-v4-0.2.5.json`) has no generator; it
  stays only as the input that `load` must refuse.
- Published v0.2.2 / v0.2.4 volumes: `pwsh
  tools/fixtures/capture-published-volume-fixtures.ps1`. It downloads and
  verifies the release binary, runs the opt-in `studiofs-e2e` scenario
  `published-volume-fixture-capture` with `--infra compose` (the runner owns
  PostgreSQL and RustFS and their fixed test-only credentials), and copies the
  run artifacts here with LF line endings. Needs `gh`, Docker and network. No
  tool or test starts its own Docker stack.

Then copy the new digests from the drift test output into the manifest and
append the reason to `regenerations`. All keys are throwaway test values;
see `test_only_secrets` in the manifest.
