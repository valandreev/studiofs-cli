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
