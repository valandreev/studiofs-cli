# redb 4.3 compatibility fixtures

Both checked-in fixtures were produced with `redb = 4.3.0`:

- `redb-4.3-metadata.redb` starts from the valid 4.1 format-v5 fixture and
  records a committed `redb_compatibility_fixture_writer = 4.3.0` settings row.
- `../../../studiofs-cache/tests/fixtures/redb-4.3-cache-index.redb` was
  created with the raw v2 cache-index record and its payload, order, and
  accounting tables.

SHA-256:

- `redb-4.3-metadata.redb`: `9aab7b0324b8e77ecadd9b4a8b54017a300b141f89009ef21a9948c80c840bd9`
- `../../../studiofs-cache/tests/fixtures/redb-4.3-cache-index.redb`: `6883dc04e7f987279cb89f013c5178239b47a356b66e00e961cb953654aa8702`
