# SFS-644 DataStore maintenance boundary

## Purpose

The VFS janitor must be able to scan and remove orphaned payload fragments without receiving the raw OpenDAL operator. After this change, storage-provider listing and deletion remain behind `studiofs-data`, invalid delete keys are rejected at that boundary, and WinFSP has no direct production dependency on `studiofs-data`.

## Design

`DataStore` will expose one narrow listing operation, `list_object_entries(prefix)`, that returns a stream of the data layer's own `DataStoreObjectEntry` values. Each value contains only a storage key and optional last-modified time; OpenDAL `Lister`, `Entry`, and provider errors do not cross the crate boundary. The data layer filters directory entries and maps provider failures into the existing `DataError` hierarchy.

The existing `delete_by_key` operation remains the single delete entry point and becomes scoped. It accepts only non-empty keys under the configured volume prefix and rejects path-like `.`/`..` segments and backslashes with a new non-retryable `DataError::InvalidObjectKey`. This preserves the existing raw-key test API while preventing callers from deleting outside the volume namespace.

`StudioVfs::background_janitor` will consume `list_object_entries` and call the validated `delete_by_key` method. The raw `DataStore::operator` method will be removed. Existing VFS tests will use `put_by_key` for setup; the encrypted-ciphertext test will construct its own test-only OpenDAL operator because inspecting/tampering ciphertext is intentionally below the DataStore encryption API.

`studiofs-data` will be moved from normal to dev dependencies in `studiofs-winfsp/Cargo.toml`. `studiofs-vfs` still requires it transitively at runtime, so validation will assert that the WinFSP package's direct edge is `dev`, not claim that the transitive package disappears from the full graph.

## Error and authorization semantics

- A provider/listing failure is returned as `DataError::OpenDal` and remains classified by OpenDAL `ErrorKind`.
- An invalid delete key is returned as `DataError::InvalidObjectKey` and is not retryable.
- A valid key under the configured volume is passed to OpenDAL unchanged; OpenDAL's idempotent delete behavior is preserved.
- The listing prefix is checked with the same volume-scope validation before any provider request.

## Verification

Unit tests in `crates/studiofs-data/src/store.rs` will prove that valid in-volume entries can be listed and that out-of-volume/path-like keys are rejected. Focused VFS tests and `cargo check`/Clippy will prove that no production caller uses `operator`. `cargo metadata --no-deps` will show `studiofs-winfsp` -> `studiofs-data` as a dev dependency. Full workspace tests remain the user's responsibility per repository instructions.
