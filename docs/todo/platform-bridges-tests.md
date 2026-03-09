# Task: Platform Bridges Test Coverage

The `studiofs-fuse` and `studiofs-winfsp` crates currently lack unit and integration tests. Since they act as thin translation layers between the platform VFS API (FUSE / WinFSP) and the internal `StudioVfs` structure, any error-mapping or mount-option logic is completely untested.

## Requirements

1. **Unit tests for option parsing:** Write unit tests to ensure that platform-specific mount options are properly translated or rejected.
2. **Unit tests for error mapping:** Create unit tests verifying that internal `VfsError` states translate to the exact correct POSIX/Windows error codes (e.g. `ENOENT`, `EACCES`).
3. **Integration sanity tests:** Use a mock `StudioVfs` backend or a dedicated tmpfs mount context to verify that the bridge successfully links against the system library and is capable of mounting and unmounting cleanly.
