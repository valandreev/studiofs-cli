# Task: Synchronize Test Names in `docs/tests.md`

The list of test names currently documented in `docs/tests.md` (such as `test_pg_...`) are hardcoded strings. If tests are renamed in the code, the documentation silently drifts and becomes stale.

## Requirements

1. **Audit existing tests:** Cross-check the test names listed in `docs/tests.md` with the macros and functions defined in `crates/studiofs-data` and `crates/studiofs-vfs`.
2. **Update the documentation:** Refresh `docs/tests.md` to accurately reflect the current test names.
3. **Add automated validation (optional):** Consider adding a simple CI script or `cargo xtask` step to assert that all names listed in the markdown file exist as substrings in the test tree.
