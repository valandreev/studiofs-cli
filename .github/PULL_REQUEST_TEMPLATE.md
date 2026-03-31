## Summary

<!-- What does this PR do? Why? Link related Linear issues (e.g. SFS-XXX). -->

## Changes

<!-- Bullet list of key changes. -->

## Testing

<!-- How was this tested? Which CI steps cover it? -->

---

## Checklist

- [ ] `cargo fmt --all` applied
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes
- [ ] `cargo test --workspace --all-targets -- --nocapture` passes
- [ ] New public items have `///` doc comments
- [ ] `docs/tests.md` regression list updated if new named regressions are added

### Platform-specific changes

<!-- Complete this section if the PR introduces or expands cfg(windows),
     cfg(target_os = "macos"), cfg(target_os = "linux"), or any other
     host-OS conditional. Delete if not applicable. -->

- [ ] I have explained below how the platform-specific path is verified

**Platform verification:** <!-- e.g. "CI covers this — the macOS job runs
studiofs-fuse --lib --tests which exercises this path." or "Validated locally
with STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse
--test mount_sanity -- --ignored." See docs/contributing.md#rules-for-platform-specific-code
for the full rules. -->
