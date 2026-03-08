# Release Readiness

Current status as of 2026-03-07: candidate is ready for external test pass.

## Gate Checklist

- PR-01 (Freeze Contract):
  - `docs/refactor/file-visibility-and-incarnation-model.md` is the canonical
    visibility contract.
  - `docs/architecture.md`, `docs/append-only-ingress-segments.md`, `docs/vfs.md`
    all align with the same local/remote semantics and terminology.
  - `vfs_test.rs` contains reserved gate test names for new contract edges.

- Build:
  - `cargo build --release` passes.
  - no known workspace warnings in the latest validated run.
- Core test surface:
  - full local/ignored suites pass in the latest run shared by the team.
  - previously hanging regression `test_crash_recovery_replays_from_segments_when_spill_files_are_missing` is fixed and stable.
  - rename regression `test_rename_to_self_is_noop` is fixed.
- Ingress architecture:
  - append-only ingress segments enabled (`ingress_spill/segments`).
  - periodic + threshold-based ingress checkpoints enabled.
  - recovery from segment records works even when legacy `.spill` artifacts are absent.
- Metadata multi-client behavior:
  - base + overlay model active for attrs/dentries/symlink/xattr.
  - foreign batch apply remains conflict-aware and non-blocking for later independent batches.
  - `sync_metadata()` follows flush-then-pull semantics.
- Operational safety:
  - upload/fetch adaptive schedulers active with bounded retry/timeout policies.
  - startup warmup is non-blocking (no `StudioVfs::new` deadlock on current-thread runtime).
  - infra test guidance includes proxy cleanup for MinIO/PG runs.
- Documentation:
  - architecture, metadata, VFS, configuration, tests, and dev docs updated to match current behavior.

## Pre-Release Smoke Commands

```powershell
Set-Location E:\dev\studiofs-cli
Get-Content .env | Where-Object { $_ -match '^\s*[^#].+=' } | ForEach-Object { $k,$v = $_.Split('=',2); Set-Item -Path "Env:$k" -Value $v }
Remove-Item Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:ALL_PROXY -ErrorAction SilentlyContinue
$env:NO_PROXY='localhost,127.0.0.1,::1'
cargo build --release
cargo test --workspace --all-targets -- --nocapture
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

## Known Operational Notes

- On Windows, stale `cargo`/`rustc` processes may hold `target\debug` artifacts and cause `Access is denied` during incremental rebuilds. If that happens, stop stale processes or use a fresh `--target-dir`.
- For LAN/VPN MinIO endpoints, process-level `HTTP_PROXY`/`HTTPS_PROXY` can cause false integration failures or apparent hangs; clear proxy env vars for infra tests.
