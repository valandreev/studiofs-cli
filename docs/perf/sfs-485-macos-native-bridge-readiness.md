# SFS-485 macOS Native Bridge Readiness

SFS-485 tracks macOS native bridge readiness and default rollout. The current
decision is pending: Do not make the native bridge default before this issue
reaches a decision.

The macOS bridge remains an explicit development opt-in:

```bash
STUDIOFS_MOUNT_BACKEND=macfuse studiofs mount myvolume /mnt/studio
```

## Decision Gate

The issue must record one clear outcome before docs or CLI help describe the
native bridge as default:

- `default`: macFUSE becomes the normal macOS mount path.
- `opt-in`: macFUSE remains behind `STUDIOFS_MOUNT_BACKEND=macfuse`.
- `reject`: macFUSE is rejected for this rollout in favor of another strategy,
  such as a StudioFS userspace hot read cache.

Throughput alone is not enough for `default`. The decision needs correctness,
callback count, callback bytes, VFS read count, S3 GET count, local payload read
bytes, kernel-cache interpretation, and throughput evidence.

## Focused Matrix

Run the macOS native bridge matrix after read, write, compatibility, and
cache-policy work is complete:

- read/write/truncate/xattr/Finder-focused scenarios;
- repeated-read performance compared against the current fuser baseline;
- callback count and throughput evidence from the same mounted-performance
  probe shape;
- Linux FUSE and Windows WinFSP focused validations show no regression;
- docs/configuration.md and docs/platform-bridges.md match the recorded
  decision.

The repeated-read comparison must not reuse completed async-FUSE experiment
issues as proof for this bridge. SFS-474 is useful only as the current fuser
baseline context.

## Operator Commands

Focused commands that the operator should run, but the agent should not run
locally as a full-suite substitute:

```bash
cargo test -p studiofs --test architecture_vocabulary_test macos_native_bridge_readiness_report_covers_sfs485_decision_gate -- --nocapture
cargo test -p studiofs-macfuse -- --nocapture
STUDIOFS_MOUNT_BACKEND=macfuse cargo test -p studiofs-macfuse --test mount_sanity macos_native_mounted_smoke -- --ignored --exact --nocapture
studiofs test-cache --mounted --profile mounted --output target/sfs-485/macfuse-report.json
studiofs test-cache --mounted --profile mounted --output target/sfs-485/fuser-baseline-report.json
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_default_sync -- --ignored --exact --nocapture
cargo test -p studiofs-winfsp --test mount_sanity winfsp_mount_fio_multi_job_smoke -- --ignored --exact --nocapture
```

The full workspace suite remains an operator choice outside the local agent
loop for SFS-485.
