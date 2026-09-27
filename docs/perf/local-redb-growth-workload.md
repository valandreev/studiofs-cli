# Local redb growth workload

`local_metadata_churn_redb_growth_workload` measures local metadata churn in a fresh `RedbMetaEngine`. It exercises file and directory creation, attribute updates, same-directory and cross-directory renames, local/base inode and dentry rows, inode lifecycle and pending compaction rows, regular-file overlay/base chunk rows, xattrs, ACL rows, unlink, and directory removal. It does not mount StudioFS or connect to PostgreSQL or S3.

The ignored workload runs four deterministic cycles. Each cycle uses names derived from its cycle and slot indexes, removes every temporary file and directory, and keeps four fixed survivor files. Each checkpoint verifies the temporary directories are empty and temporary-file compaction markers are cleared. The output has one `SFS-733` line for the initial database, every cycle, and the optional post-compaction state. It includes visible entry counts, row counts for the tracked churn-related tables (including dentry reverse indexes), physical bytes, page and fragmentation statistics, the retained local event-log span, and compaction diagnostics. Tracked metadata rows exclude the event log; the tracked total includes it. These counts cover the displayed tables, not every table in redb. Tombstones and `DeletedBeforePublish` lifecycle rows remain visible when local uncommitted file state is retired; they are reported rather than expected to match the survivor count.

Run the small default profile from the repository root:

```powershell
cargo test -p studiofs-meta local_metadata_churn_redb_growth_workload -- --ignored --nocapture
```

The default processes 32 temporary files per cycle. To also measure explicit compaction, set `STUDIOFS_REDB_CHURN_COMPACT=1` before running the same command. Compaction is disabled when the variable is unset.

For a larger manual soak, set `STUDIOFS_REDB_CHURN_FILES=1024` to process 1,024 files per cycle. PowerShell example:

```powershell
$env:STUDIOFS_REDB_CHURN_FILES = "1024"
cargo test --release -p studiofs-meta local_metadata_churn_redb_growth_workload -- --ignored --nocapture
Remove-Item Env:STUDIOFS_REDB_CHURN_FILES
```

Use the release profile for compaction timing comparisons and larger soak runs; debug-build timings are not comparable to optimized runs. On Linux or macOS, use `STUDIOFS_REDB_CHURN_FILES=1024 cargo test --release -p studiofs-meta local_metadata_churn_redb_growth_workload -- --ignored --nocapture` from the repository root. Add `STUDIOFS_REDB_CHURN_COMPACT=1` to that command to include compaction.

Compare the reported values across runs and commits, but do not require identical physical bytes across filesystems or operating systems. Page statistics are best-effort and are printed as `None` when unavailable. The event log and metadata tombstones are reported as logical rows, separately from the visible namespace; their growth is evidence for retention work, not a platform-independent pass/fail threshold. This workload does not set a production compaction threshold.

## Recorded baseline

The Windows and Linux measurements collected for SFS-737 on 2026-09-27 are
recorded in [the cross-platform redb growth, recovery, and compaction
validation](sfs-737-redb-growth-recovery-compaction-platform-validation.md).
The separate [macOS redb growth and recovery baseline](sfs-737-macos-redb-growth-baseline.md)
is also retained for platform-specific details.
