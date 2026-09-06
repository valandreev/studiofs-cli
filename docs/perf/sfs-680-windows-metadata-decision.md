# SFS-680 Windows metadata-cache decision

Date: 2026-09-06
Decision: **NO-GO** for making the adaptive WinFSP metadata policy the Windows
production default.

## Decision basis

The declared SFS-675 metadata workload was measured on the same Windows host
against the pre-SFS-678 policy and the current candidate. The selected primary
metric is the median wall time for complete enumeration of 64 entries because it
is the largest metadata-heavy workload in the suite; all four workloads remain
mandatory acceptance signals.

The candidate did not improve the primary metric and materially regressed stat.
It also regressed negative lookup. Open/close improved, but one improved workload
does not offset regressions in the other mandatory workloads.

| Workload, 1000 operations | Baseline median | Candidate median | Change |
| --- | ---: | ---: | ---: |
| Stat existing empty file | 45.169 ms | 52.766 ms | +16.82% |
| Open/close existing empty file | 55.834 ms | 49.612 ms | -11.15% |
| Negative lookup, fixed absent name | 22.390 ms | 23.645 ms | +5.60% |
| Enumerate all 64 names (**primary**) | 323.890 ms | 350.939 ms | +8.35% |

The result is therefore **NO-GO**. Keep the current conservative Windows cache
policy for production. Do not remove the policy/timeout transition switches or
the conservative fallback based on this measurement. A later follow-up may
investigate the stat, negative-lookup and enumeration regressions and repeat the
same A/B method before reconsidering GO.

## Compared implementations and configuration

The baseline is commit `3e7cf5310226c5b53a6089144c78502262e9fe04`, which contains
the SFS-675 callback instrumentation and workload but predates SFS-678. The
candidate is `7cee40bcbf23b36754b66881746b88aefc35917c`, the current branch HEAD.

Both sides used the same release test, local redb/cache fixture, empty files,
1000 operations per workload, three passes per invocation and three independent
invocations. Both left `STUDIOFS_WINFSP_METADATA_CACHE_POLICY` and all individual
timeout overrides unset and loaded only the required opt-in and `Z:` mount point
from the repository `.env`.

| Setting | Baseline | Candidate |
| --- | --- | --- |
| FileInfoTimeout | `u32::MAX` | `u32::MAX` |
| Directory/volume metadata timeout | 1000 ms | 60000 ms |
| Kernel-mode opens | disabled | disabled |
| Cleanup purge | disabled | disabled |
| Effective policy | pre-SFS-678 default | adaptive default |

The exact measured command was:

    cargo test -p studiofs-winfsp --release --test mount_sanity metadata_baseline::winfsp_metadata_baseline -- --ignored --exact --nocapture --test-threads=1

The run used Windows `Microsoft Windows NT 10.0.26200.0`, 24 logical
processors, Rust `1.98.0 (88d9e12ae 2026-08-18)` and Cargo
`1.98.0 (797e8a9bc 2026-08-05)`. The host CPU model was unavailable through the
non-privileged system queries used by the harness; this is an identity gap, not
an inferred CPU value.

## Callback observations

Callback medians were identical for baseline and candidate. The values are
callback-entry observations, not VFS lookup counts. `get_file_info=0` means that
this workload was served through the observed security/name and open paths; it
is not a substituted counter. Close can cross workload boundaries, so raw CSVs
remain authoritative for each pass.

| Workload | get_security_by_name | open | get_file_info | read_directory | cleanup | close |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Stat | 1000 | 1000 | 0 | 0 | 0 | 1000 |
| Open/close | 1000 | 1000 | 0 | 0 | 0 | 1000 |
| Negative lookup | 1000 | 0 | 0 | 0 | 0 | 1 |
| Enumeration | 1000 | 1000 | 0 | 1000 | 0 | 999 |

Every invocation passed the file-attribute, NotFound and complete-enumeration
assertions and returned exit code 0. No run was skipped or partial.

## CPU and memory supporting evidence

The harness sampled the cargo/test process set while the measured command was
running, after a separate release `--no-run` build. These values are supporting
observations, not a profiler or a production RSS budget.

| Side | Median command wall time | Median process CPU | Median max working set |
| --- | ---: | ---: | ---: |
| Baseline | 2900.270 ms | 2062.500 ms | 98.230 MiB |
| Candidate | 2888.322 ms | 1875.000 ms | 93.777 MiB |

The first baseline process had a one-time 19.255 s mount/fixture startup outlier;
the median avoids treating that startup event as workload latency. CPU and memory
sampling still includes the integration process and cargo shim, so it is useful
only as same-run supporting context.

## Correctness gate and limitations

SFS-679 is the hard correctness prerequisite. Its Linear evidence records ten
sequential native WinFSP sweeps over scenarios 1-25: 250/250 scenario executions
passed, with preflight and cleanup passing and no retry or manual reset. The
separate full integration suite was not rerun here, in accordance with the
repository rule reserving broad suites for the operator.

This benchmark uses an isolated local redb/cache fixture. It does not establish
multi-client replication, remote publication, invalidation delivery under fault,
or the full mounted E2E acceptance gate. The same callback instrumentation and
workload definition were used on both sides, but the sample contains only three
invocations per side and antivirus/indexing/storage temperature were not
independently controlled. These limitations reinforce the NO-GO decision; they
are not grounds for claiming Windows production acceptance.

## Reproduction artifacts

The successful collection is retained at
`target/perf/sfs-680-windows-metadata-ab-20260906-171036/`:

- `environment.json` records commits, settings, host/build identity and workload
  definition.
- `command.txt` records the exact measured command.
- `results.json` records all six runs, callback medians and resource observations.
- `baseline-run-01.csv` through `baseline-run-03.csv` and the corresponding
  candidate CSVs retain every raw pass and callback delta.
- `*-build.stdout.log`, `*-build.stderr.log`, `*-run-*.stdout.log` and
  `*-run-*.stderr.log` retain build and native test output.

The repeatable harness is
`tools/perf/sfs-680-windows-metadata-ab.ps1`; it creates a tracked-source
baseline snapshot with `git archive`, builds both sides before timing, refuses an
assigned mount drive and restores temporary environment values on exit.
