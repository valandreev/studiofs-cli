# POSIX Conformance Baseline (Linux FUSE)

## Latest pjdfstest run

Run `20261030T184900Z-fec5f2b79` used product revision
`fec5f2b79d416cf354f133280cc62958ada93fff`. The release binary was rebuilt
with `cargo build -p studiofs --release` before testing.

| Target | Pass | Skip | Fail | Cases |
|---|---:|---:|---:|---:|
| ext4 control | 385 | 13 | 0 | 398 |
| StudioFS, per-group | 207 | 14 | 177 | 398 |
| StudioFS, per-case | 207 | 14 | 177 | 398 |

The ext4 skips are the 13 read-only-remount cases. Both StudioFS modes covered
the same 398 case names with identical verdicts. All 15 groups completed with
no degraded cases.

| Group | Pass | Skip | Fail |
|---|---:|---:|---:|
| `chmod` | 16 | 1 | 16 |
| `chown` | 16 | 2 | 8 |
| `ftruncate` | 6 | 0 | 0 |
| `link` | 15 | 2 | 24 |
| `mkdir` | 11 | 1 | 9 |
| `mkfifo` | 9 | 1 | 11 |
| `mknod` | 15 | 0 | 23 |
| `open` | 18 | 2 | 7 |
| `posix_fallocate` | 5 | 0 | 0 |
| `rename` | 27 | 1 | 32 |
| `rmdir` | 14 | 1 | 8 |
| `symlink` | 12 | 1 | 11 |
| `truncate` | 14 | 1 | 4 |
| `unlink` | 13 | 1 | 20 |
| `utimensat` | 16 | 0 | 4 |
| **All** | **207** | **14** | **177** |

These focused cases and their ext4 controls passed; each also passed in the
full per-case run:

- `rename::to_multiply_linked::regular`
- `rename::changed_ctime_success::symlink`
- `unlink::update_ctime_success::regular`

fsx was not rerun for that product revision. The separate non-WSL mmap/fsx
measurement below uses a newer revision and does not change these pjdfstest
counts.

## Environment and run configuration

| Item | Value |
|---|---|
| Host | WSL2, kernel `6.6.114.1-microsoft-standard-WSL2`, 24 vCPU, 46 GiB RAM |
| Distro | Ubuntu 24.04.5 LTS |
| FUSE | `/dev/fuse`; `fusermount3` and `libfuse3-3` 3.14.0; `user_allow_other` enabled |
| Toolchain | rustc 1.97.1, gcc 13.3.0 |
| Storage | Checkout, build, metadata, payload cache, mount, and ext4 control on WSL ext4 (`/dev/sdd`) |
| Services | PostgreSQL and S3 for the existing `test` volume |
| Mount | `studiofs mount test /srv/sfs906/mnt/test -d --allow-other`; adaptive cache policy |
| pjdfstest | Rust rewrite, revision `ce3ca5c10d1d29f25b2881c85c62be2c74c27bf8` |
| fsx | xfstests `ltp/fsx.c`, revision `22348afe338c0f6d540c0d7ef0db749eaa51b218` |

The mount ran as regular user `valen`, outside the home directory so secondary
pjdfstest accounts could traverse it. The ignored `.env` was loaded only in
the mount and per-case runner shells.

Rebuild before a repeat run. Start the mount in a shell with the required
environment loaded, then run the group suites and per-case runner using the
same 398-case list:

```bash
cargo build -p studiofs --release
target/release/studiofs mount test /srv/sfs906/mnt/test -d --allow-other

RUN=target/posix-conformance/20261030T184900Z-fec5f2b79
sudo tools/posix-conformance/run-pjdfstest.sh ext4 /srv/sfs906/ext4 "$PWD/$RUN"
sudo tools/posix-conformance/run-pjdfstest.sh studiofs /srv/sfs906/mnt/test "$PWD/$RUN"
# Per-case runner used "$RUN/cases.txt"; command-manifest.txt records its configuration.
```

## Artifacts

Evidence is under `target/posix-conformance/20261030T184900Z-fec5f2b79/`,
including the environment and command manifests, build log, case list, suite
logs, per-group and per-case results, classifications, mode comparison, and
daemon log.

## Non-WSL Linux mmap/fsx validation (SFS-924)

Run `20261030T202500Z-4c2ebc82b` on `devbench` uses product SHA
`4c2ebc82b423d5a4bc436f3d995fa4d8ead7f17e`. The unmodified product was rebuilt
with `cargo build -p studiofs --release -j 2` (11 min 40 s). Binary SHA-256:
`da687dde988fb98b43e3437ebfe3f7854204429a464a11933218f1f456c4620b`.
Compared with the SFS-917 WSL product SHA
`76a9fc05923673b86c7d049119345abed58d821f`, this includes the SFS-923
inode-ctime fix plus documentation and executable-mode updates. SFS-915,
SFS-916, SFS-921 and SFS-922 fixes are present in both measurements.

| Item | Non-WSL environment |
|---|---|
| Host | Ubuntu 26.04.1 LTS VM, kernel `7.0.0-38-generic` |
| Memory | 7.2 GiB RAM, 4 GiB swap; no swap use observed during preparation |
| FUSE | `/dev/fuse`, fusermount3 and libfuse 3.18.2 |
| Toolchain | rustc 1.99.1, gcc 15.2.0 |
| Storage | ext4 on `/dev/vda2`; build, client metadata/cache, control directory and mount under the checkout's `target/` |
| Services | PostgreSQL and S3 test services from the ignored `.env` |
| Volume | Fresh `sfs924-20261030-202500`, isolated `STUDIOFS_CLIENT_ROOT`; existing local `test` left intact |
| Mount | Regular user `val`, `default_permissions`, `max_read=4194304`, 16 callback workers; no `allow_other` needed |
| fsx | xfstests revision `22348afe338c0f6d540c0d7ef0db749eaa51b218`; same gcc flags/config macros as `build-tools.sh` |

The VM stayed available through the release build and measurements. This does
not establish the cause of the earlier VM stops. Discovery of `test` in the
configured PostgreSQL returned "not found or not initialized" before mounting;
the measurements therefore use a fresh, uniquely named test volume and client.

### Measured behavior

The shared-writable mmap probe uses a 12,411-byte file with a deterministic
byte pattern. It checks `MAP_SHARED`, synchronous `msync` (Python
`mmap.flush()`), `munmap`, reopen and exact byte/size equality. Additional
adaptive and kernel probes disable Python's duplicate-fd tracking and check
writing through a mapping after the original file descriptor is closed.

| Check | Non-WSL result |
|---|---|
| Adaptive, mmap opt-in off (`STUDIOFS_FUSE_DIRECT_IO_MMAP=0`) | Expected `ENODEV` |
| Adaptive, mmap opt-in on (`STUDIOFS_FUSE_DIRECT_IO_MMAP=1`) | mmap/msync/munmap/reopen and close-before-write probe pass |
| Adaptive + opt-in, fsx small | Pass: 100,000 operations, seed 906003, 302 s |
| Adaptive clean-remount size/SHA-256 | Both counted files match |
| Kernel, opt-in off | mmap/msync/munmap/reopen and close-before-write probe pass |
| Kernel, fsx small | Pass: 100,000 operations, seed 906003, 348 s |
| Kernel clean-remount size/SHA-256 | All five retained probe/fsx files match |
| ext4 control, fsx small | Pass: 100,000 operations, seed 906003, 55 s |

Each counted fsx profile runs once with exactly
`-l 65536 -o 4096 -N 100000 -S 906003`. The retained command manifest records
the paths and per-mount environment. The mounts use the rebuilt binary:

```bash
RUN="$PWD/target/posix-conformance/20261030T202500Z-4c2ebc82b"
export STUDIOFS_CLIENT_ROOT="$RUN/client"
VOLUME=sfs924-20261030-202500
MOUNT="$RUN/mnt/$VOLUME"
# After initializing the isolated volume with the test-service configuration:
STUDIOFS_LINUX_FUSE_CACHE_POLICY=adaptive STUDIOFS_FUSE_DIRECT_IO_MMAP=1 \
  target/release/studiofs mount "$VOLUME" "$MOUNT" -d
tools/posix-conformance/run-fsx.sh adaptive-on \
  "$MOUNT/sfs924-20261030T202500Z-4c2ebc82b/adaptive-on" "$RUN" small
target/release/studiofs umount "$MOUNT"
# Repeat mount/probe/fsx/remount with CACHE_POLICY=kernel and DIRECT_IO_MMAP=0.
```

No BAD DATA occurred. Final daemon-log scanning found zero matches for all
six checked failure families: retained-overlay relink, missing payload,
sticky upload errors, upload-stage linking, abandoned namespace batches and
namespace degradation. Cleanup logged 1,091 orphan-dirty-object warnings
(674 after adaptive, 417 after kernel); all checked data survived. One tray
D-Bus `NameLost` warning appeared after final shutdown. There were no daemon
ERROR lines. All task mounts and their processes were gone after cleanup;
the isolated volume/client state is retained for the SFS-926 follow-up.

### Comparison with WSL

The [SFS-917 WSL run](https://linear.app/valandreev/issue/SFS-917) used kernel
`6.6.114.1-microsoft-standard-WSL2`, libfuse 3.14.0 and the older product SHA
above. Both hosts reject shared writable mmap with adaptive opt-in off, and
both pass mmap with the opt-in enabled. WSL exercised the libfuse 3.13–3.16
INIT compatibility path; this VM exercises native libfuse 3.18 capability
negotiation. On the VM, the adaptive enabled feature mask changes from
`63497` to `268498953` when opt-in is enabled.

WSL passed ten consecutive small profiles under each enabled mmap policy;
this VM passed one under adaptive + opt-in and one under kernel, as required
by SFS-924. The VM adaptive/kernel durations (302/348 s) exceed the WSL
adaptive small profile's 159 s, but these hosts and toolchains differ and
this is not a controlled performance comparison. No WSL-only data or mmap
failure was observed in this scope. The VM's slow-remount launcher failure
below is separately tracked; it does not invalidate the observed data checks.
pjdfstest, large profiles, crash recovery, multi-client convergence and the
full registered E2E scenario gate were not rerun for SFS-924. These single
successful profiles do not prove the absence of rarer data races.

### Remount startup finding

After the adaptive fsx run, clean unmount completed, but the detached remount
launcher returned exit 1 after its 15-second publication deadline. Startup
cleanup removed 674 orphan dirty entries and 1,265 unreferenced payload
objects; the daemon attached and reached `live` about 17 seconds after launch.
The subsequent size/SHA-256 comparison passed. Kernel remount reproduced
the same launcher failure, with actual attachment about 25 seconds after
launch, then passed all five file comparisons. The retained launcher errors
are not counted as successful mount commands. Continuation waited on actual
FUSE attachment and the `live` record with a 120-second deadline, without
repeating the counted adaptive fsx run. This separate lifecycle defect is
[SFS-926](https://linear.app/valandreev/issue/SFS-926).

Raw evidence and temporary validation helpers remain under
`target/posix-conformance/20261030T202500Z-4c2ebc82b/`: `environment.txt`,
`command-manifest.txt`, build/init/launcher logs, `fsx-*/`,
`hashes-*-{before,after}.json`, `warning-scan.json`, validation/continuation
logs and `client/logs/`.
Raw artifacts and client credentials are not committed.
