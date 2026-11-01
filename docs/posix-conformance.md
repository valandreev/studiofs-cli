# POSIX Conformance Baseline (Linux FUSE)

## xfstests VM baseline (SFS-907, generic/001–403)

The user capped this VM measurement at generic/403 inclusive. All 403 IDs are
accounted for; generic/404–805 (402 IDs) were not attempted. No product code was
changed. WSL2 coverage is separate in [SFS-935](https://linear.app/valandreev/issue/SFS-935).

| Canonical records | Budget per check | Pass | Fail | Notrun | Timeout | IDs |
|---|---:|---:|---:|---:|---:|---:|
| `generic/`, 001–013 | 60 s | 4 | 3 | 5 | 1 | 13 |
| `generic-fresh-014/`, 014–322 | 300 s | 43 | 29 | 236 | 1 | 309 |
| `generic-capped-323-403-v2/`, 323–403 | 300 s | 11 | 8 | 62 | 0 | 81 |
| **Total** | | **58** | **40** | **303** | **2** | **403** |

Of the 40 fail verdicts, 10 are product-bug cases, 27 are unsupported feature
or selected-profile cases, and 3 are harness mount-option limits. Both timeouts
are classified as environment/harness budget limits. No case is assigned to
the kernel-cache/TTL-only bucket without a controlled TTL comparison. A native
notrun is a skipped feature/device/tool probe, not a successful data-path test.
There are **zero manual test-ID exclusions**: expected limitations stayed
selected and are classified below. These counts include the SFS-937 contract
clarification for automatic access-time updates; the recorded verdicts are unchanged.

The measured window was **22:20:31–23:42:40 UTC on 2026-10-30**, wall time
**1 h 22 min 9 s**, including recovery, pauses and supplemental attempts. The
canonical per-case elapsed times sum to **1 h 10 min 29 s**. Release build took
174 s before measurement. Long successful cases include /069 (146 s), /113
(181 s), /208 (224 s), and the canonical /323 (124 s).

### Environment and provenance

| Item | Counted configuration |
|---|---|
| Host | `devbench`, Ubuntu 26.04.1 LTS VM, kernel `7.0.0-38-generic` |
| FUSE | libfuse / fusermount3 3.18.2 |
| Product build | `c6435f0711a8846f0221da1b5120a1f2177dca0d`, `cargo build -p studiofs --release -j 2` |
| Binary SHA-256 | `4a173bae3280ede96aafba3534122a16d9ed8b85f2d70fec80c20a8c932a40e1`, unchanged across chunks |
| xfstests | `22348afe338c0f6d540c0d7ef0db749eaa51b218` plus the narrow [FUSE subtype patch](../tools/xfstests/fuse-subtype.patch) |
| Storage | Checkout, local redb/cache, artifacts and temporary mounts on ext4 (`/dev/vda2`) |
| Mount | Root caller, `--foreground --allow-other=true`; `FSTYP=fuse`, `FUSE_SUBTYP=.studiofs`, requested `-o rw,allow_other,default_permissions` |
| Profile | `STUDIOFS_LINUX_FUSE_CACHE_POLICY=adaptive`, `STUDIOFS_FUSE_DIRECT_IO_MMAP=0`; nominal attr/entry TTL 1 s, 16 callback workers |
| Volumes | Initial `sfs907-20261030-221000-{test,scratch}`; continued on fresh `sfs907-20261030-222900-{test,scratch}` |
| Mountpoints | `/srv/sfs907/20261030T221000Z/mnt/<volume>`; test users could traverse the ancestors |
| Client/artifacts | `target/xfstests/20261030T221000Z-c6435f071-vm/`; private client state under `client/` |

The later chunk's original `metadata.tsv` key `product_sha` sampled checkout
HEAD `25495a9f`, after the harness commit. It is not the binary's build SHA;
the unchanged binary hash above identifies the c6435f071 build. The runner now
labels that field `checkout_sha`. Private credentials and mount diagnostic
tokens remain in ignored client state and are not included in this report.

### Setup, smoke and recovery

The reproducible setup, dependencies, root accounts, dedicated volume init,
config and runner commands are in [tools/xfstests/README.md](../tools/xfstests/README.md).
The helper accepts only the configured volume/mountpoint pairs. Mountpoint
basenames equal volume names. It supplies the PATH stripped by util-linux,
waits on actual FUSE attachment and usable root access, and waits for the old
daemon to release local state before a rapid remount. Ordinary unmounts release
the foreground daemon. Failed readiness and timeout cleanup are bounded and
restricted to the recorded task mount/process identities.

The pinned upstream FUSE `_scratch_mkfs` already mounts, removes scratch files
and unmounts; there is no fake mkfs or remote metadata/payload deletion. Sized
formatting and real-device requirements generally self-skip. The local patch
only normalizes `fuse.studiofs` to `fuse` for upstream filesystem-type checks.
Earlier preflight failures produced zero testcases and are not counted above.
The corrected `/srv` smoke ran /001, /002 and /005: two pass, /001 fails
([SFS-927](https://linear.app/valandreev/issue/SFS-927)); the all-pass smoke
criterion is therefore not claimed.

The initial /013 fsstress workload was active when its 60-second check budget
expired. Recovery required root `umount -l` after fusermount3 failed to detach
the stale connection. The old TEST volume then remounted `degraded` and root
creates returned EACCES; /014–016 setup timeouts on it are supplemental, not
executed testcase results. This retained state is investigated in
[SFS-929](https://linear.app/valandreev/issue/SFS-929), without asserting a
deadlock or its cause. Fresh volumes reached `live` and allowed continuation.
The /127 FSX timeout used the later 300-second budget and recovery continued.

When the user requested /403 as the limit, the uncapped parent was frozen and
its active /323 drained. That unmanifested pass is supplemental; /323 was run
again in the verified 81-ID capped chunk and its recorded result is canonical.
One erroneous empty selector restarted /001–003 before it was stopped; those
repeats are also supplemental. The runner now rejects missing selectors;
the whole group requires the explicit literal `generic/*`.

After the /403 row, the active wrapper emitted a post-loop Bash error while
its source file had been edited during execution. The exact output is retained
in `bounded-run-postloop-error.txt`; wrapper exit status was not captured.
All 403 canonical verdicts were independently checked against their matching
xunit testcases, with timeouts retained separately. Current `bash -n` and 20
focused harness/upstream tests pass. No mounted case was rerun after /403.

Focused verification commands (no mounted cases):

```bash
TMPDIR="$PWD/target/xfstests/tmp" python3 -B -m unittest discover -s tools/xfstests -p 'test_*.py'
bash -n tools/xfstests/run.sh
git diff --check
```

### Failure classification

The original SFS-906 expected-unsupported table is retained in Git commit
`7c1aed04`: special files, POSIX/NFSv4 ACLs, file flags, setgid-directory
inheritance and rename flags were not offered; hard links and ordinary xattrs
were supported. Linux's pre-epoch timestamp clamp is explicit in
`system_time_parts` in the bridge source. Shared mmap of direct-I/O handles is
disabled by the selected profile, consistent with the
[Linux FUSE I/O-mode contract](https://www.kernel.org/doc/html/latest/filesystems/fuse/fuse-io.html).
Unavailable helper options are rejected, not silently discarded.

Every fail and timeout has one of the four classification buckets below.
ACL conformance remains unsupported. In the recorded run ACL xattrs were
accepted but not applied; since SFS-934 the Linux bridge rejects
`system.posix_acl_access`/`system.posix_acl_default` with `EOPNOTSUPP` and hides
them from `listxattr`, as on a filesystem mounted without ACL support. In /193, host uutils 0.10.0
also changes EPERM wording, reproduced on ext4; the privilege-bit mismatch was
separately confirmed and is the product finding. Exact operation controls for
/001, /011, /035, /089, /093, /192, /193, /309, /313 and /355 are retained; body
replays are controls, not additional full-suite runs. Responsible layers and
TTL causes remain unassigned unless established by the specific evidence.

| Test | Bucket | Observed limit / failure | Follow-up |
|---|---|---|---|
| generic/001 | bug | copy-chain source names remain visible after rename/unlink; ext4 replay passes | [SFS-927](https://linear.app/valandreev/issue/SFS-927) |
| generic/003 | environment/harness | nodiratime/noatime/strictatime are unavailable through helper | — |
| generic/011 | bug | final dirstress cleanup leaves nonempty directories; ext4 replay passes | [SFS-928](https://linear.app/valandreev/issue/SFS-928) |
| generic/013 | environment/harness | fsstress exceeded initial 60-second budget; no deadlock inferred | — |
| generic/020 | bug | xattrs remain listed after successful removals | [SFS-931](https://linear.app/valandreev/issue/SFS-931) |
| generic/029 | unsupported by design | selected profile disables direct-I/O mmap; mmap returns ENODEV | — |
| generic/030 | unsupported by design | selected profile disables direct-I/O mmap; mmap returns ENODEV | — |
| generic/035 | bug | fstat on open displaced directory returns ENOENT after replacement rename; ext4 helper control passes | [SFS-930](https://linear.app/valandreev/issue/SFS-930) |
| generic/074 | unsupported by design | selected profile disables direct-I/O mmap; mmap returns ENODEV | — |
| generic/075 | unsupported by design | selected profile disables direct-I/O mmap; mmap returns ENODEV | — |
| generic/080 | unsupported by design | selected profile disables direct-I/O mmap; mmap returns ENODEV | — |
| generic/089 | bug | concurrent rename and lock-file namespace operations fail; ext4 replay passes | [SFS-932](https://linear.app/valandreev/issue/SFS-932) |
| generic/091 | unsupported by design | selected profile disables direct-I/O mmap; mmap returns ENODEV | — |
| generic/093 | bug | security.capability remains after append; ext4 control clears it | [SFS-933](https://linear.app/valandreev/issue/SFS-933) |
| generic/099 | unsupported by design | POSIX ACL policy is not offered; accepted ACL xattrs are not applied to permissions | [SFS-934](https://linear.app/valandreev/issue/SFS-934) |
| generic/112 | unsupported by design | selected profile disables direct-I/O mmap; mmap returns ENODEV | — |
| generic/120 | environment/harness | nodiratime/noatime/strictatime are unavailable through helper | — |
| generic/127 | environment/harness | fsx workload exceeded 300-second limit; timeout-truncated output does not establish a product bug | — |
| generic/128 | environment/harness | test adds -o nosuid, which helper rejects before scratch mount | — |
| generic/141 | unsupported by design | selected profile disables direct-I/O mmap; mmap returns ENODEV | — |
| generic/192 | unsupported by design | atime unchanged after five-second delay and read; ext4 control advances by five seconds; SFS-937 defines [automatic read-time atime updates as unsupported](platform-bridges.md#linux-access-time), without a mounted rerun | [SFS-937](https://linear.app/valandreev/issue/SFS-937) |
| generic/193 | bug | generic/193 owner truncation leaves setuid/setgid bits set; ext4 control changes mode 06770 to 0770; uutils error wording also appears on ext4 | [SFS-936](https://linear.app/valandreev/issue/SFS-936) |
| generic/198 | unsupported by design | selected profile disables direct-I/O mmap; mmap returns ENODEV | — |
| generic/215 | unsupported by design | mapped write did not run because mmap returned ENODEV under selected profile | — |
| generic/246 | unsupported by design | mmap returns ENODEV; mmap-created output is absent under selected profile | — |
| generic/247 | unsupported by design | selected profile disables direct-I/O mmap; mmap returns ENODEV | — |
| generic/248 | unsupported by design | selected profile disables direct-I/O mmap; mmap returns ENODEV | — |
| generic/258 | unsupported by design | Linux FUSE system_time_parts explicitly clamps pre-epoch timestamps to zero | — |
| generic/263 | unsupported by design | fsx MAPWRITE fails with mmap ENODEV under selected profile; no data-integrity verdict | — |
| generic/307 | unsupported by design | setfacl/ACL semantics are not offered; only ctime after setfacl is checked | [SFS-934](https://linear.app/valandreev/issue/SFS-934) |
| generic/309 | bug | destination directory mtime/ctime stay unchanged after cross-parent replacement rename; exact ext4 control advances both | [SFS-938](https://linear.app/valandreev/issue/SFS-938) |
| generic/313 | bug | mtime unchanged after all four actual-size pathname/handle truncate operations; ctime advances; exact ext4 helper passes | [SFS-939](https://linear.app/valandreev/issue/SFS-939) |
| generic/314 | unsupported by design | setgid directory inheritance is listed as unsupported in the SFS-906 expected-unsupported table | — |
| generic/319 | unsupported by design | POSIX ACL default/inheritance semantics are not offered; accepted ACL xattrs are not applied | [SFS-934](https://linear.app/valandreev/issue/SFS-934) |
| generic/340 | unsupported by design | mmap zero-fill workload stops at mmap ENODEV under selected profile | — |
| generic/344 | unsupported by design | mmap zero-fill workload stops at mmap ENODEV under selected profile | — |
| generic/345 | unsupported by design | mmap zero-fill workload stops at mmap ENODEV under selected profile | — |
| generic/346 | unsupported by design | mmap zero-fill workload stops at mmap ENODEV under selected profile | — |
| generic/354 | unsupported by design | mmap zero-fill workload stops at mmap ENODEV under selected profile | — |
| generic/355 | bug | four successful direct-I/O writes leave setuid/setgid bits set; ext4 control clears suid and conditionally sgid | [SFS-936](https://linear.app/valandreev/issue/SFS-936) |
| generic/363 | unsupported by design | fsx mmap EOF pollution and MAPWRITE operations return ENODEV under selected profile | — |
| generic/375 | unsupported by design | test requires POSIX ACL behavior not offered; setfacl/chmod SGID expectations depend on ACL application | [SFS-934](https://linear.app/valandreev/issue/SFS-934) |

### Artifacts and stop state

Raw check/xunit output is under the three canonical chunk directories above.
`summary-final.json` and `classifications-final.json` cover all 403 unique IDs;
`cleanup-final.json` records zero remaining task mounts/processes and removal
of empty temporary `/srv` directories. Client caches, both volume pairs and
failed-state diagnostics are retained. `exclude-used`, build logs, environment
and harness hashes, supplemental attempts and post-loop error are preserved
under the same ignored run root. No raw client state or credentials are committed.

This is a bounded baseline, not the original full-generic acceptance gate.
The user explicitly deferred /404–805 and WSL2 is owned by SFS-935. No CI,
registered mounted E2E gate, workspace Rust suite or other platform was run.


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
