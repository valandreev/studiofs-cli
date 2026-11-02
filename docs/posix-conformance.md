# POSIX Conformance Baseline (Linux FUSE)

## Full xfstests VM rerun (SFS-940, generic/001–805)

All **805 distinct IDs** from the pinned `tests/generic/group.list` are accounted
for, through generic/805. Every non-timeout canonical record has exactly one
matching xunit testcase; missing, zero-test or malformed reports are not passes.
No product code or registered harness source was changed. Raw evidence is local
under `target/xfstests/20261101T210128Z-sfs940/` and is not committed.

| Measurement | IDs | Pass | Fail | Notrun | Timeout |
|---|---:|---:|---:|---:|---:|
| SFS-907 reference, 001–403 | 403 | 58 | 40 | 303 | 2 |
| SFS-940 comparable range, 001–403 | 403 | 60 | 26 | 312 | 5 |
| SFS-940 newly measured range, 404–805 | 402 | 51 | 22 | 320 | 9 |
| **SFS-940 total** | **805** | **111** | **48** | **632** | **14** |

A notrun is a feature/device/tool probe outcome, not passing filesystem behavior.
There are **zero manual test-ID exclusions**. Compare only the first 403 IDs with
the reference; the additional 402 cases were not measured there and cannot be
called new regressions. All 303 old notrun cases remain notrun.

The canonical budgets remain **60 seconds for /001–013** and **300 seconds for
/014–805**, plus up to ten seconds of termination allowance. The explicit
`generic/*` attempt also repeated /001–013 at 300 seconds; those thirteen rows
are supplemental. In particular, /001 passed in 80 seconds and /007 in 46 seconds
in that attempt, but their original 60-second canonical timeouts remain counted.
A different budget or later successful attempt does not replace a canonical row
or by itself prove a product fix.

### Provenance and environment

| Item | Counted configuration |
|---|---|
| Product build source | `e9c85bf597b7a7f254e4e70f1abd2b680e50ae7b` |
| Retained binary SHA-256 | `7447b80bea8396192550a39c9f073b6faa24d885fab5abf08b306678cbc97146` |
| Build | `cargo build -p studiofs --release -j 2`, exit 0, 12 min 29 s, before counted checks |
| Later checkout | `c1dc07accd7c3aa9c4215c6c5ff513ea2a7ea8df`; fetched origin/main matched it; **this is not the binary's build source** |
| Host | Same devbench Ubuntu 26.04.1 LTS VM; kernel `7.0.0-42-generic` versus reference `7.0.0-38-generic` |
| FUSE | libfuse / fusermount3 3.18.2 |
| xfstests | `22348afe338c0f6d540c0d7ef0db749eaa51b218` plus the narrow repository FUSE subtype patch |
| Storage | Checkout, artifacts, isolated client state and both mount pairs on ext4 |
| Caller/accounts | Root suite; existing `fsgqa` and `fsgqa2` accounts |
| Requested mount/profile | `rw,allow_other,default_permissions`; `FSTYP=fuse`, `FUSE_SUBTYP=.studiofs`; adaptive cache; `STUDIOFS_FUSE_DIRECT_IO_MMAP=0` |
| Primary volumes | `sfs940-20261101-210128-{test,scratch}`; isolated client root beneath the evidence directory |
| Primary mount root | `/srv/sfs940/20261101T210128Z/mnt/`, searchable by secondary test accounts |
| Fresh controls | Separate `sfs940-20261102-ctl013-{test,scratch}` volumes and client root; mount root `/srv/sfs940/20261102-control013/mnt/` |

At the user's request, continuation reused the copied binary without recompiling
or reinitializing the original volumes after the checkout advanced. Thus this
report verifies the e9c85bf5 build, including the listed SFS-907 prerequisite
resolutions; it does not verify later changes such as SFS-943. Kernel, service
availability, interruption history and temporary-directory changes also prevent
attributing every timing difference solely to product code.

The runner, helper and patch hashes stayed unchanged across the measurement.
Full SHA-256 values, the group-list and exclusion hashes, build provenance and
per-wrapper checkout metadata are retained in `delivery.json`, `build-source.json`
and the `*-provenance.txt` / per-fragment `metadata.tsv` files. The binary hash
was checked again at delivery; no secrets or diagnostic tokens are reproduced.

### Attempts, time and recovery

The complete check/control window was **2026-11-01 21:13:58 through 2026-11-02
12:29:37 UTC**, **15 h 15 min 39 s**, including the user-requested overnight pause,
service outage, investigation and recovery gaps. The last canonical wrapper
ended at **12:21:54 UTC**. Canonical per-case elapsed times sum to **3 h 26 min
12 s**: **1 h 33 min 15 s** for /001–403 and **1 h 52 min 57 s** for /404–805.
The sum of measured wrapper windows is **3 h 38 min 25.368 s**, including
supplemental checks and in-fragment recovery but excluding investigation gaps.
These are different quantities; operation-only control setup time was not
separately instrumented. `timings.json` retains every phase's UTC start/end.

There are **826 manifest records: 805 canonical and 21 supplemental/invalid
records**, plus three unmanifested interrupted attempts. `attempts.json` records
all of them; `canonical.tsv` maps each chosen row to its fragment, budget, exit,
duration and check/xunit SHA-256. The 21 additional records are three smoke rows,
thirteen 300-second early repeats, the S3-affected /013, invalid /662 report,
first /680 permission failure and two fresh-volume controls. Standalone helper
and ext4 operation controls are recorded separately, not counted as testcase
passes. The interrupted /011, /001 and /701 attempts remain retained and uncounted.

| Runner fragment / purpose | Canonical contribution | Runner exit / disposition |
|---|---|---|
| `smoke` (/001, /002, /005) | none | 1: /001 timeout; /002 and /005 pass; all-pass smoke not claimed |
| `early-001-013` | /001–010 | intentionally stopped at user pause; /011 interrupted; execution session 137 |
| `early-resume-011-013` | /011–012 | 1; /013 S3/EIO attempt supplemental |
| `full-generic` | none | service-outage stop during /001; execution session 137 |
| `early-retry-013` | /013 | 1: retained-tree cleanup failure |
| `full-generic-retry`, explicit `generic/*` | /014–661 | 1: /662 invalid report on full tmpfs; /001–013 supplemental |
| `remaining-662-805` | /662–700 except /680 | intentionally stopped to correct TMPDIR traversal; /701 interrupted; execution session 137 |
| `tmpdir-fixed-680` | /680 | 0: pass with traversable TMPDIR |
| `remaining-traversable-tmp` | /701–805 | **1: normal completion through /805, with retained fail/timeout outcomes** |
| Fresh `/013` control | none | 1: 70-second case timeout (60 + 10 allowance); not a passing control |
| Fresh `/754` control | none | 0: pass in 24 seconds on fresh SCRATCH |

The final orchestration shell exited 0 after recording the final runner's exit
1 and verifying its complete selection. This is **not** an all-pass suite. The
earlier ENOSPC fragment's orchestration shell exited 1 at its explicit incomplete
selection guard; that failure is retained, not hidden by the later complete
ledger. There was no active edit of `run.sh`, no reference-style post-loop Bash
source-read error, and no successful zero-test setup claim.

On the first morning resume, S3 returned HTTP 502. The affected /013 attempt
failed mkdir with EIO; its output is supplemental. The service was restored by
the user before continuation. After restoration, retained TEST /013 instead
failed while removing an old fsstress tree (ENOTEMPTY followed by EEXIST); its
300-second repeat had the same cleanup failure. Fresh TEST /013 still hit its
60-second budget. Fresh SCRATCH /754 passed, while the original /754 failure was
an extra cleanup error before its symlink/xattr body, not reported target
corruption. Similar setup cleanup messages accompany /798 and /800. These
observations are tracked without assigning a shared cause in
[SFS-953](https://linear.app/valandreev/issue/SFS-953).

Generic/650's timeout left CPUs 1–3 offline. Its intended upstream cleanup enables
them; task-scoped recovery restored them. A surviving /650 descendant was then
verified by exact task environment, process start identity and its output
prologue. It held a roughly 3.8-GB output file and exhausted the 3.7-GB `/tmp`
tmpfs, causing /662 xunit writes to fail. Only that verified orphan was stopped;
its closed raw output and hash were preserved on ext4, and CPU online-state was
checked again. The following /651–661 were native notrun probes; no throughput
claim is based on them. This runner lifecycle defect is
[SFS-951](https://linear.app/valandreev/issue/SFS-951), not a proven product deadlock.
Later audits after /707 and /750 found no completed script orphans.

Continuation first placed TMPDIR on ext4 beneath the inaccessible home prefix.
That was a harness setup mistake: /680's unprivileged helper could not traverse
it. The fragment was drained/stopped after /700, its /701 partial preserved,
and a real executable probe as `fsgqa` verified the corrected task TMPDIR under
`/srv`. /680 then passed in seven seconds. The rest resumed at /701. Both TMPDIR
changes are retained in fragment provenance and the first /680 result remains
supplemental. The registered runner and pinned upstream sources stayed unchanged.

### Classification and comparison

Of the **48 fail** verdicts, **9 cases require bug/investigation follow-up**,
**35 are unsupported feature/profile cases**, and **4 are environment/harness
limits**. All **14 timeouts** are classified as preserved-budget outcomes with
cause unassigned; this does not establish that product liveness is correct.
There are **zero kernel-cache/TTL-only classifications**: no controlled TTL
experiment was performed. Nine investigation cases are not nine independently
proven root causes.

Previously failing /011, /020, /035, /093, /309, /313 and /355 now pass. /120 also
passes the accepted `noatime` behavior; this is not relatime/strictatime support.
/193's complete remaining diff contains only uutils 0.10.0 error-message wording;
its previously wrong truncation privilege bits now match expected output.
/089 is still unverified because it reaches the 300-second budget. /113 changed
from pass to timeout; this is an unresolved budget/performance observation, not
a diagnosed deadlock or confirmed functional regression.

Pass-to-notrun /053, /105, /237 and /318 all stop at unsupported ACL probes.
Fail-to-notrun /099, /307, /319 and /375 do likewise; /003 stops at the unsupported
relatime probe. These transitions reflect explicit unsupported contracts, not
new feature coverage or silently omitted tests. /192 retains the unsupported
read-time atime mismatch (SFS-937).

The [Linux bridge contract](platform-bridges.md) deliberately uses direct I/O
for write-capable adaptive handles. It also leaves shared direct-I/O mmap opt-in
disabled here. A supplemental exact /800 helper replay captured the stderr
upstream suppresses: `failed to mmap: No such device` before pread (exit 237).
/798's all-zero cachestat results are the selected direct-I/O profile, whereas
the same-host ext4 operation control reports four cached/dirty pages and clears
dirty after fsync. The ext4 /728 control advances ctime on both xattr calls;
the StudioFS testcase only advances it on setxattr. These are operation controls,
not full ext4 suite runs or additional canonical cases.

For /631, kernel messages explicitly report missing tmpfile and RENAME_WHITEOUT
support for the overlay upper filesystem; its overlay rename workload never
started. /452 additionally hits renamed-uutils `ls_on_scratch` dispatch and
remount limits. /732 needs alternate mount targets which the helper intentionally
rejects. No helper security boundary was relaxed to make those tests pass.

The following table covers **every original fail/timeout and every current
fail/timeout**. Unmeasured means no old verdict exists. Every row's exact
canonical duration and raw artifact location are in `canonical.tsv`; every
failure/timeout bucket is in `classifications.json`.

| Test | SFS-907 → canonical SFS-940 | Interpretation / retained outcome | Follow-up or resolution |
|---|---|---|---|
| `generic/001` | fail → timeout | budget/setup limit: 60s; cause unassigned; supplemental 300s attempt passes in 80s, canonical 60s result unchanged | [SFS-927](https://linear.app/valandreev/issue/SFS-927) |
| `generic/003` | fail → notrun | legitimately unsupported/notrun: relatime not supported by the current kernel | [SFS-937](https://linear.app/valandreev/issue/SFS-937) |
| `generic/007` | pass → timeout | budget/setup limit: 60s; cause unassigned; supplemental 300s attempt passes in 46s | — |
| `generic/011` | fail → pass | fixed/pass in this case | [SFS-928](https://linear.app/valandreev/issue/SFS-928) |
| `generic/013` | timeout → fail | bug/investigation: Retained fsstress tree cannot be recursively removed after interrupted/outage attempts; no root cause assigned; fresh-state control recorded separately | [SFS-953](https://linear.app/valandreev/issue/SFS-953) |
| `generic/020` | fail → pass | fixed/pass in this case | [SFS-931](https://linear.app/valandreev/issue/SFS-931) |
| `generic/029` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/030` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/035` | fail → pass | fixed/pass in this case | [SFS-930](https://linear.app/valandreev/issue/SFS-930) |
| `generic/074` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/075` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/080` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/089` | fail → timeout | budget/setup limit: 300s; cause unassigned | [SFS-932](https://linear.app/valandreev/issue/SFS-932) |
| `generic/091` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/093` | fail → pass | fixed/pass in this case | [SFS-933](https://linear.app/valandreev/issue/SFS-933) |
| `generic/099` | fail → notrun | legitimately unsupported/notrun: ACLs not supported by this filesystem type: fuse | [SFS-934](https://linear.app/valandreev/issue/SFS-934) |
| `generic/112` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/113` | pass → timeout | budget/setup limit: 300s; cause unassigned | — |
| `generic/120` | fail → pass | pass: noatime helper behavior; no automatic-atime feature claim | [SFS-937](https://linear.app/valandreev/issue/SFS-937) |
| `generic/127` | timeout → timeout | budget/setup limit: 300s; cause unassigned | — |
| `generic/128` | fail → fail | environment/harness: Helper rejects nosuid mount option | — |
| `generic/141` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/192` | fail → fail | unsupported by design/profile: Automatic read-time atime update is not implemented (SFS-937) | [SFS-937](https://linear.app/valandreev/issue/SFS-937) |
| `generic/193` | fail → fail | environment/harness: Remaining diff is uutils chown/chgrp/touch error-message wording; privilege-bit results now match expected output | [SFS-936](https://linear.app/valandreev/issue/SFS-936) |
| `generic/198` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/215` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/246` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/247` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/248` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/258` | fail → fail | unsupported by design/profile: Linux bridge explicitly clamps pre-epoch timestamps to zero | — |
| `generic/263` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/307` | fail → notrun | legitimately unsupported/notrun: ACLs not supported by this filesystem type: fuse | [SFS-934](https://linear.app/valandreev/issue/SFS-934) |
| `generic/309` | fail → pass | fixed/pass in this case | [SFS-938](https://linear.app/valandreev/issue/SFS-938) |
| `generic/313` | fail → pass | fixed/pass in this case | [SFS-939](https://linear.app/valandreev/issue/SFS-939) |
| `generic/314` | fail → fail | unsupported by design/profile: Setgid-directory inheritance is not offered | — |
| `generic/319` | fail → notrun | legitimately unsupported/notrun: ACLs not supported by this filesystem type: fuse | [SFS-934](https://linear.app/valandreev/issue/SFS-934) |
| `generic/340` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/344` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/345` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/346` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/354` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/355` | fail → pass | fixed/pass in this case | [SFS-936](https://linear.app/valandreev/issue/SFS-936) |
| `generic/363` | fail → fail | unsupported profile: mmap ENODEV | — |
| `generic/375` | fail → notrun | legitimately unsupported/notrun: ACLs not supported by this filesystem type: fuse | [SFS-934](https://linear.app/valandreev/issue/SFS-934) |
| `generic/426` | unmeasured → fail | bug/investigation: probe passes; linked handles return ESTALE | [SFS-949](https://linear.app/valandreev/issue/SFS-949) |
| `generic/428` | unmeasured → fail | unsupported profile: mmap ENODEV | — |
| `generic/431` | unmeasured → fail | bug/investigation: Immediate cmp sees ENOENT for empty EOF copy_range destination which later md5sum sees | [SFS-950](https://linear.app/valandreev/issue/SFS-950) |
| `generic/437` | unmeasured → fail | unsupported profile: mmap ENODEV | — |
| `generic/438` | unmeasured → fail | unsupported profile: mmap ENODEV | — |
| `generic/452` | unmeasured → fail | environment/harness: Renamed uutils ls binary rejects argv[0]=ls_on_scratch; remount helper also rejects the invocation | — |
| `generic/464` | unmeasured → timeout | budget/setup limit: 300s; cause unassigned | — |
| `generic/467` | unmeasured → fail | bug/investigation: probe passes; linked handles return ESTALE | [SFS-949](https://linear.app/valandreev/issue/SFS-949) |
| `generic/476` | unmeasured → timeout | budget/setup limit: 300s; cause unassigned | — |
| `generic/477` | unmeasured → fail | bug/investigation: probe passes; linked handles return ESTALE | [SFS-949](https://linear.app/valandreev/issue/SFS-949) |
| `generic/521` | unmeasured → fail | unsupported profile: mmap ENODEV | — |
| `generic/522` | unmeasured → fail | unsupported profile: mmap ENODEV | — |
| `generic/538` | unmeasured → timeout | budget/setup limit: 300s; cause unassigned | — |
| `generic/551` | unmeasured → timeout | budget/setup limit: 300s; cause unassigned | — |
| `generic/615` | unmeasured → timeout | budget/setup limit: 300s; cause unassigned | — |
| `generic/616` | unmeasured → fail | unsupported profile: mmap ENODEV | — |
| `generic/617` | unmeasured → fail | unsupported profile: mmap ENODEV | — |
| `generic/631` | unmeasured → fail | unsupported by design/profile: Overlay upper mount rejected; kernel reports missing tmpfile and RENAME_WHITEOUT support, so rename workload never ran | — |
| `generic/638` | unmeasured → fail | unsupported profile: mmap ENODEV | — |
| `generic/650` | unmeasured → timeout | budget/setup limit: 300s; cause unassigned | [SFS-951](https://linear.app/valandreev/issue/SFS-951) |
| `generic/707` | unmeasured → timeout | budget/setup limit: 300s; cause unassigned | — |
| `generic/728` | unmeasured → fail | bug/investigation: removexattr leaves ctime unchanged in the exact upstream xattr sequence; isolated ext4 control advances it; root cause unassigned | [SFS-952](https://linear.app/valandreev/issue/SFS-952) |
| `generic/732` | unmeasured → fail | environment/harness: Test requires additional mount targets; helper intentionally accepts only its configured isolated volume/mountpoint pairs | — |
| `generic/750` | unmeasured → timeout | budget/setup limit: 300s; cause unassigned | — |
| `generic/754` | unmeasured → fail | bug/investigation: Recursive cleanup of retained scratch tree reports ENOTEMPTY before symlink/xattr workload; body reports no corruption; fresh-state control tracked separately | [SFS-953](https://linear.app/valandreev/issue/SFS-953) |
| `generic/756` | unmeasured → fail | bug/investigation: probe passes; linked handles return ESTALE | [SFS-949](https://linear.app/valandreev/issue/SFS-949) |
| `generic/759` | unmeasured → fail | unsupported profile: mmap ENODEV | — |
| `generic/777` | unmeasured → fail | bug/investigation: probe passes; linked handles return ESTALE | [SFS-949](https://linear.app/valandreev/issue/SFS-949) |
| `generic/795` | unmeasured → timeout | budget/setup limit: 300s; cause unassigned | — |
| `generic/798` | unmeasured → fail | unsupported by design/profile: cachestat expects buffered dirty pages, but adaptive write-capable handles deliberately use direct I/O; all cache counts zero; retained scratch cleanup error also present | — |
| `generic/800` | unmeasured → fail | unsupported by design/profile: Supplemental exact helper replay fails at mmap with ENODEV before pread; shared direct-I/O mmap is disabled; retained scratch cleanup error also present | — |

The linked new observations are [SFS-949](https://linear.app/valandreev/issue/SFS-949)
(export handle support/advertising), [SFS-950](https://linear.app/valandreev/issue/SFS-950)
(immediate EOF copy_range visibility), [SFS-952](https://linear.app/valandreev/issue/SFS-952)
(removexattr ctime), and [SFS-953](https://linear.app/valandreev/issue/SFS-953)
(retained-state cleanup). SFS-949 does not assume full NFS export support is
required; an unsupported interface may instead need explicit rejection. The
/404–805 cases had no original measurements; neither these observations nor the
retained-state cleanup establish a new regression in current main.

SFS-950 traced the generic/431 failure to the `read`, not the open: `cmp`
opened the empty `g` destination and its first `read` returned `ENOENT`.
Opening an unwritten create treated its admitted attributes as published
metadata and dropped the local attributes while the deferred create was still
queued, so the read missed the inode until the batch committed. The fix and its
unit and in-process mounted regressions do not replace a VM rerun of
generic/431; the row above remains the SFS-940 measurement.

### Environment-prerequisite reruns (2026-11-02)

Nineteen canonical notrun rows stopped on missing host prerequisites rather than
filesystem features: `fsverity` (11), `duperemove` (3), the `fsgqa2` group (2),
an unbuilt `src/dbtest` (`libgdbm-compat-dev` was installed after configure),
`dbench` and `accton`. After installing `acct dbench duperemove fsverity
libgdbm-compat-dev`, creating group `fsgqa2`, reconfiguring with the original
arguments and rebuilding the pinned checkout, only those IDs were rerun with the
same retained binary (`e9c85bf5`), volumes, config and canonical budgets (60s for
/010, 300s otherwise). Evidence is in `env-fixed-010/` and `env-fixed-rest/`.

| Test | Rerun | Outcome |
|---|---|---|
| `generic/010` | fail | `dbm_open: No such device`: gdbm uses shared mmap, which returns ENODEV under the selected mmap-disabled profile (same class as /759) |
| `generic/241` | fail | dbench (4 clients): `unlink` of `NEWPCB.PPT` returns ENOENT right after a successful stat; afterwards a FUSE `FLUSH` from a remaining client never received a reply. The kernel stack shows `fuse_flush`; the daemon's `fuse_worker` is parked in `flush_volatile_writeback_with_outcome` on the deferred-release gate while every tokio worker is idle. Product bug: [SFS-954](https://linear.app/valandreev/issue/SFS-954) |
| `generic/597` | fail | hardlink protection returns EACCES instead of the expected EPERM; an ext4 control on the same 7.0.0-42 kernel also returns EACCES, so this is a kernel/upstream-expectation mismatch rather than StudioFS |
| `generic/559–561` | notrun | dedupe not supported |
| `generic/572–577, 579, 624, 625, 692, 788` | notrun | no fs-verity support on fuse |
| `generic/596` | notrun | `chattr +S` not supported |
| `generic/598` | notrun | mknod/mkfifo not supported |

These reruns are supplemental and the canonical table above is unchanged.
Substituting them would give 111 pass, 51 fail, 629 notrun and 14 timeout. The
/241 hang left the test mount busy; it was detached with the recorded recovery
procedure, and `env-fixed-cleanup.json` confirms zero task mounts and processes.
The `tools/xfstests/README.md` package list now includes the missing tools.

### Final cleanup and verification

Both primary and fresh-control mount pairs were detached. Global inspection at
**2026-11-02 12:34:19 UTC** verified **zero remaining task mounts, nested mounts
and live task processes**, with CPU `0–3` online and no offline CPU. All four
volumes and both client roots remain preserved; no ordinary development data
was deleted. Closed `/srv` temporary evidence was moved into the ignored evidence
root. `cleanup-final.json`, `control013/cleanup-final.json` and
`cleanup-global-final.json` retain the exact checks.

The retained artifact set includes `canonical.json`/`.tsv` (805 rows),
`classifications.json` (62 fail/timeout rows), `attempts.json`, `timings.json`,
`comparison-403.json`, build/hash provenance, all per-case raw check/xunit files,
the orphan output/hash, scoped recovery/CPU/process audits and separate controls.
Do not delete this evidence or reuse these volume names until investigation is
finished. WSL2 coverage remains separate in
[SFS-935](https://linear.app/valandreev/issue/SFS-935).

Validation at delivery: 805-ID canonical/xunit and budget assertions, exhaustive
failure classification, retained binary/hash checks, shell syntax, the 21 focused
harness/upstream tests and `git diff --check`. No workspace-wide Rust suite,
Windows/WSL mount or enabled-mmap experiment was run, and no product fix is
claimed by this measurement.

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
| generic/003 | environment/harness | nodiratime/strictatime are unavailable through helper (`noatime` is accepted as a no-op since SFS-937; not rerun); a helper change alone would not pass, because SFS-937 defines [automatic strictatime/relatime updates as unsupported](platform-bridges.md#linux-access-time) | [SFS-937](https://linear.app/valandreev/issue/SFS-937) |
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
| generic/120 | environment/harness | nodiratime/noatime/strictatime were unavailable through helper; `noatime` is now accepted as a no-op (SFS-937), not rerun | [SFS-937](https://linear.app/valandreev/issue/SFS-937) |
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


## SFS-938 focused replacement-rename rerun

On 2026-11-01, `generic/309` passed on `devbench` with kernel
`7.0.0-42-generic`, using product commit
`5deb586d220019262d1f8670d45d5ad33c9addac`. The release binary was rebuilt with
`cargo build -p studiofs --release -j 2`; its SHA-256 was
`dfd7d46ecdcec9dc4363e2dd73d23c24d25f86edd66a046ffbc37f7bcffe290c`.
The pinned xfstests revision and repository FUSE subtype patch were unchanged.
The installed mount helper was updated to the repository version before this
run; its previous copy is retained with the artifacts.

The repository `tools/xfstests/run.sh` runner selected only `generic/309`, with
fresh dedicated test/scratch volumes, adaptive caching, writable mmap disabled,
and `rw,allow_other,default_permissions`. The manifest records **1 pass, 0
failures, 0 skips**, with a 13-second wrapper duration; the xunit testcase took
2 seconds. This confirms that cross-parent replacement advances the destination
directory's mtime and ctime through the live Linux FUSE path.

The registered `mounted-symlink-attribute-convergence` scenario also passed
using the same product binary, `--infra env --platform linux-fuse`, and services
from `.env` (38 seconds, cleanup passed). All six PostgreSQL-backed rename tests
passed with `cargo test -p studiofs-meta --lib rename -- --ignored --test-threads=1`.
Device access and PostgreSQL connectivity required execution outside the agent
sandbox; their absence inside that sandbox was not a host limitation.

Artifacts remain under ignored
`target/xfstests/sfs938-5deb586d/` (manifest, xunit, build/binary/patch identity,
mount logs, configuration and `cleanup.json`) and
`target/e2e-runs/sfs938-5deb586d-attrs/` (scenario report and validation).
Both xfstests mounts were detached and their recorded daemons exited; E2E
cleanup passed. Test volumes and diagnostics are retained. This focused rerun
does not change the historical 403-case baseline totals or establish the full
SFS-940 suite gate.

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
