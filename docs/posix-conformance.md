# POSIX Conformance Baseline (Linux FUSE)

This is the first measured POSIX conformance picture of the Linux FUSE mount
(SFS-906). [pjdfstest](https://github.com/saidsay-so/pjdfstest) checks syscall
semantics and errno values. fsx from xfstests checks data integrity with
randomized operation streams. Both ran against a live StudioFS mount and,
as a control, against a plain ext4 directory in the same WSL2 instance.

The suites only see the local view of one mount. Cross-mount visibility,
multi-node convergence and crash consistency stay with the `studiofs-e2e`
scenarios. This baseline does not include product fixes; every bug found here
has its own follow-up issue.

## Summary

Run `20261028T105601Z-3760ccb9e`, commit `3760ccb9e19fe7cd745e5356661af7ccda97ac0b`.

| Suite | ext4 control | StudioFS (default mount) |
|---|---|---|
| pjdfstest, 398 cases | 385 pass, 13 skip, 0 fail | 148 pass, 14 skip, 236 fail |
| fsx, 5 profiles | 5 pass | 2 pass (no-mmap), 3 fail (mmap `ENODEV`) |

Of the 236 pjdfstest failures, 180 are unsupported by design: special files
and root ownership. The other 56 are bugs in four follow-up issues. With
`STUDIOFS_LINUX_FUSE_CACHE_POLICY=kernel`, mmap works, but fsx then finds an
intermittent **data loss** race (SFS-915).

| Issue | Bucket | Finding | pjdfstest cases |
|---|---|---|---:|
| [SFS-910](https://linear.app/valandreev/issue/SFS-910) | Bug | create → setattr → unlink abandons the deferred namespace batch and degrades the mount; every later create returns `EIO` until remount | 4 triggers |
| [SFS-911](https://linear.app/valandreev/issue/SFS-911) | Bug | `symlink`/`link` against an inode whose deferred create is not committed returns `ENOENT` | 43 |
| [SFS-912](https://linear.app/valandreev/issue/SFS-912) | Bug | names over 255 bytes return `EINVAL`/`ENOENT` instead of `ENAMETOOLONG`; Windows-reserved bytes `<>:"\|\?*` are rejected on Linux | 11 |
| [SFS-913](https://linear.app/valandreev/issue/SFS-913) | Bug (part 2: possible TTL artifact) | create/symlink do not update the parent mtime/ctime; parent nlink/times after `mkdir` lag by one attr TTL | 2 |
| [SFS-915](https://linear.app/valandreev/issue/SFS-915) | Bug | fsx: a write followed by truncates reads back as zeros under `kernel` cache policy (`failed to relink retained overlay payloads after size change`) | fsx |
| [SFS-916](https://linear.app/valandreev/issue/SFS-916) | Bug / design decision | shared writable `mmap` fails with `ENODEV` under the default `adaptive` policy | fsx |

SFS-908 added `--allow-other`, which this baseline needs; see
[mount configuration](#mount-configuration). The SFS-910 fix (PR #888) merged
after the baseline commit and is not reflected in these numbers.

## 2026-10-30 Follow-up Run

Run `20261030T151455Z-76a9fc059`, product commit
`76a9fc05923673b86c7d049119345abed58d821f` (also `origin/main`). The
release binary was rebuilt from this commit before measurements. It includes
the fixes merged as PRs #888 (SFS-910), #897 (SFS-911), #899 (SFS-912),
#900 (SFS-913), #891 (SFS-915), #894 (SFS-916), #895 (SFS-921), and
#896 (SFS-922). The pinned pjdfstest and fsx revisions are unchanged from the
previous run.

### pjdfstest

The ext4 control and StudioFS per-group run exercised all 15 groups; StudioFS
per-case mode exercised all 398 cases. Counts below are pass / skip / fail.
The per-case run is the source for failure classification.

| Group | Previous StudioFS | Current per-group | Delta (pass / fail) |
|---|---:|---:|---:|
| `chmod` | 11 / 1 / 21 | 16 / 1 / 16 | +5 / -5 |
| `chown` | 10 / 2 / 14 | 16 / 2 / 8 | +6 / -6 |
| `ftruncate` | 6 / 0 / 0 | 6 / 0 / 0 | 0 / 0 |
| `link` | 9 / 2 / 30 | 15 / 2 / 24 | +6 / -6 |
| `mkdir` | 7 / 1 / 13 | 11 / 1 / 9 | +4 / -4 |
| `mkfifo` | 6 / 1 / 14 | 9 / 1 / 11 | +3 / -3 |
| `mknod` | 10 / 0 / 28 | 15 / 0 / 23 | +5 / -5 |
| `open` | 14 / 2 / 11 | 18 / 2 / 7 | +4 / -4 |
| `posix_fallocate` | 5 / 0 / 0 | 5 / 0 / 0 | 0 / 0 |
| `rename` | 18 / 1 / 41 | 25 / 1 / 34 | +7 / -7 |
| `rmdir` | 11 / 1 / 11 | 14 / 1 / 8 | +3 / -3 |
| `symlink` | 7 / 1 / 16 | 12 / 1 / 11 | +5 / -5 |
| `truncate` | 12 / 1 / 6 | 14 / 1 / 4 | +2 / -2 |
| `unlink` | 7 / 1 / 26 | 12 / 1 / 21 | +5 / -5 |
| `utimensat` | 15 / 0 / 5 | 16 / 0 / 4 | +1 / -1 |
| **All** | **148 / 14 / 236** | **204 / 14 / 180** | **+56 / -56** |

The ext4 per-group control passed 385, skipped the same 13 EROFS cases, and
had no failures. StudioFS per-group and per-case verdicts match case by
case: all 398 case names have the same verdict in both modes. The per-case
classifier assigns every
failure exactly once: 175 unsupported special-file cases, two legacy
root-owner translations, and three inode-ctime failures (SFS-923). There
were no SFS-911, SFS-912, or SFS-913 failures and no unclassified failures. The two root-owner
cases match the documented uid/gid-zero-to-mount-user translation.

The per-case comparison against the retained previous-run results shows 40
original SFS-911 ENOENT/link failures now pass, all 11 original SFS-912
failures pass, and both SFS-913 cases pass. The previous rendered summary labeled 43 cases
as SFS-911; the raw per-case artifacts split that count into
40 ENOENT/link failures and the three ctime failures now tracked as SFS-923.
Those same three ctime cases failed in both runs, so they are reclassified
baseline failures, not newly introduced failures. Across all 398 cases, 56
previous failures now pass, 180 remain failed, and no previously passing case
became a failure. All 14 `enametoolong_component` cases pass; three of those
had previously been classified with unsupported special-file errors.

All three SFS-923 ctime cases failed in both the per-group and per-case modes
and reproduced when run alone. The tests require ctime to advance when a
rename changes an inode's link count, when a symlink is renamed, and when
unlink decrements a hard-linked inode's link count. The ext4 control passed
them. Logs, normalized results, classifier reports, the baseline-to-current
case comparison, and the exact verdict comparison are retained under
`target/posix-conformance/20261030T151455Z-76a9fc059/`.

SFS-910 is confirmed fixed for this run: all groups completed, no case was
marked degraded, and the mount log contains no `abandoning batch` warning.
The three reclassified ctime failures are tracked as
SFS-923.

#### SFS-923 focused follow-up (2026-10-30)

The local metadata transaction advances ctime when rename changes the moved inode, when rename replacement decrements the displaced inode link count, and when unlink decrements a surviving inode link count. Moved symlink and regular-file attrs use the existing `SetAttr` event; directory attrs remain in `RenameEntry`. Existing VFS handling invalidates moved and displaced inode attr caches.

After rebuilding `target/release/studiofs`, the three affected cases passed on the live WSL2 FUSE mount:

| pjdfstest case | Result |
|---|---|
| `rename::changed_ctime_success::symlink` | pass |
| `rename::to_multiply_linked::regular` | pass |
| `unlink::update_ctime_success::regular` | pass |

The run covered these three cases; the full 398-case baseline was not rerun. Results are retained in `target/sfs923/live/pjdfstest-studiofs-per-case/results.txt`; build and focused Rust validation logs are under `target/sfs923/`.

### fsx

The ext4 control passed all five profiles. The StudioFS adaptive results below
show why the shared-mmap opt-in is kept distinct from the default policy.

| Profile | Arguments | Previous adaptive | ext4 | Current adaptive (default) | Current adaptive with mmap opt-in | Current kernel policy |
|---|---|---|---|---|---|---|
| default | `-N 100000 -S 906001` | `ENODEV` | pass (18 s) | `ENODEV` at MAPWRITE (expected with opt-in off) | pass (105 s) | pass (186 s) |
| nommap | `-R -W -N 100000 -S 906002` | pass | pass (6 s) | pass (50 s) | pass (65 s) | not run |
| small | `-l 65536 -o 4096 -N 100000 -S 906003` | `ENODEV` | pass (9 s) | `ENODEV` at MAPWRITE (expected with opt-in off) | pass (159 s) | ten consecutive runs passed |
| large | `-l 67108864 -o 1048576 -N 20000 -S 906004` | `ENODEV` | pass (22 s) | `ENODEV` at MAPWRITE (expected with opt-in off) | pass (39 s) | pass (58 s) |
| large-nommap | `-R -W -l 67108864 -o 1048576 -N 20000 -S 906005` | pass | pass (15 s) | pass (18 s) | pass (21 s) | not run |

The adaptive default-policy mmap failures retain the documented SFS-916 choice:
shared writable mmap stays opt-in. With
`STUDIOFS_FUSE_DIRECT_IO_MMAP=1`, all five profiles passed once. The required
ten consecutive adaptive `small` runs also passed (100,000 operations each,
seed 906003), confirming SFS-921/922 for this WSL2 run. Their timings were
124, 158, 150, 143, 135, 131, 156, 124, 151, and 133 seconds. There were no
`READ BAD DATA` results or `failed to relink retained overlay payloads`
warnings.

The kernel-policy default profile passed 100,000 operations with seed 906001
(186 s). Its `small` profile passed 100,000 operations with seed 906003 and
was followed immediately by nine more identical runs; all ten passed. The
first run raw output is in `fsx-studiofs-kernel/small.log`; the consolidated
summary is `fsx-studiofs-kernel-small-10/summary.txt`. The kernel `large`
profile also passed 20,000 operations with seed 906004 (58 s). SFS-915 is
confirmed fixed for these WSL2 profiles. SFS-916 remains a documented
default-policy decision: mmap returns `ENODEV` when the opt-in is off, while
all mmap profiles passed with it on.

The adaptive remount durability check passed two nontrivial profiles:
`-R -W -N 50000 -S 906010` (241,791-byte output) and
`-R -W -l 67108864 -o 1048576 -N 10000 -S 906011` (67,108,864-byte output).
After a clean unmount and fresh mount, both files matched their pre-unmount
SHA-256 hashes. Before/after hashes and sizes are retained under
`fsx-durability/`.

The retained daemon log was scanned across the run, including remount
intervals. Counts were zero for retained-overlay relink failures, missing
payload reports, sticky async upload reports, payload link/submit reports,
abandoned batches, and `degraded=1`. The counts are recorded in
`warning-counts.json`.

The Linux-VM rerun was not performed: the available VM repeatedly stopped and
did not provide a stable test window. Track that validation in
SFS-924; no VM result is claimed.
Run logs, summaries, commands, and environment evidence are under
`target/posix-conformance/20261030T151455Z-76a9fc059/`, including
`command-manifest.txt`, `product-build.log`, `warning-counts.json`,
`classification-per-group.md`, `classification-per-case.md`,
`pjdfstest-mode-comparison.json`, `blocker-case-comparison.json`,
`classification-old-baseline.md`, fsx profile logs, both ten-run summaries,
and `fsx-durability/sha256-{before,after,after-check}.txt` plus
`fsx-durability/stat-{before,after}.txt`.

## Environment

| Item | Value |
|---|---|
| Host | WSL2, kernel `6.6.114.1-microsoft-standard-WSL2`, 24 vCPU, 46 GiB RAM |
| Distro | Ubuntu 24.04.5 LTS |
| FUSE | `/dev/fuse`, `fusermount3` 3.14.0, `libfuse3-3` 3.14.0-5build1, `user_allow_other` in `/etc/fuse.conf` |
| Toolchain | rustc 1.97.1, gcc 13.3.0 |
| Storage | Checkout, `target/`, redb metadata dir, payload cache, mount point and ext4 control all on the WSL ext4 root (`/dev/sdd`). Nothing under `/mnt/c` or other drvfs/9p paths. |
| Services | PostgreSQL and S3 of the existing `test` volume, reached over the network. The foreground path under test uses only the local redb view and payload cache. |
| Product build | `cargo build -p studiofs --release` at the commit above, rebuilt before the counted runs |

Use a mount point outside a `0750` home directory, such as `/srv/sfs906`:
the second pjdfstest accounts (`nobody`, `tests`) must be able to search every
path component. `studiofs mount` appends the volume name when the mount path
does not already end with it, so `/srv/sfs906/mnt` mounts at
`/srv/sfs906/mnt/test`.

## Suite versions

| Suite | Source | Revision |
|---|---|---|
| pjdfstest | Rust rewrite, `saidsay-so/pjdfstest` (crate 0.2.2) | `ce3ca5c10d1d29f25b2881c85c62be2c74c27bf8` |
| fsx | xfstests `ltp/fsx.c`, built without autoconf | `22348afe338c0f6d540c0d7ef0db749eaa51b218` |

## Mount configuration

```bash
studiofs mount test /srv/sfs906/mnt/test -d --allow-other
```

```
test /srv/sfs906/mnt/test fuse.studiofs rw,nosuid,nodev,relatime,user_id=1000,group_id=1000,default_permissions,allow_other,max_read=4194304 0 0
```

| Setting | Value |
|---|---|
| `allow_other` | on (`--allow-other`, SFS-908); without it the kernel rejects root and every second uid before StudioFS sees the request |
| `default_permissions` | always on; the kernel enforces mode bits |
| Entry TTL | 1 s (`TTL_SECONDS`, not configurable) |
| Attr TTL | 1 s for cache-safe lookups, 0 s otherwise; no zero-TTL switch exists |
| Cache policy | `adaptive` (default): kernel page cache for stable read-only handles, `direct_io` for write-capable handles. A second fsx pass used `STUDIOFS_LINUX_FUSE_CACHE_POLICY=kernel`. |
| Async writes | enabled (default) |
| Read-only | no |
| Metadata / cache dir | `~/.studiofs/meta/test`, cache `~/.studiofs/meta/test/cache` (ext4) |
| Worker threads | 48 (default policy), `max_background=128` |

Because the TTL is a constant, bucket 4 (kernel-cache artifact) could not be
confirmed by a zero-TTL rerun. SFS-913 asks for a diagnostic override.

## Expected-unsupported features

This list was derived from `docs/platform-bridges.md` and
`docs/juicefs-local-first-rust-contract.md` before the run. pjdfstest has no
switch to opt out of special files, so these cases run and are classified as
bucket 2 instead.

| Feature | Expected result | Source |
|---|---|---|
| FIFOs, sockets, character and block devices (`mknod`, `mkfifo`, `bind` on `AF_UNIX`) | `EOPNOTSUPP`; only regular files, directories and symlinks exist (`FileType`) | `platform-bridges.md`, Linux callback table (`mknod`) |
| Root as owner | inodes stored with uid/gid 0 are reported as the mount user (legacy translation in `studiofs_bridge::posix::to_bridge_attr`) | `platform-bridges.md`, POSIX caller permissions |
| setgid directory group inheritance | not implemented | `platform-bridges.md`, POSIX caller permissions |
| `rename` flags (`RENAME_NOREPLACE`, `RENAME_EXCHANGE`) | `EINVAL` | `platform-bridges.md`, Linux callback table |
| `fallocate` | no callback; `posix_fallocate` falls back to writing zeros in glibc, and fsx disables its fallocate ops | Linux callback table |
| POSIX ACLs, NFSv4 ACLs, file flags (`chflags`) | not offered; the pjdfstest features stay off | — |
| EROFS cases | need the suite to remount read-only; skipped (`allow_remount = false`) | suite config |
| Hard links | supported (`link` callback) | Linux callback table |
| xattrs | supported (`*xattr` callbacks); not exercised by these suites | Linux callback table |

## Reproduction

All helper files are in `tools/posix-conformance/`. The scripts never print
credentials; load `.env` only if the volume needs it.

```bash
# 1. One-time host setup (root).
sudo apt-get install -y libacl1-dev acl strace
echo 'tests:x:::Dummy User for pjdfstest:/:/usr/sbin/nologin' | sudo newusers
grep -qx user_allow_other /etc/fuse.conf || echo user_allow_other | sudo tee -a /etc/fuse.conf
sudo install -d -o "$USER" -g "$USER" -m 755 /srv/sfs906 /srv/sfs906/mnt /srv/sfs906/ext4

# 2. Build the suites at the recorded revisions and the product binary.
tools/posix-conformance/build-tools.sh
cargo build -p studiofs --release

# 3. Mount an initialized volume with allow_other.
target/release/studiofs mount test /srv/sfs906/mnt/test -d --allow-other
RUN=target/posix-conformance/$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse --short=9 HEAD)
mkdir -p "$RUN"

# 4. pjdfstest: ext4 control per group, then StudioFS per case.
sudo tools/posix-conformance/run-pjdfstest.sh ext4 /srv/sfs906/ext4 "$PWD/$RUN"
cat "$RUN"/pjdfstest-ext4/*.log | grep -oE '^[a-z_]+::[A-Za-z0-9_:]+' > "$RUN/cases.txt"
sudo SFS_MOUNT=/srv/sfs906/mnt/test tools/posix-conformance/run-pjdfstest-per-case.sh \
  "$PWD/$RUN" "$PWD/$RUN/cases.txt" "$(ls -t ~/.studiofs/logs/test-*.log.* | head -1)"
tools/posix-conformance/classify-pjdfstest.py "$RUN/pjdfstest-studiofs-per-case/results.txt"

# 5. fsx: ext4 control and StudioFS, then the kernel cache policy pass.
tools/posix-conformance/run-fsx.sh ext4 /srv/sfs906/ext4/fsx "$RUN"
tools/posix-conformance/run-fsx.sh studiofs /srv/sfs906/mnt/test/sfs906-fsx "$RUN"
target/release/studiofs umount /srv/sfs906/mnt/test
STUDIOFS_LINUX_FUSE_CACHE_POLICY=kernel target/release/studiofs mount test /srv/sfs906/mnt/test -d --allow-other
tools/posix-conformance/run-fsx.sh studiofs-kernel /srv/sfs906/mnt/test/sfs906-fsx-k "$RUN" default small large
```

The per-case runner exists because of SFS-910: in a per-group run, one
degrading case turns every later case of the group into `EIO`. In the first
group run on StudioFS, the `chmod` group degraded the mount after about
30 cases, and all 14 later groups failed at `mkdir`. That run is kept as
`pjdfstest-studiofs/` in the run directory but is not counted. Once SFS-910
is fixed, `run-pjdfstest.sh studiofs <mount-dir> <run-dir>` should give the
same per-case results.

A hung mount can be released with `fusermount3 -uz <mount>`, or as a last
resort with `wsl --shutdown`.

## pjdfstest results

The StudioFS results come from the per-case run. Each case ran alone, and the
mount was remounted after each of the 4 cases that degraded it (SFS-910).
Columns after `fail` split the failures by bucket.

| group | total | ok | skip | fail | SFS-911 | SFS-912 | SFS-913 | special files | root owner |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `chmod` | 33 | 11 | 1 | 21 | 4 | 1 | 0 | 16 | 0 |
| `chown` | 26 | 10 | 2 | 14 | 4 | 2 | 0 | 8 | 0 |
| `ftruncate` | 6 | 6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| `link` | 41 | 9 | 2 | 30 | 5 | 1 | 0 | 24 | 0 |
| `mkdir` | 21 | 7 | 1 | 13 | 2 | 1 | 1 | 8 | 1 |
| `mkfifo` | 21 | 6 | 1 | 14 | 2 | 0 | 0 | 12 | 0 |
| `mknod` | 38 | 10 | 0 | 28 | 3 | 0 | 0 | 25 | 0 |
| `open` | 27 | 14 | 2 | 11 | 3 | 1 | 0 | 6 | 1 |
| `posix_fallocate` | 5 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| `rename` | 60 | 18 | 1 | 41 | 7 | 1 | 1 | 32 | 0 |
| `rmdir` | 23 | 11 | 1 | 11 | 2 | 1 | 0 | 8 | 0 |
| `symlink` | 24 | 7 | 1 | 16 | 4 | 1 | 0 | 11 | 0 |
| `truncate` | 19 | 12 | 1 | 6 | 1 | 1 | 0 | 4 | 0 |
| `unlink` | 34 | 7 | 1 | 26 | 5 | 1 | 0 | 20 | 0 |
| `utimensat` | 20 | 15 | 0 | 5 | 1 | 0 | 0 | 4 | 0 |
| **all** | **398** | **148** | **14** | **236** | **43** | **11** | **2** | **178** | **2** |

The ext4 control passed every case of every group except 13 skips. All 13
are EROFS cases, which need a read-only remount by the suite. StudioFS skips
the same 13, plus `link::link_count_max`, because `pathconf(LINK_MAX)` is
unknown on FUSE. That skip is an environment limit of FUSE, not a failure.

How the failures were classified:

* **Bucket 1, bugs.** Every case with an `ENOENT` from a symlink or hard-link
  step against a freshly created inode is SFS-911; strace shows `symlinkat`
  returning `ENOENT` right after the parent `mkdir`. Every
  `*::enametoolong_component` case answered with `ENOENT`/`EINVAL` is SFS-912.
  `mkdir::changed_time_fields_success` and `rename::updates_link_parent` are
  SFS-913. All four cases that degraded the mount (`chmod::update_ctime::dir`,
  `chmod::update_ctime::regular`, `rename::unchanged_ctime_failed::dir`,
  `utimensat::utime_now`) passed their own assertions; the degrade is SFS-910.
* **Bucket 2, unsupported by design.** 178 cases need a FIFO, socket or device
  node, or create one before the step under test (`EOPNOTSUPP`). This includes
  the `mkfifo`/`mknod` `enametoolong_component` cases, which fail on the
  file type before the name check. Two `uid_gid_eq_euid_egid` cases expect a
  root-created inode to report uid 0 and see the mount user (1000).
* **Bucket 3, environment.** No failure; only the `LINK_MAX` skip above.
* **Bucket 4, kernel-cache / TTL artifact.** Not confirmed. SFS-913 part 2
  becomes visible after exactly one TTL (about 1007 ms on each of 3 probes),
  but the TTL cannot be set to zero to prove it either way.

No failure looks WSL-specific: all of them reproduce through plain syscalls
with a StudioFS-side explanation. Before SFS-915 and SFS-916 are closed, recheck
them once on the Linux VM, because both depend on kernel FUSE page-cache and
mmap behavior.

## fsx results

| Profile | Arguments | ext4 | StudioFS `adaptive` | StudioFS `kernel` |
|---|---|---|---|---|
| default | `-N 100000 -S 906001` | pass | **fail**: `domapwrite: mmap: No such device` at op 5 (SFS-916) | pass |
| nommap | `-R -W -N 100000 -S 906002` | pass | pass (50 s) | — |
| small | `-l 65536 -o 4096 -N 100000 -S 906003` | pass | **fail**: mmap `ENODEV` (SFS-916) | **fail 2 of 4 runs**: `READ BAD DATA`, zeros (SFS-915) |
| small, `-W` | `-W -l 65536 -o 4096 -N 100000 -S 906003` | — | — | **fail 1 of 3 runs** (SFS-915) |
| small, `-R` | `-R -l 65536 -o 4096 -N 100000 -S 906003` | — | — | pass |
| small, no mmap | `-R -W -l 65536 -o 4096 -N 100000 -S 906003` | — | — | pass |
| large | `-l 67108864 -o 1048576 -N 20000 -S 906004` | pass | **fail**: mmap `ENODEV` (SFS-916) | **fail** (SFS-915) |
| large-nommap | `-R -W -l 67108864 -o 1048576 -N 20000 -S 906005` | pass | pass (19 s) | — |

fsx detected that StudioFS supports no `fallocate` mode (punch hole, zero
range, collapse/insert range, unshare, write zeroes), and that ext4 lacks
`UNSHARE_RANGE`/`WRITE_ZEROES`. It disabled those operations on each target,
so the fallocate operation mix differs between ext4 and StudioFS.

SFS-915 is a race, so the same seed does not fail on every run. In the
failing runs, the zeros are in StudioFS's stored state: they persist after
`echo 3 > /proc/sys/vm/drop_caches`. Each failing run coincided with
`failed to relink retained overlay payloads after size change` warnings in the
mount log; the adaptive fsx phase produced none of these warnings. SFS-885
(fsync `EIO` / endless flush) did not reproduce in any profile.

**Durability across remount (adaptive).** `-R -W -N 50000 -S 906010` and
`-R -W -l 67108864 -o 1048576 -N 10000 -S 906011` both passed. The resulting
files kept the same sha256 after `studiofs umount` and a fresh mount, which
checks the local durability path through release and shutdown. This check is
separate from the in-mount results above.

## Other observations

* `--allow-other` works. A file created by `tests` is owned by `tests`, and
  `nobody` gets mode-bit decisions rather than a blanket kernel denial.
* Once, the first `mkdir` in the volume root right after `studiofs mount -d`
  returned `EACCES` while a repeat succeeded. Two remount attempts did not
  reproduce it, so no issue was filed.
* Stopping the mount after the adaptive fsx phase logged 1837
  `removing orphan dirty payload object without metadata reference` warnings.
  This is shutdown cleanup after truncate-heavy churn, with no visible data
  effect. It is recorded here for comparison with later runs.

## Artifacts

Raw logs stay on the host under `target/posix-conformance/<run-id>/` and are
not committed:

* `environment.txt`
* `pjdfstest-ext4/*.log`
* `pjdfstest-studiofs/` (first group run, invalidated by SFS-910)
* `pjdfstest-studiofs-pertest/results.txt`
* `fsx-<label>/` with `.fsxops`/`.fsxgood` of failing runs
* `fsx-replay/`
* `fsx-durability/`
* `diag/`: strace of `chmod::eloop_final_comp` and mount logs around the degrade and fsx phases
