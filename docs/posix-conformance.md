# POSIX Conformance Baseline (Linux FUSE)

## Latest run

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

fsx was not rerun; no fsx result is claimed for this product revision.
Non-WSL Linux validation was not performed.

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
