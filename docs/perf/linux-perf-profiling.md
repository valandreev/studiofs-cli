# Profiling a Linux Mount With `perf`

This guide builds `perf` for a WSL2 kernel, builds a `studiofs` binary that
`perf` can symbolize, and profiles foreground metadata operations on a mounted
volume with [`tools/perf/linux-metadata-profile.sh`](../../tools/perf/linux-metadata-profile.sh).
It was first used for [SFS-905](sfs-905-linux-metadata-profile.md).

## Build `perf` for the WSL2 kernel

The distro `linux-tools` package does not match a Microsoft kernel. Build
`tools/perf` from the `microsoft/WSL2-Linux-Kernel` tag that matches
`uname -r`. For `6.6.114.1-microsoft-standard-WSL2` the tag is
`linux-msft-wsl-6.6.114.1`.

Install the build dependencies once (Ubuntu 24.04):

```bash
sudo apt-get install -y flex bison libelf-dev libdw-dev libtraceevent-dev libunwind-dev libslang2-dev python3-dev
```

Fetch only the directories `tools/perf` needs and build into `target/`:

```bash
mkdir -p target/perf-build/out
git clone --depth 1 --filter=blob:none --sparse --branch linux-msft-wsl-6.6.114.1 https://github.com/microsoft/WSL2-Linux-Kernel.git target/perf-build/kernel
git -C target/perf-build/kernel sparse-checkout set tools scripts include arch/x86 lib
make -C target/perf-build/kernel/tools/perf -j"$(nproc)" NO_LIBPYTHON=1 NO_JEVENTS=1 O="$PWD/target/perf-build/out"
target/perf-build/out/perf --version
```

The build summary must report `dwarf`, `libelf`, `libunwind`, and
`libdw-dwarf-unwind` as `on`. `O=` must name an existing directory.

## Build a symbolized `studiofs`

Use a separate target directory so the regular release build is not
invalidated. Line tables give file and line attribution; frame pointers give
cheap, reliable call graphs:

```bash
CARGO_TARGET_DIR=target/profiling CARGO_PROFILE_RELEASE_DEBUG=line-tables-only \
  RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release -p studiofs
```

`perf report` prints Rust v0 symbols mangled. Pipe reports through `c++filt`
(binutils 2.36 or newer), as the profiling script does.

## Permissions

`kernel.perf_event_paranoid=2` (the WSL2 default) allows sampling your own
processes in user space only. Kernel frames are not sampled, so `perf` alone
undercounts time spent in FUSE syscalls. The profiling script therefore takes
per-thread on-CPU time from `/proc/<pid>/task/*/stat` (user plus system time)
and uses `perf` only for user-space stacks. Lowering the setting to `1` with
`sudo sysctl kernel.perf_event_paranoid=1` adds kernel stacks.

Off-CPU waits (locks, timers, network) do not appear in a `cpu-clock` profile.
When wall time per operation is far above its on-CPU time, time the individual
syscalls from the workload, then reproduce the slow call in a `studiofs-vfs`
unit test to locate the wait.

## Thread groups

| Group | Thread names |
|---|---|
| `fuse` | `studiofs-mount` (libfuse request workers inherit the mount thread name) |
| `tokio` | `tokio-rt-worker` |
| `pg-meta-poll` | `pg-meta-poll` |
| `other` | every other thread, such as `pg-meta-notify` and tray threads |

## Run the profile

Mount a fresh volume with the profiling binary, then point the script at an
empty directory inside it and at the mount process:

```bash
export STUDIOFS_CLIENT_ROOT="$PWD/target/<run>/client"
target/profiling/release/studiofs mount <volume> "$PWD/target/<run>/mnt" --foreground &
bash tools/perf/linux-metadata-profile.sh \
  --target "$PWD/target/<run>/mnt/<volume>/bench" \
  --pid <mount-pid> --out target/<run>/profile \
  --perf target/perf-build/out/perf
```

`studiofs umount` finds the mount record through `STUDIOFS_CLIENT_ROOT`, so
export the same value before unmounting.

The script writes per phase:

- `summary.tsv`: operations, wall ms/op, and on-CPU ms/op per thread group;
- `<phase>.perf.data`: the call-graph profile of the mount process;
- `<phase>.comm.txt`: samples per thread name;
- `<phase>.pg-meta-poll.txt`: inclusive `pg-meta-poll` stacks.

After each workload the script samples until the measured thread groups stop
accruing CPU for two seconds (at most 30 seconds), so trailing replication work
is charged to the phase.
