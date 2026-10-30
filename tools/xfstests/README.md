# xfstests for the StudioFS FUSE mount

This harness runs the pinned upstream generic filesystem tests against fresh,
disposable StudioFS volumes. Use Ubuntu 24.04 on a native ext4 filesystem or
Ubuntu 24.04 in WSL2 with the checkout and all test paths stored inside the
Linux distro filesystem. Do not run the workspace, mountpoints, or artifacts
from `/mnt/c` (or another DrvFS/NTFS path).

xfstests runs as root and many tests remove files or exercise destructive
operations. Both test volumes must be dedicated to this run. Never use an
existing development or production volume, and never reuse these volume names
for ordinary testing. The remote object bucket and PostgreSQL database must be
the isolated test service configured for this work.

## Host requirements

Install the upstream Ubuntu build and runtime dependencies, plus FUSE and
ext4 utilities:

```bash
sudo apt-get update
sudo apt-get install -y acl attr automake bc dbench dump e2fsprogs fio gawk \
  gcc git indent libacl1-dev libaio-dev libcap-dev libgdbm-dev libtool \
  libtool-bin liburing-dev libuuid1 lvm2 make psmisc python3 quota sed \
  uuid-dev uuid-runtime xfsprogs sqlite3 libgdbm-compat-dev \
  libfuse3-3 fuse3
```

For WSL2, enable systemd if it is not already enabled, restart the distro, and
confirm `/dev/fuse` is available. Add the test user to the `fuse` group if the
distro requires it. Install the mount helper at `/sbin/mount.fuse.studiofs`;
the test runner and mount setup therefore need `sudo`. The helper runs the
mount with `allow_other` so xfstests' secondary accounts can traverse it; keep
the mount limited to this isolated test environment.

Create the upstream `fsgqa` and `fsgqa2` test users and groups before running
the generic suite; some cases will skip if they are absent. Follow step 6 in
the pinned checkout's `README` for the upstream account creation commands.
Verify with `id fsgqa`, `id fsgqa2`, `getent group fsgqa`, and
`getent group fsgqa2`.

Keep the checkout, `target/`, `TEST_DIR`, `SCRATCH_MNT`, and run artifacts on
ext4. Confirm the filesystem backing those paths with `findmnt -T <path>` and
`findmnt -T <path> -o FSTYPE,SOURCE,TARGET`; the type must be `ext4`. In WSL2,
keep these paths under the distro's Linux root (for example `/home/<user>`),
not under `/mnt/c`.

## Build pinned xfstests

The repository keeps the upstream checkout and build output under ignored
`target/`. Fetch the pinned revision over HTTPS and build it:

```bash
mkdir -p target/xfstests/tools
git clone https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git \
  target/xfstests/tools/xfstests
git -C target/xfstests/tools/xfstests checkout 22348afe338c0f6d540c0d7ef0db749eaa51b218
git -C target/xfstests/tools/xfstests apply "$PWD/tools/xfstests/fuse-subtype.patch"
make -C target/xfstests/tools/xfstests -j2
sudo install -m 0755 tools/xfstests/mount.fuse.studiofs /sbin/mount.fuse.studiofs
```

The local patch normalizes the reported `fuse.studiofs` type to `fuse` for
upstream's mounted-filesystem checks. Apply it only to the pinned checkout;
this is a harness compatibility patch, not a product change. If the checkout
already exists, verify `git -C target/xfstests/tools/xfstests rev-parse HEAD`
is exactly `22348afe338c0f6d540c0d7ef0db749eaa51b218` and that the patch is
applied before building or running tests. Do not use a moving branch as the
baseline. Preserve the patch hash with run evidence because the upstream
checkout has a local modification.

Build StudioFS and use the resulting absolute binary path:

```bash
cargo build -p studiofs --release -j 2
export STUDIOFS_BINARY="$PWD/target/release/studiofs"
```

## Initialize dedicated volumes

Choose two new, distinct volume names and dedicate them to xfstests. The
volume name is what xfstests places in `TEST_DEV` and `SCRATCH_DEV`; these are
not block devices. Use fresh names for every run because xfstests may
repeatedly delete the contents of the scratch volume.

Use two fresh volume names and a private run ID. Configure `TEST_DEV` and
`SCRATCH_DEV` in the local harness file, then initialize both from a root shell
after sourcing that file. This ensures init and the later root-run test harness
share the same isolated `STUDIOFS_CLIENT_ROOT` and its local state has the
right owner. The ignored `.env` supplies `STUDIOFS_S3_ENDPOINT`,
`STUDIOFS_S3_BUCKET`, `STUDIOFS_S3_ACCESS_KEY`, `STUDIOFS_S3_SECRET_KEY`, and
`STUDIOFS_TEST_PG_DSN`. Do not enable shell tracing, print the environment, or
put credentials in a command manifest. `studiofs init` takes credentials as
command-line options, so use a private terminal and retain its output in the
run directory:

## Configure and run

Copy `local.config.example` to a private file under `target/xfstests/` and edit
the two volume names and absolute mount directories to match the run. Also
replace `RUN` in the mount paths and evidence/client paths with the run ID. Each
mount directory's final path component must equal its volume name (for example
`/srv/sfs907/RUN/mnt/$TEST_DEV` and `/srv/sfs907/RUN/mnt/$SCRATCH_DEV`); StudioFS uses
that identity when resolving the mountpoint. Put mount directories below a
path that xfstests' `fsgqa` accounts can traverse. A home directory with mode
750 blocks these accounts even when the mount itself allows access. Keep all
these mount paths on ext4. The template also sets the binary, isolated client
root, artifact root, mount options, and timeout. `XFSTESTS_RUN` is the evidence
root; the runner creates a new result directory below it for each invocation.
Set `FSTYP=fuse` and `FUSE_SUBTYP=.studiofs`; the upstream mount path then
invokes `mount -t fuse.studiofs`. The mount helper accepts the ordinary source,
target, and `-o` arguments from `mount` and starts the configured StudioFS
binary in the foreground. The helper waits at most 120 seconds for the mount
table entry and writable root before returning success.

Set these variables in the same private shell before invoking the runner:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-xfstests"
CONFIG="$PWD/target/xfstests/local.config"
SECRET_ENV="$PWD/.env"
cp tools/xfstests/local.config.example "$CONFIG"
# Edit $CONFIG: use fresh TEST_DEV/SCRATCH_DEV names and replace RUN in its
# mount, XFSTESTS_RUN, STUDIOFS_CLIENT_ROOT, and binary paths with real paths.
sudo install -d -m 0755 "/srv/sfs907/$RUN_ID/mnt"
```

Confirm `/srv/sfs907/$RUN_ID/mnt` is on ext4 and searchable by the `fsgqa`
accounts. This location is outside the home directory so those accounts can
traverse to the mounts. The helper creates the volume-specific directories.

Initialize both dedicated volumes as root using the same config and client
root that the test runner will use:

```bash
sudo bash -c '
  set +x
  set -e
  source "$1"
  mkdir -p "$XFSTESTS_RUN" "$STUDIOFS_CLIENT_ROOT"
  set -a
  source "$2"
  set +a
  "$STUDIOFS_BINARY" init "$TEST_DEV" \
    --endpoint "$STUDIOFS_S3_ENDPOINT" --bucket "$STUDIOFS_S3_BUCKET" \
    --access-key "$STUDIOFS_S3_ACCESS_KEY" --secret-key "$STUDIOFS_S3_SECRET_KEY" \
    --meta "$STUDIOFS_TEST_PG_DSN" >"$XFSTESTS_RUN/init-test.log" 2>&1
  "$STUDIOFS_BINARY" init "$SCRATCH_DEV" \
    --endpoint "$STUDIOFS_S3_ENDPOINT" --bucket "$STUDIOFS_S3_BUCKET" \
    --access-key "$STUDIOFS_S3_ACCESS_KEY" --secret-key "$STUDIOFS_S3_SECRET_KEY" \
    --meta "$STUDIOFS_TEST_PG_DSN" >"$XFSTESTS_RUN/init-scratch.log" 2>&1
' _ "$CONFIG" "$SECRET_ENV"
```

The xfstests runner must run as root. Source the config in that root shell so
the needed environment reaches the runner without relying on `sudo` to retain
the caller's environment. The service credentials are no longer needed for
the test cases. `tools/xfstests/exclude` is currently empty because
the pinned upstream FUSE probes self-skip unsupported features. Run the full
generic suite or named cases with the repository runner:

```bash
sudo bash -c 'source "$1"; exec tools/xfstests/run.sh "$2" "$XFSTESTS_RUN/smoke" generic/001 generic/002 generic/005' \
  _ "$CONFIG" "$PWD/target/xfstests/tools/xfstests"
sudo bash -c 'source "$1"; exec tools/xfstests/run.sh "$2" "$XFSTESTS_RUN/generic"' \
  _ "$CONFIG" "$PWD/target/xfstests/tools/xfstests"
```

The runner executes each selected test in its own check invocation with a
300-second timeout and retains per-case output, upstream result files, status,
and elapsed time beneath the new result directory. Use a fresh result path on
each invocation. A timeout, missing report, or
interrupted case is not a pass. Inspect the retained case records and logs
before summarizing results. Do not run xfstests' whole-suite command directly:
it bypasses the harness's per-test timeout and evidence layout.

The upstream FUSE `_scratch_mkfs` path does not format a volume. It mounts the
scratch volume, removes its files, and unmounts it. Tests that require actual
reformatting, block-device geometry, or unsupported mount options must remain
excluded or will report not-run/failure; do not configure a fake mkfs command.

## Cleanup and evidence

Unmount both mounts and confirm the foreground StudioFS processes have exited
after the run. Preserve the run directory, logs, per-case verdicts, product
binary hash, xfstests revision, kernel/FUSE versions, mount options, and
exclusion list. Artifacts and client state belong under ignored `target/` and
must not be committed. Treat initialized remote volumes as disposable test
data; do not delete or reuse any volume until its retained evidence is no
longer needed.

This procedure describes the Ubuntu/WSL2 setup. A run on another Linux VM is
useful evidence but must be labeled with its actual host and kernel; it does
not establish WSL2 coverage.
