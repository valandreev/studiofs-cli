# Usage Examples

## 1. Create a New Virtual Volume (Init)

Initialize a volume using loopback-only local RustFS and PostgreSQL for
metadata:

```bash
studiofs init myvolume \
  --endpoint http://127.0.0.1:9000 \
  --bucket mybucket \
  --access-key studiofs \
  --secret-key studiofs123 \
  --meta "postgres://studiofs:studiofs@127.0.0.1:5432?sslmode=disable"
```

Production endpoints should use HTTPS S3 transport and TLS-capable PostgreSQL
DSNs:

```bash
studiofs init myvolume \
  --endpoint https://s3.example.com \
  --bucket mybucket \
  --access-key YOUR_ACCESS_KEY \
  --secret-key YOUR_SECRET_KEY \
  --meta "postgresql://studiofs:secret@db.example.com/studiofs?sslmode=require"
```

## 2. Mount a Volume

### On Windows

Mount directly to a drive letter (for example `Z:`):

```bash
studiofs mount myvolume Z:
```

### On Linux / macOS

Mount to a base directory (`/mnt/studio` becomes `/mnt/studio/myvolume`):

```bash
studiofs mount myvolume /mnt/studio
```

## 3. Daemon Mode (Background)

Mount a volume in the background (terminal will not be blocked):

```bash
studiofs mount myvolume Z: -d
```

Mount with Pyroscope profiling enabled:

```bash
STUDIOFS_PROFILE=pyroscope \
STUDIOFS_PYROSCOPE_URL=http://localhost:4040 \
STUDIOFS_PYROSCOPE_TAGS=team=vfx,cache_contract=fast-local-authority \
studiofs mount myvolume Z: --profile
```

Mount with a small explicit IO tuning surface:

```bash
studiofs mount myvolume Z: \
  --cache-size 500GB \
  --read-buffer 4GB \
  --readahead 256 \
  --prefetch 192
```

Use these flags for the main operator-facing data-cache and IO controls. The
broader `STUDIOFS_*` runtime matrix is still accepted only as
developer/transitional compatibility, and metadata-cache internals remain on
strong built-in defaults.

Mount with a non-local S3 endpoint:

```bash
studiofs mount myvolume Z: --endpoint https://minio.internal.example:9000
```

Plain HTTP endpoint overrides are allowed when the operator explicitly writes
the `http://` scheme:

```bash
studiofs mount myvolume Z: --endpoint http://192.168.1.20:10000
```

To unmount a volume running in the background:

```bash
studiofs umount Z:
```

## 4. Mount an Existing Volume on a New Client (Discover)

If a volume has already been initialized on another machine, you do not need to call `init`. You only need to retrieve the latest metadata:

```bash
studiofs discover myvolume \
  --meta "postgres://studiofs:studiofs@127.0.0.1:5432?sslmode=disable"
```
After that, you can mount the volume:
```bash
studiofs mount myvolume /mnt/shared/projects
```

On Windows, use a free drive letter (for example `Z:`) instead of a folder path.

## 5. Clean Up Old Objects (Janitor)

See which objects are no longer used (dry-run):

```bash
studiofs janitor myvolume --dry-run
```

Run actual cleanup (delete up to 500 old objects):

```bash
studiofs janitor myvolume --max-deletes 500
```

## 6. Create and List Snapshots

Create an instant local snapshot (root inode by default):

```bash
studiofs snapshot myvolume create
```

List snapshots:

```bash
studiofs snapshot myvolume list
```

## 7. Mount a Historical Snapshot as Read-Only

```bash
studiofs mount myvolume Z: --snapshot-id <snapshot-uuid>
```

## 8. Run Metadata FSCK

Generate a read-only integrity report:

```bash
studiofs fsck myvolume
```

Report includes:
- dangling dentries
- orphan inodes
- `nlink` mismatches
- pending/deferred undo state

If issues are found, StudioFS asks whether to repair them.

Apply repair immediately without interactive prompt:

```bash
studiofs fsck myvolume --repair
```

Repair mode currently:
- replays undo logs
- removes dangling dentries
- removes orphan regular/symlink inodes and schedules their payload objects for GC
- leaves orphan directories / hardlink-like orphan cases and `nlink` mismatch
  entries as report-only

## 9. Benchmarks

Run a JuiceFS-style mounted filesystem benchmark against an already mounted
StudioFS path:

```bash
studiofs bench /mnt/studio/myvolume \
  --block-size 4MB \
  --big-file-size 1GB \
  --small-file-size 128KB \
  --small-file-count 1000 \
  --threads 4 \
  --json-output reports/studiofs-bench.json
```

On Windows, pass the mounted drive or directory:

```powershell
studiofs bench Z:\ --block-size 4MB --big-file-size 1GB --threads 4
```

The command must run inside an active StudioFS mount. It does not fall back to
the legacy direct cache benchmark.

