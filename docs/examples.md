# Usage Examples

## 1. Create a New Virtual Volume (Init)

Initialize a volume using local RustFS and PostgreSQL for metadata:

```bash
studiofs init myvolume \
  --endpoint http://127.0.0.1:9000 \
  --bucket mybucket \
  --access-key studiofs \
  --secret-key studiofs123 \
  --meta postgres://studiofs:studiofs@127.0.0.1:5432
```

## 2. Mount a Volume

### On Windows

Mount as drive Z:

```bash
studiofs mount myvolume Z:
```

### On Linux / macOS

Mount to a local directory:

```bash
mkdir -p /mnt/studio
studiofs mount myvolume /mnt/studio
```

## 3. Daemon Mode (Background)

Mount a volume in the background (terminal will not be blocked):

```bash
studiofs mount myvolume Z: -d
```

To unmount a volume running in the background:

```bash
studiofs umount Z:
```

## 4. Mount an Existing Volume on a New Client (Discover)

If a volume has already been initialized on another machine, you do not need to call `init`. You only need to retrieve the latest metadata:

```bash
studiofs discover myvolume \
  --meta postgres://studiofs:studiofs@127.0.0.1:5432
```
After that, you can mount the volume:
```bash
studiofs mount myvolume Y:
```

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

If issues are found, StudioFS asks whether to repair them.

Apply repair immediately without interactive prompt:

```bash
studiofs fsck myvolume --repair
```
