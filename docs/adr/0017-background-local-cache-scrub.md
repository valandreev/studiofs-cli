# The Mounted Process Scrubs The Local Payload Cache And Refetches Corrupt Objects

Status: proposed (SFS-1071). Implementation is tracked in the Linear project
"[0.2.4]" as SFS-1117, SFS-1118, SFS-1119, and SFS-1120.

## Context

The local payload cache holds one file per object under
`payload/objects/<s0>/<s1>/<id>.object`, indexed by the cache redb
`payload_objects` table. The files hold raw payload bytes without the STFS
header. Nothing verifies those bytes when they are read back: `get` and
`read_into` return whatever is on disk, and a missing file only removes the
orphan index row. Bit-rot or a damaged sector on the local disk therefore
reaches applications as wrong data, not as an error.

The checksum the cache would need already exists on both paths that make an
entry clean:

- A full-object download decodes the STFS object with `read_object`, which
  checks the header XXH64 against the raw payload before the bytes are cached.
  Range reads are never cached.
- The upload worker builds the STFS object from the cached bytes, and that
  header holds the XXH64 of the raw payload.

Dirty entries (written locally, not yet uploaded) have no copy anywhere else
and no checksum.

Remote integrity (`SFEC` authentication and STFS XXH64) is already checked on
every remote read and reported as `EIO`.

## Decision

### Scope

Only the local payload cache is scrubbed. The goal is protection against
local disk faults. Remote objects, metadata, and snapshots are out of scope.

### Checksum in the cache index

- Each `payload_objects` entry gets an optional `xxh64` of the exact cached
  bytes. The entry version is bumped; existing rows decode with `None` and are
  not rewritten.
- The value is recorded on a full-object download and on the transition from
  Dirty to Uploaded, from the XXH64 those paths already compute. No hashing is
  added to the read or write hot path.
- Any rewrite of an object's bytes clears or replaces the value.
- Entries without a checksum are skipped. They gain one when they are fetched
  again, or leave the cache through normal eviction. There is no fallback to
  the upload ledger.

### Background pass

- Each mount runs one owned background task. It is joined on unmount.
- A pass walks `payload_objects` in object id order. The cursor (last object
  id and pass start time) is stored in the cache index `counters` table, so a
  restart continues the pass.
- Defaults: enabled, one full pass every 7 days, at most 20 MiB/s and 200
  read operations per second. The knobs follow the existing settings pipeline
  (persisted `studiofs settings` default, then mount flag).
- The scrub yields while foreground local cache IO is active. The signal comes
  from the `QosController` foreground local IO permits and is awaited through
  a notification, never a sleep poll.

### Detection

An entry is corrupt when its XXH64 does not match, its file is missing, its
file size differs from `payload_len`, or reading it fails with an I/O error.
All four are handled the same way.

### Repair

- The scrub reads and hashes without the payload mutation lock.
- On a mismatch it calls `evict_corrupt`. Under the per-object mutation
  stripe, that call re-reads the index entry and the file and evicts only if
  all of the following hold:
  - the entry still matches what the scrub observed;
  - the entry still passes the budget-eviction predicate: `Uploaded`,
    `evictable`, not runtime-pinned, `upload_state == None`;
  - the corruption is confirmed under the lock.
- Removal uses the existing removal intent, so crash recovery is unchanged.
- After eviction the object is fetched again right away through the prefetch
  fetch class and the shared `PayloadThrottle`, with the normal fetch retries.
- Entries that changed or are not eligible are left alone and retried on the
  next pass.

### Invariants

- The scrub never reads, evicts, or rewrites Dirty, Pending, or Uploading
  entries. Local-only data is never at risk from the scrub.
- An entry that changed after the scrub read it is never evicted.
- The scrub does not contact PostgreSQL, and it adds no work to platform
  callbacks.

### Failure of the refetch

If the refetched object fails remote verification, or the fetch fails after
its retries, the entry stays evicted. The scrub logs an error with the object
id and increments a counter. A later foreground read of that range returns
`EIO`, as it does today.

### Reporting

- The scrub is silent for users.
- Each detected corruption logs `warn!` with the object id and kind.
- Counters for checked, corrupt by kind, refetched, and refetch failures, the
  last completed pass time, and the current progress are added to the
  diagnostics `RuntimeSnapshot` as an additive field. `studiofs status` shows
  them in one line. v5 clients keep working.

### Rejected alternatives

- **Remote scrub (`HEAD` existence or full `GET` verification).** It costs S3
  egress and requests, needs coordination between mounts and a resumable
  shared cursor, and protects against a failure the object store already
  guards against. Remote corruption is still detected on every read.
- **Bad-object quarantine ledger, janitor hold, and damaged-file marking.**
  `SFEC` binds each object to its storage key, so an object cannot be moved to
  a quarantine prefix. A ledger, a janitor hold, and a persistent damaged flag
  only make sense together with remote scrub.
- **A manual `studiofs scrub` command.** It is not needed for a background
  safety net, and a second process cannot open the cache index while it is
  mounted.
- **Verifying on every cache read.** It would hash a whole object (up to
  4 MiB) for every cache hit, including small partial reads. This can be
  studied separately with benchmarks.
- **Reading under the mutation lock.** Holding the stripe for a 4 MiB read and
  hash would delay foreground writes to the same object. The optimistic read
  takes the lock only in the rare mismatch case.

## Consequences

Local disk faults in clean cache entries are found within one pass interval
and repaired from S3 without user action. Repeated findings in the counters
point to a failing disk.

Dirty data has no protection. A fault in a not-yet-uploaded payload is still
uploaded or read as written.

Entries cached before the upgrade are not checked until they are fetched
again or evicted.

A full pass reads the whole clean cache once per interval: a 1 TB cache at
20 MiB/s takes about 14 hours of scrub time per week.

A refetch costs one object download per corrupt entry, under the user's
download limit.

Acceptance: a mounted `studiofs-e2e` scenario on macFUSE and Linux FUSE, plus
WinFSP when the environment is available. It flips a byte in a cached object,
deletes one, and truncates one, then shows that all three heal and read back
the original bytes, and that a dirty file is untouched.
