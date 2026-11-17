# Logical volume quota

SFS-1058 defines an optional maximum for logical file data in one volume. It is an admission limit for each client's locally authoritative view, not a strict cluster-wide cap or an S3 spending limit. A client may accept writes while disconnected, and two clients can each admit growth using stale peer usage; after replication their aggregate logical usage can exceed the configured maximum. Writes must not wait for PostgreSQL or S3 to decide admission.

## What is charged

Charge the logical length of each live regular-file inode once. A sparse file is charged by its logical length, including holes, rather than allocated blocks. Extending a file charges only the positive difference between its previous logical length and the new length; overwriting bytes within its existing length adds no charge. Truncation releases the removed logical length. Directories and symlinks do not consume this file-data quota.

Hard links are names for one inode and therefore count that inode once, regardless of link count. Removing one name while another remains does not release usage. After final unlink, a locally open file stays charged until its last open handle closes and the local orphan is reclaimed. Appending through that handle uses the same admission check as appending through a name. On restart, recover or reclaim durable orphans before releasing their charge.

Recoverable trash entries remain reachable files and stay charged at their logical length until purge or retention expiry actually removes them. Restoring a trashed entry transfers it back to the live namespace without charging it a second time. Snapshots may retain old physical payloads after logical files shrink or disappear; those retained versions do not count toward this logical quota. Consequently quota usage does not predict provider billing or physical storage retained by snapshots, encryption, obsolete objects, or replication.

## Admission and reporting

Maintain a local aggregate of charged logical lengths. Update it atomically with each durable size or lifecycle change, including replay. Reserve projected EOF growth by inode before accepting bytes. Admission and reservation must be atomic across inodes on the same client. Transfer a reservation to durable usage without an uncharged interval or double charge. Normal writes and capacity queries must not scan the inode table. Recovery may rebuild the aggregate from durable state.

An absent limit disables admission checks. A configured limit of zero permits no growth. For configured quotas, byte-level status reports the exact limit and free bytes, computed as the limit minus locally charged usage, saturated at zero. Block-based APIs round total and free bytes down independently to whole reporting units. Quota rejection uses ENOSPC on POSIX and STATUS_DISK_FULL on Windows.

Admission uses the local authoritative logical usage plus growth already accepted by open writers but not yet reflected in committed metadata. Concurrent handles on one client share that reservation view, so they cannot independently spend the same free bytes. A write or metadata operation that fails releases only its own uncommitted growth; a successful shrink, deletion or purge reduces usage when its local authority commit takes effect. Restart and replay rebuild usage and outstanding reservations from durable local authority without counting replayed operations twice.

The optional policy is volume-wide and persisted through the supported metadata/configuration path. A stale peer must not interpret an absent or older policy as permission to disable enforcement; policy version/propagation behavior is part of compatibility. Authorized policy reductions may put current usage over the new cap. In that state, reported free and available bytes are zero, reads and non-growing overwrites continue, and shrink, deletion, purge, flush/fsync and shutdown remain available. Already accepted data is never discarded to enforce a later policy change.

Each client enforces against locally known usage and policy. Simultaneous growth by separate clients can overshoot until shared metadata converges; tests and user-facing status must state this explicitly. Bridge errors report a consistent quota/disk-full result. `USED_SPACE`, which tracks referenced payload objects, is not the logical quota counter.

## Verification boundary

Focused local tests must cover accounting and reservations. A registered two-client mounted scenario must demonstrate both local enforcement and permitted aggregate soft overshoot: isolate peer propagation, have each client accept growth that fits its local remaining quota, reconnect/allow replication, then observe both files and aggregate logical usage above the cap. Synchronize on observable peer state with bounded polling. Do not assert a strict global cap. Report mounted/native validation separately from focused tests.
