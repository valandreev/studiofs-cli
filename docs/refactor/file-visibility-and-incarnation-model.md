# File Visibility And Incarnation Model

## Purpose

This document fixes the target behavior for `feat/new-meta-engine`.

The main goal is to remove the coupling between foreground filesystem I/O and
background S3 upload. The client that writes a file must be able to read it
immediately after local durable accept, even if the last object upload to S3
finishes much later.

This document is the contract. The refactor roadmap must follow it exactly.

## External Behavior

### Writer-visible behavior

1. `write()` updates the open-handle working set.
2. `flush()` or `close()` durably accepts the data into the local ingress WAL.
3. After local durable accept, the same mounted client can read the file
   immediately.
4. This read-after-write behavior must not depend on:
   - S3 upload completion
   - metadata publish to redb
   - metadata replication to PostgreSQL

### Remote-visible behavior

Other clients do not need exact growth tracking.

They only need to understand one coarse fact:

1. a new file is still uploading
2. an existing file is being updated
3. the current committed version is ready

Other clients are not promised:

1. exact uploaded byte count
2. exact committed byte count while upload is in progress
3. the exact moment when a still-uploading new file becomes readable

### Atomicity rule

Remote visibility is file-version atomic, not block-atomic.

1. A new file is not exposed to remote readers as a partial file.
2. An update to an existing file does not replace the old committed version
   block by block.
3. The new version becomes remote-visible only after a full incarnation commit.

## Core Entities

### Inode

`inode` is the logical file identity.

An inode owns:

1. `committed_incarnation: Option<IncarnationId>`
2. `pending_incarnation: Option<IncarnationId>`
3. `namespace_state: linked | orphaned | deleted`

`inode` does not depend on path or name.

### Dentry

`dentry` is the namespace mapping from parent/name to inode.

`rename()` changes only dentry state.

`rename()` must not:

1. create a new inode
2. create a new incarnation
3. restart upload
4. invalidate already accepted data

### Incarnation

`incarnation` is one concrete file-content version.

Rules:

1. Each inode has at most one `committed_incarnation`.
2. Each inode has at most one `pending_incarnation`.
3. `pending_incarnation` is locally newer than `committed_incarnation`.
4. `committed_incarnation` is immutable.

### Extent Record

`extent_record` is an immutable accepted record for a file byte range inside a
pending incarnation.

Each extent record has:

1. `extent_record_id`
2. byte range
3. local WAL/storage location
4. liveness flag
5. optional object reference after materialize/upload

When a range is overwritten:

1. a new extent record is created
2. the overlapping old pending extent records are invalidated
3. untouched extents remain valid

## Visibility Planes

### Local Accepted View

Visible to the writer's mounted client.

Sources:

1. open-handle dirty state
2. ingress WAL
3. live ingress index
4. pending incarnation extent map

Properties:

1. immediate read-after-write after `flush()` or `close()`
2. survives restart after WAL replay
3. independent from S3 availability until local budget is exhausted

### Remote Committed View

Visible to other clients.

Source:

1. committed incarnation manifest

Properties:

1. immutable
2. fully readable
3. replaced only by an atomic committed-incarnation switch

### Remote Upload Status

Visible to other clients as a coarse state only.

This is metadata, not a promise of readability or exact progress.

Allowed states:

1. `uploading`
2. `updating`
3. `ready`

## upload_state Derivation

`upload_state` must be derived from inode state instead of being manually
assembled from unrelated flags.

### `ready`

Conditions:

1. dentry is visible
2. `committed_incarnation.is_some()`
3. `pending_incarnation.is_none()`

Meaning:

1. the committed version is the current remote version
2. remote readers can read it normally

### `uploading`

Conditions:

1. dentry is visible
2. `committed_incarnation.is_none()`
3. `pending_incarnation.is_some()`

Meaning:

1. file creation has started
2. there is no remote-readable version yet

### `updating`

Conditions:

1. dentry is visible
2. `committed_incarnation.is_some()`
3. `pending_incarnation.is_some()`

Meaning:

1. the old committed version stays readable
2. a newer local version is being materialized and uploaded

### Hidden

Conditions:

1. dentry is not visible

Meaning:

1. no public `upload_state`
2. orphan cleanup may still exist internally

## Pending Incarnation State Machine

The pending incarnation lifecycle is:

1. `Draft`
2. `Accepted`
3. `Materializing`
4. `Uploading`
5. `ReadyToPublish`
6. `Committed`
7. `Canceled`
8. `Reaped`

### State meanings

#### `Draft`

1. writes exist only in open-handle state
2. not yet durably accepted

#### `Accepted`

1. data is persisted in the ingress WAL
2. local read-after-write is guaranteed

#### `Materializing`

1. the system is building the live extent map and object work
2. this is background work

#### `Uploading`

1. at least one live extent is in upload
2. upload retries and batching are internal details

#### `ReadyToPublish`

1. all live extents for the pending incarnation are resolved
2. each live extent either:
   - already has an uploaded object reference
   - or is reused from the committed base manifest
3. the new manifest is complete

#### `Committed`

1. the pending incarnation has been atomically published
2. it becomes the new committed incarnation

#### `Canceled`

1. further uploads are forbidden
2. stale completions must be ignored
3. eventual orphan object cleanup is delegated to GC

#### `Reaped`

1. cancellation cleanup is complete
2. WAL/index references may be compacted away

## Transition Rules

### Create and first write

1. create inode
2. create `pending_incarnation`
3. initial pending state is `Draft`
4. `committed_incarnation = None`
5. `upload_state = uploading`

### Flush or close

1. append accepted records to the ingress WAL
2. update live ingress index
3. `Draft -> Accepted`
4. local reads are now allowed without S3

### Background materialize

1. `Accepted -> Materializing`

### Upload start

1. `Materializing -> Uploading`
2. if all extents are already reusable and no upload is needed, this state may
   be skipped

### Manifest complete

1. `Materializing -> ReadyToPublish`
2. or `Uploading -> ReadyToPublish`

### Publish

1. `ReadyToPublish -> Committed`
2. `committed_incarnation = pending_incarnation`
3. `pending_incarnation = None`
4. `upload_state = ready`

### Patch existing committed file

1. if no pending incarnation exists, create a new pending incarnation using the
   current committed manifest as base
2. `upload_state = updating`
3. changed ranges create new extent records
4. overlapping pending extent records are invalidated
5. unchanged extents are reused from the committed base

### Rename

1. update dentry only
2. do not touch inode content identity
3. do not touch pending or committed incarnation IDs
4. do not restart upload

### Unlink

1. remove dentry immediately
2. if the inode has a pending incarnation, move it to `Canceled`
3. queued uploads for that pending incarnation must stop
4. stale upload completions must not publish anything
5. if open handles remain, namespace state becomes `orphaned`
6. otherwise namespace state becomes `deleted`

## Read Matrix

### Same mounted client

Read precedence:

1. open-handle dirty state
2. pending incarnation extents from local accepted view
3. committed incarnation extents

Rules:

1. accepted local data must be readable before upload finishes
2. the client must not fetch its own accepted data from S3

### Other clients

If `upload_state = uploading`:

1. dentry is visible
2. the file is marked as uploading
3. exact size/progress is not promised
4. open/read of the new version must not return partial data

If `upload_state = updating`:

1. current committed version remains readable
2. the file is marked as updating
3. pending newer data remains hidden remotely

If `upload_state = ready`:

1. committed version is readable normally

## Edge Cases

### Case 1: write 50GB, then unlink immediately

Expected behavior:

1. file disappears from namespace immediately
2. pending incarnation becomes `Canceled`
3. queued uploads stop
4. stale in-flight completions are ignored
5. orphan uploaded objects are cleaned by GC later

### Case 2: write 50GB, then patch a few blocks

Expected behavior:

1. a new pending incarnation is based on the committed one, or the current
   pending incarnation is updated if it already exists
2. only touched extents are replaced
3. untouched extents are reused
4. the writer reads the latest local version immediately
5. remote readers still see the old committed version until atomic publish

### Case 3: write 50GB, then rename immediately

Expected behavior:

1. only dentry changes
2. upload continues unchanged
3. no data invalidation happens
4. no duplicate upload is created

## Non-Goals

The new architecture does not promise:

1. remote per-block file growth visibility
2. remote partial readability of an uploading new file
3. backward compatibility with old ephemeral disks

## Hard Invariants

1. Foreground filesystem I/O must not wait on S3 upload completion.
2. Local accepted data must remain readable after restart.
3. `rename()` is namespace-only.
4. `unlink()` cancels the whole pending incarnation.
5. Partial overwrite invalidates only touched extents.
6. Remote visibility switches at file-incarnation granularity.
7. Background batching is an internal optimization only.
