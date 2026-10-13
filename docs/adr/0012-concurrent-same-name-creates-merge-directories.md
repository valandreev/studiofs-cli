# Concurrent Same-Name Creates Merge Directories And Keep The Last File

Status: accepted.

## Context

Clients work against local `redb` state and exchange metadata through one
ordered PostgreSQL event log. A delay between one client creating a name and
another client seeing it is normal. Two clients can therefore create the same
name in the same directory without either one seeing the other's entry.

Before this decision, applying `AddEntry` to base metadata overwrote
`(parent, name) -> inode`. When two clients created the same directory (the
everyday `mkdir -p` case), the directory that replayed first and every file
under it became unreachable on every client. When the second client's
directory was still in the VFS deferred namespace queue as the peer's entry
replayed, its local commit failed with `EntryExists` and the VFS abandoned the
directory and all dependent operations; applications only saw the failure at
`fsync` or `close`.

## Decision

Directories created concurrently under one name merge. Replay canonicalizes
each batch once in `RemoteApplyCoordinator::apply_decoded_batches_in_txn`
through `RedbMetaEngine::canonicalize_replay_batch_in_txn`. The directory whose
`AddEntry` replays first keeps the name (canonical); a later directory
`AddEntry` for the same name becomes an alias recorded in the local
`directory_aliases` table, and its `AddEntry` is dropped. Events that name an
alias as a parent move to the canonical directory, `SetAttr`, xattr, and ACL
events for an alias are dropped, and a `RemoveEntry` preceded in its batch by
`RemoveAttr(alias)` (an `rmdir` of the alias) is dropped so the merged
directory survives. The rules read only replicated base state, so every client
reaches the same namespace. Self batches still match the local journal with
their raw events; all appliers receive the canonical events.

Regular files created concurrently under one name resolve by last writer: the
`AddEntry` that replays later binds the name, as for any overwrite.

A VFS deferred create whose name was taken by a same-kind entry that replay
placed in base after admission commits anyway and shadows that entry locally.
The client then behaves exactly as if its create had committed first: its own
batch replays later and merges (directories) or wins (files). Local entries and
entries of a different kind still fail with `EntryExists`.

Aliases are part of the serving checkpoint. `ServingCheckpoint` format version
2 appends `directory_aliases`; version 1 bytes still decode, without aliases,
and snapshot rows accept both versions.

## Consequences

Both clients' work survives a same-name directory create on every client,
including the client whose directory became an alias. Until a client replays
its own batch, it serves its own directory; afterwards it serves the merged
directory.

Known gaps, not covered by this decision: a rename of a directory onto a name a
peer concurrently created as a directory still displaces it; a file and a
directory created under one name resolve by last writer, so a displaced
directory's children become unreachable; a `RemoveEntry` of an alias split from
its `RemoveAttr` across publication batches removes the merged directory; the
merged directory's `nlink` is exact only after its owner's next attribute event.
Clients older than checkpoint format version 2 ignore aliases in new
checkpoints and reject snapshot rows written with format version 2.
