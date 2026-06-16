# SFS-514 Linux FUSE Concurrent Dispatch Audit

## Decision

StudioFS should not make the fuser 0.17.0 upgrade the first SFS-514
prototype. The selected approach is to keep fuser 0.16 for now and extend the
existing callback-level async bridge with owned async replies for metadata
callbacks behind `STUDIOFS_FUSE_ASYNC_METADATA`.

This keeps the current mount lifecycle, notifier registration, mount error
behavior, deterministic shutdown, and existing async write barriers intact while
removing the most visible head-of-line blocking surfaces: slow read, slow
getattr, and slow readdir.

## fuser Audit

StudioFS currently depends on fuser 0.16. In that version the Linux session
runner still exposes the synchronous `Filesystem` callback shape that StudioFS
implements today, and StudioFS already uses callback-level async work for owned
read/write replies.

The latest audited fuser release is fuser 0.17.0, published on 2026-02-14.
It adds several SFS-514-relevant capabilities:

- `Config::n_threads` for multiple event loops per session.
- `FUSE_DEV_IOC_CLONE` support for cloned Linux FUSE device fds.
- Filesystem trait methods use &self and mounted filesystems must be
  `Send + Sync + 'static`.
- An experimental `AsyncFilesystem` API.
- Structured `Config` mount APIs and typed request/reply API changes.

The upgrade is useful but not a small dispatcher patch. It is a breaking API
migration across every StudioFS Linux FUSE callback, and it moves lifecycle
risk into the same area that owns unmount, destroy ordering, notifier
registration, and mount error propagation. That makes callback-level async the
safer route for the SFS-514 prototype.

Decision summary: callback-level async is the safer route.

## Prototype Scope

The opt-in prototype should extend `STUDIOFS_FUSE_ASYNC_READ` with
`STUDIOFS_FUSE_ASYNC_METADATA`:

- `read` remains on the existing owned async read path.
- `getattr` uses an owned async reply and `StudioVfs::getattr_async`.
- `readdir` uses an owned async reply and async directory entry resolution.
- `flush`, `fsync`, and `release` keep waiting on the existing async write
  barriers before local durability boundaries.
- Mount shutdown with pending replies relies on the existing reply guards:
  dropped guards complete with `EIO` instead of leaving kernel requests
  unanswered.

The prototype proves that metadata operations during slow reads do not wait for
the slow read callback to finish. It also proves the reverse: slow getattr or
slow readdir work is scheduled away from the fuser dispatch callback before the
reply is ready.

## Stress Tests

Focused validation should cover:

- parallel reads where one slow read is pending and another read can complete;
- metadata operations during slow reads, especially getattr and readdir;
- release/flush ordering while async read and metadata replies are pending;
- mount shutdown with pending replies, including reply guard fallback behavior.

The mount-level tests should stay opt-in or ignored where they require a real
Linux FUSE mount. Unit tests should cover reply guards, runtime option parsing,
pending counter behavior, and task-scheduled callback behavior without requiring
`/dev/fuse`.

## Migration Risks

The migration risks are concentrated in the fuser 0.17 API and lifecycle
surface.

The fuser 0.17.0 path remains attractive after the callback prototype lands,
but it should be handled as a separate migration:

- adapt StudioFS from `&mut self` callbacks to `&self`;
- audit shared mutable state for `Send + Sync + 'static`;
- migrate mount options from `Vec<MountOption>` to `Config`;
- validate cloned fd behavior and `Config::n_threads` on native Linux;
- repeat shutdown, notifier, mount error, flush/release, and async write
  barrier tests before making threaded dispatch default.

Until that migration is proven, callback-level owned async replies give
StudioFS the smallest Linux concurrency improvement with the least lifecycle
blast radius.
