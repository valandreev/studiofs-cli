# Linux Read-Only Kernel Cache Design

Date: 2026-07-18

## Goal

Give Linux mounts the same useful operating-system read caching that StudioFS
already gets from UBC on macOS, while preserving the current Direct + ASYNC_DIO
write path. Stable files opened strictly read-only must be reusable from the
Linux page cache; every write-capable open must remain direct.

## Context

The current Linux default is `LinuxFuseCachePolicy::Direct`. It sets
`fuse_file_info.direct_io` for every regular-file handle, so repeated reads of a
warm mounted file still enter StudioFS. The experimental `Kernel` policy proves
that generation-aware page-cache reuse and invalidation work, but it also asks
for FUSE writeback cache and sends writes through the buffered path. Previous
mounted evidence showed that this combined policy has unacceptable write drain
and CPU cost.

The local JuiceFS reference does not set `FOPEN_DIRECT_IO` for ordinary files.
It sets `FOPEN_KEEP_CACHE` when a reopened file is unchanged and invalidates the
inode otherwise. JuiceFS retains the OS cache by default; its Linux
`FADV_DONTNEED` path is opt-in through `JFS_DROP_OSCACHE`.

## Decision

Add a `ReadOnlyKernel` Linux FUSE cache policy and make it the default.

| Open access | `direct_io` | `keep_cache` | OS-cache participation |
|---|---:|---:|---|
| Strict read-only, first or changed generation | false | false | Enabled, replace old pages |
| Strict read-only, unchanged stable generation | false | true | Enabled, reuse pages |
| Write-only, read/write, append, or truncate | true | false | Disabled |

The existing explicit policies remain available as internal benchmark controls:

- `direct`: all regular-file handles use Direct I/O;
- `kernel`: all regular-file handles use the kernel cache and the connection may
  request FUSE writeback cache;
- `hybrid`, `read_only_kernel`, and `read_cache`: select the new production
  default.

An absent, empty, or invalid `STUDIOFS_LINUX_FUSE_CACHE_POLICY` value selects
`ReadOnlyKernel`. `STUDIOFS_FUSE_ASYNC_DIO` continues to control ASYNC_DIO for
policies that have direct handles.

## Data Flow

For a strict read-only open:

1. The Linux bridge converts the POSIX flags and marks the opened handle as
   read-only.
2. `prepare_os_cache_open` runs with `OsCacheParticipation::Enabled` and applies
   the existing generation, local-dirty-state, and active-writer checks.
3. The first valid read enters StudioFS and fills the mounted inode's Linux page
   cache.
4. A later unchanged reopen sets `keep_cache`; cached pages are served by Linux
   without a FUSE read callback.
5. Local or remote content changes continue to use the existing inode
   invalidation path, so stale pages are replaced.

For any write-capable open, the bridge uses the existing Direct + ASYNC_DIO
policy and disables OS-cache participation for that handle. The new default
does not request FUSE writeback cache.

## Components

Only `crates/studiofs-linux-fuse/src/low_level.rs` needs product changes:

- `LinuxFuseCachePolicy` represents the new default;
- `LinuxFuseOpenedFile` carries whether the POSIX open is strictly read-only;
- `SessionState::open` and `SessionState::create` classify access once;
- `complete_file_open_reply_with` selects cache participation per handle;
- `linux_fuse_open_cache_policy_for` maps the mount policy, access class, cache
  decision, and negotiated ASYNC_DIO support to FUSE file-info flags;
- connection negotiation requests ASYNC_DIO but not writeback cache for the new
  default.

No cache subsystem, eviction algorithm, dependency, public CLI option, or
cross-platform code is added.

## Error And Fallback Behaviour

This feature adds no new fallible I/O. Invalid internal environment values fall
back to the production `ReadOnlyKernel` policy. Existing open failure cleanup,
cache-open abort, handle release, and inode invalidation errors retain their
current behaviour. If the kernel does not negotiate ASYNC_DIO, direct write
handles still function synchronously as they do today.

## Validation

Automated validation must prove:

- the default and aliases select `ReadOnlyKernel`;
- the connection requests ASYNC_DIO and does not request writeback cache;
- strict read-only opens are non-direct and reuse stable cache pages;
- write-only, read/write, append, and truncate opens remain direct and never set
  `keep_cache`;
- explicit `direct` and `kernel` controls retain their old behaviour;
- the existing generation, dirty-inode, active-writer, and remote-invalidation
  tests remain green.

A mounted Linux test must write and close a file through the Direct path, read it
twice through fresh read-only handles, verify identical bytes, and prove the
second read adds zero FUSE read callbacks. It must also verify that the
connection negotiated no writeback cache.

The real-disk performance run uses `target/bench/studiofs-bench` against
`studiofs mount test`. It records per-sample throughput for a single large file,
full byte correctness, write/read completion, clean drain, and unmount. The
result is accepted only if read smoothness is no worse than the Direct baseline
and no correctness, callback, or cleanup error appears.

## Explicit Non-Goals

- Do not call `posix_fadvise(POSIX_FADV_DONTNEED)` for mounted user reads.
- Do not promote full kernel writeback-cache mode.
- Do not cache write-capable handles in this iteration.
- Do not add an application-specific DaVinci Resolve, Flame, or Nuke policy.
