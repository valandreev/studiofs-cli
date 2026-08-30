# SFS-668: Define the macFUSE FSKit backend contract and record the legacy macOS performance baseline

## Purpose and scope

This document is the checked-in source of truth for the macFUSE FSKit backend
contract and the legacy macOS baseline required before SFS-669, SFS-671, and
SFS-672. It records the contract and the reproducible measurement procedure;
it does not implement an FSKit provider, change metadata or S3/cache semantics,
or remove the production backend. The preflight did require a narrow local VFS
release/read correctness repair, recorded below; it adds no foreground remote
wait.

The selected host for this baseline is the current Apple Silicon Mac:

```text
macOS 26.5.2 (Darwin 25.5.0, arm64)
macFUSE 5.3.3
mount point: /Volumes/studiofs/test
backend selector: STUDIOFS_MOUNT_BACKEND=macfuse
```

The repository's older platform note names macFUSE 5.2.0 as a previously
validated version. This issue pins the installed 5.3.3 version for its
baseline and requires future A/B runs to use the same host, commit, mount
options, and macFUSE version. A version change invalidates the comparison.

## Progress

- 2026-08-29: Read SFS-668 and its SFS-669/SFS-671/SFS-672 dependencies in
  Linear; no issue comments were present.
- 2026-08-29: Traced the checked-in legacy lifecycle from
  `mount_with_request` through `Session::mount_with_ops`, the low-level C
  callback table, `CallbackState`, `Request`, `StudioVfsAdapter`, and
  `Reply`; inspected the mounted benchmark engine and validation scripts.
- 2026-08-29: Queried the macFUSE documentation and the installed
  `MFMount.framework` headers. The installed package is macFUSE 5.3.3 and the
  host supports the FSKit backend requirements.
- 2026-08-29: Mounted the real test volume at `/Volumes/studiofs/test` using
  the unchanged legacy backend. The mount was reachable and S3/PostgreSQL
  startup completed.
- 2026-08-29: The contract test was first run RED because this document did
  not exist. It is the executable checklist for the required contract and
  baseline surfaces.
- 2026-08-29: A first 64MB/10-file release smoke run returned a deterministic
  content mismatch. Two subsequent release smoke runs (16MB/4 files and
  64MB/10 files) passed. The failed run is retained as a preflight failure;
  it is not included in the baseline and any failed correctness run remains a
  hard gate.
- 2026-08-29: The reference 1GB/1000-file workload was started once and
  stopped after it reached 473 temporary files because the end-to-end mounted
  write path was too slow for five repetitions. Live observability does not
  attribute that foreground delay to remote PostgreSQL: create/open stayed at
  roughly 0.045/0.060 ms, while FUSE write and writer-stack recording were
  roughly 108 ms per callback. Its partial output is not a baseline result.
  The temporary tree was removed by exact path.
- 2026-08-29: Corrected the initial diagnosis. Deferred namespace creation
  enqueues local metadata and schedules a background drain; `metadata_commit`
  measures the local metadata apply path, not a PostgreSQL round trip. Remote
  PostgreSQL pushes run in the background replication poller.
- 2026-08-29: Found and fixed a benchmark-only correctness defect: sequential
  stream writes included `stream_id` in the deterministic payload seed, while
  the post-upload `first-read` validator did not. The defect was masked by
  `threads=1` because stream zero had the same seed as the validator. The
  seed is now derived only from the run and file identity, with a regression
  test. No VFS, metadata, S3, or cache behavior changed.
- 2026-08-29: After the harness fix, the 3-file/`threads=2` probe and the
  previous 33-file/`threads=4` concurrency shape passed all read validation,
  including `first-read`. The measured write rates (1.05 and 1.07 MiB/s) are
  diagnostic only and remain below the 1000 MiB/s write-path target.
- 2026-08-30: Verified that the mounted daemon is `target/debug/studiofs
  mount test`, while the benchmark client is `target/release/studiofs`.
  Therefore the current write-rate measurements are not release-daemon
  baseline evidence; the test volume must be remounted with the exact release
  binary before performance rows can be accepted. The current mount was left
  untouched.
- 2026-08-30: Remounted `/Volumes/studiofs/test` with the release daemon and
  verified PID 69020 and the exact release command before collecting results.
- 2026-08-30: `studiofs-bench` passed a 64MiB/100-file mounted read/write
  control with both batch fsync and `--no-batch-fsync`. Its reports are
  `target/perf/sfs-668-writer-stack-diagnosis/studiofs-bench-report-1788039123-0.json`
  and `target/perf/sfs-668-writer-stack-diagnosis/studiofs-bench-report-1788039141-0.json`.
- 2026-08-30: The fixed 256MiB/100-small-file/`threads=1` release `devbench`
  matrix completed five times with clean correctness and `first-read` target
  validation. Raw artifacts are under
  `target/perf/sfs-668-legacy-256m/macos-bench-validation-20260829-213240`.
  This is an operational matrix result, not a complete public-benchmark
  result, because `devbench` does not serialize its extra public read-phase
  telemetry into `public_bench`.
- 2026-08-30: The full `studiofs bench` public workload failed its read
  preflight on `benchmark_file_001080.bin` (~4.2GiB): offset 0 returned
  `0x00` instead of the deterministic byte. A 1GiB/1000-file `devbench`
  probe failed the same way on its large file. Both failures are excluded
  from baseline data.
- 2026-08-30: A 1GiB single-file `studiofs-bench` control passed with both
  batch fsync and `--no-batch-fsync`. Those paths call `sync_all`, whereas the
  mounted `devbench` writer only calls `tokio::fs::File::flush()` before the
  handle is dropped. This isolates the observed failure to the local
  materialization/release visibility boundary, not PostgreSQL latency.
- 2026-08-30: A missing metric is recorded as unavailable, never as zero.
- 2026-08-30: Reproduced the close/read race at the shared VFS seam. A
  write-capable handle can be closed by the kernel after the next reader has
  already opened the inode; registering the release barrier only after handle
  removal leaves that reader ahead of local materialization. A staged upload
  payload can also outlive an evicted or short primary local read.
- 2026-08-30: Added the narrow local repair: release registers the local
  barrier while the handle table still exposes last-handle state, retains it
  while materialization is in flight, and reads fall back to the local upload
  stage after a short/evicted primary read. The barrier is local and does not
  await PostgreSQL, S3, or metadata replication.
- 2026-08-30: The release `devbench` control with 1000 small files plus one
  256MiB file completed all content-validated read phases successfully. The
  report is `target/perf/sfs-668-devbench-1000-full/report.json`; only the
  optional 1000MiB/s write-throughput target remains below threshold.
- 2026-08-30: The release single-file 4GiB `devbench` control completed
  during-upload, first, hot, large-hot, and pseudo-random reads with strict
  content validation and no mismatch/EOF. Its report is
  `target/perf/sfs-668-devbench-4g-single/report.json`; its write-throughput
  and peak-RSS diagnostics do not meet the optional gate.
- 2026-08-30: The clean five-run 256MiB operational matrix completed with all
  read phases and content checks valid. The full public `studiofs bench`
  workload was retried after the repair but did not produce a terminal report:
  the client stalled in FUSE open/close while diagnostics connections were
  saturated, so that run is non-qualifying rather than a correctness result.
- 2026-08-30: A combined 1000-small-file plus 4GiB `devbench` control also did
  not reach read validation. The client blocked in FUSE `close` while the
  daemon sample stayed in serial `getattr` work that decodes all regular-file
  chunk states into nested `BTreeMap`s; `mdworker` processes were active on the
  host, consistent with host metadata-indexing pressure. The run was stopped
  after SIGINT failed to unblock it and is excluded from baseline data. It
  produced no mismatch/EOF and no evidence of a foreground PostgreSQL wait.
- 2026-08-30: The first post-repair standalone `dd conv=fsync` probe could not
  create its exact probe directory: `mkdir` blocked in mounted FUSE before
  `dd` started or any bytes were timed. No fsync sample was accepted; the
  metric remains unavailable rather than zero.

## Surprises and discoveries

1. The existing `devbench` end-to-end workload combines deferred local
   namespace drain, FUSE write callbacks, writer-stack/cache materialization,
   and asynchronous upload. The 1GB/1000-file attempt was too slow, but its
   partial run does not prove that PostgreSQL network latency was the cause.
2. The current legacy path can observe read/write callback counts and cache
   pipeline counters through the mounted report, but it does not turn an
   unobserved flush or fsync latency into a fabricated zero. The supplemental
   barrier probe below is required for that metric.
3. `metadata_commit` is a local Redb metadata-drain timer. The actual remote
   PostgreSQL transaction is issued by the background replication poller, so
   it must be measured separately from foreground create/open/write latency.
4. macFUSE FSKit is a message transport/API surface, not a direct replacement
   for the existing low-level libfuse session. The semantic adapter and VFS
   contracts must remain shared.
5. The initial multi-stream mismatch was in `devbench` validation, not in the
   mounted read path: `e2e-first-read` uses the generic read-operation worker,
   which had a different seed convention from the sequential-stream write
   worker. The corrected convention is shared across all phases.
6. A release benchmark client does not make a debug mounted daemon a release
   baseline. Process provenance is part of the measurement contract and must
   be checked before interpreting throughput or callback latency.
7. The mounted benchmark writer calls `flush()` and then drops the file. The
   legacy adapter maps that flush to volatile writeback, while the kernel can
   deliver the next open before the writer's release callback. The repaired
   release path registers a local barrier before removing the handle and keeps
   it through in-flight materialization; the read path also consults the local
   upload stage after a short or evicted primary read. This is a local
   close/read correctness seam, not a PostgreSQL round trip.
8. The `studiofs-bench` `sync_all` control passes at 1GiB, and the repaired
   volatile `devbench` controls pass at 256MiB with 1000 small files and at
   4GiB with one file. The full public run still needs a clean terminal report
   before it can be accepted as the primary baseline.
9. The `devbench` engine executes the public first/hot/random/medium phases
   when a cache path is available, and its mounted report now retains those
   phase metrics. A report with a zero-file optional phase is still not a
   successful sample for that workload.
10. The combined 1000-small-file plus 4GiB control separates a client-side FUSE
    close wait from payload correctness: the client did not reach reads, while
    the daemon was servicing expensive metadata reconstruction. This is
    non-qualifying host/FUSE pressure evidence, not a PostgreSQL-latency result.
11. A standalone fsync probe can be blocked before the timed syscall by the
    same mounted metadata pressure. The absence of a sample is therefore an
    unavailable measurement, not evidence of zero fsync latency.

## Decision log

| Decision | Reason |
| --- | --- |
| Pin macFUSE 5.3.3 for this baseline | It is the installed package and the exact version under test. |
| Keep `studiofs-macfuse` as the legacy production path | SFS-668 defines the contract; SFS-669 owns the experimental implementation and rollback selector. |
| Keep the VFS, metadata, payload, cache, and replication contracts unchanged | FSKit is a transport/backend migration, not a semantic storage change. |
| Use `MFMount.framework` and the FUSE wire protocol for the FSKit transport | macFUSE exposes channel/message APIs and owns the FSKit extension mount lifecycle. |
| Treat unsupported FSKit capabilities as explicit parity blockers | Silent degradation would violate correctness and make SFS-672 incomparable. |
| Require five successful runs per mandatory workload | One point estimate cannot distinguish a win from host noise. |
| Use a reduced operational matrix on this test volume | The reference 1GB/1000-file end-to-end write profile is too long for five repetitions; the selected matrix isolates a reproducible workload while writer-stack/cache behavior is investigated. |
| Keep the local release/read repair in this preflight | The observed zero-filled read was a correctness failure at the shared VFS seam; deferring it to FSKit would leave the legacy contract unverified. The repair adds no foreground remote wait and preserves the metadata, payload, cache, and replication contracts. |

## Local-first metadata boundary and observed foreground path

The intended local-first boundary is present in the checked-in code. The
deferred create path records the namespace overlay locally, enqueues the
metadata operation, schedules a drain, and returns. The drain calls
`PgMetaEngine::apply_namespace_mutations`; `PgMetaEngine::mutate` applies the
change to the local store and marks pending work. It does not perform a remote
PostgreSQL request. The background `pg-meta-poll` worker later claims pending
events and sends the projected batch through the PostgreSQL transaction path.

The live mounted observability report from the interrupted workload provides
the relevant separation:

| Path | Observation | Meaning |
| --- | ---: | --- |
| `vfs_write_path.create` | 549 calls, 0.045 ms average | Foreground create is local/deferred. |
| `vfs_write_path.open_file` | 705 calls, 0.060 ms average | Foreground open is not waiting on remote PG. |
| `fuse_callbacks.write` | 1165 calls, 108.35 ms average | The mounted write callback is the visible foreground cost. |
| `vfs_write_path.record_writer_stack_state` | 1165 calls, 108.28 ms average | The callback cost tracks writer-stack state recording. |
| `metadata_commit` | 1194 calls, 315.00 ms average | Background local metadata-drain timing, not PG network latency. |
| `cache_pipeline.materialize` | 1,446,379,536 bytes | Sustained payload/cache materialization was active. |

Therefore the earlier PG explanation was the wrong attribution. The next
focused measurement is writer-stack/cache contention and backpressure under
the mounted workload; remote PostgreSQL replication latency should be recorded
as a separate asynchronous backlog metric, not folded into foreground I/O.

## Correctness preflight: local close/read barrier

The observed large-file failure is reachable without a remote metadata request:

```text
tokio::fs::File::flush()
  -> StudioVfsAdapter::flush
  -> flush_volatile_writeback_with_outcome
  -> drop file handle
  -> deferred last-handle release barrier
  -> next open/read
```

`flush_volatile_writeback_with_outcome` waits for an already registered
deferred release, checks local metadata health, and schedules materialization;
it is intentionally not the strict `fsync_fh` barrier. The repaired release
path invokes its decision callback while the handle table still exposes the
closing handle and its remaining-handle count. It registers the local barrier
for a final release and for a write-capable release with pending local state,
then retains that barrier while materialization is in flight. `read_into_fh`
waits for the registered barrier even when the reader was opened before the
writer's release callback arrived.

`payload_store_read_into_async` first uses the hot upload memory and primary
local payload. If that read is short or absent, it falls back to the local
upload-stage payload before returning. That closes the second local visibility
gap without turning a read into a PostgreSQL or S3 request.

The control result is now narrow and reproducible: `studiofs-bench` calls
`sync_all` and passes at 1GiB; the repaired volatile `devbench` path passes
strict content validation with 1000 small files plus a 256MiB file and with a
single 4GiB file, including reads while upload is active. The local correctness
gate is green for these controls. The full public workload still has no valid
terminal report, so the primary performance baseline remains open.

## Current legacy macFUSE architecture

The current production backend is a low-level libfuse/macFUSE bridge. The
semantic path is:

```text
mount_with_request
  -> Session::mount_with_ops
  -> fuse_lowlevel_new / fuse_mount / fuse_session_add_chan
  -> SessionInvalidationQueue
  -> fuse_session_loop
  -> csrc/low_level_ops.c callback
  -> CallbackState / Request
  -> StudioVfsAdapter
  -> StudioVfs and metadata/cache/payload services
  -> Reply
```

The source locations are:

- `crates/studiofs-macfuse/src/lib.rs`: `mount_with_request` owns the boxed
  callback state, installs kernel invalidation, runs the session, clears the
  invalidator, waits for reply tasks, and only then drops the session state.
- `crates/studiofs-macfuse/src/low_level.rs`:
  `Session::mount_with_ops` creates the low-level session and channel;
  `Session::run` enters `fuse_session_loop`; shutdown closes the invalidation
  queue and waits for it to become idle.
- `crates/studiofs-macfuse/csrc/low_level_ops.c`: the C callback tables select
  empty, read-only, or full operation sets.
- `crates/studiofs-macfuse/src/adapter.rs`: callbacks convert the raw request
  into an owned `Request`, copy callback-owned data before asynchronous work,
  dispatch to the shared VFS, and construct exactly one `Reply`.

The lifetime rules are part of the contract: callback state stays boxed until
the session loop exits; `Request::drop` replies `EIO` if a callback exits with
an unreplied request; asynchronous replies are drained before callback state
is dropped; and invalidation shutdown waits for the queue's in-flight work.
The existing path supports local-first write visibility, flush/fsync/release,
xattrs, setattr, symlinks, hardlinks, rename/unlink, kernel invalidation,
Finder/macOS metadata, read-only callbacks, shutdown/unmount, and remote
replication through the shared layers.

## FSKit transport contract

The experimental FSKit backend must preserve the semantic callback contract
above while replacing only the transport/session seam:

1. Create a channel with `MFChannelCreate` and mount it with
   `MFMount(channel, mountPoint, options, quiet)`. `MFMount.framework` owns the
   FSKit extension connection; the backend must not assume a `/dev/fuse` file
   descriptor exists.
2. Receive messages with `MFChannelCopyNextMessage` (or the blocking wait
   operation). A received `MFMessageRef` remains retained for as long as any
   borrowed body or reply buffer is used.
3. Read the structured FUSE request header with `MFMessageGetBodyBuffers`.
   The first body buffer contains the FUSE request beginning with
   `fuse_in_header`; a second buffer may contain the variable request payload.
   Parse and validate lengths before dispatching. No raw `fuse_req_t` may cross
   into an asynchronous task.
4. Dispatch the same semantic operation names and ownership rules as the
   existing `StudioVfsAdapter`. A callback must retain/copy all data needed
   after the receive call and must complete exactly once, including cancellation
   and error paths.
5. Obtain the reply storage with `MFMessageGetReplyBuffer`. Write a valid
   `fuse_out_header` followed by the operation-specific reply, including
   `fuse_reply_buf_out` for a direct read-buffer reply, then send it with
   `MFChannelSendMessage`.
6. Treat message buffers as borrowed: `MFChannelSendMessage` does not turn
   arbitrary caller memory into durable storage. Keep the `MFMessageRef` alive
   until the send completes or copy into owned storage. Release every message
   and channel with `MFRelease`/`MFChannelClose` at the terminal path.
7. Use `MFChannelInterrupt` during shutdown to wake a blocked receive on
   macFUSE 5.3.3. The interrupt must join the receive/dispatch workers before
   the channel and callback state are released.

The FSKit path must not call the legacy-only transport functions
`fuse_mount`, `fuse_session_loop`, `fuse_session_add_chan`, or the raw
`fuse_reply_*` APIs. The legacy path remains available behind the existing
backend selector and is the rollback path.

### Zero-copy and copy-avoidance contract

Negotiation and instrumentation are mandatory; an optimization claim without
the negotiated flag and byte counters is not a result.

| Operation | Required API/feature | Ownership rule | Fallback |
| --- | --- | --- | --- |
| Handshake | `FUSE_INIT`, `FUSE_DARWIN_REPLY_BUF`, `FUSE_DARWIN_PAYLOAD_BUF` | Record negotiated request/reply capabilities before enabling direct buffers. | Use owned serialized buffers and record the fallback. |
| WRITE request | `MFMessageGetBodyBuffers` second iovec/payload | Consume the borrowed payload while the `MFMessageRef` is retained; never retain a raw pointer after release. | Copy into VFS-owned input and record bytes/ops. |
| READ reply | `MFMessageGetReplyBuffer`, `fuse_out_header`, `fuse_reply_buf_out` | Fill the borrowed reply buffer before `MFChannelSendMessage`; retain the message until send completion. | Use the existing owned reply path and record bytes/ops. |
| Interrupt/error | `MFChannelInterrupt`, message release | Cancellation must release borrowed messages and complete the request with a stable error. | Return the existing EINTR/EIO mapping. |

The backend must expose counters for negotiated flags, zero-copy read/write
operations and bytes, copy-fallback operations and bytes, rejected malformed
messages, interrupted requests, and reply-send failures. SFS-671 consumes this
exact contract. The current legacy benchmark must report its corresponding
copy/fallback state rather than imply that the legacy `Vec` path is zero-copy.

## Capability and compatibility matrix

These are hard semantic requirements for SFS-669 and the SFS-672 correctness
gate. A cell is `pass` only when behavior is characterized on the same
mounted workload; “not supported” is not an acceptable silent result.

| Capability | Legacy contract | FSKit decision for this contract |
| --- | --- | --- |
| Local-first write visibility | Writes are visible through the mounted path before remote upload completes. | Preserve through the shared VFS; transport must not wait for replication. |
| `flush`/`fsync`/`release` | Flush barriers and last-handle release use the existing VFS outcomes. | Preserve ordering and error mapping; measure latency separately. |
| xattrs and `ResourceFork` | Existing get/list/set/remove xattr callbacks and Finder metadata path. | Preserve names, bytes, and errors; include `com.apple.ResourceFork` in characterization. |
| `setattr`/`setattr_x` | Existing callbacks map mode, size, times, and flags. | Preserve partial-field semantics and truncation behavior. |
| Symlinks, hardlinks, rename, unlink | Existing low-level callbacks route to shared VFS semantics. | Preserve identity, atomicity, and invalidation. |
| Kernel invalidation | `fuse_lowlevel_notify_inval_inode` and `fuse_lowlevel_notify_inval_entry` are used by the legacy session. | The FSKit notification API is not supported; SFS-669 must provide and validate an explicit alternate invalidation path before parity. |
| Finder/macOS metadata | Finder-visible metadata and AppleDouble/resource-fork behavior are characterized by the mounted bridge. | The FSKit limitation around metadata must be explicit; do not silently accept AppleDouble or ResourceFork regressions. |
| Read-only | Legacy read-only operation table omits mutations and callbacks return the platform error. | FSKit files are always opened read/write; enforce read-only in StudioFS callbacks and document any mount-option mismatch. |
| Mount location | Current StudioFS mount is under `/Volumes`. | FSKit mount points outside `/Volumes` are unsupported by the selected macFUSE contract. |
| `fuse_context_t` | Not required by the shared semantic adapter. | No FSKit implementation may depend on `fuse_context_t`, which is unavailable on the FSKit path. |
| Shutdown/unmount | Exit request, loop return, invalidation idle, reply drain, and state drop are ordered. | `MFChannelInterrupt` must wake receive, join workers, close the channel, and preserve the same terminal ordering. |
| Remote replication | Upload and metadata workers remain independent of the bridge transport. | Preserve the existing queue, retry, and acknowledgement contracts. |

The upstream macFUSE documentation describes the FSKit limitations as
versioned behavior. In particular, the current contract records that the
notification API is not supported, mount points outside /Volumes are not
supported, and files are always opened read/write. These are compatibility
inputs, not reasons to weaken StudioFS correctness requirements.

## Packaging, runtime, support, and rollback

- Minimum macOS for the selected FSKit backend: macOS 15.4. The current host
  is macOS 26.5.2.
- Pinned macFUSE package: 5.3.3. The installed package provides
  `MFMount.framework` at
  `/Library/Filesystems/macfuse.fs/Contents/Frameworks/MFMount.framework`.
- The build must use the installed MFMount headers/library and fail clearly if
  the required framework is absent. The legacy libfuse headers and C shim
  remain a separate build path.
- Runtime installation requires the macFUSE package and its FSKit extension.
  The FSKit backend does not require the legacy KEXT approval flow, while the
  legacy kernel backend can still require its macFUSE system-extension/KEXT
  approval depending on the installed package and host policy.
- A sandboxed launcher must have the macFUSE mount entitlement/exception
  required for `io.macfuse.mount`; the backend must surface a mount failure,
  not hang, when the extension or permission is absent.
- The backend selector remains `STUDIOFS_MOUNT_BACKEND=macfuse` for the
  current production path. SFS-669 may add an explicitly experimental FSKit
  selector, but SFS-668 removes no production backend and changes no default.
- Rollback is a process restart with the legacy selector and the known-good
  macFUSE package. A failed FSKit parity check must not mutate persisted data
  or leave the mount lifecycle lock held.

The contract is based on the installed header and these primary macFUSE
references:

- https://github.com/macfuse/macfuse/wiki/FUSE-Backends
- https://github.com/macfuse/macfuse/wiki/Getting-Started
- https://github.com/macfuse/macfuse/wiki/Getting-Started-%28Developer%29-%E2%80%90-MFMount.framework
- https://github.com/macfuse/macfuse/releases

## Fixed legacy baseline procedure

### Environment and invariants

Run every repetition on the same host, checkout, release binary, mounted
volume, macFUSE version, mount options, cache directory, and backend selector.
Record `sw_vers`, `uname -a`, the installed macFUSE version, the git commit,
the command line, and the environment file name without recording secrets.

The standard script's reference defaults are retained for comparison:

```text
--runs 5
--block-size 4MB
--big-file-size 1GB
--small-file-size 128KB
--small-file-count 1000
```

Those `1GB`/`1000` defaults are not accepted as a result when the run is
stopped or when correctness fails. The current test volume's selected,
operational SFS-668 matrix is fixed as follows:

```text
mount point:       /Volumes/studiofs/test
block size:        4MB
large file:        256MB
small file size:   128KB
small file count:  100
threads:           1
runs:              5
```

The single thread makes the metadata-heavy phase reproducible on this remote
test service; the large file still exercises large-file read throughput. If a
future decision requires the reference 1GB/1000 profile, it is a new baseline
with its own five successful runs, not a mixed comparison.

### Mounted benchmark command

Build the exact release binary used by the run, mount the test volume, and
execute the existing mounted benchmark engine:

```sh
cargo build -p studiofs --release
bash ./tools/perf/validate-bench-macos.sh \
  --mount-point /Volumes/studiofs/test \
  --studiofs ./target/release/studiofs \
  --runs 5 \
  --block-size 4MB \
  --big-file-size 256MB \
  --small-file-size 128KB \
  --small-file-count 100 \
  --threads 1 \
  --output-root target/perf/sfs-668-legacy-256m
```

The script writes `environment.json`, each exact `command.txt`, stdout, and
`report.json` beneath `target/perf`. `target/perf` is the artifact location;
raw run files are not source documents and must not contain credentials.

The existing engine supplies these mandatory workload observations:

| Mandatory workload | Mounted-engine phase/source |
| --- | --- |
| cold sequential read | First logical read after the upload drain; report as a logical cold read, not an OS-cache purge. |
| warm repeated sequential read | Hot and large-hot sequential phases after the first read. |
| random read | The pseudo-random read phase in the mounted report. |
| large-file read throughput | The 256MB large-file read phase and its throughput/p95. |
| write throughput | The e2e write phase. |
| metadata-heavy directory traversal/file-open | The stat/open phase over the fixed small-file set. |
| CPU and callback count | `runtime_stats`, `fuse_callbacks`, and mounted observability when available. |
| cache hit/miss and kernel cache reuse | `cache_pipeline`, fetch/cache counters, and `mounted_memory`; record unavailable when the backend cannot observe a counter. |

`studiofs bench` remains the public fixed pseudo-random workload contract and
`studiofs devbench` is the tunable mounted engine used here. The five-run
script is `validate-bench-macos.sh`; do not replace it with a one-off timing
command for the primary read/write numbers.

### Flush/fsync barrier probe

The mounted engine's `runtime_stats` can expose barrier counters but does not
guarantee a standalone fsync-latency sample. Run the supplemental probe five
times, with a unique file per repetition, and save its stdout and exact
command under the same `target/perf/sfs-668-legacy-256m` artifact directory.
The timed interval must include an explicit `fsync(2)` (`dd conv=fsync` is the
portable command-line probe). Remove only the exact probe files afterward.
If the probe cannot produce a valid sample, mark flush/fsync latency
unavailable and keep the issue open; do not use zero.

### Reduction and acceptance

For every mandatory workload, retain the five raw observations and report:

- median;
- sample variance, `sum((x - mean)^2) / (n - 1)`;
- minimum, maximum, and the units used;
- host/runtime warnings and any unavailable observability;
- correctness status for every run.

The baseline is accepted only when all five runs complete, every correctness
assertion passes, and the report contains the required median and variance for
each workload. The SFS-672 comparison must reuse the same host, commit,
configuration, five-run policy, and primary metric. A better point estimate is
not sufficient if a material regression or correctness failure is present.

## Baseline results

### Preflight evidence (not the five-run baseline)

| Profile | Result | Use |
| --- | --- | --- |
| 64MB large file / 10 small files / release | One deterministic content mismatch at offset 0 (`0xd0` expected, `0x00` observed) | Hard correctness failure; excluded. |
| 16MB large file / 4 small files / release | Passed | Smoke only; not statistically meaningful. |
| 64MB large file / 10 small files / release | Passed | Smoke only; not statistically meaningful. |
| Reference 1GB / 1000 small files | Stopped after 473 temporary files because the mounted write path was too slow for the planned five repetitions | Partial run; excluded. |
| 1MB large file / 2 small files / `threads=2` before harness fix | First-read validation failed because stream-specific write seed differed from generic read-operation seed | Benchmark harness defect; excluded. |
| 1MB large file / 2 small files / `threads=2` after harness fix | Passed all phases and correctness validation | Correctness diagnostic only; daemon was `target/debug`. |
| 16MB large file / 32 small files / `threads=4` after harness fix | Passed all phases and correctness validation | Correctness diagnostic only; daemon was `target/debug`. |
| 256MB large file / 100 small files / `threads=1` / release / five runs | All five passed; `first-read` validation was true for all five | Operational matrix; public phase telemetry is not serialized by `devbench`. |
| `studiofs-bench` 64MiB / 100 files / release / batch fsync | Passed | Supplemental sequential control; report `studiofs-bench-report-1788039123-0.json`. |
| `studiofs-bench` 64MiB / 100 files / release / `--no-batch-fsync` | Passed | Supplemental sequential control; report `studiofs-bench-report-1788039141-0.json`. |
| `studiofs bench` public workload / release | Failed on large file 001080 at offset 0: expected `0x5f`, got `0x00` | Correctness preflight failure; excluded. |
| 1GiB large file / 1000 small files / `devbench` / release | Failed on large file 000000 at offset 0: expected `0x86`, got `0x00` | Correctness preflight failure; excluded. |
| 256MiB large file / 1000 small files / `devbench` / release after local repair | 1001 files; all during-upload, first, hot, large-hot, and pseudo-random phases content-valid; no mismatch/EOF | Correctness control; write target not met; report `target/perf/sfs-668-devbench-1000-full/report.json`. |
| 4GiB single file / `devbench` / release after local repair | All content-validated read phases passed; no mismatch/EOF | Correctness/size control; write and RSS targets not met; report `target/perf/sfs-668-devbench-4g-single/report.json`. |
| 4GiB single file / `studiofs-bench` / release | Two mounted streaming passes completed with batch fsync | Throughput-only control; it does not validate payload content; report `target/perf/sfs-668-studiofs-bench-4g/studiofs-bench-report-1788065947-0.json`. |
| `studiofs bench` public workload / release after local repair | Write completed, but read stalled in FUSE open/close under diagnostics socket saturation; no terminal report | Non-qualifying harness/observability run; excluded, not a correctness failure. |
| 1000 small files + 4GiB large file / `devbench` / release after local repair | Did not reach read validation; client blocked in FUSE `close` while daemon serviced serial `getattr`/regular-file chunk-state reconstruction; no mismatch/EOF | Non-qualifying host/FUSE metadata-pressure run; excluded, not a PostgreSQL or correctness failure. Run `8030d02b-a61b-4e4a-adb2-80fdd0c1d980`; no terminal report. |
| Standalone 64MiB `dd conv=fsync` probe / release after local repair | Probe directory creation blocked in mounted FUSE before `dd` started | No timing sample accepted; unavailable, not zero. |
| `studiofs-bench` 1GiB / single file / release / batch fsync | Passed | Strict-sync control; report `target/perf/sfs-668-public/studiofs-bench-report-1788039842-0.json`. |
| `studiofs-bench` 1GiB / single file / release / `--no-batch-fsync` | Passed | Strict-sync control; report `target/perf/sfs-668-public/studiofs-bench-report-1788039826-0.json`. |

### Five-run matrix

Populate this table only from the five raw `report.json` files and a matching
flush/fsync probe in `target/perf`. A row with a missing sample is incomplete,
not zero. The values below are from the clean post-repair matrix at
`target/perf/sfs-668-post-fix-256m-clean/macos-bench-validation-20260830-040948`.

| Workload | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Median | Sample variance | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| cold sequential read (`first-read`, MiB/s) | 1858.3469 | 1422.6459 | 1500.6913 | 988.7105 | 1614.2301 | 1500.6913 | 101549.942746 | pass in operational matrix |
| warm repeated sequential read (`hot-read`, MiB/s) | 2633.1726 | 1148.0529 | 1417.8162 | 2598.6532 | 1553.0263 | 1553.0263 | 484879.516640 | pass in operational matrix |
| random read (`pseudo-random-read`, MiB/s) | 82.9873 | 156.9003 | 156.4072 | 156.6612 | 148.4474 | 156.4072 | 1038.455307 | pass in operational matrix |
| large-file read throughput (`large-hot-read-best4`, MiB/s) | 15587.6971 | 15271.5777 | 15313.7903 | 15119.8309 | 15803.2210 | 15313.7903 | 74597.201032 | pass in operational matrix |
| write throughput (MiB/s) | 437.8385 | 379.3777 | 409.3612 | 400.9029 | 384.5633 | 400.9029 | 538.690350 | correctness pass; optional write target not met |
| flush/fsync latency (64MiB write+`fsync(2)`, s) | pending | pending | pending | pending | pending | pending | pending | unavailable for this post-repair set |
| metadata-heavy directory traversal/file-open (stat p95, ms) | 8.2157 | 0.0790 | 0.1153 | 0.0926 | 0.0893 | 0.0926 | 13.192370 | pass in operational matrix |

The local correctness baseline is now confirmed for the selected operational
matrix and the two size controls. The primary legacy performance baseline
remains `INCOMPLETE` until the public workload has a valid terminal report and
the standalone flush/fsync probe has five valid samples. The five-run matrix is
operational evidence, not an FSKit A/B result; the optional write-throughput
target is below threshold and Windows validation is unavailable on this host.

## Validation and acceptance checklist

- [x] SFS-668 scope and dependent issue boundaries are recorded.
- [x] Current legacy session/channel lifecycle and ownership rules are
      recorded from the checked-in bridge.
- [x] macFUSE 5.3.3, minimum macOS 15.4, packaging/runtime prerequisites,
      rollback, and version-sensitive limitations are explicit.
- [x] `MFMount.framework` channel/message APIs and exact zero-copy feature
      names are explicit.
- [x] Local-first write visibility, flush/fsync/release, xattrs,
      `ResourceFork`, setattr, links, rename/unlink, invalidation, Finder
      metadata, read-only, shutdown, and remote replication remain hard
      correctness requirements.
- [x] Fixed command, reference `1GB`/`1000` profile, selected operational
      matrix, artifact path, five-run policy, and reduction formula are
      checked in.
- [ ] Five successful public legacy repetitions with median/variance for every
      mandatory workload. The selected operational matrix has five
      correctness-clean repetitions, but the public post-repair run has no
      valid terminal report.
- [ ] A valid standalone flush/fsync latency sample for every repetition in
      the post-repair artifact set.
- [ ] FSKit A/B and zero-copy implementation; these belong to SFS-669 and
      SFS-671, with the decision gate in SFS-672.
- [x] no production backend removal, metadata/S3/cache semantic change, or
      foreground remote wait was added. The narrow local VFS release/read
      correctness repair is explicitly documented above.

The focused executable contract check is:

```sh
cargo test -p studiofs --test architecture_vocabulary_test macfuse_fskit_contract_and_legacy_baseline_covers_sfs668_gate -- --nocapture
```

It must pass together with `cargo fmt --all -- --check` and `git diff --check`.
Windows validation is not available on this Mac; Windows behavior remains a
shared-contract requirement and is not claimed as validated here.

## Idempotence, recovery, and artifacts

- Use a new `target/perf/sfs-668-legacy-256m/macos-bench-validation-*`
  directory for each top-level execution. Never mix reports from different
  commits, macFUSE versions, or mount configurations.
- The benchmark work root is `/Volumes/studiofs/test/.studiofs-benchmark`.
  Cleanup is limited to that exact temporary path and must happen after an
  interrupted run.
- If the benchmark process exits, first verify the mount is still alive and
  inspect the retained stdout/report before retrying. A correctness mismatch
  stops the baseline rather than being retried away.
- Keep the mounted test volume isolated from production data. Unmount only the
  exact `/Volumes/studiofs/test` mount when the operator requests teardown.

## Interfaces and dependencies

- SFS-669: experimental FSKit backend implementation consumes this contract.
- SFS-671: zero-copy path consumes the exact MFMount/FUSE buffer flags and
  counters above.
- SFS-672: legacy-vs-FSKit five-run decision gate consumes these artifacts and
  correctness rules.
- SFS-673: promotion follows only a GO decision from SFS-672.
- Shared implementation seams: `studiofs-macfuse` low-level transport,
  `StudioVfsAdapter`, `StudioVfs`, metadata/cache/payload services, and the
  existing mounted benchmark engine.

## Outcome and retrospective

SFS-668 now has a checked-in contract, an explicit compatibility boundary, a
version-pinned legacy measurement procedure, a local correctness repair with
focused regression coverage, and a test that fails when those surfaces
disappear from the source-of-truth document. The selected five-run operational
matrix and the 256MiB/1000-file plus 4GiB correctness controls are green. The
primary public performance baseline remains open because its post-repair runs
did not produce a valid terminal report: the exact public run and the combined
control were blocked in host/FUSE metadata pressure before a qualifying read
result. The post-repair flush/fsync probe is also unavailable.
