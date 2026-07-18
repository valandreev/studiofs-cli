# PR 601 Review Follow-up Design

## Purpose

Address four verified correctness and observability findings from the follow-up
review of PR 601 without changing the intended Linux cache policy or negotiated
FUSE geometry.

## Scope

The change covers:

1. atomic OS page-cache generation decisions relative to writer lifecycle;
2. safe handling of an incomplete finite-cgroup memory sample;
3. cancellation safety after writer-stack slices have been prepared for
   materialization;
4. macFUSE accounting for opens whose inode had no observed lookup reference.

The other review findings are out of scope because the current implementation
already provides the claimed behavior or deliberately preserves the first
shutdown error.

## Design

### Atomic OS-cache state

Store generation observations and active-writer counts behind one mutex. Expose
an operation that checks for an active writer and observes the supplied metadata
while holding that mutex. Starting a writer updates the writer count and removes
the cached generation in the same critical section.

`StudioVfs::prepare_os_cache_open` will continue checking local dirty state and
write-capable handles, but the final active-writer decision and generation
observation will be atomic. Therefore:

- a writer established before the read observation forces replacement;
- a writer established after the observation clears that observation;
- no interleaving can leave a stale generation eligible for reuse.

### Incomplete finite-cgroup samples

When `memory.max` is finite, `memory.current` is required to calculate remaining
cgroup headroom. If the current counter is absent, the Linux probe will return a
sampling error instead of silently converting the sample to host-only values.

The existing memory-pressure controller will then retain its previous healthy
snapshot. If no healthy snapshot exists, it will use its constrained fallback.
This avoids admitting memory from host-wide availability when the process is
actually bounded by a container limit.

An absent or unlimited `memory.max` remains a valid host-only sample.

### Cancellation-safe writer-stack preparation

Preparing an inode freezes its dirty writer slices before the asynchronous
object-ID reservation. Introduce an explicit rollback operation that unfreezes
only slices still in the prepared state. Wrap preparation in a synchronous RAII
guard:

- dropping the future during object-ID reservation invokes rollback;
- an object-ID reservation error invokes rollback through the same drop path;
- immediately before consuming the prepared slices, the guard is disarmed;
- successful materialization keeps the existing finish and installation flow.

The guard must not undo slices that have already transitioned into pending or
materializing state. The same protection applies to both full-inode and
idle-tail materialization, which share the prepare/await/finish pattern.

### macFUSE unknown-open accounting

For an ordinary macFUSE `open`, `LookupReservation::Ensure` may create a missing
lookup reference. Record `open_unknown_inode` exactly when that creation occurs.
Do not increment it when the inode was already observed, and do not apply the
counter to `LookupReservation::Observe` used by create replies, because those
replies intentionally establish a new lookup reference.

The existing reply-rejection rollback remains responsible for forgetting a
reference added by the rejected reply.

## Error Handling

- An incomplete finite-cgroup sample is reported through the existing probe
  error path; no new fallback policy is introduced.
- Writer-stack rollback is infallible and synchronous so it is safe from
  `Drop`.
- Existing materialization and reply errors retain their current public error
  types and precedence.

## Testing

Use test-first regression coverage for each behavior:

1. race a read generation observation with writer activation and verify that a
   stale generation cannot be reused;
2. provide a finite `memory.max` without `memory.current` and verify that the
   probe reports an incomplete sample;
3. cancel materialization while object-ID reservation is pending and verify that
   a subsequent materialization can consume the same dirty data;
4. verify that macFUSE counts only an `Ensure` open that repairs a missing lookup
   reference.

After focused red/green cycles, run formatting, the affected crate tests, and
Clippy for the affected workspace targets.

## Non-goals

- changing cache-policy defaults;
- changing FUSE `max_write` negotiation;
- changing shutdown error aggregation;
- changing memory-pressure percentages or configured ceilings;
- adding new user-facing configuration.
