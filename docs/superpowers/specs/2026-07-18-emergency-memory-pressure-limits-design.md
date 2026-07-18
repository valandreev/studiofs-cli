# Emergency memory-pressure limits design

## Goal

Keep StudioFS inside its calculated process-wide memory envelope when available
memory falls below the existing 64/64/32/32 MiB performance-minimum profile.
Normal-memory behavior and recovery hysteresis remain unchanged.

## Problem

The current model applies these minimum component limits after calculating the
40/25/15/10 percent shares:

- memory cache: `max(block_size, 64 MiB)`;
- dirty memory: `max(block_size, 64 MiB)`;
- transit memory: `max(block_size, 32 MiB)`;
- async writes: `max(negotiated_max_write, 32 MiB)`.

Their managed total is normally 192 MiB. During the 2026-07-18 Linux pressure
probe, the 90 percent managed budget fell to about 113 MiB. The model therefore
published limits above the envelope and set `overcommitted_minimum=true` even
though a smaller, technically viable profile was available.

## Decision

Use two limit profiles inside `MemoryPressureModel::limits_for`.

### Performance profile

Calculate the existing percentage shares and existing performance minima
without changing their values. Use this profile whenever its managed total is
no greater than 90 percent of the current envelope.

This preserves all current behavior in ordinary conditions.

### Emergency profile

When the performance profile does not fit, calculate a correctness floor:

- memory cache: one filesystem block;
- dirty memory: one filesystem block;
- transit memory: one filesystem block;
- async writes: the greater of one filesystem block and one negotiated maximum
  write request.

Existing configured component ceilings remain ceilings unless they are below a
correctness floor. This retains the current rule that a correctness floor wins
over an undersized internal ceiling.

Let `managed_budget` be 90 percent of the current envelope. If the correctness
floors fit within that budget, distribute the remaining bytes using the existing
40/25/15/10 weights, normalized across their total weight of 90. Each component
is capped at the greater of its configured ceiling and correctness floor.
Integer rounding may leave a small amount unused; it must never make the total
exceed `managed_budget`.

If even the correctness floors do not fit, publish those floors and retain
`overcommitted_minimum=true`. This represents a genuinely impossible budget;
the model must not publish zero-sized or sub-request limits merely to clear the
diagnostic flag.

## State and recovery

The existing pressure-state rules remain unchanged:

- enter `Constrained` immediately when available memory is below the reserve or
  the envelope is below currently admitted managed bytes;
- lower limits immediately;
- require two healthy samples before returning to `Normal`;
- bound growth after recovery to 25 percent of each configured ceiling per
  sample.

Emergency-profile limits participate in the same bounded recovery path. No new
state, configuration option, environment variable, or runtime wire field is
introduced.

## Safety invariants

- In normal conditions, the existing performance profile is byte-for-byte
  unchanged.
- When the managed budget can fund the correctness floors, component limits
  sum to no more than 90 percent of the envelope.
- Each active component can process at least one block or negotiated write.
- `overcommitted_minimum` is true only when the correctness floors themselves
  cannot fit.
- Existing admitted writes are not cancelled; new admissions continue to use
  the reduced live limits and existing backpressure.
- Sampling failure behavior remains unchanged.

## Testing

The public test seam is `MemoryPressureModel::observe`.

Add a regression test using the captured failure geometry: a finite cgroup,
an envelope near 126 MiB, and ordinary 4 MiB blocks / 1 MiB negotiated writes.
The test must first fail against the old 192 MiB minimum profile, then verify:

- `state == Constrained`;
- `overcommitted_minimum == false`;
- `limits.managed_total() <= envelope_bytes * 90 / 100`;
- every component remains at or above its correctness floor.

Retain and extend the existing test for an envelope too small to fund even the
correctness floors; that case must remain explicitly overcommitted. Existing
normal-profile, hysteresis, bounded-growth, cgroup, and failure tests must
continue to pass.

After focused Rust tests pass, rerun the deterministic Python gate suite and a
fresh release Linux pressure collector from a new HEAD-bound artifact root.

## Non-goals

- Relaxing holder, cgroup, OOM, hash, drain, cleanup, CPU, or throughput gates.
- Changing the host/cgroup pressure probe.
- Adding operator-facing memory tuning.
- Changing cache eviction, write admission, async queue, or transit mechanisms.
- Updating the performance conclusion before all four required collector runs
  pass their applicable gates.
