# Data Plane Redesign Guardrails

For the SFS-242 rewrite, this proposal is now historical background. The
canonical rewrite contract lives in
[juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md),
which supersedes this note when the two differ.

## Status: PROPOSAL
## Date: 2026-04-02
## Issue: SFS-155
## Parent issue: SFS-148

This document is the execution checklist for the JuiceFS-style data plane
redesign. It turns the architecture freeze into concrete acceptance and
rejection criteria for future implementation work.

## 1. Runtime Layers To Delete Or Shrink

### Delete from the active design

- `studiofs-cache::l1_mem`
- `studiofs-cache::l2_disk`
- `studiofs-cache::lru_tracker`
- `studiofs-cache::runtime`
- `studiofs-cache::platform_direct_io` (its functionality is absorbed by the redesign model's local block backend data plane and bridge adapters)
- the legacy v3 cache/runtime split as a first-class execution path, superseded by the data plane redesign model

These layers may remain in the tree only as temporary migration scaffolding.
They are not a target architecture.

### Shrink to a bridge

- `studiofs-cache::block_cache`
- `studiofs-cache::upload_queue`
- `studiofs-vfs::runtime`
- ingress spill/checkpoint machinery that only exists to keep older paths
  recoverable
- direct I/O fallback policy that only exists to patch the old cache stack

These pieces may exist while the redesign is in flight, but only as narrow
adapters. They must not accumulate new policy, new data ownership, or new
feature surface area.

### Keep as design invariants

- local-first acceptance
- eventual remote visibility
- overlay over base precedence
- crash recovery without writer-local state being required for remote readers

## 2. Invalid Directions For PRs

A PR is out of scope for the redesign if it does any of the following:

- improves the old cache/runtime path without reducing or removing a legacy
  layer
- adds a second cache tier, queue, or scheduler just to preserve transitional
  behavior
- introduces new public knobs for retired cache-generation sizing, direct I/O tuning, or
  fixed upload/fetch limits
- expands compatibility shims into permanent product surface
- relies on writer-local recovery state for remote-visible correctness (see [docs/architecture.md](./architecture.md) for the remote-visible/local-visible contract)
- mixes the new `chunk` / `slice` / `block` vocabulary with legacy semantics in
  a new API
- tunes old-path hot spots when the same work could be done by moving to the
  new model
- postpones deletion of a layer by making it faster instead of making it
  disappear

If a change only makes the current stack faster, safer, or easier to operate
without moving the redesign forward, it belongs to old-path optimization, not
to the redesign.

## 3. Required Phase Order

The implementation order is strict:

1. Freeze vocabulary and guardrails.
2. Introduce the new data-plane boundary and adapters.
3. Implement the new foreground read resolver on top of that boundary.
4. Implement the new foreground write and materialization pipeline.
5. Move recovery, migration, and publish behavior onto the new model.
6. Remove the legacy runtime layers and then tune the surviving path.

The phases are not freely reorderable. In particular:

- phase 6 (final deletion of legacy code) cannot start before phase 5 is fully verified
- phase 5 cannot be treated as a benchmark-only cleanup step
- phase 3 and phase 4 must land before deletion work is considered complete

## 4. Benchmark Rules

### Allowed before the new model is complete

- baseline measurements of the current path
- apples-to-apples comparisons between the old path and the new boundary
  behind the same interface
- correctness-oriented measurements for the new model prototype
- regressions that validate whether a deletion candidate can actually be
  removed

### Not allowed before the new model is complete

- tuning the old cache/runtime stack for throughput just to postpone removal
- benchmark-driven work whose only effect is to keep a legacy layer alive
- micro-optimizing direct I/O fallback, upload queueing, or cache placement on
  the old path after the new boundary exists
- adding benchmark-specific knobs for components scheduled for deletion

Benchmark work is acceptable only when it helps decide, verify, or de-risk the
new model. It is not acceptable when it is a substitute for completing the
redesign.

## 5. Redesign Vs Old-Path Optimization

Use this decision rule:

- a task belongs to the redesign if it changes the canonical `chunk` /
  `slice` / `block` model, the local-visible vs remote-visible contract, the
  overlay/base precedence, or the boundary between the data plane and the
  surviving runtime
- a task belongs to the redesign if it removes, narrows, or replaces one of
  the layers listed above
- a task belongs to the redesign if it is required for the new model to be
  correct after a legacy layer disappears
- a task belongs to old-path optimization if it only improves the legacy stack
  without shrinking it or making its removal easier

When a task sits on the boundary, prefer the redesign classification only if
the implementation clearly advances the new model and reduces reliance on the
legacy one.

## 6. Review Checklist

Before a future implementation PR is accepted, check that it:

- names which legacy layer it removes or shrinks
- states which phase it belongs to
- avoids adding permanent surface area to compatibility shims
- does not benchmark-optimize a component scheduled for deletion
- uses the frozen data-plane vocabulary consistently
- leaves a clear path to removing the legacy layer once the new model is live

If a PR cannot answer those questions, it is not ready for the redesign.
