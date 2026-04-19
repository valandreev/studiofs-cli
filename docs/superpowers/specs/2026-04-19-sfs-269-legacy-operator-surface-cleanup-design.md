# SFS-269 Legacy Operator Surface Cleanup Design

## Status

Approved scope for implementation.

## Goal

Delete the remaining legacy operator-facing and developer-facing surfaces that
still imply an older StudioFS mount contract after `SFS-264` through `SFS-268`
already converged production behavior on the canonical 2026-04-19 Fast Local
Authority model.

This slice is intentionally about public surface cleanup, not about changing
runtime semantics again.

## Problem

The repository still teaches and emits product-facing concepts that no longer
belong to the supported StudioFS model:

- public CLI/help/settings still expose `--writeback-delay`
- the mount config overlay still honors `STUDIOFS_UPLOAD_DELAY_MS` as a
  user-facing compatibility surface
- mount profiling metadata still advertises `cache_mode=fast-local-authority`
  even though there is no selectable cache mode anymore
- active docs still describe `writeback-delay`, `cache mode`, or older
  durable-mode wording
- validation docs still mention callback semantics in terms of the removed
  fast-vs-durable split

That creates exactly the drift `SFS-269` is supposed to remove before
`SFS-270` locks the final validation matrix.

## Constraints From SFS-263 Through SFS-270

- `SFS-264` and `SFS-265` already finalized PG-backed hard-lock authority and
  enforcement. This issue must not reopen lock semantics.
- `SFS-266` already removed the product-level fast-vs-durable split. This issue
  must not reintroduce equivalent compatibility knobs.
- `SFS-267` already settled PG-offline behavior. This issue must not change the
  reconnect or headroom rules.
- `SFS-268` already tightened first-publish visibility and overlay semantics.
  This issue must only clean public wording around that behavior.
- `SFS-270` will rely on this cleanup so the validation matrix can describe one
  active contract without caveats.

## Chosen Approach

Perform a product-surface cutoff, not a broad internal rename.

### 1. Remove `writeback-delay` from the public CLI and persisted settings

`writeback-delay` no longer belongs in the supported operator surface. The
cleanup should:

- remove the field from `PublicRuntimeTuningArgs`
- drop the flag from `mount` / `settings` help text and parser expectations
- stop printing `writeback-delay` in settings summaries
- stop documenting it as a supported user knob

This is a true removal, not a deprecated no-op shim.

### 2. Stop treating `STUDIOFS_UPLOAD_DELAY_MS` as a public mount override

The raw VFS config may still carry `upload_delay_ms` internally, but the mount
entrypoint should stop honoring `STUDIOFS_UPLOAD_DELAY_MS` as part of the
public compatibility ladder.

That keeps the upload-delay implementation detail available where the runtime
still needs it internally without teaching operators that it remains part of
the supported product surface.

### 3. Remove `cache_mode` from mount profiling metadata

There is one supported mount contract now. Profiling tags and artifact names
should stop serializing a `cache_mode` dimension at all.

Keep useful tags such as:

- platform
- volume
- mount_mode
- bridge
- runtime

But drop `cache_mode` from both tag emission and artifact naming.

### 4. Rewrite active docs to describe one contract only

Update active docs so they no longer present:

- `--writeback-delay`
- transitional operator guidance centered on that flag
- `cache mode` as a meaningful product dimension
- durable-mode wording for `flush` / `close` semantics

The docs should instead consistently say:

- ordinary operators tune cache location/size plus read-side knobs
- the canonical contract is single-contract Fast Local Authority
- `flush`, `fsync`, and last-handle release describe local durability
  boundaries without any fast-vs-durable split

### 5. Add regression coverage for the cleaned surface

Add focused regression checks that fail if active CLI/docs surfaces reintroduce:

- `writeback-delay`
- `cache mode` as a supported product concept
- durable-mode callback wording

Parser tests should also assert that `mount` and `settings` reject
`--writeback-delay` as an unknown argument after removal.

## Affected Code Boundaries

- `crates/studiofs/src/cli/runtime_tuning.rs`
  Remove the public `writeback_delay` knob and summary line.
- `crates/studiofs/src/cli/settings.rs`
  Update parser/help/tests for the reduced public runtime tuning surface.
- `crates/studiofs/src/cli/mount/mod.rs`
  Remove `writeback-delay` from public help/examples/tests.
- `crates/studiofs/src/cli/mod.rs`
  Remove top-level help references to `writeback-delay`.
- `crates/studiofs/src/cli/mount/session.rs`
  Stop layering `STUDIOFS_UPLOAD_DELAY_MS` into public mount resolution and
  remove the now-misleading `cache_mode` plumbing from startup state.
- `crates/studiofs/src/cli/mount_profile.rs`
  Drop `cache_mode` tags and artifact-name components.
- `crates/studiofs/src/cli/mount_profile_stub.rs`
  Keep the non-Unix stub signature aligned with the real profiling API.
- `crates/studiofs/tests/architecture_vocabulary_test.rs`
  Extend regression coverage for the cleaned contract wording.
- `docs/configuration.md`
  Remove `writeback-delay`, cache-mode wording, and transitional guidance that
  still treats it as supported.
- `docs/examples.md`
  Remove the example that teaches `--writeback-delay`.
- `docs/local-first-validation-matrix.md`
  Replace the stale durable-mode wording with the current single-contract
  barrier language.

## Testing Strategy

### Red/green parser tests

- `mount` rejects `--writeback-delay`
- `settings` rejects `--writeback-delay`

### Focused config/profile tests

- mount config resolution no longer treats public runtime tuning as including
  upload delay
- env overlay does not change `upload_delay_ms` through the public mount path
- profile artifact naming/tag emission no longer depends on `cache_mode`

### Surface regression tests

- active docs and CLI surfaces contain no `writeback-delay`
- active docs contain no product-facing `cache mode` wording
- validation docs contain no “durable mode” callback wording

## Risks And Guards

- If the cleanup touches the raw VFS config instead of only the public surface,
  it may accidentally widen scope into runtime behavior changes. Guard by
  keeping `upload_delay_ms` internal where production code still uses it.
- If profiling tags keep `cache_mode` in artifact naming, old wording will
  survive even after docs are fixed. Guard with a focused profile regression
  test.
- If docs are cleaned without parser changes, operators will still discover the
  removed flag through `--help`. Guard by landing parser failures first.

## Out Of Scope

- renaming every internal `upload_delay_ms` or every `fast_*` metric purely for
  cosmetic consistency
- changing VFS upload scheduling semantics
- changing lock, offline, overlay, or publication behavior from
  `SFS-264` through `SFS-268`
- broad validation expansion owned by `SFS-270`
