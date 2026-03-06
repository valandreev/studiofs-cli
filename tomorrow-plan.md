# Tomorrow Plan

## 1. Finish `base + overlay` design for namespace/attrs

- Define the exact local metadata split:
  - `base_inodes`, `overlay_inodes`
  - `base_dentries`, `overlay_dentries`
  - tombstones / replace semantics
- Lock down merge rules for:
  - `lookup`
  - `readdir`
  - `getattr`
  - `rename`
  - `unlink`
  - `truncate`

## 2. Implement serving view as `overlay over base`

- Start with read path:
  - `getattr`
  - `lookup`
  - `readdir`
- Then move write-side mutations:
  - `create`
  - `unlink`
  - `rename`
  - `mkdir/rmdir`
  - `setattr`

## 3. Move foreign PG apply fully into `base`

- Remove remaining stop-gap behavior where foreign batches are blocked by local overlay state.
- Keep self-echo suppression.
- Preserve ordered batch apply semantics.

## 4. Add targeted tests for the new namespace model

- Local overlay must not be clobbered by foreign apply.
- Unrelated foreign changes should apply immediately.
- Conflict cases must converge deterministically.
- Remote `unlink` should remove the file from namespace immediately.
- `rename` and overwrite cases should converge correctly.

## 5. Prepare the first production-grade ingress step

- Do not jump straight into the full implementation.
- First define the concrete implementation slice:
  - local NVMe ingress queue
  - acceptance point
  - replay / recovery
  - backpressure policy
- Keep it aligned with [NVMe-ingress-architecture.md](./NVMe-ingress-architecture.md).

## 6. If time remains

- Clean up old `snapshot` / `Tonbo` references in archival docs and notes.
- Make sure docs no longer drift from the current code and runtime model.

## Morning Priority

1. `base + overlay` schema/design
2. `lookup/getattr/readdir`
3. write-side mutations
