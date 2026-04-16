# SFS-237 Ingress Legacy Cleanup Design

## Goal

Remove the remaining `ingress_spill` / ingress-checkpoint legacy runtime scaffolding from
StudioFS so ZeroIO v1 uses a single physical payload cache with a clean runtime layout and
non-misleading logs.

## Approved Direction

- Remove runtime creation and use of `cache_root/ingress_spill`.
- Move the ZeroIO graceful-shutdown snapshot out of the ingress namespace into a dedicated
  runtime directory under the cache root.
- Remove dead ingress checkpoint / spill runtime metrics, configuration knobs, and status
  plumbing that no longer participate in the production write/read/replay path.
- Keep focused ZeroIO shutdown/replay coverage so we prove the cleanup does not weaken the
  current local-durable unpublished-file contract.

## Non-Goals

- Re-introducing any second payload tier.
- Changing the active Cache 3.0 payload storage contract under `cache_root/v3/**`.
- Running the full workspace test suite in this task.

## Safety Constraints

- ZeroIO local payload persistence must remain rooted in Cache 3.0 only.
- Graceful shutdown must still persist and reload the ZeroIO shutdown snapshot.
- Focused tests must cover the new runtime layout and confirm we do not recreate
  `ingress_spill` on the active path.
