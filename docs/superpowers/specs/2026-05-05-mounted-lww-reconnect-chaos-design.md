# Mounted LWW Reconnect Chaos Design

## Goal

Add a product E2E scenario that catches a class of bugs not covered by plain
mounted tree convergence: all clients can converge to the same state, but the
state can still be the wrong last-writer-wins winner.

## Scenario Name

`mounted-lww-reconnect-chaos-convergence`

Aliases:

- `lww-chaos`
- `reconnect-lww`

## Shape

Use five clients against one shared remote volume:

- `client-a`, `client-b`, `client-c`, and `client-d` mount as warm clients.
- `client-c` is the stale writer. It is force-killed after warmup and returns
  later with the same client root/cache.
- `client-e` is a late cold observer with a fresh local root/cache.

The first slice uses mounted crash/remount as the offline boundary. True
per-client PG/S3 network partitioning remains a later Toxiproxy-backed layer.

## Workload

1. Create `lww-chaos/winner.txt` and wait for warm-client convergence.
2. Force-kill `client-c`, leaving it offline with stale local state.
3. Write ordered version 1 from `client-a`.
4. Write ordered version 2 from `client-b`.
5. Wait for convergence among the online warm clients.
6. Remount `client-c`, write ordered version 3 from `client-c`, and mark it as
   the expected LWW winner in `operations.ndjson`.
7. Force-kill and remount `client-c` again before final convergence to exercise
   replay/publication of the expected winner.
8. Mount `client-e` as a cold observer.
9. Validate final convergence across all five clients and compare
   `lww-chaos/winner.txt` against the oracle checksum for `client-c` order 3.

## Validation

The scenario fails if:

- mounted trees do not converge across all five clients;
- namespace invariants fail;
- all clients converge but `lww-chaos/winner.txt` does not match the expected
  `client-c` order-3 checksum.

## Artifacts

The scenario writes:

- `operations.ndjson`
- `mounted-lww-reconnect-chaos-summary.json`
- `lww-oracle.json`
- `validation.json`

The operation log records `oracle_order` for ordered writes and
`expected_winner=true` for the third write.

## Non-Goals

- No per-client Toxiproxy partition in this slice.
- No private PG, object-store, or redb inspection to decide pass/fail.
- No broad stress fanout; the path is intentionally narrow so failures are
  easy to reproduce from one seed.
