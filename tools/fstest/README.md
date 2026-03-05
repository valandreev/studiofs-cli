# studiofs-fstest

One purpose: extract conflict/convergence metrics from a shared mount using 2 machines.

## Run (2 machines, same shared disk, different mount points allowed)

```bash
# Machine A
python fstest.py <mount_path_A> --role A --peers 2 --fs-name LucidLink --rounds 30

# Machine B
python fstest.py <mount_path_B> --role B --peers 2 --fs-name LucidLink --rounds 30
```

Run both commands at the same time.

## What metrics it extracts

- visibility delay `A -> B` and `B -> A` (`p50/p95/p99/max`)
- concurrent overwrite winner distribution
- concurrent rename winner distribution
- delete-while-open behavior:
  - delete success rate (other client)
  - read-after-delete behavior (holder client)

## Output

Per-role reports:

- `<mount>/.fstest/<run_id>/results_A.json`
- `<mount>/.fstest/<run_id>/results_B.json`

Combined contract (written by role `A`):

- `<mount>/.fstest/<run_id>/contract_summary.json`

Use `contract_summary.json` as the reference contract for StudioFS behavior matching.
