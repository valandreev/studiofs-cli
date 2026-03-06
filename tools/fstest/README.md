# studiofs-fstest

One purpose: extract conflict/convergence metrics from a shared mount using 2 machines.

## Run (2 machines, same shared disk, different mount points allowed)

```bash
# Machine A
python fstest.py <mount_path_A> --role A --peers 2 --fs-name LucidLink --rounds 30 --run-id ll-smoke-001

# Machine B
python fstest.py <mount_path_B> --role B --peers 2 --fs-name LucidLink --rounds 30 --run-id ll-smoke-001
```

Run both commands at the same time.

Optional flags:

- `--no-progress` to disable per-round progress bar output.
- `--run-id <id>` to force both peers into one run namespace (recommended on multi-PC runs).

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

## Offline merge (when A/B reports were produced separately)

If cross-machine auto-merge did not happen, combine reports manually:

```bash
python fstest.py --role A --combine-results \
  E:/dev/studiofs-cli/tools/fstest/results-new/results_A.json \
  E:/dev/studiofs-cli/tools/fstest/results-new/results_B.json \
  --output E:/dev/studiofs-cli/tools/fstest/results-new/contract_summary.json
```
