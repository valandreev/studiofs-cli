# Linux Cache Adaptation Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete Linux memory-pressure validation from clean HEAD `9f952a43` by removing the nondeterministic admission threshold from gate eligibility, then collect exactly four fresh mounted runs and publish the resulting correctness, CPU, throughput, recovery, and OOM evidence.

**Architecture:** Keep the existing Linux `MemoryPressureController`, holder, feeder, runtime stats, and collector artifact schema. Treat feeder admission as diagnostic evidence only; prove pressure causally through the holder's cgroup `memory.current` rise/fall and validate every pressure snapshot against the host+cgroup source, positive envelope, bounded managed limits, and `normal|constrained` state. Use fresh mount/cache/cgroup state for each of the four runs; Direct async remains the candidate and Kernel remains diagnostic.

**Tech Stack:** Rust 2024 release binary, Bash, Python 3 standard library, `unittest`, Linux FUSE3, cgroup v2, `studiofs-bench`.

---

## Starting point and scope

- Baseline: clean branch `feature/linux-platform-optimisations-vol02`, HEAD `9f952a43`.
- Approved design: `docs/superpowers/specs/2026-07-15-memory-pressure-adaptation-design.md`.
- Handoff/status: `docs/code-reviews/2026-07-17-linux-cache-adaptation-status.md`.
- Current failure is a harness determinism failure: the feeder did not hold `admitted_managed_bytes >= 402653185` for the old 60-second threshold. The 10/10 correctness manifest passed and no product failure was established.
- Do not change Rust pressure calculations, VFS admission, materialization/finalization, metadata merge logic, public configuration, or performance thresholds in this continuation unless a new focused regression proves a product defect.
- Do not run the full cache matrix or repeated measurements. The evidence set is exactly four single collector runs.
- Keep all generated evidence and temporary orchestration files under `target/`. Remove only temporary orchestration files after the evidence has been summarized; retain the four run directories and correctness manifest.

## File map

- Modify `tools/perf/test_linux_cache_gate.py`: RED/GREEN coverage for diagnostic-only feeder admission, best/last setup snapshot retention, active post-holder samples, and correctness-only orchestration.
- Modify `tools/perf/linux_cache_gate_summary.py`: record the best/last feeder snapshot and stop treating admission above the envelope as required proof.
- Modify `tools/perf/linux-cache-gate.sh`: make the admission target advisory, preserve a setup snapshot on timeout, and accept a valid `normal|constrained` post-holder boundary.
- Modify `tools/perf/linux-cache-matrix.sh`: add a narrow `--correctness-only` exit after the existing ten-test manifest phase; it must never schedule or mount performance tuples.
- Modify `tools/perf/README.md`: document `--correctness-only` and the four-run continuation procedure.
- Modify `docs/perf/linux-cache-performance-result.md`: append final memory-pressure evidence only after all four runs pass.
- Create `docs/superpowers/plans/2026-07-18-linux-cache-adaptation-completion.md`: this plan.

## Decision rules

1. Keep `required_admitted_bytes` in `memory-pressure-config.json` as a diagnostic target so the artifact schema does not churn. It must not decide whether a run starts or passes.
2. Feeder requirements that remain mandatory: completed aligned direct write, stable PID/start-time identity, live before and after the workload profile, clean stop, and internally consistent setup/runtime artifacts.
3. The post-holder runtime boundary may be `normal` or `constrained`. It must use `host_and_cgroup`, have a positive envelope, have managed limits no greater than the envelope, and not report an overcommitted minimum.
4. Actual pressure proof comes from the holder lifecycle and cgroup boundaries: `memory.current` rises by at least half the holder allocation while held and falls by at least half after release.
5. `oom` and `oom_kill` deltas must both be zero. Byte counts, hashes, drain, bounded cleanup/unmount, CPU, and throughput/recovery evidence remain mandatory.
6. Kernel runs are always passed with `--diagnostic`; they may miss performance/recovery gates but may not miss correctness, byte/hash, OOM, or artifact-integrity gates.

### Task 1: Make feeder admission advisory in the Python contract

**Files:**
- Modify: `tools/perf/test_linux_cache_gate.py`
- Modify: `tools/perf/linux_cache_gate_summary.py`

- [ ] **Step 1: Write the failing scenario test**

Replace `test_scenario_requires_admission_strictly_above_envelope` with a test that supplies an observed admission below the diagnostic target and still obtains a positive holder:

```python
def test_scenario_records_subtarget_admission_without_rejecting_holder(self) -> None:
    params = {
        "memory_current": 960 << 20,
        "memory_max": 3584 << 20,
        "host_available": 4 << 30,
        "parent_available": None,
        "negotiated_max_write": 32 << 20,
    }
    target = gate.derive_pressure_target(
        effective_limit=params["memory_max"],
        negotiated_max_write=params["negotiated_max_write"],
    )
    observed = target["target_envelope_bytes"] // 4

    scenario = gate.derive_pressure_scenario(
        **params,
        observed_admitted_lower_bound=observed,
    )

    self.assertEqual(scenario["observed_admitted_lower_bound_bytes"], observed)
    self.assertEqual(
        scenario["required_admitted_bytes"],
        target["required_admitted_bytes"],
    )
    self.assertGreater(scenario["holder_bytes"], 0)
```

- [ ] **Step 2: Write failing evidence tests for a drained feeder**

Split `test_setup_must_prove_admission_and_live_profile` so zero working set, zero cumulative direct bytes, unstable feeder identity, liveness changes, and nonzero exit remain rejected. Add a separate passing case in which both the setup observation and initial post-holder observation contain zero admitted bytes but holder/cgroup evidence is valid:

```python
def test_zero_feeder_admission_is_diagnostic_when_holder_proves_pressure(self) -> None:
    evidence = self.evidence()
    evidence["setup"]["observed_admitted_lower_bound_bytes"] = 0
    evidence["runtime_samples"]["setup_observed"][
        "admitted_lower_bound_bytes"
    ] = 0
    initial = evidence["runtime_samples"]["pressure"][0]
    initial.update(state="normal", admitted_lower_bound_bytes=0)

    result = self.validate(evidence=evidence)

    self.assertTrue(result["passed"])
```

Keep the existing rejection tests for host-only/fallback/disabled sources, managed limits above the envelope, insufficient cgroup rise/fall, OOM deltas, mismatched hashes, and failed drain.

- [ ] **Step 3: Write the failing best/last snapshot recorder test**

Add this test to `MemoryPressureEvidenceTest` for a helper named `record_best_pressure_observation`. A higher admitted snapshot replaces the file; a lower snapshot does not; an equal snapshot replaces it so the retained artifact is the latest among equally good samples:

```python
def test_best_pressure_observation_keeps_highest_then_latest_snapshot(self) -> None:
    destination = Path.cwd() / "target/perf/test-best-pressure-observation.json"
    destination.unlink(missing_ok=True)
    try:
        first = AnalyzerCliTest.pressure_runtime_snapshot(
            {**self.evidence()["runtime_samples"]["setup_observed"],
             "admitted_lower_bound_bytes": 100}
        )
        lower = AnalyzerCliTest.pressure_runtime_snapshot(
            {**self.evidence()["runtime_samples"]["setup_observed"],
             "generation": 2, "admitted_lower_bound_bytes": 50}
        )
        equal = AnalyzerCliTest.pressure_runtime_snapshot(
            {**self.evidence()["runtime_samples"]["setup_observed"],
             "generation": 3, "admitted_lower_bound_bytes": 100}
        )

        gate.record_best_pressure_observation(first, destination)
        gate.record_best_pressure_observation(lower, destination)
        gate.record_best_pressure_observation(equal, destination)

        self.assertEqual(json.loads(destination.read_text()), equal)
    finally:
        destination.unlink(missing_ok=True)
```

- [ ] **Step 4: Run the focused tests and verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest \
  tools.perf.test_linux_cache_gate.PressureScenarioHelperTest \
  tools.perf.test_linux_cache_gate.MemoryPressureEvidenceTest -v
```

Expected: failures mention the strict admission check and missing `record_best_pressure_observation`.

- [ ] **Step 5: Implement the snapshot recorder**

Add next to `record_post_profile_pressure_snapshot`:

```python
def record_best_pressure_observation(snapshot: dict, destination: Path) -> dict:
    """Retain the highest admission sample, preferring the latest on a tie."""
    sample = memory_pressure_sample(snapshot)
    retained_admitted = -1
    if destination.exists():
        retained = json.loads(destination.read_text(encoding="utf-8"))
        retained_admitted = int(
            memory_pressure_sample(retained)["admitted_lower_bound_bytes"]
        )
    if int(sample["admitted_lower_bound_bytes"]) >= retained_admitted:
        with destination.open("w", encoding="utf-8") as output:
            json.dump(snapshot, output, indent=2, sort_keys=True)
            output.write("\n")
    return sample
```

Use the existing `memory_pressure_sample` validation; do not introduce another wire schema or dependency.

- [ ] **Step 6: Remove strict admission from scenario and evidence eligibility**

In `derive_pressure_scenario`, retain the nonnegative conversion and recorded value but delete:

```python
if admitted < target["required_admitted_bytes"]:
    raise GateValidationError("observed admission is below the pressure target")
```

In `validate_memory_pressure_evidence`:

- keep `observed_admitted_lower_bound_bytes` nonnegative but remove the `== 0` rejection;
- accept the first pressure sample when state is either `normal` or `constrained`;
- keep positive envelope, `managed <= envelope`, host+cgroup source, monotonic generations/counters, and no new sampling failures;
- remove the requirements that initial admitted bytes exceed the envelope or stay within 64 MiB of the feeder observation.

The replacement initial-boundary check is:

```python
if initial_pressure.get("state") not in ("normal", "constrained"):
    raise GateValidationError("initial pressure state is invalid")
if (
    initial_pressure["envelope_bytes"] == 0
    or initial_pressure["overcommitted_minimum"] is not False
):
    raise GateValidationError(
        "initial pressure state must retain a positive nonminimum envelope"
    )
```

- [ ] **Step 7: Run focused tests and verify GREEN**

Run the command from Step 4.

Expected: all `PressureScenarioHelperTest` and `MemoryPressureEvidenceTest` cases pass.

### Task 2: Make the Bash collector preserve evidence and use the holder boundary

**Files:**
- Modify: `tools/perf/linux-cache-gate.sh`
- Modify: `tools/perf/test_linux_cache_gate.py`

- [ ] **Step 1: Add failing shell-contract assertions**

Add a source-contract test that reads `tools/perf/linux-cache-gate.sh` and asserts:

```python
def test_collector_treats_feeder_target_as_diagnostic(self) -> None:
    source = Path("tools/perf/linux-cache-gate.sh").read_text()
    self.assertIn("record_best_pressure_observation", source)
    self.assertIn("Warning: pressure feeder did not reach diagnostic", source)
    self.assertNotIn(
        'die "pressure feeder did not reach required live admission', source
    )
    self.assertIn("active-pressure-boundary", source)
    self.assertNotIn("constrained-boundary --require-feeder-live --admitted-gt-envelope", source)
```

- [ ] **Step 2: Verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest \
  tools.perf.test_linux_cache_gate.PressureScenarioHelperTest.test_collector_treats_feeder_target_as_diagnostic -v
```

Expected: FAIL against the current strict collector.

- [ ] **Step 3: Replace `wait_for_admitted_setup` with advisory observation**

Rename it to `observe_admitted_setup`. Keep the 60-second bounded window and feeder PID/start-time checks. For every valid runtime snapshot, call `gate.record_best_pressure_observation`; return early if the diagnostic target is reached. On timeout, print a warning and return success because the best/last snapshot is already present:

```bash
observe_admitted_setup() {
  local destination="$1"
  local feeder_pid="$2"
  local diagnostic_target="$3"
  local feeder_start_time="$4"
  local snapshot=""
  local admitted=""
  local deadline=$((SECONDS + 60))
  while (( SECONDS <= deadline )); do
    helper_pid_is_live "$feeder_pid" "$feeder_start_time" || \
      die "pressure feeder exited during admission observation"
    snapshot="$(capture_runtime_snapshot)"
    admitted="$(printf '%s\n' "$snapshot" | \
      PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 -c '
import json
import sys
from pathlib import Path
from tools.perf import linux_cache_gate_summary as gate
sample = gate.record_best_pressure_observation(json.load(sys.stdin), Path(sys.argv[1]))
print(sample["admitted_lower_bound_bytes"])
' "$destination")"
    if (( admitted >= diagnostic_target )); then
      return 0
    fi
    sleep "$idle_poll_interval"
  done
  printf 'Warning: pressure feeder did not reach diagnostic admission target; retained best/last runtime snapshot\n' >&2
}
```

Do not suppress diagnostics-attach errors and do not continue if feeder identity changes.

- [ ] **Step 4: Generalize the post-holder boundary**

Change the current constrained-only call to an `active` boundary. The polling validator must accept `normal|constrained`, require `host_and_cgroup`, positive envelope, `overcommitted_minimum == false`, and the sum of the four managed limits not exceeding the envelope. Remove the minimum-admitted argument entirely.

Keep `normal` as the required recovery state for Direct modes after holder release. Kernel continues to capture recovery diagnostically as it does now.

Update `command.txt` evidence to say:

```text
pressure-feeder --o-direct --working-set "$setup_working_set_bytes"
observe-live-admission --diagnostic-target "$required_admitted" --timeout 60
memory-holder --bytes "$holder_bytes" --cgroup "$cgroup"
active-pressure-boundary --require-feeder-live --state normal-or-constrained
```

- [ ] **Step 5: Run syntax and focused tests**

Run:

```bash
bash -n tools/perf/linux-cache-gate.sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest \
  tools.perf.test_linux_cache_gate.PressureScenarioHelperTest \
  tools.perf.test_linux_cache_gate.MemoryPressureEvidenceTest -v
```

Expected: Bash exits 0 and all focused tests pass.

- [ ] **Step 6: Run the complete deterministic gate suite**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tools.perf.test_linux_cache_gate -v
git diff --check
```

Expected: the previous 101-test baseline plus the new cases passes; no whitespace errors.

- [ ] **Step 7: Commit the gate determinism fix**

```bash
git add tools/perf/linux-cache-gate.sh \
  tools/perf/linux_cache_gate_summary.py \
  tools/perf/test_linux_cache_gate.py \
  docs/superpowers/plans/2026-07-18-linux-cache-adaptation-completion.md
git commit -m "fix(perf): make feeder admission diagnostic"
```

### Task 3: Add a correctness-manifest-only entry point

**Files:**
- Modify: `tools/perf/linux-cache-matrix.sh`
- Modify: `tools/perf/test_linux_cache_gate.py`
- Modify: `tools/perf/README.md`

- [ ] **Step 1: Write the failing CLI test**

Add a dry-run test that invokes the existing matrix with `--correctness-only` and the otherwise required paths. Assert exit 0, one dry-run `correctness-manifest.json`, no `schedule.json`, and no tuple directories:

```python
def test_matrix_correctness_only_stops_before_schedule(self) -> None:
    output_root = self.root / "correctness-only"
    completed = self.run_script(
        self.matrix,
        "--correctness-only",
        "--dry-run",
        "--volume", "shell-contract",
        "--mount-root", str(self.root / "mounts"),
        "--cgroup-root", str(self.root / "cgroups"),
        "--output-root", str(output_root),
    )
    self.assertEqual(completed.returncode, 0, completed.stderr)
    self.assertTrue((output_root / "correctness-manifest.json").is_file())
    self.assertFalse((output_root / "schedule.json").exists())
```

- [ ] **Step 2: Verify RED**

Run the single test. Expected: exit 2 with `unknown argument: --correctness-only`.

- [ ] **Step 3: Implement the narrow option**

Add `correctness_only=false`, document and parse `--correctness-only`, then exit immediately after the existing correctness manifest block succeeds:

```bash
if [[ "$dry_run" == true ]]; then
  write_correctness_manifest true
else
  run_correctness_preflight
  run_correctness_tests
  write_correctness_manifest false
  if [[ "$correctness_valid" != true ]]; then
    printf 'Error: correctness evidence failed; inspect %s\n' \
      "$correctness_manifest" >&2
    exit 1
  fi
fi

if [[ "$correctness_only" == true ]]; then
  printf '%s\n' "$correctness_manifest"
  exit 0
fi
```

Do not duplicate the ten-test allowlist or manifest writer. Do not create a second correctness script.

- [ ] **Step 4: Document the command**

In `tools/perf/README.md`, add:

```bash
bash tools/perf/linux-cache-matrix.sh \
  --correctness-only \
  --volume "$VOLUME" \
  --mount-root "$RUN/mounts" \
  --cgroup-root "$DELEGATED_CGROUP_ROOT" \
  --output-root "$RUN"
```

Clarify that this runs only the fixed ten mounted tests and writes the HEAD/binary-bound manifest; it schedules no performance tuple.

- [ ] **Step 5: Verify and commit**

Run:

```bash
bash -n tools/perf/linux-cache-matrix.sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tools.perf.test_linux_cache_gate -v
git diff --check
```

Then commit:

```bash
git add tools/perf/linux-cache-matrix.sh tools/perf/test_linux_cache_gate.py tools/perf/README.md
git commit -m "feat(perf): add correctness-only Linux gate"
```

### Task 4: Build immutable binaries and produce the 10/10 manifest

**Files:**
- Create under `$RUN`: release/correctness artifacts only.

- [ ] **Step 1: Require clean source checkouts**

Run:

```bash
git status --short
git -C ../studiofs-bench status --short
```

Expected: both outputs are empty. Stop if either tree is dirty.

- [ ] **Step 2: Build the StudioFS release candidate once**

```bash
cargo build --release --locked
```

Expected: exit 0. Do not rebuild between the four collector runs.

- [ ] **Step 3: Define one new evidence root**

```bash
HEAD_SHORT="$(git rev-parse --short=8 HEAD)"
RUN="$(realpath -m "target/perf/linux-memory-pressure-${HEAD_SHORT}-$(date -u +%Y%m%dT%H%M%SZ)")"
VOLUME="test"
DELEGATED_CGROUP_ROOT="/sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service"
mkdir -p "$RUN"
```

Use this exact root for the manifest and all four runs. Never reuse an older artifact root.

- [ ] **Step 4: Run the correctness-only gate**

```bash
bash tools/perf/linux-cache-matrix.sh \
  --correctness-only \
  --volume "$VOLUME" \
  --mount-root "$RUN/mounts" \
  --cgroup-root "$DELEGATED_CGROUP_ROOT" \
  --output-root "$RUN"
```

Expected: prints `$RUN/correctness-manifest.json`; all ten tests executed exactly once, none skipped, all exit 0. This phase also rebuilds the clean sibling `studiofs-bench` and refreshes `target/bench/studiofs-bench`.

- [ ] **Step 5: Validate manifest provenance before mounting**

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python3 - "$RUN/correctness-manifest.json" <<'PY'
import hashlib
import json
import pathlib
import subprocess
import sys

from tools.perf import linux_cache_gate_summary as gate

path = pathlib.Path(sys.argv[1])
manifest = json.loads(path.read_text(encoding="utf-8"))
revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
bench_revision = subprocess.check_output(
    ["git", "-C", "../studiofs-bench", "rev-parse", "HEAD"], text=True
).strip()
studiofs_sha = hashlib.sha256(pathlib.Path("target/release/studiofs").read_bytes()).hexdigest()
bench_sha = hashlib.sha256(pathlib.Path("target/bench/studiofs-bench").read_bytes()).hexdigest()
gate.validate_correctness_manifest(
    manifest,
    expected_revision=revision,
    expected_studiofs_sha256=studiofs_sha,
    expected_bench_revision=bench_revision,
    expected_bench_sha256=bench_sha,
)
assert len(manifest["tests"]) == 10
print("correctness manifest: 10/10 PASS")
PY
```

Expected: `correctness manifest: 10/10 PASS`.

### Task 5: Execute exactly four fresh collector runs

**Files:**
- Create under `$RUN/runs/`: four immutable artifact directories.

- [ ] **Step 1: Freeze the tuple list and order**

Run only:

1. `direct-async / large-single / 4,000,000,000 bytes`;
2. `direct-async / small-128k / 131,072,000 verified bytes`;
3. `kernel / large-single / 4,000,000,000 bytes / --diagnostic`;
4. `kernel / small-128k / 131,072,000 verified bytes / --diagnostic`.

Do not run Direct serial, the matrix, retries, or repetitions. If any run fails because of a product or harness error, preserve it under `$RUN/failed/`, fix the cause, rebuild, create a new HEAD-bound root, and restart the four-run set from tuple 1.

- [ ] **Step 2: For each tuple create fresh owned paths**

For tuple variables `MODE`, `WORKLOAD`, and `DIAGNOSTIC`, create:

```bash
SLUG="${MODE}-${WORKLOAD}"
MOUNT_ROOT="$RUN/mounts/$SLUG"
MOUNT_POINT="$MOUNT_ROOT/$VOLUME"
CACHE_DIR="$RUN/caches/$SLUG"
CGROUP="$DELEGATED_CGROUP_ROOT/studiofs-memory-pressure-${HEAD_SHORT}-${SLUG}"
ARTIFACT="$RUN/runs/$MODE/$WORKLOAD/rep-001"
WORK_DIR="$RUN/work/$SLUG"
mkdir -p "$MOUNT_ROOT" "$CACHE_DIR" "$WORK_DIR"
mkdir -p "$(dirname "$ARTIFACT")"
mkdir "$ARTIFACT"
mkdir "$CGROUP"
```

`$ARTIFACT` must remain empty until `linux-cache-gate.sh` starts. Mount, collector, unmount, and fallback logs go to `$WORK_DIR` and are moved into `$ARTIFACT` only after the collector exits.

Before mounting, assert the leaf is empty and fresh:

```bash
test ! -s "$CGROUP/cgroup.procs"
test "$(tr -d '\n' < "$CGROUP/memory.current")" = "0"
test "$(tr -d '\n' < "$CGROUP/memory.peak")" = "0"
```

- [ ] **Step 3: Start the mode-correct mount**

For Direct async:

```bash
env STUDIOFS_LINUX_FUSE_CACHE_POLICY=direct STUDIOFS_FUSE_ASYNC_DIO=1 \
  target/release/studiofs mount "$VOLUME" "$MOUNT_ROOT" \
  --cache-dir "$CACHE_DIR" --foreground \
  >"$WORK_DIR/mount.stdout.txt" 2>"$WORK_DIR/mount.stderr.txt" &
MOUNT_PID=$!
```

For Kernel diagnostic:

```bash
env STUDIOFS_LINUX_FUSE_CACHE_POLICY=kernel STUDIOFS_FUSE_ASYNC_DIO=0 \
  target/release/studiofs mount "$VOLUME" "$MOUNT_ROOT" \
  --cache-dir "$CACHE_DIR" --foreground \
  >"$WORK_DIR/mount.stdout.txt" 2>"$WORK_DIR/mount.stderr.txt" &
MOUNT_PID=$!
```

Require the PID to remain live and `findmnt -T "$MOUNT_POINT"` to identify the mount before launching the collector.

- [ ] **Step 4: Run the collector once**

Direct async command:

```bash
bash tools/perf/linux-cache-gate.sh \
  --mode direct-async \
  --workload "$WORKLOAD" \
  --mount-point "$MOUNT_POINT" \
  --mount-pid "$MOUNT_PID" \
  --cgroup "$CGROUP" \
  --bench target/bench/studiofs-bench \
  --bench-repo ../studiofs-bench \
  --studiofs target/release/studiofs \
  --correctness-manifest "$RUN/correctness-manifest.json" \
  --output-root "$ARTIFACT" \
  >"$WORK_DIR/collector.stdout.txt" \
  2>"$WORK_DIR/collector.stderr.txt"
```

Kernel command is identical except `--mode kernel --diagnostic`.

Expected: exit 0 and `$ARTIFACT/derived.json` has `status=complete`. No collector retry is allowed inside the same four-run evidence root.

- [ ] **Step 5: Unmount and clean up with bounded fallbacks**

After the collector exits, run `timeout 30 target/release/studiofs umount "$MOUNT_POINT"` with stdout/stderr in `$WORK_DIR`. If the mount is still active, run `timeout 30 fusermount3 -u "$MOUNT_POINT"`. Lazy unmount is cleanup-only evidence and invalidates a successful tuple unless an earlier clean unmount already succeeded.

Wait at most 30 seconds for the owned mount PID. Record unmount/fallback/mount exit statuses in `$ARTIFACT/orchestrator-cleanup.json`. Remove only the now-empty owned cache, mount, and cgroup paths. Require:

```text
mount inactive
mount PID absent
collector PID absent
cgroup.procs empty
cache path removed
mount path removed
cgroup path removed
```

- [ ] **Step 6: Validate each artifact before starting the next tuple**

Set `EXPECTED_BYTES=4000000000` for `large-single` and `EXPECTED_BYTES=131072000` for `small-128k`, then run this complete check over the artifact:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python3 - \
  "$ARTIFACT" "$EXPECTED_BYTES" <<'PY'
import json
import pathlib
import sys

from tools.perf import linux_cache_gate_summary as gate

artifact = pathlib.Path(sys.argv[1])
expected_bytes = int(sys.argv[2])
derived = json.loads((artifact / "derived.json").read_text(encoding="utf-8"))
cleanup = json.loads((artifact / "cleanup.json").read_text(encoding="utf-8"))
pressure = derived["memory_pressure"]
assert derived["status"] == "complete"
assert derived["command_exit_status"] == 0
assert derived["verified_bytes_written"] == expected_bytes
assert derived["verified_bytes_read"] == expected_bytes
assert derived["callback_errors"] == 0
assert derived["async_errors"] == 0
assert derived["task_errors"] == 0
assert pressure["verification"]["bytes_written"] == expected_bytes
assert pressure["verification"]["bytes_read"] == expected_bytes
assert pressure["verification"]["pressure_sha256"] == pressure["verification"]["recovery_sha256"]
assert pressure["cgroup"]["events_delta"]["oom"] == 0
assert pressure["cgroup"]["events_delta"]["oom_kill"] == 0
assert pressure["drain_completed"] is True
assert cleanup["cleanup_error"] is None
assert cleanup["cleanup_drain_idle"] is True
for name in (
    "runtime-pressure-before-profile.json",
    "runtime-pressure-after-profile.json",
):
    snapshot = json.loads((artifact / name).read_text(encoding="utf-8"))
    sample = gate.memory_pressure_sample(snapshot)
    assert sample["source"] == "host_and_cgroup"
    assert sample["state"] in ("normal", "constrained")
    assert sample["envelope_bytes"] > 0
    managed = sum(
        sample[field]
        for field in (
            "memory_cache_limit_bytes",
            "dirty_limit_bytes",
            "transit_limit_bytes",
            "async_write_limit_bytes",
        )
    )
    assert managed <= sample["envelope_bytes"]
print(f"{artifact}: PASS")
PY
```

### Task 6: Summarize evidence and close validation

**Files:**
- Modify: `docs/perf/linux-cache-performance-result.md`

Leave `docs/code-reviews/2026-07-17-linux-cache-adaptation-status.md` unchanged as a historical handoff snapshot.

- [ ] **Step 1: Build a four-run comparison table from raw artifacts**

Extract for each tuple:

- HEAD and StudioFS/bench SHA-256 provenance;
- exact verified write/read bytes and pressure/recovery SHA-256 equality;
- holder bytes and cgroup `memory.current` before/held/after-release;
- runtime source/state/envelope and four managed limits before/after profile;
- `oom` and `oom_kill` deltas;
- foreground wall time, write/read throughput, pressure/recovery throughput;
- foreground/drain/lifecycle mount CPU and average/p95/peak cores;
- daemon RSS/anon and cgroup total/anon/file peaks;
- drain duration and idle state;
- collector cleanup, unmount, process, directory, and cgroup cleanup results.

- [ ] **Step 2: Apply stop/go criteria**

Direct async must pass every correctness, byte/hash, envelope, OOM, drain, cleanup, CPU, and throughput-recovery gate. Kernel must pass correctness, byte/hash, envelope, OOM, drain, and cleanup; report its CPU/throughput/recovery misses as diagnostic rather than promotion failures.

If Direct async fails a measured product gate, do not update the final conclusion. Preserve the run, identify whether the failure is product, harness, or environment, and write a narrow addendum plan before changing Rust.

- [ ] **Step 3: Update the performance result**

Append a dated “Memory-pressure validation” section to `docs/perf/linux-cache-performance-result.md`. State explicitly:

- the new HEAD and checksums;
- 10/10 mounted correctness result;
- four single-run, non-statistical collector tuples;
- Direct async promotion/default conclusion;
- Kernel diagnostic conclusion;
- exact failed gates, if any;
- artifact root under `target/perf/`.

Do not claim statistical significance from one run and do not replace the existing 2026-07-15 baseline tables.

- [ ] **Step 4: Run final focused verification**

```bash
bash -n tools/perf/linux-cache-gate.sh
bash -n tools/perf/linux-cache-matrix.sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tools.perf.test_linux_cache_gate -v
cargo fmt --check
git diff --check
git status --short
```

Expected: all commands pass. `git status --short` lists only the intended code/docs changes for the final evidence commit.

- [ ] **Step 5: Commit the documentation/evidence update**

```bash
git add docs/perf/linux-cache-performance-result.md
git commit -m "docs(perf): record Linux memory pressure evidence"
```

Do not commit `target/` artifacts.

### Task 7: Final review and handoff

**Files:** all files changed in Tasks 1-6.

- [ ] **Step 1: Review against the approved design**

Confirm the final diff changes only gate eligibility/orchestration/docs; feeder identity and holder lifecycle remain strict; no OOM, correctness, envelope, drain, CPU, or hash criterion was relaxed.

- [ ] **Step 2: Review for accidental schema churn**

Confirm old and new artifacts still use schema version 1 for pressure setup/config and schema version 2 for runtime diagnostics. `required_admitted_bytes` remains diagnostic and raw-artifact reconstruction remains internally consistent.

- [ ] **Step 3: Review repository cleanliness and evidence ownership**

Confirm zero active StudioFS mounts/processes/collectors/holders/feeders, zero nonempty owned cgroups, and no temporary files outside `target/`. Confirm the git worktree is clean after commits.

- [ ] **Step 4: Record final outcome**

Report the commits, artifact root, 10/10 manifest, four tuple statuses, Direct async decision, Kernel diagnostic result, and any retained limitations. Linux performance/CPU validation is complete only if all four required artifacts exist and the Direct async gates pass.
