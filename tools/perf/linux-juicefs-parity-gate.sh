#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

bench_bin="$repo_root/target/bench/studiofs-bench"
juicefs_repo="/home/val/dev/juicefs"
source_repo="$repo_root"
output_root="$repo_root/target/perf"
runs=5
workload_bytes=$((1024 * 1024 * 1024))
small_workload_bytes=$((256 * 1024 * 1024))
dry_run=false
dcc_smoke_command=""

local_target=""
studiofs_new_target=""
studiofs_old_target=""
studiofs_adaptive_target=""
juicefs_target=""

usage() {
  cat <<USAGE
Usage: linux-juicefs-parity-gate.sh --local-target <path> --studiofs-new-target <path> --studiofs-old-target <path> --studiofs-adaptive-target <path> --juicefs-target <path> [options]

Runs the SFS-598 Linux parity gate against already-mounted targets and writes
artifacts under target/perf by default.

Options:
  --bench <path>                 studiofs-bench binary. Defaults to target/bench/studiofs-bench.
  --runs <n>                     Repeated runs per target/workload. Defaults to 5.
  --workload-bytes <n>           Large sequential workload size. Defaults to 1 GiB.
  --small-workload-bytes <n>     Small-file workload size. Defaults to 256 MiB.
  --source-repo <path>           Repository copied by the repo-copy workload. Defaults to this repo.
  --juicefs-repo <path>          Local JuiceFS checkout used as behavior reference. Defaults to /home/val/dev/juicefs.
  --studiofs-old-target <path>   Mounted current-main/direct target.
  --studiofs-new-target <path>   Mounted feature/direct target.
  --studiofs-adaptive-target <path>
                                 Mounted feature/adaptive target.
  --dcc-smoke-command <command>  Optional command run with TARGET_PATH set for DCC-like compatibility smoke.
  --output-root <path>           Artifact root. Defaults to target/perf.
  --dry-run                      Write the artifact skeleton without running heavy I/O.
  -h, --help                     Show this help.

JuiceFS reference checked during planning:
  /home/val/dev/juicefs/cmd/bench.go
  /home/val/dev/juicefs/cmd/mdtest.go
  /home/val/dev/juicefs/.github/scripts/fio.sh
USAGE
}

require_value() {
  local option="$1"
  local value="${2:-}"
  if [[ -z "$value" ]]; then
    echo "Error: $option requires a value" >&2
    usage >&2
    exit 2
  fi
}

positive_int() {
  local option="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ || "$((10#$value))" -lt 1 ]]; then
    echo "Error: $option must be a positive integer" >&2
    exit 2
  fi
  printf '%s' "$((10#$value))"
}

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  printf '%s' "$value"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bench) require_value "$1" "${2:-}"; bench_bin="$2"; shift 2 ;;
    --runs) require_value "$1" "${2:-}"; runs="$(positive_int "$1" "$2")"; shift 2 ;;
    --workload-bytes) require_value "$1" "${2:-}"; workload_bytes="$(positive_int "$1" "$2")"; shift 2 ;;
    --small-workload-bytes) require_value "$1" "${2:-}"; small_workload_bytes="$(positive_int "$1" "$2")"; shift 2 ;;
    --source-repo) require_value "$1" "${2:-}"; source_repo="$2"; shift 2 ;;
    --juicefs-repo) require_value "$1" "${2:-}"; juicefs_repo="$2"; shift 2 ;;
    --dcc-smoke-command) require_value "$1" "${2:-}"; dcc_smoke_command="$2"; shift 2 ;;
    --output-root) require_value "$1" "${2:-}"; output_root="$2"; shift 2 ;;
    --local-target) require_value "$1" "${2:-}"; local_target="$2"; shift 2 ;;
    --studiofs-new-target) require_value "$1" "${2:-}"; studiofs_new_target="$2"; shift 2 ;;
    --studiofs-old-target) require_value "$1" "${2:-}"; studiofs_old_target="$2"; shift 2 ;;
    --studiofs-adaptive-target) require_value "$1" "${2:-}"; studiofs_adaptive_target="$2"; shift 2 ;;
    --juicefs-target) require_value "$1" "${2:-}"; juicefs_target="$2"; shift 2 ;;
    --dry-run) dry_run=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "Error: SFS-598 parity gate is Linux-only." >&2
  exit 2
fi

for required in local_target studiofs_new_target studiofs_old_target studiofs_adaptive_target juicefs_target; do
  if [[ -z "${!required}" ]]; then
    echo "Error: --${required//_/-} is required" >&2
    usage >&2
    exit 2
  fi
done

if [[ "$dry_run" == false && ! -x "$bench_bin" ]]; then
  echo "Error: studiofs-bench is not executable: $bench_bin" >&2
  exit 1
fi

bench_bin="$(realpath -m "$bench_bin")"
source_repo="$(realpath -m "$source_repo")"
juicefs_repo="$(realpath -m "$juicefs_repo")"
output_root="$(realpath -m "$output_root")"
local_target="$(realpath -m "$local_target")"
studiofs_new_target="$(realpath -m "$studiofs_new_target")"
studiofs_old_target="$(realpath -m "$studiofs_old_target")"
studiofs_adaptive_target="$(realpath -m "$studiofs_adaptive_target")"
juicefs_target="$(realpath -m "$juicefs_target")"

mkdir -p "$output_root"
timestamp="$(date -u +%Y%m%d-%H%M%S)"
artifact_dir="$output_root/sfs-598-linux-juicefs-parity-$timestamp"
suffix=2
while ! mkdir "$artifact_dir" 2>/dev/null; do
  artifact_dir="$output_root/sfs-598-linux-juicefs-parity-$timestamp-$suffix"
  suffix=$((suffix + 1))
done

git_commit="$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || true)"
cat > "$artifact_dir/environment.json" <<JSON
{
  "issue": "SFS-598",
  "created_utc": "$timestamp",
  "git_commit": "$(json_escape "$git_commit")",
  "bench_bin": "$(json_escape "$bench_bin")",
  "juicefs_repo": "$(json_escape "$juicefs_repo")",
  "source_repo": "$(json_escape "$source_repo")",
  "runs": $runs,
  "large_workload_bytes": $workload_bytes,
  "small_workload_bytes": $small_workload_bytes,
  "dry_run": $dry_run,
  "targets": {
    "local": "$(json_escape "$local_target")",
    "studiofs_feature_direct": "$(json_escape "$studiofs_new_target")",
    "studiofs_main_direct": "$(json_escape "$studiofs_old_target")",
    "studiofs_feature_adaptive": "$(json_escape "$studiofs_adaptive_target")",
    "juicefs": "$(json_escape "$juicefs_target")"
  }
}
JSON

declare -a target_names=("local" "studiofs_feature_direct" "studiofs_main_direct" "studiofs_feature_adaptive" "juicefs")
declare -a target_paths=("$local_target" "$studiofs_new_target" "$studiofs_old_target" "$studiofs_adaptive_target" "$juicefs_target")
declare -a workload_names=(
  "large-sequential-read-write"
  "repeated-warm-read"
  "small-files-4kib"
  "small-files-16kib"
  "small-files-64kib"
  "metadata-heavy-scan"
  "source-repo-copy"
  "truncate-rewrite"
  "fsync-heavy"
)
if [[ -n "$dcc_smoke_command" ]]; then
  workload_names+=("dcc-smoke")
fi

record_dry_run() {
  local run_dir="$1"
  local command_text="$2"
  printf '%s\n' "$command_text" > "$run_dir/command.txt"
  printf 'dry-run: command not executed\n' > "$run_dir/stdout.txt"
  : > "$run_dir/stderr.txt"
  cat > "$run_dir/metrics.json" <<JSON
{"status":"dry-run","wall_ms":0,"warning_count":0,"error_count":0,"unsupported_ioctl_count":0}
JSON
}

run_workload() {
  local target_name="$1"
  local target_path="$2"
  local workload="$3"
  local run="$4"
  local run_dir="$artifact_dir/$target_name/$workload/run-$(printf '%03d' "$run")"
  mkdir -p "$run_dir"

  local command_text=""
  case "$workload" in
    large-sequential-read-write)
      command_text="$(printf '%q ' "$bench_bin" --scripted --target "$target_path" --workload-bytes "$workload_bytes" --mode read-write --layout single-file --cache enabled --save-report)"
      ;;
    repeated-warm-read)
      command_text="python3 - $(printf '%q' "$target_path") $workload_bytes <<'PY'
import json, os, pathlib, sys, time
root = pathlib.Path(sys.argv[1])
size = int(sys.argv[2])
path = root / 'sfs-598-warm-read.bin'
chunk = b'w' * (8 * 1024 * 1024)
with path.open('wb') as f:
    remaining = size
    while remaining:
        part = chunk[:min(len(chunk), remaining)]
        f.write(part)
        remaining -= len(part)
    f.flush()
    os.fsync(f.fileno())
reads = []
for _ in range(2):
    start = time.monotonic()
    total = 0
    with path.open('rb') as f:
        while True:
            data = f.read(len(chunk))
            if not data:
                break
            total += len(data)
    reads.append({'bytes': total, 'wall_ms': round((time.monotonic() - start) * 1000, 3)})
print(json.dumps({'workload': 'repeated-warm-read', 'reads': reads}, sort_keys=True))
PY"
      ;;
    small-files-4kib|small-files-16kib|small-files-64kib)
      local small_kib
      small_kib="${workload#small-files-}"
      small_kib="${small_kib%kib}"
      command_text="python3 - $(printf '%q' "$target_path") $small_kib $run <<'PY'
import concurrent.futures
import json
import pathlib
import shutil
import sys
import time

target = pathlib.Path(sys.argv[1])
size = int(sys.argv[2]) * 1024
run = int(sys.argv[3])
root = target / f'studiofs-linux-cache-small-{size}-{run}'
shutil.rmtree(root, ignore_errors=True)
root.mkdir()
payload = bytes([size % 251]) * size

def write_one(i):
    (root / f'f-{i:05d}.bin').write_bytes(payload)

def read_stat_one(i):
    path = root / f'f-{i:05d}.bin'
    data = path.read_bytes()
    if len(data) != size:
        raise RuntimeError(f'short read: {path}')
    path.stat()

started = time.monotonic()
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    list(pool.map(write_one, range(10_000)))
write_ms = (time.monotonic() - started) * 1000

started = time.monotonic()
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    list(pool.map(read_stat_one, range(10_000)))
read_stat_ms = (time.monotonic() - started) * 1000

started = time.monotonic()
shutil.rmtree(root)
cleanup_ms = (time.monotonic() - started) * 1000
print(json.dumps({
    'workload': 'small-files',
    'file_count': 10_000,
    'threads': 8,
    'file_size': size,
    'write_ms': round(write_ms, 3),
    'read_stat_ms': round(read_stat_ms, 3),
    'cleanup_ms': round(cleanup_ms, 3),
}, sort_keys=True))
PY"
      ;;
    metadata-heavy-scan)
      command_text="python3 - $(printf '%q' "$target_path") <<'PY'
import json, pathlib, time, sys
root = pathlib.Path(sys.argv[1]) / 'sfs-598-metadata-scan'
root.mkdir(exist_ok=True)
for d in range(32):
    sub = root / f'd-{d:03d}'
    sub.mkdir(exist_ok=True)
    for f in range(64):
        (sub / f'f-{f:03d}.txt').write_text('x')
start = time.monotonic()
stats = 0
for path in root.rglob('*'):
    path.stat()
    stats += 1
print(json.dumps({'workload': 'metadata-heavy-scan', 'stats': stats, 'wall_ms': round((time.monotonic() - start) * 1000, 3)}, sort_keys=True))
PY"
      ;;
    source-repo-copy)
      command_text="mkdir -p $(printf '%q' "$target_path/sfs-598-repo-copy") && tar -C $(printf '%q' "$source_repo") --exclude=./target --exclude=./.git -cf - . | tar -C $(printf '%q' "$target_path/sfs-598-repo-copy") -xf -"
      ;;
    truncate-rewrite)
      command_text="python3 - $(printf '%q' "$target_path") <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]) / 'sfs-598-truncate-rewrite.bin'
with p.open('wb') as f:
    f.write(b'a' * (64 * 1024 * 1024))
    f.truncate(8 * 1024 * 1024)
    f.seek(4 * 1024 * 1024)
    f.write(b'b' * (8 * 1024 * 1024))
    f.flush()
    import os; os.fsync(f.fileno())
PY"
      ;;
    fsync-heavy)
      command_text="python3 - $(printf '%q' "$target_path") <<'PY'
import os, pathlib, sys
root = pathlib.Path(sys.argv[1]) / 'sfs-598-fsync-heavy'
root.mkdir(exist_ok=True)
for i in range(256):
    with (root / f'f-{i:04d}.bin').open('wb') as f:
        f.write(bytes([i % 251]) * 4096)
        f.flush()
        os.fsync(f.fileno())
PY"
      ;;
    dcc-smoke)
      command_text="TARGET_PATH=$(printf '%q' "$target_path") $dcc_smoke_command"
      ;;
  esac

  if [[ "$dry_run" == true ]]; then
    record_dry_run "$run_dir" "$command_text"
    return
  fi

  printf '%s\n' "$command_text" > "$run_dir/command.txt"
  local start_ns end_ns wall_ms status
  start_ns="$(date +%s%N)"
  status=0
  if [[ -x /usr/bin/time ]]; then
    (cd "$run_dir" && /usr/bin/time -v -o time.txt bash -o pipefail -c "$command_text") > "$run_dir/stdout.txt" 2> "$run_dir/stderr.txt" || status=$?
  else
    (cd "$run_dir" && bash -o pipefail -c "$command_text") > "$run_dir/stdout.txt" 2> "$run_dir/stderr.txt" || status=$?
  fi
  end_ns="$(date +%s%N)"
  wall_ms="$(((end_ns - start_ns) / 1000000))"
  if [[ "$workload" == "repeated-warm-read" ]]; then
    wall_ms="$(python3 - "$run_dir/stdout.txt" <<'PY'
import json
import pathlib
import sys

lines = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()
payload = json.loads(lines[-1])
print(round(float(payload["reads"][-1]["wall_ms"])))
PY
)"
  fi
  local warning_count error_count unsupported_ioctl_count
  warning_count="$( (grep -Eih 'warn|warning' "$run_dir/stdout.txt" "$run_dir/stderr.txt" 2>/dev/null || true) | wc -l | tr -d ' ')"
  error_count="$( (grep -Eih 'error|failed|panic' "$run_dir/stdout.txt" "$run_dir/stderr.txt" 2>/dev/null || true) | wc -l | tr -d ' ')"
  unsupported_ioctl_count="$( (grep -Eih 'unsupported.*ioctl|ioctl.*unsupported' "$run_dir/stdout.txt" "$run_dir/stderr.txt" 2>/dev/null || true) | wc -l | tr -d ' ')"
  cat > "$run_dir/metrics.json" <<JSON
{"status":$status,"wall_ms":$wall_ms,"warning_count":$warning_count,"error_count":$error_count,"unsupported_ioctl_count":$unsupported_ioctl_count}
JSON
  if [[ "$status" != 0 ]]; then
    echo "Workload failed: $target_name/$workload run $run. See $run_dir" >&2
    exit "$status"
  fi
}

for i in "${!target_names[@]}"; do
  for workload in "${workload_names[@]}"; do
    for ((run=1; run<=runs; run++)); do
      run_workload "${target_names[$i]}" "${target_paths[$i]}" "$workload" "$run"
    done
  done
done

summary_path="$artifact_dir/summary.md"
{
  echo "# SFS-598 Linux JuiceFS Parity Gate"
  echo
  echo "| Workload | local | feature/direct | current-main/direct | feature/adaptive | JuiceFS |"
  echo "|---|---:|---:|---:|---:|---:|"
  for workload in "${workload_names[@]}"; do
    printf '| `%s` ' "$workload"
    for target_name in "${target_names[@]}"; do
      count="$(find "$artifact_dir/$target_name/$workload" -name metrics.json | wc -l | tr -d ' ')"
      printf '| %s ' "$count"
    done
    echo "|"
  done
  echo
  echo "Each cell is the number of captured run artifacts. See statistics.md and gate.json for median/p95/p99 and threshold results."
} > "$summary_path"

python3 - "$artifact_dir" "${target_names[*]}" "${workload_names[*]}" "$dry_run" <<'PY'
import json
import math
import pathlib
import statistics
import sys

root = pathlib.Path(sys.argv[1])
targets = sys.argv[2].split()
workloads = sys.argv[3].split()
dry_run = sys.argv[4] == "true"

def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]

stats = {}
for target in targets:
    stats[target] = {}
    for workload in workloads:
        values = []
        for path in sorted((root / target / workload).glob("run-*/metrics.json")):
            metric = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(metric.get("status"), int) and metric["status"] == 0:
                values.append(float(metric["wall_ms"]))
        if values:
            stats[target][workload] = {
                "runs": len(values),
                "median_ms": statistics.median(values),
                "p95_ms": percentile(values, 0.95),
                "p99_ms": percentile(values, 0.99),
            }

comparisons = []
passed = not dry_run
if not dry_run:
    for workload in workloads:
        baseline = stats["studiofs_main_direct"].get(workload)
        direct = stats["studiofs_feature_direct"].get(workload)
        adaptive = stats["studiofs_feature_adaptive"].get(workload)
        if not baseline or not direct or not adaptive:
            comparisons.append({
                "workload": workload,
                "passed": False,
                "reason": "missing successful runs",
            })
            passed = False
            continue

        checks = {
            "direct_median_vs_main": direct["median_ms"] <= baseline["median_ms"] * 1.05,
            "direct_p95_vs_main": direct["p95_ms"] <= baseline["p95_ms"] * 1.10,
            "direct_p99_vs_main": direct["p99_ms"] <= baseline["p99_ms"] * 1.10,
            "adaptive_median_vs_direct": adaptive["median_ms"] <= direct["median_ms"] * 1.05,
            "adaptive_p95_vs_direct": adaptive["p95_ms"] <= direct["p95_ms"] * 1.05,
            "adaptive_p99_vs_direct": adaptive["p99_ms"] <= direct["p99_ms"] * 1.05,
        }
        workload_passed = all(checks.values())
        passed = passed and workload_passed
        comparisons.append({
            "workload": workload,
            "passed": workload_passed,
            "checks": checks,
        })

    warm_direct = stats["studiofs_feature_direct"].get("repeated-warm-read")
    warm_adaptive = stats["studiofs_feature_adaptive"].get("repeated-warm-read")
    warm_improved = bool(
        warm_direct
        and warm_adaptive
        and warm_adaptive["median_ms"] <= warm_direct["median_ms"] * 0.95
    )
    passed = passed and warm_improved
else:
    warm_improved = False

gate = {
    "status": "dry-run" if dry_run else ("passed" if passed else "failed"),
    "thresholds": {
        "feature_direct_median_regression_max_percent": 5,
        "feature_direct_p95_p99_regression_max_percent": 10,
        "adaptive_regression_max_percent": 5,
        "required_warm_read_improvement_percent": 5,
    },
    "warm_read_improved": warm_improved,
    "comparisons": comparisons,
}
(root / "statistics.json").write_text(
    json.dumps(stats, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
(root / "gate.json").write_text(
    json.dumps(gate, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)

lines = [
    "# Linux cache-policy statistics",
    "",
    "| Workload | Target | Median ms | p95 ms | p99 ms |",
    "|---|---|---:|---:|---:|",
]
for workload in workloads:
    for target in targets:
        item = stats[target].get(workload)
        if item:
            lines.append(
                f"| `{workload}` | `{target}` | "
                f"{item['median_ms']:.3f} | {item['p95_ms']:.3f} | "
                f"{item['p99_ms']:.3f} |"
            )
lines.extend(["", f"Gate status: **{gate['status']}**.", ""])
(root / "statistics.md").write_text("\n".join(lines), encoding="utf-8")
PY

cat > "$artifact_dir/decision.md" <<'MD'
# SFS-598 Rollout Decision

Choose one after reviewing the side-by-side artifacts:

- [ ] Default adaptive Linux cache policy.
- [ ] Keep the new Linux backend opt-in and iterate.
- [ ] Stop this path and pivot to the separate Curvine-style investigation.

Notes:

- Source repository copy must complete without unsupported-ioctl log spam or severe throughput collapse.
- Warm reads must use safe cache behavior and approach local OS-cache/JuiceFS repeated-read behavior.
- `gate.json` must pass, and runtime stats must separately prove that the
  second clean warm pass adds zero VFS read bytes.
- Do not count microbenchmarks alone as success.
MD

echo "$artifact_dir"
if [[ "$dry_run" == false ]] && ! grep -q '"status": "passed"' "$artifact_dir/gate.json"; then
  echo "Linux cache-policy performance thresholds failed; see $artifact_dir/gate.json" >&2
  exit 1
fi
