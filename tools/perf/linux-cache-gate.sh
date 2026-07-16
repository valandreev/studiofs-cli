#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

mode=""
workload=""
mount_point=""
mount_pid=""
cgroup=""
bench_bin="$repo_root/target/bench/studiofs-bench"
bench_repo="$repo_root/../studiofs-bench"
studiofs_bin="$repo_root/target/release/studiofs"
correctness_manifest=""
output_root="$repo_root/target/perf/linux-cache-gate"
workload_bytes=4000000000
dry_run=false
diagnostic=false

usage() {
  cat <<'USAGE'
Usage: linux-cache-gate.sh --mode <mode> --workload <workload> --mount-point <path> --mount-pid <pid> [options]

Creates one StudioFS Linux cache/CPU gate artifact directory.

Options:
  --mode <mode>                   direct-serial, direct-async, or kernel.
  --workload <workload>           large-single, fixed-1m, small-128k,
                                  warm-reopen, same-file-parallel, or
                                  cross-file-barriers.
  --mount-point <path>            Mounted StudioFS path.
  --mount-pid <pid>               StudioFS mount process PID.
  --cgroup <path>                 Delegated cgroup v2 directory (real runs).
  --bench <path>                  Benchmark binary. Defaults to target/bench/studiofs-bench.
  --bench-repo <path>             Benchmark source checkout. Defaults to ../studiofs-bench.
  --studiofs <path>               StudioFS binary. Defaults to target/release/studiofs.
  --correctness-manifest <path>   Correctness manifest (real runs).
  --output-root <path>            Artifact root. Defaults to target/perf/linux-cache-gate.
  --workload-bytes <n>            Configured workload bytes. Defaults to 4000000000.
  --diagnostic                    Mark real output promotion-ineligible.
  --dry-run                       Write the complete artifact skeleton without real I/O.
  -h, --help                      Show this help.
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

json_placeholder() {
  local path="$1"
  local phase="$2"
  printf '{"status":"dry-run","phase":"%s"}\n' "$phase" > "$path"
}

die() {
  echo "Error: $*" >&2
  exit 1
}

capture_runtime_snapshot() {
  if ! "$studiofs_bin" internal diagnostics-attach "$mount_point" \
    --recent-events-limit 1 --log-level error | \
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 -c '
import json
import sys

from tools.perf import linux_cache_gate_summary as gate

try:
    payload = json.load(sys.stdin)
    snapshot = gate.validate_diagnostics_attach(
        payload,
        mount_pid=int(sys.argv[1]),
        mount_point=sys.argv[2],
    )
except (json.JSONDecodeError, OSError, ValueError, gate.GateValidationError) as error:
    raise SystemExit(f"Error: diagnostics-attach validation failed: {error}") from error
json.dump(snapshot, sys.stdout, separators=(",", ":"), sort_keys=True)
' "$mount_pid" "$mount_point"; then
    die "diagnostics-attach failed"
  fi
}

runtime_idle_state() {
  printf '%s\n' "$1" | PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 -c '
import json
import sys

from tools.perf import linux_cache_gate_summary as gate

try:
    snapshot = json.load(sys.stdin)
    idle = gate.snapshot_is_idle(snapshot)
    print("idle" if idle else "busy")
except (json.JSONDecodeError, ValueError, gate.GateValidationError) as error:
    raise SystemExit(f"Error: idle snapshot validation failed: {error}") from error
'
}

mount_start_time() {
  PYTHONDONTWRITEBYTECODE=1 python3 - "$1" <<'PY'
import pathlib
import sys

value = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
closing = value.rfind(")")
fields = value[closing + 2 :].split()
if closing < 0 or len(fields) < 20:
    raise SystemExit("Error: malformed mount process stat")
print(fields[19])
PY
}

verify_cgroup_membership() {
  local membership_file="$1"
  local label="$2"
  local membership=""
  local hierarchy controllers path
  while IFS=: read -r hierarchy controllers path; do
    if [[ "$hierarchy" == "0" && -z "$controllers" ]]; then
      membership="$path"
      break
    fi
  done < "$membership_file"
  if [[ -z "$membership" || "$(realpath -m -- "$cgroup_mount$membership")" != "$cgroup" ]]; then
    die "$label did not join delegated cgroup $cgroup"
  fi
}

move_pid_to_cgroup() {
  local pid="$1"
  local membership_file="$2"
  local label="$3"
  printf '%s\n' "$pid" > "$cgroup/cgroup.procs" || \
    die "failed to move $label into cgroup"
  verify_cgroup_membership "$membership_file" "$label"
}

monotonic_ns() {
  PYTHONDONTWRITEBYTECODE=1 python3 - <<'PY'
import time

print(time.monotonic_ns())
PY
}

stamp_runtime_snapshot() {
  local path="$1"
  PYTHONDONTWRITEBYTECODE=1 python3 - "$path" <<'PY'
import json
import pathlib
import sys
import time

path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text())
payload["captured_monotonic_ns"] = time.monotonic_ns()
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY
}

capture_proc_boundary() {
  local phase="$1"
  local proc_dir="$artifact_dir/proc-$phase"
  local proc_file=""
  mkdir "$proc_dir"
  for proc_file in stat status smaps_rollup io; do
    [[ -r "$proc_root/$mount_pid/$proc_file" ]] || \
      die "mount process $proc_file is unavailable at $phase"
    cp -- "$proc_root/$mount_pid/$proc_file" "$proc_dir/$proc_file.txt"
  done
  [[ "$(mount_start_time "$proc_dir/stat.txt")" == "$mount_pid_start_time" ]] || \
    die "mount PID was reused at $phase"
}

capture_cgroup_boundary() {
  local phase="$1"
  local destination="$artifact_dir/cgroup-$phase.json"
  CACHE_GATE_PHASE="$phase" PYTHONDONTWRITEBYTECODE=1 python3 - \
    "$cgroup/memory.current" "$cgroup/memory.peak" "$cgroup/memory.stat" \
    "$destination" <<'PY'
import json
import os
import pathlib
import sys
import time


def read_integer(path: pathlib.Path) -> int:
    value = int(path.read_text(encoding="utf-8").strip())
    if value < 0:
        raise ValueError(f"negative cgroup value in {path}")
    return value


try:
    stat = {}
    for line in pathlib.Path(sys.argv[3]).read_text(encoding="utf-8").splitlines():
        name, raw_value = line.split()
        value = int(raw_value)
        if value < 0:
            raise ValueError(f"negative cgroup memory.stat value: {name}")
        stat[name] = value
    for required in ("anon", "file"):
        if required not in stat:
            raise ValueError(f"cgroup memory.stat is missing {required}")
    payload = {
        "schema_version": 1,
        "phase": os.environ["CACHE_GATE_PHASE"],
        "monotonic_ns": time.monotonic_ns(),
        "memory_current": read_integer(pathlib.Path(sys.argv[1])),
        "memory_peak": read_integer(pathlib.Path(sys.argv[2])),
        "memory_stat": stat,
    }
    pathlib.Path(sys.argv[4]).write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
except (OSError, ValueError) as error:
    raise SystemExit(f"Error: cgroup snapshot failed: {error}") from error
PY
}

wait_for_idle_boundary() {
  local destination="$1"
  local snapshot=""
  local state=""
  local streak=0
  local deadline=$((SECONDS + 600))
  local retained="$artifact_dir/.idle-boundary.jsonl"
  : > "$retained"
  while (( SECONDS <= deadline )); do
    snapshot="$(capture_runtime_snapshot)"
    state="$(runtime_idle_state "$snapshot")"
    if [[ "$state" == idle ]]; then
      if (( streak == 0 )); then
        : > "$retained"
      fi
      printf '%s\n' "$snapshot" >> "$retained"
      streak=$((streak + 1))
      if (( streak == 3 )); then
        printf '%s\n' "$snapshot" > "$destination"
        rm -f -- "$retained"
        return 0
      fi
    else
      streak=0
      : > "$retained"
    fi
    sleep "$idle_poll_interval"
  done
  rm -f -- "$retained"
  die "mount did not reach three consecutive idle snapshots within 600 seconds"
}

pidstat_pid=""
pidstat_start_time=""
memory_sampler_pid=""
memory_sampler_start_time=""
memory_holder_pid=""
memory_holder_start_time=""
memory_holder_start=""
memory_holder_ready=""
memory_holder_release=""
pressure_setup_pid=""
pressure_setup_start_time=""
pressure_setup_ready=""
pressure_setup_release=""
pressure_workload_pid=""
pressure_workload_start_time=""

helper_pid_matches_start_time() {
  local pid="$1"
  local expected="$2"
  local root="${proc_root:-/proc}"
  local actual=""
  [[ -n "$expected" && -r "$root/$pid/stat" ]] || return 1
  actual="$(mount_start_time "$root/$pid/stat")" || return 1
  [[ "$actual" == "$expected" ]]
}

helper_pid_is_live() {
  local pid="$1"
  local expected_start_time="${2:-}"
  local root="${proc_root:-/proc}"
  local stat_line=""
  local rest=""
  kill -0 "$pid" 2>/dev/null || return 1
  if [[ -r "$root/$pid/stat" ]]; then
    IFS= read -r stat_line < "$root/$pid/stat"
    rest="${stat_line##*) }"
    [[ "${rest:0:1}" != Z ]] || return 1
  fi
  [[ -z "$expected_start_time" ]] || \
    helper_pid_matches_start_time "$pid" "$expected_start_time"
  return 0
}

wait_for_helper_exit() {
  local pid="$1"
  local expected_start_time="${2:-}"
  local deadline=$((SECONDS + 5))
  while helper_pid_is_live "$pid" "$expected_start_time"; do
    (( SECONDS < deadline )) || return 1
    sleep 0.1
  done
}

stop_helper_bounded() {
  local pid="$1"
  local label="$2"
  local expected_start_time="$3"
  local exit_status=0
  [[ -n "$expected_start_time" ]] || return 1
  if [[ -n "$expected_start_time" ]] && kill -0 "$pid" 2>/dev/null && \
    ! helper_pid_matches_start_time "$pid" "$expected_start_time"; then
    return 0
  fi
  if helper_pid_is_live "$pid" "$expected_start_time"; then
    if ! kill -TERM "$pid" 2>/dev/null && helper_pid_is_live "$pid" "$expected_start_time"; then
      return 1
    fi
    if ! wait_for_helper_exit "$pid" "$expected_start_time" && \
      helper_pid_is_live "$pid" "$expected_start_time"; then
      if ! kill -KILL "$pid" 2>/dev/null && \
        helper_pid_is_live "$pid" "$expected_start_time"; then
        return 1
      fi
      wait_for_helper_exit "$pid" "$expected_start_time" || return 1
    fi
  fi
  if wait "$pid" 2>/dev/null; then
    exit_status=0
  else
    exit_status=$?
  fi
  case "$label:$exit_status" in
    pidstat:0|pidstat:130|pidstat:143|memory-sampler:0|memory-holder:0|memory-holder:143|pressure-setup:0|pressure-setup:143|pressure-workload:0|pressure-workload:143) return 0 ;;
    *) return 1 ;;
  esac
}

stop_helpers_on_exit() {
  if [[ -n "$pressure_workload_pid" ]]; then
    if ! stop_helper_bounded "$pressure_workload_pid" pressure-workload \
      "$pressure_workload_start_time"; then
      printf 'Error: failed to stop pressure workload during collector cleanup\n' >&2
    fi
    pressure_workload_pid=""
    pressure_workload_start_time=""
  fi
  if [[ -n "$pidstat_pid" ]]; then
    if ! stop_helper_bounded "$pidstat_pid" pidstat "$pidstat_start_time"; then
      printf 'Error: failed to stop pidstat during collector cleanup\n' >&2
    fi
    pidstat_pid=""
    pidstat_start_time=""
  fi
  if [[ -n "$memory_sampler_pid" ]]; then
    if ! stop_helper_bounded "$memory_sampler_pid" memory-sampler \
      "$memory_sampler_start_time"; then
      printf 'Error: failed to stop memory sampler during collector cleanup\n' >&2
    fi
    memory_sampler_pid=""
    memory_sampler_start_time=""
  fi
  if [[ -n "$memory_holder_pid" ]]; then
    [[ -z "$memory_holder_release" ]] || touch "$memory_holder_release"
    if ! stop_helper_bounded "$memory_holder_pid" memory-holder \
      "$memory_holder_start_time"; then
      printf 'Error: failed to stop memory holder during collector cleanup\n' >&2
    fi
    memory_holder_pid=""
    memory_holder_start_time=""
  fi
  if [[ -n "$pressure_setup_pid" ]]; then
    [[ -z "$pressure_setup_release" ]] || touch "$pressure_setup_release"
    if ! stop_helper_bounded "$pressure_setup_pid" pressure-setup \
      "$pressure_setup_start_time"; then
      printf 'Error: failed to stop pressure setup during collector cleanup\n' >&2
    fi
    pressure_setup_pid=""
    pressure_setup_start_time=""
  fi
  rm -f -- "$memory_holder_start" "$memory_holder_ready" "$memory_holder_release"
  rm -f -- "$pressure_setup_ready" "$pressure_setup_release"
  if [[ -n "${CACHE_GATE_INTERNAL_CLEANUP_HOOK:-}" ]]; then
    if [[ -n "${CACHE_GATE_INTERNAL_WORKLOAD_PID_FILE:-}" ]] && \
      helper_pid_is_live "$(<"$CACHE_GATE_INTERNAL_WORKLOAD_PID_FILE")"; then
      printf 'workload-live\n' > "$CACHE_GATE_INTERNAL_CLEANUP_HOOK"
    else
      printf 'workload-stopped\n' > "$CACHE_GATE_INTERNAL_CLEANUP_HOOK"
    fi
  fi
}

stop_measurement_helpers() {
  helper_pid_is_live "$pidstat_pid" "$pidstat_start_time" || \
    die "pidstat exited before drain-end"
  helper_pid_is_live "$memory_sampler_pid" "$memory_sampler_start_time" || \
    die "memory sampler exited before drain-end"
  stop_helper_bounded "$pidstat_pid" pidstat "$pidstat_start_time" || \
    die "failed to stop pidstat within 10 seconds"
  pidstat_pid=""
  pidstat_start_time=""
  stop_helper_bounded "$memory_sampler_pid" memory-sampler \
    "$memory_sampler_start_time" || \
    die "failed to stop memory sampler within 10 seconds"
  memory_sampler_pid=""
  memory_sampler_start_time=""
}

wait_for_file() {
  local path="$1"
  local label="$2"
  local helper_pid="${3:-$memory_holder_pid}"
  local helper_start_time="${4:-}"
  local deadline=$((SECONDS + 30))
  while [[ ! -s "$path" ]]; do
    helper_pid_is_live "$helper_pid" "$helper_start_time" || die "helper exited before $label"
    (( SECONDS < deadline )) || die "timed out waiting for helper $label"
    sleep 0.05
  done
}

capture_memory_event_boundary() {
  local phase="$1"
  local destination="$2"
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 - \
    "$phase" "$cgroup" "$destination" <<'PY'
import json
import pathlib
import sys
import time
from tools.perf import linux_cache_gate_summary as gate

phase, cgroup, destination = sys.argv[1:]
try:
    payload = gate.read_cgroup_memory_boundary(pathlib.Path(cgroup), phase)
    pathlib.Path(destination).write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
except (OSError, ValueError, gate.GateValidationError) as error:
    raise SystemExit(f"Error: memory event boundary failed: {error}") from error
PY
}

wait_for_memory_pressure_state() {
  local expected="$1"
  local destination="$2"
  local workload_pid="${3:-}"
  local minimum_admitted="${4:-1}"
  local workload_start_time="${5:-}"
  local snapshot=""
  local matched=""
  local deadline=$((SECONDS + 30))
  while (( SECONDS <= deadline )); do
    snapshot="$(capture_runtime_snapshot)"
    matched="$(printf '%s\n' "$snapshot" | PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" \
      python3 -c '
import json
import sys
from tools.perf import linux_cache_gate_summary as gate
expected = sys.argv[1]
sample = gate.memory_pressure_sample(json.load(sys.stdin))
valid = sample["state"] == expected and sample["source"] == "host_and_cgroup"
if expected == "constrained":
    valid = valid and sample["envelope_bytes"] > 0
    valid = valid and not sample["overcommitted_minimum"]
    valid = valid and sample["admitted_lower_bound_bytes"] >= int(sys.argv[2])
print("yes" if valid else "no")
' "$expected" "$minimum_admitted")"
    if [[ "$matched" == yes ]]; then
      [[ -z "$workload_pid" ]] || helper_pid_is_live "$workload_pid" "$workload_start_time" || \
        die "pressure workload exited before constrained evidence"
      printf '%s\n' "$snapshot" > "$destination"
      return 0
    fi
    [[ -z "$workload_pid" ]] || helper_pid_is_live "$workload_pid" "$workload_start_time" || \
      die "pressure workload exited before constrained evidence"
    sleep "$idle_poll_interval"
  done
  die "memory pressure state did not become $expected within 30 seconds"
}

wait_for_admitted_setup() {
  local destination="$1"
  local feeder_pid="$2"
  local required_admitted="$3"
  local feeder_start_time="$4"
  local snapshot=""
  local admitted=""
  local deadline=$((SECONDS + 60))
  while (( SECONDS <= deadline )); do
    helper_pid_is_live "$feeder_pid" "$feeder_start_time" || \
      die "pressure feeder exited before admission target"
    snapshot="$(capture_runtime_snapshot)"
    admitted="$(printf '%s\n' "$snapshot" | PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" \
      python3 -c '
import json
import sys
from tools.perf import linux_cache_gate_summary as gate
print(gate.memory_pressure_sample(json.load(sys.stdin))["admitted_lower_bound_bytes"])
')"
    if (( admitted >= required_admitted )); then
      printf '%s\n' "$snapshot" > "$destination"
      return 0
    fi
    sleep "$idle_poll_interval"
  done
  die "pressure feeder did not reach required live admission within 60 seconds"
}

capture_constrained_pressure_once() {
  local destination="$1"
  local feeder_pid="$2"
  local feeder_start_time="$3"
  local minimum_admitted="$4"
  local snapshot=""
  helper_pid_is_live "$feeder_pid" "$feeder_start_time" || \
    die "pressure feeder identity changed before post-profile evidence"
  snapshot="$(capture_runtime_snapshot)"
  if ! printf '%s\n' "$snapshot" | PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" \
    python3 -c '
import json
import sys
from tools.perf import linux_cache_gate_summary as gate
sample = gate.memory_pressure_sample(json.load(sys.stdin))
valid = sample["source"] == "host_and_cgroup"
valid = valid and sample["state"] == "constrained"
valid = valid and sample["envelope_bytes"] > 0
valid = valid and not sample["overcommitted_minimum"]
valid = valid and sample["admitted_lower_bound_bytes"] >= int(sys.argv[1])
raise SystemExit(0 if valid else 1)
' "$minimum_admitted"; then
    die "post-profile runtime sample is not constrained under the live feeder"
  fi
  helper_pid_is_live "$feeder_pid" "$feeder_start_time" || \
    die "pressure feeder identity changed during post-profile evidence"
  printf '%s\n' "$snapshot" > "$destination"
}

run_inline_client() {
  local action="$1"
  local run_dir="$2"
  local output="$3"
  local bytes="$4"
  local workers="$5"
  shift 5
  PYTHONDONTWRITEBYTECODE=1 python3 - "$action" "$run_dir" "$output" \
    "$bytes" "$workers" "$@" <<'PY'
import concurrent.futures
import hashlib
import json
import mmap
import os
import pathlib
import signal
import sys
import threading
import time

ACTION = sys.argv[1]
RUN_DIR = pathlib.Path(sys.argv[2])
OUTPUT = pathlib.Path(sys.argv[3])
WORKLOAD_BYTES = int(sys.argv[4])
PARALLELISM = int(sys.argv[5]) if sys.argv[5] else None
EXTRA = sys.argv[6:]
MIB = 1024 * 1024
REGION_BYTES = 64 * MIB


def write_json(payload: dict) -> None:
    temporary = OUTPUT.with_suffix(OUTPUT.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(OUTPUT)


def payload(label: str, size: int) -> bytes:
    seed = hashlib.sha256(label.encode("utf-8")).digest()
    return (seed * ((size + len(seed) - 1) // len(seed)))[:size]


def write_all(handle, value: bytes) -> None:
    view = memoryview(value)
    while view:
        written = handle.write(view)
        if not isinstance(written, int) or written <= 0:
            raise OSError("short workload write")
        view = view[written:]


def write_stream(path: pathlib.Path, total: int, label: str) -> str:
    digest = hashlib.sha256()
    block = payload(label, MIB)
    remaining = total
    with path.open("xb", buffering=0) as handle:
        while remaining:
            chunk = block[: min(remaining, len(block))]
            write_all(handle, chunk)
            digest.update(chunk)
            remaining -= len(chunk)
        os.fsync(handle.fileno())
    return digest.hexdigest()


def read_stream(path: pathlib.Path, total: int) -> str:
    if path.stat().st_size != total:
        raise ValueError(f"workload file size mismatch: {path}")
    digest = hashlib.sha256()
    observed = 0
    with path.open("rb", buffering=0) as handle:
        while observed < total:
            chunk = handle.read(min(MIB, total - observed))
            if not chunk:
                raise ValueError(f"short workload read: {path}")
            digest.update(chunk)
            observed += len(chunk)
        if handle.read(1):
            raise ValueError(f"workload file grew during read: {path}")
    if observed != total:
        raise ValueError(f"workload read byte mismatch: {path}")
    return digest.hexdigest()


def throughput(byte_count: int, started_ns: int, ended_ns: int) -> float:
    elapsed = (ended_ns - started_ns) / 1_000_000_000
    if elapsed <= 0:
        raise ValueError("nonpositive workload phase duration")
    return byte_count / 1_000_000 / elapsed


def workload_files() -> list[pathlib.Path]:
    paths = sorted(RUN_DIR.rglob("*"))
    if any(path.is_symlink() for path in paths):
        raise ValueError("pressure workload tree contains a symlink")
    files = [path for path in paths if path.is_file()]
    if not files:
        raise ValueError("pressure workload tree has no files")
    return files


def hash_workload() -> tuple[int, str]:
    digest = hashlib.sha256()
    observed = 0
    for path in workload_files():
        with path.open("rb", buffering=0) as handle:
            while chunk := handle.read(MIB):
                digest.update(chunk)
                observed += len(chunk)
    if observed != WORKLOAD_BYTES:
        raise ValueError(
            f"pressure workload byte mismatch: expected {WORKLOAD_BYTES}, got {observed}"
        )
    return observed, digest.hexdigest()


def measure_workload_read() -> tuple[int, float, int, int]:
    observed = 0
    started = time.monotonic_ns()
    for path in workload_files():
        with path.open("rb", buffering=0) as handle:
            while chunk := handle.read(MIB):
                observed += len(chunk)
    ended = time.monotonic_ns()
    if observed != WORKLOAD_BYTES:
        raise ValueError(
            f"pressure workload byte mismatch: expected {WORKLOAD_BYTES}, got {observed}"
        )
    return observed, throughput(observed, started, ended), started, ended


def common_report(
    *,
    workload: str,
    file_count: int,
    bytes_written: int,
    bytes_read: int,
    expected_checksum: str,
    actual_checksum: str,
    phase_throughput: dict[str, float],
    **extra: object,
) -> dict:
    report = {
        "workload": workload,
        "file_count": file_count,
        "bytes_written": bytes_written,
        "bytes_read": bytes_read,
        "expected_checksum": expected_checksum,
        "actual_checksum": actual_checksum,
        "completed": True,
        "cleanup_error": None,
        "phase_throughput_mb_per_second": phase_throughput,
    }
    report.update(extra)
    return report


if ACTION == "small-128k":
    file_bytes = 131_072
    file_count = 1_000
    expected = hashlib.sha256()
    write_started = time.monotonic_ns()
    paths = []
    for index in range(file_count):
        path = RUN_DIR / f"small-{index:04d}.bin"
        value = payload(f"small-{index}", file_bytes)
        with path.open("xb", buffering=0) as handle:
            write_all(handle, value)
            os.fsync(handle.fileno())
        expected.update(value)
        paths.append(path)
    write_ended = time.monotonic_ns()
    actual = hashlib.sha256()
    read_started = time.monotonic_ns()
    for path in paths:
        if path.stat().st_size != file_bytes:
            raise ValueError(f"small-file size mismatch: {path}")
        with path.open("rb", buffering=0) as handle:
            value = handle.read()
        if len(value) != file_bytes:
            raise ValueError(f"small-file read mismatch: {path}")
        actual.update(value)
    read_ended = time.monotonic_ns()
    total = file_count * file_bytes
    write_json(common_report(
        workload=ACTION,
        file_count=file_count,
        bytes_written=total,
        bytes_read=total,
        expected_checksum=expected.hexdigest(),
        actual_checksum=actual.hexdigest(),
        phase_throughput={
            "write": throughput(total, write_started, write_ended),
            "read": throughput(total, read_started, read_ended),
        },
    ))
elif ACTION == "warm-prep":
    started = time.monotonic_ns()
    checksum = write_stream(RUN_DIR / "warm.bin", WORKLOAD_BYTES, "warm")
    ended = time.monotonic_ns()
    write_json({
        "completed": True,
        "bytes_written": WORKLOAD_BYTES,
        "checksum": checksum,
        "throughput_mb_per_second": throughput(WORKLOAD_BYTES, started, ended),
    })
elif ACTION == "warm-read":
    if len(EXTRA) != 2:
        raise SystemExit("warm-read requires prep path and pass number")
    prep = json.loads(pathlib.Path(EXTRA[0]).read_text(encoding="utf-8"))
    pass_number = int(EXTRA[1])
    if prep.get("completed") is not True or prep.get("bytes_written") != WORKLOAD_BYTES:
        raise ValueError("warm preparation evidence is invalid")
    started = time.monotonic_ns()
    checksum = read_stream(RUN_DIR / "warm.bin", WORKLOAD_BYTES)
    ended = time.monotonic_ns()
    write_json({
        "pass_number": pass_number,
        "completed": True,
        "bytes_read": WORKLOAD_BYTES,
        "expected_checksum": prep.get("checksum"),
        "actual_checksum": checksum,
        "throughput_mb_per_second": throughput(WORKLOAD_BYTES, started, ended),
    })
elif ACTION == "warm-aggregate":
    if len(EXTRA) != 3:
        raise SystemExit("warm-aggregate requires prep and two pass paths")
    prep = json.loads(pathlib.Path(EXTRA[0]).read_text(encoding="utf-8"))
    passes = [
        json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        for path in EXTRA[1:]
    ]
    if prep.get("completed") is not True or prep.get("bytes_written") != WORKLOAD_BYTES:
        raise ValueError("warm preparation evidence is invalid")
    for pass_number, report in enumerate(passes, 1):
        if (
            report.get("pass_number") != pass_number
            or report.get("completed") is not True
            or report.get("bytes_read") != WORKLOAD_BYTES
            or report.get("expected_checksum") != prep.get("checksum")
            or report.get("actual_checksum") != prep.get("checksum")
        ):
            raise ValueError(f"warm read pass {pass_number} is invalid")
    expected = hashlib.sha256()
    actual = hashlib.sha256()
    for report in passes:
        expected.update(bytes.fromhex(prep["checksum"]))
        actual.update(bytes.fromhex(report["actual_checksum"]))
    write_json(common_report(
        workload="warm-reopen",
        file_count=1,
        bytes_written=0,
        bytes_read=2 * WORKLOAD_BYTES,
        expected_checksum=expected.hexdigest(),
        actual_checksum=actual.hexdigest(),
        phase_throughput={
            f"read_pass_{index}": report["throughput_mb_per_second"]
            for index, report in enumerate(passes, 1)
        },
    ))
elif ACTION == "same-file-parallel":
    if PARALLELISM is None or not 1 <= PARALLELISM <= 8:
        raise ValueError("same-file parallelism is invalid")
    path = RUN_DIR / "same-file.bin"
    total = PARALLELISM * REGION_BYTES
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
        os.ftruncate(fd, total)

        def write_region(index: int) -> None:
            block = payload(f"same-{index}", MIB)
            offset = index * REGION_BYTES
            remaining = REGION_BYTES
            while remaining:
                view = memoryview(block[: min(remaining, len(block))])
                while view:
                    written = os.pwrite(fd, view, offset)
                    if written <= 0:
                        raise OSError("short parallel pwrite")
                    offset += written
                    remaining -= written
                    view = view[written:]

        write_started = time.monotonic_ns()
        with concurrent.futures.ThreadPoolExecutor(max_workers=PARALLELISM) as executor:
            futures = [executor.submit(write_region, index) for index in range(PARALLELISM)]
            for future in futures:
                future.result()
        os.fsync(fd)
        write_ended = time.monotonic_ns()
    finally:
        os.close(fd)
    expected = hashlib.sha256()
    for index in range(PARALLELISM):
        block = payload(f"same-{index}", MIB)
        for _ in range(REGION_BYTES // len(block)):
            expected.update(block)
    read_started = time.monotonic_ns()
    actual_checksum = read_stream(path, total)
    read_ended = time.monotonic_ns()
    write_json(common_report(
        workload=ACTION,
        file_count=1,
        bytes_written=total,
        bytes_read=total,
        expected_checksum=expected.hexdigest(),
        actual_checksum=actual_checksum,
        phase_throughput={
            "write": throughput(total, write_started, write_ended),
            "read": throughput(total, read_started, read_ended),
        },
        parallelism=PARALLELISM,
    ))
elif ACTION == "cross-file-barriers":
    barrier_state = {"common_start_monotonic_ns": None}

    def release_fsync_barrier() -> None:
        barrier_state["common_start_monotonic_ns"] = time.monotonic_ns()

    barrier = threading.Barrier(2, action=release_fsync_barrier)

    def write_file(index: int) -> dict:
        path = RUN_DIR / f"cross-{index}.bin"
        block = payload(f"cross-{index}", MIB)
        remaining = REGION_BYTES
        passed_barrier = False
        try:
            with path.open("xb", buffering=0) as handle:
                while remaining:
                    chunk = block[: min(remaining, len(block))]
                    write_all(handle, chunk)
                    remaining -= len(chunk)
                barrier.wait(timeout=120)
                passed_barrier = True
                os.fsync(handle.fileno())
        except BaseException:
            if not passed_barrier:
                barrier.abort()
            raise
        return {
            "file": path.name,
            "completion_monotonic_ns": time.monotonic_ns(),
        }

    write_started = time.monotonic_ns()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(write_file, index) for index in range(2)]
        completions = [future.result() for future in futures]
    common_start = barrier_state["common_start_monotonic_ns"]
    if not isinstance(common_start, int):
        raise ValueError("cross-file fsync barrier did not release")
    write_ended = max(item["completion_monotonic_ns"] for item in completions)
    expected = hashlib.sha256()
    actual = hashlib.sha256()
    read_started = time.monotonic_ns()
    for index in range(2):
        block = payload(f"cross-{index}", MIB)
        for _ in range(REGION_BYTES // len(block)):
            expected.update(block)
        path = RUN_DIR / f"cross-{index}.bin"
        if path.stat().st_size != REGION_BYTES:
            raise ValueError(f"cross-file size mismatch: {path}")
        with path.open("rb", buffering=0) as handle:
            observed = 0
            while observed < REGION_BYTES:
                chunk = handle.read(min(MIB, REGION_BYTES - observed))
                if not chunk:
                    raise ValueError(f"short cross-file read: {path}")
                actual.update(chunk)
                observed += len(chunk)
    read_ended = time.monotonic_ns()
    total = 2 * REGION_BYTES
    write_json(common_report(
        workload=ACTION,
        file_count=2,
        bytes_written=total,
        bytes_read=total,
        expected_checksum=expected.hexdigest(),
        actual_checksum=actual.hexdigest(),
        phase_throughput={
            "write": throughput(total, write_started, write_ended),
            "read": throughput(total, read_started, read_ended),
        },
        common_start_monotonic_ns=common_start,
        file_completions=sorted(completions, key=lambda item: item["file"]),
    ))
elif ACTION == "pressure-setup":
    if len(EXTRA) != 2:
        raise SystemExit("pressure-setup requires ready and release paths")
    ready_path, release_path = map(pathlib.Path, EXTRA)
    path = RUN_DIR / "pressure-setup.bin"
    started = time.monotonic_ns()
    def request_stop(_signum, _frame):
        nonlocal_stop[0] = True
    nonlocal_stop = [False]
    signal.signal(signal.SIGTERM, request_stop)
    direct_flag = getattr(os, "O_DIRECT", None)
    if direct_flag is None:
        raise OSError("O_DIRECT is unavailable")
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR | direct_flag, 0o600)
    cumulative = 0
    region = 0
    aligned = mmap.mmap(-1, MIB)
    aligned[:] = payload("pressure-setup", MIB)
    try:
        ready_published = False
        while not nonlocal_stop[0] and not release_path.exists():
            written = os.pwrite(fd, aligned, region)
            if written != MIB:
                raise OSError("short O_DIRECT pressure feeder write")
            cumulative += written
            region = (region + MIB) % WORKLOAD_BYTES
            if not ready_published:
                ready_path.write_text("ready\n", encoding="utf-8")
                ready_published = True
    finally:
        aligned.close()
        os.close(fd)
    write_json({
        "schema_version": 1,
        "working_set_bytes": WORKLOAD_BYTES,
        "cumulative_bytes_written": cumulative,
        "started_monotonic_ns": started,
        "stopped_monotonic_ns": time.monotonic_ns(),
        "exit_status": 0,
    })
elif ACTION == "profile-read-measure":
    observed, measured_throughput, started, ended = measure_workload_read()
    write_json({
        "schema_version": 1,
        "method": "inline-unbuffered-read-v1",
        "chunk_bytes": MIB,
        "bytes_read": observed,
        "throughput_mb_per_second": measured_throughput,
        "started_monotonic_ns": started,
        "completed_monotonic_ns": ended,
        "exit_status": 0,
    })
elif ACTION == "pressure-hash":
    observed, checksum = hash_workload()
    write_json({
        "schema_version": 1,
        "bytes_read": observed,
        "sha256": checksum,
        "completed_monotonic_ns": time.monotonic_ns(),
        "exit_status": 0,
    })
elif ACTION == "recovery-hash":
    if len(EXTRA) != 1:
        raise SystemExit("recovery-hash requires pressure hash path")
    pressure = json.loads(pathlib.Path(EXTRA[0]).read_text(encoding="utf-8"))
    observed, checksum = hash_workload()
    if observed != pressure.get("bytes_read") or checksum != pressure.get("sha256"):
        raise ValueError("pressure recovery hash verification failed")
    write_json({
        "schema_version": 1,
        "bytes_read": observed,
        "sha256": checksum,
        "completed_monotonic_ns": time.monotonic_ns(),
        "exit_status": 0,
    })
else:
    raise SystemExit(f"unknown inline client action: {ACTION}")
PY
}

prepare_inline_workload_directory() {
  local basename="studiofs-cache-gate-${mode}-${workload}-rep-$(printf '%03d' "$repetition")"
  local candidate="$mount_point/$basename"
  [[ ! -e "$candidate" && ! -L "$candidate" ]] || \
    die "inline workload directory already exists: $candidate"
  mkdir -- "$candidate" || die "failed to create inline workload directory: $candidate"
  [[ ! -L "$candidate" && -d "$candidate" ]] || \
    die "inline workload directory is missing or is a symlink: $candidate"
  [[ "$(realpath -m -- "$candidate")" == "$candidate" ]] || \
    die "inline workload directory did not retain its owned path"
  printf '%s\n' "$candidate"
}

cleanup_target_is_safe() {
  local candidate="$1"
  local owner="$2"
  local normalized=""
  local basename=""
  normalized="$(realpath -m -- "$candidate")" || return 1
  [[ "$candidate" == "$normalized" ]] || return 1
  [[ "$(dirname -- "$candidate")" == "$mount_point" ]] || return 1
  basename="$(basename -- "$candidate")"
  case "$owner" in
    bench) [[ "$basename" == studiofs-bench-run-* ]] || return 1 ;;
    inline) [[ "$candidate" == "$inline_workload_run_dir" ]] || return 1 ;;
    pressure-setup) [[ "$candidate" == "$pressure_setup_dir" ]] || return 1 ;;
    *) return 1 ;;
  esac
  [[ ! -L "$candidate" && -d "$candidate" ]]
}

if [[ "${CACHE_GATE_INTERNAL_FEEDER_FIXTURE:-}" == true ]]; then
  : "${CACHE_GATE_INTERNAL_FEEDER_DIR:?internal feeder directory is required}"
  mkdir -p -- "$CACHE_GATE_INTERNAL_FEEDER_DIR"
  feeder_ready="$CACHE_GATE_INTERNAL_FEEDER_DIR/.pressure-setup.ready"
  feeder_release="$CACHE_GATE_INTERNAL_FEEDER_DIR/.pressure-setup.release"
  feeder_result="$CACHE_GATE_INTERNAL_FEEDER_DIR/pressure-setup.json"
  run_inline_client pressure-setup "$CACHE_GATE_INTERNAL_FEEDER_DIR" \
    "$feeder_result" "$((8 * 1024 * 1024))" "" \
    "$feeder_ready" "$feeder_release" &
  feeder_pid=$!
  feeder_start_time="$(mount_start_time "/proc/$feeder_pid/stat")"
  wait_for_file "$feeder_ready" "internal feeder readiness" \
    "$feeder_pid" "$feeder_start_time"
  touch "$feeder_release"
  wait "$feeder_pid"
  printf '%s\n' "$feeder_result"
  exit 0
fi

if [[ "${CACHE_GATE_INTERNAL_PID_REUSE_FIXTURE:-}" == true ]]; then
  : "${CACHE_GATE_INTERNAL_PID_REUSE_RESULT:?internal PID reuse result is required}"
  : "${CACHE_GATE_INTERNAL_PID_REUSE_HELPER_KIND:?internal helper kind is required}"
  reuse_kind="$CACHE_GATE_INTERNAL_PID_REUSE_HELPER_KIND"
  case "$reuse_kind" in
    pidstat|memory-sampler|memory-holder|pressure-setup|pressure-workload) ;;
    *) die "invalid internal PID reuse helper kind" ;;
  esac
  sleep 300 &
  reuse_pid=$!
  reuse_start_time="$(mount_start_time "/proc/$reuse_pid/stat")"
  wrong_start_time="$((reuse_start_time + 1))"
  stop_helper_bounded "$reuse_pid" "$reuse_kind" "$wrong_start_time" || \
    die "PID reuse fixture guard failed"
  helper_pid_is_live "$reuse_pid" "$reuse_start_time" || \
    die "PID reuse fixture signaled a mismatched process"
  PYTHONDONTWRITEBYTECODE=1 python3 - \
    "$CACHE_GATE_INTERNAL_PID_REUSE_RESULT" "$reuse_pid" "$reuse_start_time" \
    "$reuse_kind" <<'PY'
import json
import pathlib
import sys

pathlib.Path(sys.argv[1]).write_text(json.dumps({
    "pid": int(sys.argv[2]),
    "start_time": sys.argv[3],
    "helper_kind": sys.argv[4],
    "mismatched_identity_was_not_signaled": True,
}, indent=2, sort_keys=True) + "\n")
PY
  stop_helper_bounded "$reuse_pid" pressure-workload "$reuse_start_time" || \
    die "PID reuse fixture cleanup failed"
  exit 0
fi

if [[ "${CACHE_GATE_INTERNAL_TRAP_FIXTURE:-}" == true ]]; then
  : "${CACHE_GATE_INTERNAL_WORKLOAD_PID_FILE:?internal workload pid file is required}"
  : "${CACHE_GATE_INTERNAL_CLEANUP_HOOK:?internal cleanup hook is required}"
  : "${CACHE_GATE_INTERNAL_CGROUP:?internal cgroup is required}"
  : "${CACHE_GATE_INTERNAL_CGROUP_MOUNT:?internal cgroup mount is required}"
  : "${CACHE_GATE_INTERNAL_MEMBERSHIP:?internal membership file is required}"
  cgroup="$CACHE_GATE_INTERNAL_CGROUP"
  cgroup_mount="$CACHE_GATE_INTERNAL_CGROUP_MOUNT"
  move_pid_to_cgroup "$$" "$CACHE_GATE_INTERNAL_MEMBERSHIP" "fixture process"
  sleep 300 &
  pressure_workload_pid=$!
  pressure_workload_start_time="$(mount_start_time "/proc/$pressure_workload_pid/stat")"
  printf '%s\n' "$pressure_workload_pid" > "$CACHE_GATE_INTERNAL_WORKLOAD_PID_FILE"
  trap stop_helpers_on_exit EXIT
  exit 99
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) require_value "$1" "${2:-}"; mode="$2"; shift 2 ;;
    --workload) require_value "$1" "${2:-}"; workload="$2"; shift 2 ;;
    --mount-point) require_value "$1" "${2:-}"; mount_point="$2"; shift 2 ;;
    --mount-pid) require_value "$1" "${2:-}"; mount_pid="$(positive_int "$1" "$2")"; shift 2 ;;
    --cgroup) require_value "$1" "${2:-}"; cgroup="$2"; shift 2 ;;
    --bench) require_value "$1" "${2:-}"; bench_bin="$2"; shift 2 ;;
    --bench-repo) require_value "$1" "${2:-}"; bench_repo="$2"; shift 2 ;;
    --studiofs) require_value "$1" "${2:-}"; studiofs_bin="$2"; shift 2 ;;
    --correctness-manifest) require_value "$1" "${2:-}"; correctness_manifest="$2"; shift 2 ;;
    --output-root) require_value "$1" "${2:-}"; output_root="$2"; shift 2 ;;
    --workload-bytes) require_value "$1" "${2:-}"; workload_bytes="$(positive_int "$1" "$2")"; shift 2 ;;
    --diagnostic) diagnostic=true; shift ;;
    --dry-run) dry_run=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

for required in mode workload mount_point mount_pid; do
  if [[ -z "${!required}" ]]; then
    echo "Error: --${required//_/-} is required" >&2
    usage >&2
    exit 2
  fi
done

case "$mode" in
  direct-serial|direct-async|kernel) ;;
  *) echo "Error: --mode must be direct-serial, direct-async, or kernel" >&2; exit 2 ;;
esac

case "$workload" in
  large-single|fixed-1m|small-128k|warm-reopen|same-file-parallel|cross-file-barriers) ;;
  *)
    echo "Error: --workload must be large-single, fixed-1m, small-128k, warm-reopen, same-file-parallel, or cross-file-barriers" >&2
    exit 2
    ;;
esac

bench_bin="$(realpath -m -- "$bench_bin")"
bench_repo="$(realpath -m -- "$bench_repo")"
studiofs_bin="$(realpath -m -- "$studiofs_bin")"
mount_point="$(realpath -m -- "$mount_point")"
output_root="$(realpath -m -- "$output_root")"
if [[ -n "$cgroup" ]]; then
  cgroup="$(realpath -m -- "$cgroup")"
fi
if [[ -n "$correctness_manifest" ]]; then
  correctness_manifest="$(realpath -m -- "$correctness_manifest")"
fi

pressure_scenario=false
if [[ "$workload" == large-single || "$workload" == small-128k ]]; then
  pressure_scenario=true
fi

if [[ "$dry_run" == false ]]; then
  if [[ -z "$cgroup" ]]; then
    echo "Error: --cgroup is required for real runs" >&2
    exit 2
  fi
  if [[ -z "$correctness_manifest" ]]; then
    echo "Error: --correctness-manifest is required for real runs" >&2
    exit 2
  fi
fi

if [[ "$dry_run" == true ]]; then
mkdir -p "$output_root"
timestamp="$(date -u +%Y%m%d-%H%M%S)"
artifact_dir="$output_root/linux-cache-gate-$timestamp"
suffix=2
while ! mkdir "$artifact_dir" 2>/dev/null; do
  artifact_dir="$output_root/linux-cache-gate-$timestamp-$suffix"
  suffix=$((suffix + 1))
done

cgroup_json="null"
if [[ -n "$cgroup" ]]; then
  cgroup_json="\"$(json_escape "$cgroup")\""
fi
correctness_manifest_json="null"
if [[ -n "$correctness_manifest" ]]; then
  correctness_manifest_json="\"$(json_escape "$correctness_manifest")\""
fi

cat > "$artifact_dir/environment.json" <<JSON
{
  "schema_version": 1,
  "status": "dry-run",
  "promotion_eligible": false,
  "mode": "$(json_escape "$mode")",
  "workload": "$(json_escape "$workload")",
  "mount_point": "$(json_escape "$mount_point")",
  "mount_pid": $mount_pid,
  "runs": 1,
  "cgroup": $cgroup_json,
  "bench": "$(json_escape "$bench_bin")",
  "bench_repo": "$(json_escape "$bench_repo")",
  "studiofs": "$(json_escape "$studiofs_bin")",
  "correctness_manifest": $correctness_manifest_json,
  "output_root": "$(json_escape "$output_root")",
  "workload_bytes": $workload_bytes,
  "diagnostic": $diagnostic,
  "dry_run": true
}
JSON

cat > "$artifact_dir/manifest.json" <<JSON
{
  "schema_version": 1,
  "status": "dry-run",
  "artifact_dir": "$(json_escape "$artifact_dir")",
  "mode": "$(json_escape "$mode")",
  "workload": "$(json_escape "$workload")",
  "runs": 1,
  "diagnostic": $diagnostic,
  "promotion_eligible": false,
  "correctness_manifest_path": $correctness_manifest_json,
  "correctness_manifest_sha256": null
}
JSON

cat > "$artifact_dir/command.txt" <<COMMAND
dry-run: workload command not executed
mode=$mode
workload=$workload
mount_point=$mount_point
mount_pid=$mount_pid
runs=1
workload_bytes=$workload_bytes
pressure_scenario=$pressure_scenario
COMMAND

for phase in foreground-start foreground-end drain-end cleanup-end; do
  json_placeholder "$artifact_dir/runtime-$phase.json" "$phase"
done
if [[ "$workload" == warm-reopen ]]; then
  json_placeholder "$artifact_dir/runtime-warm-first-end.json" "warm-first-end"
fi
if [[ "$pressure_scenario" == true ]]; then
  for phase in setup-observed pressure-before-profile pressure-after-profile \
    recovery-boundary recovery-1 recovery-2; do
    json_placeholder "$artifact_dir/runtime-$phase.json" "$phase"
  done
  for artifact in memory-pressure-config memory-events-before memory-events-pressure \
    memory-events-after-holder-release memory-events-after memory-holder pressure-setup \
    pressure-profile pressure-measure pressure-hash recovery-measure recovery-hash; do
    json_placeholder "$artifact_dir/$artifact.json" "$artifact"
  done
fi

for phase in foreground-start foreground-end drain-end; do
  proc_dir="$artifact_dir/proc-$phase"
  mkdir "$proc_dir"
  for proc_file in stat status smaps_rollup io; do
    printf 'dry-run: /proc snapshot not captured\n' > "$proc_dir/$proc_file.txt"
  done
done

json_placeholder "$artifact_dir/cgroup-foreground-start.json" "foreground-start"
json_placeholder "$artifact_dir/cgroup-foreground-end.json" "foreground-end"
json_placeholder "$artifact_dir/cgroup-drain-end.json" "drain-end"
: > "$artifact_dir/cgroup-memory-samples.jsonl"
: > "$artifact_dir/process-memory-samples.jsonl"
: > "$artifact_dir/pidstat.txt"
json_placeholder "$artifact_dir/client-report.json" "client"
json_placeholder "$artifact_dir/cleanup.json" "cleanup"
cat > "$artifact_dir/derived.json" <<JSON
{"status":"dry-run","phase":"derived","diagnostic":$diagnostic,"promotion_eligible":false}
JSON

cat > "$artifact_dir/summary.md" <<MD
# StudioFS Linux Cache Gate Summary

Dry-run artifact skeleton for \`$mode\` / \`$workload\`. No workload or sampler was executed.
MD

cat > "$artifact_dir/decision.md" <<'MD'
# StudioFS Linux Cache Gate Decision

Dry-run output is diagnostic only and is not promotion-eligible.
MD

echo "$artifact_dir"
exit 0
fi

test_fixture=false
declare -a cache_gate_override_names=()
while IFS= read -r exported_name; do
  if [[ "$exported_name" == STUDIOFS_CACHE_GATE_* ]]; then
    cache_gate_override_names+=("$exported_name")
  fi
done < <(compgen -e)
if [[ "${STUDIOFS_CACHE_GATE_TEST_FIXTURE:-}" == 1 ]]; then
  test_fixture=true
  for exported_name in "${cache_gate_override_names[@]}"; do
    case "$exported_name" in
      STUDIOFS_CACHE_GATE_TEST_FIXTURE|STUDIOFS_CACHE_GATE_FINDMNT|\
      STUDIOFS_CACHE_GATE_PROC_ROOT|STUDIOFS_CACHE_GATE_PIDSTAT|\
      STUDIOFS_CACHE_GATE_POLL_INTERVAL|STUDIOFS_CACHE_GATE_SAMPLE_INTERVAL) ;;
      *) die "unsupported cache-gate test override: $exported_name" ;;
    esac
  done
elif (( ${#cache_gate_override_names[@]} != 0 )); then
  die "STUDIOFS_CACHE_GATE_* overrides require the explicit test-fixture path"
fi

idle_poll_interval=0.5
memory_sample_interval=0.1
if [[ "$test_fixture" == true ]]; then
  idle_poll_interval="${STUDIOFS_CACHE_GATE_POLL_INTERVAL:-0.01}"
  memory_sample_interval="${STUDIOFS_CACHE_GATE_SAMPLE_INTERVAL:-0.01}"
fi
PYTHONDONTWRITEBYTECODE=1 python3 - "$idle_poll_interval" "$memory_sample_interval" <<'PY'
import math
import sys

for label, raw_value in zip(("poll", "sample"), sys.argv[1:], strict=True):
    try:
        value = float(raw_value)
    except ValueError as error:
        raise SystemExit(f"Error: invalid {label} interval") from error
    if not math.isfinite(value) or value <= 0:
        raise SystemExit(f"Error: invalid {label} interval")
PY

[[ "$(uname -s)" == "Linux" ]] || die "Linux cache gate requires Linux"

findmnt_bin="${STUDIOFS_CACHE_GATE_FINDMNT:-findmnt}"
proc_root="$(realpath -m -- "${STUDIOFS_CACHE_GATE_PROC_ROOT:-/proc}")"
pidstat_bin="${STUDIOFS_CACHE_GATE_PIDSTAT:-pidstat}"
if [[ "$findmnt_bin" == */* ]]; then
  [[ -x "$findmnt_bin" ]] || die "findmnt override is not executable: $findmnt_bin"
else
  command -v "$findmnt_bin" >/dev/null || die "findmnt is required"
fi
if [[ "$pidstat_bin" == */* ]]; then
  [[ -x "$pidstat_bin" ]] || die "pidstat override is not executable: $pidstat_bin"
else
  command -v "$pidstat_bin" >/dev/null || die "pidstat is required"
fi
[[ -f "$script_dir/linux_cache_gate_summary.py" ]] || die "summary validator is missing"
[[ -x "$bench_bin" ]] || die "studiofs-bench is not executable: $bench_bin"
[[ -x "$studiofs_bin" ]] || die "StudioFS is not executable: $studiofs_bin"
[[ -r "$correctness_manifest" ]] || die "correctness manifest is not readable: $correctness_manifest"
[[ -d "$cgroup" ]] || die "delegated cgroup directory does not exist: $cgroup"

mount_fstype=""
mount_deadline=$((SECONDS + 60))
if [[ "$test_fixture" == true ]]; then
  mount_deadline=$((SECONDS + 1))
fi
while true; do
  kill -0 "$mount_pid" 2>/dev/null || die "mount process is not alive: $mount_pid"
  if ! mount_fstype="$("$findmnt_bin" -n -T "$mount_point" -o FSTYPE 2>/dev/null)"; then
    mount_fstype=""
  fi
  if [[ "$mount_fstype" == "fuse.studiofs" ]]; then
    break
  fi
  if (( SECONDS >= mount_deadline )); then
    die "mount point must have fuse.studiofs type: $mount_point (found ${mount_fstype:-none})"
  fi
  sleep 1
done
kill -0 "$mount_pid" 2>/dev/null || die "mount process exited during preflight: $mount_pid"
mount_findmnt="$("$findmnt_bin" -n -T "$mount_point" -o SOURCE,TARGET,FSTYPE,OPTIONS)"
mount_target="$("$findmnt_bin" -n -T "$mount_point" -o TARGET)"
[[ "$(realpath -m -- "$mount_target")" == "$mount_point" ]] || \
  die "findmnt target does not match --mount-point"

mount_invocation_lines=""
if ! mount_invocation_lines="$(
  PYTHONDONTWRITEBYTECODE=1 python3 - \
    "$proc_root/$mount_pid/cmdline" "$studiofs_bin" "$mount_point" <<'PY'
import os
import pathlib
import sys

try:
    raw = pathlib.Path(sys.argv[1]).read_bytes()
    fields = raw.split(b"\0")
    if fields and fields[-1] == b"":
        fields.pop()
    argv = [value.decode("utf-8", errors="strict") for value in fields]
except (OSError, UnicodeDecodeError) as error:
    raise SystemExit(f"Error: cannot parse mount command line: {error}") from error

if len(argv) != 7 or argv[1] != "mount" or argv[4] != "--cache-dir" or argv[6] != "--foreground":
    raise SystemExit(
        "Error: mount command must be exactly studiofs mount <volume> <mount-root> "
        "--cache-dir <path> --foreground"
    )
if not os.path.isabs(argv[0]) or os.path.realpath(argv[0]) != os.path.realpath(sys.argv[2]):
    raise SystemExit("Error: mount command executable does not match --studiofs")
volume = argv[2]
if (
    not volume
    or volume in (".", "..")
    or "/" in volume
    or any(ord(character) < 32 or ord(character) == 127 for character in volume)
):
    raise SystemExit("Error: mount command volume is unsafe")
if not os.path.isabs(argv[3]) or not os.path.isabs(argv[5]):
    raise SystemExit("Error: mount root and cache directory must be absolute")
mount_root = os.path.abspath(os.path.normpath(argv[3]))
cache_dir = os.path.abspath(os.path.normpath(argv[5]))
for label, value in (("mount root", mount_root), ("cache directory", cache_dir)):
    if any(character in value for character in ("\n", "\r", "\t")):
        raise SystemExit(f"Error: parsed {label} contains control characters")
expected_target = os.path.abspath(os.path.normpath(os.path.join(mount_root, volume)))
actual_target = os.path.abspath(os.path.normpath(sys.argv[3]))
if expected_target != actual_target:
    raise SystemExit(
        f"Error: mount target mismatch: expected {expected_target}, got {actual_target}"
    )
print(volume)
print(mount_root)
print(cache_dir)
PY
)"; then
  die "mount command-line provenance failed"
fi
mapfile -t mount_invocation_fields <<< "$mount_invocation_lines"
(( ${#mount_invocation_fields[@]} == 3 )) || die "mount command-line provenance is incomplete"
volume_name="${mount_invocation_fields[0]}"
mount_root="${mount_invocation_fields[1]}"
cache_dir="${mount_invocation_fields[2]}"
[[ -d "$cache_dir" ]] || die "parsed cache directory does not exist: $cache_dir"

cache_source="$("$findmnt_bin" -n -T "$cache_dir" -o SOURCE)"
cache_target="$("$findmnt_bin" -n -T "$cache_dir" -o TARGET)"
cache_fstype="$("$findmnt_bin" -n -T "$cache_dir" -o FSTYPE)"
cache_options="$("$findmnt_bin" -n -T "$cache_dir" -o OPTIONS)"
[[ -n "$cache_source" && -n "$cache_target" && -n "$cache_fstype" ]] || \
  die "cache backing findmnt evidence is incomplete"
cache_backing_json="$(
  CACHE_GATE_CACHE_SOURCE="$cache_source" \
  CACHE_GATE_CACHE_TARGET="$cache_target" \
  CACHE_GATE_CACHE_FSTYPE="$cache_fstype" \
  CACHE_GATE_CACHE_OPTIONS="$cache_options" \
  PYTHONDONTWRITEBYTECODE=1 python3 - <<'PY'
import json
import os

print(json.dumps({
    "source": os.environ["CACHE_GATE_CACHE_SOURCE"],
    "target": os.path.abspath(os.path.normpath(os.environ["CACHE_GATE_CACHE_TARGET"])),
    "filesystem_type": os.environ["CACHE_GATE_CACHE_FSTYPE"],
    "options": os.environ["CACHE_GATE_CACHE_OPTIONS"],
}, separators=(",", ":"), sort_keys=True))
PY
)"

mount_env_json="$(PYTHONDONTWRITEBYTECODE=1 python3 - "$proc_root/$mount_pid/environ" <<'PY'
import json
import pathlib
import sys

allowlist = {
    "STUDIOFS_LINUX_FUSE_CACHE_POLICY",
    "STUDIOFS_FUSE_ASYNC_DIO",
    "STUDIOFS_FUSE_ASYNC_WRITE",
    "STUDIOFS_FUSE_ASYNC_WRITE_DISABLED",
    "STUDIOFS_MOUNT_BACKEND",
    "STUDIOFS_PROFILE",
    "STUDIOFS_PYROSCOPE_SAMPLE_RATE",
    "STUDIOFS_WRITER_PAGE_SIZE_BYTES",
    "STUDIOFS_READ_BUFFER_MB",
    "MIMALLOC_ARENA_EAGER_COMMIT",
    "MIMALLOC_EAGER_COMMIT",
    "MIMALLOC_LARGE_OS_PAGES",
    "MIMALLOC_PURGE_DELAY",
    "MIMALLOC_RESERVE_HUGE_OS_PAGES",
    "MIMALLOC_SHOW_STATS",
    "MIMALLOC_VERBOSE",
}
accepted = {}
for item in pathlib.Path(sys.argv[1]).read_bytes().split(b"\0"):
    if b"=" not in item:
        continue
    raw_name, raw_value = item.split(b"=", 1)
    name = raw_name.decode("utf-8", errors="strict")
    if name in allowlist:
        accepted[name] = raw_value.decode("utf-8", errors="strict")
json.dump(accepted, sys.stdout, separators=(",", ":"), sort_keys=True)
PY
)"

CACHE_GATE_MODE="$mode" CACHE_GATE_MOUNT_ENV_JSON="$mount_env_json" \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 - <<'PY'
import json
import os

from tools.perf import linux_cache_gate_summary as gate

try:
    gate.validate_mode_environment(
        os.environ["CACHE_GATE_MODE"],
        json.loads(os.environ["CACHE_GATE_MOUNT_ENV_JSON"]),
    )
except (json.JSONDecodeError, ValueError, gate.GateValidationError) as error:
    raise SystemExit(f"Error: {error}") from error
PY

[[ -d "$bench_repo/.git" ]] || die "benchmark source checkout is missing: $bench_repo"
git_revision="$(git -C "$repo_root" rev-parse HEAD)"
git_dirty_output="$(git -C "$repo_root" status --porcelain --untracked-files=all)"
[[ -z "$git_dirty_output" ]] || die "StudioFS source tree is dirty"
bench_git_revision="$(git -C "$bench_repo" rev-parse HEAD)"
bench_dirty_output="$(git -C "$bench_repo" status --porcelain --untracked-files=all)"
[[ -z "$bench_dirty_output" ]] || die "benchmark source tree is dirty"

studiofs_sha256="$(sha256sum "$studiofs_bin")"
studiofs_sha256="${studiofs_sha256%% *}"
bench_sha256="$(sha256sum "$bench_bin")"
bench_sha256="${bench_sha256%% *}"
mount_executable="$(readlink -f -- "$proc_root/$mount_pid/exe")"
[[ -n "$mount_executable" && -r "$mount_executable" ]] || die "mount executable is unavailable"
mount_executable_sha256="$(sha256sum "$mount_executable")"
mount_executable_sha256="${mount_executable_sha256%% *}"
[[ "$mount_executable_sha256" == "$studiofs_sha256" ]] || die "mount executable checksum does not match --studiofs"
mount_pid_start_time="$(mount_start_time "$proc_root/$mount_pid/stat")"

correctness_manifest_sha256="$(
  CACHE_GATE_EXPECTED_REVISION="$git_revision" \
  CACHE_GATE_EXPECTED_STUDIOFS_SHA256="$studiofs_sha256" \
  CACHE_GATE_EXPECTED_BENCH_REVISION="$bench_git_revision" \
  CACHE_GATE_EXPECTED_BENCH_SHA256="$bench_sha256" \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 - "$correctness_manifest" <<'PY'
import hashlib
import json
import os
import pathlib
import sys

from tools.perf import linux_cache_gate_summary as gate

try:
    content = pathlib.Path(sys.argv[1]).read_bytes()
    manifest = json.loads(content)
    gate.validate_correctness_manifest(
        manifest,
        expected_revision=os.environ["CACHE_GATE_EXPECTED_REVISION"],
        expected_studiofs_sha256=os.environ["CACHE_GATE_EXPECTED_STUDIOFS_SHA256"],
        expected_bench_revision=os.environ["CACHE_GATE_EXPECTED_BENCH_REVISION"],
        expected_bench_sha256=os.environ["CACHE_GATE_EXPECTED_BENCH_SHA256"],
    )
except (json.JSONDecodeError, OSError, ValueError, gate.GateValidationError) as error:
    raise SystemExit(f"Error: correctness manifest validation failed: {error}") from error
print(hashlib.sha256(content).hexdigest())
PY
)"

cgroup_fstype="$("$findmnt_bin" -n -T "$cgroup" -o FSTYPE)"
cgroup_mount="$(realpath -m -- "$("$findmnt_bin" -n -T "$cgroup" -o TARGET)")"
cgroup_preflight_json="$(
  CACHE_GATE_CGROUP_FSTYPE="$cgroup_fstype" PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" \
    python3 - "$cgroup" <<'PY'
import json
import os
import pathlib
import sys

from tools.perf import linux_cache_gate_summary as gate

path = pathlib.Path(sys.argv[1])
try:
    procs = [line for line in (path / "cgroup.procs").read_text().splitlines() if line]
    memory_current = int((path / "memory.current").read_text().strip())
    memory_peak = int((path / "memory.peak").read_text().strip())
    controllers = (path / "cgroup.controllers").read_text().split()
    preflight = {
        "path": str(path),
        "delegated_root": str(path.parent),
        "filesystem_type": os.environ["CACHE_GATE_CGROUP_FSTYPE"],
        "controllers": controllers,
        "fresh": not procs and memory_current == 0 and memory_peak == 0,
        "cgroup_procs": procs,
        "memory_current": memory_current,
        "memory_peak": memory_peak,
        "writable": {
            name: os.access(path / name, os.W_OK)
            for name in ("cgroup.procs", "memory.max")
        },
        "readable": {
            name: os.access(path / name, os.R_OK)
            for name in (
                "memory.current", "memory.peak", "memory.stat",
                "memory.max", "memory.events",
            )
        },
    }
    gate.validate_cgroup_preflight(preflight)
except (OSError, ValueError, gate.GateValidationError) as error:
    raise SystemExit(f"Error: cgroup preflight failed: {error}") from error
json.dump(preflight, sys.stdout, separators=(",", ":"), sort_keys=True)
PY
)"

mkdir -p "$output_root"
shopt -s dotglob nullglob
existing_artifacts=("$output_root"/*)
shopt -u dotglob nullglob
(( ${#existing_artifacts[@]} == 0 )) || die "real --output-root must be an empty repetition directory: $output_root"
artifact_dir="$output_root"

move_pid_to_cgroup "$$" "$proc_root/self/cgroup" collector
move_pid_to_cgroup "$mount_pid" "$proc_root/$mount_pid/cgroup" "mount process"

if [[ "$pressure_scenario" == true ]]; then
  pressure_negotiated_max_write="$(capture_runtime_snapshot | \
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 -c '
import json
import sys
from tools.perf import linux_cache_gate_summary as gate
print(gate.negotiated_max_write_bytes(json.load(sys.stdin)))
')"
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 - \
    "$cgroup" "$proc_root/meminfo" "$artifact_dir/memory-pressure-config.json" \
    "$pressure_negotiated_max_write" <<'PY'
import json
import pathlib
import sys
from tools.perf import linux_cache_gate_summary as gate

cgroup = pathlib.Path(sys.argv[1])


def finite_limit(path: pathlib.Path) -> int | None:
    raw = path.read_text(encoding="utf-8").strip()
    return None if raw == "max" else int(raw)


try:
    current = int((cgroup / "memory.current").read_text(encoding="utf-8").strip())
    host_available = None
    for line in pathlib.Path(sys.argv[2]).read_text(encoding="utf-8").splitlines():
        if line.startswith("MemAvailable:"):
            host_available = int(line.split()[1]) * 1024
            break
    leaf_limit = finite_limit(cgroup / "memory.max")
    parent_limit = finite_limit(cgroup.parent / "memory.max")
    parent_available = None
    if parent_limit is not None:
        parent_current = int(
            (cgroup.parent / "memory.current").read_text(encoding="utf-8").strip()
        )
        parent_available = parent_limit - parent_current
    if host_available is None:
        raise ValueError("MemAvailable is missing")
    headroom = 3 * 1024 * 1024 * 1024
    if host_available < headroom:
        raise ValueError("pressure scenario requires 3 GiB host headroom")
    if leaf_limit is not None and leaf_limit - current < headroom:
        raise ValueError("pressure scenario requires 3 GiB leaf headroom")
    if parent_available is not None and parent_available < headroom:
        raise ValueError("pressure scenario requires 3 GiB parent headroom")
    memory_max = current + headroom
    payload = gate.derive_pressure_target(
        effective_limit=memory_max,
        negotiated_max_write=int(sys.argv[4]),
    )
    (cgroup / "memory.max").write_text(
        f"{memory_max}\n", encoding="utf-8"
    )
    if finite_limit(cgroup / "memory.max") != memory_max:
        raise ValueError("memory.max did not retain the requested finite limit")
    payload = {
        **payload,
        "schema_version": 1,
        "memory_max_bytes": memory_max,
        "headroom_bytes": headroom,
        "memory_current_before_bytes": current,
    }
    pathlib.Path(sys.argv[3]).write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
except (OSError, ValueError) as error:
    raise SystemExit(f"Error: cannot configure pressure cgroup: {error}") from error
PY
  capture_memory_event_boundary before "$artifact_dir/memory-events-before.json"
fi

idle_snapshot_file="$artifact_dir/.preflight-idle-zero.jsonl"
first_runtime_file="$artifact_dir/.preflight-first-runtime.json"
: > "$idle_snapshot_file"
first_runtime_captured=false
final_runtime_snapshot=""
idle_streak=0
idle_deadline=$((SECONDS + 600))
while (( SECONDS <= idle_deadline )); do
  final_runtime_snapshot="$(capture_runtime_snapshot)"
  if [[ "$first_runtime_captured" == false ]]; then
    printf '%s\n' "$final_runtime_snapshot" > "$first_runtime_file"
    first_runtime_captured=true
  fi
  idle_state="$(runtime_idle_state "$final_runtime_snapshot")"
  if [[ "$idle_state" == "idle" ]]; then
    if (( idle_streak == 0 )); then
      : > "$idle_snapshot_file"
    fi
    printf '%s\n' "$final_runtime_snapshot" >> "$idle_snapshot_file"
    idle_streak=$((idle_streak + 1))
    if (( idle_streak == 3 )); then
      break
    fi
  else
    idle_streak=0
    : > "$idle_snapshot_file"
  fi
  sleep "$idle_poll_interval"
done
(( idle_streak == 3 )) || die "mount did not reach three consecutive idle snapshots within 600 seconds"
kill -0 "$mount_pid" 2>/dev/null || die "mount process exited during diagnostics preflight"
[[ "$(mount_start_time "$proc_root/$mount_pid/stat")" == "$mount_pid_start_time" ]] || die "mount PID was reused during preflight"

created_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
uname_value="$(uname -a)"
cpu_count="$(getconf _NPROCESSORS_ONLN)"
memory_total_kib="$(awk '/^MemTotal:/ { print $2; exit }' /proc/meminfo)"
parallelism=""
if [[ "$workload" == same-file-parallel ]]; then
  parallelism="$(PYTHONDONTWRITEBYTECODE=1 python3 - <<'PY'
import os

print(min(os.cpu_count() or 1, 8))
PY
)"
fi
repetition=1
artifact_basename="$(basename -- "$artifact_dir")"
if [[ "$artifact_basename" =~ ^rep-([0-9]{3})$ ]]; then
  repetition="$((10#${BASH_REMATCH[1]}))"
  (( repetition >= 1 )) || die "repetition directory must be rep-001 or greater"
fi

CACHE_GATE_CREATED_UTC="$created_utc" \
CACHE_GATE_MODE="$mode" \
CACHE_GATE_WORKLOAD="$workload" \
CACHE_GATE_REPETITION="$repetition" \
CACHE_GATE_MOUNT_POINT="$mount_point" \
CACHE_GATE_MOUNT_PID="$mount_pid" \
CACHE_GATE_MOUNT_PID_START_TIME="$mount_pid_start_time" \
CACHE_GATE_MOUNT_FINDMNT="$mount_findmnt" \
CACHE_GATE_VOLUME="$volume_name" \
CACHE_GATE_MOUNT_ROOT="$mount_root" \
CACHE_GATE_CACHE_DIR="$cache_dir" \
CACHE_GATE_CACHE_BACKING_JSON="$cache_backing_json" \
CACHE_GATE_CGROUP="$cgroup" \
CACHE_GATE_BENCH="$bench_bin" \
CACHE_GATE_STUDIOFS="$studiofs_bin" \
CACHE_GATE_OUTPUT_ROOT="$output_root" \
CACHE_GATE_WORKLOAD_BYTES="$workload_bytes" \
CACHE_GATE_MOUNT_ENV_JSON="$mount_env_json" \
CACHE_GATE_CGROUP_PREFLIGHT_JSON="$cgroup_preflight_json" \
CACHE_GATE_GIT_REVISION="$git_revision" \
CACHE_GATE_STUDIOFS_SHA256="$studiofs_sha256" \
CACHE_GATE_BENCH_GIT_REVISION="$bench_git_revision" \
CACHE_GATE_BENCH_SHA256="$bench_sha256" \
CACHE_GATE_MOUNT_EXECUTABLE="$mount_executable" \
CACHE_GATE_MOUNT_EXECUTABLE_SHA256="$mount_executable_sha256" \
CACHE_GATE_CORRECTNESS_PATH="$correctness_manifest" \
CACHE_GATE_CORRECTNESS_SHA256="$correctness_manifest_sha256" \
CACHE_GATE_UNAME="$uname_value" \
CACHE_GATE_CPU_COUNT="$cpu_count" \
CACHE_GATE_MEMORY_TOTAL_KIB="$memory_total_kib" \
CACHE_GATE_TEST_FIXTURE="$test_fixture" \
CACHE_GATE_DIAGNOSTIC="$diagnostic" \
CACHE_GATE_PARALLELISM="$parallelism" \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 - \
  "$artifact_dir/environment.json" "$artifact_dir/manifest.json" "$idle_snapshot_file" \
  "$first_runtime_file" <<'PY'
import hashlib
import json
import os
import pathlib
import sys

from tools.perf import linux_cache_gate_summary as gate

e = os.environ
correctness_path = pathlib.Path(e["CACHE_GATE_CORRECTNESS_PATH"])
content = correctness_path.read_bytes()
if hashlib.sha256(content).hexdigest() != e["CACHE_GATE_CORRECTNESS_SHA256"]:
    raise SystemExit("Error: correctness manifest changed during preflight")
correctness = json.loads(content)
gate.validate_correctness_manifest(
    correctness,
    expected_revision=e["CACHE_GATE_GIT_REVISION"],
    expected_studiofs_sha256=e["CACHE_GATE_STUDIOFS_SHA256"],
    expected_bench_revision=e["CACHE_GATE_BENCH_GIT_REVISION"],
    expected_bench_sha256=e["CACHE_GATE_BENCH_SHA256"],
)
idle_snapshots = [
    json.loads(line)
    for line in pathlib.Path(sys.argv[3]).read_text(encoding="utf-8").splitlines()
    if line
]
if len(idle_snapshots) != 3 or not gate.idle_streak_reached(idle_snapshots):
    raise SystemExit("Error: idle preflight did not retain three zero snapshots")
cgroup_preflight = json.loads(e["CACHE_GATE_CGROUP_PREFLIGHT_JSON"])
cgroup_preflight.update(
    {
        "collector_pid": os.getppid(),
        "mount_pid": int(e["CACHE_GATE_MOUNT_PID"]),
        "memberships_verified": True,
    }
)
manifest = {
    "schema_version": 1,
    "status": "measurement-pending",
    "promotion_eligible": False,
    "mode": e["CACHE_GATE_MODE"],
    "workload": e["CACHE_GATE_WORKLOAD"],
    "diagnostic": e["CACHE_GATE_DIAGNOSTIC"] == "true",
    "repetition": int(e["CACHE_GATE_REPETITION"]),
    "volume": e["CACHE_GATE_VOLUME"],
    "mount_root": e["CACHE_GATE_MOUNT_ROOT"],
    "cache_dir": e["CACHE_GATE_CACHE_DIR"],
    "cache_backing": json.loads(e["CACHE_GATE_CACHE_BACKING_JSON"]),
    "cgroup": e["CACHE_GATE_CGROUP"],
    "mount_point": e["CACHE_GATE_MOUNT_POINT"],
    "mount_pid": int(e["CACHE_GATE_MOUNT_PID"]),
    "mount_pid_start_time": e["CACHE_GATE_MOUNT_PID_START_TIME"],
    "runs": 1,
    "workload_bytes": int(e["CACHE_GATE_WORKLOAD_BYTES"]),
    "git_revision": e["CACHE_GATE_GIT_REVISION"],
    "git_dirty": False,
    "studiofs_sha256": e["CACHE_GATE_STUDIOFS_SHA256"],
    "bench_git_revision": e["CACHE_GATE_BENCH_GIT_REVISION"],
    "bench_git_dirty": False,
    "bench_sha256": e["CACHE_GATE_BENCH_SHA256"],
    "mount_executable_sha256": e["CACHE_GATE_MOUNT_EXECUTABLE_SHA256"],
    "correctness_manifest_path": str(correctness_path),
    "correctness_manifest_sha256": e["CACHE_GATE_CORRECTNESS_SHA256"],
}
parallelism = int(e["CACHE_GATE_PARALLELISM"]) if e["CACHE_GATE_PARALLELISM"] else None
manifest.update(gate.workload_expectation(
    e["CACHE_GATE_WORKLOAD"],
    workload_bytes=int(e["CACHE_GATE_WORKLOAD_BYTES"]),
    parallelism=parallelism,
))
gate.validate_run_provenance(
    manifest,
    correctness,
    correctness_manifest_path=str(correctness_path),
    correctness_manifest_sha256=e["CACHE_GATE_CORRECTNESS_SHA256"],
)
environment = {
    "schema_version": 1,
    "status": "measurement-pending",
    "promotion_eligible": False,
    "created_utc": e["CACHE_GATE_CREATED_UTC"],
    "mode": e["CACHE_GATE_MODE"],
    "workload": e["CACHE_GATE_WORKLOAD"],
    "diagnostic": e["CACHE_GATE_DIAGNOSTIC"] == "true",
    "repetition": int(e["CACHE_GATE_REPETITION"]),
    "volume": e["CACHE_GATE_VOLUME"],
    "mount_root": e["CACHE_GATE_MOUNT_ROOT"],
    "cache_dir": e["CACHE_GATE_CACHE_DIR"],
    "cache_backing": json.loads(e["CACHE_GATE_CACHE_BACKING_JSON"]),
    "mount_point": e["CACHE_GATE_MOUNT_POINT"],
    "mount_point_basename": pathlib.Path(e["CACHE_GATE_MOUNT_POINT"]).name,
    "mount_pid": int(e["CACHE_GATE_MOUNT_PID"]),
    "mount_pid_start_time": e["CACHE_GATE_MOUNT_PID_START_TIME"],
    "mount_findmnt": e["CACHE_GATE_MOUNT_FINDMNT"],
    "runs": 1,
    "cgroup": e["CACHE_GATE_CGROUP"],
    "bench": e["CACHE_GATE_BENCH"],
    "studiofs": e["CACHE_GATE_STUDIOFS"],
    "output_root": e["CACHE_GATE_OUTPUT_ROOT"],
    "workload_bytes": int(e["CACHE_GATE_WORKLOAD_BYTES"]),
    "dry_run": False,
    "test_fixture": e["CACHE_GATE_TEST_FIXTURE"] == "true",
    "host": {
        "uname": e["CACHE_GATE_UNAME"],
        "cpu_count": int(e["CACHE_GATE_CPU_COUNT"]),
        "memory_total_kib": int(e["CACHE_GATE_MEMORY_TOTAL_KIB"]),
    },
    "mount_environment": json.loads(e["CACHE_GATE_MOUNT_ENV_JSON"]),
    "cgroup_preflight": cgroup_preflight,
    "first_runtime_snapshot": json.loads(pathlib.Path(sys.argv[4]).read_text()),
    "idle_zero_snapshots": idle_snapshots,
    "sources": {
        "studiofs": {"revision": e["CACHE_GATE_GIT_REVISION"], "dirty": False},
        "bench": {"revision": e["CACHE_GATE_BENCH_GIT_REVISION"], "dirty": False},
    },
    "binaries": {
        "studiofs_sha256": e["CACHE_GATE_STUDIOFS_SHA256"],
        "bench_sha256": e["CACHE_GATE_BENCH_SHA256"],
        "mount_executable": e["CACHE_GATE_MOUNT_EXECUTABLE"],
        "mount_executable_sha256": e["CACHE_GATE_MOUNT_EXECUTABLE_SHA256"],
    },
    "correctness_manifest": {
        "path": str(correctness_path),
        "sha256": e["CACHE_GATE_CORRECTNESS_SHA256"],
    },
}
for path, payload in ((pathlib.Path(sys.argv[1]), environment), (pathlib.Path(sys.argv[2]), manifest)):
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

rm -f -- "$idle_snapshot_file" "$first_runtime_file"
if [[ "$pressure_scenario" == true ]]; then
  setup_working_set_bytes=$((512 * 1024 * 1024))
fi
workload_owner=bench
workload_run_dir=""
inline_workload_run_dir=""
generated_report=""
pressure_setup_dir=""
if [[ "$workload" != large-single && "$workload" != fixed-1m ]]; then
  workload_owner=inline
  inline_workload_run_dir="$(prepare_inline_workload_directory)"
  workload_run_dir="$inline_workload_run_dir"
fi
if [[ "$pressure_scenario" == true ]]; then
  pressure_setup_dir="$mount_point/studiofs-cache-gate-pressure-setup-${mode}-${workload}-rep-$(printf '%03d' "$repetition")"
  [[ ! -e "$pressure_setup_dir" && ! -L "$pressure_setup_dir" ]] || \
    die "pressure setup directory already exists: $pressure_setup_dir"
  mkdir -- "$pressure_setup_dir" || die "failed to create pressure setup directory"
fi

if [[ "$workload" == warm-reopen ]]; then
  if ! run_inline_client warm-prep "$workload_run_dir" \
    "$artifact_dir/.warm-prep.json" "$workload_bytes" "" \
    > "$artifact_dir/warm-prep.stdout.txt" \
    2> "$artifact_dir/warm-prep.stderr.txt"; then
    die "warm workload preparation failed"
  fi
  wait_for_idle_boundary "$artifact_dir/runtime-foreground-start.json"
else
  printf '%s\n' "$final_runtime_snapshot" > "$artifact_dir/runtime-foreground-start.json"
fi
capture_proc_boundary foreground-start
capture_cgroup_boundary foreground-start

case "$workload" in
  large-single)
    cat > "$artifact_dir/command.txt" <<COMMAND
"$bench_bin" --scripted --target "$mount_point" \\
  --workload-bytes "$workload_bytes" --run-mode mounted \\
  --mode read-write --layout single-file --cache enabled --keep-files --save-report
COMMAND
    ;;
  fixed-1m)
    cat > "$artifact_dir/command.txt" <<COMMAND
"$bench_bin" --scripted --target "$mount_point" \\
  --workload-bytes "$workload_bytes" --run-mode mounted \\
  --mode read-write --file-size-mb 1 --cache enabled --keep-files --save-report
COMMAND
    ;;
  small-128k)
    cat > "$artifact_dir/command.txt" <<COMMAND
inline-python small-128k --run-dir "$workload_run_dir" --files 1000 --file-bytes 131072
COMMAND
    ;;
  warm-reopen)
    cat > "$artifact_dir/command.txt" <<COMMAND
inline-python warm-prep --run-dir "$workload_run_dir" --bytes "$workload_bytes"
inline-python warm-read --run-dir "$workload_run_dir" --pass 1
idle-boundary runtime-warm-first-end.json
inline-python warm-read --run-dir "$workload_run_dir" --pass 2
inline-python warm-aggregate --bytes-read "$((2 * workload_bytes))"
COMMAND
    ;;
  same-file-parallel)
    cat > "$artifact_dir/command.txt" <<COMMAND
inline-python same-file-parallel --run-dir "$workload_run_dir" --parallelism "$parallelism" --region-bytes 67108864
COMMAND
    ;;
  cross-file-barriers)
    cat > "$artifact_dir/command.txt" <<COMMAND
inline-python cross-file-barriers --run-dir "$workload_run_dir" --parallelism 2 --region-bytes 67108864
COMMAND
    ;;
esac
LC_ALL=C "$pidstat_bin" -urd -h -p "$mount_pid" 1 \
  > "$artifact_dir/pidstat.txt" 2> "$artifact_dir/pidstat.stderr.txt" &
pidstat_pid=$!
pidstat_start_time="$(mount_start_time "$proc_root/$pidstat_pid/stat")"
trap stop_helpers_on_exit EXIT
PYTHONDONTWRITEBYTECODE=1 python3 - \
  "$proc_root/$mount_pid/status" \
  "$cgroup/memory.current" "$cgroup/memory.peak" "$cgroup/memory.stat" \
  "$artifact_dir/process-memory-samples.jsonl" \
  "$artifact_dir/cgroup-memory-samples.jsonl" \
  "$memory_sample_interval" \
  > /dev/null 2> "$artifact_dir/memory-sampler.stderr.txt" <<'PY' &
import json
import math
import pathlib
import signal
import sys
import time

status_path = pathlib.Path(sys.argv[1])
current_path = pathlib.Path(sys.argv[2])
peak_path = pathlib.Path(sys.argv[3])
stat_path = pathlib.Path(sys.argv[4])
process_output_path = pathlib.Path(sys.argv[5])
cgroup_output_path = pathlib.Path(sys.argv[6])
interval = float(sys.argv[7])
if not math.isfinite(interval) or interval <= 0:
    raise SystemExit("invalid memory sample interval")

running = True


def stop(_signum: int, _frame: object) -> None:
    global running
    running = False


def status_bytes(name: str) -> int:
    matches = []
    for line in status_path.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{name}:"):
            matches.append(line.split())
    if len(matches) != 1 or len(matches[0]) != 3 or matches[0][2] != "kB":
        raise ValueError(f"invalid {name} in mount process status")
    value = int(matches[0][1]) * 1024
    if value < 0:
        raise ValueError(f"negative {name} in mount process status")
    return value


def nonnegative_file(path: pathlib.Path) -> int:
    value = int(path.read_text(encoding="utf-8").strip())
    if value < 0:
        raise ValueError(f"negative value in {path}")
    return value


def memory_stat() -> dict[str, int]:
    result = {}
    for line in stat_path.read_text(encoding="utf-8").splitlines():
        key, raw_value = line.split()
        value = int(raw_value)
        if value < 0:
            raise ValueError(f"negative memory.stat value: {key}")
        result[key] = value
    for required in ("anon", "file"):
        if required not in result:
            raise ValueError(f"memory.stat missing {required}")
    return result


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
try:
    with process_output_path.open("w", encoding="utf-8", buffering=1) as process_output, \
        cgroup_output_path.open("w", encoding="utf-8", buffering=1) as cgroup_output:
        while running:
            sampled_ns = time.monotonic_ns()
            stat = memory_stat()
            process_sample = {
                "monotonic_ns": sampled_ns,
                "vm_rss_bytes": status_bytes("VmRSS"),
                "rss_anon_bytes": status_bytes("RssAnon"),
            }
            cgroup_sample = {
                "monotonic_ns": sampled_ns,
                "memory_current_bytes": nonnegative_file(current_path),
                "memory_peak_bytes": nonnegative_file(peak_path),
                "anon_bytes": stat["anon"],
                "file_bytes": stat["file"],
            }
            process_output.write(json.dumps(process_sample, separators=(",", ":")) + "\n")
            cgroup_output.write(json.dumps(cgroup_sample, separators=(",", ":")) + "\n")
            time.sleep(interval)
except (OSError, ValueError) as error:
    raise SystemExit(f"memory sampler failed: {error}") from error
PY
memory_sampler_pid=$!
memory_sampler_start_time="$(mount_start_time "$proc_root/$memory_sampler_pid/stat")"
sleep "$memory_sample_interval"
helper_pid_is_live "$pidstat_pid" "$pidstat_start_time" || die "pidstat failed to start"
helper_pid_is_live "$memory_sampler_pid" "$memory_sampler_start_time" || \
  die "memory sampler failed to start"

if [[ "$pressure_scenario" == true ]]; then
  pressure_setup_ready="$artifact_dir/.pressure-setup.ready"
  pressure_setup_release="$artifact_dir/.pressure-setup.release"
  run_inline_client pressure-setup "$pressure_setup_dir" \
    "$artifact_dir/pressure-setup.json" "$setup_working_set_bytes" "" \
    "$pressure_setup_ready" "$pressure_setup_release" \
    > "$artifact_dir/pressure-setup.stdout.txt" \
    2> "$artifact_dir/pressure-setup.stderr.txt" &
  pressure_setup_pid=$!
  pressure_setup_start_time="$(mount_start_time "$proc_root/$pressure_setup_pid/stat")"
  wait_for_file "$pressure_setup_ready" "pressure setup readiness" \
    "$pressure_setup_pid" "$pressure_setup_start_time"
  verify_cgroup_membership "$proc_root/$pressure_setup_pid/cgroup" "pressure feeder"
  required_admitted="$(PYTHONDONTWRITEBYTECODE=1 python3 -c '
import json, sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["required_admitted_bytes"])
' "$artifact_dir/memory-pressure-config.json")"
  wait_for_admitted_setup "$artifact_dir/runtime-setup-observed.json" \
    "$pressure_setup_pid" "$required_admitted" "$pressure_setup_start_time"
  setup_observed_ns="$(monotonic_ns)"
  holder_bytes="$(PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 - \
    "$cgroup" "$proc_root/meminfo" "$artifact_dir/memory-pressure-config.json" \
    "$artifact_dir/runtime-setup-observed.json" <<'PY'
import json
import pathlib
import sys
from tools.perf import linux_cache_gate_summary as gate

cgroup = pathlib.Path(sys.argv[1])
config_path = pathlib.Path(sys.argv[3])
config = json.loads(config_path.read_text(encoding="utf-8"))
sample = gate.memory_pressure_sample(json.loads(pathlib.Path(sys.argv[4]).read_text()))
host_available = next(
    int(line.split()[1]) * 1024
    for line in pathlib.Path(sys.argv[2]).read_text().splitlines()
    if line.startswith("MemAvailable:")
)
parent_raw = (cgroup.parent / "memory.max").read_text().strip()
parent_available = None
if parent_raw != "max":
    parent_available = int(parent_raw) - int((cgroup.parent / "memory.current").read_text())
scenario = gate.derive_pressure_scenario(
    memory_current=int((cgroup / "memory.current").read_text()),
    memory_max=int((cgroup / "memory.max").read_text()),
    host_available=host_available,
    parent_available=parent_available,
    observed_admitted_lower_bound=sample["admitted_lower_bound_bytes"],
    negotiated_max_write=config["negotiated_max_write_bytes"],
)
scenario["observed_generation"] = sample["generation"]
config.update(scenario)
config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
print(scenario["holder_bytes"])
PY
)"
  memory_holder_start="$artifact_dir/.memory-holder.start"
  memory_holder_ready="$artifact_dir/.memory-holder.ready"
  memory_holder_release="$artifact_dir/.memory-holder.release"
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 -c '
from pathlib import Path
import sys
from tools.perf.linux_cache_gate_summary import run_memory_holder
run_memory_holder(int(sys.argv[1]), *map(Path, sys.argv[2:]))
' \
    "$holder_bytes" "$memory_holder_start" "$memory_holder_ready" \
    "$memory_holder_release" "$artifact_dir/memory-holder.json" \
    > "$artifact_dir/memory-holder.stdout.txt" \
    2> "$artifact_dir/memory-holder.stderr.txt" &
  memory_holder_pid=$!
  memory_holder_start_time="$(mount_start_time "$proc_root/$memory_holder_pid/stat")"
  move_pid_to_cgroup "$memory_holder_pid" \
    "$proc_root/$memory_holder_pid/cgroup" "memory holder"
  touch "$memory_holder_start"
  wait_for_file "$memory_holder_ready" readiness \
    "$memory_holder_pid" "$memory_holder_start_time"
  target_envelope="$(PYTHONDONTWRITEBYTECODE=1 python3 -c '
import json, sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["target_envelope_bytes"])
' "$artifact_dir/memory-pressure-config.json")"
  wait_for_memory_pressure_state constrained \
    "$artifact_dir/runtime-pressure-before-profile.json" \
    "$pressure_setup_pid" "$((target_envelope + 1))" "$pressure_setup_start_time"
  stamp_runtime_snapshot "$artifact_dir/runtime-pressure-before-profile.json"
  capture_memory_event_boundary pressure "$artifact_dir/memory-events-pressure.json"
  cat >> "$artifact_dir/command.txt" <<COMMAND
pressure-feeder --o-direct --working-set "$setup_working_set_bytes"
observe-live-admission --minimum "$required_admitted"
memory-holder --bytes "$holder_bytes" --cgroup "$cgroup"
constrained-boundary --require-feeder-live --admitted-gt-envelope
profile-async --profile "$workload" --run-dir <owned-workload-dir>
profile-read-measure --method inline-unbuffered-read-v1
profile-hash --separate-from-timing
constrained-boundary-after-profile --require-same-feeder-live
pressure-feeder-stop
memory-holder-release
after-holder-release-boundary
profile-recovery --profile "$workload" --run-dir <owned-workload-dir> --trials 1
recovery-hash --run-dir <owned-workload-dir>
COMMAND
fi

foreground_start_ns="$(monotonic_ns)"
command_exit_status=0
pressure_profile_started_ns=""
pressure_profile_completed_ns=""
profile_workload_pid=""
profile_workload_start_time=""
execute_workload() {
  case "$workload" in
    large-single)
      (
        cd "$artifact_dir"
        "$bench_bin" --scripted --target "$mount_point" \
          --workload-bytes "$workload_bytes" --run-mode mounted \
          --mode read-write --layout single-file --cache enabled --keep-files --save-report
      )
      ;;
    fixed-1m)
      (
        cd "$artifact_dir"
        "$bench_bin" --scripted --target "$mount_point" \
          --workload-bytes "$workload_bytes" --run-mode mounted \
          --mode read-write --file-size-mb 1 --cache enabled --keep-files --save-report
      )
      ;;
    small-128k)
      run_inline_client small-128k "$workload_run_dir" \
        "$artifact_dir/client-report.json" "$workload_bytes" ""
      ;;
    warm-reopen)
      run_inline_client warm-read "$workload_run_dir" \
        "$artifact_dir/.warm-pass-1.json" "$workload_bytes" "" \
        "$artifact_dir/.warm-prep.json" 1 &&
      wait_for_idle_boundary "$artifact_dir/runtime-warm-first-end.json" &&
      run_inline_client warm-read "$workload_run_dir" \
        "$artifact_dir/.warm-pass-2.json" "$workload_bytes" "" \
        "$artifact_dir/.warm-prep.json" 2 &&
      run_inline_client warm-aggregate "$workload_run_dir" \
        "$artifact_dir/client-report.json" "$workload_bytes" "" \
        "$artifact_dir/.warm-prep.json" \
        "$artifact_dir/.warm-pass-1.json" "$artifact_dir/.warm-pass-2.json" &&
      rm -f -- "$artifact_dir/.warm-prep.json" \
        "$artifact_dir/.warm-pass-1.json" "$artifact_dir/.warm-pass-2.json"
      ;;
    same-file-parallel)
      run_inline_client same-file-parallel "$workload_run_dir" \
        "$artifact_dir/client-report.json" "$workload_bytes" "$parallelism"
      ;;
    cross-file-barriers)
      run_inline_client cross-file-barriers "$workload_run_dir" \
        "$artifact_dir/client-report.json" "$workload_bytes" 2
      ;;
  esac
}

if [[ "$pressure_scenario" == true ]]; then
  helper_pid_is_live "$pressure_setup_pid" "$pressure_setup_start_time" || \
    die "pressure feeder identity changed before workload profile"
  pressure_profile_started_ns="$(monotonic_ns)"
  execute_workload > "$artifact_dir/workload.stdout.txt" \
    2> "$artifact_dir/workload.stderr.txt" &
  pressure_workload_pid=$!
  pressure_workload_start_time="$(mount_start_time "$proc_root/$pressure_workload_pid/stat")"
  profile_workload_pid="$pressure_workload_pid"
  profile_workload_start_time="$pressure_workload_start_time"
  if wait "$pressure_workload_pid"; then
    command_exit_status=0
  else
    command_exit_status=$?
  fi
  pressure_profile_completed_ns="$(monotonic_ns)"
  helper_pid_is_live "$pressure_setup_pid" "$pressure_setup_start_time" || \
    die "pressure feeder identity changed during workload profile"
  pressure_workload_pid=""
  pressure_workload_start_time=""
elif execute_workload > "$artifact_dir/workload.stdout.txt" \
  2> "$artifact_dir/workload.stderr.txt"; then
  command_exit_status=0
else
  command_exit_status=$?
fi

if [[ "$pressure_scenario" == true ]]; then
  pressure_workload_bytes="$workload_bytes"
  if [[ "$workload" == small-128k ]]; then
    pressure_workload_bytes=131072000
  fi
  pressure_run_dir="$workload_run_dir"
  if [[ "$workload_owner" == bench && "$command_exit_status" == 0 ]]; then
    shopt -s nullglob
    pressure_reports=("$artifact_dir"/studiofs-bench-report-*.json)
    shopt -u nullglob
    (( ${#pressure_reports[@]} == 1 )) || die "pressure workload emitted an invalid report count"
    pressure_run_dir="$(PYTHONDONTWRITEBYTECODE=1 python3 - \
      "${pressure_reports[0]}" "$mount_point" <<'PY'
import json
import os
import pathlib
import sys

report = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
raw_run_dir = report.get("run", {}).get("run_dir")
if not isinstance(raw_run_dir, str) or not os.path.isabs(raw_run_dir):
    raise SystemExit("Error: pressure benchmark run_dir is invalid")
run_dir = pathlib.Path(os.path.abspath(os.path.normpath(raw_run_dir)))
mount_point = pathlib.Path(os.path.abspath(os.path.normpath(sys.argv[2])))
if run_dir.parent != mount_point or not run_dir.name.startswith("studiofs-bench-run-"):
    raise SystemExit("Error: pressure benchmark run_dir is not an owned child")
if run_dir.is_symlink() or not run_dir.is_dir():
    raise SystemExit("Error: pressure benchmark run_dir is missing or a symlink")
print(run_dir)
PY
)"
  fi
  if [[ "$command_exit_status" == 0 ]]; then
    pressure_report="$artifact_dir/client-report.json"
    [[ "$workload_owner" != bench ]] || pressure_report="${pressure_reports[0]}"
    PYTHONDONTWRITEBYTECODE=1 python3 - \
      "$pressure_report" "$workload" "$artifact_dir/pressure-profile.json" \
      "$workload_owner" "$profile_workload_pid" "$profile_workload_start_time" \
      "$pressure_setup_pid" "$pressure_setup_start_time" \
      "$pressure_profile_started_ns" "$pressure_profile_completed_ns" \
      "$command_exit_status" <<'PY'
import hashlib
import json
import pathlib
import sys

report_path = pathlib.Path(sys.argv[1])
report = json.loads(report_path.read_text(encoding="utf-8"))
if "phase_throughput_mb_per_second" in report:
    value = report["phase_throughput_mb_per_second"].get("read")
else:
    reads = [item for item in report.get("passes", []) if item.get("phase") == "read"]
    value = reads[0].get("metrics", {}).get("average_mb_per_second") if len(reads) == 1 else None
if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
    raise SystemExit("Error: pressure profile read throughput is invalid")
pathlib.Path(sys.argv[3]).write_text(json.dumps({
    "schema_version": 1,
    "profile": sys.argv[2],
    "runner": sys.argv[4],
    "workload_pid": int(sys.argv[5]),
    "workload_start_time": sys.argv[6],
    "feeder_pid": int(sys.argv[7]),
    "feeder_start_time": sys.argv[8],
    "feeder_live_before_profile": True,
    "feeder_live_after_profile": True,
    "started_monotonic_ns": int(sys.argv[9]),
    "completed_monotonic_ns": int(sys.argv[10]),
    "exit_status": int(sys.argv[11]),
    "source_report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
    # Diagnostic only: recovery gating uses the identical inline read measurer.
    "read_throughput_mb_per_second": float(value),
}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
    run_inline_client profile-read-measure "$pressure_run_dir" \
      "$artifact_dir/pressure-measure.json" "$pressure_workload_bytes" ""
    run_inline_client pressure-hash "$pressure_run_dir" \
      "$artifact_dir/pressure-hash.json" "$pressure_workload_bytes" ""
    capture_constrained_pressure_once \
      "$artifact_dir/runtime-pressure-after-profile.json" \
      "$pressure_setup_pid" "$pressure_setup_start_time" \
      "$((target_envelope + 1))"
    stamp_runtime_snapshot "$artifact_dir/runtime-pressure-after-profile.json"
  fi
  feeder_pid="$pressure_setup_pid"
  feeder_start_time="$pressure_setup_start_time"
  touch "$pressure_setup_release"
  stop_helper_bounded "$pressure_setup_pid" pressure-setup \
    "$pressure_setup_start_time" || die "pressure feeder cleanup failed"
  pressure_setup_pid=""
  pressure_setup_start_time=""
  PYTHONDONTWRITEBYTECODE=1 python3 - \
    "$artifact_dir/pressure-setup.json" "$artifact_dir/memory-pressure-config.json" \
    "$feeder_pid" "$feeder_start_time" "$setup_observed_ns" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text())
config = json.loads(pathlib.Path(sys.argv[2]).read_text())
payload.update({
    "observed_admitted_lower_bound_bytes": config[
        "observed_admitted_lower_bound_bytes"
    ],
    "observed_generation": config["observed_generation"],
    "observed_monotonic_ns": int(sys.argv[5]),
    "feeder_pid": int(sys.argv[3]),
    "feeder_start_time": sys.argv[4],
    "feeder_live_before_profile": True,
    "feeder_live_after_profile": True,
})
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY
  rm -f -- "$pressure_setup_ready" "$pressure_setup_release"
  cleanup_target_is_safe "$pressure_setup_dir" pressure-setup || \
    die "refusing to delete unsafe pressure setup directory"
  rm -rf -- "$pressure_setup_dir" || die "failed to delete pressure setup directory"
  touch "$memory_holder_release"
  wait_for_file "$artifact_dir/memory-holder.json" exit \
    "$memory_holder_pid" "$memory_holder_start_time"
  if wait "$memory_holder_pid"; then
    memory_holder_exit_status=0
  else
    memory_holder_exit_status=$?
  fi
  (( memory_holder_exit_status == 0 )) || die "memory holder exited with status $memory_holder_exit_status"
  memory_holder_pid=""
  memory_holder_start_time=""
  rm -f -- "$memory_holder_start" "$memory_holder_ready" "$memory_holder_release"
  capture_memory_event_boundary after-holder-release \
    "$artifact_dir/memory-events-after-holder-release.json"
  capture_runtime_snapshot > "$artifact_dir/runtime-recovery-boundary.json"
  if [[ "$mode" == kernel ]]; then
    cp -- "$artifact_dir/runtime-recovery-boundary.json" \
      "$artifact_dir/runtime-recovery-1.json"
  else
    wait_for_memory_pressure_state normal "$artifact_dir/runtime-recovery-1.json"
  fi
  if [[ "$command_exit_status" == 0 ]]; then
    run_inline_client profile-read-measure "$pressure_run_dir" \
      "$artifact_dir/recovery-measure.json" "$pressure_workload_bytes" ""
    run_inline_client recovery-hash "$pressure_run_dir" \
      "$artifact_dir/recovery-hash.json" "$pressure_workload_bytes" "" \
      "$artifact_dir/pressure-hash.json"
  fi
  if [[ "$mode" == kernel ]]; then
    capture_runtime_snapshot > "$artifact_dir/runtime-recovery-2.json"
  else
    wait_for_memory_pressure_state normal "$artifact_dir/runtime-recovery-2.json"
  fi
fi

foreground_end_ns="$(monotonic_ns)"
foreground_pidstat_line_count="$(wc -l < "$artifact_dir/pidstat.txt")"
capture_proc_boundary foreground-end
capture_runtime_snapshot > "$artifact_dir/runtime-foreground-end.json"
capture_cgroup_boundary foreground-end
helper_pid_is_live "$pidstat_pid" "$pidstat_start_time" || \
  die "pidstat exited during foreground"
helper_pid_is_live "$memory_sampler_pid" "$memory_sampler_start_time" || \
  die "memory sampler exited during foreground"

wait_for_idle_boundary "$artifact_dir/runtime-drain-end.json"
drain_pidstat_deadline=$((SECONDS + 3))
while (( $(wc -l < "$artifact_dir/pidstat.txt") <= foreground_pidstat_line_count )); do
  helper_pid_is_live "$pidstat_pid" "$pidstat_start_time" || \
    die "pidstat exited during drain"
  (( SECONDS <= drain_pidstat_deadline )) || die "pidstat produced no drain sample"
  sleep 0.05
done
sleep 0.05
capture_proc_boundary drain-end
capture_cgroup_boundary drain-end
if [[ "$pressure_scenario" == true ]]; then
  capture_memory_event_boundary after "$artifact_dir/memory-events-after.json"
fi
drain_end_ns="$(monotonic_ns)"
stop_measurement_helpers
trap - EXIT

[[ -s "$artifact_dir/pidstat.txt" ]] || die "pidstat produced no output"
[[ -s "$artifact_dir/process-memory-samples.jsonl" ]] || die "process memory sampler produced no output"
[[ -s "$artifact_dir/cgroup-memory-samples.jsonl" ]] || die "cgroup memory sampler produced no output"

shopt -s nullglob
generated_reports=("$artifact_dir"/studiofs-bench-report-*.json)
shopt -u nullglob
if [[ "$workload_owner" == bench ]]; then
  (( ${#generated_reports[@]} == 1 )) || die "exactly one generated benchmark JSON is required"
  generated_report="${generated_reports[0]}"
  if ! workload_run_dir="$(
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 - \
      "$generated_report" "$artifact_dir/client-report.json" \
      "$workload" "$mount_point" "$workload_bytes" <<'PY'
import json
import os
import pathlib
import shutil
import sys

from tools.perf import linux_cache_gate_summary as gate

source = pathlib.Path(sys.argv[1])
destination = pathlib.Path(sys.argv[2])
try:
    report = json.loads(source.read_text(encoding="utf-8"))
    gate.validate_bench_report(
        report,
        workload=sys.argv[3],
        mount_point=sys.argv[4],
        generated_json_files=[str(source)],
        workload_bytes=int(sys.argv[5]),
    )
    raw_run_dir = report.get("run", {}).get("run_dir")
    if not isinstance(raw_run_dir, str) or not os.path.isabs(raw_run_dir):
        raise gate.GateValidationError("benchmark run_dir must be absolute")
    run_dir = pathlib.Path(os.path.abspath(os.path.normpath(raw_run_dir)))
    mount_point = pathlib.Path(os.path.abspath(os.path.normpath(sys.argv[4])))
    if run_dir.parent != mount_point or not run_dir.name.startswith("studiofs-bench-run-"):
        raise gate.GateValidationError("benchmark run_dir is not a direct owned workload child")
    if run_dir.is_symlink() or not run_dir.is_dir():
        raise gate.GateValidationError("benchmark run_dir is missing or is a symlink")
    shutil.copyfile(source, destination)
except (json.JSONDecodeError, OSError, ValueError, gate.GateValidationError) as error:
    raise SystemExit(f"Error: benchmark report validation failed: {error}") from error
print(run_dir)
PY
  )"; then
    die "benchmark report validation failed"
  fi
else
  (( ${#generated_reports[@]} == 0 )) || \
    die "inline workloads must not emit benchmark JSON reports"
  CACHE_GATE_PARALLELISM="$parallelism" \
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 - \
    "$artifact_dir/client-report.json" "$workload" "$workload_bytes" <<'PY'
import json
import math
import os
import pathlib
import sys

from tools.perf import linux_cache_gate_summary as gate

try:
    report = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
    parallelism = int(os.environ["CACHE_GATE_PARALLELISM"]) \
        if os.environ["CACHE_GATE_PARALLELISM"] else None
    gate.validate_python_client_report(
        report,
        workload=sys.argv[2],
        workload_bytes=int(sys.argv[3]),
        parallelism=parallelism,
    )
    throughput = report.get("phase_throughput_mb_per_second")
    if not isinstance(throughput, dict) or not throughput:
        raise gate.GateValidationError("Python client throughput evidence is missing")
    for phase, value in throughput.items():
        if (
            not isinstance(phase, str)
            or not phase
            or not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value <= 0
        ):
            raise gate.GateValidationError("Python client throughput evidence is invalid")
except (json.JSONDecodeError, OSError, ValueError, gate.GateValidationError) as error:
    raise SystemExit(f"Error: Python client report validation failed: {error}") from error
PY
fi

cleanup_started_ns="$(monotonic_ns)"
cleanup_error=""
if ! cleanup_target_is_safe "$workload_run_dir" "$workload_owner"; then
  cleanup_error="refusing to delete unsafe workload directory"
elif ! rm -rf -- "$workload_run_dir"; then
  cleanup_error="failed to delete workload directory"
fi
if [[ -z "$cleanup_error" && ( -e "$workload_run_dir" || -L "$workload_run_dir" ) ]]; then
  cleanup_error="workload directory remains after deletion"
fi
wait_for_idle_boundary "$artifact_dir/runtime-cleanup-end.json"
cleanup_ended_ns="$(monotonic_ns)"
CACHE_GATE_CLEANUP_ERROR="$cleanup_error" \
CACHE_GATE_CLEANUP_STARTED_NS="$cleanup_started_ns" \
CACHE_GATE_CLEANUP_ENDED_NS="$cleanup_ended_ns" \
PYTHONDONTWRITEBYTECODE=1 python3 - "$artifact_dir/cleanup.json" <<'PY'
import json
import os
import pathlib
import sys

error = os.environ["CACHE_GATE_CLEANUP_ERROR"] or None
payload = {
    "schema_version": 1,
    "cleanup_error": error,
    "cleanup_drain_idle": error is None,
    "workload_path_removed": error is None,
    "cleanup_started_monotonic_ns": int(os.environ["CACHE_GATE_CLEANUP_STARTED_NS"]),
    "cleanup_ended_monotonic_ns": int(os.environ["CACHE_GATE_CLEANUP_ENDED_NS"]),
}
pathlib.Path(sys.argv[1]).write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY
[[ -z "$cleanup_error" ]] || die "$cleanup_error"

clock_ticks_per_second="$(getconf CLK_TCK)"
CACHE_GATE_MODE="$mode" \
CACHE_GATE_WORKLOAD="$workload" \
CACHE_GATE_MOUNT_POINT="$mount_point" \
CACHE_GATE_MOUNT_PID="$mount_pid" \
CACHE_GATE_MOUNT_PID_START_TIME="$mount_pid_start_time" \
CACHE_GATE_WORKLOAD_BYTES="$workload_bytes" \
CACHE_GATE_COMMAND_STATUS="$command_exit_status" \
CACHE_GATE_FOREGROUND_START_NS="$foreground_start_ns" \
CACHE_GATE_FOREGROUND_END_NS="$foreground_end_ns" \
CACHE_GATE_DRAIN_END_NS="$drain_end_ns" \
CACHE_GATE_PIDSTAT_FOREGROUND_LINES="$foreground_pidstat_line_count" \
CACHE_GATE_CLK_TCK="$clock_ticks_per_second" \
CACHE_GATE_SAMPLE_INTERVAL="$memory_sample_interval" \
CACHE_GATE_TEST_FIXTURE="$test_fixture" \
CACHE_GATE_DIAGNOSTIC="$diagnostic" \
CACHE_GATE_PARALLELISM="$parallelism" \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root" python3 - \
  "$artifact_dir" "$generated_report" <<'PY'
import json
import math
import os
import pathlib
import sys

from tools.perf import linux_cache_gate_summary as gate

artifact = pathlib.Path(sys.argv[1])
generated_report_path = pathlib.Path(sys.argv[2]) if sys.argv[2] else None
e = os.environ


def read_json(path: pathlib.Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise gate.GateValidationError(f"JSON artifact is not an object: {path}")
    return payload


def read_jsonl(path: pathlib.Path) -> list[dict]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    if not rows or not all(isinstance(row, dict) for row in rows):
        raise gate.GateValidationError(f"sample stream is empty or invalid: {path}")
    return rows


def nonnegative_integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise gate.GateValidationError(f"invalid nonnegative counter: {label}")
    return value


def counter_delta(before: object, after: object, label: str) -> int:
    before_value = nonnegative_integer(before, f"{label}.before")
    after_value = nonnegative_integer(after, f"{label}.after")
    if after_value < before_value:
        raise gate.GateValidationError(f"counter decreased: {label}")
    return after_value - before_value


def proc_cpu(path: pathlib.Path) -> tuple[int, int, str]:
    value = path.read_text(encoding="utf-8")
    closing = value.rfind(")")
    fields = value[closing + 2 :].split()
    if closing < 0 or len(fields) < 20:
        raise gate.GateValidationError(f"malformed process stat: {path}")
    user = int(fields[11])
    system = int(fields[12])
    if user < 0 or system < 0:
        raise gate.GateValidationError("negative process CPU counter")
    return user, system, fields[19]


def proc_io(path: pathlib.Path) -> dict[str, int]:
    result = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        name, separator, raw_value = line.partition(":")
        if separator:
            result[name] = int(raw_value.strip())
    required = {
        "rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes",
        "cancelled_write_bytes",
    }
    if set(result) != required:
        raise gate.GateValidationError("process io snapshot has an unexpected shape")
    return result


def io_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    result = {name: after[name] - before[name] for name in sorted(before)}
    for name, value in result.items():
        if name != "cancelled_write_bytes" and value < 0:
            raise gate.GateValidationError(f"process IO counter decreased: {name}")
    return result


def pidstat_samples(path: pathlib.Path, foreground_lines: int, pid: int) -> tuple[list[float], list[float]]:
    header = None
    foreground = []
    drain = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            columns = stripped.lstrip("# ").split()
            if "PID" in columns and "%CPU" in columns:
                header = columns
            continue
        if not stripped or header is None:
            continue
        values = stripped.split()
        pid_index = header.index("PID")
        cpu_index = header.index("%CPU")
        if len(values) <= max(pid_index, cpu_index) or values[pid_index] != str(pid):
            continue
        try:
            cores = float(values[cpu_index]) / 100.0
        except ValueError as error:
            raise gate.GateValidationError("pidstat %CPU is locale-dependent or invalid") from error
        if not math.isfinite(cores) or cores < 0:
            raise gate.GateValidationError("pidstat %CPU is invalid")
        (foreground if line_number <= foreground_lines else drain).append(cores)
    if not foreground:
        raise gate.GateValidationError("pidstat has no foreground samples")
    if not drain:
        raise gate.GateValidationError("pidstat has no drain samples")
    return foreground, drain


def core_stats(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    p95_index = math.ceil(0.95 * len(ordered)) - 1
    return {
        "average": sum(samples) / len(samples),
        "p95": ordered[p95_index],
        "peak": max(samples),
    }


def histogram_payload(stage: dict, label: str) -> dict:
    latency = stage.get("latency")
    if not isinstance(latency, dict) or not isinstance(latency.get("latency_histogram"), dict):
        raise gate.GateValidationError(f"missing callback histogram: {label}")
    payload = dict(latency["latency_histogram"])
    payload["latency_quantiles_saturated"] = latency.get(
        "latency_quantiles_saturated", {}
    )
    return payload


def numeric_deltas(before: object, after: object) -> dict[str, int]:
    if not isinstance(before, dict) or not isinstance(after, dict):
        return {}
    result = {}
    for key in sorted(set(before) & set(after)):
        left = before[key]
        right = after[key]
        if (
            isinstance(left, int) and not isinstance(left, bool)
            and isinstance(right, int) and not isinstance(right, bool)
            and right >= left
        ):
            result[key] = right - left
    return result


def value_at(payload: dict, path: tuple[str, ...], label: str) -> object:
    value: object = payload
    for key in path:
        if not isinstance(value, dict) or key not in value:
            raise gate.GateValidationError(f"missing runtime counter: {label}")
        value = value[key]
    return value


mode = e["CACHE_GATE_MODE"]
workload = e["CACHE_GATE_WORKLOAD"]
mount_point = e["CACHE_GATE_MOUNT_POINT"]
mount_pid = int(e["CACHE_GATE_MOUNT_PID"])
workload_bytes = int(e["CACHE_GATE_WORKLOAD_BYTES"])
command_status = int(e["CACHE_GATE_COMMAND_STATUS"])
foreground_start_ns = int(e["CACHE_GATE_FOREGROUND_START_NS"])
foreground_end_ns = int(e["CACHE_GATE_FOREGROUND_END_NS"])
drain_end_ns = int(e["CACHE_GATE_DRAIN_END_NS"])
if not foreground_start_ns < foreground_end_ns <= drain_end_ns:
    raise SystemExit("Error: measurement timestamps are not ordered")
foreground_wall_seconds = (foreground_end_ns - foreground_start_ns) / 1_000_000_000
drain_duration_seconds = (drain_end_ns - foreground_end_ns) / 1_000_000_000

runtime_start = read_json(artifact / "runtime-foreground-start.json")
runtime_end = read_json(artifact / "runtime-foreground-end.json")
runtime_drain = read_json(artifact / "runtime-drain-end.json")
runtime_cleanup = read_json(artifact / "runtime-cleanup-end.json")
runtime_first_end = (
    read_json(artifact / "runtime-warm-first-end.json")
    if workload == "warm-reopen"
    else None
)
if not gate.snapshot_is_idle(runtime_start):
    raise SystemExit("Error: foreground-start runtime snapshot is not idle")
if not gate.snapshot_is_idle(runtime_drain):
    raise SystemExit("Error: drain-end runtime snapshot is not idle")
if not gate.snapshot_is_idle(runtime_cleanup):
    raise SystemExit("Error: cleanup-end runtime snapshot is not idle")

start_write_path = runtime_start.get("write_path")
end_write_path = runtime_end.get("write_path")
drain_write_path = runtime_drain.get("write_path")
if not all(isinstance(item, dict) for item in (start_write_path, end_write_path, drain_write_path)):
    raise SystemExit("Error: runtime write_path evidence is missing")
geometry_keys = (
    "requested_async_dio", "negotiated_async_dio",
    "requested_writeback_cache", "negotiated_writeback_cache",
)
geometry = {
    key: start_write_path["fuse_connection"].get(key)
    for key in geometry_keys
}
for write_path in (end_write_path, drain_write_path):
    if any(write_path["fuse_connection"].get(key) != geometry[key] for key in geometry_keys):
        raise SystemExit("Error: negotiated FUSE geometry changed during measurement")
gate.validate_negotiated_geometry(mode, geometry)

start_callbacks = start_write_path.get("fuse_callbacks")
end_callbacks = end_write_path.get("fuse_callbacks")
drain_callbacks = drain_write_path.get("fuse_callbacks")
if not all(isinstance(item, dict) for item in (start_callbacks, end_callbacks, drain_callbacks)):
    raise SystemExit("Error: FUSE callback evidence is missing")
first_callbacks = None
if runtime_first_end is not None:
    first_callbacks = runtime_first_end.get("write_path", {}).get("fuse_callbacks")
    if not isinstance(first_callbacks, dict):
        raise SystemExit("Error: warm midpoint FUSE callback evidence is missing")
callback_intervals = {}
callback_deltas = {}
callback_errors = 0
all_callback_kinds = ("read", "write", "flush", "fsync", "release")
interval_callback_kinds = (
    ("read", "flush", "release")
    if workload == "warm-reopen"
    else all_callback_kinds
)
for kind in all_callback_kinds:
    before_stage = start_callbacks.get(kind)
    foreground_stage = end_callbacks.get(kind)
    drain_stage = drain_callbacks.get(kind)
    if not all(isinstance(item, dict) for item in (before_stage, foreground_stage, drain_stage)):
        raise SystemExit(f"Error: FUSE {kind} callback evidence is missing")
    if kind in interval_callback_kinds:
        stages = (
            (
                ("first", before_stage, first_callbacks.get(kind)),
                ("second", first_callbacks.get(kind), foreground_stage),
            )
            if first_callbacks is not None
            else ((None, before_stage, foreground_stage),)
        )
        for phase, interval_start, interval_end in stages:
            if not isinstance(interval_start, dict) or not isinstance(interval_end, dict):
                raise SystemExit(f"Error: FUSE {kind} warm callback evidence is missing")
            key = f"{phase}_fuse_{kind}" if phase else f"fuse_{kind}"
            allow_zero = mode == "kernel" and key == "second_fuse_read"
            interval = gate.histogram_interval(
                histogram_payload(interval_start, key),
                histogram_payload(interval_end, key),
                allow_zero=allow_zero,
            )
            gate.validate_histogram_applicability(
                mode,
                workload,
                key,
                interval["samples"],
            )
            callback_intervals[key] = interval
    fields = ("calls", "errors", "bytes") if kind in ("read", "write") else ("calls", "errors")
    callback_deltas[f"fuse_{kind}"] = {
        field: counter_delta(before_stage.get(field), drain_stage.get(field), f"fuse_{kind}.{field}")
        for field in fields
    }
    callback_errors += callback_deltas[f"fuse_{kind}"]["errors"]

start_async = start_write_path.get("fuse_async_write")
drain_async = drain_write_path.get("fuse_async_write")
if not isinstance(start_async, dict) or not isinstance(drain_async, dict):
    raise SystemExit("Error: async write evidence is missing")
start_async_read = start_write_path.get("fuse_async_read")
drain_async_read = drain_write_path.get("fuse_async_read")
if not isinstance(start_async_read, dict) or not isinstance(drain_async_read, dict):
    raise SystemExit("Error: async read evidence is missing")
async_errors = counter_delta(
    start_async.get("failed_writes"), drain_async.get("failed_writes"), "failed_writes"
) + counter_delta(
    start_async_read.get("failed_reads"),
    drain_async_read.get("failed_reads"),
    "failed_reads",
)
task_errors = counter_delta(
    start_async.get("task_failed_writes"),
    drain_async.get("task_failed_writes"),
    "task_failed_writes",
)
cancellations = counter_delta(
    start_async.get("cancelled_writes"),
    drain_async.get("cancelled_writes"),
    "cancelled_writes",
) + counter_delta(
    start_async_read.get("canceled_reads"),
    drain_async_read.get("canceled_reads"),
    "canceled_reads",
)
queue_peaks = {
    "async_write_pending_bytes": max(
        nonnegative_integer(start_async.get("peak_pending_bytes", 0), "start peak_pending_bytes"),
        nonnegative_integer(drain_async.get("peak_pending_bytes", 0), "drain peak_pending_bytes"),
    ),
    "async_write_handle_pending_writes": max(
        nonnegative_integer(
            start_async.get("peak_handle_pending_writes", 0),
            "start peak_handle_pending_writes",
        ),
        nonnegative_integer(
            drain_async.get("peak_handle_pending_writes", 0),
            "drain peak_handle_pending_writes",
        ),
    ),
}

clock_ticks = int(e["CACHE_GATE_CLK_TCK"])
if clock_ticks <= 0:
    raise SystemExit("Error: invalid CLK_TCK")
start_user, start_system, start_time = proc_cpu(artifact / "proc-foreground-start/stat.txt")
end_user, end_system, end_start_time = proc_cpu(artifact / "proc-foreground-end/stat.txt")
drain_user, drain_system, drain_start_time = proc_cpu(artifact / "proc-drain-end/stat.txt")
if {start_time, end_start_time, drain_start_time} != {e["CACHE_GATE_MOUNT_PID_START_TIME"]}:
    raise SystemExit("Error: mount process start time changed during measurement")
for label, before, after in (
    ("foreground user", start_user, end_user),
    ("foreground system", start_system, end_system),
    ("drain user", end_user, drain_user),
    ("drain system", end_system, drain_system),
):
    if after < before:
        raise SystemExit(f"Error: mount CPU counter decreased: {label}")
foreground_user_seconds = (end_user - start_user) / clock_ticks
foreground_system_seconds = (end_system - start_system) / clock_ticks
drain_user_seconds = (drain_user - end_user) / clock_ticks
drain_system_seconds = (drain_system - end_system) / clock_ticks
foreground_cpu_seconds = foreground_user_seconds + foreground_system_seconds
drain_cpu_seconds = drain_user_seconds + drain_system_seconds
total_cpu_seconds = foreground_cpu_seconds + drain_cpu_seconds

foreground_cores, drain_cores = pidstat_samples(
    artifact / "pidstat.txt",
    int(e["CACHE_GATE_PIDSTAT_FOREGROUND_LINES"]),
    mount_pid,
)
foreground_core_stats = core_stats(foreground_cores)
drain_core_stats = core_stats(drain_cores)

process_samples = read_jsonl(artifact / "process-memory-samples.jsonl")
cgroup_samples = read_jsonl(artifact / "cgroup-memory-samples.jsonl")
for row in process_samples:
    for field in ("monotonic_ns", "vm_rss_bytes", "rss_anon_bytes"):
        nonnegative_integer(row.get(field), f"process sample {field}")
for row in cgroup_samples:
    for field in (
        "monotonic_ns", "memory_current_bytes", "memory_peak_bytes",
        "anon_bytes", "file_bytes",
    ):
        nonnegative_integer(row.get(field), f"cgroup sample {field}")

cgroup_start = read_json(artifact / "cgroup-foreground-start.json")
cgroup_end = read_json(artifact / "cgroup-foreground-end.json")
cgroup_drain = read_json(artifact / "cgroup-drain-end.json")


def cgroup_values(payload: dict) -> dict[str, int]:
    stat = payload.get("memory_stat")
    if not isinstance(stat, dict):
        raise gate.GateValidationError("cgroup memory.stat boundary is missing")
    return {
        "total_bytes": nonnegative_integer(payload.get("memory_current"), "memory.current"),
        "anon_bytes": nonnegative_integer(stat.get("anon"), "memory.stat anon"),
        "file_bytes": nonnegative_integer(stat.get("file"), "memory.stat file"),
    }


start_memory = cgroup_values(cgroup_start)
end_memory = cgroup_values(cgroup_end)
drain_memory = cgroup_values(cgroup_drain)


def signed_memory_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {name: after[name] - before[name] for name in sorted(before)}


start_io = proc_io(artifact / "proc-foreground-start/io.txt")
end_io = proc_io(artifact / "proc-foreground-end/io.txt")
drain_io = proc_io(artifact / "proc-drain-end/io.txt")

report = read_json(artifact / "client-report.json")
parallelism = int(e["CACHE_GATE_PARALLELISM"]) if e["CACHE_GATE_PARALLELISM"] else None
phase_throughput = {}
if workload in ("large-single", "fixed-1m"):
    if generated_report_path is None:
        raise SystemExit("Error: generated benchmark report path is missing")
    generated_report = read_json(generated_report_path)
    if report != generated_report:
        raise SystemExit("Error: stable client report differs from generated report")
    expectation = gate.validate_bench_report(
        report,
        workload=workload,
        mount_point=mount_point,
        generated_json_files=[str(generated_report_path)],
        workload_bytes=workload_bytes,
    )
    for item in report["passes"]:
        metrics = item.get("metrics")
        value = metrics.get("average_mb_per_second") if isinstance(metrics, dict) else None
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value <= 0
        ):
            raise SystemExit(f"Error: client phase throughput is invalid: {item.get('phase')}")
        phase_throughput[item["phase"]] = float(value)
else:
    expectation = gate.validate_python_client_report(
        report,
        workload=workload,
        workload_bytes=workload_bytes,
        parallelism=parallelism,
    )
    raw_throughput = report.get("phase_throughput_mb_per_second")
    expected_phases = (
        {"read_pass_1", "read_pass_2"}
        if workload == "warm-reopen"
        else {"write", "read"}
    )
    if not isinstance(raw_throughput, dict) or set(raw_throughput) != expected_phases:
        raise SystemExit("Error: Python client phase throughput keys are invalid")
    for phase, value in raw_throughput.items():
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value <= 0
        ):
            raise SystemExit(f"Error: client phase throughput is invalid: {phase}")
        phase_throughput[phase] = float(value)

verified_written = expectation["verified_bytes_written"]
verified_read = expectation["verified_bytes_read"]
processed_gib = (verified_written + verified_read) / float(1 << 30)
if processed_gib <= 0:
    raise SystemExit("Error: verified processed GiB must be positive")

cleanup = read_json(artifact / "cleanup.json")
if cleanup.get("cleanup_error") is not None or cleanup.get("cleanup_drain_idle") is not True:
    raise SystemExit("Error: measured workload deletion cleanup failed")

warm_proof = None
if workload == "warm-reopen":
    if runtime_first_end is None:
        raise SystemExit("Error: warm first-read boundary is missing")
    if not gate.snapshot_is_idle(runtime_first_end):
        raise SystemExit("Error: warm first-read boundary is not idle")
    warm_counter_paths = {
        "fuse_read_calls": ("write_path", "fuse_callbacks", "read", "calls"),
        "vfs_read_path_calls": ("write_path", "vfs_read_path", "calls"),
        "os_cache_replace": ("write_path", "os_cache", "replace"),
        "os_cache_reuse": ("write_path", "os_cache", "reuse"),
    }
    warm_proof = {
        "checksum_verified": report.get("expected_checksum") == report.get("actual_checksum")
    }
    for label, path in warm_counter_paths.items():
        start_value = value_at(runtime_start, path, f"warm start {label}")
        first_value = value_at(runtime_first_end, path, f"warm first {label}")
        end_value = value_at(runtime_end, path, f"warm second {label}")
        warm_proof[f"first_{label}"] = counter_delta(
            start_value,
            first_value,
            f"warm first {label}",
        )
        warm_proof[f"second_{label}"] = counter_delta(
            first_value,
            end_value,
            f"warm second {label}",
        )

fixture = e["CACHE_GATE_TEST_FIXTURE"] == "true"
diagnostic = e["CACHE_GATE_DIAGNOSTIC"] == "true"
promotion_eligible = not fixture and not diagnostic
memory_pressure = None
pressure_config_path = artifact / "memory-pressure-config.json"
if pressure_config_path.exists():
    memory_pressure = gate.memory_pressure_evidence_from_artifacts(
        artifact,
        verified_written=verified_written,
        verified_read=verified_read,
        foreground_cpu=foreground_cpu_seconds,
        drain_cpu=drain_cpu_seconds,
    )
    gate.validate_memory_pressure_evidence(
        mode,
        workload,
        memory_pressure,
        expected_written=verified_written,
        expected_read=verified_read,
        promotion_eligible=promotion_eligible,
    )
derived = {
    "schema_version": 1,
    "status": "complete",
    "test_fixture": fixture,
    "diagnostic": diagnostic,
    "promotion_eligible": promotion_eligible,
    "command_exit_status": command_status,
    "verified_bytes_written": verified_written,
    "verified_bytes_read": verified_read,
    "processed_gib": processed_gib,
    "foreground_wall_seconds": foreground_wall_seconds,
    "foreground_throughput_bytes_per_second": (
        verified_written + verified_read
    ) / foreground_wall_seconds,
    "client_reported_phase_throughput_mb_per_second": phase_throughput,
    "foreground_mount_user_cpu_seconds": foreground_user_seconds,
    "foreground_mount_system_cpu_seconds": foreground_system_seconds,
    "foreground_mount_cpu_seconds": foreground_cpu_seconds,
    "drain_mount_user_cpu_seconds": drain_user_seconds,
    "drain_mount_system_cpu_seconds": drain_system_seconds,
    "drain_mount_cpu_seconds": drain_cpu_seconds,
    "total_lifecycle_mount_cpu_seconds": total_cpu_seconds,
    "total_lifecycle_cpu_seconds_per_gib": total_cpu_seconds / processed_gib,
    "foreground_average_cores": foreground_core_stats["average"],
    "foreground_p95_cores": foreground_core_stats["p95"],
    "foreground_peak_cores": foreground_core_stats["peak"],
    "drain_average_cores": drain_core_stats["average"],
    "drain_p95_cores": drain_core_stats["p95"],
    "drain_peak_cores": drain_core_stats["peak"],
    "drain_duration_seconds": drain_duration_seconds,
    "daemon_rss_peak_bytes": max(row["vm_rss_bytes"] for row in process_samples),
    "daemon_anon_peak_bytes": max(row["rss_anon_bytes"] for row in process_samples),
    "cgroup_total_peak_bytes": max(
        [row["memory_peak_bytes"] for row in cgroup_samples]
        + [cgroup_start["memory_peak"], cgroup_end["memory_peak"], cgroup_drain["memory_peak"]]
    ),
    "cgroup_file_peak_bytes": max(
        [row["file_bytes"] for row in cgroup_samples]
        + [start_memory["file_bytes"], end_memory["file_bytes"], drain_memory["file_bytes"]]
    ),
    "cgroup_anon_peak_bytes": max(
        [row["anon_bytes"] for row in cgroup_samples]
        + [start_memory["anon_bytes"], end_memory["anon_bytes"], drain_memory["anon_bytes"]]
    ),
    "cgroup_memory_deltas": {
        "foreground": signed_memory_delta(start_memory, end_memory),
        "drain": signed_memory_delta(end_memory, drain_memory),
        "lifecycle": signed_memory_delta(start_memory, drain_memory),
    },
    "process_io_deltas": {
        "foreground": io_delta(start_io, end_io),
        "drain": io_delta(end_io, drain_io),
        "lifecycle": io_delta(start_io, drain_io),
    },
    "callback_errors": callback_errors,
    "async_errors": async_errors,
    "task_errors": task_errors,
    "cancellations": cancellations,
    "callback_deltas": callback_deltas,
    "callback_intervals": callback_intervals,
    "histogram_intervals": callback_intervals,
    "queue_peaks": queue_peaks,
    "negotiated_geometry": geometry,
    "liveness_deltas": numeric_deltas(
        start_write_path.get("fuse_liveness"),
        drain_write_path.get("fuse_liveness"),
    ),
    "foreground_start_idle": True,
    "drain_end_idle": True,
    **({"memory_pressure": memory_pressure} if memory_pressure is not None else {}),
}
gate.validate_raw_callback_intervals(
    mode,
    workload,
    runtime_start,
    runtime_end,
    derived,
    warm_first_end=runtime_first_end,
)
if warm_proof is not None:
    derived["warm_proof"] = warm_proof
    gate.validate_warm_proof(
        mode,
        runtime_start,
        runtime_first_end,
        runtime_end,
        derived,
        checksum_verified=warm_proof["checksum_verified"],
    )

environment = read_json(artifact / "environment.json")
manifest = read_json(artifact / "manifest.json")
for payload in (environment, manifest):
    payload["status"] = "complete"
    payload["test_fixture"] = fixture
    payload["diagnostic"] = diagnostic
    payload["promotion_eligible"] = promotion_eligible
environment["measurement"] = {
    "foreground_start_monotonic_ns": foreground_start_ns,
    "foreground_end_monotonic_ns": foreground_end_ns,
    "drain_end_monotonic_ns": drain_end_ns,
    "pidstat_foreground_line_count": int(e["CACHE_GATE_PIDSTAT_FOREGROUND_LINES"]),
    "memory_sample_interval_seconds": float(e["CACHE_GATE_SAMPLE_INTERVAL"]),
    "pidstat_interval_seconds": 1,
    "clock_ticks_per_second": clock_ticks,
}
gate.validate_workload_manifest(
    manifest,
    workload=workload,
    workload_bytes=workload_bytes,
    parallelism=parallelism,
)
if command_status != 0:
    raise SystemExit(f"Error: workload command exited with status {command_status}")

for path, payload in (
    (artifact / "derived.json", derived),
    (artifact / "environment.json", environment),
    (artifact / "manifest.json", manifest),
):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)
PY

cat > "$artifact_dir/summary.md" <<MD
# StudioFS Linux Cache Gate Summary

Complete measured artifact for \`$mode\` / \`$workload\` repetition $repetition.
MD
if [[ "$test_fixture" == true || "$diagnostic" == true ]]; then
  cat > "$artifact_dir/decision.md" <<'MD'
# StudioFS Linux Cache Gate Decision

Diagnostic evidence is not promotion-eligible.
MD
else
  cat > "$artifact_dir/decision.md" <<'MD'
# StudioFS Linux Cache Gate Decision

Collection completed. Final promotion remains subject to matrix cleanup and summary gates.
MD
fi

echo "$artifact_dir"
