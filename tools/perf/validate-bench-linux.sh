#!/usr/bin/env bash
set -euo pipefail

mount_point=""
runs=5
studiofs_path="studiofs"
output_root=""
block_size="4MB"
big_file_size="1GB"
small_file_size="128KB"
small_file_count=1000
threads=4

usage() {
  cat <<'USAGE'
Usage: validate-bench-linux.sh --mount-point <path> [options]

Options:
  --runs <n>              Number of repeated runs. The recommended run count is 5.
  --studiofs <path>       studiofs binary path. Defaults to PATH lookup.
  --output-root <path>    Artifact root. Defaults to target/perf.
  --block-size <size>     Defaults to 4MB.
  --big-file-size <size>  Defaults to 1GB.
  --small-file-size <size> Defaults to 128KB.
  --small-file-count <n>  Defaults to 1000.
  --threads <n>           Defaults to 4.
USAGE
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
    --mount-point) mount_point="${2:?}"; shift 2 ;;
    --runs) runs="${2:?}"; shift 2 ;;
    --studiofs) studiofs_path="${2:?}"; shift 2 ;;
    --output-root) output_root="${2:?}"; shift 2 ;;
    --block-size) block_size="${2:?}"; shift 2 ;;
    --big-file-size) big_file_size="${2:?}"; shift 2 ;;
    --small-file-size) small_file_size="${2:?}"; shift 2 ;;
    --small-file-count) small_file_count="${2:?}"; shift 2 ;;
    --threads) threads="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$mount_point" ]]; then
  echo "--mount-point is required" >&2
  usage >&2
  exit 2
fi
if [[ "$runs" -lt 1 ]]; then
  echo "--runs must be >= 1" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [[ -z "$output_root" ]]; then
  output_root="$repo_root/target/perf"
fi
mkdir -p "$output_root"

timestamp="$(date -u +%Y%m%d-%H%M%S)"
base_name="linux-bench-validation-$timestamp"
validation_dir="$output_root/$base_name"
suffix=2
while [[ -e "$validation_dir" ]]; do
  validation_dir="$output_root/$base_name-$suffix"
  suffix=$((suffix + 1))
done
mkdir "$validation_dir"

git_commit="$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || true)"
studiofs_version="$("$studiofs_path" --version 2>/dev/null || true)"
os_name="$(uname -a)"
escaped_os="$(json_escape "$os_name")"
escaped_mount_point="$(json_escape "$mount_point")"
escaped_git_commit="$(json_escape "$git_commit")"
escaped_studiofs_version="$(json_escape "$studiofs_version")"
escaped_block_size="$(json_escape "$block_size")"
escaped_big_file_size="$(json_escape "$big_file_size")"
escaped_small_file_size="$(json_escape "$small_file_size")"

cat > "$validation_dir/environment.json" <<JSON
{
  "platform": "Linux",
  "os": "$escaped_os",
  "mount_point": "$escaped_mount_point",
  "git_commit": "$escaped_git_commit",
  "studiofs_version": "$escaped_studiofs_version",
  "benchmark_settings": {
    "block_size": "$escaped_block_size",
    "big_file_size": "$escaped_big_file_size",
    "small_file_size": "$escaped_small_file_size",
    "small_file_count": $small_file_count,
    "threads": $threads,
    "run_count": $runs
  },
  "artifact_layout_note": "Do not overwrite previous benchmark artifacts"
}
JSON

for run in $(seq 1 "$runs"); do
  run_name="$(printf 'run-%03d' "$run")"
  run_dir="$validation_dir/$run_name"
  mkdir "$run_dir"
  report_path="$run_dir/report.json"
  stdout_path="$run_dir/stdout.txt"
  command_path="$run_dir/command.txt"
  cmd=(
    "$studiofs_path" bench "$mount_point"
    --block-size "$block_size"
    --big-file-size "$big_file_size"
    --small-file-size "$small_file_size"
    --small-file-count "$small_file_count"
    --threads "$threads"
    --json-output "$report_path"
  )
  printf '%q ' "${cmd[@]}" > "$command_path"
  printf '\n' >> "$command_path"
  "${cmd[@]}" > "$stdout_path" 2>&1
  test -f "$report_path"
done

summary_path="$validation_dir/summary.md"
{
  echo "# StudioFS Linux benchmark validation"
  echo
  echo "- Platform: Linux"
  echo "- Mount point: \`$mount_point\`"
  echo "- Git commit: \`$git_commit\`"
  echo "- Runs: $runs; recommended run count is 5"
  echo "- Benchmark settings: block size $block_size, big file size $big_file_size, small file size $small_file_size, small file count $small_file_count, threads $threads"
  echo
  echo "Compare medians across runs first, then inspect p95 and p99 latency for tail regressions."
  echo
  echo "## Artifacts"
  echo
  for run in $(seq 1 "$runs"); do
    printf -- "- \`%s/run-%03d\`: command.txt, stdout.txt, report.json\n" "$validation_dir" "$run"
  done
} > "$summary_path"

echo "Benchmark validation artifacts: $validation_dir"
