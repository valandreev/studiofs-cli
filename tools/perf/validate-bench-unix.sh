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
profile=""

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

kernel_name="$(uname -s)"
case "$kernel_name" in
  Darwin)
    platform="macOS"
    artifact_platform="macos"
    ;;
  Linux)
    platform="Linux"
    artifact_platform="linux"
    ;;
  *)
    platform="Unix-like ($kernel_name)"
    artifact_platform="unix"
    ;;
esac

usage() {
  cat <<'USAGE'
Usage: validate-bench-unix.sh --mount-point <path> [options]

Options:
  --runs <n>              Number of repeated runs. The recommended run count is 5.
  --studiofs <path>       studiofs binary path. Defaults to PATH lookup.
  --output-root <path>    Artifact root. Defaults to target/perf.
  --block-size <size>     Defaults to 4MB.
  --big-file-size <size>  Defaults to 1GB.
  --small-file-size <size> Defaults to 128KB.
  --small-file-count <n>  Defaults to 1000.
  --threads <n>           Defaults to 4.
  --profile <name>        Optional devbench profile, for example write-path-gate.
USAGE
}

require_value() {
  local option="$1"
  local value="${2:-}"
  local description="$3"
  if [[ -z "$value" ]]; then
    echo "Error: $option requires $description" >&2
    usage >&2
    exit 2
  fi
}

normalize_positive_int() {
  local option="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "Error: $option must be a positive integer" >&2
    exit 2
  fi

  local normalized=$((10#$value))
  if (( normalized < 1 )); then
    echo "Error: $option must be a positive integer" >&2
    exit 2
  fi

  printf '%s' "$normalized"
}

is_executable_command() {
  local command_path="$1"
  command -v "$command_path" >/dev/null 2>&1 || [[ "$command_path" == */* && -x "$command_path" ]]
}

json_escape() {
  local value="$1"
  local nl=$'\n'
  local cr=$'\r'
  local tab=$'\t'
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$nl/\\n}"
  value="${value//$cr/\\r}"
  value="${value//$tab/\\t}"
  printf '%s' "$value"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mount-point)
      require_value "$1" "${2:-}" "a path value"
      mount_point="$2"
      shift 2
      ;;
    --runs)
      require_value "$1" "${2:-}" "a positive integer"
      runs="$2"
      shift 2
      ;;
    --studiofs)
      require_value "$1" "${2:-}" "a binary path"
      studiofs_path="$2"
      shift 2
      ;;
    --output-root)
      require_value "$1" "${2:-}" "a directory path"
      output_root="$2"
      shift 2
      ;;
    --block-size)
      require_value "$1" "${2:-}" "a size value"
      block_size="$2"
      shift 2
      ;;
    --big-file-size)
      require_value "$1" "${2:-}" "a size value"
      big_file_size="$2"
      shift 2
      ;;
    --small-file-size)
      require_value "$1" "${2:-}" "a size value"
      small_file_size="$2"
      shift 2
      ;;
    --small-file-count)
      require_value "$1" "${2:-}" "a positive integer"
      small_file_count="$2"
      shift 2
      ;;
    --threads)
      require_value "$1" "${2:-}" "a positive integer"
      threads="$2"
      shift 2
      ;;
    --profile)
      require_value "$1" "${2:-}" "a profile name"
      profile="$2"
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$mount_point" ]]; then
  echo "--mount-point is required" >&2
  usage >&2
  exit 2
fi
runs="$(normalize_positive_int "--runs" "$runs")"
small_file_count="$(normalize_positive_int "--small-file-count" "$small_file_count")"
threads="$(normalize_positive_int "--threads" "$threads")"

if ! is_executable_command "$studiofs_path"; then
  if [[ "$studiofs_path" == "studiofs" ]]; then
    if [[ -x "$repo_root/target/release/studiofs" ]]; then
      studiofs_path="$repo_root/target/release/studiofs"
    elif [[ -x "$repo_root/target/debug/studiofs" ]]; then
      studiofs_path="$repo_root/target/debug/studiofs"
    fi
  fi
fi

if ! is_executable_command "$studiofs_path"; then
  echo "Error: studiofs binary not found or not executable at: $studiofs_path" >&2
  exit 1
fi

if [[ -z "$output_root" ]]; then
  output_root="$repo_root/target/perf"
fi
mkdir -p "$output_root"

timestamp="$(date -u +%Y%m%d-%H%M%S)"
base_name="$artifact_platform-bench-validation-$timestamp"
validation_dir="$output_root/$base_name"
suffix=2
max_attempts=100
while ! mkdir "$validation_dir" 2>/dev/null; do
  if (( suffix > max_attempts )); then
    echo "Error: Failed to create validation directory under $output_root after $max_attempts attempts." >&2
    exit 1
  fi
  validation_dir="$output_root/$base_name-$suffix"
  suffix=$((suffix + 1))
done

git_commit="$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || true)"
studiofs_version="$("$studiofs_path" --version 2>/dev/null || true)"
os_name="$(uname -a)"
escaped_os="$(json_escape "$os_name")"
escaped_mount_point="$(json_escape "$mount_point")"
escaped_block_size="$(json_escape "$block_size")"
escaped_big_file_size="$(json_escape "$big_file_size")"
escaped_small_file_size="$(json_escape "$small_file_size")"
escaped_profile="$(json_escape "$profile")"
escaped_platform="$(json_escape "$platform")"
escaped_git_commit_json="null"
if [[ -n "$git_commit" ]]; then
  escaped_git_commit_json="\"$(json_escape "$git_commit")\""
fi
escaped_studiofs_version_json="null"
if [[ -n "$studiofs_version" ]]; then
  escaped_studiofs_version_json="\"$(json_escape "$studiofs_version")\""
fi

cat > "$validation_dir/environment.json" <<JSON
{
  "platform": "$escaped_platform",
  "os": "$escaped_os",
  "mount_point": "$escaped_mount_point",
  "git_commit": $escaped_git_commit_json,
  "studiofs_version": $escaped_studiofs_version_json,
  "benchmark_settings": {
    "block_size": "$escaped_block_size",
    "big_file_size": "$escaped_big_file_size",
    "small_file_size": "$escaped_small_file_size",
    "small_file_count": $small_file_count,
    "threads": $threads,
    "profile": "$escaped_profile",
    "run_count": $runs
  },
  "artifact_layout_note": "Do not overwrite previous benchmark artifacts"
}
JSON

for ((run=1; run<=runs; run++)); do
  run_name="$(printf 'run-%03d' "$run")"
  run_dir="$validation_dir/$run_name"
  mkdir "$run_dir"
  report_path="$run_dir/report.json"
  stdout_path="$run_dir/stdout.txt"
  command_path="$run_dir/command.txt"
  cmd=(
    "$studiofs_path" devbench "$mount_point"
    --block-size "$block_size"
    --big-file-size "$big_file_size"
    --small-file-size "$small_file_size"
    --small-file-count "$small_file_count"
    --threads "$threads"
    --json-output "$report_path"
  )
  if [[ -n "$profile" ]]; then
    cmd+=(--profile "$profile")
  fi
  printf '%q ' "${cmd[@]}" > "$command_path"
  printf '\n' >> "$command_path"
  echo "Starting benchmark run $run of $runs..."
  if ! "${cmd[@]}" > "$stdout_path" 2>&1; then
    echo "studiofs devbench failed on $run_name. See $stdout_path" >&2
    exit 1
  fi
  if [[ ! -f "$report_path" ]]; then
    echo "studiofs devbench did not write report.json for $run_name" >&2
    exit 1
  fi
done

summary_path="$validation_dir/summary.md"
{
  echo "# StudioFS $platform benchmark validation"
  echo
  echo "- Platform: $platform"
  echo "- Mount point: \`$mount_point\`"
  commit_text="N/A"
  if [[ -n "$git_commit" ]]; then
    commit_text="\`$git_commit\`"
  fi
  echo "- Git commit: $commit_text"
  echo "- Runs: $runs; recommended run count is 5"
  echo "- Benchmark settings: block size $block_size, big file size $big_file_size, small file size $small_file_size, small file count $small_file_count, threads $threads"
  echo
  echo "Compare medians across runs first, then inspect p95 and p99 latency for tail regressions."
  echo
  echo "## Artifacts"
  echo
  for ((run=1; run<=runs; run++)); do
    printf -- "- \`%s/run-%03d\`: command.txt, stdout.txt, report.json\n" "$validation_dir" "$run"
  done
} > "$summary_path"

echo "Benchmark validation artifacts: $validation_dir"
