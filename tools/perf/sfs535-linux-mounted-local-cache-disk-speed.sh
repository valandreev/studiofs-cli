#!/usr/bin/env bash
set -euo pipefail

mode="${1:-compact}"
case "$mode" in
  compact|sustained) ;;
  *)
    echo "usage: $0 [compact|sustained]" >&2
    exit 2
    ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
artifact_root="$repo_root/target/sfs-535-linux-mounted-local-cache-disk-speed"
run_dir="$artifact_root/$mode"
sfs532_root="$repo_root/target/sfs-532-linux-native-bridge-performance-gates/$mode"
sfs530_root="$repo_root/target/sfs-530-linux-native-async-write"

mkdir -p "$run_dir"

record_row() {
  local row="$1"
  local status="$2"
  local notes="$3"
  printf '%s\t%s\t%s\n' "$row" "$status" "$notes" >> "$run_dir/status.tsv"
  printf '| %s | %s | %s |\n' "$row" "$status" "$notes" >> "$run_dir/summary.md"
}

write_manifest() {
  {
    echo "issue=SFS-535"
    echo "name=Linux mounted local-cache disk speed"
    echo "mode=$mode"
    echo "artifact_root=$artifact_root"
    echo "run_dir=$run_dir"
    echo "sfs532_run_dir=$sfs532_root"
    echo "sfs530_telemetry_root=$sfs530_root"
    echo "target=warm trusted local-cache read at 60-75% of raw warm disk speed"
    echo "mounted_env=SFS535_RUN_MOUNTED=1"
    echo "required_fields=throughput,p95 latency,p99 latency,CPU,selected_reply_strategy,pread_local_cache_replies,splice_local_cache_replies,local_cache_reply_bytes,read_ahead_prefetches,read_ahead_suppressed,read_ahead_amplification_bytes,local_cache_willneed_hints,local_cache_willneed_errors,singleflight_waits,singleflight_wait_max_ms,fallback_reasons,unsafe_local_cache_fallbacks,bottleneck"
  } > "$run_dir/manifest.txt"
}

write_summary_header() {
  {
    echo "# SFS-535 $mode summary"
    echo
    echo "Artifact root: \`target/sfs-535-linux-mounted-local-cache-disk-speed/$mode\`"
    echo
    echo "Decision status: pending measured mounted run."
    echo
    echo "| Row | Status | Notes |"
    echo "|---|---|---|"
  } > "$run_dir/summary.md"
}

run_logged() {
  local row="$1"
  local log_name="$2"
  shift 2
  local log="$run_dir/$log_name"
  local status=0
  (
    cd "$repo_root"
    "$@"
  ) > "$log" 2>&1 || status=$?

  if [[ "$status" -eq 0 ]]; then
    record_row "$row" pass "log: ${log#$repo_root/}"
  else
    record_row "$row" investigate "command exited $status; see ${log#$repo_root/}"
  fi
}

copy_if_present() {
  local source="$1"
  local target="$2"
  if [[ -e "$source" ]]; then
    cp "$source" "$target"
    record_row "copied ${target##*/}" pass "source: ${source#$repo_root/}"
  else
    record_row "copied ${target##*/}" investigate "source unavailable: ${source#$repo_root/}"
  fi
}

copy_sfs530_telemetry_if_present() {
  local profile="$1"
  local file="$2"
  copy_if_present \
    "$sfs530_root/$profile/$file" \
    "$run_dir/$profile-$file"
}

rm -f "$run_dir/status.tsv"
write_manifest
write_summary_header

run_logged "local strategy artifact" "sfs-529-local-strategy.log" \
  cargo test -p studiofs-fuse --test native_local_cache_read_benchmark -- --ignored --nocapture

copy_if_present \
  "$repo_root/target/sfs-529-local-cache-read-benchmark/sfs-529-local-cache-read.json" \
  "$run_dir/sfs-529-local-cache-read.json"

if [[ "${SFS535_RUN_MOUNTED:-0}" == "1" ]]; then
  run_logged "mounted SFS-532 evidence wrapper" "sfs-532-mounted-wrapper.log" \
    env SFS532_RUN_MOUNTED=1 "$repo_root/tools/perf/sfs532-linux-native-gates.sh" "$mode"
  copy_if_present "$sfs532_root/summary.md" "$run_dir/sfs-532-summary.md"
  copy_if_present "$sfs532_root/status.tsv" "$run_dir/sfs-532-status.tsv"
  copy_if_present "$sfs532_root/manifest.txt" "$run_dir/sfs-532-manifest.txt"
  copy_sfs530_telemetry_if_present "native-sfs530-async-evidence" "write-path-observability-before.json"
  copy_sfs530_telemetry_if_present "native-sfs530-async-evidence" "write-path-observability-after.json"
  copy_sfs530_telemetry_if_present "native-sfs530-writeback-cache-evidence" "write-path-observability-before.json"
  copy_sfs530_telemetry_if_present "native-sfs530-writeback-cache-evidence" "write-path-observability-after.json"
else
  record_row "warm trusted local-cache read" investigate "set SFS535_RUN_MOUNTED=1 to collect mounted FUSE evidence through SFS-532"
  record_row "cold local-cache read" investigate "set SFS535_RUN_MOUNTED=1 to collect mounted FUSE evidence through SFS-532"
fi

{
  echo
  echo "## Closure Gate"
  echo
  echo "SFS-535 remains pending until mounted rows show warm trusted local-cache read throughput at 60-75% of raw warm disk speed, or record the measured bottleneck with p95 latency, p99 latency, CPU, selected reply strategy, local-cache reply counters, read_ahead_prefetches, read_ahead_suppressed, read_ahead_amplification_bytes, local_cache_willneed_hints, local_cache_willneed_errors, singleflight_waits, singleflight_wait_max_ms, fallback_reasons, and raw disk baseline."
} >> "$run_dir/summary.md"

echo "SFS-535 $mode summary: ${run_dir#$repo_root/}/summary.md"
