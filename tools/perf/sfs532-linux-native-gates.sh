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
artifact_root="$repo_root/target/sfs-532-linux-native-bridge-performance-gates"
run_dir="$artifact_root/$mode"
raw_dir="${SFS532_RAW_EXT4_DIR:-$artifact_root/raw-ext4-workdir}"

mkdir -p "$run_dir" "$raw_dir"

if [[ "$mode" == "compact" ]]; then
  export STUDIOFS_SFS509_FIO_SIZE="${STUDIOFS_SFS509_FIO_SIZE:-4m}"
  export STUDIOFS_SFS509_FIO_LARGE_SIZE="${STUDIOFS_SFS509_FIO_LARGE_SIZE:-16m}"
  export STUDIOFS_SFS509_FIO_RUNTIME_SECS="${STUDIOFS_SFS509_FIO_RUNTIME_SECS:-3}"
  export STUDIOFS_SFS509_FIO_TIMEOUT_SECS="${STUDIOFS_SFS509_FIO_TIMEOUT_SECS:-90}"
  export STUDIOFS_SFS509_METADATA_FILES="${STUDIOFS_SFS509_METADATA_FILES:-100}"
  export STUDIOFS_SFS530_FIO_SIZE="${STUDIOFS_SFS530_FIO_SIZE:-16m}"
  export STUDIOFS_SFS530_FIO_RUNTIME_SECS="${STUDIOFS_SFS530_FIO_RUNTIME_SECS:-3}"
  export STUDIOFS_SFS530_FIO_TIMEOUT_SECS="${STUDIOFS_SFS530_FIO_TIMEOUT_SECS:-90}"
else
  export STUDIOFS_SFS509_FIO_SIZE="${STUDIOFS_SFS509_FIO_SIZE:-128m}"
  export STUDIOFS_SFS509_FIO_LARGE_SIZE="${STUDIOFS_SFS509_FIO_LARGE_SIZE:-512m}"
  export STUDIOFS_SFS509_FIO_RUNTIME_SECS="${STUDIOFS_SFS509_FIO_RUNTIME_SECS:-30}"
  export STUDIOFS_SFS509_FIO_TIMEOUT_SECS="${STUDIOFS_SFS509_FIO_TIMEOUT_SECS:-300}"
  export STUDIOFS_SFS509_METADATA_FILES="${STUDIOFS_SFS509_METADATA_FILES:-1000}"
  export STUDIOFS_SFS530_FIO_SIZE="${STUDIOFS_SFS530_FIO_SIZE:-128m}"
  export STUDIOFS_SFS530_FIO_RUNTIME_SECS="${STUDIOFS_SFS530_FIO_RUNTIME_SECS:-30}"
  export STUDIOFS_SFS530_FIO_TIMEOUT_SECS="${STUDIOFS_SFS530_FIO_TIMEOUT_SECS:-300}"
fi

write_manifest() {
  {
    echo "issue=SFS-532"
    echo "name=LinuxNativeBridge performance gates"
    echo "mode=$mode"
    echo "artifact_root=$artifact_root"
    echo "run_dir=$run_dir"
    echo "raw_ext4_dir=$raw_dir"
    echo "raw_ext4_buffered=raw-ext4-buffered.json"
    echo "raw_ext4_direct=raw-ext4-direct.json"
    echo "fuser-era baseline=SFS-509/SFS-516 existing artifacts or rerun current fuser mounted matrix"
    echo "LinuxNativeBridge classic conservative profile=native-sfs530-async-evidence"
    echo "LinuxNativeBridge classic high-throughput profile=native-sfs530-writeback-cache-evidence"
    echo "LinuxNativeBridge io_uring profile=native_fusermount3_io_uring_session_init_and_unmount_smoke plus mounted matrix when supported"
    echo "JuiceFS reference=operator-provided optional row"
    echo "workloads=sequential read/write,warm repeated read,random read/write,mixed read/write,multi-job read/write,fsync-heavy,metadata create/stat/readdir,large-file local-cache read"
    echo "metrics=throughput,IOPS,latency,CPU,FUSE op amplification,queue wait,cache decisions,transport mode,io_uring fallback count,enabled kernel capabilities"
    echo "gate_states=pass/fail/investigate"
    echo "rollout decision=default NativeBridge|opt-in NativeBridge|io_uring profile decision|pause and redesign"
  } > "$run_dir/manifest.txt"
}

append_summary_header() {
  {
    echo "# SFS-532 $mode summary"
    echo
    echo "Artifact root: \`target/sfs-532-linux-native-bridge-performance-gates/$mode\`"
    echo
    echo "| Row | Status | Notes |"
    echo "|---|---|---|"
  } > "$run_dir/summary.md"
}

record_row() {
  local row="$1"
  local status="$2"
  local notes="$3"
  printf '%s\t%s\t%s\n' "$row" "$status" "$notes" >> "$run_dir/status.tsv"
  printf '| %s | %s | %s |\n' "$row" "$status" "$notes" >> "$run_dir/summary.md"
}

run_fio_raw_case() {
  local name="$1"
  shift
  if ! command -v fio >/dev/null 2>&1; then
    record_row "$name" investigate "fio unavailable; raw ext4 same-host ratio cannot be measured"
    return
  fi

  local output="$run_dir/$name.json"
  local log="$run_dir/$name.log"
  local status=0
  fio \
    "--name=sfs532-$name" \
    "--directory=$raw_dir" \
    "--filename=$name.bin" \
    "--ioengine=sync" \
    "--group_reporting" \
    "--output-format=json" \
    "--output=$output" \
    "$@" > "$log" 2>&1 || status=$?

  if [[ "$status" -eq 0 ]]; then
    record_row "$name" pass "fio JSON: ${output#$repo_root/}"
  else
    record_row "$name" investigate "fio exited $status; see ${log#$repo_root/}"
  fi
}

run_cargo_gate() {
  local row="$1"
  shift
  local log="$run_dir/${row//[^A-Za-z0-9_.-]/-}.log"
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

write_manifest
rm -f "$run_dir/status.tsv"
append_summary_header

run_fio_raw_case raw-ext4-buffered --rw=readwrite --rwmixread=70 --bs=1m --size="$STUDIOFS_SFS509_FIO_SIZE" --runtime="$STUDIOFS_SFS509_FIO_RUNTIME_SECS" --time_based
run_fio_raw_case raw-ext4-direct --rw=readwrite --rwmixread=70 --bs=1m --size="$STUDIOFS_SFS509_FIO_SIZE" --runtime="$STUDIOFS_SFS509_FIO_RUNTIME_SECS" --time_based --direct=1

run_cargo_gate "documentation gate" \
  cargo test -p studiofs --test architecture_vocabulary_test linux_native_bridge_performance_gates_cover_sfs532_rollout_decision -- --nocapture

run_cargo_gate "fuser-era baseline availability" \
  cargo test -p studiofs --test architecture_vocabulary_test linux_fuse_adapter_benchmark_report_covers_sfs516_decision_gate -- --nocapture

run_cargo_gate "LinuxNativeBridge classic conservative profile" \
  cargo test -p studiofs-fuse --test mount_sanity native_mounted_matrix_includes_sfs530_evidence_profiles -- --exact --nocapture

run_cargo_gate "LinuxNativeBridge classic high-throughput profile" \
  cargo test -p studiofs-fuse --test mount_sanity native_mounted_matrix_includes_sfs530_async_write_correctness_profiles -- --exact --nocapture

run_cargo_gate "LinuxNativeBridge io_uring profile" \
  cargo test -p studiofs-fuse --test mount_sanity native_io_uring_mount_smoke_is_available_as_sfs533_opt_in_gate -- --exact --nocapture

if [[ "${SFS532_RUN_MOUNTED:-0}" == "1" ]]; then
  export STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1
  run_cargo_gate "mounted native evidence matrix" \
    cargo test -p studiofs-fuse --test mount_sanity native_sfs530_mounted_evidence -- --ignored --nocapture --test-threads=1
  run_cargo_gate "mounted io_uring smoke" \
    cargo test -p studiofs-fuse --test mount_sanity native_fusermount3_io_uring_session_init_and_unmount_smoke -- --ignored --nocapture --test-threads=1
else
  record_row "mounted native evidence matrix" investigate "set SFS532_RUN_MOUNTED=1 to run /dev/fuse mounted workloads"
  record_row "mounted io_uring smoke" investigate "set SFS532_RUN_MOUNTED=1 on a host with FUSE-over-io_uring support"
fi

record_row "JuiceFS reference" investigate "copy operator-provided JuiceFS fio JSON into ${run_dir#$repo_root/} before final rollout decision"

{
  echo
  echo "## Decision"
  echo
  echo "rollout decision: pending"
  echo
  echo "- default NativeBridge: pending"
  echo "- opt-in NativeBridge: pending"
  echo "- io_uring profile decision: pending"
  echo "- pause and redesign: pending"
  echo
  echo "Final SFS-516 status comment must include the same-host ratio, pass/fail/investigate rows, and remaining bottlenecks."
} >> "$run_dir/summary.md"

echo "SFS-532 $mode summary: ${run_dir#$repo_root/}/summary.md"
