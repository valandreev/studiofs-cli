#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

volume=""
mount_root=""
cgroup_root=""
repetitions=""
repetitions_set=false
output_root="$repo_root/target/perf/linux-cache-matrix"
bench_bin="$repo_root/target/bench/studiofs-bench"
studiofs_bin="$repo_root/target/release/studiofs"
diagnostic=false
dry_run=false

usage() {
  cat <<'USAGE'
Usage: linux-cache-matrix.sh --volume NAME --mount-root PATH --cgroup-root PATH [options]

Build the balanced Linux cache/CPU evidence schedule. Promotion runs use five
fresh repetitions; diagnostic runs use three and are promotion-ineligible.

Required:
  --volume NAME          StudioFS volume name.
  --mount-root PATH      Parent for unique per-tuple mount directories.
  --cgroup-root PATH     Delegated cgroup v2 parent for unique per-tuple cgroups.

Options:
  --repetitions N        Repetitions. Defaults to 5, or 3 with --diagnostic.
  --output-root PATH     Matrix artifact root. Defaults to target/perf/linux-cache-matrix.
  --bench PATH           studiofs-bench binary. Defaults to target/bench/studiofs-bench.
  --studiofs PATH        StudioFS binary. Defaults to target/release/studiofs.
  --diagnostic           Mark an exact three-repetition real run promotion-ineligible.
  --dry-run              Write correctness commands, schedule, and tuple artifacts only.
  -h, --help             Show this help.

Dry-run accepts any positive repetition count and never mounts, creates a
cgroup, or executes either binary.
USAGE
}

die() {
  printf 'Error: %s\n' "$*" >&2
  exit 2
}

require_value() {
  local option="$1"
  local value="${2:-}"
  [[ -n "$value" ]] || die "$option requires a value"
}

positive_int() {
  local option="$1"
  local value="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || die "$option must be a positive integer"
  value="$((10#$value))"
  ((value > 0)) || die "$option must be a positive integer"
  printf '%s' "$value"
}

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  value="${value//$'\t'/\\t}"
  printf '%s' "$value"
}

quoted_command() {
  local rendered=""
  printf -v rendered '%q ' "$@"
  printf '%s' "${rendered% }"
}

while (($# > 0)); do
  case "$1" in
    --volume)
      require_value "$1" "${2:-}"
      volume="$2"
      shift 2
      ;;
    --mount-root)
      require_value "$1" "${2:-}"
      mount_root="$2"
      shift 2
      ;;
    --cgroup-root)
      require_value "$1" "${2:-}"
      cgroup_root="$2"
      shift 2
      ;;
    --repetitions)
      require_value "$1" "${2:-}"
      repetitions="$(positive_int "$1" "$2")"
      repetitions_set=true
      shift 2
      ;;
    --output-root)
      require_value "$1" "${2:-}"
      output_root="$2"
      shift 2
      ;;
    --bench)
      require_value "$1" "${2:-}"
      bench_bin="$2"
      shift 2
      ;;
    --studiofs)
      require_value "$1" "${2:-}"
      studiofs_bin="$2"
      shift 2
      ;;
    --diagnostic)
      diagnostic=true
      shift
      ;;
    --dry-run)
      dry_run=true
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[[ -n "$volume" ]] || die "--volume is required"
[[ -n "$mount_root" ]] || die "--mount-root is required"
[[ -n "$cgroup_root" ]] || die "--cgroup-root is required"

if [[ "$repetitions_set" == false ]]; then
  if [[ "$diagnostic" == true ]]; then
    repetitions=3
  else
    repetitions=5
  fi
fi

if [[ "$dry_run" == false ]]; then
  if [[ "$diagnostic" == true && "$repetitions" -ne 3 ]]; then
    die "diagnostic evidence requires exactly 3 repetitions"
  fi
  if [[ "$diagnostic" == false && "$repetitions" -ne 5 ]]; then
    die "promotion evidence requires exactly 5 repetitions"
  fi
fi

mount_root="$(realpath -m -- "$mount_root")"
cgroup_root="$(realpath -m -- "$cgroup_root")"
output_root="$(realpath -m -- "$output_root")"
bench_bin="$(realpath -m -- "$bench_bin")"
studiofs_bin="$(realpath -m -- "$studiofs_bin")"

if [[ -e "$output_root/correctness-manifest.json" || -e "$output_root/schedule.json" ]]; then
  die "matrix artifact root already contains evidence: $output_root"
fi
mkdir -p "$output_root"

created_utc=""
printf -v created_utc '%(%Y-%m-%dT%H:%M:%SZ)T' -1

declare -a correctness_names=(
  mounted_smoke_mounts_and_unmounts_cleanly
  mounted_regular_file_io_is_visible_across_reopen
  mounted_namespace_mutations_are_visible_through_readdir
  mounted_direct_rollback_preserves_reopen_visibility
  mounted_same_file_parallel_direct_writes_preserve_disjoint_regions
  mounted_forced_unmount_drains_admitted_and_waiting_raw_requests
  mounted_cross_file_fsync_and_close_do_not_share_async_watermark
  mounted_kernel_cache_coherence_matrix
  mounted_kernel_writeback_flush_and_release_are_locally_durable
  mounted_kernel_remote_listener_invalidation
)
declare -a correctness_modes=(
  direct-serial direct-serial direct-serial direct-serial
  direct-async direct-async direct-async
  kernel kernel kernel
)

correctness_manifest="$output_root/correctness-manifest.json"
bench_repo="/home/val/dev/studiofs-bench"
git_revision=""
git_dirty=""
studiofs_sha256=""
bench_git_revision=""
bench_git_dirty=""
bench_sha256=""
correctness_valid=true
declare -a test_started=()
declare -a test_ended=()
declare -a test_status=()
declare -a test_log_sha256=()
declare -a test_executed=()
declare -a test_skipped=()
declare -a correctness_command=()

utc_now() {
  local now=""
  printf -v now '%(%Y-%m-%dT%H:%M:%SZ)T' -1
  printf '%s' "$now"
}

file_sha256() {
  local path="$1"
  local output=""
  local digest=""
  if ! output="$(sha256sum "$path")"; then
    die "failed to hash $path"
  fi
  digest="${output%%[[:space:]]*}"
  [[ "$digest" =~ ^[[:xdigit:]]{64}$ ]] || die "invalid SHA-256 output for $path"
  printf '%s' "${digest,,}"
}

build_correctness_command() {
  local name="$1"
  local mode="$2"
  local full_name="low_level::tests::$name"
  local cache_policy=direct
  local async_dio=0
  if [[ "$mode" == direct-async ]]; then
    async_dio=1
  elif [[ "$mode" == kernel ]]; then
    cache_policy=kernel
  fi

  correctness_command=(
    env
    CARGO_TERM_COLOR=never
    LC_ALL=C
    "STUDIOFS_LINUX_FUSE_CACHE_POLICY=$cache_policy"
    "STUDIOFS_FUSE_ASYNC_DIO=$async_dio"
  )
  if [[ "$name" == mounted_kernel_remote_listener_invalidation ]]; then
    correctness_command+=(STUDIOFS_REQUIRE_REMOTE_INVALIDATION_TEST=1)
  fi
  correctness_command+=(
    cargo test -p studiofs-linux-fuse "$full_name" --
    --ignored --exact --nocapture --test-threads=1
  )
}

write_correctness_manifest() {
  local manifest_dry_run="$1"
  local manifest_tmp="$correctness_manifest.tmp"
  local promotion_eligible=true
  [[ "$diagnostic" == false ]] || promotion_eligible=false
  [[ "$manifest_dry_run" == false ]] || promotion_eligible=false

  {
    printf '{\n'
    printf '  "schema_version": 1,\n'
    printf '  "kind": "studiofs-linux-cache-correctness",\n'
    printf '  "created_utc": "%s",\n' "$(json_escape "$created_utc")"
    printf '  "dry_run": %s,\n' "$manifest_dry_run"
    printf '  "promotion_eligible": %s,\n' "$promotion_eligible"
    if [[ "$manifest_dry_run" == true ]]; then
      printf '  "valid": false,\n'
    else
      printf '  "valid": %s,\n' "$correctness_valid"
    fi
    if [[ "$manifest_dry_run" == true ]]; then
      printf '  "git_revision": null,\n'
      printf '  "git_dirty": null,\n'
      printf '  "studiofs_path": "%s",\n' "$(json_escape "$studiofs_bin")"
      printf '  "studiofs_sha256": null,\n'
      printf '  "bench_git_revision": null,\n'
      printf '  "bench_git_dirty": null,\n'
      printf '  "bench_path": "%s",\n' "$(json_escape "$bench_bin")"
      printf '  "bench_sha256": null,\n'
    else
      printf '  "git_revision": "%s",\n' "$(json_escape "$git_revision")"
      printf '  "git_dirty": %s,\n' "$git_dirty"
      printf '  "studiofs_path": "%s",\n' "$(json_escape "$studiofs_bin")"
      printf '  "studiofs_sha256": "%s",\n' "$studiofs_sha256"
      printf '  "bench_git_revision": "%s",\n' "$(json_escape "$bench_git_revision")"
      printf '  "bench_git_dirty": %s,\n' "$bench_git_dirty"
      printf '  "bench_path": "%s",\n' "$(json_escape "$bench_bin")"
      printf '  "bench_sha256": "%s",\n' "$bench_sha256"
    fi
    printf '  "tests": [\n'

    for index in "${!correctness_names[@]}"; do
      name="${correctness_names[$index]}"
      mode="${correctness_modes[$index]}"
      full_name="low_level::tests::$name"
      cache_policy=direct
      async_dio=0
      if [[ "$mode" == direct-async ]]; then
        async_dio=1
      elif [[ "$mode" == kernel ]]; then
        cache_policy=kernel
      fi
      build_correctness_command "$name" "$mode"
      command_text="$(quoted_command "${correctness_command[@]}")"
      log_path="$output_root/correctness/$name.log"

      ((index > 0)) && printf ',\n'
      printf '    {\n'
      printf '      "name": "%s",\n' "$(json_escape "$full_name")"
      printf '      "mode": "%s",\n' "$(json_escape "$mode")"
      printf '      "environment": {'
      printf '"CARGO_TERM_COLOR":"never","LC_ALL":"C",'
      printf '"STUDIOFS_LINUX_FUSE_CACHE_POLICY":"%s",' "$cache_policy"
      printf '"STUDIOFS_FUSE_ASYNC_DIO":"%s"' "$async_dio"
      if [[ "$name" == mounted_kernel_remote_listener_invalidation ]]; then
        printf ',"STUDIOFS_REQUIRE_REMOTE_INVALIDATION_TEST":"1"'
      fi
      printf '},\n'
      printf '      "command": "%s",\n' "$(json_escape "$command_text")"
      if [[ "$manifest_dry_run" == true ]]; then
        printf '      "started_at": null,\n'
        printf '      "ended_at": null,\n'
        printf '      "exit_status": null,\n'
        printf '      "log_path": "%s",\n' "$(json_escape "$log_path")"
        printf '      "log_sha256": null,\n'
        printf '      "executed": false,\n'
        printf '      "skipped": false,\n'
      else
        printf '      "started_at": "%s",\n' "$(json_escape "${test_started[$index]}")"
        printf '      "ended_at": "%s",\n' "$(json_escape "${test_ended[$index]}")"
        printf '      "exit_status": %d,\n' "${test_status[$index]}"
        printf '      "log_path": "%s",\n' "$(json_escape "$log_path")"
        printf '      "log_sha256": "%s",\n' "${test_log_sha256[$index]}"
        printf '      "executed": %s,\n' "${test_executed[$index]}"
        printf '      "skipped": %s,\n' "${test_skipped[$index]}"
      fi
      printf '      "applicable": true\n'
      printf '    }'
    done

    printf '\n  ]\n'
    printf '}\n'
  } >"$manifest_tmp"
  mv "$manifest_tmp" "$correctness_manifest"
}

run_correctness_preflight() {
  local built_bench_bin="$bench_repo/target/release/studiofs-bench"
  local bundled_bench_bin="$repo_root/target/bench/studiofs-bench"
  local built_bench_sha256=""
  for command_name in git sha256sum cargo; do
    command -v "$command_name" >/dev/null || die "required command is unavailable: $command_name"
  done
  [[ -x "$studiofs_bin" ]] || die "StudioFS binary is not executable: $studiofs_bin"

  if ! git_revision="$(git -C "$repo_root" rev-parse --verify HEAD)"; then
    die "cannot read StudioFS source revision"
  fi
  local source_status=""
  if ! source_status="$(git -C "$repo_root" status --porcelain --untracked-files=all)"; then
    die "cannot inspect StudioFS source state"
  fi
  [[ -z "$source_status" ]] || die "StudioFS source tree is dirty"
  git_dirty=false

  if ! bench_git_revision="$(git -C "$bench_repo" rev-parse --verify HEAD)"; then
    die "cannot read studiofs-bench source revision: $bench_repo"
  fi
  local bench_status=""
  if ! bench_status="$(git -C "$bench_repo" status --porcelain --untracked-files=all)"; then
    die "cannot inspect studiofs-bench source state: $bench_repo"
  fi
  [[ -z "$bench_status" ]] || die "studiofs-bench source tree is dirty: $bench_repo"
  bench_git_dirty=false

  if ! (cd "$bench_repo" && cargo build --release --locked); then
    die "failed to build studiofs-bench from clean source"
  fi
  [[ -x "$built_bench_bin" ]] || die \
    "studiofs-bench build did not produce an executable: $built_bench_bin"
  mkdir -p "$repo_root/target/bench"
  cp "$built_bench_bin" "$bundled_bench_bin"
  cp "$bench_repo/README.md" "$repo_root/target/bench/README.md"
  [[ -x "$bench_bin" ]] || die "studiofs-bench binary is not executable: $bench_bin"

  studiofs_sha256="$(file_sha256 "$studiofs_bin")"
  built_bench_sha256="$(file_sha256 "$built_bench_bin")"
  bench_sha256="$(file_sha256 "$bench_bin")"
  [[ "$bench_sha256" == "$built_bench_sha256" ]] || die \
    "--bench binary does not match the clean studiofs-bench source build"
}

run_correctness_tests() {
  mkdir -p "$output_root/correctness"
  for index in "${!correctness_names[@]}"; do
    local name="${correctness_names[$index]}"
    local mode="${correctness_modes[$index]}"
    local full_name="low_level::tests::$name"
    local log_path="$output_root/correctness/$name.log"
    local status=0
    local ok_line_count=0
    local test_case_line_count=0
    local summary_matches=false
    local skipped=false
    local executed=false
    local line=""

    build_correctness_command "$name" "$mode"
    test_started[$index]="$(utc_now)"
    if "${correctness_command[@]}" >"$log_path" 2>&1; then
      status=0
    else
      status=$?
    fi
    test_ended[$index]="$(utc_now)"

    while IFS= read -r line || [[ -n "$line" ]]; do
      line="${line%$'\r'}"
      if [[ "$line" == "test $full_name ... ok" ]]; then
        ok_line_count=$((ok_line_count + 1))
      fi
      if [[ "$line" == test\ *" ... ok" ||
        "$line" == test\ *" ... FAILED" ||
        "$line" == test\ *" ... ignored"* ]]; then
        test_case_line_count=$((test_case_line_count + 1))
      fi
      if [[ "$line" == "test result: ok. 1 passed; 0 failed; 0 ignored;"* ]]; then
        summary_matches=true
      fi
      if [[ " $line " == *" SKIP "* ]]; then
        skipped=true
      fi
      if [[ "$line" == "test $full_name ... ignored"* ]]; then
        skipped=true
      fi
    done <"$log_path"

    if [[ "$ok_line_count" -eq 1 && "$test_case_line_count" -eq 1 &&
      "$summary_matches" == true ]]; then
      executed=true
    fi

    test_status[$index]="$status"
    test_log_sha256[$index]="$(file_sha256 "$log_path")"
    test_executed[$index]="$executed"
    test_skipped[$index]="$skipped"
    if [[ "$status" -ne 0 || "$executed" != true || "$skipped" == true ]]; then
      correctness_valid=false
    fi
  done
}

if [[ "$dry_run" == true ]]; then
  write_correctness_manifest true
else
  run_correctness_preflight
  run_correctness_tests
  write_correctness_manifest false
  if [[ "$correctness_valid" != true ]]; then
    printf 'Error: correctness evidence failed; inspect %s\n' "$correctness_manifest" >&2
    exit 1
  fi
fi

declare -a modes=(direct-serial direct-async kernel)
declare -a workloads=(
  large-single
  fixed-1m
  small-128k
  warm-reopen
  same-file-parallel
  cross-file-barriers
)

schedule_file="$output_root/schedule.json"
expected_tuple_count=$((repetitions * ${#modes[@]} * ${#workloads[@]}))
tuple_count=0
first_tuple=true
promotion_eligible=true
[[ "$diagnostic" == false && "$dry_run" == false ]] || promotion_eligible=false
schedule_status=planned
[[ "$dry_run" == true ]] || schedule_status=pending
declare -a tuple_modes=()
declare -a tuple_workloads=()
declare -a tuple_slugs=()
declare -a tuple_mount_roots=()
declare -a tuple_mount_points=()
declare -a tuple_cache_dirs=()
declare -a tuple_cgroups=()
declare -a tuple_artifact_dirs=()
declare -A seen_mount_points=()
declare -A seen_cache_dirs=()
declare -A seen_cgroups=()
declare -A seen_artifact_dirs=()

exec 3>"$schedule_file"
printf '{\n' >&3
printf '  "schema_version": 1,\n' >&3
printf '  "kind": "studiofs-linux-cache-matrix-schedule",\n' >&3
printf '  "created_utc": "%s",\n' "$(json_escape "$created_utc")" >&3
printf '  "volume": "%s",\n' "$(json_escape "$volume")" >&3
printf '  "repetitions": %d,\n' "$repetitions" >&3
printf '  "diagnostic": %s,\n' "$diagnostic" >&3
printf '  "dry_run": %s,\n' "$dry_run" >&3
printf '  "promotion_eligible": %s,\n' "$promotion_eligible" >&3
printf '  "status": "%s",\n' "$schedule_status" >&3
printf '  "expected_tuple_count": %d,\n' "$expected_tuple_count" >&3
printf '  "mode_rotation": "three-row Latin square",\n' >&3
printf '  "workload_rotation": "cyclic by repetition",\n' >&3
printf '  "tuples": [\n' >&3

for ((repetition = 1; repetition <= repetitions; repetition++)); do
  mode_shift=$(((repetition - 1) % ${#modes[@]}))
  workload_shift=$(((repetition - 1) % ${#workloads[@]}))
  printf -v rep_name 'rep-%03d' "$repetition"

  for ((workload_position = 0; workload_position < ${#workloads[@]}; workload_position++)); do
    workload_index=$(((workload_position + workload_shift) % ${#workloads[@]}))
    workload="${workloads[$workload_index]}"

    for ((mode_position = 0; mode_position < ${#modes[@]}; mode_position++)); do
      mode_index=$(((mode_position + mode_shift) % ${#modes[@]}))
      mode="${modes[$mode_index]}"
      tuple_slug="$rep_name-$workload-$mode"
      mount_parent="$mount_root/$tuple_slug"
      mount_point="$mount_parent/$volume"
      cache_dir="$output_root/.work/cache/$tuple_slug"
      cgroup="$cgroup_root/studiofs-linux-cache-$tuple_slug"
      artifact_dir="$output_root/$mode/$workload/$rep_name"

      for path_kind in mount_point cache_dir cgroup artifact_dir; do
        case "$path_kind" in
          mount_point) path_value="$mount_point"; seen_name=seen_mount_points ;;
          cache_dir) path_value="$cache_dir"; seen_name=seen_cache_dirs ;;
          cgroup) path_value="$cgroup"; seen_name=seen_cgroups ;;
          artifact_dir) path_value="$artifact_dir"; seen_name=seen_artifact_dirs ;;
        esac
        declare -n seen_paths="$seen_name"
        [[ -z "${seen_paths[$path_value]+present}" ]] || die \
          "internal schedule error: reused $path_kind $path_value"
        seen_paths[$path_value]=1
        unset -n seen_paths
      done

      tuple_modes+=("$mode")
      tuple_workloads+=("$workload")
      tuple_slugs+=("$tuple_slug")
      tuple_mount_roots+=("$mount_parent")
      tuple_mount_points+=("$mount_point")
      tuple_cache_dirs+=("$cache_dir")
      tuple_cgroups+=("$cgroup")
      tuple_artifact_dirs+=("$artifact_dir")

      cache_policy=direct
      async_dio=0
      if [[ "$mode" == direct-async ]]; then
        async_dio=1
      elif [[ "$mode" == kernel ]]; then
        cache_policy=kernel
      fi

      declare -a mount_command=(
        env
        "STUDIOFS_LINUX_FUSE_CACHE_POLICY=$cache_policy"
        "STUDIOFS_FUSE_ASYNC_DIO=$async_dio"
        "$studiofs_bin" mount "$volume" "$mount_parent"
        --cache-dir "$cache_dir" --foreground
      )
      mount_command_text="$(quoted_command "${mount_command[@]}")"

      declare -a collector_command=(
        bash "$script_dir/linux-cache-gate.sh"
        --mode "$mode"
        --workload "$workload"
        --mount-point "$mount_point"
        --mount-pid '<mount-pid>'
        --runs 1
        --cgroup "$cgroup"
        --bench "$bench_bin"
        --studiofs "$studiofs_bin"
        --correctness-manifest "$correctness_manifest"
        --output-root "$artifact_dir"
      )
      if [[ "$diagnostic" == true ]]; then
        collector_command+=(--diagnostic)
      fi
      collector_command_text="$(quoted_command "${collector_command[@]}")"

      if [[ "$dry_run" == true ]]; then
        mkdir -p "$artifact_dir"
        {
          printf '{\n'
          printf '  "schema_version": 1,\n'
          printf '  "mode": "%s",\n' "$(json_escape "$mode")"
          printf '  "workload": "%s",\n' "$(json_escape "$workload")"
          printf '  "repetition": %d,\n' "$repetition"
          printf '  "runs": 1,\n'
          printf '  "dry_run": true,\n'
          printf '  "promotion_eligible": false,\n'
          printf '  "mount_point": "%s",\n' "$(json_escape "$mount_point")"
          printf '  "cache_dir": "%s",\n' "$(json_escape "$cache_dir")"
          printf '  "cgroup": "%s",\n' "$(json_escape "$cgroup")"
          printf '  "correctness_manifest_path": "%s"\n' "$(json_escape "$correctness_manifest")"
          printf '}\n'
        } >"$artifact_dir/manifest.json"
        printf '%s\n' "$collector_command_text" >"$artifact_dir/command.txt"
      fi

      if [[ "$first_tuple" == true ]]; then
        first_tuple=false
      else
        printf ',\n' >&3
      fi
      printf '    {\n' >&3
      printf '      "schedule_index": %d,\n' "$tuple_count" >&3
      printf '      "repetition": %d,\n' "$repetition" >&3
      printf '      "workload": "%s",\n' "$(json_escape "$workload")" >&3
      printf '      "mode": "%s",\n' "$(json_escape "$mode")" >&3
      printf '      "mount_root": "%s",\n' "$(json_escape "$mount_parent")" >&3
      printf '      "mount_point": "%s",\n' "$(json_escape "$mount_point")" >&3
      printf '      "cache_dir": "%s",\n' "$(json_escape "$cache_dir")" >&3
      printf '      "cgroup": "%s",\n' "$(json_escape "$cgroup")" >&3
      printf '      "artifact_dir": "%s",\n' "$(json_escape "$artifact_dir")" >&3
      printf '      "mount_command": "%s",\n' "$(json_escape "$mount_command_text")" >&3
      printf '      "collector_command": "%s",\n' "$(json_escape "$collector_command_text")" >&3
      printf '      "status": "%s",\n' "$schedule_status" >&3
      printf '      "collector_exit_status": null,\n' >&3
      printf '      "cleanup_error": null,\n' >&3
      printf '      "cleanup_actions": [\n' >&3
      printf '        "request unmount and bound completion to 30 seconds",\n' >&3
      printf '        "bound mount process exit to 30 seconds",\n' >&3
      printf '        "send TERM then KILL only if this test mount remains live",\n' >&3
      printf '        "remove workload, cache, and mount directories after artifacts are durable",\n' >&3
      printf '        "remove cgroup only after cgroup.procs is empty"\n' >&3
      printf '      ]\n' >&3
      printf '    }' >&3

      tuple_count=$((tuple_count + 1))
      unset mount_command collector_command
    done
  done
done

printf '\n  ]\n' >&3
printf '}\n' >&3
exec 3>&-

[[ "$tuple_count" -eq "$expected_tuple_count" ]] || die \
  "internal schedule error: expected $expected_tuple_count tuples, wrote $tuple_count"

if [[ "$dry_run" == true ]]; then
  printf '%s\n' "$output_root"
  exit 0
fi

for command_name in timeout findmnt fusermount3 sync python3; do
  command -v "$command_name" >/dev/null || die "required command is unavailable: $command_name"
done
[[ -d "$cgroup_root" && -w "$cgroup_root" ]] || die \
  "delegated cgroup root is not a writable directory: $cgroup_root"
[[ "$mount_root" != / ]] || die "--mount-root must not be the filesystem root"
mkdir -p "$mount_root" "$output_root/.work/cache" "$output_root/.work/tuples"

active_index=-1
active_mount_pid=""
active_mount_start_time=""
active_collector_pid=""
active_collector_start_time=""
active_mount_root=""
active_mount_point=""
active_cache_dir=""
active_cgroup=""
active_artifact_dir=""
active_work_dir=""
active_interrupted_by=""
cleanup_in_progress=false
deferred_signal=""
collector_exit_status=-1
collector_artifacts_complete=false
unmount_exit_status=-1
unmount_timed_out=false
fallback_unmount_exit_status=-1
fallback_lazy_exit_status=-1
mount_exit_status=-1
mount_exit_timed_out=false
mount_dir_removed=false
cache_dir_removed=false
cgroup_removed=false
collector_cleanup_checked=false
collector_cleanup_present=false
mount_dir_created=false
cache_dir_created=false
cgroup_created=false
work_dir_created=false
declare -a cleanup_errors=()

add_cleanup_error() {
  cleanup_errors+=("$1")
}

cleanup_error_text() {
  local rendered=""
  local error=""
  for error in "${cleanup_errors[@]}"; do
    if [[ -n "$rendered" ]]; then
      rendered+="; "
    fi
    rendered+="$error"
  done
  printf '%s' "$rendered"
}

pid_is_live() {
  local pid="$1"
  local stat_line=""
  local rest=""
  kill -0 "$pid" 2>/dev/null || return 1
  if [[ -r "/proc/$pid/stat" ]]; then
    IFS= read -r stat_line <"/proc/$pid/stat"
    rest="${stat_line##*) }"
    [[ "${rest:0:1}" != Z ]] || return 1
  fi
  return 0
}

pid_start_time() {
  local pid="$1"
  local stat_line=""
  local rest=""
  local -a fields=()
  [[ -r "/proc/$pid/stat" ]] || return 1
  IFS= read -r stat_line <"/proc/$pid/stat"
  rest="${stat_line##*) }"
  read -r -a fields <<<"$rest"
  [[ "${#fields[@]}" -gt 19 ]] || return 1
  printf '%s' "${fields[19]}"
}

owned_pid_is_live() {
  local pid="$1"
  local expected_start_time="$2"
  local current_start_time=""
  pid_is_live "$pid" || return 1
  [[ -n "$expected_start_time" ]] || return 0
  current_start_time="$(pid_start_time "$pid")" || return 1
  [[ "$current_start_time" == "$expected_start_time" ]]
}

wait_for_pid_exit() {
  local pid="$1"
  local timeout_seconds="$2"
  local expected_start_time="${3:-}"
  local deadline=$((SECONDS + timeout_seconds))
  while owned_pid_is_live "$pid" "$expected_start_time"; do
    ((SECONDS < deadline)) || return 1
    sleep 0.1
  done
  return 0
}

run_bounded_command() {
  local stdout_path="$1"
  local stderr_path="$2"
  shift 2
  bounded_status=0
  bounded_timed_out=false
  if timeout --signal=TERM --kill-after=5s 30s "$@" >"$stdout_path" 2>"$stderr_path"; then
    bounded_status=0
  else
    bounded_status=$?
  fi
  if [[ "$bounded_status" -eq 124 || "$bounded_status" -eq 137 ]]; then
    bounded_timed_out=true
  fi
}

mount_is_active() {
  findmnt -rn -M "$active_mount_point" -o TARGET >/dev/null 2>&1
}

verify_collector_artifacts() {
  collector_artifacts_complete=true
  local required_file=""
  local last_output_line=""
  local line=""
  local phase=""
  local proc_file=""
  local -a required_files=(
    environment.json
    manifest.json
    command.txt
    runtime-foreground-start.json
    runtime-foreground-end.json
    runtime-drain-end.json
    runtime-cleanup-end.json
    cgroup-foreground-start.json
    cgroup-foreground-end.json
    cgroup-drain-end.json
    cgroup-memory-samples.jsonl
    process-memory-samples.jsonl
    pidstat.txt
    client-report.json
    cleanup.json
    derived.json
  )
  if [[ "$active_index" -ge 0 && "${tuple_workloads[$active_index]}" == "warm-reopen" ]]; then
    required_files+=(runtime-warm-first-end.json)
  fi
  if [[ "$collector_exit_status" -ne 0 ]]; then
    add_cleanup_error "collector exited with status $collector_exit_status"
    collector_artifacts_complete=false
  fi
  for required_file in "${required_files[@]}"; do
    if [[ ! -s "$active_artifact_dir/$required_file" ]]; then
      add_cleanup_error "collector did not write $required_file in exact repetition directory"
      collector_artifacts_complete=false
    fi
  done
  for phase in foreground-start foreground-end drain-end; do
    if [[ ! -d "$active_artifact_dir/proc-$phase" ]]; then
      add_cleanup_error "collector did not write proc-$phase directory"
      collector_artifacts_complete=false
      continue
    fi
    for proc_file in stat status smaps_rollup io; do
      if [[ ! -s "$active_artifact_dir/proc-$phase/$proc_file.txt" ]]; then
        add_cleanup_error "collector did not write proc-$phase/$proc_file.txt"
        collector_artifacts_complete=false
      fi
    done
  done
  if [[ -f "$active_work_dir/collector.stdout.txt" ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
      [[ -z "$line" ]] || last_output_line="$line"
    done <"$active_work_dir/collector.stdout.txt"
  fi
  if [[ "$last_output_line" != "$active_artifact_dir" ]]; then
    add_cleanup_error "collector did not report the exact repetition directory"
    collector_artifacts_complete=false
  fi
}

inspect_collector_cleanup() {
  local cleanup_path="$active_artifact_dir/cleanup.json"
  local output=""
  local status=0
  [[ "$collector_cleanup_checked" == false ]] || return 0
  collector_cleanup_checked=true
  [[ -f "$cleanup_path" ]] || return 0
  if output="$(python3 - "$cleanup_path" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
try:
    cleanup = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as error:
    print(f"collector cleanup JSON is invalid: {error}")
    raise SystemExit(2)
error = cleanup.get("cleanup_error")
if error not in (None, ""):
    print(f"collector cleanup failed: {error}")
    raise SystemExit(3)
if cleanup.get("cleanup_drain_idle") is not True:
    print("collector cleanup drain did not reach idle")
    raise SystemExit(4)
PY
)"; then
    collector_cleanup_present=true
    return 0
  else
    status=$?
  fi
  collector_cleanup_present=false
  add_cleanup_error "${output:-collector cleanup validation failed with status $status}"
  return 0
}

write_matrix_cleanup_payload() {
  local destination="$1"
  local error_text="$(cleanup_error_text)"
  {
    printf '{\n'
    printf '  "schema_version": 1,\n'
    printf '  "matrix_cleanup": true,\n'
    if [[ -n "$error_text" ]]; then
      printf '  "cleanup_error": "%s",\n' "$(json_escape "$error_text")"
    else
      printf '  "cleanup_error": null,\n'
    fi
    printf '  "collector_exit_status": '
    [[ "$collector_exit_status" -ge 0 ]] && printf '%d,\n' "$collector_exit_status" || printf 'null,\n'
    printf '  "collector_artifacts_complete": %s,\n' "$collector_artifacts_complete"
    if [[ "$collector_cleanup_present" == false ]]; then
      printf '  "cleanup_drain_idle": false,\n'
    fi
    printf '  "unmount_timed_out": %s,\n' "$unmount_timed_out"
    printf '  "unmount_exit_status": '
    [[ "$unmount_exit_status" -ge 0 ]] && printf '%d,\n' "$unmount_exit_status" || printf 'null,\n'
    printf '  "fallback_unmount_exit_status": '
    [[ "$fallback_unmount_exit_status" -ge 0 ]] && printf '%d,\n' "$fallback_unmount_exit_status" || printf 'null,\n'
    printf '  "fallback_lazy_exit_status": '
    [[ "$fallback_lazy_exit_status" -ge 0 ]] && printf '%d,\n' "$fallback_lazy_exit_status" || printf 'null,\n'
    printf '  "mount_exit_timed_out": %s,\n' "$mount_exit_timed_out"
    printf '  "mount_exit_status": '
    [[ "$mount_exit_status" -ge 0 ]] && printf '%d,\n' "$mount_exit_status" || printf 'null,\n'
    printf '  "mount_dir_removed": %s,\n' "$mount_dir_removed"
    printf '  "cache_dir_removed": %s,\n' "$cache_dir_removed"
    printf '  "cgroup_removed": %s,\n' "$cgroup_removed"
    if [[ -n "$active_interrupted_by" ]]; then
      printf '  "interrupted_by": "%s"\n' "$(json_escape "$active_interrupted_by")"
    else
      printf '  "interrupted_by": null\n'
    fi
    printf '}\n'
  } >"$destination"
}

merge_cleanup_json() {
  local matrix_path="$active_work_dir/matrix-cleanup.json"
  local cleanup_path="$active_artifact_dir/cleanup.json"
  if ! write_matrix_cleanup_payload "$matrix_path"; then
    return 1
  fi
  if [[ ! -f "$cleanup_path" ]]; then
    if mv "$matrix_path" "$cleanup_path"; then
      return 0
    fi
    return 1
  fi
  local merge_status=0
  if python3 - "$cleanup_path" "$matrix_path" <<'PY'
import json
import pathlib
import sys

cleanup_path = pathlib.Path(sys.argv[1])
matrix_path = pathlib.Path(sys.argv[2])
base = json.loads(cleanup_path.read_text(encoding="utf-8"))
matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
errors = []
for value in (base.get("cleanup_error"), matrix.get("cleanup_error")):
    if value not in (None, "") and value not in errors:
        errors.append(value)
base.update(matrix)
base["cleanup_error"] = "; ".join(errors) if errors else None
temporary = cleanup_path.with_suffix(".json.tmp")
temporary.write_text(json.dumps(base, indent=2, sort_keys=True) + "\n", encoding="utf-8")
temporary.replace(cleanup_path)
PY
  then
    merge_status=0
  else
    merge_status=$?
  fi
  if [[ "$merge_status" -ne 0 ]]; then
    return "$merge_status"
  fi
  if ! rm -f "$matrix_path"; then
    return 1
  fi
  return 0
}

move_tuple_logs() {
  local file=""
  local -a files=()
  shopt -s nullglob
  files=("$active_work_dir"/*.txt)
  shopt -u nullglob
  for file in "${files[@]}"; do
    if ! mv "$file" "$active_artifact_dir/"; then
      add_cleanup_error "failed to move tuple log ${file##*/} into artifacts"
    fi
  done
}

safe_remove_tree() {
  local path="$1"
  local prefix="$2"
  local label="$3"
  if [[ "$path" != "$prefix/"* || "$path" == "$prefix" ]]; then
    add_cleanup_error "refused to remove unsafe $label path $path"
    return 1
  fi
  if [[ -e "$path" ]] && ! rm -rf -- "$path"; then
    add_cleanup_error "failed to remove $label path $path"
    return 1
  fi
  return 0
}

reap_mount_process() {
  local pid="$active_mount_pid"
  local status=0
  [[ -n "$pid" ]] || return 0
  if ! wait_for_pid_exit "$pid" 30 "$active_mount_start_time"; then
    mount_exit_timed_out=true
    add_cleanup_error "mount process did not exit within 30 seconds"
    if owned_pid_is_live "$pid" "$active_mount_start_time"; then
      if ! kill -TERM "$pid"; then
        add_cleanup_error "failed to send TERM to owned mount process"
      fi
    fi
    if ! wait_for_pid_exit "$pid" 5 "$active_mount_start_time" && \
      owned_pid_is_live "$pid" "$active_mount_start_time"; then
      if ! kill -KILL "$pid"; then
        add_cleanup_error "failed to send KILL to owned mount process"
      fi
      if ! wait_for_pid_exit "$pid" 5 "$active_mount_start_time"; then
        add_cleanup_error "owned mount process remained live after KILL"
      fi
    fi
  fi
  if owned_pid_is_live "$pid" "$active_mount_start_time"; then
    mount_exit_status=-1
    return 1
  fi
  if wait "$pid"; then
    status=0
  else
    status=$?
  fi
  mount_exit_status="$status"
  if [[ "$status" -ne 0 ]]; then
    add_cleanup_error "mount process exited with status $status"
  fi
  active_mount_pid=""
  active_mount_start_time=""
}

stop_active_collector() {
  local pid="$active_collector_pid"
  local status=0
  [[ -n "$pid" ]] || return 0
  if owned_pid_is_live "$pid" "$active_collector_start_time"; then
    if ! kill -TERM "$pid"; then
      add_cleanup_error "failed to send TERM to owned collector process"
    fi
    if ! wait_for_pid_exit "$pid" 5 "$active_collector_start_time" && \
      owned_pid_is_live "$pid" "$active_collector_start_time"; then
      if ! kill -KILL "$pid"; then
        add_cleanup_error "failed to send KILL to owned collector process"
      fi
      if ! wait_for_pid_exit "$pid" 5 "$active_collector_start_time"; then
        add_cleanup_error "owned collector process remained live after KILL"
      fi
    fi
  fi
  if owned_pid_is_live "$pid" "$active_collector_start_time"; then
    collector_exit_status=-1
    return 1
  fi
  if wait "$pid"; then
    status=0
  else
    status=$?
  fi
  collector_exit_status="$status"
  active_collector_pid=""
  active_collector_start_time=""
  [[ "$status" -eq 0 ]] || add_cleanup_error "collector exited with status $status"
}

cleanup_active_tuple() {
  local reason="${1:-}"
  local cgroup_member=""
  local cgroup_empty=true
  local paths_safe_to_remove=true
  [[ "$active_index" -ge 0 ]] || return 0
  [[ "$cleanup_in_progress" == false ]] || return 1
  cleanup_in_progress=true
  if [[ -n "$reason" ]]; then
    active_interrupted_by="$reason"
    add_cleanup_error "matrix interrupted by $reason"
  fi

  stop_active_collector

  if [[ -n "$active_mount_pid" ]]; then
    run_bounded_command "$active_work_dir/unmount.stdout.txt" \
      "$active_work_dir/unmount.stderr.txt" \
      "$studiofs_bin" umount "$active_mount_point"
    unmount_exit_status="$bounded_status"
    unmount_timed_out="$bounded_timed_out"
    [[ "$unmount_exit_status" -eq 0 ]] || add_cleanup_error \
      "studiofs umount exited with status $unmount_exit_status"

    if mount_is_active; then
      run_bounded_command "$active_work_dir/fusermount.stdout.txt" \
        "$active_work_dir/fusermount.stderr.txt" \
        fusermount3 -u "$active_mount_point"
      fallback_unmount_exit_status="$bounded_status"
      [[ "$fallback_unmount_exit_status" -eq 0 ]] || add_cleanup_error \
        "fusermount3 normal unmount exited with status $fallback_unmount_exit_status"
    fi
    if mount_is_active; then
      run_bounded_command "$active_work_dir/fusermount-lazy.stdout.txt" \
        "$active_work_dir/fusermount-lazy.stderr.txt" \
        fusermount3 -uz "$active_mount_point"
      fallback_lazy_exit_status="$bounded_status"
      [[ "$fallback_lazy_exit_status" -eq 0 ]] || add_cleanup_error \
        "fusermount3 lazy unmount exited with status $fallback_lazy_exit_status"
    fi
    if mount_is_active; then
      add_cleanup_error "mount remains active after fallback unmounts"
    fi
  fi

  reap_mount_process

  if ! mkdir -p "$active_artifact_dir"; then
    add_cleanup_error "failed to create tuple artifact directory"
    cleanup_in_progress=false
    return 1
  fi
  inspect_collector_cleanup
  move_tuple_logs
  if ! merge_cleanup_json; then
    add_cleanup_error "failed to merge matrix cleanup evidence"
    write_matrix_cleanup_payload "$active_artifact_dir/cleanup.json"
  fi
  if ! sync -f "$active_artifact_dir"; then
    add_cleanup_error "failed to sync tuple artifacts before directory cleanup"
  fi

  if [[ "$cgroup_created" == true && -d "$active_cgroup" ]]; then
    if [[ ! -r "$active_cgroup/cgroup.procs" ]]; then
      add_cleanup_error "cannot read active cgroup membership"
      cgroup_empty=false
    else
      while IFS= read -r cgroup_member; do
        if [[ -n "$cgroup_member" ]]; then
          add_cleanup_error "active cgroup is not empty after process cleanup"
          cgroup_empty=false
          break
        fi
      done <"$active_cgroup/cgroup.procs"
    fi
  fi
  if mount_is_active; then
    paths_safe_to_remove=false
  fi
  if [[ -n "$active_mount_pid" ]] && \
    owned_pid_is_live "$active_mount_pid" "$active_mount_start_time"; then
    paths_safe_to_remove=false
  fi
  if [[ -n "$active_collector_pid" ]] && \
    owned_pid_is_live "$active_collector_pid" "$active_collector_start_time"; then
    paths_safe_to_remove=false
  fi
  if [[ "$cgroup_empty" == false ]]; then
    paths_safe_to_remove=false
  fi

  if [[ "$paths_safe_to_remove" == true ]]; then
    if [[ "$mount_dir_created" == true ]]; then
      if safe_remove_tree "$active_mount_root" "$mount_root" mount; then
        mount_dir_removed=true
      fi
    elif [[ ! -e "$active_mount_root" ]]; then
      mount_dir_removed=true
    fi
    if [[ "$cache_dir_created" == true ]]; then
      if safe_remove_tree "$active_cache_dir" "$output_root/.work/cache" cache; then
        cache_dir_removed=true
      fi
    elif [[ ! -e "$active_cache_dir" ]]; then
      cache_dir_removed=true
    fi
  else
    add_cleanup_error "refused to remove mount and cache paths while tuple resources remain active"
  fi

  if [[ "$cgroup_created" == true && -d "$active_cgroup" ]]; then
    if [[ "$cgroup_empty" == true ]] && rmdir "$active_cgroup"; then
      cgroup_removed=true
    elif [[ "$cgroup_empty" == true ]]; then
      add_cleanup_error "failed to remove active cgroup"
    fi
  elif [[ ! -e "$active_cgroup" ]]; then
    cgroup_removed=true
  fi

  if ! merge_cleanup_json; then
    add_cleanup_error "failed to finalize matrix cleanup evidence"
    write_matrix_cleanup_payload "$active_artifact_dir/cleanup.json"
  fi
  if ! sync -f "$active_artifact_dir"; then
    add_cleanup_error "failed to sync final tuple cleanup evidence"
    if ! merge_cleanup_json; then
      write_matrix_cleanup_payload "$active_artifact_dir/cleanup.json"
    fi
  fi
  if [[ "$work_dir_created" == true && -d "$active_work_dir" ]]; then
    if ! rmdir "$active_work_dir" 2>/dev/null; then
      add_cleanup_error "failed to remove tuple work directory"
      if ! merge_cleanup_json; then
        write_matrix_cleanup_payload "$active_artifact_dir/cleanup.json"
      fi
    fi
  fi
  cleanup_in_progress=false
  [[ "${#cleanup_errors[@]}" -eq 0 ]]
}

update_schedule_result() {
  local index="$1"
  local passed="$2"
  local error_text="$(cleanup_error_text)"
  python3 - "$schedule_file" "$index" "$passed" "$collector_exit_status" "$error_text" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
index = int(sys.argv[2])
passed = sys.argv[3] == "true"
collector_status = int(sys.argv[4])
cleanup_error = sys.argv[5] or None
schedule = json.loads(path.read_text(encoding="utf-8"))
entry = schedule["tuples"][index]
entry["status"] = "passed" if passed else "failed"
entry["collector_exit_status"] = collector_status if collector_status >= 0 else None
entry["cleanup_error"] = cleanup_error
statuses = [item["status"] for item in schedule["tuples"]]
schedule["completed_tuple_count"] = sum(value == "passed" for value in statuses)
schedule["failed_tuple_count"] = sum(value == "failed" for value in statuses)
if schedule["failed_tuple_count"]:
    schedule["status"] = "failed"
    schedule["promotion_eligible"] = False
elif all(value == "passed" for value in statuses):
    schedule["status"] = "completed"
else:
    schedule["status"] = "pending"
temporary = path.with_suffix(".json.tmp")
temporary.write_text(json.dumps(schedule, indent=2, sort_keys=True) + "\n", encoding="utf-8")
temporary.replace(path)
PY
}

reset_active_tuple() {
  active_index=-1
  active_mount_pid=""
  active_mount_start_time=""
  active_collector_pid=""
  active_collector_start_time=""
  active_mount_root=""
  active_mount_point=""
  active_cache_dir=""
  active_cgroup=""
  active_artifact_dir=""
  active_work_dir=""
  active_interrupted_by=""
  cleanup_in_progress=false
  collector_exit_status=-1
  collector_artifacts_complete=false
  unmount_exit_status=-1
  unmount_timed_out=false
  fallback_unmount_exit_status=-1
  fallback_lazy_exit_status=-1
  mount_exit_status=-1
  mount_exit_timed_out=false
  mount_dir_removed=false
  cache_dir_removed=false
  cgroup_removed=false
  collector_cleanup_checked=false
  collector_cleanup_present=false
  mount_dir_created=false
  cache_dir_created=false
  cgroup_created=false
  work_dir_created=false
  cleanup_errors=()
}

for index in "${!tuple_modes[@]}"; do
  for fresh_path in \
    "${tuple_mount_roots[$index]}" \
    "${tuple_cache_dirs[$index]}" \
    "${tuple_cgroups[$index]}" \
    "${tuple_artifact_dirs[$index]}" \
    "$output_root/.work/tuples/${tuple_slugs[$index]}"; do
    if [[ -e "$fresh_path" ]]; then
      reset_active_tuple
      active_index="$index"
      add_cleanup_error "scheduled path is not fresh: $fresh_path"
      if ! update_schedule_result "$index" false; then
        die "failed to mark non-fresh tuple path in schedule: $fresh_path"
      fi
      reset_active_tuple
      die "real matrix requires fresh tuple paths: $fresh_path"
    fi
  done
done

on_signal() {
  local signal_name="$1"
  local exit_status=130
  [[ "$signal_name" != TERM ]] || exit_status=143
  if [[ "$cleanup_in_progress" == true ]]; then
    if [[ -z "$deferred_signal" ]]; then
      deferred_signal="$signal_name"
      active_interrupted_by="$signal_name"
      add_cleanup_error "matrix interrupted by $signal_name during cleanup"
    fi
    trap '' INT TERM
    return 0
  fi
  trap '' INT TERM
  set +e
  cleanup_active_tuple "$signal_name"
  local cleanup_status=$?
  if [[ "$active_index" -ge 0 ]]; then
    update_schedule_result "$active_index" false
    local schedule_status=$?
    if [[ "$schedule_status" -ne 0 ]]; then
      printf 'Error: failed to mark interrupted tuple in schedule\n' >&2
    fi
  fi
  if [[ "$cleanup_status" -ne 0 ]]; then
    printf 'Error: interrupted tuple cleanup failed\n' >&2
  fi
  exit "$exit_status"
}

trap 'on_signal INT' INT
trap 'on_signal TERM' TERM

run_real_tuple() {
  local index="$1"
  local cache_policy=direct
  local async_dio=0
  local setup_failed=false
  local cleanup_passed=false
  local tuple_passed=false
  local collector_status=0
  local -a collector_args=()
  reset_active_tuple
  active_index="$index"
  active_mount_root="${tuple_mount_roots[$index]}"
  active_mount_point="${tuple_mount_points[$index]}"
  active_cache_dir="${tuple_cache_dirs[$index]}"
  active_cgroup="${tuple_cgroups[$index]}"
  active_artifact_dir="${tuple_artifact_dirs[$index]}"
  active_work_dir="$output_root/.work/tuples/${tuple_slugs[$index]}"

  for fresh_path in "$active_mount_root" "$active_cache_dir" "$active_cgroup" \
    "$active_artifact_dir" "$active_work_dir"; do
    if [[ -e "$fresh_path" ]]; then
      add_cleanup_error "scheduled path is not fresh: $fresh_path"
      setup_failed=true
    fi
  done
  if [[ "$setup_failed" == false ]]; then
    if mkdir "$active_work_dir"; then
      work_dir_created=true
    else
      add_cleanup_error "failed to create fresh tuple work directory"
      setup_failed=true
    fi
  fi
  if [[ "$setup_failed" == false ]]; then
    if mkdir "$active_mount_root"; then
      mount_dir_created=true
    else
      add_cleanup_error "failed to create fresh mount directory"
      setup_failed=true
    fi
  fi
  if [[ "$setup_failed" == false ]]; then
    if mkdir "$active_cache_dir"; then
      cache_dir_created=true
    else
      add_cleanup_error "failed to create fresh cache directory"
      setup_failed=true
    fi
  fi
  if [[ "$setup_failed" == false ]]; then
    if mkdir "$active_cgroup"; then
      cgroup_created=true
    else
      add_cleanup_error "failed to create fresh cgroup"
      setup_failed=true
    fi
  fi

  if [[ "${tuple_modes[$index]}" == direct-async ]]; then
    async_dio=1
  elif [[ "${tuple_modes[$index]}" == kernel ]]; then
    cache_policy=kernel
  fi

  if [[ "$setup_failed" == false ]]; then
    env \
      "STUDIOFS_LINUX_FUSE_CACHE_POLICY=$cache_policy" \
      "STUDIOFS_FUSE_ASYNC_DIO=$async_dio" \
      "$studiofs_bin" mount "$volume" "$active_mount_root" \
      --cache-dir "$active_cache_dir" --foreground \
      >"$active_work_dir/mount.stdout.txt" \
      2>"$active_work_dir/mount.stderr.txt" &
    active_mount_pid=$!
    sleep 0.1
    if ! active_mount_start_time="$(pid_start_time "$active_mount_pid")"; then
      add_cleanup_error "cannot record owned mount process start time"
      setup_failed=true
    elif ! owned_pid_is_live "$active_mount_pid" "$active_mount_start_time"; then
      add_cleanup_error "mount process exited before collector preflight"
      setup_failed=true
    fi
  fi

  if [[ "$setup_failed" == false ]]; then
    collector_args=(
      "$script_dir/linux-cache-gate.sh"
      --mode "${tuple_modes[$index]}"
      --workload "${tuple_workloads[$index]}"
      --mount-point "$active_mount_point"
      --mount-pid "$active_mount_pid"
      --runs 1
      --cgroup "$active_cgroup"
      --bench "$bench_bin"
      --studiofs "$studiofs_bin"
      --correctness-manifest "$correctness_manifest"
      --output-root "$active_artifact_dir"
    )
    if [[ "$diagnostic" == true ]]; then
      collector_args+=(--diagnostic)
    fi
    bash "${collector_args[@]}" \
      >"$active_work_dir/collector.stdout.txt" \
      2>"$active_work_dir/collector.stderr.txt" &
    active_collector_pid=$!
    if ! active_collector_start_time="$(pid_start_time "$active_collector_pid")"; then
      add_cleanup_error "cannot record owned collector process start time"
    fi
    if wait "$active_collector_pid"; then
      collector_status=0
    else
      collector_status=$?
    fi
    collector_exit_status="$collector_status"
    active_collector_pid=""
    active_collector_start_time=""
    verify_collector_artifacts
  else
    collector_exit_status=-1
    collector_artifacts_complete=false
  fi

  if cleanup_active_tuple ""; then
    cleanup_passed=true
  fi
  if [[ "$setup_failed" == false && "$collector_exit_status" -eq 0 &&
    "$collector_artifacts_complete" == true && "$cleanup_passed" == true ]]; then
    tuple_passed=true
  fi
  if ! update_schedule_result "$index" "$tuple_passed"; then
    printf 'Error: failed to update matrix schedule for tuple %s\n' \
      "${tuple_slugs[$index]}" >&2
    tuple_passed=false
  fi
  reset_active_tuple
  [[ "$tuple_passed" == true ]]
}

matrix_failed=false
for index in "${!tuple_modes[@]}"; do
  if ! run_real_tuple "$index"; then
    matrix_failed=true
    break
  fi
done
trap - INT TERM

if [[ -n "$deferred_signal" ]]; then
  if [[ "$deferred_signal" == TERM ]]; then
    exit 143
  fi
  exit 130
fi

if [[ "$matrix_failed" == true ]]; then
  printf 'Error: matrix stopped after tuple failure; inspect %s\n' "$schedule_file" >&2
  exit 1
fi

printf '%s\n' "$output_root"
