#!/usr/bin/env bash
# Profile foreground metadata microbenchmarks on a mounted StudioFS volume.
#
# For each phase the script records:
#   - on-CPU ms/op per thread group from /proc/<pid>/task/*/stat deltas
#     (user + system time, so kernel time is included even when perf cannot
#     sample the kernel);
#   - a perf call-graph profile of the mount process (perf.data per phase);
#   - the top inclusive pg-meta-poll stacks.
#
# Phases (same as the 2026-10-24 Linux audit and SFS-905):
#   rename        N renames in one directory
#   create        N x (create + write 4 KiB + close)
#   parallel      8 processes doing rename + unlink
#   rename-big    N renames in a directory with 8,000 extra entries
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: linux-metadata-profile.sh --target <dir> --pid <mount-pid> --out <dir>
                                 [--perf <perf-binary>] [--ops <n>]
                                 [--phases rename,create,parallel,rename-big]

  --target   Empty directory inside the mounted volume used by the workload
  --pid      PID of the `studiofs mount` process
  --out      Output directory for per-phase perf.data, stacks, and summary.tsv
  --perf     perf binary (default: perf on PATH)
  --ops      Operations per phase (default: 1000)
EOF
}

TARGET="" PID="" OUT="" PERF="perf" OPS=1000 PHASES="rename,create,parallel,rename-big"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    --pid) PID="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --perf) PERF="$2"; shift 2 ;;
    --ops) OPS="$2"; shift 2 ;;
    --phases) PHASES="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done
[[ -n "$TARGET" && -n "$PID" && -n "$OUT" ]] || { usage >&2; exit 2; }
[[ -d "/proc/$PID" ]] || { echo "pid $PID is not running" >&2; exit 1; }
mkdir -p "$OUT"
CLK_TCK=$(getconf CLK_TCK)

# libfuse request workers inherit the name of the thread that runs the mount.
# With the tray that is the `studiofs-mount` thread; without it (headless host,
# tray initialization failed) the mount runs on the main thread, so workers
# carry the process name.
FUSE_COMM=$(cat /proc/"$PID"/comm)
if grep -qx 'studiofs-mount' /proc/"$PID"/task/*/comm 2>/dev/null; then
  FUSE_COMM=studiofs-mount
fi
echo "FUSE worker thread name: $FUSE_COMM" >&2

# Prints "<group>\t<ticks>" for every thread of the mount process.
# Groups: pg-meta-poll, fuse (libfuse request threads), tokio (runtime
# workers), other.
thread_ticks() {
  for stat in /proc/"$PID"/task/*/stat; do
    local line comm rest
    line=$(cat "$stat" 2>/dev/null) || continue
    comm=${line#*(}; comm=${comm%)*}
    rest=${line##*) }
    # Fields after ") ": state is field 3; utime/stime are fields 14/15.
    read -r -a f <<<"$rest"
    local ticks=$(( f[11] + f[12] ))
    local group
    case "$comm" in
      pg-meta-poll) group=pg-meta-poll ;;
      tokio-runtime-w*|tokio-rt-worker*) group=tokio ;;
      "$FUSE_COMM") group=fuse ;;
      *) group="other:$comm" ;;
    esac
    printf '%s\t%s\n' "$group" "$ticks"
  done | awk -F'\t' '{s[$1]+=$2} END {for (g in s) printf "%s\t%d\n", g, s[g]}' | sort
}

# Rust v0 symbols: c++filt (binutils >= 2.36) demangles them; drop crate hashes.
demangle() {
  if command -v c++filt >/dev/null; then
    c++filt | sed -E 's/\[[0-9a-f]{16}\]//g'
  else
    cat
  fi
}

run_phase() {
  local phase="$1" dir="$TARGET/$phase"
  mkdir -p "$dir"
  # Untimed setup.
  case "$phase" in
    rename)
      python3 - "$dir" "$OPS" <<'PY'
import os, sys
d, n = sys.argv[1], int(sys.argv[2])
for i in range(n): open(os.path.join(d, f"a{i}"), "w").close()
PY
      ;;
    parallel)
      python3 - "$dir" "$OPS" <<'PY'
import os, sys
d, n = sys.argv[1], int(sys.argv[2])
for w in range(8):
    os.makedirs(os.path.join(d, f"w{w}"), exist_ok=True)
    for i in range(n // 8): open(os.path.join(d, f"w{w}", f"a{i}"), "w").close()
PY
      ;;
    rename-big)
      python3 - "$dir" "$OPS" <<'PY'
import os, sys
d, n = sys.argv[1], int(sys.argv[2])
for i in range(8000): open(os.path.join(d, f"extra{i}"), "w").close()
for i in range(n): open(os.path.join(d, f"a{i}"), "w").close()
PY
      ;;
  esac
  sync -f "$dir" 2>/dev/null || true

  local workload ops="$OPS"
  case "$phase" in
    rename|rename-big)
      workload=(python3 -c 'import os,sys
d,n=sys.argv[1],int(sys.argv[2])
for i in range(n): os.rename(f"{d}/a{i}", f"{d}/b{i}")' "$dir" "$OPS") ;;
    create)
      workload=(python3 -c 'import os,sys
d,n=sys.argv[1],int(sys.argv[2]); b=b"x"*4096
for i in range(n):
    fd=os.open(f"{d}/c{i}", os.O_CREAT|os.O_WRONLY|os.O_TRUNC, 0o644); os.write(fd,b); os.close(fd)' "$dir" "$OPS") ;;
    parallel)
      # Each worker renames then unlinks its files: 2 ops per file.
      ops=$(( (OPS / 8) * 8 * 2 ))
      workload=(python3 -c 'import os,sys
from multiprocessing import Process
d,n=sys.argv[1],int(sys.argv[2])//8
def w(k):
    p=f"{d}/w{k}"
    for i in range(n): os.rename(f"{p}/a{i}", f"{p}/b{i}")
    for i in range(n): os.unlink(f"{p}/b{i}")
ps=[Process(target=w,args=(k,)) for k in range(8)]
[p.start() for p in ps]; [p.join() for p in ps]
sys.exit(max(p.exitcode for p in ps))' "$dir" "$OPS") ;;
  esac

  local before="$OUT/$phase.ticks.before" after="$OUT/$phase.ticks.after"
  thread_ticks >"$before"
  local t0 t1
  t0=$(date +%s%N)
  "$PERF" record -q -F 999 -g --call-graph fp -p "$PID" -o "$OUT/$phase.perf.data" -- "${workload[@]}" \
    2>"$OUT/$phase.perf-record.log"
  t1=$(date +%s%N)
  # Background replication (pg-meta-poll) can trail the workload. Sample until
  # the process CPU total is unchanged for two consecutive seconds (max 30 s)
  # so deferred work is charged to this phase. Idle housekeeping threads are
  # excluded from the check.
  local prev="" cur stable=0
  for _ in $(seq 30); do
    cur=$(thread_ticks | awk -F'\t' '$1 !~ /^other:/ {s+=$2} END {print s}')
    if [[ "$cur" == "$prev" ]]; then stable=$((stable + 1)); else stable=0; fi
    [[ $stable -ge 2 ]] && break
    prev="$cur"
    sleep 1
  done
  thread_ticks >"$after"

  local wall_ms=$(( (t1 - t0) / 1000000 ))
  join -t $'\t' -a 2 -e 0 -o 0,1.2,2.2 "$before" "$after" |
    awk -F'\t' -v phase="$phase" -v ops="$ops" -v hz="$CLK_TCK" -v wall="$wall_ms" '
      { d=$3-$2; if (d<0) d=0; g=$1; if (g ~ /^other:/) { other+=d; next } grp[g]+=d }
      END {
        printf "%s\t%d\t%d\t%.3f", phase, ops, wall, wall/ops
        n=split("fuse tokio pg-meta-poll", order, " ")
        for (i=1;i<=n;i++) printf "\t%.3f", grp[order[i]]*1000/hz/ops
        printf "\t%.3f\n", other*1000/hz/ops
      }' >>"$OUT/summary.tsv"

  "$PERF" report -i "$OUT/$phase.perf.data" --stdio --no-children -s comm \
    2>/dev/null | demangle >"$OUT/$phase.comm.txt" || true
  "$PERF" report -i "$OUT/$phase.perf.data" --stdio --children --comms pg-meta-poll \
    -g caller,0.5,callee,function,percent --percent-limit 3 \
    2>/dev/null | demangle >"$OUT/$phase.pg-meta-poll.txt" || true
  rm -rf "$dir"
}

printf 'phase\tops\twall_ms\twall_ms_per_op\tfuse_ms_per_op\ttokio_ms_per_op\tpg_meta_poll_ms_per_op\tother_ms_per_op\n' >"$OUT/summary.tsv"
IFS=, read -r -a phase_list <<<"$PHASES"
for phase in "${phase_list[@]}"; do
  echo "phase $phase" >&2
  run_phase "$phase"
done
column -t -s $'\t' "$OUT/summary.tsv"
