#!/usr/bin/env bash
# Back-to-back sustained sequential write series for Linux (SFS-893).
#
# Runs N single-job fio sequential writes (psync, end_fsync=1) into distinct
# files under --target and records, per run: throughput, the foreground write
# time versus the end-of-job fsync time, the cache-disk sectors written, the
# host page-cache Dirty+Writeback total, the StudioFS write-admission wait,
# hard-backpressure events and local cache I/O time, and
# StudioFS runtime stats plus write-path observability snapshots when the
# target is inside a StudioFS mount. Point --target at a plain directory on the
# cache disk to record the raw-disk baseline with the same workload.
#
# The script never mounts, initializes, or destroys volumes and never reads
# credentials; mount the volume with the cache placement under test first.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: linux-sustained-write.sh --target <dir> [options]

  --target <dir>       Directory to write into (StudioFS mount or raw directory)
  --mount-root <dir>   StudioFS mount root holding .studiofs-runtime-stats
                       (defaults to --target)
  --cache-root <dir>   StudioFS client root or cache dir to search for
                       write_path_observability.json (optional)
  --runs <n>           Number of back-to-back runs (default 4)
  --size <size>        fio size per run (default 2G)
  --block-size <size>  fio block size (default 1M)
  --out <dir>          Artifact directory (default target/perf/linux-sustained-write-<ts>)
  --keep-files         Keep written files after the series (default: delete)
  --sync-between       Run sync(1) before each run, outside the measured window,
                       to drain host dirty pages (control experiment)
EOF
}

TARGET=""
MOUNT_ROOT=""
CACHE_ROOT=""
RUNS=4
SIZE=2G
BLOCK_SIZE=1M
OUT=""
KEEP_FILES=0
SYNC_BETWEEN=0
# Fail with usage, not an unbound-variable error, when a flag lacks its value.
need_value() {
  [ "$2" -ge 2 ] || { echo "missing value for $1" >&2; usage >&2; exit 2; }
}
while [ $# -gt 0 ]; do
  case "$1" in
    --target) need_value "$1" $#; TARGET=$2; shift 2 ;;
    --mount-root) need_value "$1" $#; MOUNT_ROOT=$2; shift 2 ;;
    --cache-root) need_value "$1" $#; CACHE_ROOT=$2; shift 2 ;;
    --runs) need_value "$1" $#; RUNS=$2; shift 2 ;;
    --size) need_value "$1" $#; SIZE=$2; shift 2 ;;
    --block-size) need_value "$1" $#; BLOCK_SIZE=$2; shift 2 ;;
    --out) need_value "$1" $#; OUT=$2; shift 2 ;;
    --keep-files) KEEP_FILES=1; shift ;;
    --sync-between) SYNC_BETWEEN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done
[ -n "$TARGET" ] || { usage >&2; exit 2; }
[ -d "$TARGET" ] || { echo "target is not a directory: $TARGET" >&2; exit 2; }
if [ -n "$CACHE_ROOT" ] && [ ! -d "$CACHE_ROOT" ]; then
  echo "cache root is not a directory: $CACHE_ROOT" >&2
  exit 2
fi
command -v fio >/dev/null || { echo "fio is required" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 2; }

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
MOUNT_ROOT=${MOUNT_ROOT:-$TARGET}
OUT=${OUT:-$repo_root/target/perf/linux-sustained-write-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"

STATS_FILE="$MOUNT_ROOT/.studiofs-runtime-stats"
WPO_FILE=""
if [ -n "$CACHE_ROOT" ]; then
  WPO_FILE=$(find "$CACHE_ROOT" -name write_path_observability.json -print -quit 2>/dev/null || true)
fi

# Block device that backs the cache root (or the target for raw runs).
disk_probe=${CACHE_ROOT:-$TARGET}
disk_source=$(df --output=source "$disk_probe" | tail -1)
# Resolve /dev/mapper/* and /dev/disk/by-* links to the kernel name diskstats uses.
DISK_DEV=$(basename "$(readlink -f "$disk_source" 2>/dev/null || echo "$disk_source")")
if ! awk -v dev="$DISK_DEV" '$3==dev {found=1} END {exit !found}' /proc/diskstats; then
  echo "warning: $disk_probe is on '$disk_source' with no /proc/diskstats row; disk_written_mib is not measured" >&2
  DISK_DEV=""
fi
# Prints nothing when the device is not a block device in /proc/diskstats.
disk_sectors_written() {
  [ -n "$DISK_DEV" ] || return 0
  awk -v dev="$DISK_DEV" '$3==dev {print $10}' /proc/diskstats
}

now_ms() { date +%s%3N; }

# Copy the write-path observability snapshot once the 1 s publisher has
# produced one generated at or after <min_ms>; bounded at 5 s.
copy_fresh_wpo() {
  local dest=$1 min_ms=$2
  [ -n "$WPO_FILE" ] || return 0
  python3 - "$WPO_FILE" "$dest" "$min_ms" <<'PY'
import json, shutil, sys, time
src, dest, min_ms = sys.argv[1], sys.argv[2], int(sys.argv[3])
deadline = time.monotonic() + 5.0
while True:
    try:
        with open(src, "rb") as fh:
            raw = fh.read()
        if json.loads(raw).get("generated_unix_ms", 0) >= min_ms:
            break
    except (OSError, ValueError):
        pass
    if time.monotonic() >= deadline:
        print(f"warning: no observability snapshot newer than {min_ms}", file=sys.stderr)
        break
    time.sleep(0.1)
shutil.copyfile(src, dest)
PY
}

{
  echo "target=$TARGET"
  echo "cache_root=${CACHE_ROOT:-none}"
  echo "disk_device=${DISK_DEV:-unmeasured ($disk_source)}"
  echo "target_fs=$(df -T "$TARGET" | tail -1 | awk '{print $2}')"
  echo "cache_fs=$(df -T "$disk_probe" | tail -1 | awk '{print $2}')"
  echo "runs=$RUNS size=$SIZE block_size=$BLOCK_SIZE sync_between=$SYNC_BETWEEN"
  echo "git=$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "kernel=$(uname -r)"
  echo "fio=$(fio --version)"
} >"$OUT/env.txt"

vmstat -n 1 >"$OUT/vmstat.txt" &
vmstat_pid=$!
# Host page-cache dirty state: payload files in throughput-first Fast cache
# mode stay in the page cache after fsync, so kernel dirty throttling applies.
( while :; do
    echo "$(now_ms) $(awk '/^(Dirty|Writeback):/ {printf "%s ", $2}' /proc/meminfo)"
    sleep 1
  done ) >"$OUT/meminfo-dirty.txt" &
meminfo_pid=$!
trap 'kill "$vmstat_pid" "$meminfo_pid" 2>/dev/null || true' EXIT
host_dirty_kib() {
  awk '/^(Dirty|Writeback):/ {sum += $2} END {print sum + 0}' /proc/meminfo
}

printf 'run\tmib_per_s\truntime_ms\twrite_ms\tfsync_ms\twrite_lat_mean_us\twrite_lat_max_ms\tdisk_written_mib\thost_dirty_mib\tupload_pending_mib\tadmission_wait_ms\thard_backpressure\tcache_io_ms\n' \
  >"$OUT/summary.tsv"
for i in $(seq 1 "$RUNS"); do
  [ "$SYNC_BETWEEN" = 1 ] && sync
  [ -r "$STATS_FILE" ] && cat "$STATS_FILE" >"$OUT/stats-before-$i.txt"
  copy_fresh_wpo "$OUT/wpo-before-$i.json" "$(now_ms)"
  sectors_before=$(disk_sectors_written)
  fio --name="seq$i" --filename="$TARGET/sustained-write-$i.bin" --rw=write \
    --bs="$BLOCK_SIZE" --size="$SIZE" --ioengine=psync --numjobs=1 --end_fsync=1 \
    --output-format=json >"$OUT/fio-$i.json"
  finished_ms=$(now_ms)
  sectors_after=$(disk_sectors_written)
  dirty_kib=$(host_dirty_kib)
  [ -r "$STATS_FILE" ] && cat "$STATS_FILE" >"$OUT/stats-after-$i.txt"
  copy_fresh_wpo "$OUT/wpo-after-$i.json" "$finished_ms"
  python3 - "$OUT" "$i" "$sectors_before" "$sectors_after" "$dirty_kib" <<'PY' >>"$OUT/summary.tsv"
import json, os, sys
out, run = sys.argv[1], sys.argv[2]
# Empty sector counts mean the cache disk is not a measurable block device.
before = int(sys.argv[3]) if sys.argv[3] else None
after = int(sys.argv[4]) if sys.argv[4] else None
dirty_kib = int(sys.argv[5])
job = json.load(open(os.path.join(out, f"fio-{run}.json")))["jobs"][0]
write = job["write"]
runtime_ms = job["job_runtime"]
# psync issues one request at a time, so the summed request latency is the
# foreground write time; the rest of the job is the end_fsync barrier.
write_ms = write["lat_ns"]["mean"] * write["total_ios"] / 1e6
pending = ""
stats = os.path.join(out, f"stats-after-{run}.txt")
if os.path.exists(stats):
    for line in open(stats):
        parts = line.split()
        if len(parts) >= 4 and parts[1] == "upload_queue.pending_bytes":
            pending = f"{int(parts[3]) / 2**20:.0f}"
admission = hard = cache_io = ""
wpo_before = os.path.join(out, f"wpo-before-{run}.json")
wpo_after = os.path.join(out, f"wpo-after-{run}.json")
if os.path.exists(wpo_before) and os.path.exists(wpo_after):
    b, a = json.load(open(wpo_before)), json.load(open(wpo_after))
    def delta(*path):
        def get(doc):
            for key in path:
                doc = doc.get(key, {}) if isinstance(doc, dict) else {}
            return doc if isinstance(doc, (int, float)) else 0
        return get(a) - get(b)
    admission = f"{delta('vfs_write_path', 'admission_throttle', 'total_ms'):.0f}"
    hard = f"{delta('write_backpressure', 'hard_events'):.0f}"
    cache_io = f"{delta('local_cache_io', 'backend_io_total_ms'):.0f}"
print("\t".join([
    run,
    f"{write['bw_bytes'] / 2**20:.0f}",
    str(runtime_ms),
    f"{write_ms:.0f}",
    f"{max(runtime_ms - write_ms, 0):.0f}",
    f"{write['lat_ns']['mean'] / 1e3:.1f}",
    f"{write['lat_ns']['max'] / 1e6:.1f}",
    f"{(after - before) * 512 / 2**20:.0f}" if before is not None and after is not None else "",
    f"{dirty_kib / 1024:.0f}",
    pending,
    admission,
    hard,
    cache_io,
]))
PY
done

if [ "$KEEP_FILES" = 0 ]; then
  rm -f "$TARGET"/sustained-write-*.bin
fi
column -t -s $'\t' "$OUT/summary.tsv"
echo "artifacts: $OUT"
