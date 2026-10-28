#!/usr/bin/env bash
# Run every pjdfstest case alone on a StudioFS mount (SFS-906, docs/posix-conformance.md).
#
# Usage (as root):
#   run-pjdfstest-per-case.sh <run-dir> <case-list> <mount-log>
#
# Environment:
#   STUDIOFS_BIN   studiofs binary (default <repo>/target/release/studiofs)
#   SFS_VOLUME     volume name (default test)
#   SFS_MOUNT      mount path, ending in the volume name (default /srv/sfs906/mnt/<volume>)
#   SFS_MOUNT_USER user that owns the mount (default $SUDO_USER)
#
# <case-list> holds one case name per line, for example taken from the ext4
# control logs. After each case the mount log is checked for an abandoned
# deferred namespace batch (SFS-910). When one is found, the case is recorded with
# degraded=1 and the volume is remounted, so later cases start from a healthy
# mount. The 1.5 s pause lets the batch linger (10 ms) and its three 250 ms
# retries finish before the log is inspected; it is a diagnostic settle, not a
# correctness barrier.
set -u

run=$1 list=$2 mount_log=$3
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
tools=${POSIX_ROOT:-$repo/target/posix-conformance}/tools
pjdfstest=$tools/pjdfstest-rs/rust/target/release/pjdfstest
config=$here/pjdfstest-linux.toml
studiofs=${STUDIOFS_BIN:-$repo/target/release/studiofs}
volume=${SFS_VOLUME:-test}
mount=${SFS_MOUNT:-/srv/sfs906/mnt/$volume}
mount_user=${SFS_MOUNT_USER:-${SUDO_USER:?set SFS_MOUNT_USER}}
work=$mount/sfs906-per-case
secondary=/dev/shm/sfs906-secondary-studiofs
out=$run/pjdfstest-studiofs-per-case

remount() {
  sudo -u "$mount_user" "$studiofs" umount "$mount" >/dev/null 2>&1
  for _ in $(seq 1 60); do mountpoint -q "$mount" || break; sleep 1; done
  sudo -u "$mount_user" --preserve-env=STUDIOFS_LINUX_FUSE_CACHE_POLICY \
    "$studiofs" mount "$volume" "$mount" -d --allow-other >/dev/null 2>&1
  for _ in $(seq 1 60); do mountpoint -q "$mount" && break; sleep 1; done
  mkdir -p "$work"
  chmod 0777 "$work"
}

mkdir -p "$out" "$secondary"
: >"$out/results.txt"
remount
while read -r name; do
  before=$(wc -l <"$mount_log")
  timeout -k 10 300 "$pjdfstest" -e -c "$config" -p "$work" -s "$secondary" \
    "pjdfstest::tests::$name" >"$out/case.log" 2>&1
  rc=$?
  verdict=$(grep -E "^$name[[:space:]]" "$out/case.log" | awk '{print $NF}')
  detail=$(grep -A3 -E "^$name[[:space:]]" "$out/case.log" | sed -n '2,4p' | tr '\n\t' '  ' | cut -c1-200)
  sleep 1.5
  degraded=0
  if tail -n +"$((before + 1))" "$mount_log" | grep -q 'abandoning batch'; then degraded=1; fi
  printf '%s\t%s\trc=%s\tdegraded=%s\t%s\n' "$name" "${verdict:-none}" "$rc" "$degraded" "$detail" |
    tee -a "$out/results.txt"
  if [ "$degraded" = 1 ]; then remount; fi
done <"$list"
