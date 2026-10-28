#!/usr/bin/env bash
# Run pjdfstest one syscall group at a time (SFS-906, docs/posix-conformance.md).
#
# Usage (as root): run-pjdfstest.sh <label> <test-dir> <run-dir>
#
# Each group runs in its own 0777 work directory with a per-group timeout, so a
# hang loses one group, not the run. Logs go to <run-dir>/pjdfstest-<label>/.
# Use this for the ext4 control; on a StudioFS mount prefer
# run-pjdfstest-per-case.sh until SFS-910 is fixed, because one degrading case
# turns every later case of the group into EIO.
set -u

label=$1 dir=$2 run=$3
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
tools=${POSIX_ROOT:-$repo/target/posix-conformance}/tools
pjdfstest=$tools/pjdfstest-rs/rust/target/release/pjdfstest
config=$here/pjdfstest-linux.toml
secondary=/dev/shm/sfs906-secondary-$label
out=$run/pjdfstest-$label
groups="chmod chown ftruncate link mkdir mkfifo mknod open posix_fallocate rename rmdir symlink truncate unlink utimensat"

mkdir -p "$out" "$secondary"
for group in $groups; do
  work=$dir/pjd-$group
  mkdir -p "$work"
  chmod 0777 "$work"
  start=$(date +%s)
  # Test names are `pjdfstest::tests::<group>::...`; the prefix keeps `link` from matching `unlink`.
  timeout -k 30 900 "$pjdfstest" -c "$config" -p "$work" -s "$secondary" "tests::$group::" \
    >"$out/$group.log" 2>&1
  rc=$?
  echo "$group rc=$rc secs=$(($(date +%s) - start)) $(grep '^Summary' "$out/$group.log")" |
    tee -a "$out/summary.txt"
done
