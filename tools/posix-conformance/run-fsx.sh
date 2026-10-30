#!/usr/bin/env bash
# Run the SFS-906 fsx profiles with fixed seeds (docs/posix-conformance.md).
#
# Usage: run-fsx.sh <label> <test-dir> <run-dir> [profile...]
#
# Profiles: default nommap small large large-nommap (all when none is given).
# The `large` profiles use a 64 MiB file with up to 1 MiB ops, so they cross the
# 4 MiB chunk and overlay-queue boundaries (SFS-885). The `.fsxops` and
# `.fsxgood` files of a failing run are kept in <run-dir>/fsx-<label>/.
set -u

label=$1 dir=$2 run=$3
shift 3
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
fsx=${POSIX_ROOT:-$repo/target/posix-conformance}/tools/bin/fsx
out=$run/fsx-$label
declare -A args=(
  [default]="-N 100000 -S 906001"
  [nommap]="-R -W -N 100000 -S 906002"
  [small]="-l 65536 -o 4096 -N 100000 -S 906003"
  [large]="-l 67108864 -o 1048576 -N 20000 -S 906004"
  [large-nommap]="-R -W -l 67108864 -o 1048576 -N 20000 -S 906005"
)
profiles=${*:-default nommap small large large-nommap}

mkdir -p "$out" "$dir"
for profile in $profiles; do
  file=$dir/fsx-$profile
  start=$(date +%s)
  # shellcheck disable=SC2086 # the profile arguments are intentionally split
  timeout -k 30 3600 "$fsx" ${args[$profile]} -P "$out" "$file" >"$out/$profile.log" 2>&1
  rc=$?
  echo "$profile args='${args[$profile]}' rc=$rc secs=$(($(date +%s) - start)) $(tail -1 "$out/$profile.log")" |
    tee -a "$out/summary.txt"
done
