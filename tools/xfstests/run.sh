#!/usr/bin/env bash
set -u

usage() { echo "usage: $0 <xfstests-dir> <new-run-dir> [generic/NNN...]" >&2; exit 2; }
[[ $# -ge 2 ]] || usage
xfstests=$(realpath "$1") || exit 2
run=$(realpath -m "$2") || exit 2
here=$(cd "$(dirname "$0")" && pwd) || exit 2
shift 2
[[ -x $xfstests/check && -f $xfstests/tests/generic/group.list ]] || { echo "invalid xfstests checkout" >&2; exit 2; }
[[ ! -e $run ]] || { echo "run directory already exists" >&2; exit 2; }
for name in TEST_DEV TEST_DIR SCRATCH_DEV SCRATCH_MNT STUDIOFS_BINARY STUDIOFS_CLIENT_ROOT XFSTESTS_RUN; do
    [[ -n ${!name:-} ]] || { echo "missing required environment variable: $name" >&2; exit 2; }
done
[[ ${FSTYP:-} == fuse && ${FUSE_SUBTYP:-} == .studiofs ]] || { echo "FSTYP=fuse and FUSE_SUBTYP=.studiofs are required" >&2; exit 2; }
[[ $TEST_DEV != "$SCRATCH_DEV" && -x $STUDIOFS_BINARY ]] || { echo "invalid devices or StudioFS binary" >&2; exit 2; }
timeout_s=${TEST_TIMEOUT:-300}
[[ $timeout_s =~ ^[1-9][0-9]*$ ]] || { echo "TEST_TIMEOUT must be positive integer seconds" >&2; exit 2; }
mkdir -p "$run/cases" "$STUDIOFS_CLIENT_ROOT" "$TEST_DIR" "$SCRATCH_MNT"
exec 3>"$run/manifest.tsv"
printf 'test\texit_status\twall_seconds\tverdict\n' >&3

sha() { sha256sum "$1" | cut -d' ' -f1; }
{
    printf 'product_sha\t%s\n' "$(git -C "$(dirname "$STUDIOFS_BINARY")" rev-parse HEAD 2>/dev/null || git rev-parse HEAD)"
    printf 'binary_sha256\t%s\n' "$(sha "$STUDIOFS_BINARY")"
    printf 'xfstests_sha\t%s\n' "$(git -C "$xfstests" rev-parse HEAD 2>/dev/null || echo unknown)"
    for name in FSTYP FUSE_SUBTYP TEST_DEV TEST_DIR SCRATCH_DEV SCRATCH_MNT FUSE_MOUNT_OPTIONS STUDIOFS_LINUX_FUSE_CACHE_POLICY STUDIOFS_FUSE_DIRECT_IO_MMAP TEST_TIMEOUT; do
        [[ -v $name ]] && printf '%s\t%s\n' "$name" "${!name}"
    done
} >"$run/metadata.tsv"

if (($#)); then
    tests=("$@")
else
    mapfile -t tests < <(awk '!/^#/ && NF {print "generic/" $1}' "$xfstests/tests/generic/group.list")
fi
[[ ${#tests[@]} -gt 0 ]] || { echo "no selected tests" >&2; exit 2; }
for test_id in "${tests[@]}"; do
    [[ $test_id =~ ^generic/[0-9]{3}$ && -f $xfstests/tests/$test_id ]] || { echo "invalid generic test: $test_id" >&2; exit 2; }
done
printf '%s\n' "${tests[@]}" >"$run/selection.txt"

excluded() {
    [[ -f $here/exclude ]] || return 1
    awk -v id="${1#generic/}" 'NF && $1 !~ /^#/ && ($1 == id || $1 == "generic/" id) {found=1} END {exit !found}' "$here/exclude"
}

recover_mount() {
    local target=$1 source=$2 mounted_source= mounted_target= mounted_fs= pidfile pid= i daemon=0 stat state
    local -a argv=()
    mounted_source= mounted_target= mounted_fs=
    read -r mounted_source mounted_target mounted_fs < <(findmnt -rn -o SOURCE,TARGET,FSTYPE --mountpoint "$target" 2>/dev/null || true) || true
    if [[ $mounted_target == "$target" && $mounted_source == "$source" && $mounted_fs == fuse.studiofs ]]; then
        timeout -k 2 5 umount "$target" >/dev/null 2>&1 ||
            timeout -k 2 5 fusermount3 -uz "$target" >/dev/null 2>&1 ||
            timeout -k 2 5 umount -l "$target" >/dev/null 2>&1 || true
    elif [[ $mounted_target == "$target" ]]; then
        echo "refusing recovery of a different mount at $target" >&2
        return 1
    fi
    pidfile="$XFSTESTS_RUN/mounts/$source.pid"
    if [[ -f $pidfile ]]; then
        read -r pid <"$pidfile" || true
        if [[ ${pid:-} =~ ^[0-9]+$ && -r /proc/$pid/cmdline ]]; then
            mapfile -d '' -t argv </proc/"$pid"/cmdline
            if [[ ${argv[0]:-} == "$STUDIOFS_BINARY" && ${argv[1]:-} == mount &&
                  ${argv[2]:-} == "$source" && ${argv[3]:-} == "$target" ]]; then
                daemon=1
                kill -TERM "$pid" 2>/dev/null || true
                for i in {1..50}; do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
                if kill -0 "$pid" 2>/dev/null; then
                    kill -KILL "$pid" 2>/dev/null || true
                    for i in {1..50}; do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
                fi
            fi
        fi
    fi
    read -r mounted_source mounted_target mounted_fs < <(findmnt -rn -o SOURCE,TARGET,FSTYPE --mountpoint "$target" 2>/dev/null || true) || true
    [[ $mounted_target == "$target" && ( $mounted_source != "$source" || $mounted_fs != fuse.studiofs ) ]] && return 1
    if [[ $mounted_target == "$target" ]]; then echo "recovery could not detach configured mount $target" >&2; return 1; fi
    if [[ $daemon -eq 1 && -r /proc/$pid/stat ]]; then
        stat=$(</proc/"$pid"/stat); state=${stat##*) }; state=${state%% *}
        [[ $state == Z ]] || { echo "recorded StudioFS daemon did not exit: $pid" >&2; return 1; }
    fi
}

failed=0
for test_id in "${tests[@]}"; do
    dir="$run/cases/${test_id##*/}"
    mkdir -p "$dir/results"
    if excluded "$test_id"; then
        printf '%s\t-\t0\texcluded\n' "$test_id" >&3
        continue
    fi
    start=$SECONDS
    set +e
    (cd "$xfstests" && RESULT_BASE="$dir/results" REPORT_DIR="$dir/results" timeout -k 10 "$timeout_s" ./check -R xunit -E "$here/exclude" "$test_id") >"$dir/check.log" 2>&1
    status=$?
    elapsed=$((SECONDS - start))
    if [[ $status -eq 124 || $status -eq 137 ]]; then
        verdict=timeout
    elif [[ -f $dir/results/result.xml ]]; then
        verdict=$(python3 - "$dir/results/result.xml" "$test_id" "$status" <<'PY'
import sys
import xml.etree.ElementTree as ET
try:
    root = ET.parse(sys.argv[1]).getroot()
    cases = [case for case in root.iter("testcase") if case.get("name") == sys.argv[2]]
    if len(cases) != 1:
        verdict = "harness"
    elif any(case.find("failure") is not None or case.find("error") is not None for case in cases):
        verdict = "fail"
    elif sys.argv[3] != "0":
        verdict = "harness"
    elif any(case.find("skipped") is not None for case in cases):
        verdict = "notrun"
    else:
        verdict = "pass"
except (ET.ParseError, OSError):
    verdict = "harness"
print(verdict)
PY
)
    else
        verdict=harness
    fi
    printf '%s\t%s\t%s\t%s\n' "$test_id" "$status" "$elapsed" "$verdict" >&3
    # A setup failure provides no testcase verdict; repair it before resuming.
    [[ $verdict != harness ]] || exit 1
    if [[ $verdict == timeout ]]; then
        recover_mount "$TEST_DIR" "$TEST_DEV" || exit 1
        recover_mount "$SCRATCH_MNT" "$SCRATCH_DEV" || exit 1
    fi
    [[ $verdict == pass || $verdict == notrun ]] || failed=1
done
exit "$failed"
