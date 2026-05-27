#!/bin/sh
set -eu

self_pid="${STUDIOFS_E2E_SELF_PID:-${STUDIOFS_E2E_BASELINE_SELF_PID:-0}}"
runner_root="${STUDIOFS_E2E_RUNNER_ROOT:-${STUDIOFS_E2E_BASELINE_ROOT:-}}"
clean_stale="${STUDIOFS_E2E_CLEAN_STALE_PROCESSES:-0}"

if [ -z "$runner_root" ]; then
    echo "STUDIOFS_E2E_RUNNER_ROOT is required" >&2
    exit 1
fi

for proc in /proc/[0-9]*; do
    [ -d "$proc" ] || continue
    pid="${proc##*/}"
    [ "$pid" != "$self_pid" ] || continue

    comm="$(cat "$proc/comm" 2>/dev/null || true)"
    case "$comm" in
        studiofs|studiofs-e2e|studiofs-e2e-hotspot-helper|studiofs.exe|studiofs-e2e.exe|studiofs-e2e-hotspot-helper.exe) ;;
        *) continue ;;
    esac

    cmdline="$(tr '\0' ' ' < "$proc/cmdline" 2>/dev/null || true)"
    case "$cmdline" in
        *"$runner_root"*|*"__mounted-chaos-op"*) ;;
        *) continue ;;
    esac

    detail="$pid $comm $cmdline"
        if [ "$clean_stale" = "1" ]; then
            if kill -TERM "$pid" 2>/dev/null; then
                sleep 1
                if kill -0 "$pid" 2>/dev/null; then
                    if kill -KILL "$pid" 2>/dev/null; then
                        echo "force-stopped $detail"
                    else
                        echo "failed to force-stop $detail"
                    fi
                else
                    echo "stopped $detail"
                fi
            else
                echo "failed to stop $detail"
            fi
        else
            echo "$detail"
        fi
done
