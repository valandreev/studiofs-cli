#!/bin/sh
set -eu

self_pid="${STUDIOFS_E2E_SELF_PID:-${STUDIOFS_E2E_BASELINE_SELF_PID:-0}}"
runner_root="${STUDIOFS_E2E_RUNNER_ROOT:-${STUDIOFS_E2E_BASELINE_ROOT:-}}"
clean_stale="${STUDIOFS_E2E_CLEAN_STALE_PROCESSES:-0}"

if [ -z "$runner_root" ]; then
    echo "STUDIOFS_E2E_RUNNER_ROOT is required" >&2
    exit 1
fi

ps -eo pid=,comm=,args= |
    awk -v self_pid="$self_pid" -v runner_root="$runner_root" '
        $1 != self_pid &&
        ($2 == "studiofs" || $2 == "studiofs-e2e" || $2 == "studiofs.exe" || $2 == "studiofs-e2e.exe") &&
        (index($0, runner_root) || index($0, "__mounted-chaos-op")) {
            print $1 "\t" $0
        }
    ' |
    while IFS="$(printf '\t')" read -r pid detail; do
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
