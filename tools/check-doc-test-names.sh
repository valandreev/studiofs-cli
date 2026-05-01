#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd "$(dirname "$0")" && pwd)
CORE_SCRIPT="$SCRIPT_DIR/check-doc-test-names.py"

if command -v python3 >/dev/null 2>&1; then
    exec python3 "$CORE_SCRIPT" "$@"
fi

if command -v python >/dev/null 2>&1; then
    exec python "$CORE_SCRIPT" "$@"
fi

echo "Python 3 is required to run check-doc-test-names." >&2
exit 127
