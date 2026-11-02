#!/usr/bin/python3
"""Kill surviving descendants of one xfstests case, identified by its environment token.

usage: reap_case.py TOKEN STUDIOFS_BINARY RECORD_FILE TIMEOUT_SECONDS

Test descendants can leave timeout's process group (fsstress setpgid, systemd
scopes), so membership is the inherited STUDIOFS_XFSTESTS_CASE=TOKEN entry.
Exit 0 once no marked process remains; exit 1 if one outlives the bound.
"""
import os
from pathlib import Path
import select
import signal
import sys
import time


def marked_argv(pid, marker, binary):
    try:
        environ = Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
        argv = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    except OSError:
        return None
    if marker not in environ:
        return None
    # recover_mount owns StudioFS daemons; the TEST_DEV mount outlives its first case.
    if argv[:2] == [binary, b"mount"]:
        return None
    return argv


def start_ticks(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError):
        return "?"


def reap(token, binary, record, timeout):
    marker = b"STUDIOFS_XFSTESTS_CASE=" + token.encode()
    binary = os.fsencode(binary)
    deadline = time.monotonic() + timeout
    live = {}
    with open(record, "a", encoding="utf-8") as out:
        while True:
            found = False
            for entry in os.listdir("/proc"):
                if not entry.isdigit() or int(entry) in live:
                    continue
                pid = int(entry)
                try:
                    fd = os.pidfd_open(pid)
                except OSError:
                    continue
                # The pidfd pins one process: a reused PID can never receive this signal.
                argv = marked_argv(pid, marker, binary)
                if argv is None:
                    os.close(fd)
                    continue
                ticks = start_ticks(pid)
                try:
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                except ProcessLookupError:
                    os.close(fd)
                    continue
                found = True
                live[pid] = fd
                command = " ".join(arg.decode(errors="replace") for arg in argv if arg)
                out.write(f"{pid}\t{ticks}\t{command}\n")
            out.flush()
            if not live and not found:
                return 0
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                print(f"testcase descendants survived SIGKILL: {sorted(live)}", file=sys.stderr)
                return 1
            # A pidfd becomes readable when its process exits; then rescan for late forks.
            poller = select.poll()
            for fd in live.values():
                poller.register(fd, select.POLLIN)
            ready = {fd for fd, _ in poller.poll(remaining * 1000)}
            for pid, fd in list(live.items()):
                if fd in ready:
                    os.close(fd)
                    del live[pid]


if __name__ == "__main__":
    if len(sys.argv) != 5:
        sys.exit(__doc__)
    sys.exit(reap(sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])))
