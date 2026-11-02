#!/usr/bin/python3
"""Kill surviving descendants of one xfstests case, identified by its environment token.

usage: reap_case.py TOKEN RECORD_FILE TIMEOUT_SECONDS

Test descendants can leave timeout's process group (fsstress setpgid, systemd
scopes), so membership is the inherited STUDIOFS_XFSTESTS_CASE=TOKEN entry.
Mount configuration comes from the runner environment. Exit 0 once no marked
process remains; exit 1 if one outlives the bound.
"""
import os
from pathlib import Path
import select
import signal
import sys
import time


def stat_fields(pid):
    """Fields after the parenthesized comm: [state, ppid, pgrp, session, ...]."""
    return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()


def mount_daemons(env):
    """PIDs of StudioFS daemons verified like recover_mount: pidfile plus exact argv."""
    binary = os.fsencode(env["STUDIOFS_BINARY"])
    daemons = set()
    for source, target in ((env["TEST_DEV"], env["TEST_DIR"]), (env["SCRATCH_DEV"], env["SCRATCH_MNT"])):
        try:
            pid = int((Path(env["XFSTESTS_RUN"]) / "mounts" / f"{source}.pid").read_text())
            argv = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        except (OSError, ValueError):
            continue
        if argv[:4] == [binary, b"mount", os.fsencode(source), os.fsencode(target)]:
            daemons.add(pid)
    return daemons


def case_process(pid, marker, daemons):
    """Return (argv, start ticks, open files) when pid belongs to the case and must die."""
    try:
        environ = Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
        argv = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        fields = stat_fields(pid)
    except (OSError, IndexError):
        return None
    if marker not in environ:
        return None
    # recover_mount owns verified daemons and their helpers (the fusermount3 auto_unmount watchdog).
    if pid in daemons or int(fields[1]) in daemons:
        return None
    files = []
    try:
        for fd in os.scandir(f"/proc/{pid}/fd"):
            target = os.readlink(fd.path)
            if target.startswith("/") and not target.startswith(("/dev/", "/proc/")):
                files.append(target)
    except OSError:
        pass
    return argv, fields[19], sorted(set(files))


def reap(token, record, timeout, env):
    marker = b"STUDIOFS_XFSTESTS_CASE=" + token.encode()
    daemons = mount_daemons(env)
    deadline = time.monotonic() + timeout
    signalled = {}
    with open(record, "a", encoding="utf-8") as out:
        while True:
            found = False
            for entry in os.listdir("/proc"):
                if not entry.isdigit() or int(entry) in signalled:
                    continue
                pid = int(entry)
                try:
                    fd = os.pidfd_open(pid)
                except OSError:
                    continue
                # The pidfd pins one process: a reused PID can never receive this signal.
                identity = case_process(pid, marker, daemons)
                if identity is None:
                    os.close(fd)
                    continue
                try:
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                except ProcessLookupError:
                    os.close(fd)
                    continue
                found = True
                signalled[pid] = fd
                argv, ticks, files = identity
                command = " ".join(arg.decode(errors="replace") for arg in argv if arg)
                out.write(f"{pid}\t{ticks}\t{command}\t{' '.join(files)}\n")
            out.flush()
            if not signalled and not found:
                return 0
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                print(f"testcase descendants survived SIGKILL: {sorted(signalled)}", file=sys.stderr)
                return 1
            # A pidfd becomes readable when its process exits; then rescan for late forks.
            poller = select.poll()
            for fd in signalled.values():
                poller.register(fd, select.POLLIN)
            ready = {fd for fd, _ in poller.poll(remaining * 1000)}
            for pid, fd in list(signalled.items()):
                if fd in ready:
                    os.close(fd)
                    del signalled[pid]


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    sys.exit(reap(sys.argv[1], sys.argv[2], float(sys.argv[3]), os.environ))
