#!/usr/bin/env python3
"""
studiofs-fstest — Multi-Client Filesystem Behavior Test Suite

Records filesystem behavior on shared/network volumes so we can capture a
reference profile (LucidLink, SMB, NFS) and later verify StudioFS matches.

Usage:
  python fstest.py /mnt/shared --role A --peers 2 [--fs-name LucidLink]
  python fstest.py /mnt/shared --role B --peers 2 [--fs-name LucidLink]

Run on N machines simultaneously. Coordination is done through the filesystem
itself (barrier files), so no TCP/UDP/SSH between peers is needed.

Requirements: Python 3.9+, no external packages.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import stat
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Optional

IS_WINDOWS = sys.platform == "win32"

if IS_WINDOWS:
    import msvcrt
else:
    import fcntl

# ─── Constants ────────────────────────────────────────────────────────────────

COORD_DIR = ".fstest"
BARRIER_TIMEOUT = 120  # seconds to wait for peers at barrier
VISIBILITY_POLL_INTERVAL = 0.05  # 50ms between polls
VISIBILITY_TIMEOUT = 30  # max seconds to wait for file to appear
DATA_BLOCK = b"STUDIOFS_TEST_" * 1000  # ~14KB test payload
SYNC_HOOK_TIMEOUT = 120  # seconds


# ─── Helpers ──────────────────────────────────────────────────────────────────

def now_ms() -> float:
    """Current time in milliseconds."""
    return time.monotonic() * 1000


def wall_ms() -> float:
    """Wall clock in milliseconds (for cross-machine rough timing)."""
    return time.time() * 1000


def safe_remove(path: Path):
    """Remove a file, ignoring errors."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def safe_rmdir(path: Path):
    """Remove a directory recursively, ignoring errors."""
    try:
        if path.is_dir():
            for child in sorted(path.rglob("*"), reverse=True):
                try:
                    if child.is_dir():
                        child.rmdir()
                    else:
                        child.unlink()
                except OSError:
                    pass
            try:
                path.rmdir()
            except OSError:
                pass
    except OSError:
        pass


def write_file(path: Path, data: bytes):
    """Write data to a file, creating parent dirs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def wait_for_path(path: Path, timeout: float = VISIBILITY_TIMEOUT) -> Optional[float]:
    """
    Poll until path exists. Returns delay in ms, or None if timed out.
    """
    start = now_ms()
    deadline = start + timeout * 1000
    while now_ms() < deadline:
        # Force directory refresh (especially needed for macOS + FUSE/SMB)
        try:
            if path.parent.exists():
                list(path.parent.iterdir())
        except OSError:
            pass
            
        if path.exists():
            return now_ms() - start
        time.sleep(VISIBILITY_POLL_INTERVAL)
    return None


def wait_for_path_gone(path: Path, timeout: float = VISIBILITY_TIMEOUT) -> Optional[float]:
    """Poll until path no longer exists. Returns delay in ms, or None if timed out."""
    start = now_ms()
    deadline = start + timeout * 1000
    while now_ms() < deadline:
        if not path.exists():
            return now_ms() - start
        time.sleep(VISIBILITY_POLL_INTERVAL)
    return None


def lock_file_exclusive(f):
    """Acquire an exclusive lock on an open file (cross-platform)."""
    if IS_WINDOWS:
        # Lock first byte
        msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def unlock_file(f):
    """Release lock on an open file (cross-platform)."""
    if IS_WINDOWS:
        try:
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
    else:
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def try_lock_file_exclusive(f) -> bool:
    """Try to acquire exclusive lock. Returns True if acquired."""
    try:
        lock_file_exclusive(f)
        return True
    except (OSError, IOError):
        return False


# ─── Barrier (cross-machine synchronization via filesystem) ───────────────────

class Barrier:
    """
    Synchronize N peers through the shared filesystem.

    Each peer writes a signal file, then polls until all peers' files appear.
    """

    def __init__(self, coord_dir: Path, role: str, peers: int):
        self.coord_dir = coord_dir
        self.role = role
        self.peers = peers
        self.peer_roles = [chr(ord("A") + i) for i in range(peers)]
        self.barrier_count = 0

    def wait(self, label: str = "") -> float:
        """
        Block until all peers reach this barrier.
        Returns time in ms spent waiting.
        """
        self.barrier_count += 1
        tag = f"barrier_{self.barrier_count}_{label}" if label else f"barrier_{self.barrier_count}"
        my_file = self.coord_dir / f"{tag}_{self.role}"

        # Signal readiness
        my_file.write_text(f"{wall_ms()}")

        # Wait for all peers
        start = now_ms()
        deadline = start + BARRIER_TIMEOUT * 1000
        while now_ms() < deadline:
            all_ready = all(
                (self.coord_dir / f"{tag}_{r}").exists()
                for r in self.peer_roles
            )
            if all_ready:
                elapsed = now_ms() - start
                # Small sleep to let filesystem sync settle
                time.sleep(0.1)
                return elapsed
            time.sleep(VISIBILITY_POLL_INTERVAL)

        missing = [
            r for r in self.peer_roles
            if not (self.coord_dir / f"{tag}_{r}").exists()
        ]
        raise TimeoutError(
            f"Barrier '{tag}' timed out after {BARRIER_TIMEOUT}s. "
            f"Missing peers: {missing}"
        )


# ─── Test Result ──────────────────────────────────────────────────────────────

@dataclass
class TestResult:
    name: str
    passed: bool
    duration_ms: float = 0
    visibility_delay_ms: Optional[float] = None
    error: Optional[str] = None
    notes: str = ""
    details: dict = field(default_factory=dict)


# ─── Test Context ─────────────────────────────────────────────────────────────

@dataclass
class TestCtx:
    mount: Path
    role: str
    peers: int
    fs_name: str
    barrier: Barrier
    work_dir: Path  # per-run temporary work directory under mount
    consistency_model: str = "direct"
    sync_cmd_template: Optional[str] = None
    sync_settle_secs: float = 0.5
    probe_rounds: int = 30

    @property
    def is_a(self) -> bool:
        return self.role == "A"

    @property
    def is_b(self) -> bool:
        return self.role == "B"


def run_sync_hook(ctx: TestCtx, phase: str) -> dict[str, Any]:
    """
    Execute optional external sync command.
    Template placeholders:
      {role} {phase} {mount} {work_dir} {fs_name}
    """
    if not ctx.sync_cmd_template:
        time.sleep(ctx.sync_settle_secs)
        return {"executed": False, "reason": "no sync command configured"}

    cmd = ctx.sync_cmd_template.format(
        role=ctx.role,
        phase=phase,
        mount=str(ctx.mount),
        work_dir=str(ctx.work_dir),
        fs_name=ctx.fs_name,
    )
    started = now_ms()
    proc = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        timeout=SYNC_HOOK_TIMEOUT,
    )
    elapsed = now_ms() - started
    return {
        "executed": True,
        "phase": phase,
        "cmd": cmd,
        "returncode": proc.returncode,
        "duration_ms": round(elapsed, 1),
        "stdout": proc.stdout[-1000:],
        "stderr": proc.stderr[-1000:],
        "ok": proc.returncode == 0,
    }


def percentile(values: list[float], p: float) -> Optional[float]:
    """Return percentile value for p in [0, 100]."""
    if not values:
        return None
    if p <= 0:
        return min(values)
    if p >= 100:
        return max(values)
    ordered = sorted(values)
    idx = (len(ordered) - 1) * (p / 100.0)
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    frac = idx - lo
    return ordered[lo] + (ordered[hi] - ordered[lo]) * frac


# ═════════════════════════════════════════════════════════════════════════════
# SINGLE-CLIENT TESTS (run by role A only, B waits)
# ═════════════════════════════════════════════════════════════════════════════

def test_basic_crud(ctx: TestCtx) -> TestResult:
    """Create, read, stat, overwrite, rename, delete a file."""
    d = ctx.work_dir / "crud"
    d.mkdir(parents=True, exist_ok=True)
    f = d / "hello.txt"
    start = now_ms()

    # Create + write
    f.write_bytes(b"hello world")
    assert f.exists(), "file should exist after create"
    assert f.read_bytes() == b"hello world", "read should return written data"

    # Stat
    st = f.stat()
    assert st.st_size == 11, f"size should be 11, got {st.st_size}"

    # Overwrite
    f.write_bytes(b"overwritten!")
    assert f.read_bytes() == b"overwritten!", "overwrite should update content"

    # Rename
    f2 = d / "renamed.txt"
    f.rename(f2)
    assert f2.exists(), "renamed file should exist"
    assert not f.exists(), "original should not exist after rename"

    # Delete
    f2.unlink()
    assert not f2.exists(), "file should not exist after delete"

    # mkdir / rmdir
    sub = d / "subdir"
    sub.mkdir()
    assert sub.is_dir(), "subdir should be a directory"
    sub.rmdir()
    assert not sub.exists(), "subdir should not exist after rmdir"

    return TestResult("basic_crud", True, now_ms() - start)


def test_deep_path(ctx: TestCtx) -> TestResult:
    """Create deeply nested directory structure."""
    start = now_ms()
    deep = ctx.work_dir / "deep" / "a" / "b" / "c" / "d" / "e"
    deep.mkdir(parents=True, exist_ok=True)
    f = deep / "leaf.txt"
    f.write_bytes(b"deep")
    assert f.read_bytes() == b"deep"
    return TestResult("deep_path", True, now_ms() - start)


def test_large_directory(ctx: TestCtx) -> TestResult:
    """Create 1000 files, readdir, verify count."""
    start = now_ms()
    d = ctx.work_dir / "largedir"
    d.mkdir(parents=True, exist_ok=True)
    count = 1000

    for i in range(count):
        (d / f"file_{i:04d}.dat").write_bytes(f"data_{i}".encode())

    entries = list(d.iterdir())
    actual = len(entries)

    elapsed = now_ms() - start
    return TestResult(
        "large_directory", actual == count, elapsed,
        notes=f"created={count}, readdir={actual}",
        details={"created": count, "readdir_count": actual},
    )


def test_special_filenames(ctx: TestCtx) -> TestResult:
    """Test filenames with spaces, unicode, long names."""
    start = now_ms()
    d = ctx.work_dir / "names"
    d.mkdir(parents=True, exist_ok=True)
    cases = {
        "spaces": "file with spaces.txt",
        "unicode": "файл_тест_日本語.txt",
        "long": "a" * 200 + ".txt",
        "dots": "file...multiple...dots.txt",
        "dash_start": "-dashstart.txt",
    }
    results = {}
    for label, name in cases.items():
        try:
            p = d / name
            p.write_bytes(b"test")
            exists = p.exists()
            content_ok = p.read_bytes() == b"test" if exists else False
            results[label] = {"exists": exists, "content_ok": content_ok}
        except OSError as e:
            results[label] = {"error": str(e)}

    all_ok = all(
        r.get("content_ok", False) for r in results.values()
    )
    return TestResult(
        "special_filenames", all_ok, now_ms() - start,
        details=results,
    )


# ═════════════════════════════════════════════════════════════════════════════
# MULTI-CLIENT TESTS
# ═════════════════════════════════════════════════════════════════════════════

def test_cross_dir_visibility(ctx: TestCtx) -> TestResult:
    """
    A writes files in dir_A/, B writes files in dir_B/.
    After barrier, each checks the other's directory.
    """
    my_dir = ctx.work_dir / f"crossdir_{ctx.role}"
    peer_role = "B" if ctx.is_a else "A"
    peer_dir = ctx.work_dir / f"crossdir_{peer_role}"

    # Each client creates 10 files in their own directory
    my_dir.mkdir(parents=True, exist_ok=True)
    for i in range(10):
        (my_dir / f"file_{i:03d}.dat").write_bytes(DATA_BLOCK)

    # Barrier: both done writing
    ctx.barrier.wait("crossdir_written")

    # Measure visibility of peer's files
    start = now_ms()
    delays = []
    missing = []
    for i in range(10):
        target = peer_dir / f"file_{i:03d}.dat"
        delay = wait_for_path(target)
        if delay is not None:
            delays.append(delay)
        else:
            missing.append(f"file_{i:03d}.dat")

    found = 10 - len(missing)
    avg_delay = sum(delays) / len(delays) if delays else 0
    max_delay = max(delays) if delays else 0

    return TestResult(
        "cross_dir_visibility",
        found == 10,
        now_ms() - start,
        visibility_delay_ms=avg_delay,
        notes=f"found={found}/10, avg_delay={avg_delay:.0f}ms, max={max_delay:.0f}ms",
        details={
            "found": found,
            "missing": missing,
            "avg_delay_ms": round(avg_delay, 1),
            "max_delay_ms": round(max_delay, 1),
            "delays_ms": [round(d, 1) for d in delays],
        },
    )


def test_same_dir_different_files(ctx: TestCtx) -> TestResult:
    """
    Both clients create different files in the SAME directory.
    Verify all files appear for both.
    """
    shared_dir = ctx.work_dir / "samedir"
    shared_dir.mkdir(parents=True, exist_ok=True)

    # Barrier: dir ready
    ctx.barrier.wait("samedir_ready")

    # Each creates 10 files with role-prefixed names
    my_files = []
    for i in range(10):
        name = f"{ctx.role}_file_{i:03d}.dat"
        p = shared_dir / name
        p.write_bytes(DATA_BLOCK)
        my_files.append(name)

    # Barrier: both done writing
    ctx.barrier.wait("samedir_written")

    # Check all 20 files exist
    time.sleep(2)  # give filesystem time to propagate
    all_entries = set(e.name for e in shared_dir.iterdir() if not e.name.startswith("."))
    expected_peer = "B" if ctx.is_a else "A"
    peer_files = [f"{expected_peer}_file_{i:03d}.dat" for i in range(10)]

    my_found = sum(1 for f in my_files if f in all_entries)
    peer_found = sum(1 for f in peer_files if f in all_entries)

    # Also verify content of peer files
    content_ok = 0
    for name in peer_files:
        p = shared_dir / name
        if p.exists():
            try:
                data = p.read_bytes()
                if data == DATA_BLOCK:
                    content_ok += 1
            except OSError:
                pass

    return TestResult(
        "same_dir_different_files",
        my_found == 10 and peer_found == 10,
        notes=f"my={my_found}/10, peer={peer_found}/10, content_ok={content_ok}/10",
        details={
            "my_found": my_found,
            "peer_found": peer_found,
            "peer_content_ok": content_ok,
            "total_entries": len(all_entries),
        },
    )


def test_concurrent_create_same_name(ctx: TestCtx) -> TestResult:
    """
    Both clients try to create a file with the SAME name simultaneously.
    Exactly one should win. The other should either get an error or see
    the winner's content.
    """
    d = ctx.work_dir / "samename"
    d.mkdir(parents=True, exist_ok=True)
    target = d / "contested.txt"
    my_content = f"written_by_{ctx.role}_{wall_ms()}".encode()

    # Barrier: both ready
    ctx.barrier.wait("samename_ready")

    # Both write simultaneously
    wrote_ok = False
    write_error = None
    try:
        target.write_bytes(my_content)
        wrote_ok = True
    except OSError as e:
        write_error = str(e)

    # Barrier: both attempted
    ctx.barrier.wait("samename_attempted")
    time.sleep(2)  # settle

    # Read final state
    final_content = None
    try:
        final_content = target.read_bytes()
    except OSError:
        pass

    i_won = final_content == my_content if final_content else False

    return TestResult(
        "concurrent_create_same_name",
        True,  # always passes — we're observing behavior
        notes=f"wrote_ok={wrote_ok}, i_won={i_won}, error={write_error}",
        details={
            "wrote_ok": wrote_ok,
            "write_error": write_error,
            "i_am_winner": i_won,
            "final_content_mine": i_won,
            "final_content_preview": final_content[:100].decode(errors="replace") if final_content else None,
        },
    )


def test_same_file_overwrite(ctx: TestCtx) -> TestResult:
    """
    A creates a file, barrier, then both overwrite it with different content.
    Final state should be one of the two versions (LWW).
    """
    d = ctx.work_dir / "overwrite"
    d.mkdir(parents=True, exist_ok=True)
    target = d / "shared.bin"

    # A creates the initial file
    if ctx.is_a:
        target.write_bytes(b"initial_content")

    ctx.barrier.wait("overwrite_setup")

    # Both overwrite
    my_data = (f"overwrite_by_{ctx.role}_" * 100).encode()
    target.write_bytes(my_data)

    ctx.barrier.wait("overwrite_done")
    time.sleep(2)

    # Read final state
    final = target.read_bytes()
    i_won = final == my_data

    return TestResult(
        "same_file_overwrite",
        True,  # observational
        notes=f"i_won={i_won}, final_len={len(final)}",
        details={
            "i_am_winner": i_won,
            "final_size": len(final),
            "final_preview": final[:80].decode(errors="replace"),
        },
    )


def test_visibility_timing(ctx: TestCtx) -> TestResult:
    """
    Measure how long it takes for a file written by one client to appear
    on another. A writes, B measures delay (and vice versa).
    """
    d = ctx.work_dir / "timing"
    d.mkdir(parents=True, exist_ok=True)

    ctx.barrier.wait("timing_ready")

    delays = []
    rounds = 5

    for i in range(rounds):
        writer = "A" if (i % 2 == 0) else "B"
        fname = f"timing_{i:03d}_{writer}.dat"
        fpath = d / fname

        if ctx.role == writer:
            # I write
            time.sleep(0.2)  # small stagger
            fpath.write_bytes(f"round_{i}_by_{writer}".encode())
        else:
            # I measure
            delay = wait_for_path(fpath, timeout=VISIBILITY_TIMEOUT)
            if delay is not None:
                delays.append(delay)
            else:
                delays.append(-1)  # timeout

        ctx.barrier.wait(f"timing_round_{i}")

    valid_delays = [d for d in delays if d >= 0]
    avg = sum(valid_delays) / len(valid_delays) if valid_delays else -1
    timeouts = sum(1 for d in delays if d < 0)

    return TestResult(
        "visibility_timing",
        timeouts == 0,
        visibility_delay_ms=avg,
        notes=f"avg={avg:.0f}ms, timeouts={timeouts}/{len(delays)}",
        details={
            "delays_ms": [round(d, 1) for d in delays],
            "avg_ms": round(avg, 1),
            "timeouts": timeouts,
        },
    )


def test_delete_while_reading(ctx: TestCtx) -> TestResult:
    """
    A opens a file and holds it open. B deletes it.
    Observe: can A continue reading? Does B's delete succeed?
    """
    d = ctx.work_dir / "delread"
    d.mkdir(parents=True, exist_ok=True)
    target = d / "victim.dat"
    large_data = b"X" * (1024 * 1024)  # 1MB

    # A creates the file
    if ctx.is_a:
        target.write_bytes(large_data)

    ctx.barrier.wait("delread_created")

    if ctx.is_a:
        # A opens and holds file handle
        read_ok = False
        read_after_delete_ok = False
        read_error = None
        try:
            with open(target, "rb") as f:
                # Read first chunk to confirm
                chunk = f.read(1024)
                read_ok = len(chunk) == 1024

                # Signal B to delete
                (d / "a_holding").write_text("1")
                ctx.barrier.wait("delread_a_holding")

                # Wait for B to attempt delete
                ctx.barrier.wait("delread_b_deleted")
                time.sleep(1)

                # Try to continue reading with open handle
                try:
                    f.seek(0)
                    all_data = f.read()
                    read_after_delete_ok = len(all_data) > 0
                except OSError as e:
                    read_error = str(e)
        except OSError as e:
            read_error = str(e)

        return TestResult(
            "delete_while_reading",
            True,  # observational
            notes=f"read_ok={read_ok}, read_after_delete={read_after_delete_ok}, err={read_error}",
            details={
                "initial_read_ok": read_ok,
                "read_after_delete_ok": read_after_delete_ok,
                "read_error": read_error,
            },
        )
    else:
        # B waits for A to open, then deletes
        ctx.barrier.wait("delread_a_holding")

        delete_ok = False
        delete_error = None
        try:
            target.unlink()
            delete_ok = True
        except OSError as e:
            delete_error = str(e)

        ctx.barrier.wait("delread_b_deleted")

        return TestResult(
            "delete_while_reading",
            True,  # observational
            notes=f"delete_ok={delete_ok}, error={delete_error}",
            details={
                "delete_ok": delete_ok,
                "delete_error": delete_error,
                "file_exists_after": target.exists(),
            },
        )


def test_rmdir_with_locked_file(ctx: TestCtx) -> TestResult:
    """
    A locks a file in dir/. B tries to rm -rf dir/.
    Expected: B can delete unlocked files but not the locked one.
    """
    d = ctx.work_dir / "lockdir" / "target"
    d.mkdir(parents=True, exist_ok=True)

    # Create test files (by A)
    locked_file = d / "locked.dat"
    free_file1 = d / "free1.dat"
    free_file2 = d / "free2.dat"

    if ctx.is_a:
        locked_file.write_bytes(b"locked content")
        free_file1.write_bytes(b"free1")
        free_file2.write_bytes(b"free2")

    ctx.barrier.wait("lockdir_created")

    if ctx.is_a:
        # A locks the file
        lock_acquired = False
        results = {}
        try:
            f = open(locked_file, "rb")
            lock_acquired = try_lock_file_exclusive(f)

            results["lock_acquired"] = lock_acquired
            ctx.barrier.wait("lockdir_a_locked")

            # Wait for B to attempt removal
            ctx.barrier.wait("lockdir_b_done")
            time.sleep(1)

            # Check state
            results["locked_file_exists"] = locked_file.exists()
            results["free1_exists"] = free_file1.exists()
            results["free2_exists"] = free_file2.exists()
            results["dir_exists"] = d.exists()

            # Release lock
            if lock_acquired:
                unlock_file(f)
            f.close()
        except OSError as e:
            results["error"] = str(e)

        return TestResult(
            "rmdir_with_locked_file",
            True,  # observational
            details=results,
            notes=f"lock={lock_acquired}, locked_exists={results.get('locked_file_exists')}",
        )
    else:
        ctx.barrier.wait("lockdir_a_locked")

        # B tries to rm -rf
        results = {}
        for fname in ["free1.dat", "free2.dat"]:
            p = d / fname
            try:
                p.unlink()
                results[f"delete_{fname}"] = "ok"
            except OSError as e:
                results[f"delete_{fname}"] = str(e)

        # Try to delete locked file
        try:
            locked_file.unlink()
            results["delete_locked.dat"] = "ok"
        except OSError as e:
            results["delete_locked.dat"] = str(e)

        # Try rmdir
        try:
            d.rmdir()
            results["rmdir"] = "ok"
        except OSError as e:
            results["rmdir"] = str(e)

        ctx.barrier.wait("lockdir_b_done")

        return TestResult(
            "rmdir_with_locked_file",
            True,  # observational
            details=results,
        )


def test_concurrent_rename(ctx: TestCtx) -> TestResult:
    """
    Both clients try to rename the same file to different destinations.
    Exactly one should succeed.
    """
    d = ctx.work_dir / "rename_race"
    d.mkdir(parents=True, exist_ok=True)
    src = d / "original.txt"
    dst_a = d / "moved_by_A.txt"
    dst_b = d / "moved_by_B.txt"
    my_dst = dst_a if ctx.is_a else dst_b

    # A creates the file
    if ctx.is_a:
        src.write_bytes(b"rename me")

    ctx.barrier.wait("rename_created")

    # Both try to rename
    rename_ok = False
    rename_error = None
    try:
        src.rename(my_dst)
        rename_ok = True
    except OSError as e:
        rename_error = str(e)

    ctx.barrier.wait("rename_attempted")
    time.sleep(2)

    # Check final state
    state = {
        "original_exists": src.exists(),
        "moved_by_A_exists": dst_a.exists(),
        "moved_by_B_exists": dst_b.exists(),
    }

    return TestResult(
        "concurrent_rename",
        True,  # observational
        notes=f"rename_ok={rename_ok}, err={rename_error}",
        details={
            "rename_ok": rename_ok,
            "rename_error": rename_error,
            **state,
        },
    )


def test_readdir_during_modifications(ctx: TestCtx) -> TestResult:
    """
    A continuously creates files while B does readdir.
    Measure consistency of readdir results.
    """
    d = ctx.work_dir / "readdir_mod"
    d.mkdir(parents=True, exist_ok=True)

    ctx.barrier.wait("readdir_mod_ready")

    if ctx.is_a:
        # A creates 100 files one by one
        for i in range(100):
            (d / f"stream_{i:04d}.dat").write_bytes(f"data_{i}".encode())
            time.sleep(0.01)  # 10ms between creates

        ctx.barrier.wait("readdir_mod_done")

        return TestResult("readdir_during_modifications", True,
                          notes="writer: created 100 files")
    else:
        # B does repeated readdirs and records results
        snapshots = []
        start = now_ms()
        while now_ms() - start < 5000:  # 5 seconds of polling
            try:
                entries = sorted(e.name for e in d.iterdir()
                                 if e.name.startswith("stream_"))
                snapshots.append({"t_ms": round(now_ms() - start), "count": len(entries)})
            except OSError:
                pass
            time.sleep(0.1)

        ctx.barrier.wait("readdir_mod_done")

        # Final readdir
        time.sleep(2)
        final_entries = sorted(e.name for e in d.iterdir()
                               if e.name.startswith("stream_"))

        return TestResult(
            "readdir_during_modifications",
            len(final_entries) == 100,
            notes=f"final_count={len(final_entries)}/100, snapshots={len(snapshots)}",
            details={
                "snapshots": snapshots[-10:],  # last 10
                "final_count": len(final_entries),
                "snapshot_count": len(snapshots),
            },
        )


def test_file_lock_semantics(ctx: TestCtx) -> TestResult:
    """
    A acquires flock, B tries to acquire same lock.
    Observe: does B block? get error? timeout?
    """
    d = ctx.work_dir / "flock"
    d.mkdir(parents=True, exist_ok=True)
    target = d / "lockme.dat"

    if ctx.is_a:
        target.write_bytes(b"lock target")

    ctx.barrier.wait("flock_created")

    if ctx.is_a:
        # A locks exclusively
        f = open(target, "rb")
        acquired = try_lock_file_exclusive(f)

        ctx.barrier.wait("flock_a_locked")
        # Hold lock for 5 seconds
        time.sleep(5)
        if acquired:
            unlock_file(f)
        f.close()
        (d / "a_released").write_text("1")

        ctx.barrier.wait("flock_done")
        return TestResult("file_lock_semantics", True,
                          notes=f"lock_acquired={acquired}",
                          details={"acquired": acquired})
    else:
        ctx.barrier.wait("flock_a_locked")

        # B tries non-blocking lock
        f = open(target, "rb")
        b_got_lock = try_lock_file_exclusive(f)
        if b_got_lock:
            unlock_file(f)
        f.close()

        # Wait for A to release, try again
        wait_for_path(d / "a_released")
        time.sleep(0.5)

        f = open(target, "rb")
        b_got_lock_after = try_lock_file_exclusive(f)
        if b_got_lock_after:
            unlock_file(f)
        f.close()

        ctx.barrier.wait("flock_done")
        return TestResult(
            "file_lock_semantics",
            True,  # observational
            notes=f"lock_while_held={b_got_lock}, lock_after_release={b_got_lock_after}",
            details={
                "lock_while_held": b_got_lock,
                "lock_after_release": b_got_lock_after,
            },
        )


def test_hardlinks(ctx: TestCtx) -> TestResult:
    """Test hardlink creation and visibility across clients."""
    d = ctx.work_dir / "hardlinks"
    d.mkdir(parents=True, exist_ok=True)
    original = d / "original.txt"
    link = d / "link.txt"

    if ctx.is_a:
        original.write_bytes(b"hardlink test data")
        try:
            os.link(str(original), str(link))
            nlink = original.stat().st_nlink
            create_ok = True
            error = None
        except OSError as e:
            create_ok = False
            nlink = 0
            error = str(e)

        ctx.barrier.wait("hardlink_created")
        ctx.barrier.wait("hardlink_verified")

        return TestResult(
            "hardlinks",
            create_ok,
            notes=f"create_ok={create_ok}, nlink={nlink}, err={error}",
            details={"create_ok": create_ok, "nlink": nlink, "error": error},
        )
    else:
        ctx.barrier.wait("hardlink_created")
        time.sleep(2)

        orig_exists = original.exists()
        link_exists = link.exists()
        same_ino = False
        if orig_exists and link_exists:
            same_ino = original.stat().st_ino == link.stat().st_ino
        content_ok = False
        if link_exists:
            try:
                content_ok = link.read_bytes() == b"hardlink test data"
            except OSError:
                pass

        ctx.barrier.wait("hardlink_verified")
        return TestResult(
            "hardlinks",
            orig_exists and link_exists,
            notes=f"orig={orig_exists}, link={link_exists}, same_ino={same_ino}, content={content_ok}",
            details={
                "original_visible": orig_exists,
                "link_visible": link_exists,
                "same_inode": same_ino,
                "content_ok": content_ok,
            },
        )


def test_symlinks(ctx: TestCtx) -> TestResult:
    """Test symlink creation and resolution across clients."""
    d = ctx.work_dir / "symlinks"
    d.mkdir(parents=True, exist_ok=True)
    real = d / "real.txt"
    sym = d / "sym.txt"

    if ctx.is_a:
        real.write_bytes(b"symlink test data")
        error = None
        try:
            sym.symlink_to(real.name)  # relative symlink
            create_ok = True
        except OSError as e:
            create_ok = False
            error = str(e)

        ctx.barrier.wait("symlink_created")
        ctx.barrier.wait("symlink_verified")
        return TestResult(
            "symlinks", create_ok,
            notes=f"create_ok={create_ok}, err={error}",
            details={"create_ok": create_ok, "error": error},
        )
    else:
        ctx.barrier.wait("symlink_created")
        time.sleep(2)

        sym_exists = sym.exists()
        is_symlink = sym.is_symlink() if sym_exists or sym.is_symlink() else False
        content_ok = False
        target = None
        try:
            if sym.is_symlink():
                target = os.readlink(str(sym))
            content_ok = sym.read_bytes() == b"symlink test data"
        except OSError:
            pass

        ctx.barrier.wait("symlink_verified")
        return TestResult(
            "symlinks",
            is_symlink and content_ok,
            notes=f"is_symlink={is_symlink}, target={target}, content={content_ok}",
            details={
                "sym_exists": sym_exists,
                "is_symlink": is_symlink,
                "target": target,
                "content_ok": content_ok,
            },
        )


def test_delete_and_recreate(ctx: TestCtx) -> TestResult:
    """
    A creates file, B deletes it, A recreates it.
    Verify final state is consistent.
    """
    d = ctx.work_dir / "delete_recreate"
    d.mkdir(parents=True, exist_ok=True)
    target = d / "phoenix.txt"

    if ctx.is_a:
        target.write_bytes(b"version_1")

    ctx.barrier.wait("dr_v1_created")

    if ctx.is_b:
        try:
            target.unlink()
        except OSError:
            pass

    ctx.barrier.wait("dr_deleted")
    time.sleep(1)

    if ctx.is_a:
        target.write_bytes(b"version_2")

    ctx.barrier.wait("dr_v2_created")
    time.sleep(2)

    exists = target.exists()
    content = None
    try:
        content = target.read_bytes().decode(errors="replace")
    except OSError:
        pass

    return TestResult(
        "delete_and_recreate",
        exists and content == "version_2",
        notes=f"exists={exists}, content={content}",
        details={"exists": exists, "content": content},
    )


def test_rapid_create_delete(ctx: TestCtx) -> TestResult:
    """
    Both clients rapidly create and delete files in the same directory.
    After settling, directory should be clean.
    """
    d = ctx.work_dir / "rapid"
    d.mkdir(parents=True, exist_ok=True)

    ctx.barrier.wait("rapid_ready")

    # Each client creates and deletes 50 files
    for i in range(50):
        fname = f"{ctx.role}_{i:03d}.tmp"
        p = d / fname
        try:
            p.write_bytes(b"tmp")
            p.unlink()
        except OSError:
            pass

    ctx.barrier.wait("rapid_done")
    time.sleep(3)

    # Check: directory should be empty (or near-empty)
    remaining = [e.name for e in d.iterdir() if not e.name.startswith(".")]

    return TestResult(
        "rapid_create_delete",
        len(remaining) == 0,
        notes=f"remaining={len(remaining)} files",
        details={"remaining_files": remaining},
    )


def test_conflict_contract_probe(ctx: TestCtx) -> TestResult:
    """
    Single probe test that extracts conflict/convergence metrics:
    - visibility delay (A->B and B->A)
    - overwrite winner distribution
    - rename winner distribution
    - delete-while-open behavior
    """
    rounds = max(1, int(ctx.probe_rounds))
    vis_a_to_b: list[float] = []
    vis_b_to_a: list[float] = []
    overwrite_winners = {"A": 0, "B": 0, "other": 0}
    rename_winners = {"A": 0, "B": 0, "none_or_both": 0}
    delete_attempts = 0
    delete_success = 0
    holder_read_after_delete_ok = 0

    base = ctx.work_dir / "probe"
    base.mkdir(parents=True, exist_ok=True)

    for i in range(rounds):
        # 1) Visibility round-trip, alternating writer.
        vis_dir = base / "visibility"
        vis_dir.mkdir(parents=True, exist_ok=True)
        writer = "A" if i % 2 == 0 else "B"
        vis_file = vis_dir / f"v_{i:03d}_{writer}.txt"
        vis_payload = f"vis_round={i};writer={writer};ts={wall_ms()}".encode()

        ctx.barrier.wait(f"probe_vis_ready_{i}")
        if ctx.role == writer:
            vis_file.write_bytes(vis_payload)
        else:
            d = wait_for_path(vis_file, timeout=VISIBILITY_TIMEOUT)
            if d is not None:
                if writer == "A" and ctx.is_b:
                    vis_a_to_b.append(d)
                elif writer == "B" and ctx.is_a:
                    vis_b_to_a.append(d)
        ctx.barrier.wait(f"probe_vis_done_{i}")

        # 2) Concurrent overwrite winner.
        ow_dir = base / "overwrite"
        ow_dir.mkdir(parents=True, exist_ok=True)
        ow_file = ow_dir / f"shared_{i:03d}.txt"
        if ctx.is_a:
            ow_file.write_bytes(b"seed")
        ctx.barrier.wait(f"probe_ow_seed_{i}")

        my_ow_payload = f"ow_round={i};writer={ctx.role};ts={wall_ms()}".encode()
        ow_file.write_bytes(my_ow_payload)
        ctx.barrier.wait(f"probe_ow_write_{i}")
        time.sleep(0.2)
        final_ow = ow_file.read_bytes() if ow_file.exists() else b""
        winner = "other"
        if b"writer=A" in final_ow:
            winner = "A"
        elif b"writer=B" in final_ow:
            winner = "B"
        overwrite_winners[winner] += 1
        ctx.barrier.wait(f"probe_ow_done_{i}")

        # 3) Concurrent rename winner.
        rn_dir = base / "rename"
        rn_dir.mkdir(parents=True, exist_ok=True)
        src = rn_dir / f"src_{i:03d}.txt"
        dst_a = rn_dir / f"dst_a_{i:03d}.txt"
        dst_b = rn_dir / f"dst_b_{i:03d}.txt"
        if ctx.is_a:
            src.write_bytes(f"rename_round={i}".encode())
            safe_remove(dst_a)
            safe_remove(dst_b)
        ctx.barrier.wait(f"probe_rn_seed_{i}")
        try:
            src.rename(dst_a if ctx.is_a else dst_b)
        except OSError:
            pass
        ctx.barrier.wait(f"probe_rn_renamed_{i}")
        a_exists = dst_a.exists()
        b_exists = dst_b.exists()
        if a_exists and not b_exists:
            rename_winners["A"] += 1
        elif b_exists and not a_exists:
            rename_winners["B"] += 1
        else:
            rename_winners["none_or_both"] += 1
        ctx.barrier.wait(f"probe_rn_done_{i}")

        # 4) Delete while open.
        dr_dir = base / "delete_open"
        dr_dir.mkdir(parents=True, exist_ok=True)
        dr_file = dr_dir / f"f_{i:03d}.bin"
        if ctx.is_a:
            dr_file.write_bytes(b"X" * 4096)
        ctx.barrier.wait(f"probe_del_seed_{i}")

        if ctx.is_a:
            ok = 0
            try:
                with open(dr_file, "rb") as f:
                    _ = f.read(64)
                    ctx.barrier.wait(f"probe_del_hold_{i}")
                    ctx.barrier.wait(f"probe_del_try_done_{i}")
                    try:
                        f.seek(0)
                        ok = 1 if len(f.read(64)) == 64 else 0
                    except OSError:
                        ok = 0
            except OSError:
                ok = 0
            holder_read_after_delete_ok += ok
        else:
            ctx.barrier.wait(f"probe_del_hold_{i}")
            delete_attempts += 1
            try:
                dr_file.unlink()
                delete_success += 1
            except OSError:
                pass
            ctx.barrier.wait(f"probe_del_try_done_{i}")
        ctx.barrier.wait(f"probe_del_done_{i}")

    def vis_stats(vals: list[float]) -> dict[str, Any]:
        if not vals:
            return {"n": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None, "max_ms": None}
        return {
            "n": len(vals),
            "p50_ms": round(percentile(vals, 50) or 0, 2),
            "p95_ms": round(percentile(vals, 95) or 0, 2),
            "p99_ms": round(percentile(vals, 99) or 0, 2),
            "max_ms": round(max(vals), 2),
        }

    details = {
        "rounds": rounds,
        "visibility": {
            "a_to_b": vis_stats(vis_a_to_b),
            "b_to_a": vis_stats(vis_b_to_a),
        },
        "overwrite_winners": overwrite_winners,
        "rename_winners": rename_winners,
        "delete_while_open": {
            "deleter_attempts": delete_attempts,
            "deleter_success": delete_success,
            "holder_read_after_delete_ok": holder_read_after_delete_ok,
        },
    }
    return TestResult(
        "conflict_contract_probe",
        True,
        notes=f"rounds={rounds}",
        details=details,
    )


def test_eventual_visibility_ordered_sync(ctx: TestCtx) -> TestResult:
    """
    Eventual model:
      A writes local change -> A sync -> B sync -> B must observe change.
    """
    d = ctx.work_dir / "eventual_visibility"
    d.mkdir(parents=True, exist_ok=True)
    target = d / "from_a.txt"
    payload = f"eventual_{wall_ms()}".encode()
    details: dict[str, Any] = {}

    ctx.barrier.wait("ev_vis_ready")
    if ctx.is_a:
        target.write_bytes(payload)
    ctx.barrier.wait("ev_vis_written")

    immediate_visible = target.exists() if ctx.is_b else None
    if ctx.is_b:
        details["visible_before_sync"] = immediate_visible

    if ctx.is_a:
        details["a_flush"] = run_sync_hook(ctx, "flush_a_visibility")
    ctx.barrier.wait("ev_vis_after_a_flush")

    if ctx.is_b:
        details["b_pull"] = run_sync_hook(ctx, "pull_b_visibility")
    ctx.barrier.wait("ev_vis_after_b_pull")

    delay = wait_for_path(target, timeout=VISIBILITY_TIMEOUT)
    final_bytes = target.read_bytes() if target.exists() else None
    content_ok = final_bytes == payload
    passed = delay is not None and content_ok

    details["visible_after_sync"] = target.exists()
    details["content_ok"] = content_ok
    details["final_preview"] = (
        final_bytes[:120].decode(errors="replace") if final_bytes is not None else None
    )
    return TestResult(
        "eventual_visibility_ordered_sync",
        passed,
        visibility_delay_ms=delay,
        notes=(
            f"before_sync={immediate_visible}, after_sync={details['visible_after_sync']}, "
            f"content_ok={content_ok}"
        ),
        details=details,
    )


def test_eventual_same_file_ordered_flush(ctx: TestCtx) -> TestResult:
    """
    Eventual model:
      A and B mutate same file locally.
      Flush order is A then B; after both pulls final content should be B's.
    """
    d = ctx.work_dir / "eventual_conflict"
    d.mkdir(parents=True, exist_ok=True)
    target = d / "shared.txt"
    my_payload = f"payload_{ctx.role}_{wall_ms()}".encode()
    details: dict[str, Any] = {}

    ctx.barrier.wait("ev_conflict_ready")
    target.write_bytes(my_payload)
    ctx.barrier.wait("ev_conflict_local_written")

    if ctx.is_a:
        details["a_flush"] = run_sync_hook(ctx, "flush_a_conflict")
    ctx.barrier.wait("ev_conflict_after_a_flush")

    details["pull_after_a"] = run_sync_hook(ctx, "pull_after_a_conflict")
    ctx.barrier.wait("ev_conflict_after_pull_a")
    after_a = target.read_bytes() if target.exists() else None
    details["after_a_preview"] = (
        after_a[:120].decode(errors="replace") if after_a is not None else None
    )

    if ctx.is_b:
        details["b_flush"] = run_sync_hook(ctx, "flush_b_conflict")
    ctx.barrier.wait("ev_conflict_after_b_flush")

    details["pull_after_b"] = run_sync_hook(ctx, "pull_after_b_conflict")
    ctx.barrier.wait("ev_conflict_after_pull_b")
    final = target.read_bytes() if target.exists() else None
    expected_final = None
    if ctx.is_b:
        (d / "expected_final_by_b.txt").write_bytes(my_payload)
    ctx.barrier.wait("ev_conflict_expected_ready")
    expected_final = (d / "expected_final_by_b.txt").read_bytes()
    final_matches_b = final == expected_final
    details["final_preview"] = final[:120].decode(errors="replace") if final else None
    details["expected_preview"] = expected_final[:120].decode(errors="replace")

    return TestResult(
        "eventual_same_file_ordered_flush",
        final_matches_b,
        notes=f"final_matches_b={final_matches_b}",
        details=details,
    )


# ═════════════════════════════════════════════════════════════════════════════
# TEST RUNNER
# ═════════════════════════════════════════════════════════════════════════════

SINGLE_CLIENT_TESTS = [
    test_basic_crud,
    test_deep_path,
    test_large_directory,
    test_special_filenames,
]

MULTI_CLIENT_TESTS = [
    test_cross_dir_visibility,
    test_same_dir_different_files,
    test_concurrent_create_same_name,
    test_same_file_overwrite,
    test_visibility_timing,
    test_delete_while_reading,
    test_rmdir_with_locked_file,
    test_concurrent_rename,
    test_readdir_during_modifications,
    test_file_lock_semantics,
    test_hardlinks,
    test_symlinks,
    test_delete_and_recreate,
    test_rapid_create_delete,
]

PROBE_TESTS = [
    test_conflict_contract_probe,
]


def run_test(func, ctx: TestCtx) -> TestResult:
    """Run a single test with error handling."""
    name = func.__name__
    try:
        result = func(ctx)
        return result
    except Exception as e:
        tb = traceback.format_exc()
        return TestResult(
            name,
            False,
            error=f"{type(e).__name__}: {e}",
            notes=tb[-500:],
        )


def build_contract_summary(work_dir: Path) -> Optional[dict[str, Any]]:
    """Merge A/B probe reports into one contract summary."""
    reports = []
    for p in sorted(work_dir.glob("results_*.json")):
        reports.append(json.loads(p.read_text()))
    if len(reports) < 2:
        return None

    by_role = {r.get("role"): r for r in reports}
    a = by_role.get("A")
    b = by_role.get("B")
    if not a or not b:
        return None

    def details(report: dict[str, Any]) -> dict[str, Any]:
        return report.get("tests", {}).get("conflict_contract_probe", {}).get("details", {})

    da = details(a)
    db = details(b)
    vis_a_to_b = da.get("visibility", {}).get("a_to_b", {})
    vis_b_to_a = da.get("visibility", {}).get("b_to_a", {})

    def merge_counts(x: dict[str, int], y: dict[str, int]) -> dict[str, int]:
        keys = set(x.keys()) | set(y.keys())
        return {k: int(x.get(k, 0)) + int(y.get(k, 0)) for k in sorted(keys)}

    summary = {
        "run_id": a.get("run_id"),
        "filesystem": a.get("filesystem"),
        "mode": "probe",
        "rounds": da.get("rounds"),
        "visibility": {
            "a_to_b": vis_a_to_b,
            "b_to_a": vis_b_to_a,
        },
        "overwrite_winners_total": merge_counts(
            da.get("overwrite_winners", {}), db.get("overwrite_winners", {})
        ),
        "rename_winners_total": merge_counts(
            da.get("rename_winners", {}), db.get("rename_winners", {})
        ),
        "delete_while_open": {
            "deleter_attempts_total": int(da.get("delete_while_open", {}).get("deleter_attempts", 0))
            + int(db.get("delete_while_open", {}).get("deleter_attempts", 0)),
            "deleter_success_total": int(da.get("delete_while_open", {}).get("deleter_success", 0))
            + int(db.get("delete_while_open", {}).get("deleter_success", 0)),
            "holder_read_after_delete_ok_total": int(
                da.get("delete_while_open", {}).get("holder_read_after_delete_ok", 0)
            )
            + int(db.get("delete_while_open", {}).get("holder_read_after_delete_ok", 0)),
        },
    }
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="studiofs-fstest: Multi-client filesystem behavior test suite"
    )
    parser.add_argument("mount", help="Path to shared filesystem mount point")
    parser.add_argument("--role", required=True, choices=list("ABCDEF"),
                        help="This client's role identifier (A, B, C, ...)")
    parser.add_argument("--peers", type=int, default=2,
                        help="Total number of peer clients (default: 2)")
    parser.add_argument("--fs-name", default="unknown",
                        help="Filesystem name for report (e.g. LucidLink, StudioFS, SMB)")
    parser.add_argument("--rounds", type=int, default=30,
                        help="Probe rounds per metric scenario (default: 30)")

    args = parser.parse_args()
    mount = Path(args.mount)

    if not mount.is_dir():
        print(f"ERROR: Mount point does not exist: {mount}", file=sys.stderr)
        sys.exit(1)

    # Setup coordination and work directories
    coord_dir = mount / COORD_DIR
    coord_dir.mkdir(exist_ok=True)

    run_id = f"run_{int(time.time())}"
    work_dir = mount / COORD_DIR / run_id
    work_dir.mkdir(parents=True, exist_ok=True)

    barrier = Barrier(coord_dir, args.role, args.peers)
    ctx = TestCtx(
        mount=mount,
        role=args.role,
        peers=args.peers,
        fs_name=args.fs_name,
        barrier=barrier,
        work_dir=work_dir,
        consistency_model="probe",
        probe_rounds=max(1, args.rounds),
    )

    print(f"═══ studiofs-fstest ═══")
    print(f"  mount:   {mount}")
    print(f"  role:    {args.role}")
    print(f"  peers:   {args.peers}")
    print(f"  fs:      {args.fs_name}")
    print(f"  mode:    probe")
    print(f"  rounds:  {ctx.probe_rounds}")
    print(f"  run_id:  {run_id}")
    print(f"  platform: {platform.system()} {platform.release()}")
    print()

    # Announce ourselves and wait for all peers
    print(f"Waiting for {args.peers} peers to join...")
    (coord_dir / f"peer_{args.role}").write_text(json.dumps({
        "role": args.role,
        "platform": platform.system(),
        "time": wall_ms(),
        "run_id": run_id,
    }))

    barrier.wait("startup")
    print(f"All {args.peers} peers connected!\n")

    # Agree on run_id (use A's)
    run_id_file = coord_dir / f"run_id_current"
    if ctx.is_a:
        run_id_file.write_text(run_id)
    barrier.wait("run_id_agreed")
    actual_run_id = run_id_file.read_text().strip()
    if actual_run_id != run_id:
        # Use A's run_id
        work_dir = mount / COORD_DIR / actual_run_id
        work_dir.mkdir(parents=True, exist_ok=True)
        ctx.work_dir = work_dir
        run_id = actual_run_id
        print(f"  Using run_id from A: {run_id}\n")

    results: list[TestResult] = []

    print("── Probe tests ──")
    for test_fn in PROBE_TESTS:
        name = test_fn.__name__
        print(f"  {name}...", end=" ", flush=True)
        result = run_test(test_fn, ctx)
        results.append(result)
        status = "PASS" if result.passed else "FAIL"
        print(f"{status} ({result.notes})" if result.notes else status)
    print()

    # ─── Summary ───
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    print(f"═══ Results: {passed} passed, {failed} failed, {len(results)} total ═══")

    # Save results
    report = {
        "filesystem": args.fs_name,
        "role": args.role,
        "peers": args.peers,
        "platform": platform.system(),
        "platform_release": platform.release(),
        "consistency_model": "probe",
        "run_id": run_id,
        "timestamp": time.time(),
        "summary": {"passed": passed, "failed": failed, "total": len(results)},
        "tests": {r.name: asdict(r) for r in results},
    }

    report_file = work_dir / f"results_{args.role}.json"
    report_file.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nReport saved to: {report_file}")

    # Wait for all peers to finish
    barrier.wait("all_done")

    # A collects all reports
    if ctx.is_a:
        time.sleep(1)
        print("\n── Combined Report ──")
        for r in sorted(work_dir.glob("results_*.json")):
            peer_report = json.loads(r.read_text())
            role = peer_report["role"]
            s = peer_report["summary"]
            print(f"  {role}: {s['passed']}/{s['total']} passed ({peer_report['platform']})")
        summary = build_contract_summary(work_dir)
        if summary is not None:
            summary_file = work_dir / "contract_summary.json"
            summary_file.write_text(json.dumps(summary, indent=2, default=str))
            print(f"  contract: {summary_file}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
