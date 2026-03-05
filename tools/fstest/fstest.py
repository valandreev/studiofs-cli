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

    @property
    def is_a(self) -> bool:
        return self.role == "A"

    @property
    def is_b(self) -> bool:
        return self.role == "B"


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
    parser.add_argument("--skip-single", action="store_true",
                        help="Skip single-client tests")
    parser.add_argument("--skip-multi", action="store_true",
                        help="Skip multi-client tests")
    parser.add_argument("--test", action="append", default=[],
                        help="Run only specific test(s) by name (can repeat)")

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
    )

    print(f"═══ studiofs-fstest ═══")
    print(f"  mount:   {mount}")
    print(f"  role:    {args.role}")
    print(f"  peers:   {args.peers}")
    print(f"  fs:      {args.fs_name}")
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

    # ─── Single-client tests (A only, B waits) ───
    if not args.skip_single:
        if ctx.is_a:
            print("── Single-client tests (A runs, others wait) ──")
            for test_fn in SINGLE_CLIENT_TESTS:
                if args.test and test_fn.__name__ not in args.test:
                    continue
                name = test_fn.__name__
                print(f"  {name}...", end=" ", flush=True)
                result = run_test(test_fn, ctx)
                results.append(result)
                status = "PASS" if result.passed else "FAIL"
                print(f"{status} ({result.notes})" if result.notes else status)
            print()

        barrier.wait("single_done")

    # ─── Multi-client tests ───
    if not args.skip_multi:
        print("── Multi-client tests ──")
        for test_fn in MULTI_CLIENT_TESTS:
            if args.test and test_fn.__name__ not in args.test:
                continue
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

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
