#!/usr/bin/env python3
"""Run the SFS-1019 supported-release forward-compatibility mounted E2E matrix.

1. Builds and verifies the 0.2.5 baseline and compiled `test-successor` binaries
   via `tools/test_successor/build.py --require-complete`.
2. Builds `studiofs-e2e`.
3. Downloads and SHA-256-verifies the host's published `v0.2.4` binary for
   the fenced-client upgrade cell (or uses `--v024-bin`), and with
   `--crash-rollback` the `v0.2.2` binary for format-5 adoption (or
   `--v022-bin`). A host without a published asset skips those cells.
4. Runs `studiofs-e2e run mounted-forward-compat-matrix --infra compose` and
   prints the path to the published `forward-compat-matrix.json` artifact.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

V024_TAG = "v0.2.4"
V022_TAG = "v0.2.2"

# Published release assets per (tag, OS, CPU) with their release SHA-256 digests.
PUBLISHED_ASSETS = {
    ("v0.2.4", "windows", "x86_64"): (
        "studiofs-0.2.4-x86_64-pc-windows.zip",
        "cbd877327d86adc2e77395578298f9cc5f9a3d4a517559f9d8904698b805c3a6",
    ),
    ("v0.2.4", "windows", "aarch64"): (
        "studiofs-0.2.4-aarch64-pc-windows.zip",
        "baabae59dd58f5a12293c71af9f1df4e924a756bee14b498aab5a27ab0f8aeb1",
    ),
    ("v0.2.4", "linux", "x86_64"): (
        "studiofs-0.2.4-x86_64-linux-gnu.tar.gz",
        "c5872a9b4fdc4fb6f451cb91d8661a28784a921d04efe9848a070eac80cc8b7d",
    ),
    ("v0.2.4", "linux", "aarch64"): (
        "studiofs-0.2.4-aarch64-linux-gnu.tar.gz",
        "87a33dd6e6110beea583cc050feff1fe75d64320f8890c796bb501db710ce6bb",
    ),
    ("v0.2.4", "macos", "aarch64"): (
        "studiofs-0.2.4-aarch64-apple-darwin.tar.gz",
        "c1e7a31334ec8ab8206877c2b4141866fe379f5473a1386a1498b0a2d936c66b",
    ),
    ("v0.2.2", "windows", "x86_64"): (
        "studiofs-0.2.2-x86_64-pc-windows-msvc.zip",
        "10748f832568b8f918b1b316e48d819139ff9aaaccb190a77abf5aca5cbebff0",
    ),
    ("v0.2.2", "windows", "aarch64"): (
        "studiofs-0.2.2-aarch64-pc-windows-msvc.zip",
        "977e9189044b882867fd4ce6ad2946ccd60ae4661442374161986b8cb79d6b9e",
    ),
    ("v0.2.2", "linux", "x86_64"): (
        "studiofs-0.2.2-x86_64-unknown-linux-gnu.tar.gz",
        "6d0e85842c46a6812773b27df7a212cd0bb90a77af95a14b4eec6efe4e77673b",
    ),
    ("v0.2.2", "linux", "aarch64"): (
        "studiofs-0.2.2-aarch64-unknown-linux-gnu.tar.gz",
        "563614fb17b9a18f62ba14203c132b61b466148b709cc3e35ef8f1db8e602f14",
    ),
    ("v0.2.2", "macos", "aarch64"): (
        "studiofs-0.2.2-aarch64-apple-darwin.tar.gz",
        "76ad12836ad1fd382bb2821445bb24be3d8a7e57e65928619a8a538338a500e6",
    ),
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def host_key() -> tuple[str, str]:
    os_name = {"win32": "windows", "darwin": "macos"}.get(sys.platform, sys.platform)
    machine = platform.machine().lower()
    cpu = {"amd64": "x86_64", "x64": "x86_64", "arm64": "aarch64"}.get(machine, machine)
    return os_name, cpu


def read_studiofs_from_archive(archive: Path, exe: str) -> bytes:
    if archive.suffix == ".zip":
        with zipfile.ZipFile(archive) as zf:
            matches = [name for name in zf.namelist() if Path(name).name == exe]
            if len(matches) == 1:
                return zf.read(matches[0])
    else:
        with tarfile.open(archive, "r:gz") as tf:
            matches = [m for m in tf.getmembers() if m.isfile() and Path(m.name).name == exe]
            if len(matches) == 1:
                return tf.extractfile(matches[0]).read()
    raise ValueError(f"expected one {exe} inside {archive}, found {len(matches)}")


def ensure_published_binary(repo: Path, tag: str, explicit: str | None, skip: bool) -> Path | None:
    """Return a SHA-256-verified published `studiofs` for this host, downloading it once.

    Returns None when skipped or when the release has no asset for this host.
    """
    if skip:
        return None
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise FileNotFoundError(f"{tag} binary not found: {path}")
        return path

    asset = PUBLISHED_ASSETS.get((tag, *host_key()))
    if asset is None:
        print(f"note: {tag} publishes no binary for {host_key()}; its cells are skipped", file=sys.stderr)
        return None
    name, expected = asset

    work_dir = repo / "target" / "sfs-1002-published" / tag
    work_dir.mkdir(parents=True, exist_ok=True)
    archive = work_dir / name
    if not archive.is_file():
        subprocess.run(
            ["gh", "release", "download", tag, "--pattern", name, "--dir", str(work_dir)],
            cwd=repo,
            check=True,
        )
    actual = sha256_file(archive)
    if actual != expected:
        raise ValueError(f"{name} SHA-256 mismatch: expected {expected}, got {actual}")

    exe = "studiofs.exe" if sys.platform == "win32" else "studiofs"
    verified = read_studiofs_from_archive(archive, exe)
    cached = work_dir / "bin" / exe
    if not cached.is_file() or sha256_file(cached) != hashlib.sha256(verified).hexdigest():
        cached.parent.mkdir(parents=True, exist_ok=True)
        cached.write_bytes(verified)
    cached.chmod(0o755)
    return cached


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--run-id", default="sfs-1019-compat-matrix")
    parser.add_argument("--root", default="target/e2e-runs")
    parser.add_argument("--platform", default="auto")
    parser.add_argument("--retain", default="always")
    parser.add_argument("--port-range", default=None)
    parser.add_argument("--timeout", default=None)
    parser.add_argument("--v024-bin", default=None)
    parser.add_argument(
        "--v022-bin",
        default=None,
        help="verified native v0.2.2 binary for format-5 adoption (downloaded with --crash-rollback when omitted)",
    )
    parser.add_argument("--skip-v022", action="store_true")
    parser.add_argument("--skip-v024", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument(
        "--crash-rollback", action="store_true", help="enable SFS-1020 fault injection"
    )
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[2]
    manifest_path = repo / "target" / "test-successor" / "manifest.json"

    if not args.skip_build:
        subprocess.run(
            [sys.executable, str(repo / "tools" / "test_successor" / "build.py"), "--require-complete"],
            cwd=repo,
            check=True,
        )
        subprocess.run(
            ["cargo", "build", "-p", "studiofs-e2e", "--locked"],
            cwd=repo,
            check=True,
        )

    if not manifest_path.is_file():
        print(f"missing test-successor manifest: {manifest_path}", file=sys.stderr)
        return 1
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("verified") is not True:
        print("test-successor manifest is not verified", file=sys.stderr)
        return 1

    baseline_bin = repo / manifest["baseline_binary"]
    successor_bin = repo / manifest["binary"]
    v024_bin = ensure_published_binary(repo, V024_TAG, args.v024_bin, args.skip_v024)

    e2e_exe = repo / "target" / "debug" / ("studiofs-e2e.exe" if sys.platform == "win32" else "studiofs-e2e")
    cmd = [
        str(e2e_exe),
        "run",
        "mounted-forward-compat-matrix",
        "--studiofs-bin",
        str(baseline_bin),
        "--root",
        args.root,
        "--run-id",
        args.run_id,
        "--platform",
        args.platform,
        "--infra",
        "compose",
        "--retain",
        args.retain,
    ]
    if args.port_range:
        cmd.extend(["--port-range", args.port_range])
    if args.timeout:
        cmd.extend(["--timeout", args.timeout])

    env = os.environ.copy()
    env["STUDIOFS_E2E_SUCCESSOR_MANIFEST"] = str(manifest_path)
    env["STUDIOFS_E2E_SUCCESSOR_BIN"] = str(successor_bin)
    if args.crash_rollback:
        env["STUDIOFS_E2E_CRASH_ROLLBACK"] = "1"
    if args.crash_rollback:
        v022_bin = ensure_published_binary(repo, V022_TAG, args.v022_bin, args.skip_v022)
        if v022_bin is not None:
            env["STUDIOFS_E2E_V022_BIN"] = str(v022_bin.resolve(strict=True))
    if v024_bin is not None:
        env["STUDIOFS_E2E_V024_BIN"] = str(v024_bin)
        env["STUDIOFS_E2E_REQUIRE_V024"] = "1"

    proc = subprocess.run(cmd, cwd=repo, env=env)
    artifact = (
        repo
        / args.root
        / args.run_id
        / "artifacts"
        / "scenarios"
        / "mounted-forward-compat-matrix"
        / "forward-compat-matrix.json"
    )
    if artifact.is_file():
        print(f"matrix artifact: {artifact}")
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
