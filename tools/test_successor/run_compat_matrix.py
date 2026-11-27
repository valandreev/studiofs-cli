#!/usr/bin/env python3
"""Run the SFS-1019 supported-release forward-compatibility mounted E2E matrix.

1. Builds and verifies the 0.2.5 baseline and compiled `test-successor` binaries
   via `tools/test_successor/build.py --require-complete`.
2. Builds `studiofs-e2e`.
3. Optionally downloads and SHA-256-verifies the published `v0.2.4` release
   binary for the fenced-client upgrade cell (or uses `--v024-bin`).
4. Runs `studiofs-e2e run mounted-forward-compat-matrix --infra compose` and
   prints the path to the published `forward-compat-matrix.json` artifact.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

V024_TAG = "v0.2.4"
V024_WINDOWS_ASSET = "studiofs-0.2.4-x86_64-pc-windows.zip"
V024_WINDOWS_SHA256 = "cbd877327d86adc2e77395578298f9cc5f9a3d4a517559f9d8904698b805c3a6"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_v024_binary(repo: Path, explicit: str | None, skip: bool) -> Path | None:
    if skip:
        return None
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise FileNotFoundError(f"v0.2.4 binary not found: {path}")
        return path

    exe_name = "studiofs.exe" if sys.platform == "win32" else "studiofs"
    cached = repo / "target" / "sfs-1002-published" / V024_TAG / "bin" / exe_name
    if cached.is_file():
        return cached

    if sys.platform != "win32":
        print(
            "note: automatic v0.2.4 download is pinned for Windows; pass --v024-bin on Unix to include cell_08",
            file=sys.stderr,
        )
        return None

    work_dir = repo / "target" / "sfs-1002-published" / V024_TAG
    work_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "gh",
            "release",
            "download",
            V024_TAG,
            "--pattern",
            V024_WINDOWS_ASSET,
            "--pattern",
            f"{V024_WINDOWS_ASSET}.sha256",
            "--dir",
            str(work_dir),
            "--clobber",
        ],
        cwd=repo,
        check=True,
    )
    zip_path = work_dir / V024_WINDOWS_ASSET
    actual = sha256_file(zip_path)
    if actual != V024_WINDOWS_SHA256:
        raise ValueError(
            f"{V024_WINDOWS_ASSET} SHA-256 mismatch: expected {V024_WINDOWS_SHA256}, got {actual}"
        )
    bin_dir = work_dir / "bin"
    if bin_dir.exists():
        shutil.rmtree(bin_dir)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(bin_dir)
    matches = list(bin_dir.rglob(exe_name))
    if not matches:
        raise FileNotFoundError(f"{exe_name} not found inside {zip_path}")
    return matches[0]


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
    parser.add_argument("--skip-v024", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
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
    if not manifest.get("verified"):
        print("test-successor manifest is not verified", file=sys.stderr)
        return 1

    baseline_bin = repo / manifest["baseline_binary"]
    successor_bin = repo / manifest["binary"]
    v024_bin = ensure_v024_binary(repo, args.v024_bin, args.skip_v024)

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
