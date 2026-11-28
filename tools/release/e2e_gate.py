#!/usr/bin/env python3
"""Pre-release mounted E2E gate: build everything and run every E2E scenario on this host.

Run it once on each release platform (Windows/WinFSP, Linux/FUSE, macOS/macFUSE).
It needs Cargo, Python 3, Docker Compose, the native mount driver and an
authenticated `gh` CLI (for the published v0.2.4/v0.2.2 binaries).

Steps:
1. Refuses a dirty worktree (release evidence must name a clean commit) unless
   `--allow-dirty`.
2. Builds the release `studiofs`, `studiofs-e2e`, and the verified compiled
   test successor (`tools/test_successor/build.py --require-complete`).
3. `scenarios`: `studiofs-e2e run --scenario release-gate`, every registered
   scenario except opt-in tools and the forward-compat matrix. Scenarios for
   another OS are reported as skipped.
4. `forward`: the SFS-1019 forward-compat matrix in its own sandbox, because the
   successor migrates the shared PostgreSQL irreversibly.
5. `crash`: the same matrix with SFS-1020 crash/rollback fault injection.

Each step gets its own run id `<prefix>-<step>`. Every step runs even when an
earlier one fails; the gate writes `<root>/<prefix>-summary.json` and exits 1
when any step failed.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
EXE = ".exe" if sys.platform == "win32" else ""
STEPS = ("scenarios", "forward", "crash")
SCENARIO_STEPS = ("scenarios", "forward")
MATRIX_ARTIFACT = Path("artifacts/scenarios/mounted-forward-compat-matrix/forward-compat-matrix.json")


def run(cmd: list[str]) -> int:
    print("+ " + " ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=REPO).returncode


def build() -> None:
    for cmd in (
        ["cargo", "build", "--release", "-p", "studiofs", "--locked"],
        ["cargo", "build", "-p", "studiofs-e2e", "--locked"],
        [sys.executable, "tools/test_successor/build.py", "--require-complete"],
    ):
        if run(cmd) != 0:
            raise SystemExit(f"build failed: {' '.join(cmd)}")


def common_args(args: argparse.Namespace, run_id: str) -> list[str]:
    out = ["--root", args.root, "--run-id", run_id, "--platform", args.platform, "--retain", args.retain]
    if args.port_range:
        out += ["--port-range", args.port_range]
    if args.timeout:
        out += ["--timeout", args.timeout]
    return out


def step_command(step: str, args: argparse.Namespace, run_id: str) -> list[str]:
    if step == "scenarios":
        return [
            str(REPO / "target" / "debug" / f"studiofs-e2e{EXE}"),
            "run",
            "--scenario",
            "release-gate",
            "--studiofs-bin",
            str(REPO / "target" / "release" / f"studiofs{EXE}"),
            "--infra",
            "compose",
            *common_args(args, run_id),
        ]
    cmd = [sys.executable, "tools/test_successor/run_compat_matrix.py", "--skip-build"]
    if step == "crash":
        cmd.append("--crash-rollback")
    return cmd + common_args(args, run_id)


def summarize(step: str, run_dir: Path, exit_code: int) -> dict:
    result = {"step": step, "run_id": run_dir.name, "exit_code": exit_code, "passed": exit_code == 0}
    report_path = run_dir / "report.json"
    if report_path.is_file():
        report = json.loads(report_path.read_text(encoding="utf-8"))
        result["report"] = str(report_path)
        result["status"] = report.get("status")
        scenarios = report.get("scenarios", [])
        result["scenarios"] = len(scenarios)
        for status in ("failed", "skipped"):
            result[status] = [
                {"name": s["name"], "error": s.get("error")} for s in scenarios if s.get("status") == status
            ]
    matrix_path = run_dir / MATRIX_ARTIFACT
    if matrix_path.is_file():
        result["matrix"] = json.loads(matrix_path.read_text(encoding="utf-8")).get("summary")
        # A skipped cell (no published binary for this host) passes but is incomplete evidence.
        result["complete"] = result["matrix"].get("skipped", 0) == 0
    return result


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, check=True).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parser.add_argument("--run-id-prefix", default=f"release-gate-{stamp}")
    parser.add_argument("--root", default="target/e2e-runs")
    parser.add_argument("--platform", default="auto")
    parser.add_argument("--retain", default="on-failure", choices=("always", "on-failure", "never"))
    parser.add_argument("--port-range", default=None)
    parser.add_argument("--timeout", default=None, help="per-scenario timeout passed to studiofs-e2e (its default is 5m), e.g. 30m")
    parser.add_argument("--step", action="append", choices=STEPS, help="run only these steps (repeatable)")
    parser.add_argument("--skip-build", action="store_true", help="reuse binaries from an earlier gate build")
    parser.add_argument("--allow-dirty", action="store_true", help="allow uncommitted changes (not release evidence)")
    parser.add_argument(
        "--scenarios-only",
        action="store_true",
        help="run every E2E scenario (the scenarios and forward steps) on any worktree; skips the crash matrix",
    )
    args = parser.parse_args()
    if args.scenarios_only:
        args.allow_dirty = True

    missing = [tool for tool in ("cargo", "docker", "gh") if shutil.which(tool) is None]
    if missing:
        print(f"missing required tools on PATH: {', '.join(missing)}", file=sys.stderr)
        return 2
    # The CLI can exist while its engine is down (macOS OrbStack not started, Docker Desktop stopped).
    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        print("docker engine is not reachable; start OrbStack (macOS) or Docker Desktop, then retry", file=sys.stderr)
        return 2
    commit = git("rev-parse", "HEAD")
    dirty = git("status", "--porcelain", "--untracked-files=no")
    if dirty and not args.allow_dirty:
        print("worktree has uncommitted changes; commit them or pass --allow-dirty:\n" + dirty, file=sys.stderr)
        return 2

    if not args.skip_build:
        build()
    elif not args.allow_dirty:
        manifest = REPO / "target" / "test-successor" / "manifest.json"
        built = json.loads(manifest.read_text(encoding="utf-8")).get("source_commit") if manifest.is_file() else None
        if built != commit:
            print(f"--skip-build: binaries were built from {built}, not HEAD {commit}; rebuild", file=sys.stderr)
            return 2

    results = []
    for step in args.step or (SCENARIO_STEPS if args.scenarios_only else STEPS):
        run_id = f"{args.run_id_prefix}-{step}"
        print(f"=== release gate step {step} (run id {run_id}) ===", flush=True)
        exit_code = run(step_command(step, args, run_id))
        results.append(summarize(step, REPO / args.root / run_id, exit_code))

    summary = {
        "run_id_prefix": args.run_id_prefix,
        "source_commit": commit,
        "source_dirty": bool(dirty),
        "platform": sys.platform,
        "passed": all(r["passed"] for r in results),
        "complete": all(r.get("complete", True) for r in results),
        "steps": results,
    }
    summary_path = REPO / args.root / f"{args.run_id_prefix}-summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print("=== release gate summary ===")
    for r in results:
        line = f"{r['step']:<10} {'PASS' if r['passed'] else 'FAIL'}  run_id={r['run_id']}"
        if "scenarios" in r:
            line += f"  scenarios={r['scenarios']} failed={len(r['failed'])} skipped={len(r['skipped'])}"
        if r.get("matrix"):
            line += f"  cells={r['matrix']}"
            if not r["complete"]:
                line += "  INCOMPLETE (skipped cells)"
        print(line)
        for failed in r.get("failed", []):
            print(f"    failed: {failed['name']}: {failed['error']}")
    print(f"summary: {summary_path}")
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
