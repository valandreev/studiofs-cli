#!/usr/bin/env python3
"""Build the compiled test successor (SFS-1131) and write its manifest.

Builds the baseline `studiofs` and the `test-successor` feature build, diffs
their `studiofs internal version-identities` reports, and fails unless every
identity difference is a declared change. Writes
`target/test-successor/manifest.json` with the source commit, binary SHA-256
and every declared change. `--require-complete` (used by SFS-1019 before the
matrix) also requires one change of each kind and both rollback classes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

REQUIRED_KINDS = ("postgres_migration", "event_kind", "checkpoint_format", "local_format")
REQUIRED_ROLLBACKS = ("reversible", "irreversible")
MARKER = "STUDIOFS-TEST-SUCCESSOR-BUILD"


def verify(baseline: dict, successor: dict, *, require_complete: bool = False) -> list[str]:
    """Return every mismatch between the reports and the declared changes."""
    errors = []
    if baseline.get("successor_build") is not None:
        errors.append("baseline binary was built with the test-successor feature")
    if successor.get("successor_build") != MARKER:
        errors.append("successor binary does not carry the test-successor marker")
    if baseline.get("client_release") != successor.get("client_release"):
        errors.append("baseline and successor were built from different releases")

    changes = successor.get("successor_changes", [])
    ids = [change["id"] for change in changes]
    if len(ids) != len(set(ids)):
        errors.append("duplicate change ids")
    bumps: dict[str, dict] = {}
    for change in changes:
        bump = change.get("bump")
        if bump is None:
            continue
        if bump["identity"] in bumps:
            errors.append(f"identity {bump['identity']} is bumped by more than one change")
        bumps[bump["identity"]] = bump

    base = {identity["name"]: identity for identity in baseline["identities"]}
    succ = {identity["name"]: identity for identity in successor["identities"]}
    for name in sorted(base.keys() - succ.keys()):
        errors.append(f"successor dropped identity {name}")
    for name in sorted(succ.keys()):
        found = (base.get(name, {}).get("current", 0), succ[name]["current"])
        bump = bumps.pop(name, None)
        if bump is not None and bump["baseline"] == bump["successor"]:
            errors.append(f"identity {name}: declared bump must change version ({bump['baseline']} -> {bump['successor']})")
        elif name not in base and bump is None:
            errors.append(f"successor added undeclared identity {name}")
        else:
            declared = (bump["baseline"], bump["successor"]) if bump else (found[0], found[0])
            if found != declared:
                errors.append(f"identity {name}: reported {found[0]} -> {found[1]}, declared {declared[0]} -> {declared[1]}")
        if name in base and base[name]["min_supported"] != succ[name]["min_supported"]:
            errors.append(f"identity {name}: min_supported must not change")
    for name in sorted(bumps):
        errors.append(f"declared bump of unknown identity {name}")

    if require_complete:
        for kind in REQUIRED_KINDS:
            if not any(change["kind"] == kind for change in changes):
                errors.append(f"no declared {kind} change")
        for rollback in REQUIRED_ROLLBACKS:
            if not any(change["rollback"] == rollback for change in changes):
                errors.append(f"no {rollback} change")
    return errors


def run(args: list[str], repo: Path) -> str:
    return subprocess.run(
        args, cwd=repo, check=True, text=True, encoding="utf-8", stdout=subprocess.PIPE
    ).stdout


def build(repo: Path, target_dir: Path, features: list[str]) -> Path:
    subprocess.run(
        ["cargo", "build", "-p", "studiofs", "--bin", "studiofs", "--locked", "--target-dir", str(target_dir), *features],
        cwd=repo,
        check=True,
    )
    name = "studiofs.exe" if sys.platform == "win32" else "studiofs"
    return target_dir / "debug" / name


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[2]
    out_dir = repo / "target" / "test-successor"
    try:
        baseline_bin = build(repo, repo / "target", [])
        successor_bin = build(repo, out_dir, ["--features", "test-successor"])
        report = ["internal", "version-identities"]
        baseline = json.loads(run([str(baseline_bin), *report], repo))
        successor = json.loads(run([str(successor_bin), *report], repo))
        commit = run(["git", "rev-parse", "HEAD"], repo).strip()
        dirty = bool(run(["git", "status", "--porcelain"], repo).strip())
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        print(f"test-successor build failed: {error}", file=sys.stderr)
        return 1

    errors = verify(baseline, successor, require_complete=args.require_complete)
    manifest = {
        "source_commit": commit,
        "source_dirty": dirty,
        "client_release": successor["client_release"],
        "binary": successor_bin.relative_to(repo).as_posix(),
        "binary_sha256": sha256(successor_bin),
        "baseline_binary": baseline_bin.relative_to(repo).as_posix(),
        "baseline_binary_sha256": sha256(baseline_bin),
        "baseline_identities": baseline["identities"],
        "identities": successor["identities"],
        "changes": successor["successor_changes"],
        "verified": not errors,
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    for error in errors:
        print(f"test-successor: {error}", file=sys.stderr)
    print(f"wrote {manifest_path} ({len(manifest['changes'])} declared changes)")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
