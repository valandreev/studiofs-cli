#!/usr/bin/env python3
"""Check that documented Rust test filters exist in crate sources."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path


DEFAULT_DOCS = (
    "docs/tests.md",
    "docs/local-first-validation-matrix.md",
)

EXCLUDED_NAMES = {
    "serial_test",
    "temp_env",
    "workflow_dispatch",
}


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def add_candidate(token: str, candidates: set[str]) -> None:
    name = token.strip(" \t\r\n`\"'.,;:()[]{}<>|")
    if "::" in name:
        name = name.rsplit("::", 1)[-1]

    if name in EXCLUDED_NAMES:
        return

    if "_" in name and re.fullmatch(r"[a-z][a-z0-9_]*_?", name):
        candidates.add(name)


def collect_candidates(root: Path, docs: list[str]) -> set[str]:
    candidates: set[str] = set()

    for doc in docs:
        path = root / doc
        if not path.exists():
            raise FileNotFoundError(f"Document not found: {doc}")

        content = path.read_text(encoding="utf-8")

        for match in re.finditer(r"`([^`\r\n]+)`", content):
            code = match.group(1)
            if not re.search(r"\s", code):
                add_candidate(code, candidates)

        for match in re.finditer(r"cargo\s+test\s+([^`<\r\n]+)", content):
            skip_next = False
            for token in re.split(r"\s+", match.group(1).strip()):
                if not token:
                    continue
                if token == "--":
                    break
                if skip_next:
                    skip_next = False
                    continue
                if token in {"-p", "--package", "--test", "--bin", "--example"}:
                    skip_next = True
                    continue
                if token.startswith("-") or "=" in token:
                    continue
                add_candidate(token, candidates)

    return candidates


def source_contains_name(root: Path, name: str) -> bool:
    rg = shutil.which("rg")
    if rg is not None:
        result = subprocess.run(
            [
                rg,
                "--quiet",
                "--fixed-strings",
                "--glob",
                "*.rs",
                "--",
                name,
                str(root / "crates"),
            ],
            check=False,
        )
        return result.returncode == 0

    for source_path in (root / "crates").rglob("*.rs"):
        try:
            if name in source_path.read_text(encoding="utf-8"):
                return True
        except UnicodeDecodeError:
            if name.encode() in source_path.read_bytes():
                return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check documented Rust test filters against crate sources."
    )
    parser.add_argument("docs", nargs="*", default=list(DEFAULT_DOCS))
    args = parser.parse_args()

    root = repo_root()
    try:
        candidates = collect_candidates(root, args.docs)
        missing = [
            name for name in sorted(candidates) if not source_contains_name(root, name)
        ]
    except FileNotFoundError as err:
        print(str(err), file=sys.stderr)
        return 1

    if missing:
        print(
            "Documented test names not found in Rust sources: " + ", ".join(missing),
            file=sys.stderr,
        )
        return 1

    print(f"Checked {len(candidates)} documented test names.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
