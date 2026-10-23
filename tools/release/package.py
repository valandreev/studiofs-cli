#!/usr/bin/env python3
"""Package a verified StudioFS release binary and its required notices."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path
from urllib.parse import urljoin, urlsplit


TARGETS = (
    "x86_64-pc-windows-msvc",
    "aarch64-pc-windows-msvc",
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "aarch64-apple-darwin",
)
WINDOWS_TARGETS = TARGETS[:2]
ASSETS = (
    ("LICENSE", "LICENSE"),
    ("THIRD-PARTY-NOTICES.txt", "THIRD-PARTY-NOTICES.txt"),
    ("rust-dependency-inventory.json", "rust-dependency-inventory.json"),
    ("docs/releases.md", "INSTALL.md"),
    ("docs/legal/platform-distribution.md", "PLATFORM-DISTRIBUTION.md"),
)
PACKAGED_MARKDOWN = {"INSTALL.md", "PLATFORM-DISTRIBUTION.md"}
MARKDOWN_LINK = re.compile(r"(?<!!)\[([^\]]*)\]\(([^)\s]+)([^)]*)\)")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalized_lock_hash(path: Path) -> str:
    return sha256(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n"))


def executable_tarinfo(info: tarfile.TarInfo) -> tarfile.TarInfo:
    info.mode = 0o755
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    info.mtime = 0
    return info


def plain_tarinfo(info: tarfile.TarInfo) -> tarfile.TarInfo:
    info.mode = 0o644
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    info.mtime = 0
    return info


def packaged_markdown(path: Path, repo: Path, repository: str, source_commit: str) -> bytes:
    text = path.read_text(encoding="utf-8")
    source_url = urljoin(
        f"{repository.rstrip('/')}/blob/{source_commit}/",
        path.relative_to(repo).as_posix(),
    )

    def replace_link(match: re.Match[str]) -> str:
        destination = match.group(2)
        parsed = urlsplit(destination)
        if destination.startswith("#") or parsed.scheme or parsed.netloc:
            return match.group(0)
        return f"[{match.group(1)}]({urljoin(source_url, destination)}{match.group(3)})"

    return MARKDOWN_LINK.sub(replace_link, text).encode("utf-8")


def create_package(
    repo: Path,
    target: str,
    notices: Path,
    output_dir: Path,
    *,
    source_commit: str,
    rustc_version: str,
) -> Path:
    if target not in TARGETS:
        raise ValueError(f"unsupported release target: {target}")

    repo = repo.resolve()
    notices = notices if notices.is_absolute() else repo / notices
    output_dir = output_dir if output_dir.is_absolute() else repo / output_dir
    manifest = tomllib.loads((repo / "Cargo.toml").read_text(encoding="utf-8"))
    version = manifest["workspace"]["package"]["version"]
    repository = manifest["workspace"]["package"]["repository"]
    binary_name = "studiofs.exe" if target in WINDOWS_TARGETS else "studiofs"
    binary = repo / "target" / target / "release" / binary_name
    lockfile = repo / "Cargo.lock"
    inventory_path = notices / "rust-dependency-inventory.json"

    inputs: list[tuple[Path, str]] = [(binary, binary_name)]
    inputs.extend((notices / name, archive_name) for name, archive_name in ASSETS[:3])
    inputs.extend((repo / path, archive_name) for path, archive_name in ASSETS[3:])
    for path, _ in inputs:
        if not path.is_file():
            raise ValueError(f"required release input is missing: {path}")

    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    counts = inventory.get("package_counts")
    unresolved = counts.get("unresolved_third_party_evidence") if isinstance(counts, dict) else None
    if not isinstance(unresolved, int) or unresolved != 0:
        raise ValueError(f"release blocked: dependency inventory has unresolved evidence ({unresolved!r})")
    current_lock_hash = normalized_lock_hash(lockfile)
    inventory_inputs = inventory.get("inputs")
    lockfile_info = inventory_inputs.get("lockfile") if isinstance(inventory_inputs, dict) else None
    inventory_lock_hash = lockfile_info.get("sha256") if isinstance(lockfile_info, dict) else None
    if inventory_lock_hash != current_lock_hash:
        raise ValueError("release blocked: dependency inventory Cargo.lock hash does not match current Cargo.lock")

    build_info = {
        "version": version,
        "target": target,
        "source_commit": source_commit,
        "build_command": f"cargo build -p studiofs --release --locked --target {target}",
        "rustc_version": rustc_version,
        "binary_sha256": sha256(binary.read_bytes()),
        "cargo_lock_sha256": current_lock_hash,
    }
    package_name = f"studiofs-{version}-{target}"
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = output_dir / (f"{package_name}.zip" if target in WINDOWS_TARGETS else f"{package_name}.tar.gz")

    if target in WINDOWS_TARGETS:
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path, name in inputs:
                archive_name = f"{package_name}/{name}"
                if name in PACKAGED_MARKDOWN:
                    archive.writestr(archive_name, packaged_markdown(path, repo, repository, source_commit))
                else:
                    archive.write(path, archive_name)
            archive.writestr(
                f"{package_name}/BUILD-INFO.json",
                json.dumps(build_info, indent=2, sort_keys=True) + "\n",
            )
    else:
        with tarfile.open(archive_path, "w:gz") as archive:
            for path, name in inputs:
                archive_name = f"{package_name}/{name}"
                if name in PACKAGED_MARKDOWN:
                    payload = packaged_markdown(path, repo, repository, source_commit)
                    info = tarfile.TarInfo(archive_name)
                    info.size = len(payload)
                    info.mode = 0o644
                    archive.addfile(info, io.BytesIO(payload))
                    continue
                archive.add(
                    path,
                    arcname=archive_name,
                    recursive=False,
                    filter=executable_tarinfo if name == binary_name else plain_tarinfo,
                )
            info = tarfile.TarInfo(f"{package_name}/BUILD-INFO.json")
            payload = (json.dumps(build_info, indent=2, sort_keys=True) + "\n").encode("utf-8")
            info.size = len(payload)
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(payload))

    sidecar = archive_path.with_name(archive_path.name + ".sha256")
    sidecar.write_text(
        f"{sha256(archive_path.read_bytes())}  {archive_path.name}\n",
        encoding="ascii",
        newline="\n",
    )
    return archive_path


def command_output(args: list[str], repo: Path) -> str:
    return subprocess.run(
        args,
        cwd=repo,
        check=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=TARGETS)
    parser.add_argument("--notices", type=Path, default=Path("target/license-compliance"))
    parser.add_argument("--output-dir", type=Path, default=Path("target/release-assets"))
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[2]
    try:
        archive = create_package(
            repo,
            args.target,
            args.notices,
            args.output_dir,
            source_commit=command_output(["git", "rev-parse", "HEAD"], repo),
            rustc_version=command_output(["rustc", "-Vv"], repo),
        )
    except (OSError, KeyError, ValueError, json.JSONDecodeError, subprocess.CalledProcessError) as error:
        print(f"release packaging failed: {error}", file=sys.stderr)
        return 1
    print(f"created {archive} and {archive.name}.sha256")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
