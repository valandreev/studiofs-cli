#!/usr/bin/env python3
"""Generate deterministic third-party notices and a lockfile inventory."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import subprocess
import sys
import tarfile
import tempfile
import tomllib
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parents[2]
INVENTORY_GENERATOR = Path(__file__).with_name("generate-rust-dependency-inventory.py")
NATIVE_POLICY = Path(__file__).with_name("platform-distribution.md")
BASELINE_INVENTORY = Path(__file__).with_name("rust-dependency-inventory.json")
SOURCE_MATERIAL_NOTICES = Path(__file__).with_name("source-material-notices.txt")
SUPPLEMENTAL_LICENSES = Path(__file__).with_name("supplemental-licenses")
EVIDENCE_PREFIXES = ("license", "copying", "notice", "copyright", "unlicense")
SOURCE_SUFFIXES = {".rs", ".c", ".h", ".cc", ".cpp", ".py", ".js", ".json", ".toml"}


def _inventory_helpers():
    sys.dont_write_bytecode = True
    spec = importlib.util.spec_from_file_location("rust_dependency_inventory", INVENTORY_GENERATOR)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load dependency inventory evidence helpers")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cargo_metadata(repo: Path) -> dict:
    result = subprocess.run(
        ["cargo", "metadata", "--locked", "--format-version", "1"],
        cwd=repo, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, encoding="utf-8", errors="strict",
    )
    if result.stderr.strip():
        print(result.stderr.strip(), file=sys.stderr)
    return json.loads(result.stdout)


def lock_key(package: dict) -> tuple[str, str, str | None]:
    return package["name"], package["version"], package.get("source")


def validate_lock_identity(metadata: dict, lock: dict, lock_path: Path) -> tuple[dict, str]:
    lock_packages = lock.get("package", [])
    metadata_packages = metadata.get("packages", [])
    by_lock = {lock_key(package): package for package in lock_packages}
    by_metadata = {lock_key(package): package for package in metadata_packages}
    if len(by_lock) != len(lock_packages) or len(by_metadata) != len(metadata_packages):
        raise RuntimeError("duplicate package identity in Cargo.lock or cargo metadata")
    missing = sorted(set(by_lock) - set(by_metadata))
    extra = sorted(set(by_metadata) - set(by_lock))
    if missing or extra:
        raise RuntimeError(f"Cargo.lock/metadata package identities differ; missing={missing[:5]} extra={extra[:5]}")
    for key, locked in by_lock.items():
        package = by_metadata[key]
        expected = locked.get("checksum")
        actual = package.get("checksum")
        if actual is not None and expected != actual:
            raise RuntimeError(f"Cargo.lock/metadata checksum mismatch for {key[0]} {key[1]}")
        if key[2] and key[2].startswith("registry+") and not expected:
            raise RuntimeError(f"registry package has no lockfile checksum: {key[0]} {key[1]}")
    digest = hashlib.sha256(lock_path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
    return by_lock, digest


def validate_reviewed_baseline(metadata: dict, lock_packages: dict, baseline_path: Path) -> None:
    workspace_ids = set(metadata.get("workspace_members", []))
    unclassified = [
        f"{item['name']} {item['version']}" for item in metadata["packages"]
        if item.get("source") is None and item["id"] not in workspace_ids
    ]
    if unclassified:
        raise RuntimeError(f"non-workspace packages without a registry source are unsupported: {sorted(unclassified)}")
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    old = {
        (item["name"], item["version"], item.get("source")): item
        for item in baseline["packages"] if item.get("package_kind") == "third-party"
    }
    current = {
        lock_key(item): item for item in metadata["packages"] if item.get("source") is not None
    }
    if set(old) != set(current):
        missing, extra = sorted(set(old) - set(current)), sorted(set(current) - set(old))
        raise RuntimeError(f"third-party packages differ from reviewed inventory; missing={missing[:5]} extra={extra[:5]}")
    for key, reviewed in old.items():
        package = current[key]
        expected = (reviewed.get("checksum"), reviewed.get("license_expression"))
        actual = (lock_packages[key].get("checksum"), package.get("license"))
        if actual != expected:
            raise RuntimeError(f"unreviewed checksum/license change for {key[0]} {key[1]}")


def is_notice_file(relative: str) -> bool:
    path = PurePosixPath(relative)
    return path.suffix.lower() not in SOURCE_SUFFIXES and (
        path.name.lower().startswith(EVIDENCE_PREFIXES) or "licenses" in (part.lower() for part in path.parts[:-1])
    )


def complete_license_directory_evidence(package: dict, rows: list[dict], archive_path: Path | None, helpers) -> list[dict]:
    root = Path(package["manifest_path"]).parent
    known = {row["path"] for row in rows}
    added = []
    # Match the folder name case-insensitively so Linux CI and Windows release runs collect the same files.
    for folder in root.rglob("*"):
        if folder.name.lower() != "licenses" or not folder.is_dir() or folder.is_symlink():
            continue
        for path in folder.rglob("*"):
            if not path.is_file() or path.is_symlink():
                continue
            relative = path.relative_to(root).as_posix()
            if relative not in known:
                added.append({"path": relative, "sha256": helpers.sha256(path)})
    if not added:
        return rows
    if archive_path is not None:
        prefix = root.name + "/"
        try:
            with tarfile.open(archive_path, "r:gz") as archive:
                members = {member.name: member for member in archive.getmembers() if member.isfile()}
                for row in added:
                    member = members.get(prefix + row["path"])
                    if member is None:
                        helpers.set_archive_member_evidence(row, None)
                        continue
                    stream = archive.extractfile(member)
                    if stream is None:
                        raise RuntimeError(f"cannot read crate archive member {member.name}")
                    helpers.set_archive_member_evidence(row, hashlib.sha256(stream.read()).hexdigest())
        except (OSError, tarfile.TarError) as error:
            raise RuntimeError(f"cannot inspect crate archive {archive_path.name}: {error}") from error
    return rows + added


def verified_package(package: dict, locked: dict, helpers) -> tuple[list[dict], dict | None]:
    rows, cache, archive_path = helpers.evidence_for(package)
    rows = complete_license_directory_evidence(package, rows, archive_path, helpers)
    expected = locked.get("checksum")
    if (package.get("source") or "").startswith("registry+"):
        if expected is None or cache is None:
            raise RuntimeError(f"registry source checksum evidence is missing for {package['name']} {package['version']}")
        if cache["kind"] != "crate-archive":
            raise RuntimeError(f"verified .crate archive is required for {package['name']} {package['version']}")
        if cache["sha256"] != expected:
            raise RuntimeError(f"cached crate archive checksum mismatch for {package['name']} {package['version']}")
    return rows, cache


def supplemental_files(package: dict) -> list[Path]:
    """Upstream license texts committed for crates whose published archive ships none (see supplemental-licenses/README.md)."""
    folder = SUPPLEMENTAL_LICENSES / f"{package['name']}-{package['version']}"
    if not folder.is_dir():
        return []
    files = sorted((path for path in folder.iterdir() if path.is_file()), key=lambda path: path.name.casefold())
    return files if any(path.name != "SOURCE.txt" for path in files) and any(path.name == "SOURCE.txt" for path in files) else []


def primary_evidence(rows: list[dict], helpers) -> bool:
    return any(row.get("sha256") and helpers.is_primary_evidence(row["path"]) for row in rows)


def native_policy_supplement() -> str:
    text = NATIVE_POLICY.read_text(encoding="utf-8")
    start = text.index("## Statically linked third-party native code")
    end = text.index("## Release gate checks", start)
    return (text[start:end].strip() + "\n\n## Bundled data attribution\n\n"
            "`webpki-roots` includes root-certificate data under CDLA-Permissive-2.0. Preserve the CCADB provenance and attribution; no endorsement is implied.")


def source_material_supplement() -> str:
    return SOURCE_MATERIAL_NOTICES.read_text(encoding="utf-8").strip()


def render_package_notice(package: dict) -> str:
    header = [f"{package['name']} {package['version']}"]
    if package.get("authors"):
        header.append("Authors: " + ", ".join(sorted(package["authors"])))
    header.append("License: " + (package.get("license_expression") or "UNRESOLVED (manifest has no license expression)"))
    section = "\n".join(header)
    texts = sorted(package.get("license_evidence", []), key=lambda item: (item["path"].casefold(), item["path"]))
    if texts:
        for text in texts:
            section += f"\n\n----- {text['path']} -----\n{text['text']}"
    else:
        section += "\n\n[No primary license/notice text found in the verified package source.]"
    return section


def release_blockers(unresolved: list[str]) -> list[str]:
    return sorted(unresolved, key=str.casefold)


def generate(repo: Path, output_dir: Path) -> tuple[dict, str, list[str]]:
    helpers = _inventory_helpers()
    metadata = cargo_metadata(repo)
    lock_path = repo / "Cargo.lock"
    lock = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    locked_by_key, lock_digest = validate_lock_identity(metadata, lock, lock_path)
    validate_reviewed_baseline(metadata, locked_by_key, BASELINE_INVENTORY)
    baseline_missing = {
        (item["name"], item["version"], item.get("source"))
        for item in json.loads(BASELINE_INVENTORY.read_text(encoding="utf-8"))["packages"]
        if item.get("package_kind") == "third-party" and item.get("license_evidence_status") == "missing"
    }
    rows = []
    notice_sections = []
    unresolved = []
    ordered = sorted(metadata["packages"], key=lambda p: (p["name"].casefold(), p["version"], p.get("source") or ""))
    workspace_ids = set(metadata.get("workspace_members", []))
    for package in ordered:
        key = lock_key(package)
        locked = locked_by_key[key]
        evidence, cache = verified_package(package, locked, helpers)
        supplemental = supplemental_files(package)
        missing_primary = not supplemental and (
            not primary_evidence(evidence, helpers) or (package.get("source") is not None and key in baseline_missing)
        )
        is_workspace = package["id"] in workspace_ids
        if package.get("source") and missing_primary:
            unresolved.append(f"{package['name']} {package['version']}")
        texts = []
        root = Path(package["manifest_path"]).parent
        for row in evidence:
            if not row.get("sha256") or not is_notice_file(row["path"]):
                continue
            path = root.joinpath(*PurePosixPath(row["path"]).parts)
            body = path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n").rstrip()
            texts.append({"path": row["path"], "sha256": row["sha256"], "text": body})
        for path in supplemental:
            body = "\n".join(path.read_text(encoding="utf-8").splitlines()).rstrip()
            texts.append({"path": f"supplemental/{path.name}", "sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(), "text": body})
        texts.sort(key=lambda item: (item["path"].casefold(), item["path"]))
        item = {
            "name": package["name"],
            "version": package["version"],
            "authors": sorted(package.get("authors", [])),
            "source": package.get("source"),
            "checksum": locked.get("checksum"),
            "package_kind": "workspace" if is_workspace else "third-party",
            "license_expression": package.get("license"),
            "license_file": package.get("license_file"),
            "repository": package.get("repository"),
            "license_evidence_status": "present" if not missing_primary else "unresolved",
            "source_integrity_status": "verified" if cache and cache.get("kind") == "crate-archive" else ("checksum-file-verified" if cache else "not-applicable"),
            "license_evidence": texts,
        }
        rows.append(item)
        if not is_workspace:
            notice_sections.append(render_package_notice(item))

    inventory = {
        "schema_version": 1,
        "purpose": "Deterministic full Cargo.lock package and license-text inventory; dependency presence is not binary-link proof",
        "inputs": {"lockfile": {"path": "Cargo.lock", "sha256": lock_digest}},
        "package_counts": {
            "lock_rows": len(rows),
            "third_party": sum(item["package_kind"] == "third-party" for item in rows),
            "workspace": sum(item["package_kind"] == "workspace" for item in rows),
            "unresolved_third_party_evidence": len(unresolved),
        },
        "unresolved_third_party_evidence": unresolved,
        "packages": rows,
    }
    notices = (
        "THIRD-PARTY NOTICES\nGenerated from `cargo metadata --locked --format-version 1`.\n"
        "Package text is reproduced from checksum-verified Cargo sources. This automated inventory is not legal clearance.\n\n"
        + "\n\n".join(notice_sections)
        + "\n\nUNRESOLVED PRIMARY LICENSE/NOTICE EVIDENCE\n"
        + "These packages require review against their exact source before release:\n"
        + "\n".join(f"- {name}" for name in release_blockers(unresolved))
        + "\n\n"
        + native_policy_supplement()
        + "\n\n"
        + source_material_supplement()
        + "\n"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "rust-dependency-inventory.json").write_text(
        json.dumps(inventory, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    (output_dir / "THIRD-PARTY-NOTICES.txt").write_text(notices, encoding="utf-8", newline="\n")
    license_text = (repo / "LICENSE").read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
    (output_dir / "LICENSE").write_text(license_text, encoding="utf-8", newline="\n")
    return inventory, notices, unresolved


def self_test() -> None:
    assert is_notice_file("LICENSE") and is_notice_file("third_party/fiat/LICENSE")
    assert is_notice_file("vendor/components/LICENSES/LGPL-2.1.txt")
    assert not is_notice_file("src/copying.rs") and not is_notice_file("src/license.toml")
    assert native_policy_supplement().startswith("## Statically linked third-party native code")
    source_notices = source_material_supplement()
    for copyright_line in (
        "Copyright (c) 2025 OpenAI",
        "Copyright (c) 2026 Matt Pocock",
        "Copyright (c) 2025 Jesse Vincent",
    ):
        assert copyright_line in source_notices, copyright_line
    assert supplemental_files({"name": "objc2", "version": "0.6.4"}) and not supplemental_files({"name": "nonexistent", "version": "0"})
    nested = {"name": "nested", "version": "1", "authors": ["Z", "A"], "license_expression": "MIT", "license_evidence": [
        {"path": "vendor/native/LICENSE", "text": "native terms"},
    ]}
    first = render_package_notice(nested)
    second = render_package_notice(nested)
    assert first == second and "vendor/native/LICENSE" in first and "native terms" in first
    with tempfile.TemporaryDirectory() as scratch:
        crate = Path(scratch)
        (crate / "licenses").mkdir()
        (crate / "licenses" / "MIT.txt").write_text("mit", encoding="utf-8")
        found = complete_license_directory_evidence(
            {"manifest_path": str(crate / "Cargo.toml")}, [], None, type("Helpers", (), {"sha256": staticmethod(lambda _: "h")})()
        )
        assert found == [{"path": "licenses/MIT.txt", "sha256": "h"}], found
    row = {"sha256": "abc", "path": "LICENSE"}
    assert primary_evidence([row], type("Helpers", (), {"is_primary_evidence": staticmethod(lambda _: True)})())
    assert not primary_evidence([], type("Helpers", (), {"is_primary_evidence": staticmethod(lambda _: True)})())
    package = {"name": "example", "version": "1.0", "source": "registry+test", "manifest_path": str(ROOT / "Cargo.toml")}
    try:
        verified_package(package, {"checksum": "expected"}, type("Helpers", (), {"evidence_for": staticmethod(lambda _: ([], {"kind": "crate-archive", "sha256": "tampered"}, None))})())
    except RuntimeError:
        pass
    else:
        raise AssertionError("tampered archive checksum was accepted")
    try:
        verified_package(package, {"checksum": "expected"}, type("Helpers", (), {"evidence_for": staticmethod(lambda _: ([], {"kind": "cargo-checksum-file", "package_checksum": "expected"}, None))})())
    except RuntimeError:
        pass
    else:
        raise AssertionError("self-asserted checksum-file-only evidence was accepted")
    missing = ["example 1.0"] if not primary_evidence([], type("Helpers", (), {"is_primary_evidence": staticmethod(lambda _: True)})()) else []
    if not release_blockers(missing):
        raise AssertionError("release gate accepted missing primary notice evidence")
    assert release_blockers(["z 1", "A 1"]) == ["A 1", "z 1"]
    try:
        validate_reviewed_baseline(
            {"workspace_members": [], "packages": [{"id": "unreviewed-path", "name": "path-package", "version": "1", "source": None}]},
            {}, Path("unused-baseline.json"),
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("unreviewed source-less package was accepted")
    reviewed = {
        "id": "registry+test#example@1",
        "name": "example",
        "version": "1",
        "source": "registry+test",
        "checksum": "locked-digest",
        "license": "MIT",
    }
    baseline = {"packages": [{
        "name": "example", "version": "1", "source": "registry+test",
        "checksum": "locked-digest", "license_expression": "MIT", "package_kind": "third-party",
    }]}
    temp_root = ROOT / "target"
    temp_root.mkdir(exist_ok=True)
    test_dir = Path(tempfile.mkdtemp(dir=temp_root))
    baseline_path = test_dir / "inventory.json"
    try:
        baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
        validate_reviewed_baseline({"workspace_members": [], "packages": [reviewed]}, {lock_key(reviewed): {"checksum": "locked-digest"}}, baseline_path)
        changed = {**reviewed, "license": "Apache-2.0"}
        try:
            validate_reviewed_baseline({"workspace_members": [], "packages": [changed]}, {lock_key(changed): {"checksum": "locked-digest"}}, baseline_path)
        except RuntimeError:
            pass
        else:
            raise AssertionError("unreviewed license expression change was accepted")
    finally:
        baseline_path.unlink(missing_ok=True)
        test_dir.rmdir()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--output-dir", type=Path, default=Path("target/license-compliance"))
    parser.add_argument("--release", action="store_true", help="return failure when any third-party package lacks primary notice evidence")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        print("self-test passed")
        return 0
    repo = args.repo.resolve()
    output_dir = args.output_dir if args.output_dir.is_absolute() else repo / args.output_dir
    inventory, _notices, unresolved = generate(repo, output_dir)
    print(f"wrote notices and inventory to {output_dir} ({inventory['package_counts']})")
    if args.release and unresolved:
        print(f"release blocked: {len(unresolved)} third-party packages lack primary license/notice evidence", file=sys.stderr)
        for package in unresolved:
            print(f"  {package}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
