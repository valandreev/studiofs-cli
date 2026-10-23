#!/usr/bin/env python3
"""Generate the pinned SFS-849 Rust dependency inventory using only stdlib."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tarfile
import tomllib
from collections import defaultdict, deque
from pathlib import Path


TARGETS = {
    "windows-x86_64": "x86_64-pc-windows-msvc",
    "linux-x86_64": "x86_64-unknown-linux-gnu",
    "macos-aarch64": "aarch64-apple-darwin",
}
EVIDENCE_NAMES = ("license", "copying", "notice", "copyright", "unlicense")
TREE_ROW = re.compile(r"^(\d+)([A-Za-z0-9_.+-]+) v([^ |]+)(?: \(([^)]*)\))?\|(.*)$")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def lock_key(package: dict) -> tuple[str, str, str | None]:
    return package["name"], package["version"], package.get("source")


def metadata_key(package: dict) -> tuple[str, str, str | None]:
    return package["name"], package["version"], package.get("source")


def parse_tree(text: str) -> dict[tuple[str, str], set[str]]:
    """Read cargo tree's stable depth/name/version/features format."""
    found: dict[tuple[str, str], set[str]] = defaultdict(set)
    for line in text.splitlines():
        match = TREE_ROW.match(line)
        if not match:
            if line.strip():
                raise ValueError(f"unexpected cargo tree output line: {line!r}")
            continue
        name, version, feature_text = match.group(2), match.group(3), match.group(5)
        feature_text = re.sub(r"\s*\(\*\)$", "", feature_text)
        found[(name, version)].update(filter(None, feature_text.split(",")))
    return dict(found)


def _is_proc_macro(package: dict) -> bool:
    return any("proc-macro" in target.get("kind", []) for target in package["targets"])


def target_roles(metadata: dict, root_id: str, allowed_ids: set[str] | None = None) -> dict[str, set[str]]:
    """Propagate candidate roles over only packages shown by studiofs's cargo tree."""
    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    if allowed_ids is None:
        allowed_ids = set(nodes)
    if root_id not in nodes:
        raise RuntimeError(f"studiofs is absent from target-filtered metadata: {root_id}")
    roles: dict[str, set[str]] = defaultdict(set)
    if root_id not in allowed_ids:
        raise RuntimeError("cargo tree package set omitted studiofs")
    queue = deque([(root_id, "runtime")])
    while queue:
        package_id, role = queue.popleft()
        if role in roles[package_id]:
            continue
        roles[package_id].add(role)
        package = packages[package_id]
        for dep in nodes[package_id].get("deps", []):
            child = dep["pkg"]
            if child not in nodes or child not in allowed_ids:
                continue
            for kind in dep.get("dep_kinds", []):
                edge_kind = kind.get("kind") or "normal"
                if edge_kind == "dev":
                    continue
                if role == "runtime" and edge_kind == "normal" and not _is_proc_macro(packages[child]):
                    child_role = "runtime"
                else:
                    child_role = "host-build"
                if child_role not in roles[child]:
                    queue.append((child, child_role))
    return roles


def evidence_for(package: dict) -> tuple[list[dict], dict, Path | None]:
    manifest = Path(package["manifest_path"])
    package_root = manifest.parent
    files: set[Path] = {manifest}
    for path in package_root.rglob("*"):
        if not path.is_file():
            continue
        lowered = path.name.lower()
        if lowered.startswith(EVIDENCE_NAMES):
            files.add(path)
    license_file = package.get("license_file")
    if license_file:
        license_path = Path(license_file)
        if not license_path.is_absolute():
            license_path = package_root / license_path
        files.add(license_path)
    rows = []
    for path in sorted(files, key=lambda item: item.as_posix().lower()):
        if not path.is_file():
            rows.append({"path": path.name, "sha256": None, "status": "missing"})
            continue
        try:
            relative = path.relative_to(package_root).as_posix()
        except ValueError as error:
            raise RuntimeError(f"license evidence escapes package root: {path}") from error
        rows.append({"path": relative, "sha256": sha256(path)})
    checksum_path = package_root / ".cargo-checksum.json"
    cache = None
    archive_path = None
    checksum_content = json.loads(checksum_path.read_text(encoding="utf-8")) if checksum_path.exists() else None
    registry = package_root.parent.parent.parent
    archive = registry / "cache" / package_root.parent.name / f"{package_root.name}.crate"
    if archive.exists():
        archive_path = archive
        cache = {
            "kind": "crate-archive",
            "path": f"registry/cache/crates-io/{archive.name}",
            "sha256": sha256(archive),
        }
    elif checksum_content is not None:
        verified_evidence_count = verify_checksum_file_evidence(checksum_content, rows)
        cache = {
            "kind": "cargo-checksum-file",
            "path": ".cargo-checksum.json",
            "sha256": sha256(checksum_path),
            "package_checksum": checksum_content.get("package"),
            "verified_evidence_file_count": verified_evidence_count,
        }
    if archive_path:
        try:
            with tarfile.open(archive_path, "r:gz") as archive:
                members = {member.name: member for member in archive.getmembers() if member.isfile()}
                prefix = package_root.name + "/"
                for row in rows:
                    member = members.get(prefix + row["path"])
                    if member is None:
                        set_archive_member_evidence(row, None)
                        continue
                    stream = archive.extractfile(member)
                    if stream is None:
                        raise RuntimeError(f"cannot read crate archive member {member.name}")
                    digest = hashlib.sha256()
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(block)
                    set_archive_member_evidence(row, digest.hexdigest())
        except (OSError, tarfile.TarError) as error:
            raise RuntimeError(f"cannot inspect crate archive {archive_path.name}: {error}") from error
    return rows, cache, archive_path


def set_archive_member_evidence(row: dict, archive_sha256: str | None) -> None:
    extracted_sha256 = row.get("sha256")
    if archive_sha256 is None and extracted_sha256 is not None:
        raise RuntimeError(f"extracted evidence is absent from crate archive: {row['path']}")
    if archive_sha256 is not None and extracted_sha256 is not None and extracted_sha256 != archive_sha256:
        raise RuntimeError(f"cached extracted evidence differs from crate archive: {row['path']}")
    row["archive_member_sha256"] = archive_sha256
    row["archive_member_status"] = "verified" if archive_sha256 is not None and extracted_sha256 is not None else "missing-local-evidence"


def verify_checksum_file_evidence(checksum_content: dict, evidence: list[dict]) -> int:
    files = checksum_content.get("files")
    if not isinstance(files, dict):
        raise RuntimeError(".cargo-checksum.json has no file hash map")
    verified = 0
    for row in evidence:
        actual = row.get("sha256")
        if actual is None:
            continue
        expected = files.get(row["path"])
        if expected is None or expected != actual:
            raise RuntimeError(f".cargo-checksum.json evidence mismatch for {row['path']}")
        verified += 1
    return verified


def cargo_output(args: list[str], repo: Path) -> str:
    completed = subprocess.run(
        args, cwd=repo, check=True, text=True, encoding="utf-8", errors="replace",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if completed.stderr.strip():
        print(completed.stderr.strip(), file=sys.stderr)
    return completed.stdout


def portable_identity(package: dict, repo: Path) -> dict:
    identity = {"name": package["name"], "version": package["version"], "source": package.get("source")}
    if package.get("source") is None:
        try:
            identity["workspace_path"] = Path(package["manifest_path"]).relative_to(repo).parent.as_posix()
        except ValueError as error:
            raise RuntimeError(f"workspace manifest escapes repository: {package['manifest_path']}") from error
    return identity


def generate(repo: Path, metadata_path: Path, lock_path: Path) -> dict:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    lock = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    lock_packages = lock["package"]
    package_by_key = {metadata_key(package): package for package in metadata["packages"]}
    lock_keys = [lock_key(package) for package in lock_packages]
    if len(lock_keys) != len(set(lock_keys)):
        raise RuntimeError("Cargo.lock contains duplicate package identities")
    if set(lock_keys) != set(package_by_key):
        missing = sorted(set(lock_keys) - set(package_by_key))
        extra = sorted(set(package_by_key) - set(lock_keys))
        raise RuntimeError(f"lock/metadata mismatch; missing metadata={missing}; extra metadata={extra}")
    metadata_by_id = {package["id"]: package for package in metadata["packages"]}
    if len(metadata_by_id) != len(metadata["packages"]):
        raise RuntimeError("metadata contains duplicate package IDs")
    studiofs = [p for p in metadata["packages"] if p["name"] == "studiofs" and p["manifest_path"].replace("\\", "/").endswith("crates/studiofs/Cargo.toml")]
    if len(studiofs) != 1:
        raise RuntimeError(f"expected one crates/studiofs package, found {len(studiofs)}")
    root_id = studiofs[0]["id"]
    workspace_ids = set(metadata["workspace_members"])

    incoming = defaultdict(dict)
    for node in metadata["resolve"]["nodes"]:
        parent = metadata_by_id.get(node["id"])
        if parent is None:
            raise RuntimeError(f"resolve graph references an unknown parent package: {node['id']}")
        for dependency in node.get("deps", []):
            child_id = dependency["pkg"]
            child = metadata_by_id.get(child_id)
            if child is None:
                raise RuntimeError(f"resolve graph references an unknown dependency package: {child_id}")
            kinds = [
                {"kind": dep_kind.get("kind") or "normal", "target": dep_kind.get("target")}
                for dep_kind in dependency.get("dep_kinds", [])
            ] or [{"kind": "normal", "target": None}]
            edge = {"from": portable_identity(parent, repo), "dependency_kinds": sorted(kinds, key=lambda item: (item["kind"], item["target"] or ""))}
            incoming[child_id][json.dumps(edge, sort_keys=True, separators=(",", ":"))] = edge

    targets = {}
    per_package: dict[str, dict] = {}
    for target_name, triple in TARGETS.items():
        filtered_text = cargo_output(
            ["cargo", "metadata", "--locked", "--offline", "--format-version", "1", "--filter-platform", triple], repo
        )
        filtered = json.loads(filtered_text)
        filtered_studiofs = [p for p in filtered["packages"] if p["id"] == root_id]
        if len(filtered_studiofs) != 1:
            raise RuntimeError(f"{target_name}: filtered metadata did not preserve studiofs identity")
        tree_text = cargo_output(
            ["cargo", "tree", "-p", "studiofs", "--locked", "--offline", "--target", triple,
             "-e", "normal,build", "--prefix", "depth", "--format", "{p}|{f}"], repo
        )
        tree_features = parse_tree(tree_text)
        if not tree_features:
            raise RuntimeError(f"{target_name}: cargo tree produced no package rows")
        filtered_by_pair = defaultdict(list)
        for package in filtered["packages"]:
            filtered_by_pair[(package["name"], package["version"])].append(package["id"])
        missing_metadata = sorted(set(tree_features) - set(filtered_by_pair))
        ambiguous_metadata = sorted(key for key in tree_features if len(filtered_by_pair.get(key, [])) != 1)
        if missing_metadata or ambiguous_metadata:
            raise RuntimeError(f"{target_name}: Cargo tree packages not uniquely covered by target metadata; missing={missing_metadata}; ambiguous={ambiguous_metadata}")
        allowed_ids = {filtered_by_pair[key][0] for key in tree_features}
        roles = target_roles(filtered, root_id, allowed_ids)
        ids_by_name_version = defaultdict(list)
        for package_id in roles:
            package = next(p for p in filtered["packages"] if p["id"] == package_id)
            ids_by_name_version[(package["name"], package["version"])].append(package_id)
        if set(ids_by_name_version) != set(tree_features):
            missing_tree = sorted(set(ids_by_name_version) - set(tree_features))
            missing_graph = sorted(set(tree_features) - set(ids_by_name_version))
            raise RuntimeError(f"{target_name}: candidate role graph does not cover cargo tree; extra={missing_tree}; missing={missing_graph}")
        target_packages = {}
        for key, ids in ids_by_name_version.items():
            if len(ids) != 1:
                raise RuntimeError(f"{target_name}: ambiguous package name/version in target graph: {key}")
            package_id = ids[0]
            target_packages[package_id] = {
                "roles": sorted(roles[package_id]),
                "enabled_features": sorted(tree_features[key]),
            }
            per_package.setdefault(package_id, {})[target_name] = target_packages[package_id]
        targets[target_name] = {
            "triple": triple,
            "selection_source": "cargo tree -e normal,build package set and features; roles propagated through target-filtered metadata restricted to that set",
            "package_count": len(target_packages),
        }

    inventory = []
    for lock_package in sorted(lock_packages, key=lambda p: (p["name"], p["version"], p.get("source") or "")):
        package = package_by_key[lock_key(lock_package)]
        is_workspace = package["id"] in workspace_ids
        evidence, cache, _archive_path = evidence_for(package)
        expected_checksum = lock_package.get("checksum")
        integrity = "not-applicable"
        if expected_checksum and cache:
            if cache["kind"] == "cargo-checksum-file":
                actual = cache.get("package_checksum")
                if actual is not None and actual != expected_checksum:
                    raise RuntimeError(f"cached registry checksum mismatch for {package['name']} {package['version']}")
                integrity = "unresolved-archive-verification"
            elif cache["kind"] == "crate-archive":
                actual = cache["sha256"]
                if actual != expected_checksum:
                    raise RuntimeError(f"cached registry checksum mismatch for {package['name']} {package['version']}")
                integrity = "verified"
            else:
                integrity = "unresolved-cache-source-integrity"
        elif expected_checksum:
            integrity = "unresolved-cache-source-integrity"
        try:
            manifest_rel = Path(package["manifest_path"]).relative_to(repo).as_posix()
        except ValueError:
            manifest_rel = f"registry:{package['source']}#{package['name']}@{package['version']}/Cargo.toml"
        license_expr = package.get("license")
        expression_id = "missing-license" if not license_expr else "spdx:" + hashlib.sha256(license_expr.encode("utf-8")).hexdigest()[:16]
        incoming_edges = sorted(incoming.get(package["id"], {}).values(), key=lambda edge: json.dumps(edge, sort_keys=True, separators=(",", ":")))
        incoming_kinds = {kind["kind"] for edge in incoming_edges for kind in edge["dependency_kinds"]}
        incoming_targets = {kind["target"] for edge in incoming_edges for kind in edge["dependency_kinds"] if kind["target"]}
        scope_hints = []
        if "dev" in incoming_kinds:
            scope_hints.append("has-workspace-or-dependency-dev-edge")
        if "build" in incoming_kinds:
            scope_hints.append("has-build-edge")
        if incoming_targets:
            scope_hints.append("has-target-conditioned-edge")
        if not incoming_edges:
            scope_hints.append("no-resolved-incoming-edge")
        inventory.append({
            "name": package["name"],
            "version": package["version"],
            "source": lock_package.get("source"),
            "checksum": expected_checksum,
            "package_kind": "workspace" if is_workspace else "third-party",
            "license_expression": license_expr,
            "license_expression_id": expression_id,
            "repository": package.get("repository"),
            "manifest": {"path": manifest_rel, "sha256": sha256(Path(package["manifest_path"]))},
            "license_evidence": evidence,
            "cargo_cache_checksum_evidence": cache,
            "source_integrity_status": integrity,
            "license_evidence_status": "missing" if not license_expr or not any(row.get("sha256") and Path(row.get("path", "")).name.lower().startswith(EVIDENCE_NAMES) for row in evidence) else "present",
            "distribution_obligation_profile": expression_id,
            "distribution_obligation_profile_reference": "docs/legal/rust-dependency-audit.md#distribution-profiles",
            "target_scope_classification": "selected-for-studiofs-target" if per_package.get(package["id"]) else "outside-selected-studiofs-targets",
            "nonproduction_scope_hints": scope_hints,
            "resolved_incoming_dependency_edges": incoming_edges,
            "targets": per_package.get(package["id"], {}),
        })
    if len(inventory) != len(lock_packages) or len({(p["name"], p["version"], p["source"]) for p in inventory}) != len(inventory):
        raise RuntimeError("inventory rows are not a one-to-one representation of Cargo.lock")
    return {
        "schema_version": 1,
        "purpose": "SFS-849 static pinned Rust license audit; target graphs are dependency scope evidence, not final binary-link proof",
        "inputs": {
            "lockfile": {"path": "Cargo.lock", "sha256": sha256(lock_path)},
            "metadata_snapshot": {"path": "target/sfs-849/metadata.json"},
        },
        "target_scopes": targets,
        "package_counts": {
            "lock_rows": len(inventory),
            "registry_third_party": sum(p["package_kind"] == "third-party" for p in inventory),
            "workspace": sum(p["package_kind"] == "workspace" for p in inventory),
        },
        "packages": inventory,
    }


def self_test() -> None:
    parsed = parse_tree("0studiofs v1.0.0 (repo)|\n1serde v1.0.0|derive,std\n2serde v1.0.0|derive,std (*)\n")
    assert parsed == {("studiofs", "1.0.0"): set(), ("serde", "1.0.0"): {"derive", "std"}}
    try:
        parse_tree("0studiofs v1.0.0 (repo)|\nunexpected cargo tree output\n")
    except ValueError:
        pass
    else:
        raise AssertionError("parse_tree accepted an unrecognized nonempty line")
    assert verify_checksum_file_evidence({"files": {"LICENSE": "abc"}}, [{"path": "LICENSE", "sha256": "abc"}]) == 1
    try:
        verify_checksum_file_evidence({"files": {"LICENSE": "expected"}}, [{"path": "LICENSE", "sha256": "tampered"}])
    except RuntimeError:
        pass
    else:
        raise AssertionError("checksum fallback accepted tampered evidence")
    set_archive_member_evidence({"path": "LICENSE", "sha256": "abc"}, "abc")
    try:
        set_archive_member_evidence({"path": "LICENSE", "sha256": "abc"}, None)
    except RuntimeError:
        pass
    else:
        raise AssertionError("archive verification accepted an absent evidence member")
    assert lock_key({"name": "x", "version": "1", "source": None}) == ("x", "1", None)

    def package(package_id: str, proc_macro: bool = False) -> dict:
        return {"id": package_id, "targets": [{"kind": ["proc-macro"] if proc_macro else ["lib"]}]}

    def edge(package_id: str, kind: str | None = None) -> dict:
        return {"pkg": package_id, "dep_kinds": [{"kind": kind, "target": None}]}

    graph_packages = [
        package("root"), package("runtime"), package("runtime-cycle"), package("macro", True),
        package("macro-child"), package("build"), package("build-child"), package("shared"), package("dev"),
    ]
    graph_nodes = [
        {"id": "root", "deps": [edge("runtime"), edge("macro"), edge("build", "build"), edge("dev", "dev")]},
        {"id": "runtime", "deps": [edge("runtime-cycle"), edge("shared")]},
        {"id": "runtime-cycle", "deps": [edge("runtime")]},
        {"id": "macro", "deps": [edge("macro-child")]},
        {"id": "macro-child", "deps": []},
        {"id": "build", "deps": [edge("build-child"), edge("shared")]},
        {"id": "build-child", "deps": [edge("build")]},
        {"id": "shared", "deps": []},
        {"id": "dev", "deps": []},
    ]
    roles = target_roles({"packages": graph_packages, "resolve": {"nodes": graph_nodes}}, "root")
    assert roles == {
        "root": {"runtime"}, "runtime": {"runtime"}, "runtime-cycle": {"runtime"},
        "macro": {"host-build"}, "macro-child": {"host-build"},
        "build": {"host-build"}, "build-child": {"host-build"},
        "shared": {"runtime", "host-build"},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--metadata", type=Path, default=Path("target/sfs-849/metadata.json"))
    parser.add_argument("--lock", type=Path, default=Path("Cargo.lock"))
    parser.add_argument("--output", type=Path, default=Path("docs/legal/rust-dependency-inventory.json"))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        print("self-test passed")
        return 0
    repo = args.repo.resolve()
    metadata = args.metadata if args.metadata.is_absolute() else repo / args.metadata
    lock = args.lock if args.lock.is_absolute() else repo / args.lock
    output = args.output if args.output.is_absolute() else repo / args.output
    document = generate(repo, metadata, lock)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {output}: {document['package_counts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
