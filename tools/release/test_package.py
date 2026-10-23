from __future__ import annotations

import hashlib
import importlib.util
import json
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path


SCRIPT = Path(__file__).with_name("package.py")
SPEC = importlib.util.spec_from_file_location("release_package", SCRIPT)
package = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(package)


TARGETS = (
    "x86_64-pc-windows-msvc",
    "aarch64-pc-windows-msvc",
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "aarch64-apple-darwin",
)


class ReleasePackageTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path(__file__).resolve().parents[2] / "target"
        temp_root.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=temp_root)
        self.repo = Path(self.temp.name)
        self.target = TARGETS[0]
        (self.repo / "Cargo.toml").write_text(
            '[workspace.package]\nversion = "9.8.7"\nrepository = "https://github.com/valandreev/studiofs-cli"\n',
            encoding="utf-8",
        )
        self.lock = b"version = 4\n\n[[package]]\nname = \"fixture\"\n"
        (self.repo / "Cargo.lock").write_bytes(self.lock)
        self.binary = self.repo / "target" / self.target / "release" / "studiofs.exe"
        self.binary.parent.mkdir(parents=True)
        self.binary.write_bytes(b"fake executable contents")
        self.notices = self.repo / "target" / "license-compliance"
        self.notices.mkdir(parents=True)
        (self.notices / "LICENSE").write_text("license text\n", encoding="utf-8")
        (self.notices / "THIRD-PARTY-NOTICES.txt").write_text("notices\n", encoding="utf-8")
        inventory = {
            "package_counts": {"unresolved_third_party_evidence": 0},
            "inputs": {"lockfile": {"sha256": hashlib.sha256(self.lock).hexdigest()}},
        }
        (self.notices / "rust-dependency-inventory.json").write_text(
            json.dumps(inventory), encoding="utf-8"
        )
        (self.repo / "docs" / "legal").mkdir(parents=True)
        (self.repo / "docs" / "releases.md").write_text(
            "release instructions\n[platform](legal/platform-distribution.md#mount)\n"
            "[external](https://example.com/doc) [anchor](#local)\n",
            encoding="utf-8",
        )
        (self.repo / "docs" / "legal" / "platform-distribution.md").write_text(
            "platform requirements\n[license](../../LICENSE)\n", encoding="utf-8"
        )
        self.output = self.repo / "artifacts"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def package(self, target: str | None = None) -> Path:
        return package.create_package(
            self.repo,
            target or self.target,
            self.notices,
            self.output,
            source_commit="0123456789abcdef",
            rustc_version="rustc 1.95.0 (fixture)",
        )

    def test_windows_zip_contains_only_allowlisted_files_and_build_metadata(self) -> None:
        archive_path = self.package()
        self.assertEqual(archive_path.name, "studiofs-9.8.7-x86_64-pc-windows-msvc.zip")
        folder = "studiofs-9.8.7-x86_64-pc-windows-msvc"
        expected = {
            f"{folder}/studiofs.exe",
            f"{folder}/LICENSE",
            f"{folder}/THIRD-PARTY-NOTICES.txt",
            f"{folder}/rust-dependency-inventory.json",
            f"{folder}/INSTALL.md",
            f"{folder}/PLATFORM-DISTRIBUTION.md",
            f"{folder}/BUILD-INFO.json",
        }
        with zipfile.ZipFile(archive_path) as archive:
            self.assertEqual(set(archive.namelist()), expected)
            self.assertEqual(archive.read(f"{folder}/studiofs.exe"), self.binary.read_bytes())
            build_info = json.loads(archive.read(f"{folder}/BUILD-INFO.json"))
            install = archive.read(f"{folder}/INSTALL.md").decode("utf-8")
            platform = archive.read(f"{folder}/PLATFORM-DISTRIBUTION.md").decode("utf-8")
        source = "https://github.com/valandreev/studiofs-cli/blob/0123456789abcdef/"
        self.assertIn(f"[platform]({source}docs/legal/platform-distribution.md#mount)", install)
        self.assertIn("[external](https://example.com/doc)", install)
        self.assertIn("[anchor](#local)", install)
        self.assertIn(f"[license]({source}LICENSE)", platform)
        self.assertEqual(build_info["version"], "9.8.7")
        self.assertEqual(build_info["target"], self.target)
        self.assertEqual(build_info["source_commit"], "0123456789abcdef")
        self.assertEqual(
            build_info["build_command"],
            f"cargo build -p studiofs --release --locked --target {self.target}",
        )
        self.assertEqual(build_info["binary_sha256"], hashlib.sha256(self.binary.read_bytes()).hexdigest())
        self.assertEqual(build_info["cargo_lock_sha256"], hashlib.sha256(self.lock).hexdigest())
        self.assertEqual(build_info["rustc_version"], "rustc 1.95.0 (fixture)")
        sidecar = archive_path.with_name(archive_path.name + ".sha256")
        self.assertEqual(
            sidecar.read_text(encoding="ascii"),
            f"{hashlib.sha256(archive_path.read_bytes()).hexdigest()}  {archive_path.name}\n",
        )

    def test_unix_tar_contains_allowlist_and_preserves_executable_mode(self) -> None:
        self.target = "aarch64-apple-darwin"
        self.binary = self.repo / "target" / self.target / "release" / "studiofs"
        self.binary.parent.mkdir(parents=True)
        self.binary.write_bytes(b"fake unix executable")
        self.binary.chmod(0o644)
        archive_path = self.package()
        self.assertTrue(archive_path.name.endswith(".tar.gz"))
        folder = "studiofs-9.8.7-aarch64-apple-darwin"
        with tarfile.open(archive_path, "r:gz") as archive:
            members = archive.getmembers()
            self.assertEqual(
                {member.name for member in members},
                {
                    f"{folder}/studiofs",
                    f"{folder}/LICENSE",
                    f"{folder}/THIRD-PARTY-NOTICES.txt",
                    f"{folder}/rust-dependency-inventory.json",
                    f"{folder}/INSTALL.md",
                    f"{folder}/PLATFORM-DISTRIBUTION.md",
                    f"{folder}/BUILD-INFO.json",
                },
            )
            binary = next(member for member in members if member.name.endswith("/studiofs"))
            self.assertEqual(binary.mode & 0o777, 0o755)
            self.assertEqual(archive.extractfile(binary).read(), self.binary.read_bytes())

    def test_unresolved_inventory_evidence_blocks_packaging(self) -> None:
        inventory_path = self.notices / "rust-dependency-inventory.json"
        inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
        inventory["package_counts"]["unresolved_third_party_evidence"] = 1
        inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unresolved"):
            self.package()

    def test_inventory_lock_hash_must_match_current_lock(self) -> None:
        inventory_path = self.notices / "rust-dependency-inventory.json"
        inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
        inventory["inputs"]["lockfile"]["sha256"] = "0" * 64
        inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Cargo.lock"):
            self.package()


if __name__ == "__main__":
    unittest.main()
