"""Unit tests for the published-binary helpers in run_compat_matrix.py."""

from __future__ import annotations

import io
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_compat_matrix as rcm  # noqa: E402


class HostKeyTests(unittest.TestCase):
    def test_normalizes_os_and_cpu_names(self) -> None:
        for plat, machine, expected in (
            ("win32", "AMD64", ("windows", "x86_64")),
            ("win32", "ARM64", ("windows", "aarch64")),
            ("linux", "x86_64", ("linux", "x86_64")),
            ("darwin", "arm64", ("macos", "aarch64")),
        ):
            with mock.patch.object(rcm.sys, "platform", plat), mock.patch.object(
                rcm.platform, "machine", return_value=machine
            ):
                self.assertEqual(rcm.host_key(), expected)

    def test_every_asset_matches_its_tag_and_host(self) -> None:
        for (tag, os_name, cpu), (name, digest) in rcm.PUBLISHED_ASSETS.items():
            self.assertIn(tag.removeprefix("v"), name)
            self.assertIn(cpu, name)
            self.assertIn({"windows": "windows", "linux": "linux", "macos": "darwin"}[os_name], name)
            self.assertEqual(len(digest), 64)


class ArchiveTests(unittest.TestCase):
    def test_reads_studiofs_from_zip_and_tar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "a.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("pkg/studiofs.exe", b"win")
                zf.writestr("pkg/README.md", b"doc")
            self.assertEqual(rcm.read_studiofs_from_archive(zip_path, "studiofs.exe"), b"win")

            tar_path = Path(tmp) / "a.tar.gz"
            with tarfile.open(tar_path, "w:gz") as tf:
                info = tarfile.TarInfo("pkg/bin/studiofs")
                info.size = 4
                tf.addfile(info, io.BytesIO(b"unix"))
            self.assertEqual(rcm.read_studiofs_from_archive(tar_path, "studiofs"), b"unix")

    def test_rejects_archive_without_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "a.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("README.md", b"doc")
            with self.assertRaises(ValueError):
                rcm.read_studiofs_from_archive(zip_path, "studiofs.exe")


if __name__ == "__main__":
    unittest.main()
