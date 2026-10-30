#!/usr/bin/env python3
"""Self-check the pinned upstream FUSE subtype compatibility patch."""
from pathlib import Path
import os
import subprocess
import unittest


REPO = Path(__file__).resolve().parents[2]
UPSTREAM = REPO / "target/xfstests/tools/xfstests"
PINNED_REVISION = "22348afe338c0f6d540c0d7ef0db749eaa51b218"


class FstestsFuseSubtypeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not (UPSTREAM / "common/rc").is_file():
            raise unittest.SkipTest("pinned xfstests checkout is absent")
        revision = subprocess.run(
            ["git", "-C", str(UPSTREAM), "rev-parse", "HEAD"],
            check=True, capture_output=True, text=True).stdout.strip()
        if revision != PINNED_REVISION:
            raise unittest.SkipTest("xfstests checkout is not the pinned revision")

    def test_only_studiofs_fuse_subtype_normalizes_to_fuse(self):
        source = (UPSTREAM / "common/rc").read_text()
        start = source.index("_fs_type()\n")
        end = source.index("\n}\n", start) + len("\n}\n")
        function = source[start:end]
        cases = {
            "fuse.studiofs": "fuse",
            "fuse.sshfs": "fuse.sshfs",
            "fuse.glusterfs": "glusterfs",
            "fuse.ceph-fuse": "ceph-fuse",
        }

        for input_type, expected in cases.items():
            with self.subTest(input_type=input_type):
                result = subprocess.run(
                    ["bash", "-c", function + "\n_df_device() { printf 'dev %s\\n' \"$MOCK_FS_TYPE\"; }\nAWK_PROG=awk\n_fs_type test"],
                    env=dict(os.environ, MOCK_FS_TYPE=input_type),
                    check=True, capture_output=True, text=True)
                self.assertEqual(result.stdout.strip(), expected)


if __name__ == "__main__":
    unittest.main()
