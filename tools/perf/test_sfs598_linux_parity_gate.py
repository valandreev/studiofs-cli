#!/usr/bin/env python3
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "tools" / "perf" / "linux-juicefs-parity-gate.sh"


class Sfs598LinuxParityGateTest(unittest.TestCase):
    def test_help_lists_required_targets_and_juicefs_reference(self) -> None:
        result = subprocess.run(
            ["bash", str(SCRIPT), "--help"],
            cwd=REPO_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )

        self.assertIn("--studiofs-new-target", result.stdout)
        self.assertIn("--juicefs-target", result.stdout)
        self.assertIn("/home/val/dev/juicefs", result.stdout)

    def test_dry_run_writes_gate_artifacts_under_output_root(self) -> None:
        target_tmp = REPO_ROOT / "target"
        target_tmp.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target_tmp) as temp:
            temp_path = Path(temp)
            result = subprocess.run(
                [
                    "bash",
                    str(SCRIPT),
                    "--dry-run",
                    "--runs",
                    "1",
                    "--local-target",
                    str(temp_path / "local"),
                    "--studiofs-new-target",
                    str(temp_path / "new"),
                    "--studiofs-old-target",
                    str(temp_path / "old"),
                    "--juicefs-target",
                    str(temp_path / "juicefs"),
                    "--output-root",
                    str(temp_path / "artifacts"),
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )

            artifact_dir = Path(result.stdout.strip().splitlines()[-1])
            self.assertTrue((artifact_dir / "environment.json").is_file())
            self.assertTrue((artifact_dir / "summary.md").is_file())
            self.assertTrue((artifact_dir / "decision.md").is_file())


if __name__ == "__main__":
    unittest.main()
