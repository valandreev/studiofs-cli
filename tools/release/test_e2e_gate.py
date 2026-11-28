"""Unit tests for tools/release/e2e_gate.py summary and step commands."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e2e_gate  # noqa: E402


def gate_args(**overrides) -> argparse.Namespace:
    values = {"root": "target/e2e-runs", "platform": "auto", "retain": "on-failure", "port_range": None, "timeout": None}
    values.update(overrides)
    return argparse.Namespace(**values)


class StepCommandTests(unittest.TestCase):
    def test_scenarios_step_runs_release_gate_selector_with_release_binary(self) -> None:
        cmd = e2e_gate.step_command("scenarios", gate_args(timeout="30m"), "p-scenarios")
        self.assertEqual(cmd[1:4], ["run", "--scenario", "release-gate"])
        self.assertIn(str(Path("target") / "release" / f"studiofs{e2e_gate.EXE}"), cmd[cmd.index("--studiofs-bin") + 1])
        self.assertEqual(cmd[cmd.index("--timeout") + 1], "30m")
        self.assertEqual(cmd[cmd.index("--run-id") + 1], "p-scenarios")

    def test_matrix_steps_skip_build_and_only_crash_injects_faults(self) -> None:
        forward = e2e_gate.step_command("forward", gate_args(), "p-forward")
        crash = e2e_gate.step_command("crash", gate_args(), "p-crash")
        self.assertIn("--skip-build", forward)
        self.assertNotIn("--crash-rollback", forward)
        self.assertIn("--crash-rollback", crash)
        self.assertNotIn("--timeout", forward)


class SummarizeTests(unittest.TestCase):
    def test_reports_failed_and_skipped_scenarios_and_incomplete_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "p-forward"
            (run_dir / e2e_gate.MATRIX_ARTIFACT).parent.mkdir(parents=True)
            report = {
                "status": "failed",
                "scenarios": [
                    {"name": "a", "status": "passed"},
                    {"name": "b", "status": "failed", "error": "boom"},
                    {"name": "c", "status": "skipped", "error": "needs macOS"},
                ],
            }
            (run_dir / "report.json").write_text(json.dumps(report), encoding="utf-8")
            matrix = {"summary": {"total_cells": 8, "passed": 7, "failed": 0, "skipped": 1}}
            (run_dir / e2e_gate.MATRIX_ARTIFACT).write_text(json.dumps(matrix), encoding="utf-8")

            result = e2e_gate.summarize("forward", run_dir, 1)

        self.assertFalse(result["passed"])
        self.assertEqual(result["scenarios"], 3)
        self.assertEqual(result["failed"], [{"name": "b", "error": "boom"}])
        self.assertEqual(result["skipped"], [{"name": "c", "error": "needs macOS"}])
        self.assertFalse(result["complete"])

    def test_missing_run_dir_still_reports_exit_code(self) -> None:
        result = e2e_gate.summarize("crash", Path(tempfile.gettempdir()) / "no-such-gate-run", 2)
        self.assertEqual(result, {"step": "crash", "run_id": "no-such-gate-run", "exit_code": 2, "passed": False})


if __name__ == "__main__":
    unittest.main()
