"""Unit tests for tools/release/e2e_gate.py summary and step commands."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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


class RepeatTests(unittest.TestCase):
    def test_single_run_keeps_plain_run_ids_and_repeats_are_numbered(self) -> None:
        self.assertEqual(e2e_gate.run_id_for("p", "forward", 1, 1), "p-forward")
        self.assertEqual(e2e_gate.run_id_for("p", "forward", 3, 10), "p-r3-forward")

    def test_failed_iteration_stops_the_loop(self) -> None:
        outcomes = iter([0, 0, 1, 0])  # iteration 2's scenarios step fails
        calls = []

        def fake_run(cmd: list[str]) -> int:
            calls.append(cmd)
            return next(outcomes)

        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            sys, "argv", ["e2e_gate.py", "--scenarios-only", "--skip-build", "--repeat", "10", "--root", tmp]
        ), mock.patch.object(e2e_gate.shutil, "which", return_value="x"), mock.patch.object(
            e2e_gate.subprocess, "run", return_value=mock.Mock(returncode=0)
        ), mock.patch.object(e2e_gate, "git", return_value=""), mock.patch.object(e2e_gate, "run", fake_run):
            self.assertEqual(e2e_gate.main(), 1)
            summary = json.loads(next(Path(tmp).glob("*-summary.json")).read_text(encoding="utf-8"))

        self.assertEqual(len(calls), 4)  # both steps of iterations 1 and 2, nothing after
        self.assertEqual(summary["iterations_run"], 2)
        self.assertEqual([r["passed"] for r in summary["steps"]], [True, True, False, True])

    def test_scenarios_only_rejects_explicit_steps(self) -> None:
        with mock.patch.object(sys, "argv", ["e2e_gate.py", "--scenarios-only", "--step", "crash"]), mock.patch.object(
            sys, "stderr"
        ), self.assertRaises(SystemExit) as raised:
            e2e_gate.main()
        self.assertEqual(raised.exception.code, 2)

    def test_repeat_must_be_positive(self) -> None:
        self.assertEqual(e2e_gate.positive_int("10"), 10)
        with self.assertRaises(argparse.ArgumentTypeError):
            e2e_gate.positive_int("0")


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
