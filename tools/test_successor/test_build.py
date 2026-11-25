from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("build.py")
SPEC = importlib.util.spec_from_file_location("test_successor_build", SCRIPT)
build = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(build)


def identity(name: str, current: int, min_supported: int = 1) -> dict:
    return {"name": name, "current": current, "min_supported": min_supported}


def change(change_id: str, kind: str, rollback: str, bump: tuple[str, int, int] | None = None) -> dict:
    return {
        "id": change_id,
        "issue": "SFS-0",
        "kind": kind,
        "rollback": rollback,
        "bump": None if bump is None else {"identity": bump[0], "baseline": bump[1], "successor": bump[2]},
    }


BASELINE = {
    "client_release": "0.2.5",
    "identities": [identity("serving checkpoint format", 4), identity("event codec wire", 1)],
    "successor_changes": [],
}


def successor(identities: list[dict], changes: list[dict]) -> dict:
    return {
        "client_release": "0.2.5",
        "successor_build": build.MARKER,
        "identities": identities,
        "successor_changes": changes,
    }


COMPLETE = [
    change("pg", "postgres_migration", "reversible", ("schema component x", 0, 1)),
    change("event", "event_kind", "reversible"),
    change("checkpoint", "checkpoint_format", "irreversible", ("serving checkpoint format", 4, 5)),
    change("local", "local_format", "irreversible"),
]
COMPLETE_IDENTITIES = [
    identity("serving checkpoint format", 5),
    identity("event codec wire", 1),
    identity("schema component x", 1),
]


class VerifyTests(unittest.TestCase):
    def test_unchanged_identities_without_changes_verify(self) -> None:
        self.assertEqual(build.verify(BASELINE, successor(BASELINE["identities"], [])), [])

    def test_declared_bumps_and_new_identity_verify_complete(self) -> None:
        report = successor(COMPLETE_IDENTITIES, COMPLETE)
        self.assertEqual(build.verify(BASELINE, report, require_complete=True), [])

    def test_undeclared_identity_change_fails(self) -> None:
        report = successor([identity("serving checkpoint format", 5), identity("event codec wire", 1)], [])
        errors = build.verify(BASELINE, report)
        self.assertEqual(errors, ["identity serving checkpoint format: reported 4 -> 5, declared 4 -> 4"])

    def test_declared_bump_that_does_not_happen_fails(self) -> None:
        changes = [change("c", "checkpoint_format", "irreversible", ("serving checkpoint format", 4, 5))]
        errors = build.verify(BASELINE, successor(BASELINE["identities"], changes))
        self.assertEqual(errors, ["identity serving checkpoint format: reported 4 -> 4, declared 4 -> 5"])

    def test_min_supported_change_and_dropped_identity_fail(self) -> None:
        report = successor([identity("serving checkpoint format", 4, min_supported=2)], [])
        errors = build.verify(BASELINE, report)
        self.assertIn("successor dropped identity event codec wire", errors)
        self.assertIn("identity serving checkpoint format: min_supported must not change", errors)

    def test_bump_of_unreported_identity_fails(self) -> None:
        changes = [change("c", "local_format", "irreversible", ("local payload index", 2, 3))]
        errors = build.verify(BASELINE, successor(BASELINE["identities"], changes))
        self.assertEqual(errors, ["declared bump of unknown identity local payload index"])

    def test_marker_and_release_are_checked(self) -> None:
        report = successor(BASELINE["identities"], [])
        del report["successor_build"]
        baseline = copy.deepcopy(BASELINE) | {"successor_build": build.MARKER, "client_release": "0.2.6"}
        errors = build.verify(baseline, report)
        self.assertEqual(len(errors), 3)

    def test_require_complete_lists_missing_kinds_and_rollbacks(self) -> None:
        report = successor(COMPLETE_IDENTITIES, COMPLETE[:3])
        self.assertEqual(build.verify(BASELINE, report, require_complete=True), ["no declared local_format change"])
        errors = build.verify(BASELINE, successor(BASELINE["identities"], []), require_complete=True)
        self.assertEqual(len(errors), 6)


if __name__ == "__main__":
    unittest.main()
