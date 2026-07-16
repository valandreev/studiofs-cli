#!/usr/bin/env python3
import hashlib
import json
import os
import shutil
import subprocess
import time
import unittest
from pathlib import Path

from tools.perf import linux_cache_gate_summary as gate


REQUIRED_TESTS = (
    ("direct-serial", "low_level::tests::mounted_smoke_mounts_and_unmounts_cleanly"),
    (
        "direct-serial",
        "low_level::tests::mounted_regular_file_io_is_visible_across_reopen",
    ),
    (
        "direct-serial",
        "low_level::tests::mounted_namespace_mutations_are_visible_through_readdir",
    ),
    (
        "direct-serial",
        "low_level::tests::mounted_direct_rollback_preserves_reopen_visibility",
    ),
    (
        "direct-async",
        "low_level::tests::mounted_same_file_parallel_direct_writes_preserve_disjoint_regions",
    ),
    (
        "direct-async",
        "low_level::tests::mounted_forced_unmount_drains_admitted_and_waiting_raw_requests",
    ),
    (
        "direct-async",
        "low_level::tests::mounted_cross_file_fsync_and_close_do_not_share_async_watermark",
    ),
    ("kernel", "low_level::tests::mounted_kernel_cache_coherence_matrix"),
    (
        "kernel",
        "low_level::tests::mounted_kernel_writeback_flush_and_release_are_locally_durable",
    ),
    (
        "kernel",
        "low_level::tests::mounted_kernel_remote_listener_invalidation",
    ),
)


def mode_environment(mode: str) -> dict[str, str]:
    if mode == "direct-serial":
        return {
            "STUDIOFS_LINUX_FUSE_CACHE_POLICY": "direct",
            "STUDIOFS_FUSE_ASYNC_DIO": "0",
        }
    if mode == "direct-async":
        return {
            "STUDIOFS_LINUX_FUSE_CACHE_POLICY": "direct",
            "STUDIOFS_FUSE_ASYNC_DIO": "1",
        }
    environment = {
        "STUDIOFS_LINUX_FUSE_CACHE_POLICY": "kernel",
        "STUDIOFS_FUSE_ASYNC_DIO": "0",
    }
    return environment


def correctness_manifest(log_root: Path) -> dict:
    log_root.mkdir(parents=True, exist_ok=True)
    tests = []
    for mode, name in REQUIRED_TESTS:
        environment = mode_environment(mode)
        if name.endswith("mounted_kernel_remote_listener_invalidation"):
            environment = {
                **environment,
                "STUDIOFS_REQUIRE_REMOTE_INVALIDATION_TEST": "1",
            }
        log_path = log_root / f"{name.replace('::', '-')}.log"
        log_path.write_text(f"test {name} ... ok\n\ntest result: ok. 1 passed\n")
        tests.append(
            {
                "name": name,
                "mode": mode,
                "environment": environment,
                "command": f"cargo test {name} -- --exact --ignored",
                "started_at": "2026-07-15T00:00:00Z",
                "ended_at": "2026-07-15T00:01:00Z",
                "exit_status": 0,
                "log_path": str(log_path.resolve()),
                "log_sha256": hashlib.sha256(log_path.read_bytes()).hexdigest(),
                "executed": True,
                "skipped": False,
                "applicable": True,
            }
        )
    return {
        "schema_version": 1,
        "kind": "studiofs-linux-cache-correctness",
        "dry_run": False,
        "promotion_eligible": True,
        "valid": True,
        "git_revision": "studio-revision",
        "git_dirty": False,
        "studiofs_sha256": "a" * 64,
        "bench_git_revision": "bench-revision",
        "bench_git_dirty": False,
        "bench_sha256": "b" * 64,
        "tests": tests,
    }


def schedule_manifest(repetitions: int = 5, diagnostic: bool = False) -> dict:
    tuples = []
    schedule_index = -1
    modes = ("direct-serial", "direct-async", "kernel")
    workloads = (
        "large-single",
        "fixed-1m",
        "small-128k",
        "warm-reopen",
        "same-file-parallel",
        "cross-file-barriers",
    )
    for repetition in range(1, repetitions + 1):
        shift = (repetition - 1) % len(workloads)
        mode_shift = (repetition - 1) % len(modes)
        for workload_position in range(len(workloads)):
            workload = workloads[(workload_position + shift) % len(workloads)]
            for mode_position in range(len(modes)):
                mode = modes[(mode_position + mode_shift) % len(modes)]
                schedule_index += 1
                artifact_dir = f"/matrix/{mode}/{workload}/rep-{repetition:03d}"
                tuples.append(
                    {
                        "schedule_index": schedule_index,
                        "repetition": repetition,
                        "workload": workload,
                        "mode": mode,
                        "mount_root": "/matrix/mounts",
                        "mount_point": f"/matrix/mounts/mount-{schedule_index}",
                        "cache_dir": f"/matrix/cache/cache-{schedule_index}",
                        "cgroup": f"/sys/fs/cgroup/gate-{schedule_index}",
                        "artifact_dir": artifact_dir,
                        "mount_command": "studiofs mount",
                        "collector_command": "linux-cache-gate.sh",
                        "cleanup_actions": ["unmount", "remove-cgroup"],
                        "status": "passed",
                        "collector_exit_status": 0,
                        "cleanup_error": None,
                    }
                )
    return {
        "schema_version": 1,
        "kind": "studiofs-linux-cache-matrix-schedule",
        "status": "completed",
        "dry_run": False,
        "repetitions": repetitions,
        "diagnostic": diagnostic,
        "promotion_eligible": repetitions == 5 and not diagnostic,
        "expected_tuple_count": len(tuples),
        "mode_rotation": "three-row Latin square",
        "workload_rotation": "cyclic by repetition",
        "tuples": tuples,
    }


class LinuxCacheGateImportTest(unittest.TestCase):
    def test_summary_module_is_importable(self) -> None:
        self.assertIsNotNone(gate)


class StatisticsTest(unittest.TestCase):
    def test_median_and_mad_are_correct_for_odd_and_even_samples(self) -> None:
        self.assertEqual(gate.median_mad([1, 2, 9]), {"median": 2, "mad": 1})
        self.assertEqual(
            gate.median_mad([1, 2, 8, 9]),
            {"median": 5.0, "mad": 3.5},
        )

    def test_five_percent_throughput_boundary_passes_and_larger_drop_fails(self) -> None:
        baseline = [100.0, 100.0, 100.0]
        self.assertTrue(gate.throughput_passes(baseline, [95.0, 95.0, 95.0]))
        self.assertFalse(gate.throughput_passes(baseline, [94.9, 94.9, 94.9]))

    def test_robust_upper_gate_uses_combined_mad_and_floors(self) -> None:
        baseline = [9.0, 10.0, 11.0]
        candidate = [10.0, 11.0, 12.0]
        result = gate.robust_upper_gate(
            baseline,
            candidate,
            relative_floor=0.10,
            absolute_floor=0.25,
        )
        self.assertTrue(result["passed"])
        self.assertEqual(result["allowance"], 4.0)

        failed = gate.robust_upper_gate(
            baseline,
            [15.0, 16.0, 17.0],
            relative_floor=0.10,
            absolute_floor=0.25,
        )
        self.assertFalse(failed["passed"])

    def test_throughput_improvement_requires_exceeding_combined_mad(self) -> None:
        baseline = [99.0, 100.0, 101.0]
        boundary = gate._throughput_gate(baseline, [101.0, 102.0, 103.0])
        improved = gate._throughput_gate(baseline, [102.0, 103.0, 104.0])

        self.assertFalse(boundary["improved"])
        self.assertTrue(improved["improved"])

    def test_lower_is_better_improvement_requires_exceeding_combined_mad(self) -> None:
        baseline = [99.0, 100.0, 101.0]
        boundary = gate.robust_upper_gate(
            baseline,
            [97.0, 98.0, 99.0],
            relative_floor=0.0,
        )
        improved = gate.robust_upper_gate(
            baseline,
            [96.0, 97.0, 98.0],
            relative_floor=0.0,
        )

        self.assertEqual(boundary["baseline_mad"], 1.0)
        self.assertEqual(boundary["candidate_mad"], 1.0)
        self.assertFalse(boundary["improved"])
        self.assertTrue(improved["improved"])


class HistogramIntervalTest(unittest.TestCase):
    @staticmethod
    def histogram(
        counts: list[int],
        *,
        bounds: list[int] | None = None,
        saturated: bool = False,
    ) -> dict:
        return {
            "bucket_upper_bounds_ns": bounds or [10, 20, 30],
            "bucket_counts": counts,
            "latency_quantiles_saturated": {
                "p50": saturated,
                "p95": False,
                "p99": False,
            },
        }

    def test_interval_rebuilds_nearest_rank_quantiles_from_bucket_deltas(self) -> None:
        before = self.histogram([2, 3, 5, 0])
        after = self.histogram([4, 6, 10, 0])

        interval = gate.histogram_interval(before, after)

        self.assertEqual(interval["bucket_counts"], [2, 3, 5])
        self.assertEqual(interval["samples"], 10)
        self.assertEqual(interval["p50_upper_bound_ns"], 20)
        self.assertEqual(interval["p95_upper_bound_ns"], 30)
        self.assertEqual(interval["p99_upper_bound_ns"], 30)
        self.assertEqual(interval["max_upper_bound_ns"], 30)

    def test_interval_rejects_changed_bounds_and_negative_deltas(self) -> None:
        with self.assertRaisesRegex(gate.GateValidationError, "bounds changed"):
            gate.histogram_interval(
                self.histogram([0, 0, 0, 0]),
                self.histogram([1, 0, 0, 0], bounds=[10, 21, 30]),
            )
        with self.assertRaisesRegex(gate.GateValidationError, "counter decreased"):
            gate.histogram_interval(
                self.histogram([2, 0, 0, 0]),
                self.histogram([1, 1, 0, 0]),
            )

    def test_interval_rejects_overflow_saturation_and_zero_samples(self) -> None:
        empty = self.histogram([0, 0, 0, 0])
        with self.assertRaisesRegex(gate.GateValidationError, "overflow"):
            gate.histogram_interval(empty, self.histogram([1, 0, 0, 1]))
        with self.assertRaisesRegex(gate.GateValidationError, "saturated"):
            gate.histogram_interval(
                empty,
                self.histogram([1, 0, 0, 0], saturated=True),
            )
        with self.assertRaisesRegex(gate.GateValidationError, "zero samples"):
            gate.histogram_interval(empty, empty)

    def test_zero_sample_interval_is_allowed_only_when_explicit(self) -> None:
        empty = self.histogram([0, 0, 0, 0])
        self.assertEqual(
            gate.histogram_interval(empty, empty, allow_zero=True),
            {
                "bucket_upper_bounds_ns": [10, 20, 30],
                "bucket_counts": [0, 0, 0],
                "samples": 0,
                "p50_upper_bound_ns": None,
                "p95_upper_bound_ns": None,
                "p99_upper_bound_ns": None,
                "max_upper_bound_ns": None,
            },
        )


class RawCallbackEvidenceTest(unittest.TestCase):
    @staticmethod
    def runtime(kinds: tuple[str, ...], counts: list[int]) -> dict:
        callbacks = {}
        for kind in kinds:
            callbacks[kind] = {
                "latency": {
                    "latency_histogram": {
                        "bucket_upper_bounds_ns": [10, 20, 30],
                        "bucket_counts": list(counts),
                    },
                    "latency_quantiles_saturated": {
                        "p50": False,
                        "p95": False,
                        "p99": False,
                    },
                }
            }
        return {
            "write_path": {
                "fuse_callbacks": callbacks,
            }
        }

    @staticmethod
    def interval(samples: int = 1) -> dict:
        quantile = 10 if samples else None
        return {
            "bucket_upper_bounds_ns": [10, 20, 30],
            "bucket_counts": [samples, 0, 0],
            "samples": samples,
            "p50_upper_bound_ns": quantile,
            "p95_upper_bound_ns": quantile,
            "p99_upper_bound_ns": quantile,
            "max_upper_bound_ns": quantile,
        }

    def test_read_write_rebuilds_all_literal_raw_callback_kinds(self) -> None:
        kinds = ("read", "write", "flush", "fsync", "release")
        derived = {
            "callback_intervals": {
                f"fuse_{kind}": self.interval() for kind in kinds
            }
        }
        rebuilt = gate.validate_raw_callback_intervals(
            "direct-serial",
            "large-single",
            self.runtime(kinds, [0, 0, 0, 0]),
            self.runtime(kinds, [1, 0, 0, 0]),
            derived,
        )
        self.assertEqual(rebuilt, derived["callback_intervals"])

    def test_raw_callbacks_reject_missing_invalid_or_derived_mismatch(self) -> None:
        kinds = ("read", "write", "flush", "fsync", "release")
        before = self.runtime(kinds, [0, 0, 0, 0])
        after = self.runtime(kinds, [1, 0, 0, 0])
        derived = {
            "callback_intervals": {
                f"fuse_{kind}": self.interval() for kind in kinds
            }
        }
        missing = self.runtime(kinds[:-1], [1, 0, 0, 0])
        overflow = self.runtime(kinds, [1, 0, 0, 1])
        mismatched = {
            "callback_intervals": {
                **derived["callback_intervals"],
                "fuse_read": {**self.interval(), "p95_upper_bound_ns": 20},
            }
        }
        for changed_after, changed_derived, message in (
            (missing, derived, "missing"),
            (overflow, derived, "overflow"),
            (after, mismatched, "derived"),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    gate.validate_raw_callback_intervals(
                        "direct-serial",
                        "large-single",
                        before,
                        changed_after,
                        changed_derived,
                    )

    def test_warm_rebuilds_first_and_second_callback_intervals(self) -> None:
        kinds = ("read", "flush", "release")
        derived = {
            "callback_intervals": {
                f"{phase}_fuse_{kind}": self.interval()
                for phase in ("first", "second")
                for kind in kinds
            }
        }
        rebuilt = gate.validate_raw_callback_intervals(
            "direct-serial",
            "warm-reopen",
            self.runtime(kinds, [0, 0, 0, 0]),
            self.runtime(kinds, [2, 0, 0, 0]),
            derived,
            warm_first_end=self.runtime(kinds, [1, 0, 0, 0]),
        )
        self.assertEqual(rebuilt, derived["callback_intervals"])

    def test_kernel_second_warm_read_allows_zero_but_direct_rejects_it(self) -> None:
        kinds = ("read", "flush", "release")
        start = self.runtime(kinds, [0, 0, 0, 0])
        midpoint = self.runtime(kinds, [1, 0, 0, 0])
        end = self.runtime(kinds, [2, 0, 0, 0])
        end["write_path"]["fuse_callbacks"]["read"]["latency"][
            "latency_histogram"
        ]["bucket_counts"] = [1, 0, 0, 0]
        intervals = {
            f"first_fuse_{kind}": self.interval() for kind in kinds
        }
        intervals.update(
            {
                "second_fuse_read": self.interval(0),
                "second_fuse_flush": self.interval(),
                "second_fuse_release": self.interval(),
            }
        )
        derived = {"callback_intervals": intervals}

        gate.validate_raw_callback_intervals(
            "kernel",
            "warm-reopen",
            start,
            end,
            derived,
            warm_first_end=midpoint,
        )
        with self.assertRaisesRegex(gate.GateValidationError, "zero samples"):
            gate.validate_raw_callback_intervals(
                "direct-serial",
                "warm-reopen",
                start,
                end,
                derived,
                warm_first_end=midpoint,
            )

    def test_warm_callback_intervals_reject_corrupt_midpoint_or_derived(self) -> None:
        kinds = ("read", "flush", "release")
        start = self.runtime(kinds, [0, 0, 0, 0])
        midpoint = self.runtime(kinds, [1, 0, 0, 0])
        end = self.runtime(kinds, [2, 0, 0, 0])
        intervals = {
            f"{phase}_fuse_{kind}": self.interval()
            for phase in ("first", "second")
            for kind in kinds
        }
        corrupt_midpoint = self.runtime(kinds, [3, 0, 0, 0])
        with self.assertRaisesRegex(gate.GateValidationError, "counter decreased"):
            gate.validate_raw_callback_intervals(
                "direct-serial",
                "warm-reopen",
                start,
                end,
                {"callback_intervals": intervals},
                warm_first_end=corrupt_midpoint,
            )
        with self.assertRaisesRegex(gate.GateValidationError, "derived"):
            gate.validate_raw_callback_intervals(
                "direct-serial",
                "warm-reopen",
                start,
                end,
                {
                    "callback_intervals": {
                        **intervals,
                        "first_fuse_read": {
                            **self.interval(),
                            "p95_upper_bound_ns": 20,
                        },
                    }
                },
                warm_first_end=midpoint,
            )


class RuntimeEvidenceTest(unittest.TestCase):
    @staticmethod
    def snapshot(**overrides: int) -> dict:
        snapshot = {
            "schema_version": 2,
            "ingress": {"dirty_mem_bytes": 0},
            "upload": {"pending_uploads": 0, "active_uploads": 0},
            "write_path": {
                "fuse_async_read": {
                    "pending_reads": 0,
                    "pending_bytes": 0,
                },
                "fuse_async_write": {
                    "pending_writes": 0,
                    "pending_bytes": 0,
                    "pending_handles": 0,
                },
                "os_cache": {
                    "active_writers": 0,
                    "active_foreground_handles": 0,
                },
            },
        }
        for path, value in overrides.items():
            section, key = path.split("__", 1)
            if section == "ingress":
                snapshot["ingress"][key] = value
            elif section == "upload":
                snapshot["upload"][key] = value
            elif section == "async":
                snapshot["write_path"]["fuse_async_write"][key] = value
            elif section == "async_read":
                snapshot["write_path"]["fuse_async_read"][key] = value
            elif section == "cache":
                snapshot["write_path"]["os_cache"][key] = value
        return snapshot

    def test_idle_requires_three_consecutive_zero_snapshots(self) -> None:
        zero = self.snapshot()
        busy = self.snapshot(ingress__dirty_mem_bytes=1)
        self.assertFalse(gate.idle_streak_reached([zero, zero]))
        self.assertFalse(gate.idle_streak_reached([zero, zero, busy, zero, zero]))
        self.assertTrue(
            gate.idle_streak_reached([zero, zero, busy, zero, zero, zero])
        )

    def test_idle_includes_async_reservations_and_handle_lifecycles(self) -> None:
        for override in (
            {"upload__pending_uploads": 1},
            {"upload__active_uploads": 1},
            {"async_read__pending_reads": 1},
            {"async_read__pending_bytes": 1},
            {"async__pending_writes": 1},
            {"async__pending_bytes": 1},
            {"async__pending_handles": 1},
            {"cache__active_writers": 1},
            {"cache__active_foreground_handles": 1},
        ):
            self.assertFalse(gate.snapshot_is_idle(self.snapshot(**override)))

    def test_diagnostics_attach_must_match_pid_path_and_schema(self) -> None:
        payload = {
            "session": {"pid": 42, "mount_point": "/mnt/studio/volume"},
            "runtime_snapshot": self.snapshot(),
        }
        self.assertEqual(
            gate.validate_diagnostics_attach(
                payload,
                mount_pid=42,
                mount_point="/mnt/studio/../studio/volume",
            ),
            payload["runtime_snapshot"],
        )
        for changed, message in (
            ({"session": {"pid": 43, "mount_point": "/mnt/studio/volume"}}, "pid"),
            ({"session": {"pid": 42, "mount_point": "/mnt/other"}}, "mount point"),
            ({"runtime_snapshot": {**self.snapshot(), "schema_version": 1}}, "schema"),
        ):
            invalid = {**payload, **changed}
            with self.assertRaisesRegex(gate.GateValidationError, message):
                gate.validate_diagnostics_attach(
                    invalid,
                    mount_pid=42,
                    mount_point="/mnt/studio/volume",
                )

    def test_diagnostics_attach_rejects_mounted_file_fallback(self) -> None:
        payload = {
            "session": {"pid": 42, "mount_point": "/mnt/studio/volume"},
            "runtime_snapshot": self.snapshot(),
            "runtime_snapshot_source": "mounted_file",
        }
        with self.assertRaisesRegex(gate.GateValidationError, "out-of-band"):
            gate.validate_diagnostics_attach(
                payload,
                mount_pid=42,
                mount_point="/mnt/studio/volume",
            )


class MemoryPressureEvidenceTest(unittest.TestCase):
    @staticmethod
    def evidence(
        expected_written: int = 131_072_000,
        expected_read: int = 131_072_000,
        **overrides: object,
    ) -> dict:
        constrained = {
            "source": "host_and_cgroup",
            "state": "constrained",
            "generation": 2,
            "envelope_bytes": 1_000,
            "memory_cache_limit_bytes": 400,
            "dirty_limit_bytes": 250,
            "transit_limit_bytes": 150,
            "async_write_limit_bytes": 100,
            "overcommitted_minimum": False,
            "transition_count": 1,
            "sampling_failure_count": 0,
            "write_admission_wait_count": 1,
            "write_admission_wait_ns": 1,
            "admitted_lower_bound_bytes": 536_870_912,
        }
        recovered = {
            **constrained,
            "state": "normal",
            "generation": 4,
        }
        setup_observed = {
            **constrained,
            "state": "normal",
            "generation": 1,
            "transition_count": 0,
            "write_admission_wait_count": 0,
            "write_admission_wait_ns": 0,
            "admitted_lower_bound_bytes": 469_762_048,
        }
        evidence = {
            "status": "complete",
            "cgroup": {
                "memory_max_bytes": 2_147_483_648,
                "memory_current_before_bytes": 536_870_912,
                "memory_current_pressure_bytes": 1_610_612_736,
                "memory_current_after_holder_release_bytes": 671_088_640,
                "events_before": {"oom": 3, "oom_kill": 1},
                "events_after": {"oom": 3, "oom_kill": 1},
                "events_delta": {"oom": 0, "oom_kill": 0},
            },
            "holder": {
                "schema_version": 1,
                "bytes": 536_870_912,
                "started_monotonic_ns": 300_000_000,
                "ready_monotonic_ns": 400_000_000,
                "released_monotonic_ns": 900_000_000,
                "exited_monotonic_ns": 1_000_000_000,
                "exit_status": 0,
            },
            "setup": {
                "schema_version": 1,
                "working_set_bytes": 536_870_912,
                "cumulative_bytes_written": 1_073_741_824,
                "observed_admitted_lower_bound_bytes": 469_762_048,
                "observed_generation": 1,
                "started_monotonic_ns": 100_000_000,
                "observed_monotonic_ns": 200_000_000,
                "stopped_monotonic_ns": 850_000_000,
                "feeder_pid": 4242,
                "feeder_start_time": "123456",
                "feeder_live_before_profile": True,
                "feeder_live_after_profile": True,
                "exit_status": 0,
            },
            "runtime_samples": {
                "setup_observed": setup_observed,
                "pressure": [constrained, {**constrained, "generation": 2}],
                "recovery_boundary": {**constrained, "generation": 3},
                "recovery": [recovered, {**recovered, "generation": 5}],
            },
            "verification": {
                "bytes_written": expected_written,
                "bytes_read": expected_read,
                "pressure_sha256": "a" * 64,
                "recovery_sha256": "a" * 64,
            },
            "throughput_mb_per_second": {
                "profile": "small-128k",
                "method": "inline-unbuffered-read-v1",
                "chunk_bytes": 1 << 20,
                "bytes_read": expected_read,
                "pressure": [80.0],
                "recovery": [81.0],
            },
            "cpu": {
                "foreground_seconds": 1.0,
                "drain_seconds": 0.1,
            },
            "drain_completed": True,
        }
        evidence.update(overrides)
        return evidence

    def validate(self, mode: str = "direct-async", evidence: dict | None = None) -> dict:
        return gate.validate_memory_pressure_evidence(
            mode,
            "small-128k",
            evidence if evidence is not None else self.evidence(),
            expected_written=131_072_000,
            expected_read=131_072_000,
            promotion_eligible=True,
        )

    def test_complete_direct_pressure_evidence_passes(self) -> None:
        result = self.validate()

        self.assertTrue(result["passed"])
        self.assertTrue(result["recovery_passed"])

    def test_runtime_snapshot_is_normalized_from_wire_metrics(self) -> None:
        names = {
            "source": 3,
            "state": 2,
            "generation": 7,
            "envelope_bytes": 1_000,
            "memory_cache_limit_bytes": 400,
            "dirty_limit_bytes": 250,
            "transit_limit_bytes": 150,
            "async_write_limit_bytes": 100,
            "overcommitted_minimum": 0,
        }
        snapshot = {
            "schema_version": 2,
            "ingress": {"dirty_mem_bytes": 300},
            "write_path": {
                "fuse_async_read": {"pending_bytes": 10},
                "fuse_async_write": {"pending_bytes": 10},
            },
            "memory_pressure": {
                "gauges": {
                    name: {
                        "value": value,
                        "unit": (
                            "count"
                            if name
                            in {"source", "state", "generation", "overcommitted_minimum"}
                            else "bytes"
                        ),
                    }
                    for name, value in names.items()
                },
                "counters": {
                    name: {"value": value, "unit": unit}
                    for name, value, unit in (
                        ("transition_count", 2, "count"),
                        ("sampling_failure_count", 0, "count"),
                        ("write_admission_wait_count", 3, "count"),
                        ("write_admission_wait_ns", 4, "ns"),
                    )
                },
                "latencies": {},
            },
        }

        self.assertEqual(
            gate.memory_pressure_sample(snapshot),
            {
                "source": "host_and_cgroup",
                "state": "constrained",
                "generation": 7,
                "envelope_bytes": 1_000,
                "memory_cache_limit_bytes": 400,
                "dirty_limit_bytes": 250,
                "transit_limit_bytes": 150,
                "async_write_limit_bytes": 100,
                "overcommitted_minimum": False,
                "transition_count": 2,
                "sampling_failure_count": 0,
                "write_admission_wait_count": 3,
                "write_admission_wait_ns": 4,
                "admitted_lower_bound_bytes": 320,
            },
        )

    def test_runtime_snapshot_rejects_wrong_units(self) -> None:
        snapshot = AnalyzerCliTest.pressure_runtime_snapshot(
            self.evidence()["runtime_samples"]["pressure"][0]
        )
        for section, name in (
            ("gauges", "envelope_bytes"),
            ("gauges", "generation"),
            ("counters", "write_admission_wait_ns"),
        ):
            with self.subTest(name=name):
                invalid = json.loads(json.dumps(snapshot))
                invalid["memory_pressure"][section][name]["unit"] = "wrong"
                with self.assertRaisesRegex(gate.GateValidationError, "unit"):
                    gate.memory_pressure_sample(invalid)

    def test_oom_and_oom_kill_are_rejected_separately(self) -> None:
        for counter in ("oom", "oom_kill"):
            with self.subTest(counter=counter):
                evidence = self.evidence()
                evidence["cgroup"]["events_after"][counter] += 1
                evidence["cgroup"]["events_delta"][counter] = 1
                with self.assertRaisesRegex(gate.GateValidationError, counter):
                    self.validate(evidence=evidence)

    def test_managed_limits_must_not_exceed_envelope_even_when_overcommitted(self) -> None:
        evidence = self.evidence()
        sample = evidence["runtime_samples"]["pressure"][0]
        sample["memory_cache_limit_bytes"] += 101
        sample["overcommitted_minimum"] = True

        with self.assertRaisesRegex(gate.GateValidationError, "envelope"):
            self.validate(evidence=evidence)

    def test_pressure_requires_joint_source_and_cgroup_current_transitions(self) -> None:
        cases = (
            (
                lambda evidence: evidence["runtime_samples"]["pressure"][0].update(
                    source="host"
                ),
                "source",
            ),
            (
                lambda evidence: evidence["cgroup"].update(
                    memory_current_pressure_bytes=(
                        evidence["cgroup"]["memory_current_before_bytes"]
                        + evidence["holder"]["bytes"] // 3
                    )
                ),
                "rise",
            ),
            (
                lambda evidence: evidence["cgroup"].update(
                    memory_current_after_holder_release_bytes=(
                        evidence["cgroup"]["memory_current_pressure_bytes"]
                        - evidence["holder"]["bytes"] // 3
                    )
                ),
                "fall",
            ),
        )
        for mutate, message in cases:
            with self.subTest(message=message):
                evidence = self.evidence()
                mutate(evidence)
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    self.validate(evidence=evidence)

    def test_setup_must_prove_admission_and_live_profile(self) -> None:
        cases = (
            ("working_set_bytes", 0, "working set"),
            ("cumulative_bytes_written", 0, "direct write"),
            ("observed_admitted_lower_bound_bytes", 0, "observed admission"),
            ("feeder_pid", 0, "feeder pid"),
            ("feeder_start_time", "0", "start time"),
            ("feeder_live_before_profile", False, "live"),
            ("feeder_live_after_profile", False, "live"),
            ("exit_status", 1, "setup exit"),
            ("exit_status", False, "integer"),
        )
        for field, value, message in cases:
            with self.subTest(field=field):
                evidence = self.evidence()
                evidence["setup"][field] = value
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    self.validate(evidence=evidence)

        drained = self.evidence()
        drained["setup"]["cumulative_bytes_written"] = 10 << 30
        drained["runtime_samples"]["pressure"][0]["admitted_lower_bound_bytes"] = 1_000
        with self.assertRaisesRegex(gate.GateValidationError, "envelope"):
            self.validate(evidence=drained)

    def test_holder_exit_status_rejects_bool(self) -> None:
        evidence = self.evidence()
        evidence["holder"]["exit_status"] = False

        with self.assertRaisesRegex(gate.GateValidationError, "integer"):
            self.validate(evidence=evidence)

        for section in ("holder", "setup"):
            with self.subTest(schema=section):
                evidence = self.evidence()
                evidence[section]["schema_version"] = True
                with self.assertRaisesRegex(gate.GateValidationError, "integer"):
                    self.validate(evidence=evidence)

    def test_pressure_counters_are_monotonic_and_sampling_failure_free(self) -> None:
        self.assertTrue(self.validate()["passed"])

        decreased = self.evidence()
        decreased["runtime_samples"]["setup_observed"]["transition_count"] = 9
        with self.assertRaisesRegex(gate.GateValidationError, "transition_count decreased"):
            self.validate(evidence=decreased)

        sampling_failed = self.evidence()
        for sample in (
            *sampling_failed["runtime_samples"]["pressure"],
            sampling_failed["runtime_samples"]["recovery_boundary"],
            *sampling_failed["runtime_samples"]["recovery"],
        ):
            sample["sampling_failure_count"] = 1
        with self.assertRaisesRegex(gate.GateValidationError, "sampling failures increased"):
            self.validate(evidence=sampling_failed)

        for mode in ("direct-async", "kernel"):
            with self.subTest(profile_transition=mode):
                transitioned = self.evidence()
                transitioned["runtime_samples"]["pressure"][1][
                    "transition_count"
                ] = 3
                for sample in (
                    transitioned["runtime_samples"]["recovery_boundary"],
                    *transitioned["runtime_samples"]["recovery"],
                ):
                    sample["transition_count"] = 3
                with self.assertRaisesRegex(
                    gate.GateValidationError, "state changed during the profile"
                ):
                    self.validate(mode=mode, evidence=transitioned)

    def test_pressure_and_recovery_require_the_same_measurer(self) -> None:
        evidence = self.evidence()
        evidence["throughput_mb_per_second"]["method"] = "different"

        with self.assertRaisesRegex(gate.GateValidationError, "measurer mismatch"):
            self.validate(evidence=evidence)

    def test_hash_bytes_and_drain_fail_closed(self) -> None:
        cases = (
            ("verification", "recovery_sha256", "b" * 64, "hash"),
            ("verification", "bytes_read", 1, "bytes"),
            (None, "drain_completed", False, "drain"),
        )
        for section, field, value, message in cases:
            with self.subTest(field=field):
                evidence = self.evidence()
                target = evidence if section is None else evidence[section]
                target[field] = value
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    self.validate(evidence=evidence)

    def test_promotion_requires_pressure_evidence_and_recovery(self) -> None:
        with self.assertRaisesRegex(gate.GateValidationError, "pressure evidence"):
            self.validate(evidence={"status": "not_run"})

        evidence = self.evidence()
        evidence["throughput_mb_per_second"]["recovery"] = [70.0]
        with self.assertRaisesRegex(gate.GateValidationError, "recovery"):
            self.validate(evidence=evidence)

    def test_kernel_keeps_recovery_miss_diagnostic_but_rejects_correctness(self) -> None:
        slow = self.evidence()
        slow["throughput_mb_per_second"]["recovery"] = [70.0]
        result = self.validate(mode="kernel", evidence=slow)
        self.assertTrue(result["passed"])
        self.assertFalse(result["recovery_passed"])
        self.assertTrue(result["performance_diagnostic"])

        missing = self.evidence()
        missing["throughput_mb_per_second"]["recovery"] = []
        self.assertFalse(self.validate(mode="kernel", evidence=missing)["recovery_passed"])

        malformed = self.evidence()
        malformed["throughput_mb_per_second"]["recovery"] = [-1.0]
        with self.assertRaisesRegex(gate.GateValidationError, "invalid"):
            self.validate(mode="kernel", evidence=malformed)

        corrupt = self.evidence()
        corrupt["verification"]["recovery_sha256"] = "b" * 64
        with self.assertRaisesRegex(gate.GateValidationError, "hash"):
            self.validate(mode="kernel", evidence=corrupt)

    def test_kernel_constrained_recovery_is_diagnostic_but_direct_rejects_it(self) -> None:
        evidence = self.evidence()
        evidence["runtime_samples"]["recovery"][-1]["state"] = "constrained"

        result = self.validate(mode="kernel", evidence=evidence)
        self.assertTrue(result["passed"])
        self.assertFalse(result["recovery_state_passed"])
        self.assertTrue(result["performance_diagnostic"])

        with self.assertRaisesRegex(gate.GateValidationError, "recovery state"):
            self.validate(mode="direct-async", evidence=evidence)


class PressureScenarioHelperTest(unittest.TestCase):
    @staticmethod
    def raw_contract() -> dict[str, dict]:
        boundary = lambda phase, timestamp: {
            "schema_version": 1,
            "phase": phase,
            "monotonic_ns": timestamp,
        }
        return {
            "before": boundary("before", 1),
            "setup": {
                "schema_version": 1,
                "started_monotonic_ns": 2,
                "observed_monotonic_ns": 3,
                "stopped_monotonic_ns": 14,
                "feeder_pid": 4242,
                "feeder_start_time": "123456",
                "exit_status": 0,
            },
            "holder": {
                "schema_version": 1,
                "started_monotonic_ns": 4,
                "ready_monotonic_ns": 5,
                "released_monotonic_ns": 15,
                "exited_monotonic_ns": 16,
                "exit_status": 0,
            },
            "runtime_before_profile": {
                "schema_version": 2,
                "captured_monotonic_ns": 6,
            },
            "pressure": boundary("pressure", 7),
            "profile": {
                "schema_version": 1,
                "profile": "small-128k",
                "runner": "inline",
                "workload_pid": 4343,
                "workload_start_time": "234567",
                "feeder_pid": 4242,
                "feeder_start_time": "123456",
                "feeder_live_before_profile": True,
                "feeder_live_after_profile": True,
                "started_monotonic_ns": 8,
                "completed_monotonic_ns": 9,
                "exit_status": 0,
                "source_report_sha256": "a" * 64,
                "read_throughput_mb_per_second": 79.0,
            },
            "pressure_measure": {
                "schema_version": 1,
                "method": "inline-unbuffered-read-v1",
                "chunk_bytes": 1 << 20,
                "bytes_read": 1024,
                "throughput_mb_per_second": 80.0,
                "started_monotonic_ns": 10,
                "completed_monotonic_ns": 11,
                "exit_status": 0,
            },
            "pressure_hash": {
                "schema_version": 1,
                "completed_monotonic_ns": 12,
                "exit_status": 0,
            },
            "runtime_after_profile": {
                "schema_version": 2,
                "captured_monotonic_ns": 13,
            },
            "after_holder_release": boundary("after-holder-release", 17),
            "recovery_measure": {
                "schema_version": 1,
                "method": "inline-unbuffered-read-v1",
                "chunk_bytes": 1 << 20,
                "bytes_read": 1024,
                "throughput_mb_per_second": 81.0,
                "started_monotonic_ns": 18,
                "completed_monotonic_ns": 19,
                "exit_status": 0,
            },
            "recovery_hash": {
                "schema_version": 1,
                "completed_monotonic_ns": 20,
                "exit_status": 0,
            },
            "drain": boundary("drain-end", 21),
            "after": boundary("after", 22),
            "cleanup": {
                "schema_version": 1,
                "cleanup_ended_monotonic_ns": 23,
                "unmount_exit_status": 0,
                "unmount_started_monotonic_ns": 24,
                "unmount_ended_monotonic_ns": 25,
            },
        }

    def test_raw_pressure_contract_rejects_schema_phase_and_order(self) -> None:
        gate.validate_pressure_artifact_contract(self.raw_contract())
        cases = (
            ("before", "schema_version", 2, "schema"),
            ("setup", "schema_version", True, "integer"),
            ("pressure", "phase", "wrong", "phase"),
            ("recovery_measure", "started_monotonic_ns", 25, "order"),
        )
        for artifact, field, value, message in cases:
            with self.subTest(field=field):
                raw = self.raw_contract()
                raw[artifact][field] = value
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    gate.validate_pressure_artifact_contract(raw)

        for artifact in (
            "holder",
            "setup",
            "profile",
            "pressure_measure",
            "pressure_hash",
            "recovery_measure",
            "recovery_hash",
        ):
            with self.subTest(bool_exit=artifact):
                raw = self.raw_contract()
                raw[artifact]["exit_status"] = False
                with self.assertRaisesRegex(gate.GateValidationError, "integer"):
                    gate.validate_pressure_artifact_contract(raw)

        mismatched = self.raw_contract()
        mismatched["recovery_measure"]["chunk_bytes"] = 4096
        with self.assertRaisesRegex(gate.GateValidationError, "measurer mismatch"):
            gate.validate_pressure_artifact_contract(mismatched)

    def test_inline_pressure_feeder_installs_signal_and_uses_aligned_buffer(self) -> None:
        root = Path.cwd() / "target/perf/test-inline-pressure-feeder"
        shutil.rmtree(root, ignore_errors=True)
        completed = subprocess.run(
            ["bash", "tools/perf/linux-cache-gate.sh"],
            cwd=Path.cwd(),
            env={
                **os.environ,
                "CACHE_GATE_INTERNAL_FEEDER_FIXTURE": "true",
                "CACHE_GATE_INTERNAL_FEEDER_DIR": str(root),
            },
            capture_output=True,
            text=True,
            timeout=10,
        )
        try:
            self.assertEqual(completed.returncode, 0, completed.stderr)
            payload = json.loads((root / "pressure-setup.json").read_text())
            self.assertEqual(payload["schema_version"], 1)
            self.assertEqual(payload["working_set_bytes"], 8 << 20)
            self.assertGreater(payload["cumulative_bytes_written"], 0)
            self.assertLess(payload["started_monotonic_ns"], payload["stopped_monotonic_ns"])
            self.assertTrue((root / ".pressure-setup.ready").exists())
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_pid_reuse_guard_does_not_signal_mismatched_identity(self) -> None:
        for helper_kind in (
            "pidstat",
            "memory-sampler",
            "memory-holder",
            "pressure-setup",
            "pressure-workload",
        ):
            with self.subTest(helper_kind=helper_kind):
                result = Path.cwd() / f"target/perf/test-{helper_kind}-pid-reuse.json"
                result.unlink(missing_ok=True)
                completed = subprocess.run(
                    ["bash", "tools/perf/linux-cache-gate.sh"],
                    cwd=Path.cwd(),
                    env={
                        **os.environ,
                        "CACHE_GATE_INTERNAL_PID_REUSE_FIXTURE": "true",
                        "CACHE_GATE_INTERNAL_PID_REUSE_RESULT": str(result),
                        "CACHE_GATE_INTERNAL_PID_REUSE_HELPER_KIND": helper_kind,
                    },
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                try:
                    self.assertEqual(completed.returncode, 0, completed.stderr)
                    payload = json.loads(result.read_text())
                    self.assertEqual(payload["helper_kind"], helper_kind)
                    self.assertTrue(payload["mismatched_identity_was_not_signaled"])
                    self.assertGreater(payload["pid"], 0)
                    self.assertTrue(payload["start_time"].isdigit())
                finally:
                    result.unlink(missing_ok=True)

    def test_json_readers_reject_symlinks(self) -> None:
        root = Path.cwd() / "target/perf/test-pressure-artifact-symlink"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True)
        target = root / "target.json"
        target.write_text("{}\n")
        link = root / "artifact.json"
        link.symlink_to(target)
        try:
            with self.assertRaisesRegex(gate.GateValidationError, "symlink"):
                gate._read_json(link)
            with self.assertRaisesRegex(gate.GateValidationError, "symlink"):
                gate._read_jsonl(link)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_fake_cgroup_boundary_reads_current_limit_and_events(self) -> None:
        root = Path.cwd() / "target/perf/test-pressure-cgroup"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True)
        (root / "memory.max").write_text("4294967296\n")
        (root / "memory.current").write_text("2147483648\n")
        (root / "memory.events").write_text("low 2\noom 0\noom_kill 0\n")
        try:
            boundary = gate.read_cgroup_memory_boundary(root, "pressure", monotonic_ns=7)
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(
            boundary,
            {
                "schema_version": 1,
                "phase": "pressure",
                "monotonic_ns": 7,
                "memory_max_bytes": 4_294_967_296,
                "memory_current_bytes": 2_147_483_648,
                "events": {"low": 2, "oom": 0, "oom_kill": 0},
            },
        )

    def test_reserve_aware_scenario_keeps_a_positive_nonminimum_envelope(self) -> None:
        gib = 1 << 30
        mib = 1 << 20
        scenario = gate.derive_pressure_scenario(
            memory_current=960 << 20,
            memory_max=3584 << 20,
            host_available=4 * gib,
            parent_available=None,
            observed_admitted_lower_bound=448 * mib,
            negotiated_max_write=32 * mib,
        )

        self.assertGreaterEqual(scenario["holder_bytes"], gib)
        envelope = scenario["target_envelope_bytes"]
        self.assertGreaterEqual(envelope, 384 * mib)
        self.assertEqual(scenario["target_envelope_bytes"], envelope)
        self.assertLessEqual(
            scenario["managed_minimum_bytes"], envelope * 90 // 100
        )
        self.assertFalse(scenario["overcommitted_minimum"])
        target = gate.derive_pressure_target(
            effective_limit=(3584 << 20) + 7,
            negotiated_max_write=32 * mib,
        )
        self.assertEqual(
            target["reserve_bytes"],
            max(gib, ((3584 << 20) + 7) * 15 // 100),
        )

    def test_scenario_fails_when_three_gib_headroom_is_unavailable(self) -> None:
        with self.assertRaisesRegex(gate.GateValidationError, "768 MiB"):
            gate.derive_pressure_scenario(
                memory_current=2 << 30,
                memory_max=3 << 30,
                host_available=1 << 30,
                parent_available=None,
                observed_admitted_lower_bound=448 << 20,
                negotiated_max_write=1 << 20,
            )

    def test_negotiated_max_write_uses_schema2_fuse_connection_field(self) -> None:
        valid = {
            "schema_version": 2,
            "bridge": {},
            "write_path": {
                "fuse_connection": {
                    "negotiated_max_write_bytes": 1 << 20,
                }
            }
        }
        self.assertEqual(gate.negotiated_max_write_bytes(valid), 1 << 20)
        invalid = (
            ({}, "schema"),
            ({**valid, "schema_version": 3}, "schema"),
            ({**valid, "write_path": {}}, "negotiated"),
            ({**valid, "write_path": {"fuse_connection": {}}}, "negotiated"),
        )
        for snapshot, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    gate.negotiated_max_write_bytes(snapshot)
        for value in (None, 0, -1, False, "1048576"):
            with self.subTest(value=value):
                snapshot = json.loads(json.dumps(valid))
                snapshot["write_path"]["fuse_connection"][
                    "negotiated_max_write_bytes"
                ] = value
                with self.assertRaises(gate.GateValidationError):
                    gate.negotiated_max_write_bytes(snapshot)

    def test_holder_ready_release_exit_and_cleanup_use_target_dotfiles(self) -> None:
        root = Path.cwd() / "target/perf/test-memory-holder-helper"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True)
        start = root / ".memory-holder.start"
        ready = root / ".memory-holder.ready"
        release = root / ".memory-holder.release"
        result = root / "memory-holder.json"
        process = subprocess.Popen(
            [
                "python3",
                "-c",
                (
                    "from pathlib import Path; "
                    "from tools.perf.linux_cache_gate_summary import run_memory_holder; "
                    "import sys; "
                    "run_memory_holder(int(sys.argv[1]), *map(Path, sys.argv[2:]))"
                ),
                str(8 << 20),
                str(start),
                str(ready),
                str(release),
                str(result),
            ],
            cwd=Path.cwd(),
        )
        try:
            start.touch()
            deadline = time.monotonic() + 5
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(ready.exists())
            release.touch()
            self.assertEqual(process.wait(timeout=5), 0)
            payload = json.loads(result.read_text())
            self.assertEqual(
                set(payload),
                {
                    "schema_version",
                    "bytes",
                    "started_monotonic_ns",
                    "ready_monotonic_ns",
                    "released_monotonic_ns",
                    "exited_monotonic_ns",
                    "exit_status",
                },
            )
        finally:
            process.kill() if process.poll() is None else None
            gate.cleanup_memory_holder_files(start, ready, release)
            shutil.rmtree(root, ignore_errors=True)
        self.assertFalse(start.exists() or ready.exists() or release.exists())

    def test_forced_holder_cleanup_removes_signals_and_membership_is_verified(self) -> None:
        root = Path.cwd() / "target/perf/test-memory-holder-forced-cleanup"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True)
        start = root / ".memory-holder.start"
        ready = root / ".memory-holder.ready"
        release = root / ".memory-holder.release"
        result = root / "memory-holder.json"
        process = subprocess.Popen(
            [
                "python3",
                "-c",
                (
                    "from pathlib import Path; "
                    "from tools.perf.linux_cache_gate_summary import run_memory_holder; "
                    "import sys; run_memory_holder(int(sys.argv[1]), *map(Path, sys.argv[2:]))"
                ),
                str(8 << 20),
                str(start),
                str(ready),
                str(release),
                str(result),
            ],
            cwd=Path.cwd(),
        )
        try:
            start.touch()
            deadline = time.monotonic() + 5
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(ready.exists())
            process.terminate()
            process.wait(timeout=5)
            gate.cleanup_memory_holder_files(start, ready, release)
            self.assertFalse(start.exists() or ready.exists() or release.exists())
        finally:
            process.kill() if process.poll() is None else None
            shutil.rmtree(root, ignore_errors=True)

        fixture = Path.cwd() / "target/perf/test-pressure-trap-fixture"
        shutil.rmtree(fixture, ignore_errors=True)
        cgroup = fixture / "cgroup" / "leaf"
        cgroup.mkdir(parents=True)
        (cgroup / "cgroup.procs").touch()
        membership = fixture / "membership"
        membership.write_text("0::/leaf\n")
        pid_file = fixture / "workload.pid"
        hook = fixture / "cleanup-hook.txt"
        completed = subprocess.run(
            ["bash", "tools/perf/linux-cache-gate.sh"],
            cwd=Path.cwd(),
            env={
                **os.environ,
                "CACHE_GATE_INTERNAL_TRAP_FIXTURE": "true",
                "CACHE_GATE_INTERNAL_WORKLOAD_PID_FILE": str(pid_file),
                "CACHE_GATE_INTERNAL_CLEANUP_HOOK": str(hook),
                "CACHE_GATE_INTERNAL_CGROUP": str(cgroup),
                "CACHE_GATE_INTERNAL_CGROUP_MOUNT": str(cgroup.parent),
                "CACHE_GATE_INTERNAL_MEMBERSHIP": str(membership),
            },
            capture_output=True,
            text=True,
            timeout=10,
        )
        try:
            self.assertEqual(completed.returncode, 99, completed.stderr)
            workload_pid = int(pid_file.read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(workload_pid, 0)
            self.assertEqual(hook.read_text(), "workload-stopped\n")
            self.assertTrue((cgroup / "cgroup.procs").read_text().strip())
        finally:
            shutil.rmtree(fixture, ignore_errors=True)


class PreflightValidationTest(unittest.TestCase):
    def test_mode_environment_mismatch_is_rejected(self) -> None:
        valid = {
            "STUDIOFS_LINUX_FUSE_CACHE_POLICY": "direct",
            "STUDIOFS_FUSE_ASYNC_DIO": "0",
        }
        gate.validate_mode_environment("direct-serial", valid)
        with self.assertRaisesRegex(gate.GateValidationError, "mode environment"):
            gate.validate_mode_environment(
                "direct-async",
                valid,
            )

    def test_negotiated_geometry_mismatch_fails(self) -> None:
        direct_async = {
            "requested_async_dio": True,
            "negotiated_async_dio": True,
            "requested_writeback_cache": False,
            "negotiated_writeback_cache": False,
        }
        gate.validate_negotiated_geometry("direct-async", direct_async)
        with self.assertRaisesRegex(gate.GateValidationError, "geometry"):
            gate.validate_negotiated_geometry(
                "direct-async",
                {**direct_async, "negotiated_async_dio": False},
            )

    def test_cgroup_preflight_requires_fresh_empty_v2_memory_leaf(self) -> None:
        valid = {
            "filesystem_type": "cgroup2",
            "path": "/sys/fs/cgroup/delegated/gate-1",
            "delegated_root": "/sys/fs/cgroup/delegated",
            "controllers": ["cpu", "memory", "pids"],
            "cgroup_procs": [],
            "memory_current": 0,
            "memory_peak": 0,
            "writable": {"cgroup.procs": True, "memory.max": True},
            "readable": {
                "memory.current": True,
                "memory.peak": True,
                "memory.stat": True,
                "memory.max": True,
                "memory.events": True,
            },
            "fresh": True,
        }
        gate.validate_cgroup_preflight(valid)
        invalid_cases = (
            ({**valid, "filesystem_type": "tmpfs"}, "cgroup2"),
            ({**valid, "controllers": ["cpu"]}, "memory controller"),
            ({**valid, "cgroup_procs": [99]}, "empty"),
            ({**valid, "memory_current": 1}, "memory.current"),
            ({**valid, "memory_peak": 1}, "memory.peak"),
            ({**valid, "fresh": False}, "fresh"),
            ({**valid, "path": valid["delegated_root"]}, "leaf"),
            (
                {**valid, "writable": {**valid["writable"], "memory.max": False}},
                "memory.max",
            ),
            (
                {
                    **valid,
                    "readable": {**valid["readable"], "memory.events": False},
                },
                "memory.events",
            ),
        )
        for payload, message in invalid_cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    gate.validate_cgroup_preflight(payload)


class RunEligibilityTest(unittest.TestCase):
    @staticmethod
    def derived() -> dict:
        return {
            "command_exit_status": 0,
            "callback_errors": 0,
            "async_errors": 0,
            "task_errors": 0,
            "cancellations": 0,
            "foreground_start_idle": True,
            "drain_end_idle": True,
            "negotiated_geometry": {
                "requested_async_dio": False,
                "negotiated_async_dio": False,
                "requested_writeback_cache": False,
                "negotiated_writeback_cache": False,
            },
        }

    @staticmethod
    def cleanup() -> dict:
        return {
            "cleanup_error": None,
            "cleanup_drain_idle": True,
            "unmount_timed_out": False,
            "unmount_exit_status": 0,
            "mount_exit_timed_out": False,
            "mount_exit_status": 0,
        }

    def test_command_callback_async_task_cancellation_or_cleanup_failure_fails(self) -> None:
        gate.validate_run_eligibility(
            "direct-serial",
            self.derived(),
            self.cleanup(),
        )
        for field in (
            "command_exit_status",
            "callback_errors",
            "async_errors",
            "task_errors",
            "cancellations",
        ):
            with self.subTest(field=field):
                with self.assertRaises(gate.GateValidationError):
                    gate.validate_run_eligibility(
                        "direct-serial",
                        {**self.derived(), field: 1},
                        self.cleanup(),
                    )

        for field, value in (
            ("cleanup_error", "delete failed"),
            ("cleanup_drain_idle", False),
            ("unmount_timed_out", True),
            ("unmount_exit_status", 1),
            ("mount_exit_timed_out", True),
            ("mount_exit_status", 1),
        ):
            with self.subTest(field=field):
                with self.assertRaises(gate.GateValidationError):
                    gate.validate_run_eligibility(
                        "direct-serial",
                        self.derived(),
                        {**self.cleanup(), field: value},
                    )

    def test_run_requires_idle_boundaries_and_matching_geometry(self) -> None:
        for field in ("foreground_start_idle", "drain_end_idle"):
            with self.subTest(field=field):
                with self.assertRaisesRegex(gate.GateValidationError, "idle"):
                    gate.validate_run_eligibility(
                        "direct-serial",
                        {**self.derived(), field: False},
                        self.cleanup(),
                    )
        with self.assertRaisesRegex(gate.GateValidationError, "geometry"):
            gate.validate_run_eligibility(
                "kernel",
                self.derived(),
                self.cleanup(),
            )

    def test_exit_statuses_reject_bool(self) -> None:
        with self.assertRaisesRegex(gate.GateValidationError, "integer"):
            gate.validate_run_eligibility(
                "direct-serial",
                {**self.derived(), "command_exit_status": False},
                self.cleanup(),
            )
        for field in ("unmount_exit_status", "mount_exit_status"):
            with self.subTest(field=field):
                with self.assertRaisesRegex(gate.GateValidationError, "integer"):
                    gate.validate_run_eligibility(
                        "direct-serial",
                        self.derived(),
                        {**self.cleanup(), field: False},
                    )


class CorrectnessAndProvenanceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path.cwd() / "target/perf/test-linux-cache-correctness"
        shutil.rmtree(self.root, ignore_errors=True)
        self.manifest = correctness_manifest(self.root / "logs")

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def validate_correctness(self, manifest: dict) -> None:
        gate.validate_correctness_manifest(
            manifest,
            expected_revision="studio-revision",
            expected_studiofs_sha256="a" * 64,
            expected_bench_revision="bench-revision",
            expected_bench_sha256="b" * 64,
        )

    def test_complete_correctness_allowlist_passes(self) -> None:
        self.validate_correctness(self.manifest)

    def test_missing_skipped_checksum_or_revision_mismatch_fails(self) -> None:
        valid = self.manifest
        invalid_cases = (
            ({**valid, "tests": valid["tests"][:-1]}, "missing"),
            (
                {
                    **valid,
                    "tests": [
                        {**valid["tests"][0], "skipped": True},
                        *valid["tests"][1:],
                    ],
                },
                "skipped",
            ),
            (
                {
                    **valid,
                    "tests": [
                        {**valid["tests"][0], "log_sha256": "d" * 64},
                        *valid["tests"][1:],
                    ],
                },
                "log checksum",
            ),
            (
                {
                    **valid,
                    "tests": [
                        {
                            **valid["tests"][0],
                            "log_path": str(self.root / "missing.log"),
                        },
                        *valid["tests"][1:],
                    ],
                },
                "log",
            ),
            ({**valid, "studiofs_sha256": "c" * 64}, "checksum"),
            ({**valid, "bench_sha256": "not-a-sha"}, "checksum"),
            ({**valid, "git_revision": "other-revision"}, "revision"),
            ({**valid, "git_revision": ""}, "revision"),
            ({**valid, "bench_git_revision": ""}, "revision"),
            (
                {
                    **valid,
                    "tests": [
                        {**valid["tests"][0], "exit_status": False},
                        *valid["tests"][1:],
                    ],
                },
                "integer",
            ),
            ({**valid, "schema_version": 2}, "schema"),
            ({**valid, "kind": "other"}, "kind"),
            ({**valid, "dry_run": True}, "dry-run"),
        )
        for manifest, message in invalid_cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    self.validate_correctness(manifest)

    def test_final_provenance_rejects_dirty_sources_or_executable_mismatch(self) -> None:
        correctness = self.manifest
        correctness_path = str((self.root / "correctness-manifest.json").resolve())
        valid = {
            "git_revision": "studio-revision",
            "git_dirty": False,
            "studiofs_sha256": "a" * 64,
            "mount_executable_sha256": "a" * 64,
            "bench_git_revision": "bench-revision",
            "bench_git_dirty": False,
            "bench_sha256": "b" * 64,
            "correctness_manifest_path": correctness_path,
            "correctness_manifest_sha256": "c" * 64,
        }
        gate.validate_run_provenance(
            valid,
            correctness,
            correctness_manifest_path=correctness_path,
            correctness_manifest_sha256="c" * 64,
        )
        for changed, message in (
            ({"git_dirty": True}, "dirty"),
            ({"bench_git_dirty": True}, "dirty"),
            ({"mount_executable_sha256": "e" * 64}, "executable checksum"),
            ({"mount_executable_sha256": "invalid"}, "checksum"),
            ({"correctness_manifest_sha256": "f" * 64}, "manifest checksum"),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    gate.validate_run_provenance(
                        {**valid, **changed},
                        correctness,
                        correctness_manifest_path=correctness_path,
                        correctness_manifest_sha256="c" * 64,
                    )


class WorkloadReportValidationTest(unittest.TestCase):
    @staticmethod
    def bench_report() -> dict:
        return {
            "run": {
                "workload_bytes": 4_000_000_000,
                "file_count": 1,
                "files_kept": True,
                "stopped": False,
                "cleanup_error": None,
            },
            "config": {
                "target_path": "/mnt/studio/volume",
                "run_mode": "mounted_filesystem",
                "test_mode": "read_write",
                "file_layout": "single_file",
                "keep_files": True,
                "execution_mode": "run_once",
            },
            "passes": [
                {
                    "phase": "write",
                    "pass_number": 1,
                    "bytes_processed": 4_000_000_000,
                    "stopped": False,
                },
                {
                    "phase": "read",
                    "pass_number": 1,
                    "bytes_processed": 4_000_000_000,
                    "stopped": False,
                },
            ],
        }

    def test_workload_manifest_rejects_wrong_byte_or_file_counts(self) -> None:
        valid = {
            "workload": "small-128k",
            "workload_bytes": 131_072_000,
            "file_count": 1_000,
            "verified_bytes_written": 131_072_000,
            "verified_bytes_read": 131_072_000,
        }
        gate.validate_workload_manifest(valid, workload="small-128k")
        for changed, message in (
            ({"verified_bytes_read": 131_071_999}, "verified_bytes_read"),
            ({"file_count": 999}, "file_count"),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    gate.validate_workload_manifest(
                        {**valid, **changed},
                        workload="small-128k",
                    )

    def test_workload_expectations_cover_every_exact_client_shape(self) -> None:
        self.assertEqual(
            gate.workload_expectation("warm-reopen", workload_bytes=8),
            {
                "workload": "warm-reopen",
                "workload_bytes": 8,
                "file_count": 1,
                "verified_bytes_written": 0,
                "verified_bytes_read": 16,
            },
        )
        self.assertEqual(
            gate.workload_expectation("same-file-parallel", parallelism=3),
            {
                "workload": "same-file-parallel",
                "workload_bytes": 201_326_592,
                "file_count": 1,
                "verified_bytes_written": 201_326_592,
                "verified_bytes_read": 201_326_592,
                "parallelism": 3,
            },
        )
        self.assertEqual(
            gate.workload_expectation("cross-file-barriers"),
            {
                "workload": "cross-file-barriers",
                "workload_bytes": 134_217_728,
                "file_count": 2,
                "verified_bytes_written": 134_217_728,
                "verified_bytes_read": 134_217_728,
            },
        )

    def test_bench_report_rejects_stop_cleanup_pass_bytes_or_extra_json(self) -> None:
        valid = self.bench_report()
        gate.validate_bench_report(
            valid,
            workload="large-single",
            mount_point="/mnt/studio/volume",
            generated_json_files=["studiofs-bench-report.json"],
        )
        invalid_cases = (
            ({**valid, "run": {**valid["run"], "stopped": True}}, ["one.json"], "stopped"),
            (
                {**valid, "run": {**valid["run"], "cleanup_error": "failed"}},
                ["one.json"],
                "cleanup",
            ),
            ({**valid, "passes": [valid["passes"][0]]}, ["one.json"], "passes"),
            (
                {**valid, "passes": [valid["passes"][0], valid["passes"][0]]},
                ["one.json"],
                "passes",
            ),
            (
                {
                    **valid,
                    "passes": [
                        {**valid["passes"][0], "bytes_processed": 3_999_999_999},
                        valid["passes"][1],
                    ],
                },
                ["one.json"],
                "bytes_processed",
            ),
            (valid, ["one.json", "extra.json"], "exactly one"),
        )
        for report, generated, message in invalid_cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    gate.validate_bench_report(
                        report,
                        workload="large-single",
                        mount_point="/mnt/studio/volume",
                        generated_json_files=generated,
                    )

    def test_python_client_report_rejects_incomplete_or_checksum_mismatch(self) -> None:
        valid = {
            "workload": "small-128k",
            "file_count": 1_000,
            "bytes_written": 131_072_000,
            "bytes_read": 131_072_000,
            "expected_checksum": "client-checksum",
            "actual_checksum": "client-checksum",
            "completed": True,
            "cleanup_error": None,
        }
        gate.validate_python_client_report(valid, workload="small-128k")
        for changed, message in (
            ({"completed": False}, "completed"),
            ({"actual_checksum": "different"}, "checksum"),
            ({"file_count": 999}, "file_count"),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    gate.validate_python_client_report(
                        {**valid, **changed},
                        workload="small-128k",
                    )

    def test_cross_file_report_requires_exact_two_file_concurrency_proof(self) -> None:
        total = 2 * 64 * 1024 * 1024
        valid = {
            "workload": "cross-file-barriers",
            "file_count": 2,
            "bytes_written": total,
            "bytes_read": total,
            "expected_checksum": "client-checksum",
            "actual_checksum": "client-checksum",
            "completed": True,
            "cleanup_error": None,
            "common_start_monotonic_ns": 100,
            "file_completions": [
                {
                    "file": "left.bin",
                    "fsync_started_monotonic_ns": 105,
                    "completion_monotonic_ns": 110,
                },
                {
                    "file": "right.bin",
                    "fsync_started_monotonic_ns": 115,
                    "completion_monotonic_ns": 120,
                },
            ],
        }
        gate.validate_python_client_report(valid, workload="cross-file-barriers")
        for changed, message in (
            ({"file_completions": valid["file_completions"][:1]}, "two"),
            (
                {
                    "file_completions": [
                        valid["file_completions"][0],
                        {
                            "file": "left.bin",
                            "completion_monotonic_ns": 120,
                        },
                    ]
                },
                "distinct",
            ),
            (
                {
                    "file_completions": [
                        {"file": "left.bin", "completion_monotonic_ns": 99},
                        valid["file_completions"][1],
                    ]
                },
                "start",
            ),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    gate.validate_python_client_report(
                        {**valid, **changed},
                        workload="cross-file-barriers",
                    )


class HistogramAndWarmDecisionTest(unittest.TestCase):
    @staticmethod
    def warm_snapshot(read: int, vfs_read: int, replace: int, reuse: int) -> dict:
        return {
            "write_path": {
                "fuse_callbacks": {"read": {"calls": read}},
                "vfs_read_path": {"calls": vfs_read},
                "os_cache": {"replace": replace, "reuse": reuse},
            }
        }

    def test_warm_midpoint_rebuilds_exact_first_and_second_deltas(self) -> None:
        valid = {
            "checksum_verified": True,
            "first_fuse_read_calls": 2,
            "first_vfs_read_path_calls": 2,
            "first_os_cache_replace": 1,
            "first_os_cache_reuse": 0,
            "second_fuse_read_calls": 0,
            "second_vfs_read_path_calls": 0,
            "second_os_cache_replace": 0,
            "second_os_cache_reuse": 1,
        }
        derived = {"warm_proof": valid}
        rebuilt = gate.validate_warm_proof(
            "kernel",
            self.warm_snapshot(10, 20, 30, 40),
            self.warm_snapshot(12, 22, 31, 40),
            self.warm_snapshot(12, 22, 31, 41),
            derived,
            checksum_verified=True,
        )
        self.assertEqual(rebuilt, valid)

    def test_kernel_warm_proof_rejects_mismatch_or_missing_replace_reuse(self) -> None:
        valid = {
            "checksum_verified": True,
            "first_fuse_read_calls": 1,
            "first_vfs_read_path_calls": 1,
            "first_os_cache_replace": 1,
            "first_os_cache_reuse": 0,
            "second_fuse_read_calls": 0,
            "second_vfs_read_path_calls": 0,
            "second_os_cache_replace": 0,
            "second_os_cache_reuse": 1,
        }
        invalid_cases = (
            ({"checksum_verified": False}, "checksum"),
            ({"first_fuse_read_calls": 0}, "first"),
            ({"first_vfs_read_path_calls": 0}, "first"),
            ({"first_os_cache_replace": 0}, "Replace"),
            ({"second_os_cache_reuse": 0}, "Reuse"),
            ({"second_fuse_read_calls": 1}, "second"),
            ({"second_vfs_read_path_calls": 1}, "second"),
        )
        for changed, message in invalid_cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    gate.validate_kernel_warm_proof({**valid, **changed})

        with self.assertRaisesRegex(gate.GateValidationError, "raw warm proof"):
            gate.validate_warm_proof(
                "kernel",
                self.warm_snapshot(0, 0, 0, 0),
                self.warm_snapshot(1, 1, 1, 0),
                self.warm_snapshot(1, 1, 1, 1),
                {"warm_proof": {**valid, "second_os_cache_reuse": 2}},
                checksum_verified=True,
            )


class NumericDecisionTest(unittest.TestCase):
    @staticmethod
    def row(**overrides: object) -> dict:
        row = {
            "verified_bytes_written": 50,
            "verified_bytes_read": 50,
            "foreground_wall_seconds": 1.0,
            "foreground_mount_cpu_seconds": 1.0,
            "drain_mount_cpu_seconds": 0.1,
            "total_lifecycle_cpu_seconds_per_gib": 1.0,
            "foreground_average_cores": 1.0,
            "foreground_p95_cores": 1.0,
            "foreground_peak_cores": 1.0,
            "daemon_rss_peak_bytes": 100.0,
            "daemon_anon_peak_bytes": 100.0,
            "cgroup_file_peak_bytes": 100.0,
            "callback_intervals": {
                "fuse_read": {
                    "samples": 1,
                    "p95_upper_bound_ns": 100.0,
                    "p99_upper_bound_ns": 100.0,
                    "max_upper_bound_ns": 100.0,
                }
            },
        }
        row.update(overrides)
        return row

    def test_direct_async_rejection_selects_eligible_direct_serial(self) -> None:
        self.assertEqual(
            gate.select_kernel_baseline(
                {"eligible": True, "passed": True},
                {"eligible": True, "passed": True},
            ),
            "direct-async",
        )
        self.assertEqual(
            gate.select_kernel_baseline(
                {"eligible": True, "passed": True},
                {"eligible": True, "passed": False},
            ),
            "direct-serial",
        )
        with self.assertRaisesRegex(gate.GateValidationError, "Direct serial"):
            gate.select_kernel_baseline(
                {"eligible": False, "passed": False},
                {"eligible": True, "passed": True},
            )

    def test_cpu_core_tail_and_memory_gates_fail_independently(self) -> None:
        baseline = [self.row()] * 3
        candidate = [
            self.row(
                total_lifecycle_cpu_seconds_per_gib=1.02,
                foreground_average_cores=1.26,
                foreground_p95_cores=1.26,
                foreground_peak_cores=1.26,
                daemon_rss_peak_bytes=111.0,
                daemon_anon_peak_bytes=111.0,
                cgroup_file_peak_bytes=111.0,
                callback_intervals={
                    "fuse_read": {
                        "samples": 1,
                        "p95_upper_bound_ns": 111.0,
                        "p99_upper_bound_ns": 111.0,
                        "max_upper_bound_ns": 126.0,
                    }
                },
                memory_waiver="ignore numeric memory regressions",
            )
        ] * 3
        result = gate.evaluate_numeric_gates(
            baseline,
            candidate,
            candidate_mode="direct-async",
            workload_bytes=100,
        )
        for name in (
            "total_lifecycle_cpu_seconds_per_gib",
            "foreground_average_cores",
            "foreground_p95_cores",
            "foreground_peak_cores",
            "fuse_read.p95_upper_bound_ns",
            "fuse_read.p99_upper_bound_ns",
            "fuse_read.max_upper_bound_ns",
            "daemon_rss_peak_bytes",
            "daemon_anon_peak_bytes",
            "cgroup_file_peak_bytes",
        ):
            with self.subTest(name=name):
                self.assertFalse(result["gates"][name]["passed"])
        self.assertFalse(result["passed"])

    def test_kernel_file_memory_allows_one_working_set_then_fails(self) -> None:
        baseline = [self.row()] * 3
        passed = gate.evaluate_numeric_gates(
            baseline,
            [self.row(cgroup_file_peak_bytes=210.0)] * 3,
            candidate_mode="kernel",
            workload_bytes=100,
            compare_throughput=False,
        )
        failed = gate.evaluate_numeric_gates(
            baseline,
            [self.row(cgroup_file_peak_bytes=211.0)] * 3,
            candidate_mode="kernel",
            workload_bytes=100,
            compare_throughput=False,
        )
        self.assertTrue(passed["gates"]["cgroup_file_peak_bytes"]["passed"])
        self.assertFalse(failed["gates"]["cgroup_file_peak_bytes"]["passed"])

    def test_kernel_file_memory_improvement_uses_strict_combined_mad(self) -> None:
        baseline = [
            self.row(cgroup_file_peak_bytes=value)
            for value in (99.0, 100.0, 101.0)
        ]
        boundary = gate.evaluate_numeric_gates(
            baseline,
            [
                self.row(cgroup_file_peak_bytes=value)
                for value in (97.0, 98.0, 99.0)
            ],
            candidate_mode="kernel",
            workload_bytes=100,
            compare_throughput=False,
        )
        improved = gate.evaluate_numeric_gates(
            baseline,
            [
                self.row(cgroup_file_peak_bytes=value)
                for value in (96.0, 97.0, 98.0)
            ],
            candidate_mode="kernel",
            workload_bytes=100,
            compare_throughput=False,
        )

        self.assertFalse(boundary["gates"]["cgroup_file_peak_bytes"]["improved"])
        self.assertTrue(improved["gates"]["cgroup_file_peak_bytes"]["improved"])

    def test_kernel_zero_second_warm_read_skips_only_its_tail_gates(self) -> None:
        baseline_interval = {
            "samples": 1,
            "p95_upper_bound_ns": 100.0,
            "p99_upper_bound_ns": 100.0,
            "max_upper_bound_ns": 100.0,
        }
        reused_interval = {
            "samples": 0,
            "p95_upper_bound_ns": None,
            "p99_upper_bound_ns": None,
            "max_upper_bound_ns": None,
        }
        baseline = [
            self.row(callback_intervals={"second_fuse_read": baseline_interval})
        ] * 3
        candidate = [
            self.row(callback_intervals={"second_fuse_read": reused_interval})
        ] * 3

        result = gate.evaluate_numeric_gates(
            baseline,
            candidate,
            candidate_mode="kernel",
            workload_bytes=100,
        )

        self.assertTrue(result["passed"])
        self.assertFalse(
            any(name.startswith("second_fuse_read.") for name in result["gates"])
        )
        with self.assertRaisesRegex(gate.GateValidationError, "zero samples"):
            gate.evaluate_numeric_gates(
                baseline,
                candidate,
                candidate_mode="direct-async",
                workload_bytes=100,
            )

    def test_kernel_performance_regressions_are_diagnostic(self) -> None:
        baseline = {
            workload: [{**self.row(), "_workload_bytes": 100}] * 3
            for workload in gate.WORKLOADS
        }
        candidate = {
            workload: [
                {
                    **self.row(total_lifecycle_cpu_seconds_per_gib=10.0),
                    "_workload_bytes": 100,
                    "_memory_pressure": {
                        "passed": True,
                        "recovery_passed": False,
                    },
                }
            ]
            * 3
            for workload in gate.WORKLOADS
        }

        result = gate._compare_modes(baseline, candidate, "kernel")

        self.assertTrue(result["passed"])
        self.assertTrue(
            all(
                workload["performance_diagnostic"]
                and not workload["performance_passed"]
                for workload in result["workloads"].values()
            )
        )

class ScheduleValidationTest(unittest.TestCase):
    def test_exact_five_run_schedule_is_promotion_eligible(self) -> None:
        schedule = schedule_manifest()
        discovered = [
            (item["mode"], item["workload"], item["repetition"])
            for item in schedule["tuples"]
        ]
        result = gate.validate_schedule(schedule, discovered_tuples=discovered)
        self.assertTrue(result["promotion_eligible"])
        self.assertEqual(result["repetitions"], 5)

    def test_analyzed_schedule_requires_completed_root_and_passed_tuples(self) -> None:
        valid = schedule_manifest()
        for changed, message in (
            ({**valid, "status": "running"}, "completed"),
            (
                {
                    **valid,
                    "tuples": [
                        {**valid["tuples"][0], "status": "failed"},
                        *valid["tuples"][1:],
                    ],
                },
                "passed",
            ),
            (
                {
                    **valid,
                    "tuples": [
                        {**valid["tuples"][0], "collector_exit_status": 1},
                        *valid["tuples"][1:],
                    ],
                },
                "collector",
            ),
            (
                {
                    **valid,
                    "tuples": [
                        {**valid["tuples"][0], "collector_exit_status": False},
                        *valid["tuples"][1:],
                    ],
                },
                "integer",
            ),
            (
                {
                    **valid,
                    "tuples": [
                        {**valid["tuples"][0], "cleanup_error": "failed"},
                        *valid["tuples"][1:],
                    ],
                },
                "cleanup",
            ),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    gate.validate_schedule(changed)

    def test_schedule_requires_exact_latin_and_cyclic_indexed_order(self) -> None:
        valid = schedule_manifest()
        swapped = [valid["tuples"][1], valid["tuples"][0], *valid["tuples"][2:]]
        with self.assertRaisesRegex(gate.GateValidationError, "rotation"):
            gate.validate_schedule({**valid, "tuples": swapped})

    def test_schedule_rejects_missing_duplicate_extra_or_reused_resources(self) -> None:
        valid = schedule_manifest()
        duplicate = [*valid["tuples"][:-1], valid["tuples"][0]]
        extra_item = {
            **valid["tuples"][-1],
            "schedule_index": len(valid["tuples"]),
            "workload": "unexpected",
        }
        reused_cgroup = [
            valid["tuples"][0],
            {**valid["tuples"][1], "cgroup": valid["tuples"][0]["cgroup"]},
            *valid["tuples"][2:],
        ]
        reused_mount = [
            valid["tuples"][0],
            {
                **valid["tuples"][1],
                "mount_point": valid["tuples"][0]["mount_point"],
            },
            *valid["tuples"][2:],
        ]
        invalid_cases = (
            ({**valid, "tuples": valid["tuples"][:-1]}, "missing"),
            ({**valid, "tuples": duplicate}, "duplicate"),
            (
                {
                    **valid,
                    "tuples": [*valid["tuples"], extra_item],
                    "expected_tuple_count": len(valid["tuples"]) + 1,
                },
                "extra",
            ),
            ({**valid, "tuples": reused_cgroup}, "cgroup"),
            ({**valid, "tuples": reused_mount}, "mount"),
        )
        for schedule, message in invalid_cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(gate.GateValidationError, message):
                    gate.validate_schedule(schedule)

    def test_schedule_matches_filesystem_and_three_diagnostic_runs_are_ineligible(self) -> None:
        diagnostic = schedule_manifest(3, diagnostic=True)
        result = gate.validate_schedule(diagnostic)
        self.assertFalse(result["promotion_eligible"])
        with self.assertRaisesRegex(gate.GateValidationError, "filesystem"):
            gate.validate_schedule(
                diagnostic,
                discovered_tuples=[("kernel", "large-single", 1)],
            )
        for repetitions, is_diagnostic in ((3, False), (5, True), (4, False)):
            with self.subTest(repetitions=repetitions, diagnostic=is_diagnostic):
                with self.assertRaises(gate.GateValidationError):
                    gate.repetition_eligibility(repetitions, is_diagnostic)


class AnalyzerCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path.cwd() / "target/perf/test-linux-cache-analyzer"
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True)
        self.correctness_path = self.root / "correctness-manifest.json"
        self.write_json(
            self.correctness_path,
            correctness_manifest(self.root / "correctness-logs"),
        )
        self.correctness_sha256 = hashlib.sha256(
            self.correctness_path.read_bytes()
        ).hexdigest()
        self.create_matrix()

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    @staticmethod
    def write_json(path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    @staticmethod
    def geometry(mode: str) -> dict:
        return {
            "requested_async_dio": mode == "direct-async",
            "negotiated_async_dio": mode == "direct-async",
            "requested_writeback_cache": mode == "kernel",
            "negotiated_writeback_cache": mode == "kernel",
        }

    @staticmethod
    def client_report(workload: str, expectation: dict) -> dict:
        report = {
            "workload": workload,
            "file_count": expectation["file_count"],
            "bytes_written": expectation["verified_bytes_written"],
            "bytes_read": expectation["verified_bytes_read"],
            "expected_checksum": "verified-checksum",
            "actual_checksum": "verified-checksum",
            "completed": True,
            "cleanup_error": None,
            **(
                {"parallelism": expectation["parallelism"]}
                if "parallelism" in expectation
                else {}
            ),
        }
        if workload == "cross-file-barriers":
            report.update(
                {
                    "common_start_monotonic_ns": 1_100_000_000,
                    "file_completions": [
                        {
                            "file": "left.bin",
                            "fsync_started_monotonic_ns": 1_150_000_000,
                            "completion_monotonic_ns": 1_200_000_000,
                        },
                        {
                            "file": "right.bin",
                            "fsync_started_monotonic_ns": 1_250_000_000,
                            "completion_monotonic_ns": 1_300_000_000,
                        },
                    ],
                }
            )
        return report

    @staticmethod
    def bench_report(workload: str, expectation: dict, mount_point: str) -> dict:
        layout: object = (
            "single_file"
            if workload == "large-single"
            else {"fixed_file_size_mb": 1}
        )
        return {
            "run": {
                "workload_bytes": expectation["workload_bytes"],
                "file_count": expectation["file_count"],
                "files_kept": True,
                "stopped": False,
                "cleanup_error": None,
            },
            "config": {
                "target_path": mount_point,
                "run_mode": "mounted_filesystem",
                "test_mode": "read_write",
                "file_layout": layout,
                "keep_files": True,
                "execution_mode": "run_once",
            },
            "passes": [
                {
                    "phase": phase,
                    "pass_number": 1,
                    "bytes_processed": expectation["workload_bytes"],
                    "stopped": False,
                }
                for phase in ("write", "read")
            ],
        }

    @classmethod
    def runtime_snapshot(
        cls,
        mode: str,
        *,
        read_calls: int,
        vfs_read_calls: int,
        replace: int,
        reuse: int,
        callback_samples: int,
        read_bytes: int,
        write_bytes: int,
        read_histogram_samples: int | None = None,
        async_read_errors: int = 0,
    ) -> dict:
        callbacks = {}
        for kind in ("read", "write", "flush", "fsync", "release"):
            samples = (
                read_histogram_samples
                if kind == "read" and read_histogram_samples is not None
                else callback_samples
            )
            callbacks[kind] = {
                "calls": read_calls if kind == "read" else samples,
                "errors": 0,
                **(
                    {
                        "bytes": (
                            read_bytes
                            if kind == "read"
                            else write_bytes
                        )
                    }
                    if kind in ("read", "write")
                    else {}
                ),
                "latency": {
                    "latency_histogram": {
                        "bucket_upper_bounds_ns": [10, 20, 30],
                        "bucket_counts": [samples, 0, 0, 0],
                    },
                    "latency_quantiles_saturated": {
                        "p50": False,
                        "p95": False,
                        "p99": False,
                    },
                },
            }
        return {
            "schema_version": 2,
            "bridge": {},
            "ingress": {"dirty_mem_bytes": 0},
            "upload": {"pending_uploads": 0, "active_uploads": 0},
            "write_path": {
                "fuse_async_read": {
                    "pending_reads": 0,
                    "pending_bytes": 0,
                    "failed_reads": async_read_errors,
                    "canceled_reads": 0,
                },
                "fuse_async_write": {
                    "pending_writes": 0,
                    "pending_bytes": 0,
                    "pending_handles": 0,
                    "failed_writes": 0,
                    "task_failed_writes": 0,
                    "cancelled_writes": 0,
                },
                "os_cache": {
                    "active_writers": 0,
                    "active_foreground_handles": 0,
                    "replace": replace,
                    "reuse": reuse,
                },
                "fuse_connection": {
                    **cls.geometry(mode),
                    "configured_max_background": 16,
                    "negotiated_max_write_bytes": 1 << 20,
                    "parallel_direct_writes_supported": True,
                },
                "vfs_read_path": {"calls": vfs_read_calls},
                "fuse_callbacks": callbacks,
            },
        }

    @staticmethod
    def proc_stat(pid: int, user_ticks: int, system_ticks: int, start_time: str) -> str:
        fields = ["S", *(["0"] * 21)]
        fields[11] = str(user_ticks)
        fields[12] = str(system_ticks)
        fields[19] = start_time
        return f"{pid} (studiofs mount) {' '.join(fields)}\n"

    @staticmethod
    def cgroup_boundary(
        phase: str,
        monotonic_ns: int,
        current: int,
        peak: int,
        anon: int,
        file: int,
    ) -> dict:
        return {
            "schema_version": 1,
            "phase": phase,
            "monotonic_ns": monotonic_ns,
            "memory_current": current,
            "memory_peak": peak,
            "memory_stat": {
                "anon": anon,
                "file": file,
            },
        }

    @staticmethod
    def pressure_runtime_snapshot(sample: dict) -> dict:
        source_codes = {"host": 1, "cgroup": 2, "host_and_cgroup": 3, "fallback": 4}
        state_codes = {"normal": 1, "constrained": 2}
        counter_units = {
            "transition_count": "count",
            "sampling_failure_count": "count",
            "write_admission_wait_count": "count",
            "write_admission_wait_ns": "ns",
        }
        values = {
            **{
                name: value
                for name, value in sample.items()
                if name not in counter_units and name != "admitted_lower_bound_bytes"
            },
            "source": source_codes[sample["source"]],
            "state": state_codes[sample["state"]],
            "overcommitted_minimum": int(sample["overcommitted_minimum"]),
        }
        return {
            "schema_version": 2,
            "ingress": {"dirty_mem_bytes": sample.get("admitted_lower_bound_bytes", 0)},
            "write_path": {
                "fuse_async_read": {"pending_bytes": 0},
                "fuse_async_write": {"pending_bytes": 0},
            },
            "memory_pressure": {
                "gauges": {
                    name: {
                        "value": value,
                        "unit": (
                            "count"
                            if name
                            in {"source", "state", "generation", "overcommitted_minimum"}
                            else "bytes"
                        ),
                    }
                    for name, value in values.items()
                },
                "counters": {
                    name: {"value": sample[name], "unit": unit}
                    for name, unit in counter_units.items()
                },
                "latencies": {},
            },
        }

    def create_matrix(self) -> None:
        schedule = schedule_manifest()
        delegated_root = "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service"
        for item in schedule["tuples"]:
            mode = item["mode"]
            workload = item["workload"]
            repetition = item["repetition"]
            index = item["schedule_index"]
            run_dir = self.root / mode / workload / f"rep-{repetition:03d}"
            mount_point = str(self.root / "mounts" / f"mount-{index:03d}")
            cgroup = f"{delegated_root}/gate-{index:03d}"
            cache_dir = str(self.root / "cache" / f"cache-{index:03d}")
            mount_pid = 4_000 + index
            collector_pid = 9_000 + index
            mount_pid_start_time = str(50_000 + index)
            item.update(
                {
                    "mount_root": str(self.root / "mounts"),
                    "mount_point": mount_point,
                    "cache_dir": cache_dir,
                    "cgroup": cgroup,
                    "artifact_dir": str(run_dir),
                }
            )
            parallelism = 2 if workload == "same-file-parallel" else None
            expectation = gate.workload_expectation(
                workload,
                parallelism=parallelism,
            )
            manifest = {
                "schema_version": 1,
                "status": "complete",
                "promotion_eligible": True,
                "mode": mode,
                "workload": workload,
                "repetition": repetition,
                "cache_dir": cache_dir,
                "mount_point": mount_point,
                "mount_pid": mount_pid,
                "mount_pid_start_time": mount_pid_start_time,
                "runs": 1,
                "cgroup": cgroup,
                "git_revision": "studio-revision",
                "git_dirty": False,
                "studiofs_sha256": "a" * 64,
                "mount_executable_sha256": "a" * 64,
                "bench_git_revision": "bench-revision",
                "bench_git_dirty": False,
                "bench_sha256": "b" * 64,
                "correctness_manifest_path": str(self.correctness_path),
                "correctness_manifest_sha256": self.correctness_sha256,
                **expectation,
            }
            cgroup_preflight = {
                "filesystem_type": "cgroup2",
                "path": cgroup,
                "delegated_root": delegated_root,
                "controllers": ["memory"],
                "cgroup_procs": [],
                "memory_current": 0,
                "memory_peak": 0,
                "writable": {"cgroup.procs": True, "memory.max": True},
                "readable": {
                    "memory.current": True,
                    "memory.peak": True,
                    "memory.stat": True,
                    "memory.max": True,
                    "memory.events": True,
                },
                "fresh": True,
                "collector_pid": collector_pid,
                "mount_pid": mount_pid,
                "memberships_verified": True,
            }
            environment = {
                "schema_version": 1,
                "status": "complete",
                "promotion_eligible": True,
                "mode": mode,
                "workload": workload,
                "repetition": repetition,
                "cache_dir": cache_dir,
                "mount_point": mount_point,
                "mount_pid": mount_pid,
                "mount_pid_start_time": mount_pid_start_time,
                "runs": 1,
                "cgroup": cgroup,
                "test_fixture": False,
                "mount_environment": mode_environment(mode),
                "cgroup_preflight": cgroup_preflight,
                "correctness_manifest": {
                    "path": str(self.correctness_path),
                    "sha256": self.correctness_sha256,
                },
                "measurement": {
                    "foreground_start_monotonic_ns": 1_000_000_000,
                    "foreground_end_monotonic_ns": 2_000_000_000,
                    "drain_end_monotonic_ns": 3_000_000_000,
                    "clock_ticks_per_second": 100,
                    "pidstat_foreground_line_count": 2,
                    "memory_sample_interval_seconds": 0.1,
                    "pidstat_interval_seconds": 1,
                },
            }
            callback_kinds = (
                ("read", "flush", "release")
                if workload == "warm-reopen"
                else ("read", "write", "flush", "fsync", "release")
            )
            runtime_start = self.runtime_snapshot(
                mode,
                read_calls=0,
                vfs_read_calls=0,
                replace=0,
                reuse=0,
                callback_samples=0,
                read_bytes=0,
                write_bytes=0,
            )
            warm_midpoint = None
            end_read_calls = 1
            end_callback_samples = 1
            end_replace = 0
            end_reuse = 0
            warm_proof = None
            if workload == "warm-reopen":
                warm_midpoint = self.runtime_snapshot(
                    mode,
                    read_calls=1,
                    vfs_read_calls=1,
                    replace=1,
                    reuse=0,
                    callback_samples=1,
                    read_bytes=expectation["verified_bytes_read"] // 2,
                    write_bytes=0,
                )
                end_read_calls = 1 if mode == "kernel" else 2
                end_callback_samples = 2
                end_replace = 1
                end_reuse = 1
                warm_proof = {
                    "checksum_verified": True,
                    "first_fuse_read_calls": 1,
                    "first_vfs_read_path_calls": 1,
                    "first_os_cache_replace": 1,
                    "first_os_cache_reuse": 0,
                    "second_fuse_read_calls": end_read_calls - 1,
                    "second_vfs_read_path_calls": end_read_calls - 1,
                    "second_os_cache_replace": 0,
                    "second_os_cache_reuse": 1,
                }
            runtime_end = self.runtime_snapshot(
                mode,
                read_calls=end_read_calls,
                vfs_read_calls=end_read_calls,
                replace=end_replace,
                reuse=end_reuse,
                callback_samples=end_callback_samples,
                read_bytes=expectation["verified_bytes_read"],
                write_bytes=expectation["verified_bytes_written"],
                read_histogram_samples=(
                    end_read_calls if workload == "warm-reopen" else None
                ),
            )
            processed_gib = (
                expectation["verified_bytes_written"]
                + expectation["verified_bytes_read"]
            ) / float(1 << 30)
            foreground_user_cpu = 10 / 100
            foreground_system_cpu = 5 / 100
            foreground_cpu = foreground_user_cpu + foreground_system_cpu
            drain_user_cpu = 4 / 100
            drain_system_cpu = 1 / 100
            drain_cpu = drain_user_cpu + drain_system_cpu
            total_cpu = foreground_cpu + drain_cpu
            callback_intervals = {
                f"fuse_{kind}": RawCallbackEvidenceTest.interval(
                    end_callback_samples
                )
                for kind in callback_kinds
            }
            if workload == "warm-reopen":
                callback_intervals = {
                    **{
                        f"first_fuse_{kind}": RawCallbackEvidenceTest.interval()
                        for kind in callback_kinds
                    },
                    "second_fuse_read": RawCallbackEvidenceTest.interval(
                        end_read_calls - 1
                    ),
                    "second_fuse_flush": RawCallbackEvidenceTest.interval(),
                    "second_fuse_release": RawCallbackEvidenceTest.interval(),
                }
            pressure_evidence = (
                MemoryPressureEvidenceTest.evidence(
                    expectation["verified_bytes_written"],
                    expectation["verified_bytes_read"],
                )
                if workload in ("large-single", "small-128k")
                else None
            )
            if pressure_evidence is not None:
                pressure_evidence["throughput_mb_per_second"]["profile"] = workload
                pressure_evidence["setup"]["profile"] = workload
            derived = {
                "schema_version": 1,
                "status": "complete",
                "promotion_eligible": True,
                "command_exit_status": 0,
                "callback_errors": 0,
                "async_errors": 0,
                "task_errors": 0,
                "cancellations": 0,
                "foreground_start_idle": True,
                "drain_end_idle": True,
                "negotiated_geometry": self.geometry(mode),
                "verified_bytes_written": expectation["verified_bytes_written"],
                "verified_bytes_read": expectation["verified_bytes_read"],
                "processed_gib": processed_gib,
                "foreground_wall_seconds": 1.0,
                "foreground_throughput_bytes_per_second": (
                    expectation["verified_bytes_written"]
                    + expectation["verified_bytes_read"]
                ),
                "foreground_mount_user_cpu_seconds": foreground_user_cpu,
                "foreground_mount_system_cpu_seconds": foreground_system_cpu,
                "foreground_mount_cpu_seconds": foreground_cpu,
                "drain_mount_user_cpu_seconds": drain_user_cpu,
                "drain_mount_system_cpu_seconds": drain_system_cpu,
                "drain_mount_cpu_seconds": drain_cpu,
                "total_lifecycle_mount_cpu_seconds": total_cpu,
                "total_lifecycle_cpu_seconds_per_gib": total_cpu / processed_gib,
                "foreground_average_cores": 1.0,
                "foreground_p95_cores": 1.0,
                "foreground_peak_cores": 1.0,
                "drain_average_cores": 0.5,
                "drain_p95_cores": 0.5,
                "drain_peak_cores": 0.5,
                "drain_duration_seconds": 1.0,
                "daemon_rss_peak_bytes": 100,
                "daemon_anon_peak_bytes": 50,
                "cgroup_total_peak_bytes": 150,
                "cgroup_file_peak_bytes": 80,
                "cgroup_anon_peak_bytes": 60,
                "callback_intervals": callback_intervals,
                **({"memory_pressure": pressure_evidence} if pressure_evidence else {}),
                **({"warm_proof": warm_proof} if warm_proof else {}),
            }
            cleanup = {
                "schema_version": 1,
                "cleanup_started_monotonic_ns": 4_000_000_000,
                "cleanup_ended_monotonic_ns": 5_000_000_000,
                "cleanup_error": None,
                "cleanup_drain_idle": True,
                "unmount_timed_out": False,
                "unmount_exit_status": 0,
                "unmount_started_monotonic_ns": 6_000_000_000,
                "unmount_ended_monotonic_ns": 7_000_000_000,
                "mount_exit_timed_out": False,
                "mount_exit_status": 0,
            }
            self.write_json(run_dir / "environment.json", environment)
            self.write_json(run_dir / "manifest.json", manifest)
            self.write_json(run_dir / "derived.json", derived)
            self.write_json(run_dir / "cleanup.json", cleanup)
            (run_dir / "command.txt").write_text("client command\n")
            for name, runtime in (
                ("runtime-foreground-start.json", runtime_start),
                ("runtime-foreground-end.json", runtime_end),
                ("runtime-drain-end.json", runtime_end),
                ("runtime-cleanup-end.json", runtime_end),
            ):
                self.write_json(run_dir / name, runtime)
            if pressure_evidence is not None:
                cgroup = pressure_evidence["cgroup"]
                self.write_json(
                    run_dir / "memory-pressure-config.json",
                    {
                        "schema_version": 1,
                        **gate.derive_pressure_target(
                            effective_limit=cgroup["memory_max_bytes"],
                            negotiated_max_write=1 << 20,
                        ),
                        "memory_max_bytes": cgroup["memory_max_bytes"],
                        "memory_current_before_bytes": cgroup[
                            "memory_current_before_bytes"
                        ],
                        "holder_bytes": pressure_evidence["holder"]["bytes"],
                        "negotiated_max_write_bytes": 1 << 20,
                        "negotiated_max_write_source": gate.NEGOTIATED_MAX_WRITE_SOURCE,
                        "observed_admitted_lower_bound_bytes": pressure_evidence["setup"][
                            "observed_admitted_lower_bound_bytes"
                        ],
                        "observed_generation": pressure_evidence["setup"][
                            "observed_generation"
                        ],
                    },
                )
                for phase, current, events, monotonic_ns in (
                    (
                        "before",
                        cgroup["memory_current_before_bytes"],
                        cgroup["events_before"],
                        50_000_000,
                    ),
                    (
                        "pressure",
                        cgroup["memory_current_pressure_bytes"],
                        cgroup["events_before"],
                        500_000_000,
                    ),
                    (
                        "after-holder-release",
                        cgroup["memory_current_after_holder_release_bytes"],
                        cgroup["events_before"],
                        1_100_000_000,
                    ),
                    (
                        "after",
                        cgroup["memory_current_after_holder_release_bytes"],
                        cgroup["events_after"],
                        3_500_000_000,
                    ),
                ):
                    self.write_json(
                        run_dir / f"memory-events-{phase}.json",
                        {
                            "schema_version": 1,
                            "phase": phase,
                            "monotonic_ns": monotonic_ns,
                            "memory_max_bytes": cgroup["memory_max_bytes"],
                            "memory_current_bytes": current,
                            "events": events,
                        },
                    )
                self.write_json(run_dir / "memory-holder.json", pressure_evidence["holder"])
                self.write_json(run_dir / "pressure-setup.json", pressure_evidence["setup"])
                profile_report = (
                    self.bench_report(workload, expectation, mount_point)
                    if workload == "large-single"
                    else self.client_report(workload, expectation)
                )
                profile_report_bytes = (
                    json.dumps(profile_report, indent=2, sort_keys=True) + "\n"
                ).encode()
                self.write_json(
                    run_dir / "pressure-profile.json",
                    {
                        "schema_version": 1,
                        "profile": workload,
                        "runner": "bench" if workload == "large-single" else "inline",
                        "workload_pid": 4343,
                        "workload_start_time": "234567",
                        "feeder_pid": pressure_evidence["setup"]["feeder_pid"],
                        "feeder_start_time": pressure_evidence["setup"][
                            "feeder_start_time"
                        ],
                        "feeder_live_before_profile": True,
                        "feeder_live_after_profile": True,
                        "started_monotonic_ns": 550_000_000,
                        "read_throughput_mb_per_second": pressure_evidence[
                            "throughput_mb_per_second"
                        ]["pressure"][0],
                        "completed_monotonic_ns": 600_000_000,
                        "exit_status": 0,
                        "source_report_sha256": hashlib.sha256(
                            profile_report_bytes
                        ).hexdigest(),
                    },
                )
                measurer = pressure_evidence["throughput_mb_per_second"]
                self.write_json(
                    run_dir / "pressure-measure.json",
                    {
                        "schema_version": 1,
                        "method": measurer["method"],
                        "chunk_bytes": measurer["chunk_bytes"],
                        "bytes_read": measurer["bytes_read"],
                        "throughput_mb_per_second": measurer["pressure"][0],
                        "started_monotonic_ns": 650_000_000,
                        "completed_monotonic_ns": 700_000_000,
                        "exit_status": 0,
                    },
                )
                self.write_json(
                    run_dir / "pressure-hash.json",
                    {
                        "schema_version": 1,
                        "bytes_read": pressure_evidence["verification"]["bytes_read"],
                        "sha256": pressure_evidence["verification"]["pressure_sha256"],
                        "completed_monotonic_ns": 750_000_000,
                        "exit_status": 0,
                    },
                )
                self.write_json(
                    run_dir / "recovery-measure.json",
                    {
                        "schema_version": 1,
                        "method": measurer["method"],
                        "chunk_bytes": measurer["chunk_bytes"],
                        "bytes_read": measurer["bytes_read"],
                        "throughput_mb_per_second": measurer["recovery"][0],
                        "started_monotonic_ns": 1_200_000_000,
                        "completed_monotonic_ns": 1_300_000_000,
                        "exit_status": 0,
                    },
                )
                self.write_json(
                    run_dir / "recovery-hash.json",
                    {
                        "schema_version": 1,
                        "bytes_read": pressure_evidence["verification"]["bytes_read"],
                        "sha256": pressure_evidence["verification"]["recovery_sha256"],
                        "completed_monotonic_ns": 1_400_000_000,
                        "exit_status": 0,
                    },
                )
                samples = pressure_evidence["runtime_samples"]
                for name, sample, captured_ns in (
                    ("runtime-setup-observed.json", samples["setup_observed"], None),
                    (
                        "runtime-pressure-before-profile.json",
                        samples["pressure"][0],
                        450_000_000,
                    ),
                    (
                        "runtime-pressure-after-profile.json",
                        samples["pressure"][1],
                        800_000_000,
                    ),
                    (
                        "runtime-recovery-boundary.json",
                        samples["recovery_boundary"],
                        None,
                    ),
                    ("runtime-recovery-1.json", samples["recovery"][0], None),
                    ("runtime-recovery-2.json", samples["recovery"][1], None),
                ):
                    snapshot = self.pressure_runtime_snapshot(sample)
                    if captured_ns is not None:
                        snapshot["captured_monotonic_ns"] = captured_ns
                    self.write_json(run_dir / name, snapshot)
            if warm_midpoint is not None:
                self.write_json(
                    run_dir / "runtime-warm-first-end.json",
                    warm_midpoint,
                )
            proc_boundaries = (
                ("proc-foreground-start", 100, 50),
                ("proc-foreground-end", 110, 55),
                ("proc-drain-end", 114, 56),
            )
            for phase, user_ticks, system_ticks in proc_boundaries:
                for name in ("stat", "status", "smaps_rollup", "io"):
                    path = run_dir / phase / f"{name}.txt"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    if name == "stat":
                        payload = self.proc_stat(
                            mount_pid,
                            user_ticks,
                            system_ticks,
                            mount_pid_start_time,
                        )
                    elif name == "status":
                        payload = "VmRSS:\t100 kB\nRssAnon:\t50 kB\n"
                    elif name == "smaps_rollup":
                        payload = "Rss: 100 kB\nAnonymous: 50 kB\n"
                    else:
                        payload = (
                            "rchar: 0\nwchar: 0\nsyscr: 0\nsyscw: 0\n"
                            "read_bytes: 0\nwrite_bytes: 0\n"
                            "cancelled_write_bytes: 0\n"
                        )
                    path.write_text(payload)
            for name, boundary in (
                (
                    "cgroup-foreground-start.json",
                    self.cgroup_boundary(
                        "foreground-start", 1_000_000_000, 100, 120, 40, 60
                    ),
                ),
                (
                    "cgroup-foreground-end.json",
                    self.cgroup_boundary(
                        "foreground-end", 2_000_000_000, 120, 140, 50, 70
                    ),
                ),
                (
                    "cgroup-drain-end.json",
                    self.cgroup_boundary(
                        "drain-end", 3_000_000_000, 130, 150, 60, 80
                    ),
                ),
            ):
                self.write_json(run_dir / name, boundary)
            process_samples = (
                {"monotonic_ns": 1_000_000_000, "vm_rss_bytes": 80, "rss_anon_bytes": 40},
                {"monotonic_ns": 2_000_000_000, "vm_rss_bytes": 100, "rss_anon_bytes": 50},
                {"monotonic_ns": 3_000_000_000, "vm_rss_bytes": 90, "rss_anon_bytes": 45},
            )
            cgroup_samples = (
                {
                    "monotonic_ns": 1_000_000_000,
                    "memory_current_bytes": 100,
                    "memory_peak_bytes": 120,
                    "anon_bytes": 40,
                    "file_bytes": 60,
                },
                {
                    "monotonic_ns": 2_000_000_000,
                    "memory_current_bytes": 120,
                    "memory_peak_bytes": 140,
                    "anon_bytes": 50,
                    "file_bytes": 70,
                },
                {
                    "monotonic_ns": 3_000_000_000,
                    "memory_current_bytes": 130,
                    "memory_peak_bytes": 150,
                    "anon_bytes": 60,
                    "file_bytes": 80,
                },
            )
            (run_dir / "process-memory-samples.jsonl").write_text(
                "".join(json.dumps(row) + "\n" for row in process_samples)
            )
            (run_dir / "cgroup-memory-samples.jsonl").write_text(
                "".join(json.dumps(row) + "\n" for row in cgroup_samples)
            )
            (run_dir / "pidstat.txt").write_text(
                "# Time UID PID %usr %system %guest %wait %CPU CPU Command\n"
                f"00:00:01 1000 {mount_pid} 80.0 20.0 0.0 0.0 100.0 0 studiofs\n"
                f"00:00:02 1000 {mount_pid} 40.0 10.0 0.0 0.0 50.0 0 studiofs\n"
            )
            if workload in ("large-single", "fixed-1m"):
                report = self.bench_report(workload, expectation, mount_point)
                self.write_json(run_dir / "client-report.json", report)
                self.write_json(run_dir / "studiofs-bench-report-000.json", report)
            else:
                self.write_json(
                    run_dir / "client-report.json",
                    self.client_report(workload, expectation),
                )
        self.write_json(self.root / "schedule.json", schedule)

    def test_cli_reads_matrix_and_writes_all_decision_outputs(self) -> None:
        output = self.root / "comparison"
        exit_code = gate.main(
            [
                "--direct-serial",
                str(self.root / "direct-serial"),
                "--direct-async",
                str(self.root / "direct-async"),
                "--kernel",
                str(self.root / "kernel"),
                "--correctness-manifest",
                str(self.correctness_path),
                "--output",
                str(output),
            ]
        )
        self.assertEqual(exit_code, 0)
        comparison = json.loads((output / "comparison.json").read_text())
        self.assertTrue(comparison["passed"])
        self.assertTrue(comparison["promotion_eligible"])
        self.assertTrue(comparison["direct_async_passed"])
        self.assertTrue(comparison["kernel_passed"])
        self.assertTrue(comparison["all_candidates_passed"])
        self.assertEqual(comparison["kernel_baseline"], "direct-async")
        for candidate in ("direct_async", "kernel"):
            for workload in comparison[candidate]["workloads"].values():
                for metric in workload["gates"].values():
                    self.assertIsInstance(metric["improved"], bool)
            for workload in ("large-single", "small-128k"):
                self.assertTrue(
                    comparison[candidate]["workloads"][workload]["memory_pressure"][
                        "recovery_passed"
                    ]
                )
        self.assertIn("direct-async", (output / "summary.md").read_text())
        self.assertIn("Memory pressure", (output / "summary.md").read_text())
        self.assertIn("direct-async", (output / "decision.md").read_text())

    def test_cli_rejects_missing_or_empty_stable_evidence(self) -> None:
        (self.root / "direct-serial/large-single/rep-001/pidstat.txt").unlink()
        output = self.root / "missing-artifact-comparison"
        exit_code = gate.main(
            [
                "--direct-serial",
                str(self.root / "direct-serial"),
                "--direct-async",
                str(self.root / "direct-async"),
                "--kernel",
                str(self.root / "kernel"),
                "--correctness-manifest",
                str(self.correctness_path),
                "--output",
                str(output),
            ]
        )
        self.assertEqual(exit_code, 1)
        comparison = json.loads((output / "comparison.json").read_text())
        self.assertIn("required artifact", comparison["errors"][0])

    def test_artifact_layout_rejects_file_and_directory_symlinks(self) -> None:
        run_dir = self.root / "direct-serial" / "large-single" / "rep-001"
        pidstat = run_dir / "pidstat.txt"
        pidstat_target = run_dir / "pidstat-target.txt"
        pidstat.rename(pidstat_target)
        pidstat.symlink_to(pidstat_target)
        with self.assertRaisesRegex(gate.GateValidationError, "required artifact"):
            gate.validate_artifact_layout(run_dir, workload="large-single")
        pidstat.unlink()
        pidstat_target.rename(pidstat)

        proc_dir = run_dir / "proc-foreground-start"
        proc_target = run_dir / "proc-foreground-start-target"
        proc_dir.rename(proc_target)
        proc_dir.symlink_to(proc_target, target_is_directory=True)
        with self.assertRaisesRegex(gate.GateValidationError, "artifact directory"):
            gate.validate_artifact_layout(run_dir, workload="large-single")

    def test_cli_rejects_missing_promotion_pressure_evidence(self) -> None:
        derived_path = (
            self.root / "direct-async" / "small-128k" / "rep-001" / "derived.json"
        )
        derived = json.loads(derived_path.read_text())
        derived.pop("memory_pressure")
        self.write_json(derived_path, derived)
        output = self.root / "missing-pressure-comparison"
        exit_code = gate.main(
            [
                "--direct-serial",
                str(self.root / "direct-serial"),
                "--direct-async",
                str(self.root / "direct-async"),
                "--kernel",
                str(self.root / "kernel"),
                "--correctness-manifest",
                str(self.correctness_path),
                "--output",
                str(output),
            ]
        )

        self.assertEqual(exit_code, 1)
        comparison = json.loads((output / "comparison.json").read_text())
        self.assertIn(
            "memory pressure evidence",
            comparison["direct_async"]["errors"][0],
        )

    def test_cli_rejects_pressure_derived_raw_mismatch(self) -> None:
        path = (
            self.root
            / "direct-async"
            / "large-single"
            / "rep-001"
            / "runtime-pressure-before-profile.json"
        )
        snapshot = json.loads(path.read_text())
        snapshot["memory_pressure"]["gauges"]["envelope_bytes"]["value"] += 1
        self.write_json(path, snapshot)
        output = self.root / "pressure-raw-mismatch"

        exit_code = gate.main(
            [
                "--direct-serial",
                str(self.root / "direct-serial"),
                "--direct-async",
                str(self.root / "direct-async"),
                "--kernel",
                str(self.root / "kernel"),
                "--correctness-manifest",
                str(self.correctness_path),
                "--output",
                str(output),
            ]
        )

        self.assertEqual(exit_code, 1)
        comparison = json.loads((output / "comparison.json").read_text())
        self.assertIn("raw memory pressure", comparison["direct_async"]["errors"][0])

    def test_pressure_sources_must_match_validated_raw_evidence(self) -> None:
        run_dir = self.root / "direct-async" / "large-single" / "rep-001"
        profile_path = run_dir / "pressure-profile.json"
        profile = json.loads(profile_path.read_text())
        profile["source_report_sha256"] = "b" * 64
        self.write_json(profile_path, profile)
        derived = json.loads((run_dir / "derived.json").read_text())

        with self.assertRaisesRegex(gate.GateValidationError, "source report mismatch"):
            gate.validate_memory_pressure_artifacts(
                run_dir,
                derived["memory_pressure"],
            )
        profile["source_report_sha256"] = hashlib.sha256(
            (run_dir / "client-report.json").read_bytes()
        ).hexdigest()
        self.write_json(profile_path, profile)
        config_path = run_dir / "memory-pressure-config.json"
        config = json.loads(config_path.read_text())
        config["negotiated_max_write_source"] = "wrong"
        self.write_json(config_path, config)

        with self.assertRaisesRegex(gate.GateValidationError, "raw source mismatch"):
            gate.validate_memory_pressure_artifacts(
                run_dir,
                derived["memory_pressure"],
            )

    def test_cli_rejects_benchmark_report_copy_mismatch(self) -> None:
        original = (
            self.root
            / "direct-serial"
            / "large-single"
            / "rep-001"
            / "studiofs-bench-report-000.json"
        )
        report = json.loads(original.read_text())
        report["run"]["workload_bytes"] -= 1
        self.write_json(original, report)

        output = self.root / "bench-copy-mismatch"
        exit_code = gate.main(
            [
                "--direct-serial",
                str(self.root / "direct-serial"),
                "--direct-async",
                str(self.root / "direct-async"),
                "--kernel",
                str(self.root / "kernel"),
                "--correctness-manifest",
                str(self.correctness_path),
                "--output",
                str(output),
            ]
        )
        self.assertEqual(exit_code, 1)
        comparison = json.loads((output / "comparison.json").read_text())
        self.assertIn("benchmark report copy", comparison["errors"][0])

    def test_cli_accepts_warm_foreground_end_backlog(self) -> None:
        foreground_end = (
            self.root
            / "kernel"
            / "warm-reopen"
            / "rep-001"
            / "runtime-foreground-end.json"
        )
        snapshot = json.loads(foreground_end.read_text())
        snapshot["ingress"]["dirty_mem_bytes"] = 1
        self.write_json(foreground_end, snapshot)

        output = self.root / "warm-foreground-backlog"
        exit_code = gate.main(
            [
                "--direct-serial",
                str(self.root / "direct-serial"),
                "--direct-async",
                str(self.root / "direct-async"),
                "--kernel",
                str(self.root / "kernel"),
                "--correctness-manifest",
                str(self.correctness_path),
                "--output",
                str(output),
            ]
        )
        self.assertEqual(exit_code, 0)

    def test_cli_rejects_noncanonical_cold_workload_bytes(self) -> None:
        wrong_bytes = 3_999_999_999
        for mode in ("direct-serial", "direct-async", "kernel"):
            for repetition in range(1, 6):
                run_dir = (
                    self.root
                    / mode
                    / "large-single"
                    / f"rep-{repetition:03d}"
                )
                manifest = json.loads((run_dir / "manifest.json").read_text())
                manifest.update(
                    {
                        "workload_bytes": wrong_bytes,
                        "verified_bytes_written": wrong_bytes,
                        "verified_bytes_read": wrong_bytes,
                    }
                )
                self.write_json(run_dir / "manifest.json", manifest)
                derived = json.loads((run_dir / "derived.json").read_text())
                derived.update(
                    {
                        "verified_bytes_written": wrong_bytes,
                        "verified_bytes_read": wrong_bytes,
                    }
                )
                self.write_json(run_dir / "derived.json", derived)
                report = json.loads((run_dir / "client-report.json").read_text())
                report["run"]["workload_bytes"] = wrong_bytes
                for benchmark_pass in report["passes"]:
                    benchmark_pass["bytes_processed"] = wrong_bytes
                self.write_json(run_dir / "client-report.json", report)
                self.write_json(run_dir / "studiofs-bench-report-000.json", report)
        output = self.root / "wrong-cold-comparison"
        exit_code = gate.main(
            [
                "--direct-serial",
                str(self.root / "direct-serial"),
                "--direct-async",
                str(self.root / "direct-async"),
                "--kernel",
                str(self.root / "kernel"),
                "--correctness-manifest",
                str(self.correctness_path),
                "--output",
                str(output),
            ]
        )
        self.assertEqual(exit_code, 1)
        comparison = json.loads((output / "comparison.json").read_text())
        self.assertIn("workload_bytes", comparison["errors"][0])

    def test_async_failure_uses_serial_and_reports_kernel_pass_separately(self) -> None:
        for repetition in range(1, 6):
            run_dir = (
                self.root
                / "direct-async"
                / "large-single"
                / f"rep-{repetition:03d}"
            )
            path = run_dir / "derived.json"
            derived = json.loads(path.read_text())
            for field in (
                "foreground_average_cores",
                "foreground_p95_cores",
                "foreground_peak_cores",
            ):
                derived[field] = 2.0
            self.write_json(path, derived)
            pidstat = (run_dir / "pidstat.txt").read_text().replace(
                "80.0 20.0 0.0 0.0 100.0",
                "160.0 40.0 0.0 0.0 200.0",
            )
            (run_dir / "pidstat.txt").write_text(pidstat)
        output = self.root / "async-failed-comparison"
        exit_code = gate.main(
            [
                "--direct-serial",
                str(self.root / "direct-serial"),
                "--direct-async",
                str(self.root / "direct-async"),
                "--kernel",
                str(self.root / "kernel"),
                "--correctness-manifest",
                str(self.correctness_path),
                "--output",
                str(output),
            ]
        )
        self.assertEqual(exit_code, 1)
        comparison = json.loads((output / "comparison.json").read_text())
        self.assertFalse(comparison["direct_async_passed"])
        self.assertTrue(comparison["direct_async"]["eligible"])
        self.assertTrue(comparison["kernel_passed"])
        self.assertFalse(comparison["all_candidates_passed"])
        self.assertFalse(comparison["passed"])
        self.assertEqual(comparison["kernel_baseline"], "direct-serial")

    def test_invalid_direct_async_evidence_still_evaluates_kernel_against_serial(self) -> None:
        path = (
            self.root
            / "direct-async"
            / "large-single"
            / "rep-001"
            / "derived.json"
        )
        derived = json.loads(path.read_text())
        derived["foreground_average_cores"] = "not-numeric"
        self.write_json(path, derived)

        output = self.root / "async-invalid-comparison"
        exit_code = gate.main(
            [
                "--direct-serial",
                str(self.root / "direct-serial"),
                "--direct-async",
                str(self.root / "direct-async"),
                "--kernel",
                str(self.root / "kernel"),
                "--correctness-manifest",
                str(self.correctness_path),
                "--output",
                str(output),
            ]
        )

        self.assertEqual(exit_code, 1)
        comparison = json.loads((output / "comparison.json").read_text())
        self.assertFalse(comparison["direct_async"]["eligible"])
        self.assertFalse(comparison["direct_async_passed"])
        self.assertTrue(comparison["direct_async"]["errors"])
        self.assertEqual(comparison["kernel_baseline"], "direct-serial")
        self.assertTrue(comparison["kernel_passed"])

    def test_cli_rejects_gate_critical_derived_or_raw_corruption(self) -> None:
        run_dir = self.root / "direct-serial" / "large-single" / "rep-001"
        cases = ("derived CPU", "derived memory", "raw async read")
        for index, case in enumerate(cases):
            with self.subTest(case=case):
                self.create_matrix()
                if case.startswith("derived"):
                    path = run_dir / "derived.json"
                    payload = json.loads(path.read_text())
                    field = (
                        "total_lifecycle_cpu_seconds_per_gib"
                        if case == "derived CPU"
                        else "cgroup_file_peak_bytes"
                    )
                    payload[field] += 1
                else:
                    path = run_dir / "runtime-drain-end.json"
                    payload = json.loads(path.read_text())
                    payload["write_path"]["fuse_async_read"]["failed_reads"] = 1
                self.write_json(path, payload)
                output = self.root / f"raw-corruption-{index}"
                exit_code = gate.main(
                    [
                        "--direct-serial",
                        str(self.root / "direct-serial"),
                        "--direct-async",
                        str(self.root / "direct-async"),
                        "--kernel",
                        str(self.root / "kernel"),
                        "--correctness-manifest",
                        str(self.correctness_path),
                        "--output",
                        str(output),
                    ]
                )
                self.assertEqual(exit_code, 1)

    def test_cli_requires_warm_midpoint_artifact(self) -> None:
        path = (
            self.root
            / "kernel"
            / "warm-reopen"
            / "rep-001"
            / "runtime-warm-first-end.json"
        )
        path.unlink()
        output = self.root / "missing-warm-midpoint"
        exit_code = gate.main(
            [
                "--direct-serial",
                str(self.root / "direct-serial"),
                "--direct-async",
                str(self.root / "direct-async"),
                "--kernel",
                str(self.root / "kernel"),
                "--correctness-manifest",
                str(self.correctness_path),
                "--output",
                str(output),
            ]
        )
        self.assertEqual(exit_code, 1)
        comparison = json.loads((output / "comparison.json").read_text())
        self.assertIn("runtime-warm-first-end", comparison["errors"][0])

    def test_cli_rejects_correctness_promotion_mismatch(self) -> None:
        correctness = json.loads(self.correctness_path.read_text())
        correctness["promotion_eligible"] = False
        self.write_json(self.correctness_path, correctness)
        output = self.root / "correctness-promotion-mismatch"
        exit_code = gate.main(
            [
                "--direct-serial",
                str(self.root / "direct-serial"),
                "--direct-async",
                str(self.root / "direct-async"),
                "--kernel",
                str(self.root / "kernel"),
                "--correctness-manifest",
                str(self.correctness_path),
                "--output",
                str(output),
            ]
        )
        self.assertEqual(exit_code, 1)
        comparison = json.loads((output / "comparison.json").read_text())
        self.assertIn("correctness promotion", comparison["errors"][0])

    def test_cli_rejects_identity_membership_or_promotion_mismatch(self) -> None:
        run_dir = self.root / "direct-serial" / "large-single" / "rep-001"
        cases = (
            ("environment.json", ("cache_dir",), "/wrong/cache"),
            ("manifest.json", ("cache_dir",), "/wrong/cache"),
            ("environment.json", ("runs",), 2),
            ("manifest.json", ("runs",), 2),
            ("environment.json", ("mount_pid",), 99_999),
            (
                "environment.json",
                ("cgroup_preflight", "collector_pid"),
                0,
            ),
            (
                "environment.json",
                ("cgroup_preflight", "memberships_verified"),
                False,
            ),
            (
                "environment.json",
                ("cgroup_preflight", "mount_pid"),
                99_999,
            ),
            ("environment.json", ("promotion_eligible",), False),
            ("manifest.json", ("promotion_eligible",), False),
            ("derived.json", ("promotion_eligible",), False),
        )
        for index, (name, keys, value) in enumerate(cases):
            with self.subTest(name=name, keys=keys):
                self.create_matrix()
                path = run_dir / name
                payload = json.loads(path.read_text())
                target = payload
                for key in keys[:-1]:
                    target = target[key]
                target[keys[-1]] = value
                self.write_json(path, payload)
                output = self.root / f"identity-corruption-{index}"
                exit_code = gate.main(
                    [
                        "--direct-serial",
                        str(self.root / "direct-serial"),
                        "--direct-async",
                        str(self.root / "direct-async"),
                        "--kernel",
                        str(self.root / "kernel"),
                        "--correctness-manifest",
                        str(self.correctness_path),
                        "--output",
                        str(output),
                    ]
                )
                self.assertEqual(exit_code, 1)


class ShellContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.root = self.repo_root / "target/perf/test-linux-cache-shell-contract"
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True)
        self.collector = Path(__file__).with_name("linux-cache-gate.sh").resolve()
        self.matrix = Path(__file__).with_name("linux-cache-matrix.sh").resolve()

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def run_script(
        self,
        script: Path,
        *args: str,
        env: dict[str, str] | None = None,
        timeout: float = 10,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(script), *args],
            cwd=self.repo_root,
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )

    @staticmethod
    def write_executable(path: Path, body: str) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"#!/usr/bin/env bash\nset -euo pipefail\n{body}")
        path.chmod(0o755)
        return path

    def test_shell_help_lists_required_and_evidence_options(self) -> None:
        contracts = (
            (
                self.collector,
                (
                    "--mode",
                    "--workload",
                    "--mount-point",
                    "--mount-pid",
                    "--cgroup",
                    "--bench-repo",
                    "--correctness-manifest",
                    "--diagnostic",
                    "--dry-run",
                ),
            ),
            (
                self.matrix,
                (
                    "--volume",
                    "--mount-root",
                    "--cgroup-root",
                    "--bench-repo",
                    "--diagnostic",
                    "--dry-run",
                ),
            ),
        )
        for script, expected_options in contracts:
            with self.subTest(script=script.name):
                completed = self.run_script(script, "--help")
                self.assertEqual(completed.returncode, 0, completed.stderr)
                for option in expected_options:
                    self.assertIn(option, completed.stdout)

    def test_single_choice_count_options_are_not_public_cli(self) -> None:
        cases = (
            (self.collector, "--runs", "1"),
            (self.matrix, "--repetitions", "5"),
        )
        for script, option, value in cases:
            with self.subTest(script=script.name, option=option):
                completed = self.run_script(script, option, value)
                self.assertEqual(completed.returncode, 2)
                self.assertIn(f"unknown argument: {option}", completed.stderr)

    def test_collector_dry_run_writes_stable_warm_reopen_skeleton(self) -> None:
        output_root = self.root / "collector"
        completed = self.run_script(
            self.collector,
            "--mode",
            "kernel",
            "--workload",
            "warm-reopen",
            "--mount-point",
            str(self.root / "mount"),
            "--mount-pid",
            "4242",
            "--output-root",
            str(output_root),
            "--dry-run",
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        artifact_dir = Path(completed.stdout.strip())
        self.assertEqual(artifact_dir.parent, output_root)

        expected_files = {
            "environment.json",
            "manifest.json",
            "command.txt",
            "cgroup-memory-samples.jsonl",
            "process-memory-samples.jsonl",
            "pidstat.txt",
            "client-report.json",
            "cleanup.json",
            "derived.json",
            "summary.md",
            "decision.md",
        }
        expected_files.update(
            f"runtime-{phase}.json"
            for phase in (
                "foreground-start",
                "foreground-end",
                "warm-first-end",
                "drain-end",
                "cleanup-end",
            )
        )
        expected_files.update(
            f"cgroup-{phase}.json"
            for phase in ("foreground-start", "foreground-end", "drain-end")
        )
        expected_files.update(
            f"proc-{phase}/{name}.txt"
            for phase in ("foreground-start", "foreground-end", "drain-end")
            for name in ("stat", "status", "smaps_rollup", "io")
        )
        actual_files = {
            str(path.relative_to(artifact_dir))
            for path in artifact_dir.rglob("*")
            if path.is_file()
        }
        self.assertEqual(actual_files, expected_files)
        self.assertEqual(
            json.loads(
                (artifact_dir / "runtime-warm-first-end.json").read_text()
            ),
            {"status": "dry-run", "phase": "warm-first-end"},
        )

    def test_collector_dry_run_declares_pressure_artifacts_without_starting_holder(self) -> None:
        output_root = self.root / "pressure-collector"
        completed = self.run_script(
            self.collector,
            "--mode",
            "direct-async",
            "--workload",
            "small-128k",
            "--mount-point",
            str(self.root / "mount"),
            "--mount-pid",
            "4242",
            "--output-root",
            str(output_root),
            "--dry-run",
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        artifact_dir = Path(completed.stdout.strip())
        for name in (
            "memory-events-before.json",
            "memory-events-after-holder-release.json",
            "memory-events-after.json",
            "memory-holder.json",
            "pressure-setup.json",
            "pressure-profile.json",
            "pressure-measure.json",
            "pressure-hash.json",
            "recovery-measure.json",
            "recovery-hash.json",
            "runtime-pressure-before-profile.json",
            "runtime-pressure-after-profile.json",
            "runtime-setup-observed.json",
            "runtime-recovery-boundary.json",
            "runtime-recovery-1.json",
            "runtime-recovery-2.json",
        ):
            self.assertEqual(
                json.loads((artifact_dir / name).read_text())["status"],
                "dry-run",
            )
        self.assertIn("pressure_scenario=true", (artifact_dir / "command.txt").read_text())
        self.assertFalse((artifact_dir / "memory-holder.ready").exists())
        self.assertFalse((artifact_dir / "memory-holder.release").exists())

    def test_matrix_dry_run_writes_exact_diagnostic_rotation(self) -> None:
        output_root = self.root / "matrix"
        bench_repo = self.root / "studiofs-bench-source"
        completed = self.run_script(
            self.matrix,
            "--volume",
            "shell-contract",
            "--mount-root",
            str(self.root / "mounts"),
            "--cgroup-root",
            str(self.root / "cgroups"),
            "--output-root",
            str(output_root),
            "--bench-repo",
            str(bench_repo),
            "--diagnostic",
            "--dry-run",
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(Path(completed.stdout.strip()), output_root)

        schedule = json.loads((output_root / "schedule.json").read_text())
        tuples = schedule["tuples"]
        self.assertEqual(
            {
                key: schedule[key]
                for key in (
                    "repetitions",
                    "diagnostic",
                    "dry_run",
                    "promotion_eligible",
                    "status",
                    "expected_tuple_count",
                )
            },
            {
                "repetitions": 3,
                "diagnostic": True,
                "dry_run": True,
                "promotion_eligible": False,
                "status": "planned",
                "expected_tuple_count": 54,
            },
        )

        workload_rows = (
            (
                "large-single",
                "fixed-1m",
                "small-128k",
                "warm-reopen",
                "same-file-parallel",
                "cross-file-barriers",
            ),
            (
                "fixed-1m",
                "small-128k",
                "warm-reopen",
                "same-file-parallel",
                "cross-file-barriers",
                "large-single",
            ),
            (
                "small-128k",
                "warm-reopen",
                "same-file-parallel",
                "cross-file-barriers",
                "large-single",
                "fixed-1m",
            ),
        )
        mode_rows = (
            ("direct-serial", "direct-async", "kernel"),
            ("direct-async", "kernel", "direct-serial"),
            ("kernel", "direct-serial", "direct-async"),
        )
        expected_rotation = [
            (repetition, workload, mode)
            for repetition, (workloads, modes) in enumerate(
                zip(workload_rows, mode_rows, strict=True),
                start=1,
            )
            for workload in workloads
            for mode in modes
        ]
        self.assertEqual(
            [
                (item["repetition"], item["workload"], item["mode"])
                for item in tuples
            ],
            expected_rotation,
        )
        self.assertEqual(
            [item["schedule_index"] for item in tuples],
            list(range(54)),
        )
        resource_fields = (
            "mount_root",
            "mount_point",
            "cache_dir",
            "cgroup",
            "artifact_dir",
        )
        self.assertEqual(
            {
                field: len({item[field] for item in tuples})
                for field in resource_fields
            },
            {field: 54 for field in resource_fields},
        )
        self.assertTrue(
            all(
                f"--bench-repo {bench_repo}" in item["collector_command"]
                and item["collector_command"].endswith("--diagnostic")
                for item in tuples
            )
        )

    def test_collector_real_preflight_rejects_wrong_mount_fstype_quickly(self) -> None:
        bin_dir = self.root / "bin"
        findmnt = self.write_executable(bin_dir / "findmnt", "printf 'ext4\\n'\n")
        pidstat = self.write_executable(bin_dir / "pidstat", "exit 0\n")
        bench = self.write_executable(bin_dir / "studiofs-bench", "exit 0\n")
        studiofs = self.write_executable(bin_dir / "studiofs", "exit 0\n")
        correctness = self.root / "correctness-manifest.json"
        correctness.write_text("{}\n")
        mount_point = self.root / "mount"
        mount_point.mkdir()
        cgroup = self.root / "cgroup"
        cgroup.mkdir()
        environment = {
            name: value
            for name, value in os.environ.items()
            if not name.startswith("STUDIOFS_CACHE_GATE_")
        }
        environment.update(
            {
                "STUDIOFS_CACHE_GATE_TEST_FIXTURE": "1",
                "STUDIOFS_CACHE_GATE_FINDMNT": str(findmnt),
                "STUDIOFS_CACHE_GATE_PIDSTAT": str(pidstat),
            }
        )

        started = time.monotonic()
        completed = self.run_script(
            self.collector,
            "--mode",
            "direct-serial",
            "--workload",
            "large-single",
            "--mount-point",
            str(mount_point),
            "--mount-pid",
            str(os.getpid()),
            "--cgroup",
            str(cgroup),
            "--bench",
            str(bench),
            "--studiofs",
            str(studiofs),
            "--correctness-manifest",
            str(correctness),
            "--output-root",
            str(self.root / "preflight-output"),
            env=environment,
            timeout=5,
        )
        elapsed = time.monotonic() - started

        self.assertEqual(completed.returncode, 1, completed.stderr)
        self.assertIn(
            "mount point must have fuse.studiofs type",
            completed.stderr,
        )
        self.assertIn("found ext4", completed.stderr)
        self.assertLess(elapsed, 3)


if __name__ == "__main__":
    unittest.main()
