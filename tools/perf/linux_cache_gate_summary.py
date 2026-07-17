#!/usr/bin/env python3
"""Validate and summarize Linux cache-gate artifacts using only the stdlib."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
from collections.abc import Sequence
from pathlib import Path


EXPECTED_RUNTIME_SCHEMA = 2
NEGOTIATED_MAX_WRITE_SOURCE = (
    "runtime_snapshot.write_path.fuse_connection.negotiated_max_write_bytes"
)
IDLE_GAUGE_PATHS = (
    ("ingress", "dirty_mem_bytes"),
    ("upload", "pending_uploads"),
    ("upload", "active_uploads"),
    ("write_path", "fuse_async_read", "pending_reads"),
    ("write_path", "fuse_async_read", "pending_bytes"),
    ("write_path", "fuse_async_write", "pending_writes"),
    ("write_path", "fuse_async_write", "pending_bytes"),
    ("write_path", "fuse_async_write", "pending_handles"),
    ("write_path", "os_cache", "active_writers"),
    ("write_path", "os_cache", "active_foreground_handles"),
)


class GateValidationError(ValueError):
    """Raised when evidence cannot be used by the performance gate."""


def median_mad(samples: Sequence[float]) -> dict[str, float]:
    """Return the median and median absolute deviation for nonempty samples."""
    if not samples:
        raise GateValidationError("at least one sample is required")
    sample_median = statistics.median(samples)
    deviations = [abs(value - sample_median) for value in samples]
    return {
        "median": sample_median,
        "mad": statistics.median(deviations),
    }


def throughput_passes(
    baseline_samples: Sequence[float],
    candidate_samples: Sequence[float],
    minimum_ratio: float = 0.95,
) -> bool:
    """Return whether candidate median throughput stays on the allowed boundary."""
    if minimum_ratio <= 0:
        raise GateValidationError("minimum throughput ratio must be positive")
    baseline = median_mad(baseline_samples)["median"]
    candidate = median_mad(candidate_samples)["median"]
    return candidate >= baseline * minimum_ratio


def robust_upper_gate(
    baseline_samples: Sequence[float],
    candidate_samples: Sequence[float],
    *,
    relative_floor: float,
    absolute_floor: float = 0.0,
) -> dict[str, float | bool]:
    """Apply the accepted combined-MAD upper-regression allowance."""
    if relative_floor < 0 or absolute_floor < 0:
        raise GateValidationError("regression floors must be nonnegative")
    baseline = median_mad(baseline_samples)
    candidate = median_mad(candidate_samples)
    allowance = max(
        2 * (baseline["mad"] + candidate["mad"]),
        relative_floor * baseline["median"],
        absolute_floor,
    )
    limit = baseline["median"] + allowance
    return {
        "passed": candidate["median"] <= limit,
        "improved": baseline["median"] - candidate["median"]
        > baseline["mad"] + candidate["mad"],
        "baseline_median": baseline["median"],
        "baseline_mad": baseline["mad"],
        "candidate_median": candidate["median"],
        "candidate_mad": candidate["mad"],
        "allowance": allowance,
        "limit": limit,
    }


def _histogram_saturated(histogram: dict) -> bool:
    saturation = histogram.get("latency_quantiles_saturated", {})
    return bool(histogram.get("saturated", False)) or any(
        bool(saturation.get(quantile, False)) for quantile in ("p50", "p95", "p99")
    )


def _nearest_rank_upper_bound(
    bounds: list[int], counts: list[int], quantile: float
) -> int:
    rank = math.ceil(quantile * sum(counts))
    cumulative = 0
    for bound, count in zip(bounds, counts, strict=True):
        cumulative += count
        if cumulative >= rank:
            return bound
    raise GateValidationError("histogram nearest-rank lookup failed")


def histogram_interval(
    before: dict,
    after: dict,
    *,
    allow_zero: bool = False,
) -> dict[str, object]:
    """Rebuild an interval histogram from monotonic mutually-exclusive buckets."""
    before_bounds = before.get("bucket_upper_bounds_ns")
    after_bounds = after.get("bucket_upper_bounds_ns")
    if before_bounds != after_bounds:
        raise GateValidationError("histogram bounds changed during interval")
    if not isinstance(before_bounds, list) or not all(
        isinstance(value, int) and value >= 0 for value in before_bounds
    ):
        raise GateValidationError("histogram bounds are invalid")

    before_counts = before.get("bucket_counts")
    after_counts = after.get("bucket_counts")
    expected_count = len(before_bounds) + 1
    if (
        not isinstance(before_counts, list)
        or not isinstance(after_counts, list)
        or len(before_counts) != expected_count
        or len(after_counts) != expected_count
        or not all(isinstance(value, int) and value >= 0 for value in before_counts)
        or not all(isinstance(value, int) and value >= 0 for value in after_counts)
    ):
        raise GateValidationError("histogram bucket shape is invalid")

    deltas = []
    for before_count, after_count in zip(before_counts, after_counts, strict=True):
        if after_count < before_count:
            raise GateValidationError("histogram counter decreased during interval")
        deltas.append(after_count - before_count)

    if before_counts[-1] or after_counts[-1] or deltas[-1]:
        raise GateValidationError("histogram overflow makes interval ineligible")
    if _histogram_saturated(before) or _histogram_saturated(after):
        raise GateValidationError("histogram is saturated")

    interval_counts = deltas[:-1]
    samples = sum(interval_counts)
    if samples == 0 and not allow_zero:
        raise GateValidationError("histogram interval has zero samples")

    nonzero_indexes = [index for index, count in enumerate(interval_counts) if count]
    return {
        "bucket_upper_bounds_ns": list(before_bounds),
        "bucket_counts": interval_counts,
        "samples": samples,
        "p50_upper_bound_ns": (
            _nearest_rank_upper_bound(before_bounds, interval_counts, 0.50)
            if samples
            else None
        ),
        "p95_upper_bound_ns": (
            _nearest_rank_upper_bound(before_bounds, interval_counts, 0.95)
            if samples
            else None
        ),
        "p99_upper_bound_ns": (
            _nearest_rank_upper_bound(before_bounds, interval_counts, 0.99)
            if samples
            else None
        ),
        "max_upper_bound_ns": (
            before_bounds[nonzero_indexes[-1]] if nonzero_indexes else None
        ),
    }


def _value_at(payload: dict, path: tuple[str, ...]) -> object:
    value: object = payload
    for key in path:
        if not isinstance(value, dict) or key not in value:
            raise GateValidationError(f"missing required field: {'.'.join(path)}")
        value = value[key]
    return value


def validate_raw_callback_intervals(
    mode: str,
    workload: str,
    foreground_start: dict,
    foreground_end: dict,
    derived: dict,
    *,
    warm_first_end: dict | None = None,
) -> dict[str, dict]:
    """Rebuild required FUSE callback intervals and match the derived artifact."""
    if workload == "warm-reopen":
        callback_kinds = ("read", "flush", "release")
        if not isinstance(warm_first_end, dict):
            raise GateValidationError("warm callback midpoint is missing")
        boundaries = (
            ("first", foreground_start, warm_first_end),
            ("second", warm_first_end, foreground_end),
        )
    elif workload in (
        "large-single",
        "fixed-1m",
        "small-128k",
        "same-file-parallel",
        "cross-file-barriers",
    ):
        callback_kinds = ("read", "write", "flush", "fsync", "release")
        boundaries = ((None, foreground_start, foreground_end),)
    else:
        raise GateValidationError(f"unknown workload: {workload}")

    rebuilt = {}
    for phase, before, after in boundaries:
        for kind in callback_kinds:
            key = f"{phase}_fuse_{kind}" if phase else f"fuse_{kind}"
            path = ("write_path", "fuse_callbacks", kind, "latency")
            before_latency = _value_at(before, path)
            after_latency = _value_at(after, path)
            if not isinstance(before_latency, dict) or not isinstance(after_latency, dict):
                raise GateValidationError(f"raw {key} latency evidence is invalid")
            before_histogram = before_latency.get("latency_histogram")
            after_histogram = after_latency.get("latency_histogram")
            before_saturation = before_latency.get("latency_quantiles_saturated")
            after_saturation = after_latency.get("latency_quantiles_saturated")
            if (
                not isinstance(before_histogram, dict)
                or not isinstance(after_histogram, dict)
                or not isinstance(before_saturation, dict)
                or not isinstance(after_saturation, dict)
            ):
                raise GateValidationError(f"raw {key} histogram is missing")
            allow_zero = mode == "kernel" and key == "second_fuse_read"
            interval = histogram_interval(
                {
                    **before_histogram,
                    "latency_quantiles_saturated": before_saturation,
                },
                {
                    **after_histogram,
                    "latency_quantiles_saturated": after_saturation,
                },
                allow_zero=allow_zero,
            )
            validate_histogram_applicability(
                mode,
                workload,
                key,
                interval["samples"],
            )
            rebuilt[key] = interval

    if derived.get("callback_intervals") != rebuilt:
        raise GateValidationError("derived callback intervals do not match raw evidence")
    return rebuilt


def snapshot_is_idle(snapshot: dict) -> bool:
    """Return whether every accepted idle gauge is present and zero."""
    for path in IDLE_GAUGE_PATHS:
        value = _value_at(snapshot, path)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise GateValidationError(f"idle gauge is invalid: {'.'.join(path)}")
        if value != 0:
            return False
    return True


def idle_streak_reached(snapshots: Sequence[dict], consecutive: int = 3) -> bool:
    """Return whether the sequence contains the required consecutive idle polls."""
    if consecutive < 1:
        raise GateValidationError("idle streak length must be positive")
    streak = 0
    for snapshot in snapshots:
        if snapshot_is_idle(snapshot):
            streak += 1
            if streak >= consecutive:
                return True
        else:
            streak = 0
    return False


def memory_pressure_sample(snapshot: dict) -> dict[str, object]:
    """Normalize one runtime-stats memory-pressure section for gate evidence."""
    _require_schema_version(
        snapshot, EXPECTED_RUNTIME_SCHEMA, "runtime memory pressure"
    )
    section = snapshot.get("memory_pressure")
    gauges = section.get("gauges") if isinstance(section, dict) else None
    counters = section.get("counters") if isinstance(section, dict) else None
    if not isinstance(gauges, dict) or not isinstance(counters, dict):
        raise GateValidationError("runtime memory_pressure metrics are missing")

    def value(metrics: dict, name: str, unit: str) -> int:
        metric = metrics.get(name)
        if not isinstance(metric, dict):
            raise GateValidationError(f"runtime memory_pressure metric is missing: {name}")
        if metric.get("unit") != unit:
            raise GateValidationError(f"runtime memory_pressure unit is invalid: {name}")
        return _nonnegative_integer(metric.get("value"), f"memory_pressure.{name}")

    def gauge(name: str, unit: str) -> int:
        return value(gauges, name, unit)

    def counter(name: str, unit: str) -> int:
        return value(counters, name, unit)

    sources = {
        0: "disabled",
        1: "host",
        2: "cgroup",
        3: "host_and_cgroup",
        4: "fallback",
    }
    states = {0: "disabled", 1: "normal", 2: "constrained"}
    source_code = gauge("source", "count")
    state_code = gauge("state", "count")
    if source_code not in sources or state_code not in states:
        raise GateValidationError("runtime memory_pressure wire code is invalid")
    overcommitted = gauge("overcommitted_minimum", "count")
    if overcommitted not in (0, 1):
        raise GateValidationError("runtime overcommitted minimum flag is invalid")
    admitted = gauge("admitted_managed_bytes", "bytes")
    return {
        "source": sources[source_code],
        "state": states[state_code],
        "generation": gauge("generation", "count"),
        "envelope_bytes": gauge("envelope_bytes", "bytes"),
        "memory_cache_limit_bytes": gauge("memory_cache_limit_bytes", "bytes"),
        "dirty_limit_bytes": gauge("dirty_limit_bytes", "bytes"),
        "transit_limit_bytes": gauge("transit_limit_bytes", "bytes"),
        "async_write_limit_bytes": gauge("async_write_limit_bytes", "bytes"),
        "overcommitted_minimum": bool(overcommitted),
        "transition_count": counter("transition_count", "count"),
        "sampling_failure_count": counter("sampling_failure_count", "count"),
        "write_admission_wait_count": counter("write_admission_wait_count", "count"),
        "write_admission_wait_ns": counter("write_admission_wait_ns", "ns"),
        "admitted_lower_bound_bytes": admitted,
    }


def record_post_profile_pressure_snapshot(snapshot: dict, destination: Path) -> dict:
    """Preserve and validate the runtime sample captured after the pressure profile."""
    with destination.open("w", encoding="utf-8") as output:
        json.dump(snapshot, output, indent=2, sort_keys=True)
        output.write("\n")

    sample = memory_pressure_sample(snapshot)
    if sample["source"] != "host_and_cgroup":
        raise GateValidationError("post-profile memory pressure source is invalid")
    envelope = sample["envelope_bytes"]
    if envelope == 0:
        raise GateValidationError("post-profile memory envelope must be positive")
    managed = sum(
        sample[field]
        for field in (
            "memory_cache_limit_bytes",
            "dirty_limit_bytes",
            "transit_limit_bytes",
            "async_write_limit_bytes",
        )
    )
    if managed > envelope:
        raise GateValidationError("post-profile managed memory limits exceed the envelope")
    return sample


def negotiated_max_write_bytes(snapshot: dict) -> int:
    _require_schema_version(
        snapshot, EXPECTED_RUNTIME_SCHEMA, "negotiated max_write runtime"
    )
    write_path = snapshot.get("write_path")
    connection = (
        write_path.get("fuse_connection") if isinstance(write_path, dict) else None
    )
    if (
        not isinstance(connection, dict)
        or connection.get("negotiated_max_write_bytes") is None
    ):
        raise GateValidationError("negotiated max_write bytes are missing")
    # Schema 2 types this canonical field as Option<u32>; its name carries the byte unit.
    value = _nonnegative_integer(
        connection.get("negotiated_max_write_bytes"), "negotiated max_write bytes"
    )
    if value == 0:
        raise GateValidationError("negotiated max_write bytes must be positive")
    if value > (1 << 32) - 1:
        raise GateValidationError("negotiated max_write bytes must fit u32")
    return value


def derive_pressure_target(*, effective_limit: int, negotiated_max_write: int) -> dict[str, int]:
    """Choose the smallest non-overcommitted envelope for the live feeder."""
    gib = 1 << 30
    limit = _nonnegative_integer(effective_limit, "effective memory limit")
    reserve = max(gib, limit * 15 // 100)
    mib = 1 << 20
    max_write = _nonnegative_integer(negotiated_max_write, "negotiated max_write")
    target_allocatable = 512 * mib
    while True:
        envelope = target_allocatable * 75 // 100
        limits = (
            max(envelope * 40 // 100, 64 * mib),
            max(envelope * 25 // 100, 64 * mib),
            max(envelope * 15 // 100, 32 * mib),
            max(envelope * 10 // 100, max(32 * mib, max_write)),
        )
        managed = sum(limits)
        if managed <= envelope * 90 // 100:
            break
        target_allocatable += mib
    return {
        "reserve_bytes": reserve,
        "target_allocatable_bytes": target_allocatable,
        "target_envelope_bytes": envelope,
        "required_admitted_bytes": envelope + 1,
        "managed_minimum_bytes": managed,
        "negotiated_max_write_bytes": max_write,
        "overcommitted_minimum": managed > envelope * 90 // 100,
    }


def derive_pressure_scenario(
    *,
    memory_current: int,
    memory_max: int,
    host_available: int,
    parent_available: int | None,
    observed_admitted_lower_bound: int,
    negotiated_max_write: int,
) -> dict[str, int]:
    """Size the holder after the bounded feeder has established live admission."""
    current = _nonnegative_integer(memory_current, "memory.current")
    limit = _nonnegative_integer(memory_max, "memory.max")
    host = _nonnegative_integer(host_available, "host available memory")
    if current > limit:
        raise GateValidationError("memory.current exceeds memory.max")
    target = derive_pressure_target(
        effective_limit=limit, negotiated_max_write=negotiated_max_write
    )
    admitted = _nonnegative_integer(
        observed_admitted_lower_bound, "observed admitted lower bound"
    )
    if admitted < target["required_admitted_bytes"]:
        raise GateValidationError("observed admission is below the pressure target")
    available = min(host, limit - current)
    if parent_available is not None:
        available = min(
            available,
            _nonnegative_integer(parent_available, "parent available memory"),
        )
    desired_available = target["reserve_bytes"] + target["target_allocatable_bytes"]
    holder = available - desired_available
    if holder <= 0:
        raise GateValidationError("memory pressure holder must be positive")
    return {
        **target,
        "memory_max_bytes": limit,
        "memory_current_after_feeder_bytes": current,
        "observed_admitted_lower_bound_bytes": admitted,
        "holder_bytes": holder,
        "desired_available_bytes": desired_available,
    }


def run_memory_holder(
    holder_bytes: int,
    start_path: Path,
    ready_path: Path,
    release_path: Path,
    result_path: Path,
) -> None:
    """Allocate and touch memory between explicit start and release signals."""
    import time

    size = _nonnegative_integer(holder_bytes, "holder bytes")
    if size == 0:
        raise GateValidationError("holder bytes must be positive")
    started = time.monotonic_ns()
    while not start_path.exists():
        time.sleep(0.01)
    allocation = bytearray(size)
    for offset in range(0, size, 4096):
        allocation[offset] = 1
    ready = time.monotonic_ns()
    ready_path.write_text("ready\n")
    while not release_path.exists():
        time.sleep(0.01)
    released = time.monotonic_ns()
    del allocation
    exited = time.monotonic_ns()
    result_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "bytes": size,
                "started_monotonic_ns": started,
                "ready_monotonic_ns": ready,
                "released_monotonic_ns": released,
                "exited_monotonic_ns": exited,
                "exit_status": 0,
            },
            sort_keys=True,
        )
        + "\n"
    )


def cleanup_memory_holder_files(*paths: Path) -> None:
    for path in paths:
        path.unlink(missing_ok=True)


def read_cgroup_memory_boundary(
    cgroup: Path,
    phase: str,
    *,
    monotonic_ns: int | None = None,
) -> dict[str, object]:
    """Read the cgroup files retained at one pressure boundary."""
    import time

    raw_limit = (cgroup / "memory.max").read_text().strip()
    if raw_limit == "max":
        raise GateValidationError("memory.max is not finite")
    memory_max = _nonnegative_integer(int(raw_limit), "memory.max")
    current = _nonnegative_integer(
        int((cgroup / "memory.current").read_text().strip()), "memory.current"
    )
    events = {}
    for line in (cgroup / "memory.events").read_text().splitlines():
        name, raw_value = line.split()
        events[name] = _nonnegative_integer(int(raw_value), f"memory.events {name}")
    if memory_max == 0 or current > memory_max:
        raise GateValidationError("invalid cgroup memory boundary")
    for required in ("oom", "oom_kill"):
        if required not in events:
            raise GateValidationError(f"memory.events is missing {required}")
    return {
        "schema_version": 1,
        "phase": phase,
        "monotonic_ns": time.monotonic_ns() if monotonic_ns is None else monotonic_ns,
        "memory_max_bytes": memory_max,
        "memory_current_bytes": current,
        "events": events,
    }


def _normalized_path(path: str) -> str:
    return os.path.abspath(os.path.normpath(path))


def validate_diagnostics_attach(
    payload: dict,
    *,
    mount_pid: int,
    mount_point: str,
    expected_schema: int = EXPECTED_RUNTIME_SCHEMA,
) -> dict:
    """Validate an out-of-band diagnostics-attach response and return its snapshot."""
    if payload.get("runtime_snapshot_source") == "mounted_file" or payload.get(
        "mounted_file_fallback", False
    ):
        raise GateValidationError("runtime evidence must use out-of-band diagnostics")
    session = payload.get("session")
    if not isinstance(session, dict) or session.get("pid") != mount_pid:
        raise GateValidationError("diagnostics session pid does not match mount pid")
    actual_mount = session.get("mount_point")
    if not isinstance(actual_mount, str) or _normalized_path(actual_mount) != _normalized_path(
        mount_point
    ):
        raise GateValidationError("diagnostics session mount point does not match target")
    snapshot = payload.get("runtime_snapshot")
    if not isinstance(snapshot, dict):
        raise GateValidationError("diagnostics runtime snapshot is missing")
    _require_schema_version(snapshot, expected_schema, "diagnostics runtime snapshot")
    for path in IDLE_GAUGE_PATHS:
        _value_at(snapshot, path)
    return snapshot


MODE_ENVIRONMENTS = {
    "direct-serial": ("direct", "0"),
    "direct-async": ("direct", "1"),
    "kernel": ("kernel", "0"),
}


def validate_mode_environment(mode: str, environment: dict[str, str]) -> None:
    expected = MODE_ENVIRONMENTS.get(mode)
    if expected is None:
        raise GateValidationError(f"unknown cache mode: {mode}")
    actual = (
        environment.get("STUDIOFS_LINUX_FUSE_CACHE_POLICY"),
        environment.get("STUDIOFS_FUSE_ASYNC_DIO"),
    )
    if actual != expected:
        raise GateValidationError(
            f"mode environment mismatch for {mode}: expected {expected}, got {actual}"
        )


def validate_negotiated_geometry(mode: str, geometry: dict) -> None:
    if mode not in MODE_ENVIRONMENTS:
        raise GateValidationError(f"unknown cache mode: {mode}")
    async_dio = mode == "direct-async"
    writeback = mode == "kernel"
    expected = {
        "requested_async_dio": async_dio,
        "negotiated_async_dio": async_dio,
        "requested_writeback_cache": writeback,
        "negotiated_writeback_cache": writeback,
    }
    mismatched = [
        key
        for key, value in expected.items()
        if type(geometry.get(key)) is not bool or geometry[key] is not value
    ]
    if mismatched:
        raise GateValidationError(
            f"negotiated geometry contradicts {mode}: {', '.join(mismatched)}"
        )


def validate_cgroup_preflight(preflight: dict) -> None:
    """Require an empty, fresh delegated cgroup v2 memory leaf."""
    if preflight.get("filesystem_type") != "cgroup2":
        raise GateValidationError("cgroup filesystem must be cgroup2")
    if "memory" not in preflight.get("controllers", []):
        raise GateValidationError("cgroup memory controller is not active")
    path = preflight.get("path")
    delegated_root = preflight.get("delegated_root")
    if not isinstance(path, str) or not isinstance(delegated_root, str) or path == delegated_root:
        raise GateValidationError("fresh cgroup leaf is required")
    if preflight.get("fresh") is not True:
        raise GateValidationError("fresh cgroup evidence is required")
    if preflight.get("cgroup_procs") != []:
        raise GateValidationError("fresh cgroup must have empty cgroup.procs")
    if preflight.get("memory_current") != 0:
        raise GateValidationError("fresh cgroup memory.current must be zero")
    if preflight.get("memory_peak") != 0:
        raise GateValidationError("fresh cgroup memory.peak must be zero")
    if preflight.get("writable", {}).get("cgroup.procs") is not True:
        raise GateValidationError("cgroup.procs must be writable")
    if preflight.get("writable", {}).get("memory.max") is not True:
        raise GateValidationError("memory.max must be writable")
    readable = preflight.get("readable", {})
    missing = [
        name
        for name in (
            "memory.current",
            "memory.peak",
            "memory.stat",
            "memory.max",
            "memory.events",
        )
        if readable.get(name) is not True
    ]
    if missing:
        raise GateValidationError(f"required cgroup files are unreadable: {', '.join(missing)}")


REQUIRED_CORRECTNESS_TESTS = {
    "low_level::tests::mounted_smoke_mounts_and_unmounts_cleanly": "direct-serial",
    "low_level::tests::mounted_regular_file_io_is_visible_across_reopen": "direct-serial",
    "low_level::tests::mounted_namespace_mutations_are_visible_through_readdir": "direct-serial",
    "low_level::tests::mounted_direct_rollback_preserves_reopen_visibility": "direct-serial",
    "low_level::tests::mounted_same_file_parallel_direct_writes_preserve_disjoint_regions": "direct-async",
    "low_level::tests::mounted_forced_unmount_drains_admitted_and_waiting_raw_requests": "direct-async",
    "low_level::tests::mounted_cross_file_fsync_and_close_do_not_share_async_watermark": "direct-async",
    "low_level::tests::mounted_kernel_cache_coherence_matrix": "kernel",
    "low_level::tests::mounted_kernel_writeback_flush_and_release_are_locally_durable": "kernel",
    "low_level::tests::mounted_kernel_remote_listener_invalidation": "kernel",
}


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdefABCDEF" for character in value)
    )


def validate_cleanup_record(cleanup: dict) -> None:
    """Reject incomplete cleanup, unmount, or mount-process teardown."""
    if cleanup.get("cleanup_error") is not None:
        raise GateValidationError("cleanup error makes run ineligible")
    if cleanup.get("cleanup_drain_idle") is not True:
        raise GateValidationError("cleanup drain did not reach idle")
    if cleanup.get("unmount_timed_out") is not False:
        raise GateValidationError("unmount timed out")
    unmount_status = _nonnegative_integer(
        cleanup.get("unmount_exit_status"), "unmount exit status"
    )
    if unmount_status != 0:
        fallback_status = _nonnegative_integer(
            cleanup.get("fallback_unmount_exit_status"),
            "fallback unmount exit status",
        )
        if fallback_status != 0:
            raise GateValidationError("unmount and fallback exit statuses are nonzero")
    if cleanup.get("mount_exit_timed_out") is not False:
        raise GateValidationError("mount process exit timed out")
    if _nonnegative_integer(cleanup.get("mount_exit_status"), "mount exit status") != 0:
        raise GateValidationError("mount process exit status is nonzero")


def validate_run_eligibility(mode: str, derived: dict, cleanup: dict) -> None:
    """Validate non-statistical evidence required for one measured run."""
    if _nonnegative_integer(derived.get("command_exit_status"), "command exit status") != 0:
        raise GateValidationError("workload command failed")
    for field in ("callback_errors", "async_errors", "task_errors", "cancellations"):
        value = derived.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value != 0:
            raise GateValidationError(f"{field} makes run ineligible")
    for field in ("foreground_start_idle", "drain_end_idle"):
        if derived.get(field) is not True:
            raise GateValidationError(f"{field} idle evidence is required")
    geometry = derived.get("negotiated_geometry")
    if not isinstance(geometry, dict):
        raise GateValidationError("negotiated geometry is missing")
    validate_negotiated_geometry(mode, geometry)
    if not isinstance(cleanup, dict):
        raise GateValidationError("cleanup record is missing")
    validate_cleanup_record(cleanup)


def validate_memory_pressure_evidence(
    mode: str,
    workload: str,
    evidence: object,
    *,
    expected_written: int,
    expected_read: int,
    promotion_eligible: bool,
) -> dict[str, object]:
    """Validate the mounted pressure proof attached to one measured run."""
    required = (
        promotion_eligible
        and mode in MODES
        and workload in ("large-single", "small-128k")
    )
    if not isinstance(evidence, dict) or evidence.get("status") != "complete":
        if required:
            raise GateValidationError("complete memory pressure evidence is required")
        return {
            "status": "not_run",
            "passed": True,
            "recovery_passed": True,
            "performance_diagnostic": mode == "kernel",
        }

    cgroup = evidence.get("cgroup")
    holder = evidence.get("holder")
    setup = evidence.get("setup")
    runtime_samples = evidence.get("runtime_samples")
    verification = evidence.get("verification")
    throughput = evidence.get("throughput_mb_per_second")
    cpu = evidence.get("cpu")
    if not all(
        isinstance(item, dict)
        for item in (cgroup, holder, setup, runtime_samples, verification, throughput, cpu)
    ):
        raise GateValidationError("memory pressure evidence has an invalid shape")
    assert isinstance(cgroup, dict)
    assert isinstance(holder, dict)
    assert isinstance(setup, dict)
    assert isinstance(runtime_samples, dict)
    assert isinstance(verification, dict)
    assert isinstance(throughput, dict)
    assert isinstance(cpu, dict)
    for name, payload in (("holder", holder), ("setup", setup)):
        if _nonnegative_integer(payload.get("schema_version"), f"{name} schema") != 1:
            raise GateValidationError(f"{name} schema mismatch")

    memory_max = _nonnegative_integer(cgroup.get("memory_max_bytes"), "memory.max")
    if memory_max == 0:
        raise GateValidationError("memory.max must be finite and positive")
    for field in (
        "memory_current_before_bytes",
        "memory_current_pressure_bytes",
        "memory_current_after_holder_release_bytes",
    ):
        if _nonnegative_integer(cgroup.get(field), field) > memory_max:
            raise GateValidationError(f"{field} exceeds memory.max")
    events_before = cgroup.get("events_before")
    events_after = cgroup.get("events_after")
    events_delta = cgroup.get("events_delta")
    if not all(isinstance(item, dict) for item in (events_before, events_after, events_delta)):
        raise GateValidationError("cgroup memory.events evidence is missing")
    assert isinstance(events_before, dict)
    assert isinstance(events_after, dict)
    assert isinstance(events_delta, dict)
    for counter in ("oom", "oom_kill"):
        before = _nonnegative_integer(events_before.get(counter), f"{counter}.before")
        after = _nonnegative_integer(events_after.get(counter), f"{counter}.after")
        delta = _nonnegative_integer(events_delta.get(counter), f"{counter}.delta")
        if after - before != delta or delta != 0:
            raise GateValidationError(f"cgroup {counter} delta must be zero")

    holder_bytes = _nonnegative_integer(holder.get("bytes"), "holder bytes")
    if holder_bytes == 0:
        raise GateValidationError("holder bytes must be positive")
    current_before = _nonnegative_integer(
        cgroup.get("memory_current_before_bytes"), "memory current before"
    )
    current_pressure = _nonnegative_integer(
        cgroup.get("memory_current_pressure_bytes"), "memory current pressure"
    )
    current_recovery = _nonnegative_integer(
        cgroup.get("memory_current_after_holder_release_bytes"),
        "memory current after holder release",
    )
    if current_pressure - current_before < holder_bytes // 2:
        raise GateValidationError("cgroup memory.current rise does not prove holder pressure")
    if current_pressure - current_recovery < holder_bytes // 2:
        raise GateValidationError("cgroup memory.current fall does not prove holder recovery")
    timestamps = [
        _nonnegative_integer(holder.get(field), f"holder {field}")
        for field in (
            "started_monotonic_ns",
            "ready_monotonic_ns",
            "released_monotonic_ns",
            "exited_monotonic_ns",
        )
    ]
    if timestamps != sorted(timestamps) or len(set(timestamps)) != len(timestamps):
        raise GateValidationError("holder lifecycle timestamps are not ordered")
    if _nonnegative_integer(holder.get("exit_status"), "holder exit status") != 0:
        raise GateValidationError("memory holder failed to exit cleanly")
    if _nonnegative_integer(setup.get("working_set_bytes"), "feeder working set") == 0:
        raise GateValidationError("pressure feeder working set must be positive")
    if (
        _nonnegative_integer(
            setup.get("cumulative_bytes_written"), "feeder cumulative bytes"
        )
        == 0
    ):
        raise GateValidationError("pressure feeder did not complete a direct write")
    observed_admitted = _nonnegative_integer(
        setup.get("observed_admitted_lower_bound_bytes"),
        "observed admission lower bound",
    )
    if observed_admitted == 0:
        raise GateValidationError("observed admission must be positive")
    _nonnegative_integer(setup.get("observed_generation"), "observed generation")
    _nonnegative_integer(setup.get("observed_monotonic_ns"), "observed monotonic time")
    if _nonnegative_integer(setup.get("feeder_pid"), "pressure feeder pid") == 0:
        raise GateValidationError("pressure feeder pid must be positive")
    feeder_start_time = setup.get("feeder_start_time")
    if (
        not isinstance(feeder_start_time, str)
        or not feeder_start_time.isdigit()
        or int(feeder_start_time) == 0
    ):
        raise GateValidationError("pressure feeder start time is invalid")
    for field in ("feeder_live_before_profile", "feeder_live_after_profile"):
        if setup.get(field) is not True:
            raise GateValidationError("pressure feeder was not live around the profile")
    if _nonnegative_integer(setup.get("exit_status"), "setup exit status") != 0:
        raise GateValidationError("pressure setup exit status is nonzero")

    setup_observed = runtime_samples.get("setup_observed")
    pressure_samples = runtime_samples.get("pressure")
    recovery_samples = runtime_samples.get("recovery")
    recovery_boundary = runtime_samples.get("recovery_boundary")
    if (
        not isinstance(setup_observed, dict)
        or not isinstance(pressure_samples, list)
        or len(pressure_samples) != 2
        or not isinstance(recovery_samples, list)
        or not recovery_samples
        or not isinstance(recovery_boundary, dict)
    ):
        raise GateValidationError("pressure and recovery runtime samples are required")
    samples = [setup_observed, *pressure_samples, recovery_boundary, *recovery_samples]
    for sample in samples:
        if not isinstance(sample, dict):
            raise GateValidationError("memory pressure runtime sample is invalid")
        if sample.get("source") != "host_and_cgroup":
            raise GateValidationError("memory pressure source must be host_and_cgroup")
        _nonnegative_integer(sample.get("generation"), "memory pressure generation")
        envelope = _nonnegative_integer(sample.get("envelope_bytes"), "memory envelope")
        managed = sum(
            _nonnegative_integer(sample.get(field), field)
            for field in (
                "memory_cache_limit_bytes",
                "dirty_limit_bytes",
                "transit_limit_bytes",
                "async_write_limit_bytes",
            )
        )
        if managed > envelope:
            raise GateValidationError("managed memory limits exceed the envelope")
        if not isinstance(sample.get("overcommitted_minimum"), bool):
            raise GateValidationError("overcommitted minimum flag is invalid")
        for field in (
            "transition_count",
            "sampling_failure_count",
            "write_admission_wait_count",
            "write_admission_wait_ns",
        ):
            _nonnegative_integer(sample.get(field), field)
    initial_pressure, post_profile = pressure_samples
    if initial_pressure.get("state") != "constrained":
        raise GateValidationError("initial pressure sample must be constrained")
    if (
        initial_pressure["envelope_bytes"] == 0
        or initial_pressure["overcommitted_minimum"] is not False
    ):
        raise GateValidationError("constrained state must retain a positive nonminimum envelope")
    if (
        _nonnegative_integer(
            initial_pressure.get("admitted_lower_bound_bytes"),
            "admitted lower bound",
        )
        <= initial_pressure["envelope_bytes"]
    ):
        raise GateValidationError("constrained admission lower bound must exceed the envelope")
    if (
        initial_pressure.get("admitted_lower_bound_bytes", 0) + (64 << 20)
        < observed_admitted
    ):
        raise GateValidationError("constrained admission fell below the observed feeder admission")
    if post_profile["envelope_bytes"] == 0:
        raise GateValidationError("post-profile memory envelope must be positive")
    recovery_state_passed = recovery_samples[-1].get("state") == "normal"
    if mode in ("direct-serial", "direct-async") and not recovery_state_passed:
        raise GateValidationError("recovery state did not return to normal")
    generations = [sample["generation"] for sample in samples]
    if generations != sorted(generations):
        raise GateValidationError("memory pressure generations decreased")
    if recovery_samples[-1]["generation"] <= pressure_samples[-1]["generation"]:
        raise GateValidationError("memory pressure generation did not advance during recovery")
    counter_fields = (
        "transition_count",
        "write_admission_wait_count",
        "write_admission_wait_ns",
        "sampling_failure_count",
    )
    for field in counter_fields:
        values = [sample[field] for sample in samples]
        if values != sorted(values):
            raise GateValidationError(f"memory pressure {field} decreased")
    sampling_failures = [sample["sampling_failure_count"] for sample in samples]
    if any(value != sampling_failures[0] for value in sampling_failures[1:]):
        raise GateValidationError("memory pressure sampling failures increased")

    for field, expected in (
        ("bytes_written", expected_written),
        ("bytes_read", expected_read),
    ):
        if verification.get(field) != expected:
            raise GateValidationError(f"pressure verification {field} mismatch")
    pressure_hash = verification.get("pressure_sha256")
    recovery_hash = verification.get("recovery_sha256")
    if not _is_sha256(pressure_hash) or pressure_hash != recovery_hash:
        raise GateValidationError("pressure/recovery hash verification failed")
    if evidence.get("drain_completed") is not True:
        raise GateValidationError("memory pressure drain did not complete")

    numeric_throughput = {}
    if throughput.get("profile") != workload:
        raise GateValidationError("pressure throughput profile mismatch")
    if throughput.get("method") != "inline-unbuffered-read-v1":
        raise GateValidationError("pressure/recovery measurer mismatch")
    if _nonnegative_integer(throughput.get("chunk_bytes"), "read chunk bytes") == 0:
        raise GateValidationError("read chunk bytes must be positive")
    if (
        _nonnegative_integer(throughput.get("bytes_read"), "measured bytes read")
        != expected_read
    ):
        raise GateValidationError("measured read byte count mismatch")
    for phase in ("pressure", "recovery"):
        values = throughput.get(phase)
        valid = isinstance(values, list) and len(values) == 1 and not any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value <= 0
            for value in (values if isinstance(values, list) else [])
        )
        if not valid:
            if phase == "recovery" and mode == "kernel" and values == []:
                numeric_throughput[phase] = []
                continue
            if not isinstance(values, list) or len(values) != 1:
                raise GateValidationError(f"{phase} throughput evidence is missing")
            raise GateValidationError(f"{phase} throughput evidence is invalid")
        assert isinstance(values, list)
        numeric_throughput[phase] = values
    for field in ("foreground_seconds", "drain_seconds"):
        value = cpu.get(field)
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value < 0
        ):
            raise GateValidationError(f"pressure {field} CPU evidence is invalid")

    throughput_recovery_passed = bool(numeric_throughput["recovery"]) and (
        numeric_throughput["recovery"][0] >= numeric_throughput["pressure"][0]
    )
    if mode in ("direct-serial", "direct-async") and not throughput_recovery_passed:
        raise GateValidationError("throughput recovery was absent after memory pressure")
    recovery_passed = recovery_state_passed and throughput_recovery_passed
    return {
        "status": "complete",
        "passed": True,
        "recovery_passed": recovery_passed,
        "recovery_state_passed": recovery_state_passed,
        "throughput_recovery_passed": throughput_recovery_passed,
        "performance_diagnostic": mode == "kernel",
    }


def validate_correctness_manifest(
    manifest: dict,
    *,
    expected_revision: str,
    expected_studiofs_sha256: str,
    expected_bench_revision: str,
    expected_bench_sha256: str,
) -> None:
    """Validate the exact mounted-correctness allowlist and its provenance."""
    if manifest.get("schema_version") != 1:
        raise GateValidationError("correctness manifest schema mismatch")
    if manifest.get("kind") != "studiofs-linux-cache-correctness":
        raise GateValidationError("correctness manifest kind mismatch")
    if manifest.get("dry_run") is not False:
        raise GateValidationError("correctness manifest dry-run evidence is invalid")
    if manifest.get("valid") is not True:
        raise GateValidationError("correctness manifest is not valid")
    for field in ("git_revision", "bench_git_revision"):
        if not isinstance(manifest.get(field), str) or not manifest[field]:
            raise GateValidationError(f"correctness manifest revision is missing: {field}")
    for label, value in (
        ("expected StudioFS", expected_studiofs_sha256),
        ("expected benchmark", expected_bench_sha256),
        ("StudioFS", manifest.get("studiofs_sha256")),
        ("benchmark", manifest.get("bench_sha256")),
    ):
        if not _is_sha256(value):
            raise GateValidationError(f"correctness manifest {label} checksum is invalid")
    if manifest.get("git_revision") != expected_revision:
        raise GateValidationError("correctness manifest revision mismatch")
    if manifest.get("git_dirty") is not False:
        raise GateValidationError("correctness manifest StudioFS source is dirty")
    if manifest.get("studiofs_sha256") != expected_studiofs_sha256:
        raise GateValidationError("correctness manifest StudioFS checksum mismatch")
    if manifest.get("bench_git_revision") != expected_bench_revision:
        raise GateValidationError("correctness manifest benchmark revision mismatch")
    if manifest.get("bench_git_dirty") is not False:
        raise GateValidationError("correctness manifest benchmark source is dirty")
    if manifest.get("bench_sha256") != expected_bench_sha256:
        raise GateValidationError("correctness manifest benchmark checksum mismatch")

    tests = manifest.get("tests")
    if not isinstance(tests, list):
        raise GateValidationError("correctness manifest tests are missing")
    names = [result.get("name") for result in tests if isinstance(result, dict)]
    required_names = set(REQUIRED_CORRECTNESS_TESTS)
    actual_names = set(names)
    missing = sorted(required_names - actual_names)
    if missing:
        raise GateValidationError(
            f"missing required correctness tests: {', '.join(missing)}"
        )
    extra = sorted(name for name in actual_names - required_names if isinstance(name, str))
    if extra or len(names) != len(REQUIRED_CORRECTNESS_TESTS):
        raise GateValidationError("correctness manifest has duplicate or extra tests")

    for result in tests:
        if not isinstance(result, dict):
            raise GateValidationError("correctness test result is invalid")
        name = result.get("name")
        expected_mode = REQUIRED_CORRECTNESS_TESTS.get(name)
        if result.get("mode") != expected_mode:
            raise GateValidationError(f"correctness mode mismatch for {name}")
        environment = result.get("environment")
        if not isinstance(environment, dict):
            raise GateValidationError(f"correctness environment is missing for {name}")
        validate_mode_environment(expected_mode, environment)
        if (
            isinstance(name, str)
            and name.endswith("mounted_kernel_remote_listener_invalidation")
            and environment.get("STUDIOFS_REQUIRE_REMOTE_INVALIDATION_TEST") != "1"
        ):
            raise GateValidationError("remote correctness test was not required")
        if result.get("executed") is not True:
            raise GateValidationError(f"correctness test was not executed: {name}")
        if result.get("skipped") is not False:
            raise GateValidationError(f"correctness test was skipped: {name}")
        if _nonnegative_integer(result.get("exit_status"), "correctness exit status") != 0:
            raise GateValidationError(f"correctness test failed: {name}")
        for field in ("command", "started_at", "ended_at", "log_path"):
            if not isinstance(result.get(field), str) or not result[field]:
                raise GateValidationError(f"correctness {field} is missing for {name}")
        if not _is_sha256(result.get("log_sha256")):
            raise GateValidationError(f"correctness log checksum is invalid for {name}")
        log_path = Path(result["log_path"])
        if not log_path.is_absolute() or not log_path.is_file() or log_path.stat().st_size == 0:
            raise GateValidationError(f"correctness log is missing or empty for {name}")
        if _file_sha256(log_path) != result["log_sha256"]:
            raise GateValidationError(f"correctness log checksum mismatch for {name}")


def validate_run_provenance(
    manifest: dict,
    correctness_manifest: dict,
    *,
    correctness_manifest_path: str,
    correctness_manifest_sha256: str,
) -> None:
    """Require one run to match its validated source, binaries, and manifest."""
    for field in ("git_revision", "bench_git_revision"):
        if not isinstance(manifest.get(field), str) or not manifest[field]:
            raise GateValidationError(f"run provenance revision is missing: {field}")
    for field in (
        "studiofs_sha256",
        "mount_executable_sha256",
        "bench_sha256",
        "correctness_manifest_sha256",
    ):
        if not _is_sha256(manifest.get(field)):
            raise GateValidationError(f"run provenance checksum is invalid: {field}")
    if not _is_sha256(correctness_manifest_sha256):
        raise GateValidationError("correctness manifest checksum is invalid")
    if manifest.get("git_dirty") is not False or manifest.get("bench_git_dirty") is not False:
        raise GateValidationError("dirty source tree makes final evidence ineligible")
    if correctness_manifest.get("git_dirty") is not False or correctness_manifest.get(
        "bench_git_dirty"
    ) is not False:
        raise GateValidationError("dirty correctness source tree makes evidence ineligible")
    for field in ("git_revision", "studiofs_sha256", "bench_git_revision", "bench_sha256"):
        if manifest.get(field) != correctness_manifest.get(field):
            raise GateValidationError(f"run provenance mismatch: {field}")
    if manifest.get("mount_executable_sha256") != correctness_manifest.get(
        "studiofs_sha256"
    ):
        raise GateValidationError("mount executable checksum mismatch")
    recorded_path = manifest.get("correctness_manifest_path")
    if (
        not isinstance(recorded_path, str)
        or not os.path.isabs(recorded_path)
        or _normalized_path(recorded_path) != _normalized_path(correctness_manifest_path)
    ):
        raise GateValidationError("correctness manifest path mismatch")
    if not _is_sha256(correctness_manifest_sha256) or manifest.get(
        "correctness_manifest_sha256"
    ) != correctness_manifest_sha256:
        raise GateValidationError("correctness manifest checksum mismatch")


def workload_expectation(
    workload: str,
    *,
    workload_bytes: int = 4_000_000_000,
    parallelism: int | None = None,
) -> dict[str, int | str]:
    """Return exact logical byte and file counts for one accepted workload."""
    if not isinstance(workload_bytes, int) or isinstance(workload_bytes, bool) or workload_bytes <= 0:
        raise GateValidationError("workload_bytes must be a positive integer")
    if workload == "large-single":
        file_count = 1
        written = read = workload_bytes
    elif workload == "fixed-1m":
        file_count = (workload_bytes + 999_999) // 1_000_000
        written = read = workload_bytes
    elif workload == "small-128k":
        workload_bytes = 1_000 * 131_072
        file_count = 1_000
        written = read = workload_bytes
    elif workload == "warm-reopen":
        file_count = 1
        written = 0
        read = 2 * workload_bytes
    elif workload == "same-file-parallel":
        if (
            not isinstance(parallelism, int)
            or isinstance(parallelism, bool)
            or not 1 <= parallelism <= 8
        ):
            raise GateValidationError("same-file parallelism must be between 1 and 8")
        workload_bytes = parallelism * 64 * 1024 * 1024
        file_count = 1
        written = read = workload_bytes
    elif workload == "cross-file-barriers":
        workload_bytes = 2 * 64 * 1024 * 1024
        file_count = 2
        written = read = workload_bytes
    else:
        raise GateValidationError(f"unknown workload: {workload}")

    expectation: dict[str, int | str] = {
        "workload": workload,
        "workload_bytes": workload_bytes,
        "file_count": file_count,
        "verified_bytes_written": written,
        "verified_bytes_read": read,
    }
    if workload == "same-file-parallel":
        expectation["parallelism"] = parallelism
    return expectation


def validate_workload_manifest(
    manifest: dict,
    *,
    workload: str,
    workload_bytes: int = 4_000_000_000,
    parallelism: int | None = None,
) -> dict[str, int | str]:
    """Require configured and verified workload totals to be exact."""
    expected = workload_expectation(
        workload,
        workload_bytes=workload_bytes,
        parallelism=parallelism,
    )
    for field, expected_value in expected.items():
        if manifest.get(field) != expected_value:
            raise GateValidationError(
                f"workload {field} mismatch: expected {expected_value}, got {manifest.get(field)}"
            )
    return expected


def validate_bench_report(
    report: dict,
    *,
    workload: str,
    mount_point: str,
    generated_json_files: Sequence[str],
    workload_bytes: int = 4_000_000_000,
) -> dict[str, int | str]:
    """Validate a single studiofs-bench JSON report for a cold workload."""
    if isinstance(generated_json_files, (str, bytes)) or len(generated_json_files) != 1:
        raise GateValidationError("exactly one generated benchmark JSON is required")
    if not generated_json_files[0].endswith(".json"):
        raise GateValidationError("generated benchmark report must be JSON")
    if workload not in ("large-single", "fixed-1m"):
        raise GateValidationError(f"studiofs-bench does not own workload: {workload}")
    expected = workload_expectation(workload, workload_bytes=workload_bytes)

    run = report.get("run")
    config = report.get("config")
    passes = report.get("passes")
    if not isinstance(run, dict) or not isinstance(config, dict):
        raise GateValidationError("benchmark run/config is missing")
    if run.get("workload_bytes") != expected["workload_bytes"]:
        raise GateValidationError("benchmark workload_bytes mismatch")
    if run.get("file_count") != expected["file_count"]:
        raise GateValidationError("benchmark file_count mismatch")
    if run.get("stopped") is not False:
        raise GateValidationError("benchmark run was stopped")
    if run.get("cleanup_error") is not None:
        raise GateValidationError("benchmark cleanup error")
    if run.get("files_kept") is not True:
        raise GateValidationError("benchmark files must be kept until measured cleanup")

    expected_layout: object = (
        "single_file" if workload == "large-single" else {"fixed_file_size_mb": 1}
    )
    expected_config = {
        "run_mode": "mounted_filesystem",
        "test_mode": "read_write",
        "file_layout": expected_layout,
        "keep_files": True,
        "execution_mode": "run_once",
    }
    for field, expected_value in expected_config.items():
        if config.get(field) != expected_value:
            raise GateValidationError(f"benchmark config {field} mismatch")
    target_path = config.get("target_path")
    if not isinstance(target_path, str) or _normalized_path(target_path) != _normalized_path(
        mount_point
    ):
        raise GateValidationError("benchmark target_path mismatch")

    if not isinstance(passes, list) or len(passes) != 2:
        raise GateValidationError("benchmark passes must contain one write and one read")
    phases = [item.get("phase") for item in passes if isinstance(item, dict)]
    if sorted(phases) != ["read", "write"]:
        raise GateValidationError("benchmark passes must contain one write and one read")
    for item in passes:
        if item.get("pass_number") != 1:
            raise GateValidationError("benchmark pass_number must be one")
        if item.get("stopped") is not False:
            raise GateValidationError("benchmark pass was stopped")
        if item.get("bytes_processed") != expected["workload_bytes"]:
            raise GateValidationError("benchmark bytes_processed mismatch")
    return expected


def validate_python_client_report(
    report: dict,
    *,
    workload: str,
    workload_bytes: int = 4_000_000_000,
    parallelism: int | None = None,
) -> dict[str, int | str]:
    """Validate the common report emitted by the four Python clients."""
    if workload not in (
        "small-128k",
        "warm-reopen",
        "same-file-parallel",
        "cross-file-barriers",
    ):
        raise GateValidationError(f"Python client does not own workload: {workload}")
    expected = workload_expectation(
        workload,
        workload_bytes=workload_bytes,
        parallelism=parallelism,
    )
    report_fields = {
        "workload": expected["workload"],
        "file_count": expected["file_count"],
        "bytes_written": expected["verified_bytes_written"],
        "bytes_read": expected["verified_bytes_read"],
    }
    if "parallelism" in expected:
        report_fields["parallelism"] = expected["parallelism"]
    for field, expected_value in report_fields.items():
        if report.get(field) != expected_value:
            raise GateValidationError(f"Python client {field} mismatch")
    if report.get("completed") is not True:
        raise GateValidationError("Python client completed flag is false")
    if report.get("cleanup_error") is not None:
        raise GateValidationError("Python client cleanup error")
    expected_checksum = report.get("expected_checksum")
    actual_checksum = report.get("actual_checksum")
    if (
        not isinstance(expected_checksum, str)
        or not expected_checksum
        or expected_checksum != actual_checksum
    ):
        raise GateValidationError("Python client checksum mismatch")
    if workload == "cross-file-barriers":
        common_start = report.get("common_start_monotonic_ns")
        if (
            not isinstance(common_start, int)
            or isinstance(common_start, bool)
            or common_start < 0
        ):
            raise GateValidationError("cross-file common start is invalid")
        completions = report.get("file_completions")
        if not isinstance(completions, list) or len(completions) != 2:
            raise GateValidationError("cross-file proof requires exactly two completions")
        files = []
        for completion in completions:
            required_fields = {"file", "completion_monotonic_ns"}
            allowed_fields = {
                *required_fields,
                "fsync_started_monotonic_ns",
            }
            if (
                not isinstance(completion, dict)
                or not required_fields <= set(completion)
                or not set(completion) <= allowed_fields
            ):
                raise GateValidationError("cross-file completion object is invalid")
            file_name = completion["file"]
            completion_ns = completion["completion_monotonic_ns"]
            if not isinstance(file_name, str) or not file_name:
                raise GateValidationError("cross-file completion file is invalid")
            if (
                not isinstance(completion_ns, int)
                or isinstance(completion_ns, bool)
                or completion_ns < common_start
            ):
                raise GateValidationError("cross-file completion precedes common start")
            fsync_started = completion.get("fsync_started_monotonic_ns")
            if fsync_started is not None and (
                not isinstance(fsync_started, int)
                or isinstance(fsync_started, bool)
                or not common_start <= fsync_started <= completion_ns
            ):
                raise GateValidationError("cross-file fsync start is invalid")
            files.append(file_name)
        if len(set(files)) != 2:
            raise GateValidationError("cross-file completions must name distinct files")
    return expected


MODES = ("direct-serial", "direct-async", "kernel")
WORKLOADS = (
    "large-single",
    "fixed-1m",
    "small-128k",
    "warm-reopen",
    "same-file-parallel",
    "cross-file-barriers",
)


def validate_histogram_applicability(
    mode: str,
    workload: str,
    kind: str,
    samples: int,
) -> None:
    """Require samples except for Kernel's successful second warm read."""
    if mode not in MODES or workload not in WORKLOADS:
        raise GateValidationError("unknown mode or workload for histogram")
    if not isinstance(samples, int) or isinstance(samples, bool) or samples < 0:
        raise GateValidationError("histogram sample count is invalid")
    zero_is_warm_reuse = (
        mode == "kernel"
        and workload == "warm-reopen"
        and kind == "second_fuse_read"
    )
    if samples == 0 and not zero_is_warm_reuse:
        raise GateValidationError(f"histogram interval has zero samples: {kind}")


def _counter_delta(before: object, after: object, label: str) -> int:
    for value in (before, after):
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise GateValidationError(f"invalid nonnegative counter: {label}")
    if after < before:
        raise GateValidationError(f"counter decreased: {label}")
    return after - before


def validate_warm_proof(
    mode: str,
    foreground_start: dict,
    first_end: dict,
    foreground_end: dict,
    derived: dict,
    *,
    checksum_verified: bool,
) -> dict:
    """Rebuild the two warm-read intervals and match their derived proof."""
    counter_paths = {
        "fuse_read_calls": ("write_path", "fuse_callbacks", "read", "calls"),
        "vfs_read_path_calls": ("write_path", "vfs_read_path", "calls"),
        "os_cache_replace": ("write_path", "os_cache", "replace"),
        "os_cache_reuse": ("write_path", "os_cache", "reuse"),
    }
    proof = {"checksum_verified": checksum_verified is True}
    for label, path in counter_paths.items():
        start_value = _value_at(foreground_start, path)
        first_value = _value_at(first_end, path)
        end_value = _value_at(foreground_end, path)
        proof[f"first_{label}"] = _counter_delta(
            start_value,
            first_value,
            f"warm first {label}",
        )
        proof[f"second_{label}"] = _counter_delta(
            first_value,
            end_value,
            f"warm second {label}",
        )
    if derived.get("warm_proof") != proof:
        raise GateValidationError("derived warm proof does not match raw warm proof")
    if mode == "kernel":
        validate_kernel_warm_proof(proof)
    return proof


def validate_kernel_warm_proof(proof: dict) -> None:
    """Require first-read work and exactly one Replace-to-Reuse transition."""
    if proof.get("checksum_verified") is not True:
        raise GateValidationError("Kernel warm checksum proof failed")
    for field in ("first_fuse_read_calls", "first_vfs_read_path_calls"):
        value = proof.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise GateValidationError("Kernel warm first read lacks callback proof")
    if proof.get("first_os_cache_replace") != 1 or proof.get(
        "first_os_cache_reuse"
    ) != 0:
        raise GateValidationError("Kernel warm first cache decision must be Replace")
    if proof.get("second_os_cache_replace") != 0 or proof.get(
        "second_os_cache_reuse"
    ) != 1:
        raise GateValidationError("Kernel warm second cache decision must be Reuse")
    for field in ("second_fuse_read_calls", "second_vfs_read_path_calls"):
        if proof.get(field) != 0:
            raise GateValidationError("Kernel warm second read must add zero callbacks")


def select_kernel_baseline(direct_serial: dict, direct_async: dict) -> str:
    """Select Direct async only after it passes against an eligible serial base."""
    if direct_serial.get("eligible") is not True or direct_serial.get("passed") is not True:
        raise GateValidationError("Direct serial baseline is ineligible")
    if direct_async.get("eligible") is True and direct_async.get("passed") is True:
        return "direct-async"
    return "direct-serial"


def _numeric_samples(rows: Sequence[dict], field: str) -> list[float]:
    if not rows:
        raise GateValidationError("at least one repetition is required")
    samples = []
    for row in rows:
        value = row.get(field)
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value < 0
        ):
            raise GateValidationError(f"invalid numeric metric: {field}")
        samples.append(float(value))
    return samples


def _primary_throughputs(rows: Sequence[dict]) -> list[float]:
    throughputs = []
    for row in rows:
        written = row.get("verified_bytes_written")
        read = row.get("verified_bytes_read")
        seconds = row.get("foreground_wall_seconds")
        if (
            not isinstance(written, int)
            or isinstance(written, bool)
            or written < 0
            or not isinstance(read, int)
            or isinstance(read, bool)
            or read < 0
            or not isinstance(seconds, (int, float))
            or isinstance(seconds, bool)
            or not math.isfinite(seconds)
            or seconds <= 0
        ):
            raise GateValidationError("invalid verified-byte throughput evidence")
        throughputs.append((written + read) / float(seconds))
    if not throughputs:
        raise GateValidationError("at least one repetition is required")
    return throughputs


def _throughput_gate(baseline: Sequence[float], candidate: Sequence[float]) -> dict:
    baseline_stats = median_mad(baseline)
    candidate_stats = median_mad(candidate)
    limit = 0.95 * baseline_stats["median"]
    return {
        "passed": candidate_stats["median"] >= limit,
        "improved": candidate_stats["median"] - baseline_stats["median"]
        > baseline_stats["mad"] + candidate_stats["mad"],
        "baseline_median": baseline_stats["median"],
        "baseline_mad": baseline_stats["mad"],
        "candidate_median": candidate_stats["median"],
        "candidate_mad": candidate_stats["mad"],
        "limit": limit,
    }


def evaluate_numeric_gates(
    baseline_rows: Sequence[dict],
    candidate_rows: Sequence[dict],
    *,
    candidate_mode: str,
    workload_bytes: int,
    compare_throughput: bool = True,
) -> dict:
    """Apply the accepted CPU, core, tail, memory, and optional throughput gates."""
    if candidate_mode not in ("direct-async", "kernel"):
        raise GateValidationError("numeric candidate must be direct-async or kernel")
    if not isinstance(workload_bytes, int) or isinstance(workload_bytes, bool) or workload_bytes <= 0:
        raise GateValidationError("workload_bytes must be positive")
    gates: dict[str, dict] = {}
    if compare_throughput:
        gates["primary_throughput_bytes_per_second"] = _throughput_gate(
            _primary_throughputs(baseline_rows),
            _primary_throughputs(candidate_rows),
        )

    gates["total_lifecycle_cpu_seconds_per_gib"] = robust_upper_gate(
        _numeric_samples(baseline_rows, "total_lifecycle_cpu_seconds_per_gib"),
        _numeric_samples(candidate_rows, "total_lifecycle_cpu_seconds_per_gib"),
        relative_floor=0.01,
    )
    for field in (
        "foreground_average_cores",
        "foreground_p95_cores",
        "foreground_peak_cores",
    ):
        gates[field] = robust_upper_gate(
            _numeric_samples(baseline_rows, field),
            _numeric_samples(candidate_rows, field),
            relative_floor=0.10,
            absolute_floor=0.25,
        )

    baseline_intervals = [row.get("callback_intervals") for row in baseline_rows]
    candidate_intervals = [row.get("callback_intervals") for row in candidate_rows]
    if not all(isinstance(value, dict) for value in [*baseline_intervals, *candidate_intervals]):
        raise GateValidationError("callback interval evidence is missing")
    baseline_kinds = set(baseline_intervals[0])
    if any(set(interval) != baseline_kinds for interval in baseline_intervals[1:]) or any(
        set(interval) != baseline_kinds for interval in candidate_intervals
    ):
        raise GateValidationError("callback interval kinds do not match")
    for kind in sorted(baseline_kinds):
        baseline_values = [interval[kind] for interval in baseline_intervals]
        candidate_values = [interval[kind] for interval in candidate_intervals]
        if not all(isinstance(value, dict) for value in [*baseline_values, *candidate_values]):
            raise GateValidationError(f"invalid callback interval: {kind}")
        sample_counts = [value.get("samples") for value in [*baseline_values, *candidate_values]]
        if not all(
            isinstance(count, int) and not isinstance(count, bool) and count >= 0
            for count in sample_counts
        ):
            raise GateValidationError(f"callback interval sample count is invalid: {kind}")
        kernel_reuse = (
            candidate_mode == "kernel"
            and kind == "second_fuse_read"
            and all(value["samples"] == 0 for value in candidate_values)
            and all(
                value.get(field) is None
                for value in candidate_values
                for field in (
                    "p95_upper_bound_ns",
                    "p99_upper_bound_ns",
                    "max_upper_bound_ns",
                )
            )
        )
        if kernel_reuse:
            if not all(value["samples"] > 0 for value in baseline_values):
                raise GateValidationError(
                    "Direct baseline second_fuse_read has zero samples"
                )
            continue
        if any(count == 0 for count in sample_counts):
            raise GateValidationError(f"callback interval has zero samples: {kind}")
        for field, relative_floor in (
            ("p95_upper_bound_ns", 0.10),
            ("p99_upper_bound_ns", 0.10),
            ("max_upper_bound_ns", 0.25),
        ):
            gates[f"{kind}.{field}"] = robust_upper_gate(
                _numeric_samples(baseline_values, field),
                _numeric_samples(candidate_values, field),
                relative_floor=relative_floor,
            )

    for field in ("daemon_rss_peak_bytes", "daemon_anon_peak_bytes"):
        gates[field] = robust_upper_gate(
            _numeric_samples(baseline_rows, field),
            _numeric_samples(candidate_rows, field),
            relative_floor=0.10,
        )

    baseline_file = _numeric_samples(baseline_rows, "cgroup_file_peak_bytes")
    candidate_file = _numeric_samples(candidate_rows, "cgroup_file_peak_bytes")
    if candidate_mode == "direct-async":
        gates["cgroup_file_peak_bytes"] = robust_upper_gate(
            baseline_file,
            candidate_file,
            relative_floor=0.10,
        )
    else:
        baseline_stats = median_mad(baseline_file)
        candidate_stats = median_mad(candidate_file)
        allowance = max(
            2 * (baseline_stats["mad"] + candidate_stats["mad"]),
            0.10 * workload_bytes,
        )
        limit = baseline_stats["median"] + workload_bytes + allowance
        gates["cgroup_file_peak_bytes"] = {
            "passed": candidate_stats["median"] <= limit,
            "improved": baseline_stats["median"] - candidate_stats["median"]
            > baseline_stats["mad"] + candidate_stats["mad"],
            "baseline_median": baseline_stats["median"],
            "baseline_mad": baseline_stats["mad"],
            "candidate_median": candidate_stats["median"],
            "candidate_mad": candidate_stats["mad"],
            "working_set_allowance": workload_bytes,
            "noise_allowance": allowance,
            "limit": limit,
        }
    return {
        "passed": all(gate["passed"] for gate in gates.values()),
        "gates": gates,
    }


def repetition_eligibility(repetitions: int, diagnostic: bool) -> dict[str, object]:
    """Classify the only two accepted matrix repetition counts."""
    if repetitions == 5 and diagnostic is False:
        return {"repetitions": 5, "diagnostic": False, "promotion_eligible": True}
    if repetitions == 3 and diagnostic is True:
        return {
            "repetitions": 3,
            "diagnostic": True,
            "promotion_eligible": False,
        }
    raise GateValidationError(
        "promotion requires five repetitions; diagnostic requires exactly three"
    )


def validate_schedule(
    schedule: dict,
    *,
    discovered_tuples: Sequence[tuple[str, str, int]] | None = None,
) -> dict[str, object]:
    """Validate the exact mode/workload matrix and fresh per-tuple resources."""
    if schedule.get("schema_version") != 1:
        raise GateValidationError("schedule schema mismatch")
    if schedule.get("kind") != "studiofs-linux-cache-matrix-schedule":
        raise GateValidationError("schedule kind mismatch")
    if schedule.get("status") != "completed":
        raise GateValidationError("schedule root is not completed")
    if schedule.get("dry_run") is not False:
        raise GateValidationError("dry-run schedule is not real evidence")
    eligibility = repetition_eligibility(
        schedule.get("repetitions"),
        schedule.get("diagnostic"),
    )
    if schedule.get("promotion_eligible") is not eligibility["promotion_eligible"]:
        raise GateValidationError("schedule promotion eligibility mismatch")
    if (
        schedule.get("mode_rotation") != "three-row Latin square"
        or schedule.get("workload_rotation") != "cyclic by repetition"
    ):
        raise GateValidationError("schedule rotation metadata mismatch")
    repetitions = eligibility["repetitions"]
    expected = {
        (mode, workload, repetition)
        for repetition in range(1, repetitions + 1)
        for mode in MODES
        for workload in WORKLOADS
    }
    tuples = schedule.get("tuples")
    if not isinstance(tuples, list):
        raise GateValidationError("schedule tuples are missing")
    keys = []
    for item in tuples:
        if not isinstance(item, dict):
            raise GateValidationError("schedule tuple is invalid")
        if item.get("status") != "passed":
            raise GateValidationError("schedule tuple status is not passed")
        if (
            _nonnegative_integer(
                item.get("collector_exit_status"), "collector exit status"
            )
            != 0
        ):
            raise GateValidationError("schedule tuple collector failed")
        if item.get("cleanup_error") is not None:
            raise GateValidationError("schedule tuple cleanup failed")
        keys.append((item.get("mode"), item.get("workload"), item.get("repetition")))
    duplicates = sorted({key for key in keys if keys.count(key) > 1})
    if duplicates:
        raise GateValidationError("schedule has duplicate tuples")
    actual = set(keys)
    missing = expected - actual
    if missing:
        raise GateValidationError("schedule has missing tuples")
    extra = actual - expected
    if extra:
        raise GateValidationError("schedule has extra tuples")
    if schedule.get("expected_tuple_count") != len(expected) or len(tuples) != len(expected):
        raise GateValidationError("schedule tuple count mismatch")

    expected_order = []
    for repetition in range(1, repetitions + 1):
        workload_shift = (repetition - 1) % len(WORKLOADS)
        mode_shift = (repetition - 1) % len(MODES)
        for workload_position in range(len(WORKLOADS)):
            workload = WORKLOADS[(workload_position + workload_shift) % len(WORKLOADS)]
            for mode_position in range(len(MODES)):
                mode = MODES[(mode_position + mode_shift) % len(MODES)]
                expected_order.append((mode, workload, repetition))
    if keys != expected_order:
        raise GateValidationError("schedule rotation order mismatch")

    indexes = [item.get("schedule_index") for item in tuples]
    if not all(
        isinstance(index, int) and not isinstance(index, bool) for index in indexes
    ) or indexes != list(range(len(tuples))):
        raise GateValidationError("schedule indexes must be contiguous from zero")
    for field, label in (
        ("cgroup", "cgroup"),
        ("mount_point", "mount"),
        ("cache_dir", "cache directory"),
        ("artifact_dir", "artifact directory"),
    ):
        values = [item.get(field) for item in tuples]
        if not all(isinstance(value, str) and value for value in values):
            raise GateValidationError(f"schedule {label} is missing")
        if len(set(values)) != len(values):
            raise GateValidationError(f"schedule reuses a {label}")
    for item in tuples:
        suffix = os.path.join(
            item["mode"],
            item["workload"],
            f"rep-{item['repetition']:03d}",
        )
        if not _normalized_path(item["artifact_dir"]).endswith(os.sep + suffix):
            raise GateValidationError("schedule artifact directory does not match tuple")
        for command_field in ("mount_command", "collector_command"):
            if not isinstance(item.get(command_field), str) or not item[command_field]:
                raise GateValidationError(f"schedule {command_field} is invalid")

    if discovered_tuples is not None:
        discovered = list(discovered_tuples)
        if len(discovered) != len(set(discovered)) or set(discovered) != expected:
            raise GateValidationError("schedule does not match filesystem artifacts")
    return eligibility


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _read_json(path: Path) -> dict:
    if path.is_symlink():
        raise GateValidationError(f"JSON artifact must not be a symlink: {path}")
    try:
        payload = json.loads(
            path.read_text(),
            parse_constant=_reject_json_constant,
        )
    except (OSError, ValueError) as error:
        raise GateValidationError(f"cannot read JSON artifact {path}: {error}") from error
    if not isinstance(payload, dict):
        raise GateValidationError(f"JSON artifact must contain an object: {path}")
    return payload


def _read_jsonl(path: Path) -> list[dict]:
    if path.is_symlink():
        raise GateValidationError(f"sampled evidence must not be a symlink: {path}")
    try:
        rows = [
            json.loads(line, parse_constant=_reject_json_constant)
            for line in path.read_text().splitlines()
            if line.strip()
        ]
    except (OSError, ValueError) as error:
        raise GateValidationError(f"invalid sampled evidence {path}: {error}") from error
    if not rows or not all(isinstance(row, dict) for row in rows):
        raise GateValidationError(f"sampled evidence is empty or invalid: {path}")
    return rows


def validate_pressure_artifact_contract(artifacts: dict[str, dict]) -> None:
    """Validate raw pressure schemas, phases, and causal timestamp order."""
    for name, payload in artifacts.items():
        expected_schema = (
            EXPECTED_RUNTIME_SCHEMA
            if name in ("runtime_before_profile", "runtime_after_profile")
            else 1
        )
        _require_schema_version(
            payload, expected_schema, f"pressure artifact: {name}"
        )
    for name, phase in (
        ("before", "before"),
        ("pressure", "pressure"),
        ("after_holder_release", "after-holder-release"),
        ("after", "after"),
        ("drain", "drain-end"),
    ):
        if artifacts[name].get("phase") != phase:
            raise GateValidationError(f"pressure artifact phase mismatch: {name}")

    holder = artifacts["holder"]
    setup = artifacts["setup"]
    profile = artifacts["profile"]
    pressure_measure = artifacts["pressure_measure"]
    pressure_hash = artifacts["pressure_hash"]
    recovery_measure = artifacts["recovery_measure"]
    recovery_hash = artifacts["recovery_hash"]
    cleanup = artifacts["cleanup"]
    for name, payload in (
        ("holder", holder),
        ("setup", setup),
        ("profile", profile),
        ("pressure measure", pressure_measure),
        ("pressure hash", pressure_hash),
        ("recovery measure", recovery_measure),
        ("recovery hash", recovery_hash),
    ):
        if _nonnegative_integer(payload.get("exit_status"), f"{name} exit status") != 0:
            raise GateValidationError(f"{name} exit status is nonzero")
    profile_name = profile.get("profile")
    expected_runner = {"large-single": "bench", "small-128k": "inline"}.get(
        profile_name
    )
    if expected_runner is None or profile.get("runner") != expected_runner:
        raise GateValidationError("pressure workload profile is invalid")
    for field in ("workload_pid", "feeder_pid"):
        if _nonnegative_integer(profile.get(field), f"profile {field}") == 0:
            raise GateValidationError(f"profile {field} must be positive")
    for field in ("workload_start_time", "feeder_start_time"):
        value = profile.get(field)
        if not isinstance(value, str) or not value.isdigit() or int(value) == 0:
            raise GateValidationError(f"profile {field} is invalid")
    for field in ("feeder_live_before_profile", "feeder_live_after_profile"):
        if profile.get(field) is not True:
            raise GateValidationError("pressure feeder was not live around workload profile")
    if (
        profile.get("feeder_pid") != setup.get("feeder_pid")
        or profile.get("feeder_start_time") != setup.get("feeder_start_time")
    ):
        raise GateValidationError("pressure profile feeder identity mismatch")
    if not _is_sha256(profile.get("source_report_sha256")):
        raise GateValidationError("pressure profile source report hash is invalid")
    diagnostic_throughput = profile.get("read_throughput_mb_per_second")
    if (
        not isinstance(diagnostic_throughput, (int, float))
        or isinstance(diagnostic_throughput, bool)
        or not math.isfinite(diagnostic_throughput)
        or diagnostic_throughput <= 0
    ):
        raise GateValidationError("pressure profile diagnostic throughput is invalid")
    pressure_measurer = (
        pressure_measure.get("method"),
        _nonnegative_integer(pressure_measure.get("chunk_bytes"), "pressure chunk bytes"),
        _nonnegative_integer(pressure_measure.get("bytes_read"), "pressure bytes read"),
    )
    recovery_measurer = (
        recovery_measure.get("method"),
        _nonnegative_integer(recovery_measure.get("chunk_bytes"), "recovery chunk bytes"),
        _nonnegative_integer(recovery_measure.get("bytes_read"), "recovery bytes read"),
    )
    if (
        pressure_measurer != recovery_measurer
        or pressure_measurer[0] != "inline-unbuffered-read-v1"
        or pressure_measurer[1] == 0
        or pressure_measurer[2] == 0
    ):
        raise GateValidationError("pressure/recovery measurer mismatch")
    for name, measurement in (
        ("pressure", pressure_measure),
        ("recovery", recovery_measure),
    ):
        value = measurement.get("throughput_mb_per_second")
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value <= 0
        ):
            raise GateValidationError(f"{name} throughput evidence is invalid")
    timeline = (
        ("before", artifacts["before"].get("monotonic_ns")),
        ("setup started", setup.get("started_monotonic_ns")),
        ("setup observed", setup.get("observed_monotonic_ns")),
        ("holder started", holder.get("started_monotonic_ns")),
        ("holder ready", holder.get("ready_monotonic_ns")),
        (
            "pressure before profile",
            artifacts["runtime_before_profile"].get("captured_monotonic_ns"),
        ),
        ("pressure", artifacts["pressure"].get("monotonic_ns")),
        ("profile started", profile.get("started_monotonic_ns")),
        ("profile completed", profile.get("completed_monotonic_ns")),
        ("pressure measure started", pressure_measure.get("started_monotonic_ns")),
        (
            "pressure measure completed",
            pressure_measure.get("completed_monotonic_ns"),
        ),
        ("pressure hash completed", pressure_hash.get("completed_monotonic_ns")),
        (
            "pressure after profile",
            artifacts["runtime_after_profile"].get("captured_monotonic_ns"),
        ),
        ("setup stopped", setup.get("stopped_monotonic_ns")),
        ("holder released", holder.get("released_monotonic_ns")),
        ("holder exited", holder.get("exited_monotonic_ns")),
        (
            "after holder release",
            artifacts["after_holder_release"].get("monotonic_ns"),
        ),
        ("recovery measure started", recovery_measure.get("started_monotonic_ns")),
        (
            "recovery measure completed",
            recovery_measure.get("completed_monotonic_ns"),
        ),
        ("recovery hash completed", recovery_hash.get("completed_monotonic_ns")),
        ("drain", artifacts["drain"].get("monotonic_ns")),
        ("after", artifacts["after"].get("monotonic_ns")),
        ("cleanup", cleanup.get("cleanup_ended_monotonic_ns")),
    )
    values = [_nonnegative_integer(value, label) for label, value in timeline]
    if any(left >= right for left, right in zip(values, values[1:])):
        raise GateValidationError("pressure artifact timestamps are out of order")
    if cleanup.get("unmount_exit_status") is not None:
        unmount_started = _nonnegative_integer(
            cleanup.get("unmount_started_monotonic_ns"), "unmount started"
        )
        unmount_ended = _nonnegative_integer(
            cleanup.get("unmount_ended_monotonic_ns"), "unmount ended"
        )
        if values[-1] >= unmount_started or unmount_started >= unmount_ended:
            raise GateValidationError("pressure artifact unmount timestamps are out of order")


def memory_pressure_evidence_from_artifacts(
    run_dir: Path,
    *,
    verified_written: int,
    verified_read: int,
    foreground_cpu: float,
    drain_cpu: float,
) -> dict:
    """Build pressure evidence from retained raw artifacts and measured totals."""
    config = _read_json(run_dir / "memory-pressure-config.json")
    before = _read_json(run_dir / "memory-events-before.json")
    pressure = _read_json(run_dir / "memory-events-pressure.json")
    after_holder_release = _read_json(
        run_dir / "memory-events-after-holder-release.json"
    )
    after = _read_json(run_dir / "memory-events-after.json")
    holder = _read_json(run_dir / "memory-holder.json")
    setup = _read_json(run_dir / "pressure-setup.json")
    profile = _read_json(run_dir / "pressure-profile.json")
    pressure_measure = _read_json(run_dir / "pressure-measure.json")
    pressure_hash = _read_json(run_dir / "pressure-hash.json")
    recovery_measure = _read_json(run_dir / "recovery-measure.json")
    recovery_hash = _read_json(run_dir / "recovery-hash.json")
    runtime_before_profile = _read_json(
        run_dir / "runtime-pressure-before-profile.json"
    )
    runtime_after_profile = _read_json(
        run_dir / "runtime-pressure-after-profile.json"
    )
    drain = _read_json(run_dir / "cgroup-drain-end.json")
    cleanup = _read_json(run_dir / "cleanup.json")
    if _nonnegative_integer(config.get("schema_version"), "config schema") != 1:
        raise GateValidationError("pressure artifact schema mismatch: config")
    negotiated = negotiated_max_write_bytes(
        _read_json(run_dir / "runtime-foreground-start.json")
    )
    if config.get("negotiated_max_write_bytes") != negotiated:
        raise GateValidationError("negotiated max_write raw evidence mismatch")
    if config.get("negotiated_max_write_source") != NEGOTIATED_MAX_WRITE_SOURCE:
        raise GateValidationError("negotiated max_write raw source mismatch")
    target = derive_pressure_target(
        effective_limit=config.get("memory_max_bytes"),
        negotiated_max_write=negotiated,
    )
    if any(config.get(name) != value for name, value in target.items()):
        raise GateValidationError("pressure target derivation raw evidence mismatch")
    observed_sample = memory_pressure_sample(
        _read_json(run_dir / "runtime-setup-observed.json")
    )
    if (
        setup.get("observed_admitted_lower_bound_bytes")
        != observed_sample["admitted_lower_bound_bytes"]
        or setup.get("observed_generation") != observed_sample["generation"]
    ):
        raise GateValidationError("observed feeder admission raw evidence mismatch")
    validate_pressure_artifact_contract(
        {
            "before": before,
            "pressure": pressure,
            "after_holder_release": after_holder_release,
            "after": after,
            "holder": holder,
            "setup": setup,
            "profile": profile,
            "pressure_measure": pressure_measure,
            "pressure_hash": pressure_hash,
            "recovery_measure": recovery_measure,
            "recovery_hash": recovery_hash,
            "runtime_before_profile": runtime_before_profile,
            "runtime_after_profile": runtime_after_profile,
            "drain": drain,
            "cleanup": cleanup,
        }
    )
    if profile.get("source_report_sha256") != _file_sha256(
        run_dir / "client-report.json"
    ):
        raise GateValidationError("pressure profile source report mismatch")
    if any(
        artifact.get("bytes_read") != verified_read
        for artifact in (
            pressure_measure,
            pressure_hash,
            recovery_measure,
            recovery_hash,
        )
    ):
        raise GateValidationError("pressure verification byte count mismatch")
    if (
        config.get("holder_bytes") != holder.get("bytes")
        or config.get("observed_admitted_lower_bound_bytes")
        != setup.get("observed_admitted_lower_bound_bytes")
        or any(
            boundary.get("memory_max_bytes") != config.get("memory_max_bytes")
            for boundary in (before, pressure, after_holder_release, after)
        )
    ):
        raise GateValidationError("pressure cgroup configuration evidence mismatch")
    return {
        "status": "complete",
        "cgroup": {
            "memory_max_bytes": config.get("memory_max_bytes"),
            "memory_current_before_bytes": before.get("memory_current_bytes"),
            "memory_current_pressure_bytes": pressure.get("memory_current_bytes"),
            "memory_current_after_holder_release_bytes": after_holder_release.get(
                "memory_current_bytes"
            ),
            "events_before": before.get("events"),
            "events_after": after.get("events"),
            "events_delta": {
                name: _counter_delta(
                    before.get("events", {}).get(name),
                    after.get("events", {}).get(name),
                    name,
                )
                for name in sorted(set(before.get("events", {})) & set(after.get("events", {})))
            },
        },
        "holder": holder,
        "setup": setup,
        "runtime_samples": {
            "setup_observed": observed_sample,
            "pressure": [
                memory_pressure_sample(runtime_before_profile),
                memory_pressure_sample(runtime_after_profile),
            ],
            "recovery_boundary": memory_pressure_sample(
                _read_json(run_dir / "runtime-recovery-boundary.json")
            ),
            "recovery": [
                memory_pressure_sample(_read_json(run_dir / "runtime-recovery-1.json")),
                memory_pressure_sample(_read_json(run_dir / "runtime-recovery-2.json")),
            ],
        },
        "verification": {
            "bytes_written": verified_written,
            "bytes_read": verified_read,
            "pressure_sha256": pressure_hash.get("sha256"),
            "recovery_sha256": recovery_hash.get("sha256"),
        },
        "throughput_mb_per_second": {
            "profile": profile.get("profile"),
            "method": pressure_measure.get("method"),
            "chunk_bytes": pressure_measure.get("chunk_bytes"),
            "bytes_read": pressure_measure.get("bytes_read"),
            # The workload profile proves behavior; recovery compares this identical measurer.
            "pressure": [pressure_measure.get("throughput_mb_per_second")],
            "recovery": [recovery_measure.get("throughput_mb_per_second")],
        },
        "cpu": {
            "foreground_seconds": foreground_cpu,
            "drain_seconds": drain_cpu,
        },
        "drain_completed": True,
    }


def validate_memory_pressure_artifacts(run_dir: Path, evidence: dict) -> None:
    """Require derived pressure evidence to match its retained raw artifacts."""
    verification = evidence.get("verification", {})
    cpu = evidence.get("cpu", {})
    raw = memory_pressure_evidence_from_artifacts(
        run_dir,
        verified_written=verification.get("bytes_written"),
        verified_read=verification.get("bytes_read"),
        foreground_cpu=cpu.get("foreground_seconds"),
        drain_cpu=cpu.get("drain_seconds"),
    )
    if raw != evidence:
        raise GateValidationError("derived pressure evidence does not match raw memory pressure artifacts")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise GateValidationError(f"cannot checksum artifact {path}: {error}") from error
    return digest.hexdigest()


def validate_artifact_layout(run_dir: Path, *, workload: str) -> None:
    """Require the complete stable per-repetition evidence layout."""
    if run_dir.is_symlink() or not run_dir.is_dir():
        raise GateValidationError(f"artifact directory is invalid: {run_dir}")
    required_files = (
        "environment.json",
        "manifest.json",
        "command.txt",
        "runtime-foreground-start.json",
        "runtime-foreground-end.json",
        "runtime-drain-end.json",
        "runtime-cleanup-end.json",
        "cgroup-foreground-start.json",
        "cgroup-foreground-end.json",
        "cgroup-drain-end.json",
        "cgroup-memory-samples.jsonl",
        "process-memory-samples.jsonl",
        "pidstat.txt",
        "client-report.json",
        "cleanup.json",
        "derived.json",
    )
    if workload == "warm-reopen":
        required_files = (*required_files, "runtime-warm-first-end.json")
    if workload in ("large-single", "small-128k"):
        required_files = (
            *required_files,
            "memory-pressure-config.json",
            "memory-events-before.json",
            "memory-events-pressure.json",
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
        )
    for name in required_files:
        path = run_dir / name
        if path.is_symlink() or not path.is_file() or path.stat().st_size == 0:
            raise GateValidationError(f"required artifact is missing or empty: {path}")
    for directory in (
        "proc-foreground-start",
        "proc-foreground-end",
        "proc-drain-end",
    ):
        path = run_dir / directory
        if path.is_symlink() or not path.is_dir():
            raise GateValidationError(f"required artifact directory is missing: {path}")
        for name in ("stat.txt", "status.txt", "smaps_rollup.txt", "io.txt"):
            evidence = path / name
            if (
                evidence.is_symlink()
                or not evidence.is_file()
                or evidence.stat().st_size == 0
            ):
                raise GateValidationError(
                    f"required artifact is missing or empty: {evidence}"
                )
    for name in (
        "runtime-foreground-start.json",
        "runtime-foreground-end.json",
        "runtime-drain-end.json",
        "runtime-cleanup-end.json",
        "cgroup-foreground-start.json",
        "cgroup-foreground-end.json",
        "cgroup-drain-end.json",
    ):
        _read_json(run_dir / name)
    for name in ("cgroup-memory-samples.jsonl", "process-memory-samples.jsonl"):
        _read_jsonl(run_dir / name)


def _nonnegative_integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise GateValidationError(f"invalid nonnegative integer: {label}")
    return value


def _require_schema_version(payload: dict, expected: int, label: str) -> None:
    actual = _nonnegative_integer(payload.get("schema_version"), f"{label} schema")
    if actual != expected:
        raise GateValidationError(f"{label} schema mismatch")


def _proc_cpu(path: Path, expected_pid: int) -> tuple[int, int, str]:
    try:
        value = path.read_text()
    except OSError as error:
        raise GateValidationError(f"cannot read process stat {path}: {error}") from error
    opening = value.find("(")
    closing = value.rfind(")")
    if opening <= 0 or closing <= opening:
        raise GateValidationError(f"malformed process stat: {path}")
    try:
        pid = int(value[:opening].strip())
        fields = value[closing + 1 :].split()
        user_ticks = int(fields[11])
        system_ticks = int(fields[12])
        start_time = fields[19]
    except (ValueError, IndexError) as error:
        raise GateValidationError(f"malformed process stat: {path}") from error
    if pid != expected_pid:
        raise GateValidationError(f"process stat pid mismatch: {path}")
    _nonnegative_integer(user_ticks, "process user CPU ticks")
    _nonnegative_integer(system_ticks, "process system CPU ticks")
    if not start_time.isdigit():
        raise GateValidationError(f"process start time is invalid: {path}")
    return user_ticks, system_ticks, start_time


def _pidstat_core_samples(
    path: Path,
    *,
    foreground_line_count: int,
    mount_pid: int,
) -> tuple[list[float], list[float]]:
    if foreground_line_count <= 0:
        raise GateValidationError("pidstat foreground line count is invalid")
    try:
        lines = path.read_text().splitlines()
    except OSError as error:
        raise GateValidationError(f"cannot read pidstat evidence {path}: {error}") from error
    header: list[str] | None = None
    foreground: list[float] = []
    drain: list[float] = []
    for line_number, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            columns = stripped.lstrip("# ").split()
            header = columns if "PID" in columns and "%CPU" in columns else None
            continue
        if not stripped or header is None:
            continue
        values = stripped.split()
        pid_index = header.index("PID")
        cpu_index = header.index("%CPU")
        if len(values) <= max(pid_index, cpu_index) or values[pid_index] != str(
            mount_pid
        ):
            continue
        try:
            cores = float(values[cpu_index]) / 100.0
        except ValueError as error:
            raise GateValidationError("pidstat %CPU is locale-dependent or invalid") from error
        if not math.isfinite(cores) or cores < 0:
            raise GateValidationError("pidstat %CPU is invalid")
        target = foreground if line_number <= foreground_line_count else drain
        target.append(cores)
    if not foreground:
        raise GateValidationError("pidstat has no foreground samples")
    if not drain:
        raise GateValidationError("pidstat has no drain samples")
    return foreground, drain


def _core_stats(samples: Sequence[float]) -> tuple[float, float, float]:
    ordered = sorted(samples)
    p95 = ordered[math.ceil(0.95 * len(ordered)) - 1]
    return sum(samples) / len(samples), p95, ordered[-1]


def _memory_peaks(
    run_dir: Path,
    *,
    foreground_start_ns: int,
    foreground_end_ns: int,
    drain_end_ns: int,
) -> dict[str, int]:
    process_rows = _read_jsonl(run_dir / "process-memory-samples.jsonl")
    cgroup_rows = _read_jsonl(run_dir / "cgroup-memory-samples.jsonl")
    process_fields = ("monotonic_ns", "vm_rss_bytes", "rss_anon_bytes")
    cgroup_fields = (
        "monotonic_ns",
        "memory_current_bytes",
        "memory_peak_bytes",
        "anon_bytes",
        "file_bytes",
    )
    for label, rows, fields in (
        ("process", process_rows, process_fields),
        ("cgroup", cgroup_rows, cgroup_fields),
    ):
        previous_timestamp = -1
        for row in rows:
            for field in fields:
                _nonnegative_integer(row.get(field), f"{label} sample {field}")
            timestamp = row["monotonic_ns"]
            if timestamp <= previous_timestamp:
                raise GateValidationError(f"{label} sample timestamps are not increasing")
            previous_timestamp = timestamp
    process_timestamps = [row["monotonic_ns"] for row in process_rows]
    cgroup_timestamps = [row["monotonic_ns"] for row in cgroup_rows]
    if process_timestamps != cgroup_timestamps:
        raise GateValidationError("process and cgroup memory sample timestamps differ")
    if not any(
        foreground_start_ns <= timestamp <= foreground_end_ns
        for timestamp in process_timestamps
    ):
        raise GateValidationError("memory evidence has no foreground sample")
    if not any(
        foreground_end_ns < timestamp <= drain_end_ns
        for timestamp in process_timestamps
    ):
        raise GateValidationError("memory evidence has no drain sample")
    for row in cgroup_rows:
        if row["memory_peak_bytes"] < row["memory_current_bytes"]:
            raise GateValidationError("cgroup sample memory peak is below current")

    boundaries = []
    for name, phase in (
        ("cgroup-foreground-start.json", "foreground-start"),
        ("cgroup-foreground-end.json", "foreground-end"),
        ("cgroup-drain-end.json", "drain-end"),
    ):
        boundary = _read_json(run_dir / name)
        if boundary.get("schema_version") != 1 or boundary.get("phase") != phase:
            raise GateValidationError(f"cgroup boundary schema/phase mismatch: {phase}")
        for field in ("monotonic_ns", "memory_current", "memory_peak"):
            _nonnegative_integer(boundary.get(field), f"{phase} {field}")
        memory_stat = boundary.get("memory_stat")
        if not isinstance(memory_stat, dict):
            raise GateValidationError(f"cgroup memory.stat is missing: {phase}")
        for field in ("anon", "file"):
            _nonnegative_integer(memory_stat.get(field), f"{phase} memory.stat {field}")
        if boundary["memory_peak"] < boundary["memory_current"]:
            raise GateValidationError(f"cgroup boundary peak is below current: {phase}")
        boundaries.append(boundary)
    boundary_timestamps = [boundary["monotonic_ns"] for boundary in boundaries]
    if boundary_timestamps != sorted(boundary_timestamps) or len(
        set(boundary_timestamps)
    ) != len(boundary_timestamps):
        raise GateValidationError("cgroup boundary timestamps are not increasing")

    return {
        "daemon_rss_peak_bytes": max(row["vm_rss_bytes"] for row in process_rows),
        "daemon_anon_peak_bytes": max(row["rss_anon_bytes"] for row in process_rows),
        "cgroup_total_peak_bytes": max(
            [row["memory_peak_bytes"] for row in cgroup_rows]
            + [boundary["memory_peak"] for boundary in boundaries]
        ),
        "cgroup_file_peak_bytes": max(
            [row["file_bytes"] for row in cgroup_rows]
            + [boundary["memory_stat"]["file"] for boundary in boundaries]
        ),
        "cgroup_anon_peak_bytes": max(
            [row["anon_bytes"] for row in cgroup_rows]
            + [boundary["memory_stat"]["anon"] for boundary in boundaries]
        ),
    }


def _match_raw_derived(derived: dict, expected: dict[str, object]) -> None:
    for field, expected_value in expected.items():
        actual = derived.get(field)
        if isinstance(expected_value, bool):
            valid = actual is expected_value
        elif isinstance(expected_value, float):
            valid = (
                isinstance(actual, (int, float))
                and not isinstance(actual, bool)
                and math.isfinite(actual)
                and actual == expected_value
            )
        elif isinstance(expected_value, int):
            valid = isinstance(actual, int) and not isinstance(actual, bool) and actual == expected_value
        else:
            valid = actual == expected_value
        if not valid:
            raise GateValidationError(
                f"derived raw measurement mismatch: {field}"
            )


def validate_raw_measurements(
    run_dir: Path,
    *,
    mode: str,
    environment: dict,
    manifest: dict,
    derived: dict,
) -> None:
    """Recompute every gate-critical derived value from stable raw artifacts."""
    runtime_start = _read_json(run_dir / "runtime-foreground-start.json")
    runtime_end = _read_json(run_dir / "runtime-foreground-end.json")
    runtime_drain = _read_json(run_dir / "runtime-drain-end.json")
    runtime_cleanup = _read_json(run_dir / "runtime-cleanup-end.json")
    runtimes = (runtime_start, runtime_end, runtime_drain, runtime_cleanup)
    midpoint_path = run_dir / "runtime-warm-first-end.json"
    if midpoint_path.exists():
        runtimes += (_read_json(midpoint_path),)
    for snapshot in runtimes:
        _require_schema_version(snapshot, EXPECTED_RUNTIME_SCHEMA, "runtime snapshot")
    if not snapshot_is_idle(runtime_start):
        raise GateValidationError("foreground-start runtime snapshot is not idle")
    if not snapshot_is_idle(runtime_drain):
        raise GateValidationError("drain-end runtime snapshot is not idle")
    if not snapshot_is_idle(runtime_cleanup):
        raise GateValidationError("cleanup-end runtime snapshot is not idle")

    geometry_path = ("write_path", "fuse_connection")
    start_connection = _value_at(runtime_start, geometry_path)
    if not isinstance(start_connection, dict):
        raise GateValidationError("raw negotiated geometry is missing")
    geometry_keys = (
        "requested_async_dio",
        "negotiated_async_dio",
        "requested_writeback_cache",
        "negotiated_writeback_cache",
    )
    geometry = {key: start_connection.get(key) for key in geometry_keys}
    validate_negotiated_geometry(mode, geometry)
    for snapshot in runtimes[1:]:
        connection = _value_at(snapshot, geometry_path)
        if not isinstance(connection, dict):
            raise GateValidationError("raw negotiated geometry is missing")
        candidate = {key: connection.get(key) for key in geometry_keys}
        validate_negotiated_geometry(mode, candidate)
        if candidate != geometry:
            raise GateValidationError("negotiated geometry changed during run")

    def lifecycle_counter(path: tuple[str, ...], label: str) -> int:
        start_value = _value_at(runtime_start, path)
        end_value = _value_at(runtime_end, path)
        drain_value = _value_at(runtime_drain, path)
        _counter_delta(start_value, end_value, f"{label} foreground")
        _counter_delta(end_value, drain_value, f"{label} drain")
        return _counter_delta(start_value, drain_value, label)

    callback_errors = 0
    for kind in ("read", "write", "flush", "fsync", "release"):
        path = ("write_path", "fuse_callbacks", kind, "errors")
        callback_errors += lifecycle_counter(path, f"fuse_{kind}.errors")
    async_errors = lifecycle_counter(
        ("write_path", "fuse_async_write", "failed_writes"),
        "failed_writes",
    ) + lifecycle_counter(
        ("write_path", "fuse_async_read", "failed_reads"),
        "failed_reads",
    )
    task_errors = lifecycle_counter(
        ("write_path", "fuse_async_write", "task_failed_writes"),
        "task_failed_writes",
    )
    cancellations = lifecycle_counter(
        ("write_path", "fuse_async_write", "cancelled_writes"),
        "cancelled_writes",
    ) + lifecycle_counter(
        ("write_path", "fuse_async_read", "canceled_reads"),
        "canceled_reads",
    )

    measurement = environment.get("measurement")
    if not isinstance(measurement, dict):
        raise GateValidationError("environment measurement is missing")
    timestamp_fields = (
        "foreground_start_monotonic_ns",
        "foreground_end_monotonic_ns",
        "drain_end_monotonic_ns",
    )
    timestamps = [
        _nonnegative_integer(measurement.get(field), f"measurement {field}")
        for field in timestamp_fields
    ]
    foreground_start_ns, foreground_end_ns, drain_end_ns = timestamps
    if not foreground_start_ns < foreground_end_ns < drain_end_ns:
        raise GateValidationError("measurement timestamps are not ordered")
    clock_ticks = _nonnegative_integer(
        measurement.get("clock_ticks_per_second"),
        "measurement clock_ticks_per_second",
    )
    if clock_ticks == 0:
        raise GateValidationError("measurement clock ticks must be positive")
    foreground_line_count = _nonnegative_integer(
        measurement.get("pidstat_foreground_line_count"),
        "pidstat foreground line count",
    )

    mount_pid = _nonnegative_integer(manifest.get("mount_pid"), "manifest mount_pid")
    if mount_pid == 0:
        raise GateValidationError("manifest mount_pid must be positive")
    expected_start_time = manifest.get("mount_pid_start_time")
    if not isinstance(expected_start_time, str) or not expected_start_time.isdigit():
        raise GateValidationError("manifest mount_pid_start_time is invalid")
    start_cpu = _proc_cpu(run_dir / "proc-foreground-start/stat.txt", mount_pid)
    end_cpu = _proc_cpu(run_dir / "proc-foreground-end/stat.txt", mount_pid)
    drain_cpu = _proc_cpu(run_dir / "proc-drain-end/stat.txt", mount_pid)
    if {start_cpu[2], end_cpu[2], drain_cpu[2]} != {expected_start_time}:
        raise GateValidationError("mount process start time changed during measurement")
    for label, before, after in (
        ("foreground user", start_cpu[0], end_cpu[0]),
        ("foreground system", start_cpu[1], end_cpu[1]),
        ("drain user", end_cpu[0], drain_cpu[0]),
        ("drain system", end_cpu[1], drain_cpu[1]),
    ):
        if after < before:
            raise GateValidationError(f"mount CPU counter decreased: {label}")
    foreground_user = (end_cpu[0] - start_cpu[0]) / clock_ticks
    foreground_system = (end_cpu[1] - start_cpu[1]) / clock_ticks
    drain_user = (drain_cpu[0] - end_cpu[0]) / clock_ticks
    drain_system = (drain_cpu[1] - end_cpu[1]) / clock_ticks
    foreground_cpu = foreground_user + foreground_system
    drain_cpu_seconds = drain_user + drain_system
    total_cpu = foreground_cpu + drain_cpu_seconds

    foreground_cores, drain_cores = _pidstat_core_samples(
        run_dir / "pidstat.txt",
        foreground_line_count=foreground_line_count,
        mount_pid=mount_pid,
    )
    foreground_core_stats = _core_stats(foreground_cores)
    drain_core_stats = _core_stats(drain_cores)
    memory_peaks = _memory_peaks(
        run_dir,
        foreground_start_ns=foreground_start_ns,
        foreground_end_ns=foreground_end_ns,
        drain_end_ns=drain_end_ns,
    )

    written = _nonnegative_integer(
        manifest.get("verified_bytes_written"),
        "verified_bytes_written",
    )
    read = _nonnegative_integer(
        manifest.get("verified_bytes_read"),
        "verified_bytes_read",
    )
    processed_gib = (written + read) / float(1 << 30)
    if processed_gib <= 0:
        raise GateValidationError("verified processed GiB must be positive")
    foreground_seconds = (foreground_end_ns - foreground_start_ns) / 1_000_000_000
    drain_seconds = (drain_end_ns - foreground_end_ns) / 1_000_000_000
    expected = {
        "foreground_start_idle": True,
        "drain_end_idle": True,
        "negotiated_geometry": geometry,
        "callback_errors": callback_errors,
        "async_errors": async_errors,
        "task_errors": task_errors,
        "cancellations": cancellations,
        "processed_gib": processed_gib,
        "foreground_wall_seconds": foreground_seconds,
        "foreground_throughput_bytes_per_second": (written + read)
        / foreground_seconds,
        "foreground_mount_user_cpu_seconds": foreground_user,
        "foreground_mount_system_cpu_seconds": foreground_system,
        "foreground_mount_cpu_seconds": foreground_cpu,
        "drain_mount_user_cpu_seconds": drain_user,
        "drain_mount_system_cpu_seconds": drain_system,
        "drain_mount_cpu_seconds": drain_cpu_seconds,
        "total_lifecycle_mount_cpu_seconds": total_cpu,
        "total_lifecycle_cpu_seconds_per_gib": total_cpu / processed_gib,
        "foreground_average_cores": foreground_core_stats[0],
        "foreground_p95_cores": foreground_core_stats[1],
        "foreground_peak_cores": foreground_core_stats[2],
        "drain_average_cores": drain_core_stats[0],
        "drain_p95_cores": drain_core_stats[1],
        "drain_peak_cores": drain_core_stats[2],
        "drain_duration_seconds": drain_seconds,
        **memory_peaks,
    }
    _match_raw_derived(derived, expected)


def _discover_mode(mode_dir: Path, mode: str) -> dict[tuple[str, str, int], Path]:
    if not mode_dir.is_dir():
        raise GateValidationError(f"mode artifact directory is missing: {mode_dir}")
    unexpected = [entry.name for entry in mode_dir.iterdir() if entry.name not in WORKLOADS]
    if unexpected:
        raise GateValidationError(f"unexpected entries in {mode}: {', '.join(sorted(unexpected))}")
    discovered = {}
    for workload in WORKLOADS:
        workload_dir = mode_dir / workload
        if not workload_dir.is_dir():
            raise GateValidationError(f"missing workload directory: {mode}/{workload}")
        for run_dir in workload_dir.iterdir():
            name = run_dir.name
            if (
                not run_dir.is_dir()
                or len(name) != len("rep-000")
                or not name.startswith("rep-")
                or not name[4:].isdigit()
            ):
                raise GateValidationError(f"invalid repetition directory: {run_dir}")
            repetition = int(name[4:])
            key = (mode, workload, repetition)
            if key in discovered:
                raise GateValidationError(f"duplicate repetition directory: {run_dir}")
            discovered[key] = run_dir.resolve()
    return discovered


def _load_run(
    run_dir: Path,
    schedule_item: dict,
    correctness_manifest: dict,
    correctness_manifest_path: Path,
    correctness_manifest_sha256: str,
    promotion_eligible: bool,
) -> dict:
    mode = schedule_item["mode"]
    workload = schedule_item["workload"]
    repetition = schedule_item["repetition"]
    validate_artifact_layout(run_dir, workload=workload)
    environment = _read_json(run_dir / "environment.json")
    manifest = _read_json(run_dir / "manifest.json")
    derived = _read_json(run_dir / "derived.json")
    cleanup = _read_json(run_dir / "cleanup.json")

    if environment.get("test_fixture") is not False:
        raise GateValidationError(f"test fixture evidence is ineligible: {run_dir}")

    for payload, label in ((environment, "environment"), (manifest, "manifest")):
        if payload.get("schema_version") != 1 or payload.get("status") != "complete":
            raise GateValidationError(f"{label} schema/status mismatch in {run_dir}")
        if payload.get("promotion_eligible") is not promotion_eligible:
            raise GateValidationError(f"{label} promotion eligibility mismatch in {run_dir}")
        if payload.get("mode") != mode or payload.get("workload") != workload:
            raise GateValidationError(f"{label} mode/workload mismatch in {run_dir}")
        if payload.get("repetition") != repetition:
            raise GateValidationError(f"{label} repetition mismatch in {run_dir}")
        if payload.get("runs") != 1:
            raise GateValidationError(f"{label} runs must equal one in {run_dir}")
        for field in ("cache_dir", "mount_point", "cgroup"):
            if payload.get(field) != schedule_item.get(field):
                raise GateValidationError(f"{label} {field} mismatch in {run_dir}")
    if derived.get("schema_version") != 1 or derived.get("status") != "complete":
        raise GateValidationError(f"derived schema/status mismatch in {run_dir}")
    if derived.get("promotion_eligible") is not promotion_eligible:
        raise GateValidationError(f"derived promotion eligibility mismatch in {run_dir}")

    mount_pid = manifest.get("mount_pid")
    if (
        not isinstance(mount_pid, int)
        or isinstance(mount_pid, bool)
        or mount_pid <= 0
        or environment.get("mount_pid") != mount_pid
    ):
        raise GateValidationError(f"mount pid mismatch in {run_dir}")
    if environment.get("mount_pid_start_time") != manifest.get(
        "mount_pid_start_time"
    ):
        raise GateValidationError(f"mount pid start time mismatch in {run_dir}")
    correctness_marker = environment.get("correctness_manifest")
    if not isinstance(correctness_marker, dict):
        raise GateValidationError(f"environment correctness marker is missing in {run_dir}")
    marker_path = correctness_marker.get("path")
    if (
        not isinstance(marker_path, str)
        or _normalized_path(marker_path) != _normalized_path(str(correctness_manifest_path))
        or correctness_marker.get("sha256") != correctness_manifest_sha256
    ):
        raise GateValidationError(f"environment correctness marker mismatch in {run_dir}")

    mount_environment = environment.get("mount_environment")
    if not isinstance(mount_environment, dict):
        raise GateValidationError(f"mount environment is missing in {run_dir}")
    validate_mode_environment(mode, mount_environment)
    cgroup_preflight = environment.get("cgroup_preflight")
    if not isinstance(cgroup_preflight, dict):
        raise GateValidationError(f"cgroup preflight is missing in {run_dir}")
    validate_cgroup_preflight(cgroup_preflight)
    if cgroup_preflight.get("path") != schedule_item.get("cgroup"):
        raise GateValidationError(f"cgroup preflight path mismatch in {run_dir}")
    collector_pid = cgroup_preflight.get("collector_pid")
    if (
        not isinstance(collector_pid, int)
        or isinstance(collector_pid, bool)
        or collector_pid <= 0
    ):
        raise GateValidationError(f"cgroup collector pid is invalid in {run_dir}")
    if cgroup_preflight.get("mount_pid") != mount_pid:
        raise GateValidationError(f"cgroup mount pid mismatch in {run_dir}")
    if cgroup_preflight.get("memberships_verified") is not True:
        raise GateValidationError(f"cgroup memberships are not verified in {run_dir}")

    validate_run_provenance(
        manifest,
        correctness_manifest,
        correctness_manifest_path=str(correctness_manifest_path),
        correctness_manifest_sha256=correctness_manifest_sha256,
    )
    parallelism = manifest.get("parallelism") if workload == "same-file-parallel" else None
    workload_bytes = (
        4_000_000_000
        if workload in ("large-single", "fixed-1m")
        else manifest.get("workload_bytes")
    )
    expected = validate_workload_manifest(
        manifest,
        workload=workload,
        workload_bytes=workload_bytes,
        parallelism=parallelism,
    )
    for field in ("verified_bytes_written", "verified_bytes_read"):
        if derived.get(field) != expected[field]:
            raise GateValidationError(f"derived {field} mismatch in {run_dir}")
    validate_raw_measurements(
        run_dir,
        mode=mode,
        environment=environment,
        manifest=manifest,
        derived=derived,
    )
    validate_run_eligibility(mode, derived, cleanup)
    if workload in (
        "large-single",
        "small-128k",
    ) and isinstance(derived.get("memory_pressure"), dict):
        validate_memory_pressure_artifacts(run_dir, derived["memory_pressure"])
    pressure_result = validate_memory_pressure_evidence(
        mode,
        workload,
        derived.get("memory_pressure"),
        expected_written=expected["verified_bytes_written"],
        expected_read=expected["verified_bytes_read"],
        promotion_eligible=promotion_eligible,
    )

    runtime_start = _read_json(run_dir / "runtime-foreground-start.json")
    runtime_end = _read_json(run_dir / "runtime-foreground-end.json")
    warm_first_end = None
    if workload == "warm-reopen":
        warm_first_end = _read_json(run_dir / "runtime-warm-first-end.json")
        _require_schema_version(
            warm_first_end, EXPECTED_RUNTIME_SCHEMA, "warm midpoint runtime snapshot"
        )
        if not snapshot_is_idle(warm_first_end):
            raise GateValidationError("warm first-read boundary is not idle")
    validate_raw_callback_intervals(
        mode,
        workload,
        runtime_start,
        runtime_end,
        derived,
        warm_first_end=warm_first_end,
    )
    client_report = _read_json(run_dir / "client-report.json")
    if workload in ("large-single", "fixed-1m"):
        generated_reports = [str(path) for path in run_dir.glob("studiofs-bench-report-*.json")]
        validate_bench_report(
            client_report,
            workload=workload,
            mount_point=schedule_item["mount_point"],
            generated_json_files=generated_reports,
            workload_bytes=workload_bytes,
        )
        original_report = _read_json(Path(generated_reports[0]))
        if original_report != client_report:
            raise GateValidationError("benchmark report copy does not match original")
    else:
        validate_python_client_report(
            client_report,
            workload=workload,
            workload_bytes=workload_bytes,
            parallelism=parallelism,
        )
    if workload == "warm-reopen":
        assert warm_first_end is not None
        validate_warm_proof(
            mode,
            runtime_start,
            warm_first_end,
            runtime_end,
            derived,
            checksum_verified=(
                client_report.get("expected_checksum")
                == client_report.get("actual_checksum")
            ),
        )

    result = dict(derived)
    result["_workload_bytes"] = expected["workload_bytes"]
    result["_memory_pressure"] = pressure_result
    return result


def _compare_modes(
    baseline: dict[str, list[dict]],
    candidate: dict[str, list[dict]],
    candidate_mode: str,
) -> dict:
    workloads = {}
    for workload in WORKLOADS:
        baseline_rows = baseline[workload]
        candidate_rows = candidate[workload]
        workload_bytes = baseline_rows[0]["_workload_bytes"]
        if any(row["_workload_bytes"] != workload_bytes for row in [*baseline_rows, *candidate_rows]):
            raise GateValidationError(f"workload byte configuration differs for {workload}")
        compare_throughput = workload in (
            "large-single",
            "fixed-1m",
            "small-128k",
        ) or (candidate_mode == "direct-async" and workload == "same-file-parallel")
        result = evaluate_numeric_gates(
            baseline_rows,
            candidate_rows,
            candidate_mode=candidate_mode,
            workload_bytes=workload_bytes,
            compare_throughput=compare_throughput,
        )
        result["performance_passed"] = result["passed"]
        result["performance_diagnostic"] = candidate_mode == "kernel"
        if candidate_mode == "kernel":
            result["passed"] = True
        result["reported_cpu"] = {
            "baseline_foreground": median_mad(
                _numeric_samples(baseline_rows, "foreground_mount_cpu_seconds")
            ),
            "candidate_foreground": median_mad(
                _numeric_samples(candidate_rows, "foreground_mount_cpu_seconds")
            ),
            "baseline_drain": median_mad(
                _numeric_samples(baseline_rows, "drain_mount_cpu_seconds")
            ),
            "candidate_drain": median_mad(
                _numeric_samples(candidate_rows, "drain_mount_cpu_seconds")
            ),
        }
        if workload in ("large-single", "small-128k"):
            pressure = [row["_memory_pressure"] for row in candidate_rows]
            result["memory_pressure"] = {
                "status": "complete",
                "passed": all(item["passed"] for item in pressure),
                "recovery_passed": all(item["recovery_passed"] for item in pressure),
                "performance_diagnostic": candidate_mode == "kernel",
            }
        workloads[workload] = result
    return {
        "eligible": True,
        "passed": all(result["passed"] for result in workloads.values()),
        "workloads": workloads,
    }


def analyze_artifacts(
    *,
    direct_serial_dir: Path,
    direct_async_dir: Path,
    kernel_dir: Path,
    correctness_manifest_path: Path,
) -> dict:
    """Read one matrix and return its deterministic promotion comparison."""
    mode_dirs = {
        "direct-serial": direct_serial_dir.resolve(),
        "direct-async": direct_async_dir.resolve(),
        "kernel": kernel_dir.resolve(),
    }
    matrix_roots = {path.parent for path in mode_dirs.values()}
    if len(matrix_roots) != 1:
        raise GateValidationError("all mode artifacts must share one matrix root")
    matrix_root = matrix_roots.pop()
    correctness_manifest_path = correctness_manifest_path.resolve()
    if correctness_manifest_path.parent != matrix_root:
        raise GateValidationError("correctness manifest must belong to the matrix root")
    schedule = _read_json(matrix_root / "schedule.json")
    correctness_manifest = _read_json(correctness_manifest_path)
    correctness_manifest_sha256 = _file_sha256(correctness_manifest_path)

    discovered = {}
    for mode, mode_dir in mode_dirs.items():
        discovered.update(_discover_mode(mode_dir, mode))
    eligibility = validate_schedule(
        schedule,
        discovered_tuples=list(discovered),
    )
    if correctness_manifest.get("promotion_eligible") is not eligibility[
        "promotion_eligible"
    ]:
        raise GateValidationError("correctness promotion eligibility mismatch")
    schedule_by_key = {
        (item["mode"], item["workload"], item["repetition"]): item
        for item in schedule["tuples"]
    }
    for key, run_dir in discovered.items():
        if Path(schedule_by_key[key]["artifact_dir"]).resolve() != run_dir:
            raise GateValidationError(f"scheduled artifact path mismatch: {run_dir}")

    anchor_manifest = _read_json(next(iter(discovered.values())) / "manifest.json")
    validate_correctness_manifest(
        correctness_manifest,
        expected_revision=anchor_manifest.get("git_revision"),
        expected_studiofs_sha256=anchor_manifest.get("studiofs_sha256"),
        expected_bench_revision=anchor_manifest.get("bench_git_revision"),
        expected_bench_sha256=anchor_manifest.get("bench_sha256"),
    )

    mode_rows = {
        mode: {workload: [] for workload in WORKLOADS}
        for mode in MODES
    }
    direct_async_errors = []
    for key in sorted(discovered):
        mode, workload, _repetition = key
        try:
            row = _load_run(
                discovered[key],
                schedule_by_key[key],
                correctness_manifest,
                correctness_manifest_path,
                correctness_manifest_sha256,
                bool(eligibility["promotion_eligible"]),
            )
        except GateValidationError as error:
            if mode != "direct-async":
                raise
            direct_async_errors.append(f"{discovered[key]}: {error}")
        else:
            mode_rows[mode][workload].append(row)

    direct_serial = {"eligible": True, "passed": True}
    if direct_async_errors:
        direct_async = {
            "eligible": False,
            "passed": False,
            "errors": direct_async_errors,
        }
    else:
        try:
            direct_async = _compare_modes(
                mode_rows["direct-serial"],
                mode_rows["direct-async"],
                "direct-async",
            )
        except GateValidationError as error:
            direct_async = {
                "eligible": False,
                "passed": False,
                "errors": [str(error)],
            }
    kernel_baseline = select_kernel_baseline(direct_serial, direct_async)
    kernel = _compare_modes(
        mode_rows[kernel_baseline],
        mode_rows["kernel"],
        "kernel",
    )
    direct_async_passed = bool(direct_async["passed"])
    kernel_passed = bool(kernel["passed"])
    all_candidates_passed = direct_async_passed and kernel_passed
    passed = bool(
        eligibility["promotion_eligible"]
        and all_candidates_passed
    )
    return {
        "schema_version": 1,
        "promotion_eligible": eligibility["promotion_eligible"],
        "passed": passed,
        "direct_async_passed": direct_async_passed,
        "kernel_passed": kernel_passed,
        "all_candidates_passed": all_candidates_passed,
        "kernel_baseline": kernel_baseline,
        "correctness_manifest_path": str(correctness_manifest_path),
        "correctness_manifest_sha256": correctness_manifest_sha256,
        "direct_serial": direct_serial,
        "direct_async": direct_async,
        "kernel": kernel,
    }


def _write_outputs(output: Path, comparison: dict) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / "comparison.json").write_text(
        json.dumps(comparison, indent=2, sort_keys=True) + "\n"
    )
    baseline = comparison.get("kernel_baseline", "unavailable")
    direct_pressure = all(
        comparison.get("direct_async", {})
        .get("workloads", {})
        .get(workload, {})
        .get("memory_pressure", {})
        .get("recovery_passed", False)
        for workload in ("large-single", "small-128k")
    )
    kernel_pressure = all(
        comparison.get("kernel", {})
        .get("workloads", {})
        .get(workload, {})
        .get("memory_pressure", {})
        .get("recovery_passed", False)
        for workload in ("large-single", "small-128k")
    )
    summary = (
        "# Linux cache gate summary\n\n"
        f"- Promotion eligible: {'yes' if comparison.get('promotion_eligible') else 'no'}\n"
        f"- Direct async: {'PASS' if comparison.get('direct_async', {}).get('passed') else 'FAIL'}\n"
        f"- Kernel baseline: {baseline}\n"
        f"- Kernel: {'PASS' if comparison.get('kernel', {}).get('passed') else 'FAIL'}\n"
        f"- Memory pressure recovery (Direct async): {'PASS' if direct_pressure else 'FAIL'}\n"
        f"- Memory pressure recovery (kernel diagnostic): {'PASS' if kernel_pressure else 'MISS'}\n"
    )
    errors = comparison.get("errors", [])
    if errors:
        summary += f"- Error: {errors[0]}\n"
    (output / "summary.md").write_text(summary)
    verdict = "PROMOTE" if comparison.get("passed") else "RETAIN CURRENT DEFAULT"
    decision = (
        "# Linux cache decision\n\n"
        f"Decision: {verdict}.\n\n"
        f"Kernel baseline: {baseline}.\n"
    )
    if errors:
        decision += f"\nReason: {errors[0]}\n"
    (output / "decision.md").write_text(decision)


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate and summarize Linux cache-gate artifacts."
    )
    parser.add_argument("--direct-serial", required=True, type=Path)
    parser.add_argument("--direct-async", required=True, type=Path)
    parser.add_argument("--kernel", required=True, type=Path)
    parser.add_argument("--correctness-manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _argument_parser().parse_args(argv)
    try:
        comparison = analyze_artifacts(
            direct_serial_dir=args.direct_serial,
            direct_async_dir=args.direct_async,
            kernel_dir=args.kernel,
            correctness_manifest_path=args.correctness_manifest,
        )
    except GateValidationError as error:
        comparison = {
            "schema_version": 1,
            "promotion_eligible": False,
            "passed": False,
            "direct_async_passed": False,
            "kernel_passed": False,
            "all_candidates_passed": False,
            "kernel_baseline": "unavailable",
            "direct_serial": {"eligible": False, "passed": False},
            "direct_async": {"eligible": False, "passed": False},
            "kernel": {"eligible": False, "passed": False},
            "errors": [str(error)],
        }
    _write_outputs(args.output, comparison)
    return 0 if comparison["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
