# Runtime Investigation Attach PRD

Date: 2026-04-13
Status: Draft approved for planning

This PRD is intended to serve as the durable product and engineering reference
for the feature. It is not itself a work-tracking issue. Once a supported
hypothesis and a concrete implementation slice exist, the relevant subset of
this PRD should be distilled into one or more Linear issues in English.

## Problem Statement

StudioFS currently relies on a weak debugging loop for performance,
synchronization, and cache-behavior investigations under real workloads.
Developers can inspect code, run tests, and review logs, but they cannot
efficiently attach an AI investigation agent to a live mounted instance and let
that agent observe the system from inside the running workload context.

This creates a recurring gap between what the user can describe and what the
system is actually doing. For many important problems, especially performance
regressions, queue buildup, synchronization stalls, upload pipeline issues, and
cache inefficiencies, user-provided descriptions are not enough. The real
signals only appear while the mounted filesystem is handling a live workload
such as AJA System Test, large file copies, open/read workflows, or repeated
editor/media access patterns.

The project needs a durable feature that lets an external AI agent attach to a
running StudioFS daemon, observe structured runtime state, request targeted
actions from the user or local automation, correlate workload phases with
runtime evidence, identify likely bottlenecks, track and test hypotheses across
multiple runs, and turn supported findings into implementation work. The goal
is not merely better visibility into a live mount, but a materially shorter
engineering loop from symptom to evidence to hypothesis to issue to fix to
re-test.

## Solution

StudioFS will provide a local attachable diagnostics and control plane for live
mount investigations. A running daemon will expose a same-user local debug
protocol that remains dormant by default in normal builds and activates only
when a controller attaches or diagnostics are requested. An external
`studiofs-dev-mcp` adapter will connect to that protocol and expose structured
resources and tools to an AI investigation agent.

The system will support long-lived investigation cases, short-lived runs within
those cases, manual and agent-created markers, structured runtime snapshots,
recent event and log streams, selective drill-down by object or operation, and
on-demand profiling windows with an explicit diagnostics budget. Runtime
snapshots will be the primary source of truth, event and log streams will
provide causal context, and profiling captures will reveal where wall time,
blocking time, and CPU time are actually spent under real workloads.

The attach flow will allow the agent to guide the user through workloads such
as AJA System Test, file copies, or GUI open/read scenarios while observing the
live system, identifying anomalies, and generating investigation reports. Those
reports will support structured hypotheses, compare runs before and after code
changes, and provide the evidence required to create or update Linear issues
once there is a supported hypothesis and a clear next engineering step.

The design is intentionally split into a daemon-side diagnostics protocol and
an external MCP adapter so that StudioFS runtime logic remains decoupled from
AI tooling and so the same diagnostics surface can later support CLI, TUI,
dashboard, or automation clients in addition to MCP.

## User Stories

1. As a StudioFS developer, I want to attach an AI agent to a live mounted daemon, so that the agent can observe the real runtime state instead of relying only on my description.
2. As a StudioFS developer, I want the agent to guide me through a workload step by step, so that the system can collect evidence during the exact moments that matter.
3. As a StudioFS developer, I want to start an investigation case for a specific symptom in a specific environment, so that later runs are grouped under the same problem statement.
4. As a StudioFS developer, I want to create multiple runs inside one investigation case, so that I can compare behavior before and after code changes.
5. As a StudioFS developer, I want the agent to place markers around workload phases, so that runtime data can be correlated with the exact test steps I performed.
6. As a StudioFS developer, I want to place markers manually from a CLI, so that GUI workloads such as AJA can still be correlated with runtime evidence.
7. As an AI investigation agent, I want structured runtime snapshots from a live daemon, so that I can reason about system behavior without parsing only free-form logs.
8. As an AI investigation agent, I want recent event and log streams from key subsystems, so that I can understand how the system reached its current state.
9. As an AI investigation agent, I want on-demand wall-time and blocking-aware profiling windows, so that I can identify where real workload time is being spent.
10. As an AI investigation agent, I want CPU profiling windows in addition to wall-time profiling, so that I can distinguish hot computation from waiting and coordination overhead.
11. As a StudioFS developer, I want the attach system to remain low-overhead when inactive, so that production-like behavior is not distorted by the existence of diagnostics support.
12. As a StudioFS developer, I want cheap counters and snapshots to remain available even when deep profiling is off, so that basic investigations start quickly.
13. As a StudioFS developer, I want diagnostics presets such as minimal, balanced, and deep-investigation, so that I can trade off visibility and overhead intentionally.
14. As an AI investigation agent, I want to adjust diagnostics cost knobs such as profiling duration, sampling rate, and drill-down scope, so that I can focus on useful evidence without overwhelming the workload.
15. As an AI investigation agent, I want selective drill-down by subsystem, time window, operation type, inode, path, or file handle, so that I can narrow a broad symptom into a concrete hotspot.
16. As a StudioFS developer, I want the system to show the current cost of diagnostics itself, so that I can tell whether observability is perturbing the workload.
17. As an AI investigation agent, I want to generate derived summaries such as queue growth episodes, blocking intervals, and hotspot comparisons, so that I can reason from prepared evidence instead of raw telemetry alone.
18. As a StudioFS developer, I want investigation runs to persist after the mount exits, so that I can review, compare, and discuss evidence later.
19. As a StudioFS developer, I want investigation artifacts such as reports, bundles, and profiles to be stored with searchable metadata, so that I can find past evidence quickly.
20. As an AI investigation agent, I want a complete environment fingerprint for every run, so that I can avoid invalid comparisons across commits, configs, machines, or workloads.
21. As a StudioFS developer, I want to compare two runs across equivalent marker spans, so that I can see whether a fix improved the relevant phase instead of only the overall session.
22. As an AI investigation agent, I want to compare hot paths and anomalies between runs, so that I can determine whether a suspected bottleneck was actually reduced.
23. As a StudioFS developer, I want investigation hypotheses to be structured and tracked over time, so that false leads, supported explanations, and re-test results are not lost.
24. As an AI investigation agent, I want to update hypothesis status with linked evidence, so that the investigation remains coherent as new runs arrive.
25. As a StudioFS developer, I want the system to create or update a Linear issue only when there is a supported hypothesis and a clear next engineering step, so that work tracking stays focused and useful.
26. As a StudioFS developer, I want the attach system to support both manual workload steps and automated CLI or GUI steps, so that I can investigate both scriptable and interactive scenarios.
27. As an AI investigation agent, I want to perform a safe set of debug actions without repeated confirmation, so that the investigation loop remains fast.
28. As a StudioFS developer, I want louder safe actions such as counter resets or self-checks to be recorded visibly in the timeline, so that the evidence trail remains trustworthy.
29. As a StudioFS developer, I want multiple clients to read investigation data while only one client controls the session, so that collaboration is possible without conflicting commands.
30. As a StudioFS developer, I want controller ownership to use a lease with timeout and takeover, so that a crashed client does not permanently lock the investigation session.
31. As a StudioFS developer, I want the diagnostics plane to fail open by default, so that a debug subsystem failure does not take down the mounted filesystem.
32. As a StudioFS developer, I want an optional stricter diagnostics mode for special experiments, so that I can choose stronger failure behavior when the investigation demands it.
33. As a StudioFS developer, I want the first version of the system to focus on ingress, write-path, upload pipeline, cache behavior, and synchronization state, so that it helps immediately with the kinds of issues we hit most often.
34. As a StudioFS developer, I want the architecture to be durable enough to add richer profiling and observability later, so that this work becomes a long-term debugging platform rather than a one-off feature.
35. As an engineering team, we want the main KPI for this feature to be a shorter symptom-to-evidence-to-hypothesis-to-issue-to-fix-to-re-test loop, so that observability improves delivery speed and not just visibility.

## Implementation Decisions

- The system will use an external attach model, not an in-process embedded AI runtime.
- The daemon will expose a local debug protocol, and MCP support will be provided by a separate adapter process.
- The first transport target is local same-user attach using platform-appropriate local IPC primitives, with the abstraction left open for remote support later.
- Normal builds will include the diagnostics surface, but it must remain dormant and low-overhead until used.
- Runtime snapshots are the primary source of truth; event and log streams are secondary context; profiling captures are a tertiary but essential investigation tool.
- The first profiling focus is wall-time and blocking-aware captures, with CPU profiling also supported in the same investigation flow.
- Diagnostics collection follows a hybrid model: cheap counters and summaries may stay live, while heavy profiling and deep drill-down are enabled on demand.
- The central product entity is an investigation case defined by a problem statement plus target environment and workload. A case contains multiple runs.
- Each run stores an environment fingerprint including platform, bridge, build identity, mount and runtime configuration, machine characteristics, workload identity, and active diagnostics settings.
- Each run includes a timeline of markers, notes, selected snapshots, event windows, profiling windows, and generated artifacts.
- Hypotheses are structured objects with statement, expected signal, linked evidence, current status, and optional linkage to an implementation issue.
- Linear issues are downstream artifacts created only after a supported hypothesis and a clear implementation slice exist.
- The diagnostics plane supports multi-reader access but exactly one controller at a time. Controller ownership uses a renewable lease with timeout and takeover.
- Safe debug actions are divided conceptually into silent-safe and loud-safe behavior. Loud-safe actions remain allowed for the controller but must be explicitly recorded in the timeline with time, actor, and reason.
- The system will support derived summaries rather than raw data alone, including span comparisons, queue growth episodes, blocking intervals, hotspot summaries, anomaly summaries, and hypothesis-oriented verdict support.
- The system will support selective drill-down by subsystem, operation type, time window, and object identity, with explicit caps and filters to control cost.
- Diagnostics budget is a first-class concept with visible cost and adjustable knobs for sampling rate, profile window duration, verbosity, drill-down scope, snapshot cadence, and artifact retention.
- The daemon-side design should favor deep modules with simple, stable interfaces rather than many shallow integrations spread across runtime code paths.
- The expected module boundaries are:
- A daemon diagnostics host that publishes stable runtime resources and executes safe debug actions.
- A local attach transport and discovery layer that lets clients find and connect to active mount sessions.
- A controller lease manager that arbitrates control actions while allowing concurrent readers.
- A case and run store that persists investigation metadata, hypotheses, timeline entries, and artifact indexes.
- A snapshot and event model that normalizes runtime evidence into stable schemas suitable for comparison and reporting.
- A profiling capture coordinator that starts and stops bounded capture windows under diagnostics budget rules.
- A report and comparison engine that turns raw and derived evidence into run reports and before/after comparisons.
- An MCP adapter that exposes attach, investigation, evidence, comparison, and export capabilities to the AI agent.
- A Linear integration layer that can create or update issues once the investigation reaches the right threshold.
- An optional workload orchestration surface that lets the agent run existing scripts and automation while still supporting manual workload steps.
- The system defaults to fail-open behavior for diagnostics failures. A stricter mode may be added for special investigations.

## Testing Decisions

- Good tests for this feature validate externally observable behavior rather than internal implementation details. They should verify what a client can see, do, and compare, not how counters or structs are internally organized.
- The daemon diagnostics host must be tested for schema stability, resource availability, safe-action boundaries, and fail-open degradation behavior.
- The local attach transport and discovery layer must be tested for same-user access behavior, reconnects, stale session handling, and platform-specific edge cases.
- The controller lease manager must be tested for renewal, expiry, takeover, and multi-reader versus single-controller semantics.
- The case and run store must be tested for persistence, restart recovery, artifact indexing, and hypothesis state transitions.
- The marker and timeline system must be tested for ordering, span construction, loud-safe action auditability, and mixed manual plus agent-driven marker flows.
- The profiling capture coordinator must be tested for bounded window behavior, budget enforcement, and graceful failure when profiling cannot be collected.
- The snapshot and event model must be tested for compatibility across versions and for correct comparison behavior across equivalent spans.
- The report and comparison engine must be tested with fixed input fixtures and representative integration runs to confirm that it produces stable summaries and meaningful deltas.
- The MCP adapter must be tested as an external client surface, focusing on attach workflows, controller behavior, investigation actions, and evidence export.
- Cross-platform behavior must be covered where relevant so that attach, session discovery, and diagnostics semantics remain consistent across WinFSP and FUSE-backed mounts.
- Prior art should be taken from existing CLI state tests, PID and session lifecycle tests, mount sanity tests, VFS integration tests, and metrics snapshot style tests already present in the repository.
- For implementation modules, testing priority should be highest for the diagnostics protocol, controller lease behavior, investigation persistence, comparison engine, and profiling coordinator because those modules determine trust in the system.

## Out of Scope

- A full remote multi-user observability platform in the first iteration.
- Immediate support for arbitrary network-exposed attach endpoints.
- Giving the agent unrestricted runtime mutation powers over mount behavior, throttles, caching policy, or synchronization semantics.
- Making every GUI workload fully automated in the first version.
- Requiring that every investigation step be orchestrated through automation rather than allowing manual workload phases.
- Building the entire Grafana, Pyroscope, or distributed observability stack in the first iteration.
- Always-on heavy profiling or always-on deep event tracing for every mount session.
- Replacing existing logs and debugging tools entirely.
- Guaranteeing that the first iteration can automatically diagnose every class of bug without developer guidance.
- Turning raw performance data directly into implementation work items without a supported hypothesis review step.

## Further Notes

- The first high-value problem area is expected to be ingress, write-path, upload pipeline, cache behavior, and synchronization observability because those are the dominant live-workload pain points today.
- The most important product KPI is not the number of metrics exposed but the reduction in cycle time from symptom to evidence to supported hypothesis to issue to fix to re-test.
- The first implementation should optimize for trustworthiness and repeatability over visual polish.
- Comparison between runs is a first-class capability, not an optional report embellishment.
- A successful implementation will let the AI agent operate as a real investigation partner during live workloads rather than as a passive code reviewer with delayed context.
