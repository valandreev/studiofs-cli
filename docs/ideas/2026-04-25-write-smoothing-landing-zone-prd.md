# Write Smoothing Landing Zone PRD

Date: 2026-04-25
Status: Draft approved for planning

This PRD is intended to serve as the durable product and engineering reference
for write-path smoothing work. It is not itself a work-tracking issue. Once a
supported implementation slice exists, the relevant subset of this PRD should
be distilled into one or more Linear issues in English.

## Problem Statement

StudioFS currently delivers strong read responsiveness and strong cache-hit
behavior on Windows, but mounted write performance remains visually and
operationally bursty under real sequential media-style workloads.

In AJA System Test and similar capture-like workflows, StudioFS can show a
high average write rate when the operating system disk cache is enabled, but
the graph contains deep periodic dropouts instead of a stable plateau. With
the operating system disk cache disabled, the same write path becomes much more
stable, but sustained throughput falls to a much lower level. This creates an
uncomfortable trade-off for the operator: either accept impressive average
numbers with visible stalls, or accept stable behavior at a much lower
throughput ceiling.

From the user's perspective, the problem is not merely "make writes faster."
The real problem is that StudioFS does not currently provide a write path that
feels capture-safe and operationally steady under sequential mounted writes. A
filesystem that alternates between fast bursts and visible stalls is harder to
trust than one that holds a lower but smooth rate, and it is also harder to
reason about during diagnostics and tuning.

The first urgent scope is Windows mounted writes through WinFSP, because that
is where the current operator pain is concentrated. However, the design should
avoid baking in a Windows-only architectural dead end. If StudioFS needs a new
write-acceptance architecture to smooth writes, that architecture should be
usable as a shared VFS core later, with platform-specific cache and durability
policies layered on top for Windows, Linux, and macOS.

## Solution

StudioFS will introduce a write-smoothing architecture built around a fast
local landing zone for accepted writes and a steady background drain pipeline.

Instead of forcing the mounted write path to transition too quickly from
accepted bytes into payload-object materialization and metadata waves, StudioFS
will first persist accepted write data into a local append-oriented landing
zone that is optimized for smooth sequential ingestion. A separate background
pipeline will then convert landed data into the existing payload-object,
metadata, and upload lifecycle model at a controlled cadence.

From the user's perspective, this changes the mounted write path from a bursty
"buffer, then drain in waves" system into a steadier "accept locally at a
predictable rate, then finalize in the background" system. On Windows, this is
expected to reduce visible AJA write dropouts when the OS cache is enabled and
to raise the real sustained write floor that is exposed when the OS cache is
disabled.

The initial implementation target is Windows and WinFSP. The architecture,
however, will be split into:

- a shared landing-zone and drain core in the VFS/runtime, and
- platform-specific admission, cache, and barrier policy at the bridge layer.

This allows StudioFS to improve Windows immediately without assuming that
Linux FUSE and macOS FUSE-style bridges should use identical cache semantics.

## User Stories

1. As a StudioFS developer, I want sequential mounted writes on Windows to produce a smooth throughput graph, so that media-style workloads look operationally stable instead of bursty.
2. As a StudioFS developer, I want the Windows cached write path to avoid deep periodic stalls, so that the OS page cache does not merely hide a bursty second-stage drain.
3. As a StudioFS developer, I want the uncached Windows write path to achieve a meaningfully higher sustained floor than today, so that disabling the OS cache does not force throughput down to the current low plateau.
4. As a StudioFS developer, I want ordinary mounted writes to acknowledge against a fast local landing boundary, so that write callback latency remains predictable.
5. As a StudioFS developer, I want explicit flush, fsync, and last-handle close to remain the strict local durability barriers, so that operator-facing durability semantics stay understandable.
6. As a StudioFS developer, I want StudioFS to keep read-your-writes behavior intact while data is still in the landing zone, so that newly written bytes are immediately visible to the writing mount.
7. As a StudioFS developer, I want the background write-finalization pipeline to drain at a steady cadence, so that it does not reintroduce burst-then-stall behavior under pressure.
8. As a StudioFS developer, I want the write path to distinguish sequential media-style writers from other write patterns, so that smoothing logic can be specialized without overfitting every workload to a capture profile.
9. As a StudioFS developer, I want the new write architecture to minimize extra copies and avoid avoidable objectization work on the foreground path, so that smoothing is achieved through better boundaries rather than just larger buffers.
10. As a StudioFS developer, I want the new landing-zone architecture to reuse as much of the existing payload-object, metadata, and upload lifecycle machinery as practical, so that the change remains incremental and reviewable.
11. As a StudioFS developer, I want the Windows WinFSP bridge to preserve its current cleanup and close barrier ownership model, so that smoothing work does not regress existing WinFSP callback contracts.
12. As a StudioFS developer, I want the write path to expose better observability around landing, drain lag, burst size, and barrier ownership, so that performance tuning is evidence-driven.
13. As a StudioFS developer, I want to tell whether a slowdown comes from landing-zone ingestion, payload persistence, metadata commit cadence, or upload handoff, so that hypotheses can be tested separately.
14. As a StudioFS developer, I want write smoothing to improve real AJA cache-on behavior, so that StudioFS can be evaluated against a real operator workload rather than a synthetic-only benchmark.
15. As a StudioFS developer, I want the AJA cache-off case to remain smooth and become faster over time, so that the true sustained write sink improves and not only the burst behavior.
16. As a StudioFS developer, I want the implementation to remain Windows-first in scope, so that the team can solve the urgent operator problem without waiting for full cross-platform parity.
17. As a StudioFS developer, I want the shared VFS write core to remain reusable by Linux and macOS later, so that future platform work builds on one well-tested landing-zone design rather than three unrelated fixes.
18. As a Linux StudioFS developer, I want the future Linux adoption of the landing zone to coexist with FUSE cached mode and writeback-cache semantics, so that the kernel page cache and the StudioFS write core do not fight each other.
19. As a macOS StudioFS developer, I want the future macOS adoption of the landing zone to preserve platform-specific buffer-cache expectations, so that the design does not assume Windows rules everywhere.
20. As a StudioFS developer, I want sequential writer detection and policy selection to be explicit and observable, so that we can reason about which workloads are on the smoothed path.
21. As a StudioFS developer, I want the landing zone to support append-oriented preallocation, so that sequential ingestion avoids allocation jitter and fragmentation.
22. As a StudioFS developer, I want the drain worker to use steady quotas or cadence-based progress rather than only large threshold-triggered waves, so that throughput remains visually and operationally smooth.
23. As a StudioFS developer, I want the write path to apply softer earlier backpressure based on drain lag, so that pressure is absorbed gradually instead of erupting as rare large stalls.
24. As a StudioFS developer, I want the landing-zone format and state machine to survive restart or recovery requirements, so that accepted local writes are not orphaned by a daemon restart.
25. As a StudioFS developer, I want the landing-zone to integrate cleanly with existing upload replay and startup reconciliation, so that local-first guarantees remain coherent after crashes or restarts.
26. As a StudioFS developer, I want the new write path to preserve the current local-first contract that foreground barriers do not imply remote publication, so that smoothing work does not accidentally drag S3 or PostgreSQL into the foreground.
27. As a StudioFS developer, I want metadata commit waves to become smaller and more regular during heavy sequential writes, so that metadata is no longer one of the major sources of visible write pulsation.
28. As a StudioFS developer, I want the new architecture to keep cleanup observational and keep close-side fallback barriers intact on WinFSP, so that SFS-192-style callback sequencing guarantees stay protected.
29. As a StudioFS developer, I want to measure write smoothness using callback latency and throughput-gap metrics in addition to average MiB/s, so that the success criteria match the operator problem.
30. As a StudioFS developer, I want the AJA-style mounted smoke coverage to evolve into cache-on and cache-off validation modes, so that the repo can detect regressions in both the bursty and sustained cases.
31. As a StudioFS developer, I want the landing-zone implementation to be partitioned into deep modules with small interfaces, so that the design is testable and future platform work can plug into it cleanly.
32. As a StudioFS developer, I want the first rollout to be narrow and reversible, so that we can ship a controlled vertical slice and validate it with real workloads before broadening the policy surface.
33. As an operator, I want the write graph to look boring in the best possible way, so that I can trust StudioFS during sustained capture-style writes.
34. As an engineering team, we want the primary KPI for this effort to be smoother mounted write behavior under real Windows sequential workloads, so that throughput gains do not come at the cost of visible operational instability.

## Implementation Decisions

- The first implementation scope is Windows mounted writes through the WinFSP bridge.
- The architecture will introduce a local write landing zone that acts as the fast acceptance boundary for normal mounted writes.
- The landing zone will be append-oriented and optimized for sequential ingestion rather than immediate payload-object materialization.
- The landing zone will feed a separate steady drain pipeline that performs payload finalization, metadata updates, and upload-queue handoff at a controlled cadence.
- The existing payload-object and metadata model remains the long-term durable representation; the landing zone is an intermediate local acceptance stage, not a replacement product model.
- The design will preserve the current local-first contract: foreground strict barriers remain local durability boundaries and do not imply remote publication.
- The bridge layer will continue to own platform-specific policy decisions, including whether a handle participates in cached, opportunistic, or stricter local-write behavior.
- The shared VFS/runtime core will own the landing-zone format, drain scheduling, recovery hooks, and observability.
- Sequential media-style writers are an explicit target. The system may use sequential-write detection or explicit policy selection to route those workloads onto the smoothed path.
- The first design goal is to reduce burstiness in the Windows cache-on case without regressing correctness or WinFSP callback ownership semantics.
- A secondary goal is to raise the sustained throughput floor visible in the Windows cache-off case by making the StudioFS durable sink itself faster and more append-friendly.
- The system will favor steady quota- or cadence-based draining over large threshold-triggered drain waves wherever practical.
- Early, gentle backpressure based on drain lag is preferred over rare large fallback stalls caused by oversized dirty bursts.
- Existing WinFSP cleanup, flush, and close behavior remains part of the contract. Smoothing work must not reassign barrier ownership in a way that regresses established callback sequencing expectations.
- The initial rollout will not assume identical Linux and macOS cache semantics. Instead, it will provide a shared core that later platforms can adopt with platform-specific admission and barrier policies.
- Linux is expected to use the same landing-zone core later only if it can coexist cleanly with FUSE cached mode and writeback-cache behavior.
- macOS is expected to use the same landing-zone core later only if it can coexist cleanly with macOS buffer-cache behavior and macFUSE-style cache invalidation rules.
- The implementation should bias toward deep modules with narrow stable interfaces, such as landing-zone ownership, drain scheduling, recovery/replay integration, and policy classification.
- Observability must expand to make landing, drain lag, burst size, queue wait, metadata cadence, and barrier attribution first-class runtime signals.
- Success criteria will include smoothness metrics, not only average throughput metrics.

## Testing Decisions

- A good test validates externally observable behavior and contract outcomes rather than incidental implementation details. For this effort, good tests assert write acceptance boundaries, barrier ownership, read-your-writes behavior, recovery outcomes, and smoothness-oriented runtime signals without overfitting to one internal data structure.
- The landing-zone ownership and replay module should receive focused tests for append acceptance, restart/recovery semantics, and tail-drain correctness.
- The drain scheduler should receive focused tests for steady progress, quota or cadence behavior, and soft-backpressure behavior under sustained dirty pressure.
- The write policy classification layer should receive focused tests that distinguish ordinary cached writes, sequential media-style writes, and explicit strict barrier paths.
- The Windows bridge path should receive focused WinFSP tests to ensure cleanup remains observational, close retains fallback strict-barrier ownership, and mounted sequential writer behavior does not regress callback semantics.
- The VFS read path should receive focused regression coverage proving that data landed but not yet fully finalized remains visible to reads on the writing mount.
- Recovery and startup reconciliation should receive focused tests that prove accepted local writes in the landing zone survive restart and re-enter the finalization pipeline correctly.
- Runtime observability should receive focused snapshot tests that expose the new landing, drain, and smoothness metrics in stable operator-facing summaries.
- Prior art for these tests already exists in the repo's focused VFS and WinFSP test suites, especially around handle-writer ownership, explicit flush and close barriers, local dirty state, write-path observability, and the existing AJA-style WinFSP smoke coverage.
- Manual validation remains necessary for the real AJA System Test workload, because the user-visible problem is specifically about live mounted smoothness under operator-controlled settings.
- Full-suite validation is explicitly out of scope for the implementation loop; focused crate- and scenario-level coverage is the desired verification style for local development.

## Out of Scope

- Full cross-platform rollout in the first implementation slice.
- Reworking Linux or macOS bridge policy in the same first slice as the Windows fix.
- Changing the product contract so that ordinary writes imply remote visibility or remote durability.
- Replacing the existing payload-object or upload lifecycle architecture wholesale.
- General read-path optimization work unrelated to the write smoothing problem.
- Arbitrary benchmark chasing that improves average throughput while leaving the smoothness problem unsolved.
- A complete operator-facing tuning surface for every landing-zone knob in the first version.
- Full end-to-end CI or full workspace validation as part of this initial planning artifact.

## Further Notes

- The current evidence suggests that the most painful operator-visible behavior is not explained by raw bridge copy overhead alone. The more likely issue is the interaction between operating-system write-back behavior and StudioFS's own deferred materialization and bursty local drain behavior.
- The Windows cache-on and cache-off cases should be treated as two related but distinct validation modes: one reveals double-buffering and drain-wave behavior, while the other reveals the true sustained speed of the StudioFS durable sink.
- The landing-zone design should be shaped so that a future Linux or macOS rollout can share core mechanics without forcing those platforms into Windows-specific cache assumptions.
- When this PRD is broken into implementation work, the preferred first Linear slice should be a narrow vertical path that introduces the landing-zone abstraction, routes one Windows sequential mounted-write policy through it, and exposes enough observability to compare before and after AJA runs.
