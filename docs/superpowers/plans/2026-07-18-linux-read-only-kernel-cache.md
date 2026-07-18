# Linux Read-Only Kernel Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable Linux page-cache reuse for strict read-only file handles while preserving Direct + ASYNC_DIO for every write-capable handle.

**Architecture:** Add a `ReadOnlyKernel` FUSE cache policy as the Linux default. Classify access once during open/create, feed read-only handles into the existing generation-aware OS-cache lifecycle, and keep write-capable handles on the existing direct path without requesting FUSE writeback cache.

**Tech Stack:** Rust 2024, libfuse3 low-level API, existing StudioFS OS-cache lifecycle, Cargo tests, `studiofs-bench`.

## Global Constraints

- Modify only the Linux FUSE bridge and related Linux cache documentation.
- Add no dependency, cache layer, eviction algorithm, or public CLI option.
- Do not use `POSIX_FADV_DONTNEED` for mounted user reads.
- Preserve explicit `direct` and `kernel` policies as internal controls.
- Follow `docs/juicefs-local-first-rust-contract.md` and use test-first development.

---

### Task 1: Add the read-only kernel policy

**Files:**
- Modify: `crates/studiofs-linux-fuse/src/low_level.rs`

**Interfaces:**
- Produces: `LinuxFuseCachePolicy::ReadOnlyKernel`.
- Produces: `linux_fuse_open_cache_policy_for(cache_policy, read_only, async_dio_enabled, field_supported, disposition) -> LinuxFuseOpenCachePolicy`.

- [ ] **Step 1: Write failing policy tests**

Add cases proving that `ReadOnlyKernel` makes a read-only reuse non-direct with
`keep_cache`, makes a write-capable open direct with no `keep_cache`, requests
ASYNC_DIO, and does not request writeback cache.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cargo test -p studiofs-linux-fuse read_only_kernel -- --nocapture
```

Expected: compilation or assertion failure because `ReadOnlyKernel` does not
exist and the current default is `Direct`.

- [ ] **Step 3: Implement the minimal policy mapping**

Add the enum variant and make it the environment fallback. Extend the open
policy helper with a `read_only: bool` argument. Request ASYNC_DIO for `Direct`
and `ReadOnlyKernel`; request writeback cache only for `Kernel`.

- [ ] **Step 4: Run focused policy tests and verify GREEN**

Run:

```bash
cargo test -p studiofs-linux-fuse read_only_kernel -- --nocapture
cargo test -p studiofs-linux-fuse linux_cache_decision_flags_truth_table -- --nocapture
```

Expected: all selected tests pass.

### Task 2: Classify opens and reuse the existing coherence lifecycle

**Files:**
- Modify: `crates/studiofs-linux-fuse/src/low_level.rs`

**Interfaces:**
- Consumes: `LinuxFuseCachePolicy::ReadOnlyKernel` from Task 1.
- Produces: `LinuxFuseOpenedFile { fh, ino, read_only }`.
- Produces: `linux_fuse_open_is_read_only(open_flags, trunc) -> bool`.

- [ ] **Step 1: Write failing access-class tests**

Test `RDONLY` as read-only and test `WRONLY`, `RDWR`, `APPEND`, and truncation as
write-capable. Extend cache-open tests so a `ReadOnlyKernel` read participates in
generation-aware reuse while a writer reports `PolicyDisabled` and stays direct.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cargo test -p studiofs-linux-fuse linux_read_only_kernel -- --nocapture
```

Expected: failure because opened files do not carry the access class.

- [ ] **Step 3: Implement access classification**

Classify the translated `OpenFlags` once in `SessionState::open` and
`SessionState::create`. Pass `OsCacheParticipation::Enabled` only for
`ReadOnlyKernel` read-only handles. Use the same boolean to choose the FUSE
`direct_io`, `keep_cache`, and `parallel_direct_writes` fields.

- [ ] **Step 4: Run focused and existing coherence tests**

Run:

```bash
cargo test -p studiofs-linux-fuse linux_read_only_kernel -- --nocapture
cargo test -p studiofs-linux-fuse linux_kernel_ -- --nocapture
cargo test -p studiofs-linux-fuse linux_direct_open_enrolls_foreground_lifetime_without_keep_cache -- --nocapture
```

Expected: all selected tests pass.

### Task 3: Mounted proof and real-disk performance evidence

**Files:**
- Modify: `crates/studiofs-linux-fuse/src/low_level.rs`
- Modify: `docs/perf/linux-cache-performance-result.md`

**Interfaces:**
- Consumes: the read-only kernel policy from Tasks 1 and 2.
- Produces: an ignored mounted regression test and retained benchmark artifacts under `target/perf/`.

- [ ] **Step 1: Add the mounted regression test**

Add an ignored Linux test that writes and closes a deterministic file, performs
two fresh read-only reads, verifies both payloads, and asserts that the second
read adds zero FUSE read callbacks. Assert requested writeback cache is false.

- [ ] **Step 2: Run focused compile/unit validation**

Run:

```bash
cargo test -p studiofs-linux-fuse --no-run
cargo test -p studiofs-linux-fuse linux_read_only_kernel -- --nocapture
cargo clippy -p studiofs-linux-fuse --all-targets --locked -- -D warnings
```

Expected: compilation, tests, and clippy pass with no warnings.

- [ ] **Step 3: Run the mounted regression**

Run the ignored mounted test with the default cache policy and `/dev/fuse`
available:

```bash
cargo test -p studiofs-linux-fuse mounted_read_only_kernel_cache_reuses_stable_pages -- --ignored --nocapture
```

Expected: payload equality, zero second-read FUSE callbacks, clean unmount.

- [ ] **Step 4: Build release and run `studiofs-bench`**

Build `target/release/studiofs`, mount the real `test` volume with a fresh cache
directory under `target/perf/`, and run `target/bench/studiofs-bench` with cache
enabled, single-file layout, and a workload larger than the effective cache
window. Retain JSON/CSV samples and mount/runtime logs under the run directory.

- [ ] **Step 5: Record the result and commit**

Update `docs/perf/linux-cache-performance-result.md` with exact artifact paths,
bytes, throughput mean/min/CV, cache reuse evidence, errors, drain, and unmount
status. Commit code, tests, and result documentation together.
