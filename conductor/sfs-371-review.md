# Code Review for sfs-371: E2E Scenario Isolation

## Summary of Intent
The changes implement a supervisor-child model for E2E runs to isolate individual scenarios into their own processes. This prevents a hang in one scenario from blocking the entire test sweep and allows for reliable timeouts and cleanup. Additionally, it addresses compiler warnings about missing documentation by adding `#![allow(missing_docs)]` to several crates.

---

## Analysis of Changes

### 1. E2E Runner Isolation (sfs-371)
The implementation of `__run-one` and the supervised execution logic in `runner.rs` correctly addresses the isolation goal. The parent process now manages timeouts and collects reports from child processes.

#### Critical Observations:
- **Child Process Execution:** The use of `executor.run()` in `run_scenario_child_with_timeout` captures the entire stdout/stderr into memory (`CommandOutput`). For long-running scenarios with verbose logging, this could lead to excessive memory usage.
- **Error Propagation:** If a child process exits with a non-zero status but fails to write a `scenario-report.json` (e.g., due to a panic or early exit), the parent constructs a fallback report. However, the logic for checking `output.status == 0` vs `-1` vs other non-zero values might mask specific failure modes.
- **Infra Mode Compatibility:** The `infra` mapping in `run_scenario_child_with_timeout` is hardcoded to a string match. While functional, it's a minor maintenance point if `InfraMode` variants change.

### 2. Documentation Recommendations
The user requested a fix for "missing docs". The current implementation adds `#![allow(missing_docs)]` to `lib.rs` of multiple crates:
- `studiofs-cache`
- `studiofs-data`
- `studiofs-fuse`
- `studiofs-scenario`
- `studiofs-vfs`
- `studiofs-winfsp`
- `studiofs-e2e` (in `main.rs` and `scenario.rs`)

**Recommendation:** Suppressing the warning is a "quick fix" but does not fulfill the intent of improving codebase maintainability. At a minimum, key public structs and traits should be documented.

---

## Detailed Review Comments

### `crates/studiofs-e2e/src/runner.rs`

#### [MEDIUM] Memory usage risk in child output capture
**Line 1399:** `let output = executor.run(&CommandSpec { ... })?;`
The current `CommandExecutor` implementation (likely `SystemCommandExecutor`) buffers all output into a `String`. For E2E tests generating massive logs, this can lead to OOM or performance degradation.
**Recommendation:** Implement streaming of child stdout/stderr to the parent's logs or directly to a file in the sandbox to keep the memory footprint constant.

#### [LOW] Brittle InfraMode string mapping
**Lines 1386-1391:**
```rust
match request.infra {
    InfraMode::Disabled => "disabled",
    InfraMode::Compose => "compose",
    InfraMode::ExternalEnv => "external-env",
}
```
**Recommendation:** Implement `Display` for `InfraMode` or use a dedicated method to ensure consistency between the CLI parser and the child process argument generation.

#### [MEDIUM] Ambiguous failure handling in child processes
**Lines 1419-1425:**
```rust
} else if output.status == 0 {
    return Err(RunnerError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!(
            "scenario child exited successfully without report: {}",
            report_path.display()
        ),
    )));
```
If the child process crashes (e.g., SEGFAULT) or is killed by an external signal (not the parent's timeout), the exit status might be non-zero, but `report_path.exists()` would be false. The current logic puts this into the "Failed" bucket with the stderr, which is acceptable but could be more explicit about "abnormal termination".

---

### Documentation (Across all crates)

#### [HIGH] Suppressing `missing_docs` instead of fixing them
**Multiple files (`lib.rs`):** `+#![allow(missing_docs)]`
This change technically silences the compiler, but it hides the lack of documentation rather than resolving it.
**Recommendation:** For a project of this complexity, public APIs in the VFS, Cache, and Data layers should have at least basic doc comments. I recommend removing the `allow` and adding documentation to the most critical public symbols.

---

## Summary of Recommendations

1. **Streaming Logs:** Refactor the child process execution to stream output rather than buffering it in memory.
2. **Infrastructure for Docs:** Instead of a global `#![allow(missing_docs)]`, consider documenting the main entry points for each crate.
3. **Internal CLI Stability:** Use the existing clap metadata to generate the subcommand arguments for `__run-one` to avoid manual string mapping.
4. **Child Report Reliability:** Ensure the child process uses a `Drop` guard or similar mechanism to attempt writing a "Crash" report even on panic.
