# `studiofs-e2e` agent contract

This crate runs real multi-client mounted scenarios and owns process, Docker,
cleanup, and artifact reporting. Read the root `AGENTS.md`, `docs/tests.md`,
`docs/development.md`, and `docs/contributing.md` before changing the runner.

- Start with `cargo run -p studiofs-e2e -- list` and use a registered scenario;
  inspect the current runner contract instead of inventing a scenario name.
- Rebuild the product binary before counting a rerun. Keep separate client roots,
  cache directories, logs, failure reports, validation trees, and Docker
  diagnostics under `target/e2e-runs/<run-id>/`.
- Synchronize on observable state with bounded polling. Fixed coordination sleeps,
  silent retries, and cleanup that hides the first failure are invalid evidence.
- A focused runner contract test proves the harness contract only. Full mounted
  scenario acceptance requires the requested scenario set, the actual native
  driver/services, and retained artifacts; report preflight unavailability as an
  environment boundary.
- Preserve graceful unmount, forced-kill, restart/remount, and cleanup semantics.
  Do not weaken teardown to make a later scenario pass.

Use `docs/tests.md` for the mounted command matrix and artifact interpretation.
