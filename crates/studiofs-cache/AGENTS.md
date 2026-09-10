# `studiofs-cache` agent contract

This crate owns local staging, cached reads, pending-upload bookkeeping, and
upload helpers. Read the root `AGENTS.md`, `docs/cache.md`, `docs/architecture.md`,
and the relevant local-first contract section before editing.

- Local cache and staged payload state are part of the local-first path. Do not
  make foreground reads depend on remote upload completion or peer visibility.
- Preserve pending-upload identity, pinning, replay, and shutdown ownership.
  Background upload tasks must not retain metadata/runtime resources past the
  owning lifecycle boundary.
- Keep cache invalidation and writer observation generations coherent: a result
  that crossed invalidation must not be recached, and local dirty data must not be
  discarded merely because remote state is unavailable.
- Use bounded waits and observable state for upload-idle tests. Do not stabilize
  a race with fixed sleeps or unbounded polling.
- Run focused cache tests and crate check/Clippy. Remote S3 validation is ignored
  infrastructure evidence and must be reported separately.

Use `docs/cache.md` and `docs/tests.md` for the current runtime and test details.
