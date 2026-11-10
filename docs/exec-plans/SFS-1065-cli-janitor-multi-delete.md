# Batch CLI janitor object-store deletes

This living ExecPlan follows `.agent/PLANS.md`. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.

## Purpose / Big Picture

SFS-1065 removes the default two-objects-per-second ceiling by deleting the canonical keys of a claimed batch in one S3 DeleteObjects request. PostgreSQL still claims and fences objects before deletion and confirms each outcome afterward. Only objects whose every key is independently confirmed absent may be finished as deleted.

## Progress

- [x] (2026-11-10) Read SFS-1065, inspected the clean `main` checkout, contracts, store and pipeline; baseline focused deletion-cap test passed (1 test).
- [x] Inspected OpenDAL 0.55 source and delegated API research, pipeline implementation and focused regression coverage.
- [x] Implement bounded batched deletion, partial-result confirmation and unsupported-provider fallback.
- [x] Verify partial failure, cap, cancellation, overload and fallback: CLI Janitor module 58 passed, 11 environment/benchmark tests ignored; data Janitor unit tests 2 passed. Scoped Clippy, fmt and diff checks passed.
- [x] (2026-11-10) Backblaze B2 live smoke passed (1 test): four keys deleted with one POST, zero scalar fallback DELETEs, protected sentinel intact; all nine scratch version/marker entries removed, zero remain.
- [ ] Record matched live throughput and safety with disposable PostgreSQL/RustFS when Docker is available.

## Surprises & Discoveries

OpenDAL 0.55's public deleter and raw deleter both expose a count of successful deletions, not per-key results. Its S3 implementation parses per-key errors internally. `close` repeatedly flushes buffered failures; using it would make partial-result recovery unbounded. Docker CLI is installed, but its configured OrbStack socket does not exist, so Docker-based live validation is currently unavailable. The actual shell is macOS zsh despite the supplied Windows instruction.

## Decision Log

- Decision: Flush each bounded batch once and accept success only when the returned count equals its input size. For partial/ambiguous outcomes, repeat idempotent single-key deletes to obtain independently attributable confirmations. Rationale: retain OpenDAL's signing, XML and retry implementation without inventing an S3 client or trusting an unknown partial outcome. Date: 2026-11-10.
- Decision: Detect batch HTTP 405/501 at the existing HTTP transport gate and remember lack of support across store clones; also recognize typed Unsupported. Rationale: OpenDAL 0.55 classifies S3 HTTP 501 as Unexpected, so kind alone misses unsupported providers. Date: 2026-11-10.
- Decision: A submitted batch is the admission unit; cancellation drains all its outcomes before claim settlement. The cap limits the number claimed to remaining confirmed deletion allowance. Rationale: an already-submitted DeleteObjects request cannot cancel individual keys safely. Date: 2026-11-10.

## Outcomes & Retrospective

Implementation and focused regression validation are complete. The 1,000-key/500-orphan synthetic smoke completed in 2,018 ms at 4 HTTP request starts/s with eight batch requests and at most one request in flight; this debug-profile fake-metadata result does not replace matched live acceptance. The user-authorized `.env` credentials were used only for a new random scratch prefix in Backblaze B2; the file was not modified. The live adapter smoke passed and its prefix was fully cleaned. Matched PostgreSQL/RustFS throughput and referenced-object safety against live PostgreSQL remain open; the B2 adapter smoke does not prove those.

## Context and Orientation

`crates/studiofs-data/src/janitor.rs` owns the OpenDAL adapter and adaptive HTTP request gate. `crates/studiofs/src/cli/janitor/pipeline.rs` groups candidates, checks grace and budgets, claims up to 64 objects, and settles fenced per-object outcomes. `crates/studiofs/src/cli/janitor.rs` connects the adapter and contains fake metadata and loopback S3 tests. Each object has a legacy key and a fragment key; both must be absent to finish as deleted. Claim fencing means a token identifies the current deletion owner and an old token cannot settle a new owner's work.

## Plan of Work

Add `delete_batch(&[String])` returning per-key results in input order. Use OpenDAL's capability limit, capped at 1,000 keys. Keep HTTP retries and adaptive pacing at the existing transport boundary. Bound adapter recovery to 35 seconds, leaving margin below the pipeline's 40-second batch timeout and 60-second claim lease. After batch failures use bounded single-key confirmation; on unsupported batch requests remember the paced single-DELETE fallback. Bound confirmations by the shared deadline and leave unconfirmed keys retryable.

Replace per-object remote calls with one call for the admitted claimed batch; map every key result back to its candidate. Preserve protected objects, terminal errors, overload accounting, queue deferral and PostgreSQL confirmation. Update loopback S3 to respond to DeleteObjects XML and verify the actual HTTP request count and key membership. Update cancellation expectations to drain the submitted batch.

## Concrete Steps

From `/Volumes/Storage/dev/studiofs`, run focused Cargo test filters (one filter per command), `cargo check -p studiofs-data`, `cargo clippy -p studiofs-data -p studiofs --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check`. Do not run workspace-wide suites. Update the performance documentation with actual evidence and explicitly mark live evidence unavailable if services remain absent.

## Validation and Acceptance

A healthy two-object batch should send one POST containing four canonical keys and confirm two objects. Partial failure must never confirm an object with an unconfirmed key. Unsupported DeleteObjects must fall back to paced single deletes and remember that decision. The delete cap must remain exact and cancellation must settle every admitted claim before returning. Throttles must increment transport counters and back off through the same HTTP gate.

For live acceptance, rebuild release CLI/tests and run `cargo test -p studiofs --bin studiofs --release live_cli_janitor_benchmark -- --ignored --nocapture` against its disposable Compose fixture, at 4 requests/s and the SFS-1064 matched parameters. Record objects/s above 2, zero referenced objects missing and zero orphans remaining. Run the focused live adversarial filters listed in `crates/studiofs/src/cli/janitor/tests/adversarial_live.rs` only with their isolated fixture. The user selected Backblaze B2 as the target production-provider class. The user explicitly authorized the empty `.env` bucket for this smoke. Keep all writes and exact-version cleanup inside one new random prefix; do not touch any other key or change bucket settings.

## Idempotence and Recovery

Backblaze B2 was selected as the production-provider class. B2 buckets are always versioned: a name-only delete adds a delete marker and retains older versions. The janitor's existing key-based deletion contract therefore confirms logical absence, not reclamation of historical versions. A tiny disposable B2 batch smoke must inspect version history as well as HEAD/list absence; explicit version deletion or lifecycle cleanup is outside SFS-1065. See [B2 bucket versions](https://www.backblaze.com/docs/cloud-storage-s3-compatible-api-bucket-versions) and [B2 Delete Objects](https://www.backblaze.com/apidocs/s3-delete-objects). Repeated name-only confirmations preserve logical absence but may add markers.


Delete is logically idempotent. Missing keys count as success. Failed confirmations leave the object retryable and defer its queue row; lost settlement is recovered by claim expiry. Repeated runs continue the persisted sweep. No schema or dependency changes are planned.

## Artifacts and Notes

B2 smoke command: `cargo test -p studiofs-data --test janitor_live janitor_live_s3_uses_multi_delete_and_cleans_exact_versions -- --ignored --nocapture` (1 passed, 4.35 seconds). Report: `target/janitor-benchmark/sfs1065-live-smoke.json`. The test instruments the real OpenDAL HTTP client to verify the batch method, then uses version-scoped cleanup; it does not mutate PostgreSQL or change bucket settings. Scoped `cargo clippy -p studiofs-data --test janitor_live -- -D warnings`, formatting and diff checks passed.

Baseline: `cargo test -p studiofs --bin studiofs delete_cap_counts_only_confirmed_successes_and_stops_listing -- --nocapture`: 1 passed. Docker: configured socket `/Users/val/.orbstack/run/docker.sock` unavailable.

## Interfaces and Dependencies

Reuse installed OpenDAL 0.55 and existing Tokio timeouts, cancellation token and PostgreSQL claim/settlement ports. Add no dependency or CLI flag. The store batch result vector is aligned with the input keys; missing or ambiguous confirmations must not become success.

Revision 2026-11-10: initial plan records inspected implementation and OpenDAL limitations before completing edits.

Revision 2026-11-10: recorded completed adapter and pipeline implementation, bounded single-key recovery after all ambiguous batch errors, and synthetic smoke evidence.

Revision 2026-11-10: recorded passing focused regression results and user-selected Backblaze B2 provider; matched live acceptance remains open.

Revision 2026-11-10: user authorized the empty B2 bucket configured in `.env`; preparing the isolated smoke without PostgreSQL mutations.

Revision 2026-11-10: recorded successful authorized Backblaze B2 smoke and exact-version cleanup; only Docker-dependent live acceptance remains pending.
