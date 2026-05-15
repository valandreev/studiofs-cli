# Code Review: Startup Synchronization & Snapshot Replay Refinement

## Intent
The changes aim to harden the `PgMetaEngine` startup sequence by ensuring the local metadata state is fully synchronized with the PostgreSQL remote head and correctly identifying the event replay floor after a snapshot import.

## Context
- **Files Affected**: `crates/studiofs-meta/src/pg_engine.rs`
- **Key Symbols**: `PgMetaEngine::sync_metadata_to_current_head`, `PgMetaEngine::snapshot_replay_start_version`

## Review Summary
The implementation significantly improves the reliability of the local-first metadata engine during initial catch-up. The `snapshot_replay_start_version` logic correctly addresses potential gaps between snapshot data epochs and the advertised PG head. However, the catch-up loop in `sync_metadata_to_current_head` has some efficiency overhead and its error handling at the call site in `open()` is currently permissive.

---

## Detailed Findings

### 1. Inefficient Remote Head Queries (MEDIUM)
**Location**: `PgMetaEngine::sync_metadata_to_current_head` (New implementation)
**Issue**: The method calls `self.remote_metadata_head_version()` twice in each iteration of the 16-round loop. While `sync_metadata()` might take time, performing two network round-trips to Postgres for the same (or nearly same) version value is redundant.
**Impact**: Increases Postgres load and startup latency unnecessarily.
**Recommendation**: Cache the `confirmed_head` from the end of the previous iteration and use it as the `observed_head` for the next check.

### 2. Ignored Catch-up Failure during Startup (MEDIUM)
**Location**: `PgMetaEngine::open` (Line 2252 in `pg_engine.rs`)
**Issue**: The call to `engine.sync_metadata_to_current_head()` explicitly ignores errors with `let _ = ...`. 
**Impact**: If the synchronization "stalls" or fails to reach the head after 16 rounds, the system continues startup without any warning. If reaching the head is critical for correctness (as implied by the new aggressive catch-up logic), this failure should be logged or handled.
**Recommendation**: Add at least a `warn!` log if `sync_metadata_to_current_head()` returns an error, or reconsider if the error should be terminal for `open()`.

### 3. Snapshot Replay Floor Calculation (Correctness - HIGH)
**Location**: `PgMetaEngine::snapshot_replay_start_version`
**Analysis**: This is a very strong addition. It correctly prioritizes the `LOCAL_PG_VERSION_COUNTER` embedded within the snapshot itself. This ensures that even if a snapshot is "stale" compared to the PG head at the time of export, the engine will resume replaying events from the exact point the snapshot data was consistent.
**Verdict**: Correct and robust implementation.

### 4. Stall Detection Logic (LOW)
**Location**: `PgMetaEngine::sync_metadata_to_current_head`
**Analysis**: The check `if after == before` inside the catch-up loop correctly identifies a lack of progress. Given that `sync_metadata()` is supposed to fetch available events, a failure to advance when `confirmed_head > after` is a legitimate stall.
**Verdict**: Good defensive programming.

---

## Recommendations

1. **Optimize Catch-up Loop**:
   ```rust
   // Proposed optimization structure
   let mut current_head = self.remote_metadata_head_version()?;
   for _ in 0..MAX_STARTUP_CATCHUP_ROUNDS {
       if self.local_version.load(Ordering::SeqCst) >= current_head {
           return Ok(());
       }
       // ... sync_metadata() ...
       current_head = self.remote_metadata_head_version()?;
   }
   ```
2. **Improve Error Visibility**: Update `PgMetaEngine::open` to at least log a warning if the aggressive catch-up fails to reach the head.
3. **Documentation**: Add a brief doc comment to `MAX_STARTUP_CATCHUP_ROUNDS` explaining why 16 rounds was chosen as the threshold.
