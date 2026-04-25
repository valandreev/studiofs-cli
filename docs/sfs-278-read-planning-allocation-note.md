# SFS-278 Read Planning Allocation Note

`DataReader::claim_unresolved_ranges` now uses `SmallVec<[ReadRange; 8]>`
for unresolved and claimed range lists. The common fragmented read-planning
case stays in inline storage while preserving heap fallback for unusually
fragmented requests.

Targeted verification:

```powershell
cargo test -p studiofs-vfs claim_unresolved_ranges_keeps_common_fragmented_case_inline
```

The test exercises a fragmented two-range claim and asserts both resulting
range lists do not spill to heap storage. Existing mixed-source read-path tests
cover precedence and segment construction semantics.
