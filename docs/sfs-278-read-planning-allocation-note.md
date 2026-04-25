# SFS-278 Read Planning Allocation Note

`DataReader::claim_unresolved_ranges` now uses reusable `SmallVec<[ReadRange; 8]>`
buffers for unresolved and claimed range lists. The common fragmented
read-planning case stays in inline storage, while unusually fragmented requests
reuse spilled heap capacity across repeated claim passes.

The pending-overlay block candidate list also uses a sorted/deduplicated
`SmallVec<[u32; 16]>`, avoiding `BTreeSet` node allocation for the common small
unresolved-range case while preserving deterministic ascending block order.

Targeted verification:

```powershell
cargo test -p studiofs-vfs claim_unresolved_ranges_keeps_common_fragmented_case_inline
cargo test -p studiofs-vfs claim_unresolved_ranges_reuses_spilled_buffers_for_fragmented_reads
```

The first test exercises a fragmented two-range claim and asserts both resulting
range lists do not spill to heap storage. The second test exercises a >8-range
claim and asserts spilled buffers retain capacity across repeated passes.
Existing mixed-source read-path tests cover precedence and segment construction
semantics.
