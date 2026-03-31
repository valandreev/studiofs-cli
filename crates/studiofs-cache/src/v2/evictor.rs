//! Watermark-driven background eviction loop.
//!
//! The per-operation eviction path (evict on `put` when full) lives inside
//! [`super::block_store::BlockStore`].  This module will contain an optional
//! background task that proactively evicts down to a low-watermark when the
//! cache fills up, reducing latency spikes on large puts.
//!
//! Planned for a follow-up issue; wired in SFS-136 only as a stub.
