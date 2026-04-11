//! Cache v2 — local block backend implementations and supporting helpers.

pub mod local_block_backend;

pub use local_block_backend::{
    LocalBlockBackend, LocalBlockBackendError, LocalBlockLifecycleState,
};
use uuid::Uuid;

/// Derive a stable 16-byte local backend volume UUID from a format UUID string.
///
/// Accepts plain UUID strings and falls back to deterministic UUIDv5 over the
/// provided bytes so legacy/non-UUID values still map consistently.
pub fn derive_backend_volume_uuid(volume_uuid: &str) -> [u8; 16] {
    Uuid::parse_str(volume_uuid)
        .unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_URL, volume_uuid.as_bytes()))
        .into_bytes()
}
