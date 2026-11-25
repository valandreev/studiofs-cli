//! Pins the capability evaluator's payload index identity to the cache.

use studiofs_cache::runtime::cache_index::{INDEX_FORMAT_VERSION, LEGACY_INDEX_FORMAT_VERSION};
use studiofs_meta::compat::LOCAL_PAYLOAD_INDEX_FORMAT;

#[test]
fn compat_payload_index_identity_matches_cache_constants() {
    assert_eq!(
        u64::from(LOCAL_PAYLOAD_INDEX_FORMAT.current),
        INDEX_FORMAT_VERSION
    );
    assert_eq!(
        u64::from(LOCAL_PAYLOAD_INDEX_FORMAT.min_supported),
        LEGACY_INDEX_FORMAT_VERSION
    );
}
