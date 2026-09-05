//! Platform delivery contract for committed local-view invalidation scopes.

/// Immediate admission result, never proof of native cache invalidation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidationAcceptance {
    /// The adapter retained all scopes for asynchronous processing.
    Queued,
    /// The adapter cannot establish all requested effects.
    Unsupported,
    /// Work could not be retained, for example during shutdown or saturation.
    Failed,
}
