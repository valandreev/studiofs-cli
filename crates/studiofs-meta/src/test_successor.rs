//! Compiled test successor (SFS-1131): the registry of declared changes.
//!
//! The `test-successor` feature builds a binary that plays a newer release
//! for the forward-compatibility matrix (SFS-1019, SFS-1020). Each format
//! issue adds its change behind `#[cfg(feature = "test-successor")]` in the
//! owning code and registers one [`SuccessorChange`] in [`CHANGES`]. The
//! successor only adds behavior; it never edits a baseline encoder.
//!
//! `tools/test_successor/build.py` builds the binary, diffs its
//! `studiofs internal version-identities` report against the baseline build,
//! and writes the manifest. Release builds refuse the feature at compile time,
//! and `tools/release/package.py` refuses a binary that embeds
//! [`BUILD_MARKER`].

#[cfg(not(debug_assertions))]
compile_error!(
    "the test-successor feature is test-only and must not be built with a release profile"
);

/// Bytes embedded only in successor binaries; release packaging rejects them.
pub const BUILD_MARKER: &str = "STUDIOFS-TEST-SUCCESSOR-BUILD";

/// Kind of declared change; the matrix needs at least one of each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    /// Additive PostgreSQL migration (SFS-1005).
    PostgresMigration,
    /// New `MetaEvent` kind (SFS-1009).
    EventKind,
    /// New serving checkpoint format (SFS-1011).
    CheckpointFormat,
    /// New local redb metadata or payload-index format (SFS-1012).
    LocalFormat,
}

/// Whether a volume can return to the baseline after the change is activated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Rollback {
    /// A documented deactivate step restores baseline compatibility.
    Reversible,
    /// No supported rollback after activation.
    Irreversible,
}

/// A version identity the successor reports with a different `current`.
/// `baseline` 0 means the baseline binary does not report the identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct IdentityBump {
    /// `VersionIdentity::name` as reported.
    pub identity: &'static str,
    /// `current` in the baseline binary.
    pub baseline: u16,
    /// `current` in the successor binary.
    pub successor: u16,
}

/// One declared protocol or format change of the successor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct SuccessorChange {
    /// Stable identifier, unique in [`CHANGES`].
    pub id: &'static str,
    /// Linear issue that owns the change.
    pub issue: &'static str,
    /// Change kind.
    pub kind: ChangeKind,
    /// Rollback class.
    pub rollback: Rollback,
    /// Identity the change bumps, if any; a new event tag bumps none.
    pub bump: Option<IdentityBump>,
}

/// Declared successor changes. Format issues append their entries here.
pub const CHANGES: &[SuccessorChange] = &[];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::IDENTITIES;

    #[test]
    fn declared_changes_match_compiled_identities() {
        let mut ids: Vec<_> = CHANGES.iter().map(|change| change.id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), CHANGES.len(), "change ids must be unique");
        for bump in CHANGES.iter().filter_map(|change| change.bump) {
            let identity = IDENTITIES
                .iter()
                .find(|identity| identity.name == bump.identity)
                .unwrap_or_else(|| panic!("unknown identity {}", bump.identity));
            assert_eq!(identity.current, bump.successor, "{}", bump.identity);
            assert_ne!(bump.baseline, bump.successor, "{}", bump.identity);
        }
    }

    #[test]
    fn successor_report_carries_marker_and_changes() {
        let report = crate::version::identity_report();
        assert_eq!(report["successor_build"], BUILD_MARKER);
        assert_eq!(
            report["successor_changes"].as_array().unwrap().len(),
            CHANGES.len()
        );
    }
}
