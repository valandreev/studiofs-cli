//! Typed version identities of the persisted boundaries (ADR 0018, SFS-1003).
//!
//! Each persisted boundary has its own identity, so a change bumps only the
//! identity it touches. The values here are plain data with no PostgreSQL or
//! redb types, so the capability evaluator and other metadata backends can
//! reuse them. Bump rules are in `docs/persisted-format-compatibility.md`
//! ("Version-bump rules").
//!
//! Identities owned by other crates keep their own constants and checks; this
//! table only points at them:
//!
//! | Identity | Owner constant |
//! |---|---|
//! | Local payload index format 2 / entry encoding 3 | `studiofs-cache` `payload_index.rs`: `INDEX_FORMAT_VERSION`, `ENTRY_ENCODING_VERSION` |
//! | Snapshot artifact format 1 / schema 1 | `studiofs_vfs::snapshot_worker::SNAPSHOT_ARTIFACT_FORMAT_VERSION`, `SNAPSHOT_ARTIFACT_SCHEMA_VERSION` |
//! | Dump format 4 | `studiofs` CLI `dump_load.rs`: `MetadataDump::dump_version` |
//! | STFS payload object v2 / v3 | `studiofs_data::object::VERSION`, `VERSION_V3` |
//! | SFEC envelope v1 | `studiofs_data::envelope::SFEC_VERSION` |
//! | Remote payload encryption config 1 | [`crate::schema::CURRENT_ENCRYPTION_FORMAT_VERSION`] |

use crate::engine::{MetaError, MetaResult};

/// Release of the running binary. Informative only: it is never a storage
/// compatibility gate; the identities below are.
pub const CLIENT_RELEASE: &str = env!("CARGO_PKG_VERSION");

/// One persisted boundary's version identity and the range this binary reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct VersionIdentity {
    /// Boundary name used in diagnostics.
    pub name: &'static str,
    /// Version this binary writes.
    pub current: u16,
    /// Oldest version this binary still reads.
    pub min_supported: u16,
}

impl VersionIdentity {
    /// Whether this binary reads `found`.
    #[must_use]
    pub const fn supports(&self, found: u16) -> bool {
        found >= self.min_supported && found <= self.current
    }

    /// Like [`Self::supports`] for a PostgreSQL `SMALLINT`; negative values are unsupported.
    #[must_use]
    pub fn supports_i16(&self, found: i16) -> bool {
        u16::try_from(found).is_ok_and(|found| self.supports(found))
    }

    pub(crate) const fn current_pg_smallint(&self) -> i16 {
        assert!(self.current <= i16::MAX as u16);
        self.current.cast_signed()
    }

    /// Typed refusal of an unsupported persisted version.
    pub fn check(&self, found: u16) -> MetaResult<()> {
        if self.supports(found) {
            return Ok(());
        }
        Err(MetaError::UnsupportedFormatVersion {
            found,
            expected: self.current,
        })
    }
}

/// Shared volume format: `FormatInfo.format_version` in
/// `metadata_state.format_info`, the `metadata_format_version` column (volume
/// protocol), the bootstrap checkpoint descriptor and the publication predicate.
pub const SHARED_VOLUME_FORMAT: VersionIdentity = VersionIdentity {
    name: "shared volume format",
    current: 6,
    min_supported: 6,
};

/// Physical PostgreSQL DDL version of the existing `payload_gc` schema component.
pub const POSTGRES_PAYLOAD_GC_SCHEMA: VersionIdentity = VersionIdentity {
    name: "PostgreSQL payload GC schema",
    current: 2,
    min_supported: 2,
};

/// Database-wide PostgreSQL `core` schema component: the 0.2.5 baseline is
/// version 1 and each later migration in
/// `pg_engine::migrations::MIGRATIONS` raises it by one (SFS-1005).
pub const POSTGRES_CORE_SCHEMA: VersionIdentity = VersionIdentity {
    name: "PostgreSQL core schema",
    current: if cfg!(feature = "test-successor") {
        2
    } else {
        1
    },
    min_supported: 1,
};

/// Per-volume write requirement (`metadata_state.write_requirement`, SFS-1007).
/// Publication succeeds only while the volume's value is at most `current`, in
/// the same UPDATE that bumps the volume version; Activate of a write
/// capability raises the volume's value to fence older writers without
/// touching the read gate.
pub const SHARED_WRITE_REQUIREMENT: VersionIdentity = VersionIdentity {
    name: "shared write requirement",
    current: 1,
    min_supported: 1,
};

/// Local redb metadata layout, recorded in the redb `SETTINGS_TABLE["format"]`
/// header. Both checks use `FormatInfo.format_version` until SFS-1012 adds a
/// dedicated local layout marker, so their supported ranges must agree.
pub const LOCAL_METADATA_LAYOUT: VersionIdentity = VersionIdentity {
    name: "local redb metadata layout",
    current: 6,
    min_supported: 6,
};

const _: () = assert!(
    LOCAL_METADATA_LAYOUT.current == SHARED_VOLUME_FORMAT.current
        && LOCAL_METADATA_LAYOUT.min_supported == SHARED_VOLUME_FORMAT.min_supported,
    "redb and the shared volume use the same format field until SFS-1012"
);

/// `MetaEvent` wire envelope version (`SFS-EVT`); tags are append-only.
pub const EVENT_CODEC_WIRE: VersionIdentity = VersionIdentity {
    name: "event codec wire",
    current: crate::events::META_EVENT_WIRE_VERSION as u16,
    min_supported: crate::events::META_EVENT_WIRE_VERSION as u16,
};

/// Serving checkpoint byte format (formats 1–4 decode).
pub const SERVING_CHECKPOINT_FORMAT: VersionIdentity = VersionIdentity {
    name: "serving checkpoint format",
    current: 4,
    min_supported: 1,
};

/// Serving checkpoint serving-state schema.
pub const SERVING_CHECKPOINT_SCHEMA: VersionIdentity = VersionIdentity {
    name: "serving checkpoint schema",
    current: 1,
    min_supported: 1,
};

/// Every identity owned by this crate, in report order.
pub const IDENTITIES: &[VersionIdentity] = &[
    SHARED_VOLUME_FORMAT,
    POSTGRES_PAYLOAD_GC_SCHEMA,
    LOCAL_METADATA_LAYOUT,
    EVENT_CODEC_WIRE,
    SERVING_CHECKPOINT_FORMAT,
    SERVING_CHECKPOINT_SCHEMA,
    POSTGRES_CORE_SCHEMA,
    SHARED_WRITE_REQUIREMENT,
];

/// Machine-readable report of this binary's identities, printed by
/// `studiofs internal version-identities` and diffed by
/// `tools/test_successor/build.py` (SFS-1131).
#[must_use]
pub fn identity_report() -> serde_json::Value {
    let report = serde_json::json!({
        "client_release": CLIENT_RELEASE,
        "identities": IDENTITIES,
        "successor_changes": [],
    });
    #[cfg(feature = "test-successor")]
    let report = {
        let mut report = report;
        report["successor_build"] = crate::test_successor::BUILD_MARKER.into();
        report["successor_changes"] = serde_json::json!(crate::test_successor::CHANGES);
        report
    };
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_identity_range_is_inclusive_and_typed() {
        assert!(SERVING_CHECKPOINT_FORMAT.supports(1));
        assert!(SERVING_CHECKPOINT_FORMAT.supports(4));
        assert!(!SERVING_CHECKPOINT_FORMAT.supports(0));
        assert!(!SERVING_CHECKPOINT_FORMAT.supports(5));
        assert!(SERVING_CHECKPOINT_SCHEMA.supports_i16(1));
        assert!(!SERVING_CHECKPOINT_SCHEMA.supports_i16(2));
        assert!(!SERVING_CHECKPOINT_SCHEMA.supports_i16(-1));
        assert!(matches!(
            SHARED_VOLUME_FORMAT.check(7),
            Err(MetaError::UnsupportedFormatVersion {
                found: 7,
                expected: 6
            })
        ));
    }

    #[test]
    fn version_identities_keep_current_baseline_values() {
        // SFS-1003 splits identities without bumping any of them.
        assert_eq!(SHARED_VOLUME_FORMAT.current, 6);
        assert_eq!(LOCAL_METADATA_LAYOUT.current, 6);
        assert_eq!(crate::schema::CURRENT_FORMAT_VERSION, 6);
        assert_eq!(EVENT_CODEC_WIRE.current, 1);
        assert_eq!(POSTGRES_PAYLOAD_GC_SCHEMA.current, 2);
        assert!(!POSTGRES_PAYLOAD_GC_SCHEMA.supports(1));
        assert!(!POSTGRES_PAYLOAD_GC_SCHEMA.supports(3));
        assert_eq!(
            i16::try_from(SERVING_CHECKPOINT_FORMAT.current).unwrap(),
            crate::snapshot::ServingCheckpoint::FORMAT_VERSION
        );
        assert!(!CLIENT_RELEASE.is_empty());
    }

    #[test]
    fn identity_report_lists_every_identity_with_its_range() {
        let report = identity_report();
        let identities = report["identities"].as_array().unwrap();
        assert_eq!(identities.len(), IDENTITIES.len());
        assert_eq!(identities[0]["name"], SHARED_VOLUME_FORMAT.name);
        assert_eq!(identities[4]["current"], SERVING_CHECKPOINT_FORMAT.current);
        assert_eq!(identities[4]["min_supported"], 1);
        assert_eq!(
            report.get("successor_build").is_some(),
            cfg!(feature = "test-successor")
        );
    }
}
