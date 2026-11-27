//! `studiofs compatibility <volume>`: a read-only report of what this client can
//! do with a volume. `studiofs upgrade <volume> --dry-run` appends the same
//! report, and `studiofs status` prints its local-only lines. Local redb is only
//! opened read-only, so a mounted volume is fine. No DSN, key or other secret is
//! printed.

use anyhow::{Context, anyhow, bail};
use clap::Args;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use studiofs_meta::client_sessions::ClientSession;
use studiofs_meta::compat::{
    ClientCapabilities, Compatibility, Operation, ReasonCode, VolumeRequirements,
};
use studiofs_meta::pg_engine::migrations::{
    Migration, RecordedComponent, registered_schema_status,
};
use studiofs_meta::pg_engine::{read_client_sessions, read_volume_protocol};
use studiofs_meta::schema::FormatInfo;
use studiofs_meta::version::{LOCAL_METADATA_LAYOUT, SHARED_WRITE_REQUIREMENT};
use studiofs_meta::volume_protocol::{
    CutoverLocalState, ProtocolRejection, VolumeProtocol, missing_capabilities_for_session,
};
use studiofs_meta::{MetaError, RedbMetaEngine};
use tokio_postgres::GenericClient;

const SESSION_ACTION: &str = "upgrade or unmount that client";
const UNPUBLISHED_ACTION: &str = "do not reinitialize or clear this cache; mount it with a compatible release so it publishes, or keep it until upgraded";

#[derive(Args, Debug)]
pub struct CompatibilityArgs {
    /// Locally discovered volume; read-only, so it may stay mounted.
    pub name: String,
}

/// Local redb facts from read-only snapshots.
pub(crate) struct LocalFacts {
    pub(crate) format: FormatInfo,
    pub(crate) state: CutoverLocalState,
    /// Local store identities by `VersionIdentity::name`, as `evaluate` expects.
    pub(crate) stores: BTreeMap<String, u16>,
}

/// Shared PostgreSQL facts from one bounded block.
pub(crate) struct SharedFacts {
    pub(crate) recorded: Vec<RecordedComponent>,
    pub(crate) pending: Vec<Migration>,
    /// `None` when the volume has no shared row.
    pub(crate) shared_format: Option<u16>,
    pub(crate) remote_head: Option<i64>,
    /// `None` when no policy is stored, or when schema migrations are pending.
    pub(crate) policy: Option<VolumeProtocol>,
    /// `None` while schema migrations are pending; the session table may not exist yet.
    pub(crate) sessions: Option<Vec<ClientSession>>,
}

pub(crate) fn read_local_facts(path: &Path) -> anyhow::Result<LocalFacts> {
    let (format, state) = RedbMetaEngine::read_upgrade_and_cutover_state_readonly(path)
        .context("Cannot read local metadata; nothing was changed")?;
    let layout = RedbMetaEngine::read_local_revision(path)
        .context("Cannot read the local metadata layout; nothing was changed")?;
    Ok(LocalFacts {
        format,
        state,
        stores: BTreeMap::from([(LOCAL_METADATA_LAYOUT.name.to_owned(), layout)]),
    })
}

/// Reads the shared side of the report. `repair_schema` matches `upgrade --repair-schema`.
pub(crate) async fn read_shared_facts<C: GenericClient + Sync>(
    client: &C,
    volume: i64,
    repair_schema: bool,
) -> anyhow::Result<SharedFacts> {
    let (recorded, pending) = registered_schema_status(client, repair_schema).await?;
    // Backend error text may echo connection data; keep only the operation.
    let row = client
        .query_opt(
            "SELECT metadata_format_version, version FROM metadata_state WHERE vol=$1",
            &[&volume],
        )
        .await
        .map_err(|_| anyhow!("reading the shared volume format failed"))?;
    let (shared_format, remote_head) = match row {
        Some(row) => (
            row.get::<_, Option<i16>>(0)
                .and_then(|format| u16::try_from(format).ok()),
            Some(row.get::<_, i64>(1)),
        ),
        None => (None, None),
    };
    let (policy, sessions) = if pending.is_empty() {
        let policy = match read_volume_protocol(client, volume).await {
            Ok(policy) => Some(policy),
            Err(MetaError::VolumeProtocolRejected(ProtocolRejection::Missing)) => None,
            Err(_) => bail!(
                "reading the volume protocol failed; it may have been written by a newer StudioFS"
            ),
        };
        let sessions = read_client_sessions(client, volume)
            .await
            .map_err(|_| anyhow!("reading client sessions failed"))?;
        (policy, Some(sessions))
    } else {
        (None, None)
    };
    Ok(SharedFacts {
        recorded,
        pending,
        shared_format,
        remote_head,
        policy,
        sessions,
    })
}

fn or_unknown<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(|| "unknown".to_owned(), |value| value.to_string())
}

fn add_action(actions: &mut Vec<String>, action: &str) {
    if !actions.iter().any(|known| known == action) {
        actions.push(action.to_owned());
    }
}

/// Renders the report from already-read facts. Pure: it prints numbers and the
/// typed policy, never connection data or keys.
pub(crate) fn format_report(
    client: &ClientCapabilities,
    build: &str,
    local_format: u16,
    local: &CutoverLocalState,
    local_stores: &BTreeMap<String, u16>,
    shared: &SharedFacts,
) -> String {
    let mut lines = Vec::new();
    let mut actions = Vec::new();
    let effective = shared
        .policy
        .clone()
        .unwrap_or_else(VolumeProtocol::baseline);
    // A client that publishes below the policy's write requirement is fenced, even
    // though the capability evaluator does not know that rule.
    let write_newer = effective.write_requirement > SHARED_WRITE_REQUIREMENT.current;
    let schema = shared
        .recorded
        .iter()
        .filter_map(|row| Some((row.component.clone(), u16::try_from(row.version).ok()?)))
        .collect();
    let decision = shared.shared_format.map(|format| {
        effective.compatibility_decision(
            client,
            VolumeRequirements {
                format,
                schema_components: schema,
                ..VolumeRequirements::baseline()
            },
            Some(effective.requirements.checkpoint),
            local_stores,
        )
    });

    // Operation -> (reason and subject, action) for every blocked operation.
    let mut blocked: Vec<(Operation, String, &'static str)> = Vec::new();
    if let Some(decision) = &decision {
        for operation in Operation::ALL {
            if let Some(refusal) = decision.refusal(operation) {
                let detail = format!("{} ({})", refusal.reason.as_str(), refusal.subject);
                blocked.push((operation, detail, refusal.reason.action()));
            } else if operation == Operation::Publication && write_newer {
                let detail = format!("write_requirement_newer ({})", effective.write_requirement);
                // Same upgrade action as a missing write capability.
                blocked.push((
                    operation,
                    detail,
                    ReasonCode::WriteCapabilityMissing.action(),
                ));
            }
        }
    }
    for (_, _, action) in &blocked {
        add_action(&mut actions, action);
    }

    let (overall, overall_reason) = match &decision {
        None => (
            "unknown",
            Some("shared volume metadata is missing".to_owned()),
        ),
        Some(decision) => match (&decision.reason, write_newer) {
            (Some(refusal), _) => (
                decision.compatibility.as_str(),
                Some(format!("{} ({})", refusal.reason.as_str(), refusal.subject)),
            ),
            (None, true) => (
                Compatibility::CompatibleRo.as_str(),
                Some(format!(
                    "write_requirement_newer ({})",
                    effective.write_requirement
                )),
            ),
            (None, false) => (decision.compatibility.as_str(), None),
        },
    };

    lines.push("Client".to_owned());
    lines.push(format!("  Build:              {build}"));
    lines.push(format!(
        "  Volume format:      {}..={}",
        client.volume_format.min_supported, client.volume_format.current
    ));
    lines.push(format!(
        "  Write requirement:  up to {}",
        SHARED_WRITE_REQUIREMENT.current
    ));

    lines.push("Volume".to_owned());
    lines.push(format!("  Local format:       {local_format}"));
    lines.push(format!(
        "  Shared format:      {}",
        or_unknown(shared.shared_format)
    ));
    let policy_text = if !shared.pending.is_empty() {
        format!(
            "not read; {} schema migration(s) pending, run `studiofs upgrade`",
            shared.pending.len()
        )
    } else if shared.policy.is_none() {
        "none recorded; `studiofs upgrade` adopts the 0.2.5 baseline".to_owned()
    } else {
        format!(
            "epoch {}, write requirement {} (publishes up to {})",
            effective.epoch, effective.write_requirement, SHARED_WRITE_REQUIREMENT.current
        )
    };
    lines.push(format!("  Policy:             {policy_text}"));

    lines.push("Operations".to_owned());
    for operation in Operation::ALL {
        let state = match (&decision, blocked.iter().find(|(op, ..)| *op == operation)) {
            (None, _) => "not evaluated: the shared volume is missing".to_owned(),
            (Some(_), None) => "allowed".to_owned(),
            (Some(_), Some((_, detail, action))) => format!("blocked: {detail} — {action}"),
        };
        lines.push(format!("  {:<12} {state}", operation.as_str()));
    }

    lines.push("Sessions".to_owned());
    match &shared.sessions {
        None => lines.push("  not read: schema migrations pending".to_owned()),
        Some(sessions) => {
            let active = sessions.iter().filter(|session| session.active).count();
            lines.push(format!(
                "  active {active}, expired {}",
                sessions.len() - active
            ));
            for session in sessions.iter().filter(|session| session.active) {
                let mut problems = Vec::new();
                if session.policy_epoch < i64::try_from(effective.epoch).unwrap_or(i64::MAX) {
                    problems.push(format!(
                        "stale epoch {} (current {})",
                        session.policy_epoch, effective.epoch
                    ));
                }
                let missing = missing_capabilities_for_session(session, &effective);
                if !missing.is_empty() {
                    problems.push(format!("missing capabilities: {}", missing.join(", ")));
                }
                let short_id = &session.session_id.to_string()[..8];
                if problems.is_empty() {
                    lines.push(format!(
                        "  {short_id} build {} epoch {}: ok",
                        session.build, session.policy_epoch
                    ));
                } else {
                    lines.push(format!(
                        "  {short_id} build {} epoch {}: {} — {SESSION_ACTION}",
                        session.build,
                        session.policy_epoch,
                        problems.join("; ")
                    ));
                    add_action(&mut actions, SESSION_ACTION);
                }
            }
        }
    }

    lines.push("Local".to_owned());
    lines.push(format!(
        "  Layout:             {}",
        or_unknown(local_stores.get(LOCAL_METADATA_LAYOUT.name))
    ));
    lines.push(format!(
        "  Unpublished events: {}",
        local.unpublished_events
    ));
    lines.push(format!(
        "  Local version:      {} (shared head {})",
        or_unknown(local.local_version),
        or_unknown(shared.remote_head)
    ));
    if local.unpublished_events > 0 {
        lines.push(
            "  Unsafe pending:     yes; these changes are not on the shared volume yet".to_owned(),
        );
        add_action(&mut actions, UNPUBLISHED_ACTION);
    }

    lines.push(format!("Overall: {overall}"));
    if let Some(reason) = overall_reason {
        lines.push(format!("  reason: {reason}"));
    }
    lines.push("Actions".to_owned());
    if actions.is_empty() {
        lines.push("  none".to_owned());
    }
    for action in &actions {
        lines.push(format!("  - {action}"));
    }
    lines.join("\n") + "\n"
}

/// Local-only lines for `studiofs status`: no PostgreSQL or object-store call.
pub(crate) fn format_local_status(
    client: &ClientCapabilities,
    volume: &str,
    local_format: u16,
    unpublished_events: u64,
) -> Vec<String> {
    let range = client.volume_format;
    let format = if range.supports(local_format) {
        format!(
            "{local_format} (this client supports {}..={})",
            range.min_supported, range.current
        )
    } else {
        format!(
            "{local_format} (outside this client's range {}..={})",
            range.min_supported, range.current
        )
    };
    vec![
        format!("  Volume format:       {format}"),
        format!("  Unpublished events:  {unpublished_events}"),
        format!("  Shared check:        run `studiofs compatibility {volume}`"),
    ]
}

impl CompatibilityArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        let path =
            super::utils::get_default_meta_dir(&self.name)?.join(format!("{}.db", self.name));
        if !path.is_file() {
            bail!(
                "Local volume metadata is missing; run compatibility where this volume already has a local cache."
            );
        }
        let local = read_local_facts(&path)?;
        let pg = super::upgrade::postgres_binding(&local.format, &self.name)?;
        let local = &local;
        let runtime = tokio::runtime::Runtime::new()?;
        let report = runtime
            .block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(45),
                    super::init::with_postgres_client(&pg.url, |client| async move {
                        let shared = read_shared_facts(&client, pg.volume_id, false).await?;
                        Ok(format_report(
                            &ClientCapabilities::current(),
                            env!("STUDIOFS_BUILD_VERSION"),
                            local.format.format_version,
                            &local.state,
                            &local.stores,
                            &shared,
                        ))
                    }),
                )
                .await
            })
            .context("Compatibility check timed out; nothing was changed")?
            .context("Compatibility check failed; nothing was changed")?;
        print!("{report}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_meta::client_sessions::SessionCapabilities;
    use studiofs_meta::schema::{FormatInfoDataStore, PostgresConfig};
    use studiofs_meta::version::POSTGRES_PAYLOAD_GC_SCHEMA;
    use studiofs_meta::volume_protocol::FeatureState;

    const FUTURE_WRITER: &str = "table.future_writer.v1";

    fn current_stores() -> BTreeMap<String, u16> {
        BTreeMap::from([(
            LOCAL_METADATA_LAYOUT.name.to_owned(),
            LOCAL_METADATA_LAYOUT.current,
        )])
    }

    fn healthy_shared() -> SharedFacts {
        SharedFacts {
            recorded: vec![RecordedComponent {
                component: POSTGRES_PAYLOAD_GC_SCHEMA.name.to_owned(),
                version: i32::from(POSTGRES_PAYLOAD_GC_SCHEMA.current),
                checksum: None,
            }],
            pending: Vec::new(),
            shared_format: Some(7),
            remote_head: Some(10),
            policy: Some(VolumeProtocol::baseline()),
            sessions: Some(Vec::new()),
        }
    }

    /// Session that predates the policy epoch and lacks the future writer.
    fn stale_session() -> ClientSession {
        let client = ClientCapabilities::current();
        let now = chrono::Utc::now();
        ClientSession {
            session_id: uuid::Uuid::parse_str("3f2a9c1e-0000-4000-8000-000000000001").unwrap(),
            build: "0.2.4".to_owned(),
            capabilities: SessionCapabilities {
                volume_format_min: 7,
                volume_format_max: 7,
                write_requirement_max: 1,
                read: client.read.iter().map(|name| (*name).to_owned()).collect(),
                write: client.write.iter().map(|name| (*name).to_owned()).collect(),
                maintain: client
                    .maintain
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect(),
            },
            policy_epoch: 1,
            fencing_token: 1,
            heartbeat_at: now,
            lease_expires_at: now + chrono::Duration::seconds(60),
            active: true,
        }
    }

    /// Policy whose write set names a capability this client does not declare,
    /// with a session that is also stale and unsupported.
    fn blocked_shared() -> SharedFacts {
        let mut policy = VolumeProtocol::baseline();
        policy.epoch = 3;
        policy.requirements.write.insert(FUTURE_WRITER.to_owned());
        policy
            .requirements
            .features
            .insert(FUTURE_WRITER.to_owned(), FeatureState::Required);
        SharedFacts {
            policy: Some(policy),
            sessions: Some(vec![stale_session()]),
            ..healthy_shared()
        }
    }

    #[test]
    fn compatibility_report_marks_a_healthy_volume_read_write() {
        let report = format_report(
            &ClientCapabilities::current(),
            "0.2.5",
            7,
            &CutoverLocalState::default(),
            &current_stores(),
            &healthy_shared(),
        );
        assert!(report.contains("Overall: compatible_rw"), "{report}");
        assert!(report.contains("  publication  allowed"), "{report}");
        assert!(report.contains("Actions\n  none"), "{report}");
    }

    #[test]
    fn compatibility_report_explains_blocked_publication_stale_session_and_local_work() {
        let local = CutoverLocalState {
            unpublished_events: 2,
            local_version: Some(8),
        };
        let report = format_report(
            &ClientCapabilities::current(),
            "0.2.5",
            7,
            &local,
            &current_stores(),
            &blocked_shared(),
        );
        let layout = LOCAL_METADATA_LAYOUT.current;
        let expected = format!(
            "\
Client
  Build:              0.2.5
  Volume format:      7..=7
  Write requirement:  up to 1
Volume
  Local format:       7
  Shared format:      7
  Policy:             epoch 3, write requirement 1 (publishes up to 1)
Operations
  bootstrap    allowed
  replay       allowed
  publication  blocked: write_capability_missing (table.future_writer.v1) — upgrade StudioFS to a release that supports this volume
  checkpoint   blocked: write_capability_missing (table.future_writer.v1) — upgrade StudioFS to a release that supports this volume
  maintenance  allowed
Sessions
  active 1, expired 0
  3f2a9c1e build 0.2.4 epoch 1: stale epoch 1 (current 3); missing capabilities: write:table.future_writer.v1 — upgrade or unmount that client
Local
  Layout:             {layout}
  Unpublished events: 2
  Local version:      8 (shared head 10)
  Unsafe pending:     yes; these changes are not on the shared volume yet
Overall: compatible_ro
  reason: write_capability_missing (table.future_writer.v1)
Actions
  - upgrade StudioFS to a release that supports this volume
  - upgrade or unmount that client
  - do not reinitialize or clear this cache; mount it with a compatible release so it publishes, or keep it until upgraded
"
        );
        assert_eq!(report, expected);
    }

    #[test]
    fn compatibility_report_asks_for_upgrade_before_adopting_format_five_or_six() {
        let shared = SharedFacts {
            recorded: Vec::new(),
            pending: Vec::new(),
            shared_format: Some(6),
            remote_head: Some(3),
            policy: None,
            sessions: Some(Vec::new()),
        };
        let report = format_report(
            &ClientCapabilities::current(),
            "0.2.5",
            6,
            &CutoverLocalState::default(),
            &current_stores(),
            &shared,
        );
        assert!(
            report.contains("Overall: needs_migration\n  reason: volume_format_needs_migration"),
            "{report}"
        );
        assert!(
            report.contains(ReasonCode::VolumeFormatNeedsMigration.action()),
            "{report}"
        );
    }

    #[test]
    fn compatibility_report_never_prints_connection_secrets() {
        let mut info = FormatInfo::new(
            "secret-test".to_owned(),
            uuid::Uuid::new_v4().to_string(),
            FormatInfoDataStore {
                endpoint: "http://127.0.0.1:9000".to_owned(),
                bucket: "bucket".to_owned(),
                access_key: "AKIASECRETACCESS".to_owned(),
                secret_key: "S3SECRETKEYVALUE".to_owned(),
                region: "us-east-1".to_owned(),
            },
            1,
        )
        .expect("format info");
        info.metadata.postgres = Some(PostgresConfig {
            url: "postgres://studiofs:DSNPASSWORD@127.0.0.1:5432/db".to_owned(),
            volume_id: 1,
        });
        let report = format_report(
            &ClientCapabilities::current(),
            "0.2.5",
            info.format_version,
            &CutoverLocalState::default(),
            &current_stores(),
            &blocked_shared(),
        );
        for secret in [
            "DSNPASSWORD",
            "S3SECRETKEYVALUE",
            "AKIASECRETACCESS",
            "postgres://",
        ] {
            assert!(!report.contains(secret), "{secret} leaked:\n{report}");
        }
    }

    #[test]
    fn compatibility_subcommand_parses_one_volume() {
        let parsed = super::super::parse_for_test(&["compatibility", "volume"]).unwrap();
        match parsed.command {
            super::super::Commands::Compatibility(args) => assert_eq!(args.name, "volume"),
            other => panic!("expected Compatibility command, got {other:?}"),
        }
        assert!(super::super::parse_for_test(&["compatibility"]).is_err());
        assert!(super::super::parse_for_test(&["compatibility", "one", "two"]).is_err());
    }

    #[test]
    fn status_local_compatibility_lines_point_at_the_shared_check() {
        let client = ClientCapabilities::current();
        let supported = format_local_status(&client, "vol", 7, 2);
        assert_eq!(
            supported,
            [
                "  Volume format:       7 (this client supports 7..=7)",
                "  Unpublished events:  2",
                "  Shared check:        run `studiofs compatibility vol`",
            ]
        );
        let outside = format_local_status(&client, "vol", 5, 0);
        assert!(
            outside[0].contains("outside this client's range 7..=7"),
            "{outside:?}"
        );
    }
}
