//! `studiofs compatibility <volume>`: a read-only report of what this client can
//! do with a volume. `studiofs upgrade <volume> --dry-run` appends the same
//! report, and `studiofs status` prints its local-only lines. Local redb is
//! opened read-only, but a mount on this computer holds its lock, so the report
//! needs a stopped local cache. No DSN, key or other secret is printed.

use anyhow::{Context, anyhow, bail};
use clap::Args;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use studiofs_meta::client_sessions::ClientSession;
use studiofs_meta::compat::{
    CheckpointDescriptor, ClientCapabilities, Compatibility, CompatibilityDecision, Operation,
    ReasonCode, VolumeRequirements,
};
use studiofs_meta::pg_engine::migrations::{
    Migration, RecordedComponent, registered_schema_status,
};
use studiofs_meta::pg_engine::{collect_cutover_shared_state, read_client_sessions};
use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo};
use studiofs_meta::version::{LOCAL_METADATA_LAYOUT, SHARED_WRITE_REQUIREMENT};
use studiofs_meta::volume_protocol::{
    CutoverLocalState, ProtocolRejection, VolumeProtocol, missing_capabilities_for_session,
};
use studiofs_meta::{MetaError, RedbMetaEngine};
use tokio_postgres::GenericClient;

const SESSION_ACTION: &str = "upgrade or unmount that client";
const UNPUBLISHED_ACTION: &str = "do not reinitialize or clear this cache; mount it with a compatible release so it publishes, or keep it until upgraded";
const UNKNOWN_ACTION: &str = "check that this cache points at the right PostgreSQL database and volume; run `studiofs discover` to list the volumes there";
const MISSING_POLICY_ACTION: &str = "mounts refuse this volume; do not reinitialize it or clear any cache; restore the PostgreSQL backup or ask for support";
const MOUNTED_HERE: &str = "This volume is mounted on this computer, and the mount holds its local metadata lock. The mount already enforces compatibility. Unmount it to run this report, or run the report on another computer. Nothing was changed.";

#[derive(Args, Debug)]
pub struct CompatibilityArgs {
    /// Locally discovered volume with a stopped local cache on this computer.
    pub name: String,
}

/// Local redb facts from read-only snapshots.
pub(crate) struct LocalFacts {
    pub(crate) format: FormatInfo,
    pub(crate) state: CutoverLocalState,
    /// Local store identities by `VersionIdentity::name`, as `evaluate` expects;
    /// empty for a cache whose volume format still needs `studiofs upgrade`.
    pub(crate) stores: BTreeMap<String, u16>,
}

/// Stored volume policy as the report sees it.
pub(crate) enum SharedPolicy {
    /// Not read: schema migrations are pending, or the volume format needs adoption.
    NotRead,
    /// Current-format volume without a stored policy; mounts refuse it.
    Missing,
    /// Written by a newer StudioFS; the text names versions only.
    Newer(String),
    Read {
        policy: VolumeProtocol,
        /// Serving checkpoint encodings a bootstrap can choose from.
        checkpoints: Vec<CheckpointDescriptor>,
    },
}

/// Shared PostgreSQL facts from one bounded block.
pub(crate) struct SharedFacts {
    pub(crate) recorded: Vec<RecordedComponent>,
    pub(crate) pending: Vec<Migration>,
    /// A schema component newer than this binary maintains; versions only.
    pub(crate) schema_newer: Option<String>,
    /// `None` when the volume has no shared row.
    pub(crate) shared_format: Option<u16>,
    pub(crate) remote_head: Option<i64>,
    pub(crate) policy: SharedPolicy,
    /// `None` when not read: schema migrations pending, or the read failed.
    pub(crate) sessions: Option<Vec<ClientSession>>,
}

pub(crate) fn read_local_facts(path: &Path) -> anyhow::Result<LocalFacts> {
    let (format, state) = RedbMetaEngine::read_upgrade_and_cutover_state_readonly(path).context(
        "Cannot read local metadata; if this volume is mounted on this computer, unmount it first. Nothing was changed",
    )?;
    // The layout reader decodes strictly and refuses formats 5/6; their
    // report comes from the volume format instead.
    let stores = if format.format_version == CURRENT_FORMAT_VERSION {
        let layout = RedbMetaEngine::read_local_revision(path)
            .context("Cannot read the local metadata layout; nothing was changed")?;
        BTreeMap::from([(LOCAL_METADATA_LAYOUT.name.to_owned(), layout)])
    } else {
        BTreeMap::new()
    };
    Ok(LocalFacts {
        format,
        state,
        stores,
    })
}

/// Reads the shared side of the report. `repair_schema` matches `upgrade --repair-schema`.
pub(crate) async fn read_shared_facts<C: GenericClient + Sync>(
    client: &C,
    volume: i64,
    repair_schema: bool,
) -> anyhow::Result<SharedFacts> {
    let (recorded, pending, schema_newer) =
        match registered_schema_status(client, repair_schema).await {
            Ok((recorded, pending)) => (recorded, pending, None),
            Err(MetaError::UpgradeRequired { reason }) => {
                (Vec::new(), Vec::new(), Some(reason.to_string()))
            }
            Err(error) => return Err(error.into()),
        };
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
    let schema_ready = pending.is_empty() && schema_newer.is_none();
    let policy = if !schema_ready || shared_format != Some(CURRENT_FORMAT_VERSION) {
        SharedPolicy::NotRead
    } else {
        match collect_cutover_shared_state(client, volume).await {
            Ok((policy, state)) => SharedPolicy::Read {
                policy,
                checkpoints: state.available_checkpoints,
            },
            Err(MetaError::VolumeProtocolRejected(ProtocolRejection::Missing)) => {
                SharedPolicy::Missing
            }
            Err(MetaError::UpgradeRequired { reason }) => SharedPolicy::Newer(reason.to_string()),
            Err(_) => bail!("reading the volume protocol failed"),
        }
    };
    let sessions = if schema_ready {
        read_client_sessions(client, volume).await.ok()
    } else {
        None
    };
    Ok(SharedFacts {
        recorded,
        pending,
        schema_newer,
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

/// A refusal that stops the whole volume before any operation is evaluated.
struct VolumeRefusal {
    overall: &'static str,
    reason: String,
    action: &'static str,
}

fn volume_refusal(local: &LocalFacts, shared: &SharedFacts) -> Option<VolumeRefusal> {
    let upgrade = ReasonCode::SchemaComponentNewer.action();
    if shared.shared_format.is_none() {
        return Some(VolumeRefusal {
            overall: "unknown",
            reason: "shared volume metadata is missing".to_owned(),
            action: UNKNOWN_ACTION,
        });
    }
    if let Some(reason) = &shared.schema_newer {
        return Some(VolumeRefusal {
            overall: Compatibility::UpgradeRequired.as_str(),
            reason: reason.clone(),
            action: upgrade,
        });
    }
    match &shared.policy {
        SharedPolicy::Newer(reason) => {
            return Some(VolumeRefusal {
                overall: Compatibility::UpgradeRequired.as_str(),
                reason: reason.clone(),
                action: upgrade,
            });
        }
        SharedPolicy::Missing => {
            return Some(VolumeRefusal {
                overall: Compatibility::Incompatible.as_str(),
                reason: "volume_policy_missing".to_owned(),
                action: MISSING_POLICY_ACTION,
            });
        }
        SharedPolicy::NotRead | SharedPolicy::Read { .. } => {}
    }
    // Shared adoption runs first; this cache still has the 0.2.x header.
    let local_format = local.format.format_version;
    if shared.shared_format == Some(CURRENT_FORMAT_VERSION) && local_format < CURRENT_FORMAT_VERSION
    {
        return Some(VolumeRefusal {
            overall: Compatibility::NeedsMigration.as_str(),
            reason: format!("local_format_outdated (format {local_format})"),
            action: ReasonCode::LocalFormatOutdated.action(),
        });
    }
    None
}

/// Mount bootstraps from any serving checkpoint it can decode, so the report
/// prefers a candidate that allows bootstrap.
fn decide(
    client: &ClientCapabilities,
    policy: &VolumeProtocol,
    format: u16,
    checkpoints: &[CheckpointDescriptor],
    local: &BTreeMap<String, u16>,
) -> CompatibilityDecision {
    let decide_with = |checkpoint| {
        policy.compatibility_decision(
            client,
            VolumeRequirements {
                format,
                ..VolumeRequirements::baseline()
            },
            checkpoint,
            local,
        )
    };
    let mut candidates = checkpoints
        .iter()
        .map(|checkpoint| decide_with(Some(*checkpoint)));
    let first = candidates.next();
    match first {
        None => decide_with(None),
        Some(first) if first.allows(Operation::Bootstrap) => first,
        Some(first) => candidates
            .find(|decision| decision.allows(Operation::Bootstrap))
            .unwrap_or(first),
    }
}

/// Renders the report from already-read facts. Pure: it prints numbers and the
/// typed policy, never connection data or keys.
pub(crate) fn format_report(
    client: &ClientCapabilities,
    build: &str,
    local: &LocalFacts,
    shared: &SharedFacts,
) -> String {
    let mut lines = Vec::new();
    let mut actions = Vec::new();
    let baseline = VolumeProtocol::baseline();
    let (effective, checkpoints) = match &shared.policy {
        SharedPolicy::Read {
            policy,
            checkpoints,
        } => (policy, checkpoints.as_slice()),
        _ => (&baseline, &[][..]),
    };
    // A client that publishes below the policy's write requirement is fenced, even
    // though the capability evaluator does not know that rule.
    let write_newer = effective.write_requirement > SHARED_WRITE_REQUIREMENT.current;
    let refusal = volume_refusal(local, shared);
    let decision = match (&refusal, shared.shared_format) {
        (None, Some(format)) => Some(decide(
            client,
            effective,
            format,
            checkpoints,
            &local.stores,
        )),
        _ => None,
    };

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

    let (overall, overall_reason) = match (&refusal, &decision) {
        (Some(refusal), _) => {
            add_action(&mut actions, refusal.action);
            (refusal.overall, Some(refusal.reason.clone()))
        }
        (None, Some(decision)) => match (&decision.reason, write_newer) {
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
        (None, None) => ("unknown", None),
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
    lines.push(format!(
        "  Local format:       {}",
        local.format.format_version
    ));
    lines.push(format!(
        "  Shared format:      {}",
        or_unknown(shared.shared_format)
    ));
    let policy_text = match &shared.policy {
        SharedPolicy::Read { .. } => format!(
            "epoch {}, write requirement {} (publishes up to {})",
            effective.epoch, effective.write_requirement, SHARED_WRITE_REQUIREMENT.current
        ),
        SharedPolicy::Missing => "missing".to_owned(),
        SharedPolicy::Newer(_) => "written by a newer StudioFS".to_owned(),
        SharedPolicy::NotRead if !shared.pending.is_empty() => format!(
            "not read; {} schema migration(s) pending, run `studiofs upgrade`",
            shared.pending.len()
        ),
        SharedPolicy::NotRead
            if shared
                .shared_format
                .is_some_and(|f| f < CURRENT_FORMAT_VERSION) =>
        {
            "none yet; `studiofs upgrade` adopts the 0.2.5 baseline".to_owned()
        }
        SharedPolicy::NotRead => "not read".to_owned(),
    };
    lines.push(format!("  Policy:             {policy_text}"));

    lines.push("Operations".to_owned());
    for operation in Operation::ALL {
        let state = match (&decision, blocked.iter().find(|(op, ..)| *op == operation)) {
            (None, _) => "not evaluated; see Overall".to_owned(),
            (Some(_), None) => "allowed".to_owned(),
            (Some(_), Some((_, detail, action))) => format!("blocked: {detail} — {action}"),
        };
        lines.push(format!("  {:<12} {state}", operation.as_str()));
    }

    lines.push("Sessions".to_owned());
    match &shared.sessions {
        None => lines.push("  not read".to_owned()),
        Some(sessions) => {
            let active = sessions.iter().filter(|session| session.active).count();
            lines.push(format!(
                "  active {active}, expired {}",
                sessions.len() - active
            ));
            for session in sessions {
                let short_id = &session.session_id.to_string()[..8];
                let head = format!(
                    "  {short_id} build {} epoch {}",
                    session.build, session.policy_epoch
                );
                if !session.active {
                    lines.push(format!("{head}: expired"));
                    continue;
                }
                let missing = missing_capabilities_for_session(session, effective);
                let stale =
                    session.policy_epoch < i64::try_from(effective.epoch).unwrap_or(i64::MAX);
                if !missing.is_empty() {
                    lines.push(format!(
                        "{head}: missing capabilities: {} — {SESSION_ACTION}",
                        missing.join(", ")
                    ));
                    add_action(&mut actions, SESSION_ACTION);
                } else if stale {
                    // A capable client adopts the new epoch when it renews its lease.
                    lines.push(format!(
                        "{head}: stale epoch (current {}); refreshes on its next heartbeat",
                        effective.epoch
                    ));
                } else {
                    lines.push(format!("{head}: ok"));
                }
            }
        }
    }

    lines.push("Local".to_owned());
    lines.push(format!(
        "  Layout:             {}",
        or_unknown(local.stores.get(LOCAL_METADATA_LAYOUT.name))
    ));
    lines.push(format!(
        "  Unpublished events: {}",
        local.state.unpublished_events
    ));
    lines.push(format!(
        "  Local version:      {} (shared head {})",
        or_unknown(local.state.local_version),
        or_unknown(shared.remote_head)
    ));
    if local.state.unpublished_events > 0 {
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
        if super::utils::active_mount_record_for_volume(&self.name)?.is_some() {
            bail!(MOUNTED_HERE);
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
                            local,
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

    fn baseline_checkpoint() -> CheckpointDescriptor {
        VolumeRequirements::baseline()
            .checkpoint
            .expect("baseline checkpoint")
    }

    /// Local facts whose header carries a DSN password and S3 keys, so every
    /// report test also proves they never reach the output.
    fn local_facts(format_version: u16, state: CutoverLocalState) -> LocalFacts {
        let mut format = FormatInfo::new(
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
        format.format_version = format_version;
        format.metadata.postgres = Some(PostgresConfig {
            url: "postgres://studiofs:DSNPASSWORD@127.0.0.1:5432/db".to_owned(),
            volume_id: 1,
        });
        let stores = if format_version == CURRENT_FORMAT_VERSION {
            BTreeMap::from([(
                LOCAL_METADATA_LAYOUT.name.to_owned(),
                LOCAL_METADATA_LAYOUT.current,
            )])
        } else {
            BTreeMap::new()
        };
        LocalFacts {
            format,
            state,
            stores,
        }
    }

    fn report(local: &LocalFacts, shared: &SharedFacts) -> String {
        let report = format_report(&ClientCapabilities::current(), "0.2.5", local, shared);
        for secret in [
            "DSNPASSWORD",
            "S3SECRETKEYVALUE",
            "AKIASECRETACCESS",
            "postgres://",
        ] {
            assert!(!report.contains(secret), "{secret} leaked:\n{report}");
        }
        report
    }

    fn healthy_shared() -> SharedFacts {
        SharedFacts {
            recorded: vec![RecordedComponent {
                component: POSTGRES_PAYLOAD_GC_SCHEMA.name.to_owned(),
                version: i32::from(POSTGRES_PAYLOAD_GC_SCHEMA.current),
                checksum: None,
            }],
            pending: Vec::new(),
            schema_newer: None,
            shared_format: Some(CURRENT_FORMAT_VERSION),
            remote_head: Some(10),
            policy: SharedPolicy::Read {
                policy: VolumeProtocol::baseline(),
                checkpoints: vec![baseline_checkpoint()],
            },
            sessions: Some(Vec::new()),
        }
    }

    fn read_policy(policy: VolumeProtocol, checkpoints: Vec<CheckpointDescriptor>) -> SharedPolicy {
        SharedPolicy::Read {
            policy,
            checkpoints,
        }
    }

    fn session(id: &str, policy_epoch: i64, active: bool) -> ClientSession {
        let client = ClientCapabilities::current();
        let now = chrono::Utc::now();
        ClientSession {
            session_id: uuid::Uuid::parse_str(id).unwrap(),
            build: "0.2.5".to_owned(),
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
            policy_epoch,
            fencing_token: 1,
            heartbeat_at: now,
            lease_expires_at: now + chrono::Duration::seconds(60),
            active,
        }
    }

    /// Policy whose write set names a capability neither this client nor the
    /// sessions declare.
    fn blocked_shared() -> SharedFacts {
        let mut policy = VolumeProtocol::baseline();
        policy.epoch = 3;
        policy.requirements.write.insert(FUTURE_WRITER.to_owned());
        policy
            .requirements
            .features
            .insert(FUTURE_WRITER.to_owned(), FeatureState::Required);
        SharedFacts {
            policy: read_policy(policy, vec![baseline_checkpoint()]),
            sessions: Some(vec![
                session("3f2a9c1e-0000-4000-8000-000000000001", 1, true),
                session("5b7d0a42-0000-4000-8000-000000000002", 2, false),
            ]),
            ..healthy_shared()
        }
    }

    #[test]
    fn compatibility_report_marks_a_healthy_volume_read_write() {
        let report = report(
            &local_facts(CURRENT_FORMAT_VERSION, CutoverLocalState::default()),
            &healthy_shared(),
        );
        assert!(report.contains("Overall: compatible_rw"), "{report}");
        assert!(report.contains("  publication  allowed"), "{report}");
        assert!(report.contains("Actions\n  none"), "{report}");
    }

    #[test]
    fn compatibility_report_explains_blocked_publication_session_and_local_work() {
        let local = local_facts(
            CURRENT_FORMAT_VERSION,
            CutoverLocalState {
                unpublished_events: 2,
                local_version: Some(8),
            },
        );
        let report = report(&local, &blocked_shared());
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
  active 1, expired 1
  3f2a9c1e build 0.2.5 epoch 1: missing capabilities: write:table.future_writer.v1 — upgrade or unmount that client
  5b7d0a42 build 0.2.5 epoch 2: expired
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
    fn compatibility_report_lets_a_capable_stale_session_refresh_itself() {
        let mut policy = VolumeProtocol::baseline();
        policy.epoch = 3;
        let shared = SharedFacts {
            policy: read_policy(policy, vec![baseline_checkpoint()]),
            sessions: Some(vec![session(
                "3f2a9c1e-0000-4000-8000-000000000001",
                1,
                true,
            )]),
            ..healthy_shared()
        };
        let report = report(
            &local_facts(CURRENT_FORMAT_VERSION, CutoverLocalState::default()),
            &shared,
        );
        assert!(
            report.contains("epoch 1: stale epoch (current 3); refreshes on its next heartbeat"),
            "{report}"
        );
        assert!(report.contains("Actions\n  none"), "{report}");
    }

    #[test]
    fn compatibility_report_refuses_bootstrap_only_when_no_checkpoint_decodes() {
        let newer = CheckpointDescriptor {
            format: u16::MAX,
            schema: u16::MAX,
        };
        let local = local_facts(CURRENT_FORMAT_VERSION, CutoverLocalState::default());
        let mut shared = healthy_shared();
        shared.policy = read_policy(
            VolumeProtocol::baseline(),
            vec![newer, baseline_checkpoint()],
        );
        let text = report(&local, &shared);
        assert!(text.contains("  bootstrap    allowed"), "{text}");
        shared.policy = read_policy(VolumeProtocol::baseline(), vec![newer]);
        let text = report(&local, &shared);
        assert!(
            text.contains("  bootstrap    blocked: checkpoint_format_newer"),
            "{text}"
        );
        assert!(text.contains("Overall: upgrade_required"), "{text}");
    }

    #[test]
    fn compatibility_report_asks_for_upgrade_before_adopting_format_five_or_six() {
        for format in [5, 6] {
            let shared = SharedFacts {
                recorded: Vec::new(),
                shared_format: Some(format),
                policy: SharedPolicy::NotRead,
                ..healthy_shared()
            };
            let text = report(&local_facts(format, CutoverLocalState::default()), &shared);
            assert!(
                text.contains("Overall: needs_migration\n  reason: volume_format_needs_migration"),
                "{text}"
            );
            assert!(
                text.contains(ReasonCode::VolumeFormatNeedsMigration.action()),
                "{text}"
            );
        }
    }

    #[test]
    fn compatibility_report_asks_this_cache_to_upgrade_after_shared_adoption() {
        let text = report(
            &local_facts(6, CutoverLocalState::default()),
            &healthy_shared(),
        );
        assert!(
            text.contains("Overall: needs_migration\n  reason: local_format_outdated (format 6)"),
            "{text}"
        );
        assert!(
            text.contains(ReasonCode::LocalFormatOutdated.action()),
            "{text}"
        );
    }

    #[test]
    fn compatibility_report_names_newer_missing_and_unknown_volumes() {
        let local = local_facts(CURRENT_FORMAT_VERSION, CutoverLocalState::default());
        let newer = SharedFacts {
            policy: SharedPolicy::Newer("volume write requirement is 9".to_owned()),
            ..healthy_shared()
        };
        let text = report(&local, &newer);
        assert!(
            text.contains("Overall: upgrade_required\n  reason: volume write requirement is 9"),
            "{text}"
        );
        assert!(
            text.contains("  bootstrap    not evaluated; see Overall"),
            "{text}"
        );

        let schema_newer = SharedFacts {
            schema_newer: Some("schema component `core` is newer".to_owned()),
            policy: SharedPolicy::NotRead,
            ..healthy_shared()
        };
        let text = report(&local, &schema_newer);
        assert!(text.contains("Overall: upgrade_required"), "{text}");

        let missing = SharedFacts {
            policy: SharedPolicy::Missing,
            ..healthy_shared()
        };
        let text = report(&local, &missing);
        assert!(
            text.contains("Overall: incompatible\n  reason: volume_policy_missing"),
            "{text}"
        );
        assert!(text.contains(MISSING_POLICY_ACTION), "{text}");

        let unknown = SharedFacts {
            shared_format: None,
            remote_head: None,
            policy: SharedPolicy::NotRead,
            ..healthy_shared()
        };
        let text = report(&local, &unknown);
        assert!(text.contains("Overall: unknown"), "{text}");
        assert!(text.contains(UNKNOWN_ACTION), "{text}");
    }

    /// The published 0.2.4 cache is format 6, which the strict layout reader refuses.
    #[test]
    fn read_local_facts_accepts_a_published_format_six_cache() {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../studiofs-meta/tests/fixtures/published-v0.2.4/local-cache.redb");
        let dir = studiofs_test_support::workspace_target_tempdir("compatibility-format6-");
        let path = dir.path().join("cache.redb");
        studiofs_test_support::copy_file_contents(&fixture, &path).expect("copy fixture");
        let local = read_local_facts(&path).expect("format-6 cache is readable");
        assert_eq!(local.format.format_version, 6);
        assert!(local.stores.is_empty());
        let shared = SharedFacts {
            shared_format: Some(6),
            policy: SharedPolicy::NotRead,
            ..healthy_shared()
        };
        let text = report(&local, &shared);
        assert!(text.contains("Overall: needs_migration"), "{text}");
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
