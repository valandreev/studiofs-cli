use serde::{Deserialize, Serialize};

/// Protocol capabilities advertised by a running StudioFS client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCapabilities {
    /// Shared volume formats the client can open.
    pub volume_format_min: u16,
    /// Latest shared volume format the client can open.
    pub volume_format_max: u16,
    /// Maximum publication write requirement the client supports.
    pub write_requirement_max: u16,
    /// Capabilities the client supports for reads.
    pub read: Vec<String>,
    /// Capabilities the client supports for publication.
    pub write: Vec<String>,
    /// Capabilities the client supports for maintenance.
    pub maintain: Vec<String>,
}

/// A lease observed in a consistent, per-volume session snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSession {
    /// Process-local session identity.
    pub session_id: uuid::Uuid,
    /// Client build version.
    pub build: String,
    /// Declared protocol bounds and capabilities.
    pub capabilities: SessionCapabilities,
    /// Policy epoch observed by the heartbeat, or zero before policy adoption.
    pub policy_epoch: i64,
    /// Lease generation. Reacquisition after expiry advances it.
    pub fencing_token: i64,
    /// Most recent heartbeat on the metadata backend's clock.
    pub heartbeat_at: chrono::DateTime<chrono::Utc>,
    /// Lease deadline on the metadata backend's clock.
    pub lease_expires_at: chrono::DateTime<chrono::Utc>,
    /// Whether the lease was active at the snapshot's observation time.
    pub active: bool,
}
