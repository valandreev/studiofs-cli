use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum TestCacheProfileMode {
    Pyroscope,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileArtifactReport {
    pub kind: ProfileArtifactKind,
    pub path: String,
    pub bytes: u64,
    pub sent_to_endpoint: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum ProfileArtifactKind {
    Cpu,
    Memory,
    WritePathObservability,
    WritePathSummary,
}

impl ProfileArtifactKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Memory => "memory",
            Self::WritePathObservability => "write-path-observability",
            Self::WritePathSummary => "write-path-summary",
        }
    }
}

impl std::fmt::Display for ProfileArtifactKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileEndpointReport {
    pub url: String,
    pub application_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TestCacheProfileResult {
    pub endpoint: Option<ProfileEndpointReport>,
    pub artifacts: Vec<ProfileArtifactReport>,
    pub warnings: Vec<String>,
}
