use clap::ValueEnum;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum TestCacheProfileMode {
    Pyroscope,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileArtifactReport {
    pub kind: String,
    pub path: String,
    pub bytes: u64,
    pub sent_to_endpoint: bool,
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
