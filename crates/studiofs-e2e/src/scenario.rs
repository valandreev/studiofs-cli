//! Scenario registry and lifecycle hooks for mounted E2E runs.

use std::fs;
use std::path::PathBuf;

use serde_json::json;
use thiserror::Error;

use crate::client::{
    ClientControllerConfig, ClientControllerError, DEFAULT_CLIENT_NAMES, MultiClientController,
    SystemClientProcessExecutor, VolumeBootstrapOptions, VolumeBootstrapSummary,
};

pub const BASIC_MULTI_CLIENT_SMOKE: &str = "basic-multi-client-smoke";

#[cfg(test)]
pub(crate) const TEST_FAILING_VALIDATE: &str = "__test-failing-validate";
#[cfg(test)]
pub(crate) const TEST_FAILING_VALIDATE_AND_CLEANUP: &str = "__test-failing-validate-and-cleanup";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScenarioDefinition {
    pub name: &'static str,
    pub description: &'static str,
}

#[derive(Debug, Clone)]
pub struct ScenarioContext {
    pub run_id: String,
    pub seed: u64,
    pub sandbox: PathBuf,
    pub scenario_dir: PathBuf,
    pub client_controller_config: Option<ClientControllerConfig>,
}

#[derive(Debug, Error)]
pub enum ScenarioError {
    #[error("scenario I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("scenario JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("scenario client controller failed: {0}")]
    Client(#[from] ClientControllerError),
}

pub trait Scenario {
    fn definition(&self) -> ScenarioDefinition;
    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError>;
    fn cleanup(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
}

pub fn available_scenarios() -> Vec<ScenarioDefinition> {
    vec![BasicMultiClientSmoke.definition()]
}

pub(crate) fn resolve_scenario(name: &str) -> Option<Box<dyn Scenario>> {
    match name {
        BASIC_MULTI_CLIENT_SMOKE | "smoke" => Some(Box::new(BasicMultiClientSmoke)),
        #[cfg(test)]
        TEST_FAILING_VALIDATE => Some(Box::new(TestFailingScenario {
            cleanup_fails: false,
        })),
        #[cfg(test)]
        TEST_FAILING_VALIDATE_AND_CLEANUP => Some(Box::new(TestFailingScenario {
            cleanup_fails: true,
        })),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
struct BasicMultiClientSmoke;

impl Scenario for BasicMultiClientSmoke {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: BASIC_MULTI_CLIENT_SMOKE,
            description: "placeholder smoke scenario for the mounted E2E runner skeleton",
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        if let Some(config) = &context.client_controller_config {
            let mut controller =
                MultiClientController::new(config.clone(), SystemClientProcessExecutor);
            let summary = controller.bootstrap_volume(VolumeBootstrapOptions {
                client_names: DEFAULT_CLIENT_NAMES
                    .iter()
                    .map(|name| (*name).to_string())
                    .collect(),
                clean_existing_local_metadata: true,
            })?;
            write_bootstrap_summary(context, &summary)?;
            return Ok(());
        }

        let workload = json!({
            "scenario": BASIC_MULTI_CLIENT_SMOKE,
            "run_id": context.run_id,
            "seed": context.seed,
            "workload": "placeholder",
        });
        fs::write(
            context.scenario_dir.join("workload.json"),
            serde_json::to_vec_pretty(&workload)?,
        )?;
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let bootstrap_summary = context.scenario_dir.join("bootstrap-summary.json");
        let validation = if bootstrap_summary.exists() {
            let summary: VolumeBootstrapSummary =
                serde_json::from_slice(&fs::read(&bootstrap_summary)?)?;
            json!({
                "scenario": BASIC_MULTI_CLIENT_SMOKE,
                "run_id": context.run_id,
                "seed": context.seed,
                "status": "passed",
                "workflow": "volume-bootstrap-discovery",
                "volume_name": summary.volume_name,
                "init_client": summary.init_client,
                "discovered_clients": summary.discovered_clients,
                "local_metadata_dbs": summary.clients
                    .iter()
                    .map(|client| client.local_metadata_db.display().to_string())
                    .collect::<Vec<_>>(),
            })
        } else {
            json!({
                "scenario": BASIC_MULTI_CLIENT_SMOKE,
                "run_id": context.run_id,
                "seed": context.seed,
                "status": "passed",
                "notes": "placeholder validation for the initial runner crate",
            })
        };

        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        let mut artifact_paths = Vec::new();
        if bootstrap_summary.exists() {
            artifact_paths.push(format!(
                "artifacts/scenarios/{BASIC_MULTI_CLIENT_SMOKE}/bootstrap-summary.json"
            ));
        }
        artifact_paths.push(format!(
            "artifacts/scenarios/{BASIC_MULTI_CLIENT_SMOKE}/validation.json"
        ));
        Ok(artifact_paths)
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn write_bootstrap_summary(
    context: &ScenarioContext,
    summary: &VolumeBootstrapSummary,
) -> Result<(), ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    fs::write(
        context.scenario_dir.join("bootstrap-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct TestFailingScenario {
    cleanup_fails: bool,
}

#[cfg(test)]
impl Scenario for TestFailingScenario {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: if self.cleanup_fails {
                TEST_FAILING_VALIDATE_AND_CLEANUP
            } else {
                TEST_FAILING_VALIDATE
            },
            description: "test-only failing scenario",
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }

    fn validate(&self, _context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        Err(ScenarioError::Io(std::io::Error::other(
            "validation failed",
        )))
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        if self.cleanup_fails {
            Err(ScenarioError::Io(std::io::Error::other("cleanup failed")))
        } else {
            Ok(())
        }
    }
}
