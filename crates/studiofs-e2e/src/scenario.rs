//! Scenario registry and lifecycle hooks for mounted E2E runs.

use std::fs;
use std::path::PathBuf;

use serde_json::json;
use thiserror::Error;

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
}

#[derive(Debug, Error)]
pub enum ScenarioError {
    #[error("scenario I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("scenario JSON failed: {0}")]
    Json(#[from] serde_json::Error),
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
        let validation = json!({
            "scenario": BASIC_MULTI_CLIENT_SMOKE,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": "passed",
            "notes": "placeholder validation for the initial runner crate",
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![format!(
            "artifacts/scenarios/{BASIC_MULTI_CLIENT_SMOKE}/validation.json"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
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
