//! Test-only scenario fixtures used by runner lifecycle tests.

use super::*;

#[derive(Debug, Clone, Copy)]
pub(in crate::scenario) struct TestFailingScenario {
    pub(in crate::scenario) cleanup_fails: bool,
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
            requires_studiofs_bin: false,
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

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(in crate::scenario) struct TestPassingScenario;

#[cfg(test)]
impl Scenario for TestPassingScenario {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: TEST_PASSING,
            description: "test-only passing scenario",
            requires_studiofs_bin: false,
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
        Ok(Vec::new())
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}
