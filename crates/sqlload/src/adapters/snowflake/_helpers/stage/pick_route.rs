use crate::adapters::snowflake::models::StageRoute;
use crate::adapters::snowflake::models::{RouteChoice, StepFailure};

/// The first usable step of `outcomes` (each step's name and what probing it found), with why those before it were passed over. A forced step has no alternative: if it cannot be used, the plan fails with the reason. So does a step whose probe failed for any other reason than privileges or a missing object, which says nothing about the step.
pub(crate) fn pick_route(
    forced: bool,
    outcomes: Vec<(String, Result<StageRoute, StepFailure>)>,
) -> Result<RouteChoice, String> {
    let mut passed_over: Vec<String> = Vec::new();
    for (step, outcome) in outcomes {
        match (outcome, forced) {
            (Ok(route), _) => return Ok(RouteChoice { route, passed_over }),
            (Err(StepFailure::Unusable(reason) | StepFailure::Failed(reason)), true) => {
                return Err(format!("stage {step} cannot be used: {reason}"));
            }
            (Err(StepFailure::Failed(reason)), false) => {
                return Err(format!("cannot check whether {step} can be used: {reason}"));
            }
            (Err(StepFailure::Unusable(reason)), false) => {
                passed_over.push(format!("{step} cannot be used: {reason}"));
            }
        }
    }
    Err(format!(
        "no route can load this table: {}",
        passed_over.join("; ")
    ))
}
