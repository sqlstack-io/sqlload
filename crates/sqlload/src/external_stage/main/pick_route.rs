//! Choose a table's route from what the chain's steps found.

use crate::external_stage::models::{RouteChoice, StepOutcome};

/// The first usable step of `outcomes` (each step's name and what probing it found), with why those before it were passed over. A forced step has no alternative: if it cannot be used, the plan fails with the reason.
pub fn pick_route(
    forced: bool,
    outcomes: Vec<(String, StepOutcome)>,
) -> Result<RouteChoice, String> {
    let mut passed_over: Vec<String> = Vec::new();
    for (step, outcome) in outcomes {
        match (outcome, forced) {
            (Ok(route), _) => return Ok(RouteChoice { route, passed_over }),
            (Err(reason), true) => return Err(format!("stage {step} cannot be used: {reason}")),
            (Err(reason), false) => passed_over.push(format!("{step} cannot be used: {reason}")),
        }
    }
    Err(format!(
        "no route can load this table: {}",
        passed_over.join("; ")
    ))
}
