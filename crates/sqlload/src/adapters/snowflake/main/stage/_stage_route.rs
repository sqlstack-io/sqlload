//! Check that the route a connection's `load_method` sets can load one table, without changing anything.

use crate::adapters::snowflake::_helpers::stage::probes::{
    checked_external, planned_stage, probed_step, step_name,
};
use crate::adapters::snowflake::models::{ForcedStage, StageRoute};
use crate::adapters::snowflake::models::{PlannedStage, RouteRequest, StepFailure};

/// The table's route: the one the connection's `load_method` sets, failing with the reason if it cannot be used; there is no fallback. Each check changes nothing and is asked once per run. Also the stage `apply` must create, if any.
pub(in crate::adapters) async fn stage_route(
    request: &RouteRequest<'_>,
) -> Result<(StageRoute, Option<PlannedStage>), String> {
    let (name, outcome, planned) = match &request.snowflake.stage {
        ForcedStage::External(stage) => {
            let outcome = checked_external(request.snowflake, stage).await;
            let planned = matches!(outcome, Ok(true))
                .then(|| planned_stage(stage))
                .flatten();
            (
                stage.location.to_string(),
                outcome.map(|_| StageRoute::External(stage.clone())),
                planned,
            )
        }
        step => (step_name(step), probed_step(request, step).await, None),
    };
    match outcome {
        Ok(route) => Ok((route, planned)),
        Err(StepFailure::Unusable(reason)) => {
            Err(format!("load_method {name} cannot be used: {reason}"))
        }
        Err(StepFailure::Failed(reason)) => Err(format!(
            "cannot check whether load_method {name} can be used: {reason}"
        )),
    }
}
