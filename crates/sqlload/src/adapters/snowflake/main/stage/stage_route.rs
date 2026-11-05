//! Choose how one table's rows reach Snowflake, trying what the role can use without changing anything.

use crate::adapters::snowflake::_helpers::pick_route::pick_route;
use crate::adapters::snowflake::_helpers::probes::{
    checked_external, planned_stage, probed_step, step_name,
};
use crate::adapters::snowflake::models::{ForcedStage, StageRoute};
use crate::adapters::snowflake::models::{PlannedStage, RouteChoice, RouteRequest, StepFailure};

/// The table's route: what the connection's `stage` forces (failing with the reason if it cannot be used), else the first of `@~`, `@%table`, Snowpipe Streaming and `INSERT` the role can use; a probe failing for any other reason than privileges or a missing object fails the plan. Each probe changes nothing and is asked once per run. Also the stage `apply` must create, if any.
pub async fn stage_route(
    request: &RouteRequest<'_>,
) -> Result<(RouteChoice, Option<PlannedStage>), String> {
    match &request.snowflake.stage {
        None => {
            let mut outcomes: Vec<(String, Result<StageRoute, StepFailure>)> = Vec::new();
            for step in [
                ForcedStage::User,
                ForcedStage::Table,
                ForcedStage::Streaming,
            ] {
                match probed_step(request, &step, true).await {
                    Ok(route) => {
                        outcomes.push((step_name(&step), Ok(route)));
                        return Ok((pick_route(false, outcomes)?, None));
                    }
                    Err(reason) => outcomes.push((step_name(&step), Err(reason))),
                }
            }
            outcomes.push((step_name(&ForcedStage::Insert), Ok(StageRoute::Insert)));
            Ok((pick_route(false, outcomes)?, None))
        }
        Some(ForcedStage::External(stage)) => {
            let outcome = checked_external(request.snowflake, stage).await;
            let creates = matches!(outcome, Ok(true));
            let route = outcome.map(|_| StageRoute::External(stage.clone()));
            let choice = pick_route(true, vec![(stage.location.to_string(), route)])?;
            let planned = creates.then(|| planned_stage(stage)).flatten();
            Ok((choice, planned))
        }
        Some(step) => {
            let outcome = probed_step(request, step, false).await;
            Ok((pick_route(true, vec![(step_name(step), outcome)])?, None))
        }
    }
}
