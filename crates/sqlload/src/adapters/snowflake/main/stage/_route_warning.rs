//! The warning a stage route earns in plan.

use crate::adapters::snowflake::models::StageRoute;

/// The warning `route` earns: batched `INSERT` is the slowest route.
pub(in crate::adapters) fn route_warning(route: Option<&StageRoute>) -> Option<&'static str> {
    matches!(route, Some(StageRoute::Insert)).then_some(
        "rows load by batched INSERT, the slowest route; no stage or streaming was usable",
    )
}
