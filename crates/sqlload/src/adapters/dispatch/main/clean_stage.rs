//! Remove the load folders failed runs left on a stage.

use std::collections::BTreeSet;

use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::snowflake::main::stage::_remove_stale::remove_stale;
use crate::adapters::snowflake::main::stage::_remove_stale_external::remove_stale_external;
use crate::adapters::snowflake::main::stage::route_reference::route_reference;
use crate::adapters::snowflake::models::StageRoute;

/// Remove the load folders failed runs left over a day ago on the stage `route` loads `target` through, once per stage of `connection` (`seen`, returned with this stage added); an external stage keeping its files stays as it is, and a staging table's stage goes with its table. Also returns the stage and what was removed, `None` when there was nothing to clean.
pub async fn clean_stage(
    destination: &Destination,
    (connection, route, target): (&str, &StageRoute, &TableName),
    mut seen: BTreeSet<String>,
) -> (BTreeSet<String>, Option<(String, Result<usize, String>)>) {
    let Destination::Snowflake(snowflake) = destination else {
        return (seen, None);
    };
    let Some(reference) = route_reference(route, target, None) else {
        return (seen, None);
    };
    if !seen.insert(format!("{connection}{reference}")) {
        return (seen, None);
    }
    let removed = match route {
        StageRoute::External(stage) if stage.keep_staged_files => Ok(0),
        StageRoute::External(stage) => remove_stale_external(stage).await,
        _ => remove_stale(snowflake, &reference)
            .await
            .map_err(|error| error.to_string()),
    };
    (seen, Some((reference, removed)))
}
