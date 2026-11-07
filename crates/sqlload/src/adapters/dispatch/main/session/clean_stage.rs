//! Remove the load folders failed runs left on a stage.

use std::collections::BTreeSet;

use crate::adapters::contract::models::TableName;
use crate::adapters::databricks::main::stage::_remove_stale::remove_stale as remove_stale_volume;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::snowflake::main::stage::_remove_stale::remove_stale;
use crate::adapters::snowflake::main::stage::_remove_stale_external::remove_stale_external;
use crate::adapters::snowflake::main::stage::_route_reference::route_reference;
use crate::adapters::snowflake::models::StageRoute;

/// Remove the load folders failed runs left over a day ago on the stage `route` loads `target` through, and on `@~` too when the table's files are copied there (`copies_files`), once per stage of `connection` (`seen`, returned with these stages added); an external stage keeping its files stays as it is, and a staging table's stage goes with its table. Also returns each stage cleaned and what was removed from it.
pub async fn clean_stage(
    destination: &Destination,
    (connection, route, target): (&str, Option<&StageRoute>, &TableName),
    copies_files: bool,
    mut seen: BTreeSet<String>,
) -> (BTreeSet<String>, Vec<(String, Result<usize, String>)>) {
    if let Destination::Databricks(databricks) = destination {
        if !seen.insert(connection.to_owned()) {
            return (seen, Vec::new());
        }
        let removed = remove_stale_volume(databricks)
            .await
            .map_err(|error| error.to_string());
        return (seen, vec![(databricks.volume.clone(), removed)]);
    }
    let (Destination::Snowflake(snowflake), Some(route)) = (destination, route) else {
        return (seen, Vec::new());
    };
    let user = copies_files.then_some(&StageRoute::User);
    let mut cleaned: Vec<(String, Result<usize, String>)> = Vec::new();
    for route in std::iter::once(route).chain(user) {
        let Some(reference) = route_reference(route, target, None) else {
            continue;
        };
        if !seen.insert(format!("{connection}{reference}")) {
            continue;
        }
        let removed = match route {
            StageRoute::External(stage) if stage.keep_staged_files => Ok(0),
            StageRoute::External(stage) => remove_stale_external(stage).await,
            _ => remove_stale(snowflake, &reference)
                .await
                .map_err(|error| error.to_string()),
        };
        cleaned.push((reference, removed));
    }
    (seen, cleaned)
}
