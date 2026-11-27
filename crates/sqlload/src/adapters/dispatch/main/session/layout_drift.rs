//! Whether a live table is laid out as its config says.

use crate::adapters::bigquery::constants::RELAYOUT_SUFFIX;
use crate::adapters::bigquery::main::session::_layout_problems::layout_problems;
use crate::adapters::dispatch::models::Destination;
use crate::loader::errors::LoaderError;
use crate::loader::models::TableSpec;

/// How `spec`'s live table (when it `exists`) is laid out otherwise than its config (a BigQuery table's partitioning, clustering and `require_partition_filter`), which a load keeping the table cannot change: the problem blocking it, or under `reload`, which rebuilds the table laid out as configured, the warning instead. Nothing where it matches, where the config sets no layout (SQLLoad then leaves the table's layout as it is), or where the warehouse lays tables out only at creation.
pub async fn layout_drift(
    destination: &Destination,
    spec: &TableSpec,
    (exists, reload): (bool, bool),
) -> Result<(Option<String>, Option<String>), LoaderError> {
    let drift = match destination {
        Destination::BigQuery(bigquery) if exists && !spec.layout.is_empty() => {
            layout_problems(bigquery, &spec.destination, &spec.layout).await?
        }
        _ => None,
    };
    Ok(match (drift, reload) {
        (None, _) => (None, None),
        (Some(drift), false) => (
            Some(format!(
                "{drift}; load with --reload to rebuild it as configured, or change the config to match"
            )),
            None,
        ),
        (Some(drift), true) => (
            None,
            Some(format!(
                "{drift}; --reload renames the table to {}{RELAYOUT_SUFFIX} and creates it anew laid out as configured, then drops the renamed one, which keeps its rows should the load fail in between",
                spec.destination.table
            )),
        ),
    })
}
