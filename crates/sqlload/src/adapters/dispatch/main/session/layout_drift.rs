//! Whether a live table is laid out as its config says.

use crate::adapters::bigquery::main::session::_layout_problems::layout_problems;
use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::models::Destination;
use crate::loader::errors::LoaderError;
use crate::loader::models::TableLayout;

/// How the live `table` is laid out otherwise than `layout` (a BigQuery table's partitioning, clustering and `require_partition_filter`), which a load keeping the table cannot change; `None` where it matches or the warehouse lays tables out only at creation. A config setting no layout is compared too, so a table partitioned by hand is reported rather than replaced unpartitioned.
pub async fn layout_drift(
    destination: &Destination,
    table: &TableName,
    layout: &TableLayout,
) -> Result<Option<String>, LoaderError> {
    match destination {
        Destination::BigQuery(bigquery) => Ok(layout_problems(bigquery, table, layout).await?),
        _ => Ok(None),
    }
}
