//! Drop the staging tables crashed loads left.

use crate::adapters::contract::constants::STALE_STAGING_HOURS;
use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
use crate::column_types::models::Warehouse;
use crate::loader::errors::LoaderError;

/// Drop the staging tables in `name`'s schema that crashed loads left: untouched for a day on Snowflake, named over a day old elsewhere, and none on Postgres, whose staging tables are temporary and went with their session (any table in the schema is someone's own); returns how many.
pub async fn drop_stale_staging(
    destination: &Destination,
    name: &TableName,
    warehouse: Warehouse,
) -> Result<usize, LoaderError> {
    if let Destination::Postgres(_) = destination {
        return Ok(0);
    }
    let database = match destination {
        Destination::Snowflake(snowflake) => Some(snowflake.database.as_str()),
        Destination::Databricks(databricks) => Some(databricks.catalog.as_str()),
        Destination::BigQuery(bigquery) => Some(bigquery.project.as_str()),
        _ => None,
    };
    let adapter = sql_adapter(warehouse);
    let found = text_values(
        destination,
        adapter.list_staging_relations_sql(&listed_name(destination, name), database),
    )
    .await?;
    let before = chrono::Utc::now() - chrono::Duration::hours(STALE_STAGING_HOURS);
    let drops = adapter.render_stale_staging(name, &found, before.naive_utc());
    let dropped = drops.len();
    execute(destination, drops).await?;
    Ok(dropped)
}

/// The name whose schema DuckDB lists staging tables in: without its database, as DuckDB has no `<catalog>.information_schema` and lists only the current database's (where MotherDuck's tables are); `name` elsewhere.
fn listed_name(destination: &Destination, name: &TableName) -> TableName {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(_) => TableName {
            database: None,
            ..name.clone()
        },
        _ => name.clone(),
    }
}
