use std::collections::{BTreeMap, BTreeSet};

use arrow_array::RecordBatch;
use tokio::sync::OwnedMutexGuard;

use crate::column_types::models::Warehouse;
use crate::write_sql::constants::STALE_STAGING_HOURS;
use crate::write_sql::main::staging_tables_query::staging_tables_query;
use crate::write_sql::main::stale_staging::stale_staging;

#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::append_batch::append_batch;
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::describe_table::describe_table as describe_duckdb;
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::execute_statements::execute_statements;
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::own_connection::own_connection;
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::query_text::query_text as query_duckdb;
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::query_texts::query_texts as query_duckdb_texts;
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::table_shape::table_shape as duckdb_table_shape;
use crate::external_stage::main::route_reference::route_reference;
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, LoadRun, ProjectPlan, TablePlan};
use crate::schema_plan::models::LiveColumn;
use crate::snowflake_destination::main::describe_table::describe_table as describe_snowflake;
use crate::snowflake_destination::main::query_text::query_text as query_snowflake;
use crate::snowflake_destination::main::query_texts::query_texts as query_snowflake_texts;
use crate::snowflake_destination::main::remove_stale::remove_stale;
use crate::snowflake_destination::main::remove_stale_external::remove_stale_external;
use crate::snowflake_destination::main::run_statements::run_statements;
use crate::snowflake_destination::main::stage_parquet::stage_parquet;
use crate::snowflake_destination::models::{StageRoute, StagedFile};
use crate::stage_api::models::StageInfo;
use crate::write_sql::models::{TableName, TableShape};

/// The error for a SQL operation on a files target, which plans and loads without SQL.
fn no_sql() -> LoaderError {
    LoaderError::Invalid(
        "a files target runs no SQL; its tables are planned and loaded as files".to_owned(),
    )
}

/// The table's live columns, `None` if it does not exist.
pub(crate) async fn live_columns(
    destination: &Destination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, LoaderError> {
    tracing::debug!(table = %table.table, "reading the table's live columns");
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(describe_duckdb(duckdb, table).await?),
        Destination::Snowflake(snowflake) => Ok(describe_snowflake(snowflake, table).await?),
        Destination::Files(_) => Err(no_sql()),
    }
}

/// Run statements in order, in one session.
pub(crate) async fn execute(
    destination: &Destination,
    statements: Vec<String>,
) -> Result<(), LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(execute_statements(duckdb, statements).await?),
        Destination::Snowflake(snowflake) => Ok(run_statements(snowflake, &statements).await?),
        Destination::Files(_) => Err(no_sql()),
    }
}

/// One text value from `query`.
pub(crate) async fn single_text(
    destination: &Destination,
    query: String,
) -> Result<Option<String>, LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(query_duckdb(duckdb, query).await?),
        Destination::Snowflake(snowflake) => Ok(query_snowflake(snowflake, &query).await?),
        Destination::Files(_) => Err(no_sql()),
    }
}

/// The first column of every row of `query`, as text.
pub(crate) async fn text_values(
    destination: &Destination,
    query: String,
) -> Result<Vec<String>, LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(query_duckdb_texts(duckdb, query).await?),
        Destination::Snowflake(snowflake) => Ok(query_snowflake_texts(snowflake, &query).await?),
        Destination::Files(_) => Err(no_sql()),
    }
}

/// Run schema changes with the destination's other loading tables' schema changes held off, so parallel tables never conflict in its catalog.
pub(crate) async fn execute_ddl(
    destination: &Destination,
    statements: Vec<String>,
) -> Result<(), LoaderError> {
    let _held = ddl_guard(destination).await;
    execute(destination, statements).await
}

/// The destination's schema changes held off until the guard drops; a files target has none.
pub(crate) async fn ddl_guard(destination: &Destination) -> Option<OwnedMutexGuard<()>> {
    let lock = match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Some(duckdb.ddl.clone()),
        Destination::Snowflake(snowflake) => Some(snowflake.ddl.clone()),
        Destination::Files(_) => None,
    };
    match lock {
        Some(lock) => Some(lock.lock_owned().await),
        None => None,
    }
}

/// Before a load: drop the staging tables crashed loads left in the schemas it writes to. A failure only warns.
pub(crate) async fn clean_staging(run: &LoadRun<'_>, plan: &ProjectPlan) {
    let mut schemas: BTreeMap<(String, Option<String>, String), &TablePlan> = BTreeMap::new();
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none() && table.output.is_none())
    {
        let name = &table.spec.destination;
        schemas
            .entry((
                table.spec.target.clone(),
                name.database.clone(),
                name.schema.clone(),
            ))
            .or_insert(table);
    }
    for table in schemas.into_values() {
        let Some(destination) = run.destinations.get(&table.spec.target) else {
            continue;
        };
        let name = &table.spec.destination;
        if let Err(error) = drop_stale_staging(destination, name, table.spec.warehouse).await {
            tracing::warn!(%error, schema = %name.schema, "cannot clean up old staging tables");
        }
    }
}

/// Drop the staging tables in `name`'s schema that crashed loads left: untouched for a day on Snowflake, named over a day old elsewhere.
async fn drop_stale_staging(
    destination: &Destination,
    name: &TableName,
    warehouse: Warehouse,
) -> Result<(), LoaderError> {
    let database = match destination {
        Destination::Snowflake(snowflake) => Some(snowflake.database.as_str()),
        _ => None,
    };
    let query = staging_tables_query(name, database, warehouse);
    let found = text_values(destination, query).await?;
    let before = chrono::Utc::now() - chrono::Duration::hours(STALE_STAGING_HOURS);
    let drops = stale_staging(name, &found, before.naive_utc(), warehouse);
    if !drops.is_empty() {
        tracing::info!(dropped = drops.len(), schema = %name.schema, "dropped staging tables crashed loads left");
    }
    execute(destination, drops).await
}

/// The destination for one table's load: DuckDB gets a connection of its own, so the table's transactions never mix with another table loading at the same time.
pub(crate) fn table_destination(destination: &Destination) -> Result<Destination, LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(Destination::DuckDb(own_connection(duckdb)?)),
        other => Ok(other.clone()),
    }
}

/// What a rebuild of `table` must carry over itself: nothing where the warehouse copies a table's shape (Snowflake's `LIKE`).
pub(crate) async fn table_shape(
    destination: &Destination,
    table: &TableName,
) -> Result<Option<TableShape>, LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(duckdb_table_shape(duckdb, table).await?),
        _ => Ok(None),
    }
}

/// Append `batch` to `table` by column name (DuckDB, which takes Arrow directly).
pub(crate) async fn append_rows(
    destination: &Destination,
    table: &TableName,
    batch: RecordBatch,
) -> Result<(), LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(append_batch(duckdb, table, batch).await?),
        _ => Err(LoaderError::Invalid(
            "only DuckDB takes Arrow batches directly".to_owned(),
        )),
    }
}

/// Upload one staged Parquet file to an internal stage (Snowflake), with the batch's `known` credentials; returns them for its next file.
pub(crate) async fn stage_file(
    destination: &Destination,
    known: Option<StageInfo>,
    file: StagedFile<'_>,
) -> Result<StageInfo, LoaderError> {
    match destination {
        Destination::Snowflake(snowflake) => Ok(stage_parquet(snowflake, known, file).await?),
        _ => Err(no_sql()),
    }
}

/// Remove the load folders failed runs left, over a day ago, on the stages this plan's tables use (an external stage keeping its files stays as it is; a staging table's stage goes with its table). A failure only warns.
pub(crate) async fn clean_stages(run: &LoadRun<'_>, plan: &ProjectPlan) {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
    {
        let (Some(route), Some(Destination::Snowflake(snowflake))) =
            (&table.stage, run.destinations.get(&table.spec.target))
        else {
            continue;
        };
        let target = &table.spec.destination;
        let Some(reference) = route_reference(route, target, None) else {
            continue;
        };
        if !seen.insert(format!("{}{reference}", table.spec.target)) {
            continue;
        }
        let removed = match route {
            StageRoute::External(stage) if stage.keep_staged_files => Ok(0),
            StageRoute::External(stage) => remove_stale_external(stage).await,
            _ => remove_stale(snowflake, &reference)
                .await
                .map_err(|error| error.to_string()),
        };
        match removed {
            Ok(0) => {}
            Ok(removed) => {
                tracing::info!(removed, stage = %reference, "removed load folders failed runs left on the stage")
            }
            Err(error) => tracing::warn!(%error, stage = %reference, "cannot clean up the stage"),
        }
    }
}
