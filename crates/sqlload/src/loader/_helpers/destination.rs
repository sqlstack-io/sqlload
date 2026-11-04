use arrow_array::RecordBatch;

use crate::column_types::models::Warehouse;
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::append_batch::append_batch;
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::describe_table::describe_table as describe_duckdb;
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::execute_statements::execute_statements;
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::query_text::query_text as query_duckdb;
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::query_texts::query_texts as query_duckdb_texts;
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, StagingSession};
use crate::schema_plan::models::LiveColumn;
use crate::snowflake_destination::main::copy_staged::copy_staged;
use crate::snowflake_destination::main::describe_table::describe_table as describe_snowflake;
use crate::snowflake_destination::main::query_text::query_text as query_snowflake;
use crate::snowflake_destination::main::query_texts::query_texts as query_snowflake_texts;
use crate::snowflake_destination::main::remove_staged::remove_staged;
use crate::snowflake_destination::main::run_statements::run_statements;
use crate::snowflake_destination::main::stage_parquet::stage_parquet;
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::main::swap_table::swap_table;
use crate::write_sql::models::TableName;

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

/// Add batch number `session.parts` of the load: appended to the staging table, or staged as a Parquet file.
pub(crate) async fn stage_batch(
    destination: &Destination,
    session: &StagingSession,
    batch: RecordBatch,
) -> Result<(), LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(append_batch(duckdb, &session.staging, batch).await?),
        Destination::Snowflake(snowflake) => {
            Ok(stage_parquet(snowflake, &session.load_id, session.parts, &batch).await?)
        }
        Destination::Files(_) => Err(no_sql()),
    }
}

/// Make every staged batch visible in the staging table (Snowflake: `COPY` the files).
pub(crate) async fn finish_staging(
    destination: &Destination,
    session: &StagingSession,
) -> Result<(), LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(_) => Ok(()),
        Destination::Snowflake(_) if session.parts == 0 => Ok(()),
        Destination::Snowflake(snowflake) => {
            Ok(copy_staged(snowflake, &session.staging, &session.load_id).await?)
        }
        Destination::Files(_) => Err(no_sql()),
    }
}

/// Drop the load's staging and swap tables and any staged files; best effort, after success or failure.
pub(crate) async fn cleanup_staging(
    destination: &Destination,
    session: &StagingSession,
    warehouse: Warehouse,
) {
    let drops: Vec<String> = [session.staging.clone(), swap_table(&session.staging)]
        .iter()
        .map(|table| format!("DROP TABLE IF EXISTS {}", qualified_name(table, warehouse)))
        .collect();
    if let Err(error) = execute(destination, drops).await {
        tracing::warn!(%error, table = %session.staging.table, "cannot drop the staging table");
    }
    if let Destination::Snowflake(snowflake) = destination
        && session.parts > 0
        && let Err(error) = remove_staged(snowflake, &session.load_id).await
    {
        tracing::warn!(%error, load_id = %session.load_id, "cannot remove the staged files");
    }
}
