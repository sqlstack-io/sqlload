use arrow_array::RecordBatch;

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
use crate::loader::errors::LoaderError;
use crate::loader::models::Destination;
use crate::schema_plan::models::LiveColumn;
use crate::snowflake_destination::main::describe_table::describe_table as describe_snowflake;
use crate::snowflake_destination::main::query_text::query_text as query_snowflake;
use crate::snowflake_destination::main::query_texts::query_texts as query_snowflake_texts;
use crate::snowflake_destination::main::remove_stale::remove_stale;
use crate::snowflake_destination::main::run_statements::run_statements;
use crate::snowflake_destination::main::stage_parquet::stage_parquet;
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

/// The destination for one table's load: DuckDB gets a connection of its own, so the table's transactions never mix with another table loading at the same time.
pub(crate) fn table_destination(destination: &Destination) -> Result<Destination, LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(Destination::DuckDb(own_connection(duckdb)?)),
        other => Ok(other.clone()),
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

/// Upload one staged Parquet file to `key` on the user stage (Snowflake).
pub(crate) async fn stage_file(
    destination: &Destination,
    key: &str,
    body: &[u8],
) -> Result<(), LoaderError> {
    match destination {
        Destination::Snowflake(snowflake) => Ok(stage_parquet(snowflake, key, body).await?),
        _ => Err(no_sql()),
    }
}

/// Remove the load folders failed runs left on the user stage over a day ago; a failure only warns.
pub(crate) async fn clean_stage(destination: &Destination) {
    let Destination::Snowflake(snowflake) = destination else {
        return;
    };
    match remove_stale(snowflake).await {
        Ok(0) => {}
        Ok(removed) => tracing::info!(
            removed,
            "removed load folders failed runs left on the user stage"
        ),
        Err(error) => tracing::warn!(%error, "cannot clean up the user stage"),
    }
}
