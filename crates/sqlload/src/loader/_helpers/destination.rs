use arrow_array::RecordBatch;

use crate::duckdb_destination::main::append_batch::append_batch;
use crate::duckdb_destination::main::describe_table::describe_table;
use crate::duckdb_destination::main::execute_statements::execute_statements;
use crate::duckdb_destination::main::query_text::query_text;
use crate::loader::errors::LoaderError;
use crate::loader::models::Destination;
use crate::schema_plan::models::LiveColumn;
use crate::write_sql::models::TableName;

/// The table's live columns, `None` if it does not exist.
pub(crate) async fn live_columns(
    destination: &Destination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, LoaderError> {
    match destination {
        Destination::DuckDb(duckdb) => Ok(describe_table(duckdb, table).await?),
    }
}

/// Run statements in order.
pub(crate) async fn execute(
    destination: &Destination,
    statements: Vec<String>,
) -> Result<(), LoaderError> {
    match destination {
        Destination::DuckDb(duckdb) => Ok(execute_statements(duckdb, statements).await?),
    }
}

/// One text value from `query`.
pub(crate) async fn single_text(
    destination: &Destination,
    query: String,
) -> Result<Option<String>, LoaderError> {
    match destination {
        Destination::DuckDb(duckdb) => Ok(query_text(duckdb, query).await?),
    }
}

/// Add a batch of rows to the staging table.
pub(crate) async fn stage_batch(
    destination: &Destination,
    staging: &TableName,
    batch: RecordBatch,
) -> Result<(), LoaderError> {
    match destination {
        Destination::DuckDb(duckdb) => Ok(append_batch(duckdb, staging, batch).await?),
    }
}
