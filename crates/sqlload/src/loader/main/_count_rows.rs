//! How many rows of a source table a condition selects, counted by the database.

use crate::loader::errors::LoaderError;
use crate::loader::models::SourceReader;
use crate::postgres_source::models::SourceTable;

/// How many rows of `table` (or of its query `sql`) `filter` selects, counted by the source's database; `None` for a files or Google Sheets source, which has no database to ask.
pub(in crate::loader) async fn count_rows(
    reader: &SourceReader,
    (table, sql): (&SourceTable, Option<&str>),
    filter: &str,
) -> Result<Option<u64>, LoaderError> {
    Ok(Some(match reader {
        SourceReader::Postgres(client, _) => {
            crate::postgres_source::main::count_rows::count_rows(client, table, filter).await?
        }
        SourceReader::Snowflake(snowflake) => {
            crate::snowflake_source::main::count_rows::count_rows(snowflake, (table, sql), filter)
                .await?
        }
        SourceReader::BigQuery(bigquery) => {
            crate::bigquery_source::main::count_rows::count_rows(bigquery, (table, sql), filter)
                .await?
        }
        SourceReader::Databricks(databricks) => {
            crate::databricks_source::main::count_rows::count_rows(
                databricks,
                (table, sql),
                filter,
            )
            .await?
        }
        #[cfg(feature = "odbc")]
        SourceReader::Odbc(odbc) => {
            crate::odbc_source::main::count_rows::count_rows(odbc, (table, sql), filter).await?
        }
        #[cfg(feature = "duckdb")]
        SourceReader::DuckDb(duckdb) => {
            crate::duckdb_source::main::count_rows::count_rows(duckdb, (table, sql), filter)
                .await?
        }
        SourceReader::Files(_) | SourceReader::Sheets(..) => return Ok(None),
    }))
}
