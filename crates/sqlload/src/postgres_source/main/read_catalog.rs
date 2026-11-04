//! Discover source tables from the Postgres catalog.

use tokio_postgres::Client;

use crate::postgres_source::_helpers::catalog::{table_matcher, tables_from_rows};
use crate::postgres_source::constants::{CATALOG_COLUMNS_SQL, CATALOG_PRIMARY_KEYS_SQL};
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::SourceTable;

/// Every table and view whose `schema.table` matches `include` and not `exclude`, with columns and keys.
pub async fn read_catalog(
    client: &Client,
    include: &[String],
    exclude: &[String],
) -> Result<Vec<SourceTable>, PostgresError> {
    let pattern_error = |reason: String| PostgresError::Batch {
        table: "the source selection".to_owned(),
        reason,
    };
    let include = table_matcher(include).map_err(pattern_error)?;
    let exclude = table_matcher(exclude).map_err(pattern_error)?;
    let columns = client.query(CATALOG_COLUMNS_SQL, &[]).await?;
    let keys = client.query(CATALOG_PRIMARY_KEYS_SQL, &[]).await?;
    Ok(tables_from_rows(&columns, &keys, &include, &exclude))
}
