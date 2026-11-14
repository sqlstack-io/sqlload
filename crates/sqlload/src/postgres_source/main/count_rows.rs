//! Count a table's rows that a condition selects.

use tokio_postgres::Client;

use crate::postgres_source::_helpers::type_map::quote_identifier;
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::SourceTable;

/// How many rows of `table` `filter` selects.
pub async fn count_rows(
    client: &Client,
    table: &SourceTable,
    filter: &str,
) -> Result<u64, PostgresError> {
    let sql = format!(
        "SELECT count(*) FROM {}.{} WHERE {filter}",
        quote_identifier(&table.schema),
        quote_identifier(&table.name)
    );
    let row = client.query_one(&sql, &[]).await?;
    let count: i64 = row.get(0);
    Ok(u64::try_from(count).unwrap_or_default())
}
