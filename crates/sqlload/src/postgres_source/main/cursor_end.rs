//! Where an integer cursor's windows end when no end is given.

use tokio_postgres::Client;

use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::SourceTable;

/// The largest value of integer column `column` in `table` and one past it, as text; `None` when the table has no value there.
pub async fn cursor_end(
    client: &Client,
    table: &SourceTable,
    column: &str,
) -> Result<Option<(String, String)>, PostgresError> {
    let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
    let column = quote(column);
    let sql = format!(
        "SELECT max({column})::text, (max({column})::numeric + 1)::text FROM {}.{}",
        quote(&table.schema),
        quote(&table.name)
    );
    let row = client.query_one(&sql, &[]).await?;
    let maximum: Option<String> = row.get(0);
    let end: Option<String> = row.get(1);
    Ok(maximum.zip(end))
}
