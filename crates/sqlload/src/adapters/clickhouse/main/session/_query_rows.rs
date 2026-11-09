//! Read a query's rows.

use crate::adapters::clickhouse::_helpers::session::http::send;
use crate::adapters::clickhouse::_helpers::session::rows::text_rows;
use crate::adapters::clickhouse::models::ClickHouseDestination;
use crate::http::errors::ApiError;

/// Every row `query` returns, each value as text (`None` for NULL), read as `JSONCompactStrings`.
pub(in crate::adapters) async fn query_rows(
    destination: &ClickHouseDestination,
    query: &str,
) -> Result<Vec<Vec<Option<String>>>, ApiError> {
    let text = send(
        destination,
        None,
        format!("{query} FORMAT JSONCompactStrings").into_bytes(),
    )
    .await?;
    text_rows(&text)
}
