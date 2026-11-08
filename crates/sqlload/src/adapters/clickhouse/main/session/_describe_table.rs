//! Read a table's live columns.

use crate::adapters::clickhouse::main::session::_query_rows::query_rows;
use crate::adapters::clickhouse::models::{ClickHouseDestination, ClickHouseSql};
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::LiveColumnType;
use crate::http::errors::ApiError;
use crate::schema_plan::models::LiveColumn;

/// The table's columns in order, or `None` if it does not exist, from `system.columns`; the database is the table's schema.
pub(in crate::adapters) async fn describe_table(
    destination: &ClickHouseDestination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, ApiError> {
    let adapter = ClickHouseSql;
    let query = format!(
        "SELECT name, type FROM system.columns WHERE database = {} AND table = {} ORDER BY position",
        adapter.render_loader_value_literal(&table.schema),
        adapter.render_loader_value_literal(&table.table)
    );
    let columns: Vec<LiveColumn> = query_rows(destination, &query)
        .await?
        .into_iter()
        .map(|row| {
            let mut values = row.into_iter().map(Option::unwrap_or_default);
            let name = values.next().unwrap_or_default();
            let data_type = values.next().unwrap_or_default();
            LiveColumn {
                name,
                column_type: adapter.live_type(&LiveColumnType {
                    data_type,
                    ..LiveColumnType::default()
                }),
                fraction_digits: None,
            }
        })
        .collect();
    Ok((!columns.is_empty()).then_some(columns))
}
