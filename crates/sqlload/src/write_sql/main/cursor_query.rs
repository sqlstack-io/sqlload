//! Read a cursor's position from the destination.

use crate::column_types::models::{ColumnType, Warehouse};
use crate::write_sql::_helpers::identifiers::{qualified, quote};
use crate::write_sql::models::CursorQuery;

/// `MAX(column)` minus `lookback` as text Postgres accepts (UTC wall-clock arithmetic on DuckDB, which lacks ICU); `None` if a lookback needs a time column.
pub fn cursor_query(request: &CursorQuery<'_>) -> Option<String> {
    let CursorQuery {
        table,
        column,
        column_type,
        lookback,
        warehouse,
    } = *request;
    let column = quote(column, warehouse);
    let target = qualified(table, warehouse);
    let is_time = matches!(
        column_type,
        ColumnType::TimestampTz | ColumnType::TimestampNtz | ColumnType::Date
    );
    if lookback.is_some() && !is_time {
        return None;
    }
    let seconds = lookback.map_or(0, |lookback| lookback.as_secs());
    let maximum = match (warehouse, column_type) {
        (Warehouse::Snowflake, _) if is_time => {
            format!("DATEADD(second, -{seconds}, MAX({column}))")
        }
        (_, ColumnType::TimestampTz) => {
            format!("CAST(MAX({column}) AS TIMESTAMP) - INTERVAL '{seconds} seconds'")
        }
        _ if is_time => format!("MAX({column}) - INTERVAL '{seconds} seconds'"),
        _ => format!("MAX({column})"),
    };
    Some(match (warehouse, column_type) {
        (Warehouse::Snowflake, ColumnType::TimestampTz) => format!(
            "SELECT TO_VARCHAR({maximum}, 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM') FROM {target}"
        ),
        (Warehouse::Snowflake, ColumnType::TimestampNtz) => {
            format!("SELECT TO_VARCHAR({maximum}, 'YYYY-MM-DD HH24:MI:SS.FF6') FROM {target}")
        }
        (Warehouse::Snowflake, _) => format!("SELECT TO_VARCHAR({maximum}) FROM {target}"),
        (_, ColumnType::TimestampTz) => {
            format!("SELECT CAST({maximum} AS VARCHAR) || '+00' FROM {target}")
        }
        _ => format!("SELECT CAST({maximum} AS VARCHAR) FROM {target}"),
    })
}
