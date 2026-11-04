//! Read a cursor's position from the destination.

use crate::column_types::models::{ColumnType, Warehouse};
use crate::write_sql::_helpers::identifiers::{qualified, quote};
use crate::write_sql::models::{CursorQuery, CursorRead, CursorValue};

/// A query for `MAX(column)` whose text never depends on session time zones or output formats; `None` if a lookback needs a time column.
pub fn cursor_query(request: &CursorQuery<'_>) -> Option<CursorRead> {
    let column = quote(request.column, request.warehouse);
    let target = qualified(request.table, request.warehouse);
    let snowflake = request.warehouse == Warehouse::Snowflake;
    let (expression, value) = match request.column_type {
        ColumnType::TimestampTz | ColumnType::TimestampNtz => (
            match snowflake {
                true => format!("DATE_PART(epoch_microsecond, MAX({column}))"),
                false => format!("CAST(epoch_us(MAX({column})) AS VARCHAR)"),
            },
            CursorValue::EpochMicros {
                zoned: request.column_type == &ColumnType::TimestampTz,
            },
        ),
        ColumnType::Date => (
            match snowflake {
                true => format!("TO_VARCHAR(MAX({column}), 'YYYY-MM-DD')"),
                false => format!("strftime(MAX({column}), '%Y-%m-%d')"),
            },
            CursorValue::Date,
        ),
        _ if request.lookback.is_some() => return None,
        _ => (
            match snowflake {
                true => format!("TO_VARCHAR(MAX({column}))"),
                false => format!("CAST(MAX({column}) AS VARCHAR)"),
            },
            CursorValue::Plain,
        ),
    };
    Some(CursorRead {
        sql: format!("SELECT {expression} FROM {target}"),
        value,
    })
}
