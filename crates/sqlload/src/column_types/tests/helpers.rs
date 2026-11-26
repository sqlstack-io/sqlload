use std::sync::Arc;

use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::main::parse_type::parse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::column_types::tests::test_types::RoundTripException;

use arrow_array::ArrayRef;
use arrow_array::builder::{Float64Builder, ListBuilder};

/// A list array with one row per item of `rows`.
pub(crate) fn float_lists(rows: &[&[f64]]) -> ArrayRef {
    let mut lists = ListBuilder::new(Float64Builder::new());
    for values in rows {
        lists.values().append_slice(values);
        lists.append(true);
    }
    Arc::new(lists.finish())
}

/// Every scalar portable type, decimals at the edges of what each warehouse stores alike.
pub(crate) fn portable_types() -> Vec<ColumnType> {
    let decimal = |precision, scale| ColumnType::Decimal { precision, scale };
    vec![
        ColumnType::Bool,
        ColumnType::SmallInt,
        ColumnType::Int,
        decimal(18, 2),
        decimal(38, 0),
        decimal(38, 20),
        decimal(5, 5),
        ColumnType::Float,
        ColumnType::Text,
        ColumnType::Varchar(20),
        ColumnType::Date,
        ColumnType::Time,
        ColumnType::TimestampTz,
        ColumnType::TimestampNtz,
        ColumnType::Json,
        ColumnType::Uuid,
        ColumnType::Binary,
    ]
}

/// `column_type` as `warehouse` renders it, and what that parses back as for a table loading only into it (the error's text when it doesn't parse).
pub(crate) fn round_trip(warehouse: Warehouse, column_type: &ColumnType) -> (String, String) {
    let rendered = sql_adapter(warehouse)
        .render_loader_logical_type(column_type)
        .sql;
    let read_back = parse_type(&rendered, &[warehouse])
        .map(|parsed| parsed.column_type.to_string())
        .unwrap_or_else(|error| error.to_string());
    (rendered, read_back)
}

/// What each portable type, created in `warehouse`, reads back as for a table loading only there: the type, the warehouse type and what it reads back as.
pub(crate) fn read_backs(warehouse: Warehouse) -> Vec<(String, String, String)> {
    portable_types()
        .iter()
        .map(|column_type| {
            let (rendered, read_back) = round_trip(warehouse, column_type);
            (column_type.to_string(), rendered, read_back)
        })
        .collect()
}

/// Each portable type with what it must read back as: itself, unless `exceptions` say otherwise; and the warehouse type of each exception.
pub(crate) fn expected_read_backs(
    exceptions: &[RoundTripException],
) -> Vec<(String, Option<String>, String)> {
    portable_types()
        .iter()
        .map(|column_type| {
            let exception = exceptions
                .iter()
                .find(|exception| exception.column_type == *column_type);
            (
                column_type.to_string(),
                exception.map(|exception| exception.expected_rendered.to_owned()),
                exception.map_or_else(
                    || column_type.to_string(),
                    |exception| exception.expected_read_back.to_string(),
                ),
            )
        })
        .collect()
}

/// `found` (from [`read_backs`]) with the warehouse type kept only where `exceptions` name the type, to compare with [`expected_read_backs`].
pub(crate) fn compared_read_backs(
    found: Vec<(String, String, String)>,
    exceptions: &[RoundTripException],
) -> Vec<(String, Option<String>, String)> {
    found
        .into_iter()
        .map(|(column_type, rendered, read_back)| {
            let listed = exceptions
                .iter()
                .any(|exception| exception.column_type.to_string() == column_type);
            (column_type, listed.then_some(rendered), read_back)
        })
        .collect()
}
