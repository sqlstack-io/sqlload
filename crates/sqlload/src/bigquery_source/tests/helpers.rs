use std::sync::Arc;

use arrow_array::builder::{Float64Builder, ListBuilder};
use arrow_array::{ArrayRef, Decimal128Array, Int64Array, StringArray, StructArray};
use arrow_schema::{DataType, Field};
use serde_json::json;

use crate::bigquery_source::_helpers::type_map::source_column;
use crate::postgres_source::models::SourceTable;

/// A `STRUCT<a INT64, s STRING>` holding `(1, "x")`.
pub(crate) fn record() -> ArrayRef {
    Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("s", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
        ),
    ]))
}

/// A `NUMERIC` holding 1.25, as the Read API sends it: `Decimal128(38, 9)`.
pub(crate) fn numeric() -> ArrayRef {
    Arc::new(
        Decimal128Array::from(vec![1_250_000_000_i128])
            .with_precision_and_scale(38, 9)
            .expect("a NUMERIC column"),
    )
}

/// A repeated `FLOAT64` holding `[NaN, 1.5]`.
pub(crate) fn floats() -> ArrayRef {
    let mut list = ListBuilder::new(Float64Builder::new());
    list.values().append_value(f64::NAN);
    list.values().append_value(1.5);
    list.append(true);
    Arc::new(list.finish())
}

/// `marts.orders` with an `INT64` id, a repeated `STRING` and an `INTERVAL`.
pub(crate) fn orders() -> SourceTable {
    SourceTable {
        schema: "marts".to_owned(),
        name: "orders".to_owned(),
        columns: [
            json!({"name": "id", "type": "INT64"}),
            json!({"name": "tags", "type": "STRING", "mode": "REPEATED"}),
            json!({"name": "span", "type": "INTERVAL"}),
        ]
        .iter()
        .map(source_column)
        .collect(),
        primary_key: Vec::new(),
        indexed_columns: Vec::new(),
        estimated_rows: -1,
        estimated_bytes: -1,
        comment: None,
        listed_keys: Vec::new(),
    }
}
