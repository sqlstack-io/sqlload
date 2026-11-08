use std::sync::Arc;

use arrow_array::{ArrayRef, Decimal128Array, RecordBatch};
use arrow_cast::display::array_value_to_string;
use arrow_schema::{DataType, Field, Schema};

use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{PgType, ReadColumn, SourceColumn, SourceTable, WireType};
use crate::snowflake_source::_helpers::convert::{convert_batch, output_schema};

fn column(
    name: &str,
    spelling: Option<&str>,
    type_text: &str,
    column_type: ColumnType,
) -> SourceColumn {
    SourceColumn {
        name: name.to_owned(),
        postgres_type: type_text.to_owned(),
        pg_type: PgType::Other,
        column_type: Ok(column_type),
        nullable: true,
        source_spelling: spelling.map(str::to_owned),
    }
}

/// `MARTS.EVENTS`: two upper-case columns, which load in lower case, and a quoted mixed-case one.
pub(crate) fn events() -> SourceTable {
    SourceTable {
        schema: "MARTS".to_owned(),
        name: "EVENTS".to_owned(),
        columns: vec![
            column(
                "event_id",
                Some("EVENT_ID"),
                "NUMBER(18,0)",
                ColumnType::Int,
            ),
            column("MixedCase", None, "VARCHAR(10)", ColumnType::Varchar(10)),
            column(
                "seen_at",
                Some("SEEN_AT"),
                "TIMESTAMP_TZ(9)",
                ColumnType::TimestampTz,
            ),
        ],
        primary_key: Vec::new(),
        indexed_columns: Vec::new(),
        estimated_rows: 0,
        estimated_bytes: 0,
        comment: None,
    }
}

/// One unloaded column `value` holding `values` as a Parquet decimal of `precision` and `scale`, converted to `output`; each value as text, or the error.
pub(crate) fn convert_values(
    (precision, scale): (u8, i8),
    values: &[Option<i128>],
    output: &ColumnType,
) -> Result<Vec<String>, String> {
    let unloaded = Schema::new(vec![Field::new(
        "value",
        DataType::Decimal128(precision, scale),
        true,
    )]);
    let array = Decimal128Array::from(values.to_vec())
        .with_precision_and_scale(precision, scale)
        .map_err(|error| error.to_string())?;
    let batch = RecordBatch::try_new(Arc::new(unloaded), vec![Arc::new(array) as ArrayRef])
        .map_err(|error| error.to_string())?;
    let schema = output_schema(&[ReadColumn {
        name: "value".to_owned(),
        expression: "\"VALUE\"".to_owned(),
        wire: WireType::Text,
        output: output.clone(),
    }])?;
    let converted = convert_batch(&batch, &schema)?;
    (0..converted.num_rows())
        .map(|row| {
            array_value_to_string(converted.column(0), row).map_err(|error| error.to_string())
        })
        .collect()
}
