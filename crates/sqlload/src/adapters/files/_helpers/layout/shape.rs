use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::SchemaRef;

use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};

/// `batch` as the table's columns: each cast to the file type, and the load id and time filled in.
pub(crate) fn shape_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    load_id: &str,
    loaded_at_micros: i64,
) -> Result<RecordBatch, String> {
    let rows = batch.num_rows();
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| -> Result<ArrayRef, String> {
            match (field.name().as_str(), batch.column_by_name(field.name())) {
                (LOAD_ID_COLUMN, _) => Ok(Arc::new(StringArray::from(vec![load_id; rows]))),
                (LOADED_AT_COLUMN, _) => Ok(Arc::new(
                    TimestampMicrosecondArray::from(vec![loaded_at_micros; rows])
                        .with_timezone("UTC"),
                )),
                (name, Some(column)) => cast_with_options(column, field.data_type(), &strict)
                    .map_err(|error| {
                        format!("{name}: cannot convert to {}: {error}", field.data_type())
                    }),
                (name, None) => Err(format!("the source batch has no column {name}")),
            }
        })
        .collect::<Result<_, String>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())
}
