use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::SchemaRef;

use crate::load_naming::models::MetadataRole;
use crate::row_ids::main::random_row_ids::random_row_ids;

/// `batch` as the table's columns: each cast to the file type, and the load id and time filled in where `metadata`, aligned with `schema`'s fields, says the load adds them.
pub(crate) fn shape_batch(
    batch: &RecordBatch,
    (schema, metadata): (&SchemaRef, &[Option<MetadataRole>]),
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
        .zip(metadata)
        .map(|(field, role)| -> Result<ArrayRef, String> {
            match (role, batch.column_by_name(field.name())) {
                (Some(MetadataRole::LoadId), _) => {
                    Ok(Arc::new(StringArray::from(vec![load_id; rows])))
                }
                (Some(MetadataRole::LoadedAt), _) => Ok(Arc::new(
                    TimestampMicrosecondArray::from(vec![loaded_at_micros; rows])
                        .with_timezone("UTC"),
                )),
                (Some(MetadataRole::RowId), _) => {
                    Ok(Arc::new(StringArray::from(random_row_ids(rows)?)))
                }
                (None, Some(column)) => cast_with_options(column, field.data_type(), &strict)
                    .map_err(|error| {
                        format!(
                            "{}: cannot convert to {}: {error}",
                            field.name(),
                            field.data_type()
                        )
                    }),
                (None, None) => Err(format!("the source batch has no column {}", field.name())),
            }
        })
        .collect::<Result<_, String>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())
}
