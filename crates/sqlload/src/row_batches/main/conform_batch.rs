//! Cast a batch to the merged schema.

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, new_null_array};
use arrow_cast::cast::cast;
use arrow_schema::SchemaRef;

/// `batch` with `schema`'s columns in its order: missing columns are NULL and differing types are cast; a column `schema` lacks is an error.
pub fn conform_batch(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, String> {
    for field in batch.schema().fields() {
        if schema.column_with_name(field.name()).is_none() {
            return Err(format!(
                "column {} is not in the merged schema",
                field.name()
            ));
        }
    }
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| match batch.column_by_name(field.name()) {
            Some(column) if column.data_type() == field.data_type() => Ok(column.clone()),
            Some(column) => cast(column, field.data_type())
                .map_err(|error| format!("column {}: {error}", field.name())),
            None => Ok(new_null_array(field.data_type(), batch.num_rows())),
        })
        .collect::<Result<_, String>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), arrays, &options)
        .map_err(|error| error.to_string())
}
