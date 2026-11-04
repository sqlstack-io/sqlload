use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray, new_null_array};
use arrow_cast::cast;
use arrow_schema::{Field, Schema, SchemaRef};

use crate::column_types::main::arrow_field::arrow_field;
use crate::files_source::constants::SOURCE_FILE_COLUMN;
use crate::files_source::models::DataFile;
use crate::schema_plan::models::DesiredColumn;

/// The Arrow schema of the batches handed on: one field per output column.
pub(crate) fn output_schema(columns: &[DesiredColumn]) -> Result<SchemaRef, String> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|column| {
            arrow_field(&column.name, &column.column_type)
                .ok_or_else(|| format!("{} has no Arrow representation", column.name))
        })
        .collect::<Result<_, String>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// `batch` from `file` reshaped to `schema`: data columns cast, missing ones NULL, partition and file columns filled in.
pub(crate) fn shape_batch(
    batch: &RecordBatch,
    file: &DataFile,
    file_name: &str,
    schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    let mut constants: Vec<(&str, &str)> = file
        .partitions
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    constants.push((SOURCE_FILE_COLUMN, file_name));
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| shape_column(batch, &constants, field))
        .collect::<Result<_, String>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())
}

/// One output column: a constant (partition value or file name), the file's column cast, or NULLs.
fn shape_column(
    batch: &RecordBatch,
    constants: &[(&str, &str)],
    field: &Field,
) -> Result<ArrayRef, String> {
    let rows = batch.num_rows();
    let constant = constants.iter().find(|(key, _)| key == field.name());
    let source: ArrayRef = match (constant, batch.column_by_name(field.name())) {
        (Some((_, value)), _) => Arc::new(StringArray::from(vec![*value; rows])),
        (None, Some(column)) => column.clone(),
        (None, None) => return Ok(new_null_array(field.data_type(), rows)),
    };
    cast(&source, field.data_type()).map_err(|error| {
        format!(
            "{}: cannot convert to {}: {error}",
            field.name(),
            field.data_type()
        )
    })
}
