//! Batches as the driver gives them, converted to the columns' planned types.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{Schema, SchemaRef};

use crate::column_types::main::arrow_field::arrow_field;
use crate::postgres_source::models::ReadColumn;

/// The schema of the batches a read passes on: each read column under its name, in its planned type.
pub(crate) fn output_schema(columns: &[ReadColumn]) -> Result<SchemaRef, String> {
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        let field = arrow_field(&column.name, &column.output).ok_or_else(|| {
            format!(
                "{} cannot be read as {}; pin another type",
                column.name, column.output
            )
        })?;
        fields.push(field);
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// `batch`'s columns, by position, converted to `schema`'s types; a value that does not convert fails, naming its column.
pub(crate) fn converted(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, String> {
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        let cast = cast_with_options(array, field.data_type(), &strict).map_err(|error| {
            format!(
                "{} does not load as {}: {error}",
                field.name(),
                field.data_type()
            )
        })?;
        arrays.push(cast);
    }
    RecordBatch::try_new(schema.clone(), arrays).map_err(|error| error.to_string())
}
