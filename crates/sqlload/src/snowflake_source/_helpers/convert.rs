use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Schema, SchemaRef};

use crate::column_types::main::arrow_field::arrow_field;
use crate::postgres_source::models::ReadColumn;

/// The Arrow schema the read columns are handed on in.
pub(crate) fn output_schema(columns: &[ReadColumn]) -> Result<SchemaRef, String> {
    let fields = columns
        .iter()
        .map(|column| {
            arrow_field(&column.name, &column.output)
                .ok_or_else(|| format!("{} has no Arrow representation", column.name))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// An unloaded batch in `schema`: times and timestamps from whole microseconds, every other column cast to its type; a value that does not convert is an error, never NULL.
pub(crate) fn convert_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| {
            let unloaded = batch
                .column_by_name(field.name())
                .ok_or_else(|| format!("the unloaded files have no column {}", field.name()))?;
            let counted = matches!(
                field.data_type(),
                DataType::Timestamp(..) | DataType::Time64(_)
            );
            let unloaded = match counted {
                true => cast_with_options(unloaded, &DataType::Int64, &strict),
                false => Ok(unloaded.clone()),
            };
            unloaded
                .and_then(|array| cast_with_options(&array, field.data_type(), &strict))
                .map_err(|error| format!("{}: {error}", field.name()))
        })
        .collect::<Result<_, String>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())
}
