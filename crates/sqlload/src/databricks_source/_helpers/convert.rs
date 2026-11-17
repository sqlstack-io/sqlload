//! Result batches in the read columns' types.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};

use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::main::exact_cast::exact_cast;
use crate::postgres_source::models::ReadColumn;
use crate::row_batches::main::normalise_batch::normalise_batch;

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

/// Whether Databricks sent `array` as nested Arrow (an array, map or struct) rather than its JSON text.
fn nested(array: &ArrayRef) -> bool {
    matches!(
        array.data_type(),
        DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_) | DataType::Map(..)
    )
}

/// A result batch in `schema`: a nested column as its JSON text, every other column cast to its type; a value that does not convert (such as a decimal too wide for a pinned one) is an error, never NULL.
pub(crate) fn convert_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    let nested_columns: Vec<(&str, ArrayRef)> = schema
        .fields()
        .iter()
        .filter_map(|field| {
            Some((
                field.name().as_str(),
                batch.column_by_name(field.name())?.clone(),
            ))
        })
        .filter(|(_, array)| nested(array))
        .collect();
    let texts = match nested_columns.is_empty() {
        true => None,
        false => Some(
            RecordBatch::try_from_iter(nested_columns)
                .map_err(|error| error.to_string())
                .and_then(|nested| normalise_batch(&nested, &[]))?,
        ),
    };
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| {
            let read = texts
                .as_ref()
                .and_then(|texts| texts.column_by_name(field.name()))
                .or_else(|| batch.column_by_name(field.name()))
                .ok_or_else(|| format!("the result has no column {}", field.name()))?;
            exact_cast(read, field.data_type())
                .map_err(|error| format!("{}: {error}", field.name()))
        })
        .collect::<Result<_, String>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())
}
