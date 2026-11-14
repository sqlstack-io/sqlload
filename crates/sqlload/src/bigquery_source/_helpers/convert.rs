//! Read API batches in the read columns' types.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};

use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::main::exact_cast::exact_cast;
use crate::column_types::models::ColumnType;
use crate::postgres_source::models::ReadColumn;
use crate::row_batches::main::normalise_batch::normalise_batch;

/// The Arrow schema the read columns are handed on in, with the columns `json` reads from records and lists.
pub(crate) fn output_schema(columns: &[ReadColumn]) -> Result<(SchemaRef, Vec<bool>), String> {
    let fields = columns
        .iter()
        .map(|column| {
            arrow_field(&column.name, &column.output)
                .ok_or_else(|| format!("{} has no Arrow representation", column.name))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let json: Vec<bool> = columns
        .iter()
        .map(|column| column.output == ColumnType::Json)
        .collect();
    Ok((Arc::new(Schema::new(fields)), json))
}

/// A read batch in `schema`: records and lists of a `json` column as their JSON text, every other column cast to its type; a value that does not convert (such as a `NUMERIC` too wide for a pinned decimal) is an error, never NULL.
pub(crate) fn convert_batch(
    batch: &RecordBatch,
    (schema, json): (&SchemaRef, &[bool]),
) -> Result<RecordBatch, String> {
    let nested =
        |array: &ArrayRef| matches!(array.data_type(), DataType::Struct(_) | DataType::List(_));
    let texts: Vec<(&str, ArrayRef)> = schema
        .fields()
        .iter()
        .zip(json)
        .filter(|(_, json)| **json)
        .filter_map(|(field, _)| {
            Some((
                field.name().as_str(),
                batch.column_by_name(field.name())?.clone(),
            ))
        })
        .filter(|(_, array)| nested(array))
        .collect();
    let texts = match texts.is_empty() {
        true => None,
        false => Some(
            RecordBatch::try_from_iter(texts)
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
                .ok_or_else(|| format!("the read has no column {}", field.name()))?;
            exact_cast(read, field.data_type())
                .map_err(|error| format!("{}: {error}", field.name()))
        })
        .collect::<Result<_, String>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())
}
