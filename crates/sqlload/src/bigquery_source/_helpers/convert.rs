//! Read API batches in the read columns' types.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Schema, SchemaRef};

use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{ReadColumn, SourceColumn, SourceTable};
use crate::row_batches::main::normalise_batch::normalise_batch;

/// How the table spells its column `name`, which the read asks for and gets back: its source spelling when the load renames it.
pub(crate) fn source_name<'a>(table: &'a SourceTable, name: &'a str) -> &'a str {
    table
        .columns
        .iter()
        .find(|column| column.name == name)
        .and_then(|column| column.source_spelling.as_deref())
        .unwrap_or(name)
}

/// The column of `table` the source spells `spelling` and the load renames.
fn spelled<'a>(table: &'a SourceTable, spelling: &str) -> Option<&'a SourceColumn> {
    table
        .columns
        .iter()
        .find(|column| column.source_spelling.as_deref() == Some(spelling))
}

/// `batch` with each field the load renames under its loaded name, so the read columns find it.
pub(crate) fn loaded_fields(
    batch: &RecordBatch,
    table: &SourceTable,
) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let fields: Vec<arrow_schema::FieldRef> = schema
        .fields()
        .iter()
        .map(|field| match spelled(table, field.name()) {
            Some(column) => Arc::new(field.as_ref().clone().with_name(&column.name)),
            None => field.clone(),
        })
        .collect();
    batch
        .clone()
        .with_schema(Arc::new(Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        )))
        .map_err(|error| error.to_string())
}

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
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
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
            cast_with_options(read, field.data_type(), &strict)
                .map_err(|error| format!("{}: {error}", field.name()))
        })
        .collect::<Result<_, String>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())
}
