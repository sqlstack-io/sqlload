//! Expanding one listed object column of a batch.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};

use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::column_types::models::ColumnType;
use crate::row_batches::constants::FLATTEN_SEPARATOR;
use crate::row_batches::main::columns_batch::columns_batch;
use crate::row_batches::main::json_rows::json_rows;
use crate::row_batches::models::{JsonLayout, JsonRead};

/// `columns` as a batch of `rows` rows.
pub(crate) fn rebuilt(
    columns: Vec<(Arc<Field>, ArrayRef)>,
    rows: usize,
) -> Result<RecordBatch, String> {
    let (fields, arrays): (Vec<Arc<Field>>, Vec<ArrayRef>) = columns.into_iter().unzip();
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)
        .map_err(|error| error.to_string())
}

/// The listed column's objects as one column per key, named `<object>__<key>`; a name `schema` already has is an error.
pub(crate) fn key_columns(
    field: &Field,
    array: &ArrayRef,
    schema: &Schema,
) -> Result<Vec<(Arc<Field>, ArrayRef)>, String> {
    let document = object_lines(field, array)?;
    let read = JsonRead {
        layout: JsonLayout::Document,
        path: None,
        batch_rows: array.len().max(1),
    };
    let mut gathered: Vec<RecordBatch> = Vec::new();
    json_rows(document.as_bytes(), &read, |columns| {
        gathered.push(columns_batch(columns)?);
        Ok(())
    })
    .map_err(|error| format!("flatten {}: {error}", field.name()))?;
    named_columns(field, gathered, schema)
}

/// The expanded keys' columns named `<object>__<key>`; a name `schema` already has is an error.
fn named_columns(
    field: &Field,
    gathered: Vec<RecordBatch>,
    schema: &Schema,
) -> Result<Vec<(Arc<Field>, ArrayRef)>, String> {
    let mut named: Vec<(Arc<Field>, ArrayRef)> = Vec::new();
    for expanded in gathered {
        for (key, values) in expanded.schema().fields().iter().zip(expanded.columns()) {
            let name = format!("{}{FLATTEN_SEPARATOR}{}", field.name(), key.name());
            if schema.fields().iter().any(|other| *other.name() == name) {
                return Err(format!(
                    "flatten {}: the key {} gives column {name}, which the data already has; rename one of them or leave {} unflattened",
                    field.name(),
                    key.name(),
                    field.name()
                ));
            }
            named.push((
                Arc::new(key.as_ref().clone().with_name(name)),
                values.clone(),
            ));
        }
    }
    Ok(named)
}

/// The listed column as one JSON object per line (`{}` for NULL), or an error naming the first row whose value is not an object.
fn object_lines(field: &Field, array: &ArrayRef) -> Result<String, String> {
    let is_json = field
        .metadata()
        .get(ARROW_TYPE_METADATA_KEY)
        .is_some_and(|kind| *kind == ColumnType::Json.to_string());
    let shape_error = |row: usize, shape: String| {
        format!(
            "flatten {}: row {} holds {shape}, not an object; the shape changed, so leave {} unflattened or fix the source",
            field.name(),
            row + 1,
            field.name()
        )
    };
    let texts = match (array.data_type(), is_json) {
        (DataType::Null, _) => None,
        (DataType::Utf8, true) => Some(array.as_string::<i32>()),
        (data_type, _) => {
            let row = (0..array.len())
                .find(|row| array.is_valid(*row))
                .unwrap_or_default();
            return Err(shape_error(row, format!("a {data_type} value")));
        }
    };
    let mut document = String::new();
    for row in 0..array.len() {
        let text = texts
            .filter(|texts| texts.is_valid(row))
            .map_or("null", |texts| texts.value(row).trim());
        let line = match (text, text.starts_with('{'), text.starts_with('[')) {
            ("null", _, _) => "{}",
            (object, true, _) => object,
            (_, false, true) => return Err(shape_error(row, "a list".to_owned())),
            (_, false, false) => return Err(shape_error(row, "a scalar".to_owned())),
        };
        document.push_str(line);
        document.push('\n');
    }
    Ok(document)
}
