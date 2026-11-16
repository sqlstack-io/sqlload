//! Gathered rows as a typed Arrow batch.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};

use crate::column_types::constants::PYTHON_BYTES_METADATA_KEY;
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::ColumnType;
use crate::row_batches::_helpers::cells::{column_array, column_type};
use crate::row_batches::_helpers::flatten::marked;
use crate::row_batches::models::RowColumns;

/// `columns` as one batch, each column typed by the files sources' inference rules, or, `as_text` (`infer_types = false`), each column of scalars `text` as written (nested values are `json` text; a column with only NULLs has Arrow's null type); the schema's metadata lists the columns flattening made.
pub fn columns_batch(columns: RowColumns, as_text: bool) -> Result<RecordBatch, String> {
    let flattened = columns.flattened().to_vec();
    let (rows, columns) = columns.into_columns();
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (name, cells) in columns {
        let (inferred, micros) = match column_type(&cells) {
            (Some(inferred), _) if as_text && inferred != ColumnType::Json => {
                (Some(ColumnType::Text), Vec::new())
            }
            typed => typed,
        };
        let field = column_field(&name, inferred.as_ref());
        arrays.push(column_array(cells, inferred.as_ref(), micros));
        fields.push(field);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    let schema = marked(Schema::new(fields), &flattened);
    RecordBatch::try_new_with_options(Arc::new(schema), arrays, &options)
        .map_err(|error| error.to_string())
}

/// The field for column `name` of `inferred` type: Arrow's null type when there is none, and a binary column, which only Python bytes make, marked as such.
fn column_field(name: &str, inferred: Option<&ColumnType>) -> Field {
    match inferred.and_then(|inferred| arrow_field(name, inferred)) {
        Some(field) if field.data_type() == &DataType::Binary => field.with_metadata(
            HashMap::from([(PYTHON_BYTES_METADATA_KEY.to_owned(), "true".to_owned())]),
        ),
        Some(field) => field,
        None => Field::new(name, DataType::Null, true),
    }
}
