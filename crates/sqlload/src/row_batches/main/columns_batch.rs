//! Gathered rows as a typed Arrow batch.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};

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
        let field = inferred
            .as_ref()
            .and_then(|inferred| arrow_field(&name, inferred))
            .unwrap_or_else(|| Field::new(&name, DataType::Null, true));
        arrays.push(column_array(cells, inferred.as_ref(), micros));
        fields.push(field);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    let schema = marked(Schema::new(fields), &flattened);
    RecordBatch::try_new_with_options(Arc::new(schema), arrays, &options)
        .map_err(|error| error.to_string())
}
