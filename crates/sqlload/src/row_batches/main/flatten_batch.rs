//! Expand the listed top-level objects of a batch one level into `<object>__<key>` columns.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Field;

use crate::row_batches::_helpers::flatten::{key_columns, rebuilt};

/// `batch` with each column named in `flatten` replaced, where it stands, by one column per key of its objects (`customer` -> `customer__country`, ...), typed by inference; values that are objects or lists stay `json`, and a NULL or missing object gives NULLs. A listed column that holds anything but objects, or a new name that is already a column, is an error naming the column, the row and both shapes.
pub fn flatten_batch(batch: &RecordBatch, flatten: &[String]) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let mut columns: Vec<(Arc<Field>, ArrayRef)> = Vec::with_capacity(schema.fields().len());
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        match flatten.contains(field.name()) {
            false => columns.push((field.clone(), array.clone())),
            true => columns.extend(key_columns(field, array, &schema)?),
        }
    }
    rebuilt(columns, batch.num_rows())
}
