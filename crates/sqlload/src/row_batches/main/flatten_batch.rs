//! Expand the objects of a batch that a table flattens into `<object>__<key>` columns.

use arrow_array::RecordBatch;

use crate::row_batches::_helpers::flatten::{expanded_columns, rebuilt};
use crate::row_batches::models::Flatten;

/// `batch` with each column holding objects that `flatten` flattens (a listed path, or under `"all"` any column of objects) replaced, where it stands, by one column per key of its objects (`customer` -> `customer__country`, ...), and those flattened in turn down to its `max_depth` (`customer__address__city`): a struct's children keep their types, JSON objects are typed by inference, values that are lists stay `json`, and a NULL or missing object gives NULLs. An object `flatten` keeps whole stays one column. The schema's metadata lists the columns flattening made. A listed column holding anything but objects, a new name another column or object already gives, or an object pinned as another type than `json` is an error naming it.
pub fn flatten_batch(batch: &RecordBatch, flatten: &Flatten) -> Result<RecordBatch, String> {
    if flatten.is_none() {
        return Ok(batch.clone());
    }
    match expanded_columns(batch, flatten)? {
        Some((columns, flattened)) => rebuilt(columns, batch.num_rows(), &flattened),
        None => Ok(batch.clone()),
    }
}
