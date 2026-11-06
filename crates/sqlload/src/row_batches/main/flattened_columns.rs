//! Which columns of a batch flattening made.

use arrow_schema::Schema;

use crate::row_batches::_helpers::flatten::flattened_names;

/// The columns of a batch with this schema that flattening a listed object made, as its metadata lists them.
pub fn flattened_columns(schema: &Schema) -> Vec<String> {
    flattened_names(schema)
}
