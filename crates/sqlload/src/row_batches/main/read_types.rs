//! A batch converted to the types it loads in.

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, SchemaRef};

use crate::column_types::main::exact_cast::exact_cast;
use crate::row_batches::_helpers::cells::nonfinite_problem;
use crate::row_batches::main::json_lists::json_lists;

/// `batch` with `target`'s columns (others dropped, missing ones NULL), each converted to its type (an array from the list, or from the JSON text a list arrives as); a value that does not convert is an error naming the column, and a NaN or infinity in a column turning integer or decimal names its row too, counting `first_row` rows before the batch.
pub fn read_types(
    batch: &RecordBatch,
    target: &SchemaRef,
    first_row: u64,
) -> Result<RecordBatch, String> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let column = batch
            .column_by_name(field.name())
            .cloned()
            .unwrap_or_else(|| arrow_array::new_null_array(field.data_type(), batch.num_rows()));
        let converted = match (field.data_type(), column.data_type()) {
            (DataType::List(_), DataType::Utf8) => json_lists(&column, field.data_type())
                .map_err(|error| format!("column {}: {error}", field.name())),
            _ => exact_cast(&column, field.data_type()).map_err(|error| {
                nonfinite_problem(field, &column, first_row).unwrap_or_else(|| {
                    format!(
                        "column {}: {error}; pin a type for {} in columns that holds every value",
                        field.name(),
                        field.name()
                    )
                })
            }),
        };
        arrays.push(converted?);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(target.clone(), arrays, &options)
        .map_err(|error| error.to_string())
}
