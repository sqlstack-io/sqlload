//! The large values of an Arrow batch: those DuckDB keeps in memory until a checkpoint.

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;

use crate::memory_budget::constants::LARGE_VALUE_BYTES;

/// Each row's bytes in text and binary values of at least [`LARGE_VALUE_BYTES`]; `None` when the batch has none.
pub fn large_values(batch: &RecordBatch) -> Option<Vec<u64>> {
    let mut rows: Option<Vec<u64>> = None;
    for column in batch.columns() {
        let lengths: Box<dyn Fn(usize) -> usize + '_> = match column.data_type() {
            DataType::Utf8 => {
                let values = column.as_string::<i32>();
                Box::new(move |row| values.value_length(row).unsigned_abs() as usize)
            }
            DataType::LargeUtf8 => {
                let values = column.as_string::<i64>();
                Box::new(move |row| values.value_length(row).unsigned_abs() as usize)
            }
            DataType::Binary => {
                let values = column.as_binary::<i32>();
                Box::new(move |row| values.value_length(row).unsigned_abs() as usize)
            }
            DataType::LargeBinary => {
                let values = column.as_binary::<i64>();
                Box::new(move |row| values.value_length(row).unsigned_abs() as usize)
            }
            DataType::Utf8View => {
                let values = column.as_string_view();
                Box::new(move |row| values.value(row).len())
            }
            DataType::BinaryView => {
                let values = column.as_binary_view();
                Box::new(move |row| values.value(row).len())
            }
            _ => continue,
        };
        for row in 0..column.len() {
            let length = lengths(row);
            if length < LARGE_VALUE_BYTES || column.is_null(row) {
                continue;
            }
            let rows = rows.get_or_insert_with(|| vec![0; batch.num_rows()]);
            rows[row] += length as u64;
        }
    }
    rows
}
