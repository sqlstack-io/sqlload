//! A batch's values as text, for routes that send rows rather than files.

use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};

/// Each row of `batch` as one text per column in the batch's order, `None` for NULL; binary as hex and instants in RFC 3339, which Snowflake casts back to the same values.
pub fn text_rows(batch: &RecordBatch) -> Result<Vec<Vec<Option<String>>>, String> {
    let options = FormatOptions::default();
    let formatters: Vec<ArrayFormatter<'_>> = batch
        .columns()
        .iter()
        .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
        .collect::<Result<_, _>>()
        .map_err(|error| error.to_string())?;
    let mut rows: Vec<Vec<Option<String>>> = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut values: Vec<Option<String>> = Vec::with_capacity(batch.num_columns());
        for (column, formatter) in batch.columns().iter().zip(&formatters) {
            values.push(match column.is_null(row) {
                true => None,
                false => Some(formatter.value(row).to_string()),
            });
        }
        rows.push(values);
    }
    Ok(rows)
}
