use std::collections::BTreeMap;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, UInt32Array};
use arrow_cast::cast;
use arrow_schema::DataType;
use arrow_select::take::take_record_batch;

use crate::adapters::files::_helpers::layout::shape::shape_batch;
use crate::adapters::files::models::{FilesWriter, PartitionBy};

/// `batch` shaped to the writer's columns and split by partition (one part, keyed `""`, without partitions).
pub(crate) fn partition_rows(
    writer: &FilesWriter,
    batch: &RecordBatch,
) -> Result<Vec<(String, RecordBatch)>, String> {
    let shaped = shape_batch(
        batch,
        &writer.schema,
        &writer.load_id,
        writer.loaded_at_micros,
    )?;
    match (&writer.cursor_column, &writer.partition) {
        (Some(column), Some(partition)) => split_batch(&shaped, column, partition.by),
        _ => Ok(vec![(String::new(), shaped)]),
    }
}

/// `batch` split by the partition value of column `column` (its text, or its date), in value order.
pub(crate) fn split_batch(
    batch: &RecordBatch,
    column: &str,
    by: PartitionBy,
) -> Result<Vec<(String, RecordBatch)>, String> {
    let values = batch
        .column_by_name(column)
        .ok_or_else(|| format!("the rows have no cursor column {column}"))?;
    let failed = |error: arrow_schema::ArrowError| format!("{column}: {error}");
    let dated = match by {
        PartitionBy::Date => cast(values, &DataType::Date32).map_err(failed)?,
        PartitionBy::Value => values.clone(),
    };
    let text = cast(&dated, &DataType::Utf8).map_err(failed)?;
    let text = text.as_string::<i32>();
    if text.null_count() > 0 {
        return Err(format!(
            "{column} is NULL in some rows, but the path puts every row in a partition folder by {column}"
        ));
    }
    let mut rows: BTreeMap<&str, Vec<u32>> = BTreeMap::new();
    for (index, value) in text.iter().enumerate() {
        let row = u32::try_from(index).map_err(|error| error.to_string())?;
        rows.entry(value.unwrap_or_default()).or_default().push(row);
    }
    if rows.contains_key("") {
        return Err(format!(
            "{column} is empty in some rows, which cannot name a partition folder"
        ));
    }
    let mut parts: Vec<(String, RecordBatch)> = Vec::with_capacity(rows.len());
    for (value, indices) in rows {
        let part = take_record_batch(batch, &UInt32Array::from(indices))
            .map_err(|error| error.to_string())?;
        parts.push((value.to_owned(), part));
    }
    Ok(parts)
}
