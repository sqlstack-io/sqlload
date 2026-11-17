use std::collections::{BTreeMap, BTreeSet};

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, UInt32Array};
use arrow_cast::cast;
use arrow_schema::DataType;
use arrow_select::take::take_record_batch;

use crate::adapters::files::_helpers::layout::shape::shape_batch;
use crate::adapters::files::models::{FilesWriter, PartitionBy};
use crate::files_source::constants::{SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN};

/// `batch` shaped to the writer's columns and split by partition (one part, keyed `""`, without partitions).
pub(crate) fn partition_rows(
    writer: &FilesWriter,
    batch: &RecordBatch,
) -> Result<Vec<(String, RecordBatch)>, String> {
    let shaped = shape_batch(
        batch,
        (&writer.schema, &writer.metadata),
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

/// The distinct source files (name and version) `part`'s rows came from, from its source file columns; empty when it has none.
pub(crate) fn row_sources(part: &RecordBatch) -> Result<BTreeSet<(String, String)>, String> {
    let column = |name: &str| {
        let schema = part.schema();
        let position = schema
            .fields()
            .iter()
            .position(|field| field.name().eq_ignore_ascii_case(name))?;
        Some(cast(part.column(position), &DataType::Utf8).map_err(|error| error.to_string()))
    };
    let (Some(files), Some(versions)) = (column(SOURCE_FILE_COLUMN), column(SOURCE_VERSION_COLUMN))
    else {
        return Ok(BTreeSet::new());
    };
    let (files, versions) = (files?, versions?);
    let (files, versions) = (files.as_string::<i32>(), versions.as_string::<i32>());
    let mut sources: BTreeSet<(String, String)> = BTreeSet::new();
    let mut last: Option<(&str, &str)> = None;
    for (file, version) in files.iter().zip(versions.iter()) {
        let (Some(file), Some(version)) = (file, version) else {
            continue;
        };
        if last != Some((file, version)) {
            sources.insert((file.to_owned(), version.to_owned()));
            last = Some((file, version));
        }
    }
    Ok(sources)
}
