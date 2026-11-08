//! What the ClickHouse tests build their cases from.

use arrow_array::{ArrayRef, RecordBatch};

use crate::adapters::clickhouse::_helpers::write::arrow_batch::clickhouse_batch;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// A planned column `name` of `column_type`, mapped from the source.
pub(crate) fn desired(name: &str, column_type: ColumnType) -> DesiredColumn {
    DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Source,
    }
}

/// A batch of `columns`.
pub(crate) fn batch_of(columns: Vec<(&str, ArrayRef)>) -> Result<RecordBatch, String> {
    RecordBatch::try_from_iter(columns).map_err(|error| error.to_string())
}

/// Each column of `batch` as `name: type = value|value`, NULLs as `null`, columns joined by `; `.
fn shown(batch: &RecordBatch) -> Result<String, String> {
    let options = arrow_cast::display::FormatOptions::default().with_null("null");
    let mut shown: Vec<String> = Vec::new();
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let formatter = arrow_cast::display::ArrayFormatter::try_new(array, &options)
            .map_err(|error| error.to_string())?;
        let values: Vec<String> = (0..array.len())
            .map(|row| formatter.value(row).to_string())
            .collect();
        shown.push(format!(
            "{}: {} = {}",
            field.name(),
            field.data_type(),
            values.join("|")
        ));
    }
    Ok(shown.join("; "))
}

/// `batch` prepared for ClickHouse with `columns` (into a build table when `builds`), shown as [`shown`], or its error up to the first `: `.
pub(crate) fn converted(
    batch: &RecordBatch,
    (columns, builds): (&[DesiredColumn], bool),
) -> Result<Result<String, String>, String> {
    clickhouse_batch(batch, (columns, builds))
        .map_err(|error| error.split(": ").next().unwrap_or_default().to_owned())
        .map(|batch| shown(&batch))
        .map_or_else(|error| Ok(Err(error)), |shown| shown.map(Ok))
}
