use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef};
use arrow_cast::cast;
use arrow_schema::{DataType, TimeUnit};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;

use crate::column_types::models::ColumnType;
use crate::files_destination::constants::SECONDS_PER_DAY;
use crate::files_source::models::Footer;

/// The row group maximums of `column` in one footer; an error when the file has rows but no statistics.
pub(crate) fn row_group_maxes(footer: &Footer, column: &str) -> Result<ArrayRef, String> {
    let failed = |error: parquet::errors::ParquetError| {
        format!(
            "{}: cannot read the statistics of {column}: {error}",
            footer.relative_key
        )
    };
    let converter = StatisticsConverter::try_new(
        column,
        &footer.schema,
        footer.metadata.file_metadata().schema_descr(),
    )
    .map_err(failed)?;
    let row_groups = footer.metadata.row_groups();
    let maxes = converter
        .row_group_maxes(row_groups.iter())
        .map_err(failed)?;
    let rows: i64 = row_groups.iter().map(|group| group.num_rows()).sum();
    match rows > 0 && maxes.null_count() == maxes.len() {
        true => Err(format!(
            "{} has no statistics for {column}; give --start-cursor or rewrite with --reload",
            footer.relative_key
        )),
        false => Ok(maxes),
    }
}

/// The largest of `maxes` as cursor bound text takes it: epoch microseconds for timestamps, `YYYY-MM-DD` for dates.
pub(crate) fn largest(
    maxes: &[ArrayRef],
    cursor_type: &ColumnType,
) -> Result<Option<String>, String> {
    let (target, compared) = match cursor_type {
        ColumnType::TimestampTz | ColumnType::TimestampNtz => (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Int64,
        ),
        ColumnType::Date => (DataType::Date32, DataType::Int64),
        ColumnType::SmallInt | ColumnType::Int => (DataType::Int64, DataType::Int64),
        _ => (DataType::Utf8, DataType::Utf8),
    };
    let mut texts: Vec<String> = Vec::new();
    let mut numbers: Vec<i64> = Vec::new();
    for array in maxes {
        let converted = cast(array, &target)
            .and_then(|typed| cast(&typed, &compared))
            .map_err(|error| error.to_string())?;
        match compared {
            DataType::Utf8 => texts.extend(
                converted
                    .as_string::<i32>()
                    .iter()
                    .flatten()
                    .map(str::to_owned),
            ),
            _ => numbers.extend(converted.as_primitive::<Int64Type>().iter().flatten()),
        }
    }
    Ok(match (numbers.into_iter().max(), cursor_type) {
        (Some(days), ColumnType::Date) => {
            chrono::DateTime::from_timestamp(days * SECONDS_PER_DAY, 0)
                .map(|time| time.date_naive().format("%Y-%m-%d").to_string())
        }
        (Some(number), _) => Some(number.to_string()),
        (None, _) => texts.into_iter().max(),
    })
}
