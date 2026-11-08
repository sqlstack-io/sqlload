//! The checks that decide whether a destination may read a files table's files itself and load them exactly.

use crate::adapters::contract::models::DirectBatch;
use crate::column_types::main::portable_type::portable_type;
use crate::column_types::models::ColumnType;
use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::main::batch_refusals::batch_refusals;
use crate::files_source::main::stored_read_of::stored_read_of;
use crate::files_source::models::{BatchFooters, DataFile, FileColumn, StoredRead};
use crate::loader::models::FilesSource;
use crate::schema_plan::models::DesiredColumn;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;

/// How `column` is held when a plain cast would not load it exactly, as a problem.
pub(crate) fn held_as(column: &FileColumn) -> Option<String> {
    let held = match (
        &column.column_type,
        &column.direct_refusal,
        column.stored_read,
    ) {
        (Err(reason), _, _) => Some(reason.clone()),
        (Ok(_), Some(arrow_type), _) => Some(arrow_type.clone()),
        (Ok(_), None, StoredRead::Plain) => None,
        (Ok(_), None, StoredRead::Nanos) => Some("nanoseconds".to_owned()),
        (Ok(_), None, StoredRead::Epoch(_)) => Some("an epoch integer".to_owned()),
    };
    Some(format!("column {} is held as {}", column.name, held?))
}

/// A read column the files hold as another type than it loads as; SQLLoad's own values (file name, version, row, partitions) are not the files'.
pub(crate) fn retyped(files: &FilesSource, column: &DesiredColumn) -> Option<String> {
    let own = [SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN]
        .contains(&column.name.as_str())
        || files
            .partition_types
            .iter()
            .any(|(partition, _)| *partition == column.name);
    let held = files
        .table
        .columns
        .iter()
        .find(|found| found.name == column.name)?;
    match &held.column_type {
        Ok(file_type) if !own && *file_type != column.column_type => Some(format!(
            "{} is {file_type} in the files but loads as {}; SQLLoad converts it",
            column.name, column.column_type
        )),
        _ => None,
    }
}

/// A file whose path the destination's reader would take as a glob pattern.
pub(crate) fn pattern(files: &FilesSource, data: &DataFile) -> Option<String> {
    let path = format!("{}{}", files.location, data.file.relative_key);
    path.contains(['*', '?', '[', '{'])
        .then(|| format!("the path {path} reads as a pattern"))
}

/// Why `batch`'s own files cannot be loaded exactly by the destination reading them itself: `planned`, the problem plan's checks give, else a column the batch's files hold in a way only SQLLoad reads (`batch_refusals`), else a read column its footers hold in different ways.
pub(crate) fn batch_problem(
    batch: &DirectBatch<'_>,
    source: &FilesSource,
    planned: Option<String>,
) -> Option<String> {
    let refused = batch_refusals(batch.footers, batch.files, &source.table.columns);
    let read = |name: &str| batch.read.columns.iter().any(|column| column.name == name);
    planned
        .or_else(|| {
            refused
                .iter()
                .find(|(column, _)| read(column))
                .map(|(column, held)| format!("column {column} is held as {held}"))
        })
        .or_else(|| {
            batch
                .read
                .columns
                .iter()
                .filter(|column| !constant(source, &column.name))
                .find_map(|column| disagreeing(batch.footers, &column.name))
        })
}

/// Why the batch's footers disagree on `name`, or hold it in a way only SQLLoad reads exactly: a different portable type, or any file storing it as nanoseconds (whatever Arrow type it names), which plan's sample may not have seen and the destination would cast differently. Epoch integers fail earlier, in `batch_refusals`. Arrow-only differences (Int32 and Int64, Utf8 and LargeUtf8, dictionary strings) a destination's reader promotes exactly.
fn disagreeing(footers: &BatchFooters, name: &str) -> Option<String> {
    let typed = |footer: &ArrowReaderMetadata| {
        let schema = footer.schema().clone();
        schema
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .map(|field| {
                (
                    portable_type(field).map_err(|_| field.data_type().to_string()),
                    stored_read_of(footer, name).unwrap_or_default(),
                )
            })
    };
    let held: Vec<(Result<ColumnType, String>, StoredRead)> =
        footers.parquet.values().filter_map(typed).collect();
    let first = held.first()?;
    held.iter()
        .find(|other| *other != first || other.1 != StoredRead::Plain)
        .map(|_| format!("column {name} is held in different ways by this batch's files, or in a way a cast would not keep exactly"))
}

/// The rows the batch's footers record.
pub(crate) fn footer_rows(footers: &BatchFooters) -> u64 {
    footers
        .parquet
        .values()
        .map(|footer| u64::try_from(footer.metadata().file_metadata().num_rows()).unwrap_or(0))
        .sum()
}

/// Whether `name` is a value SQLLoad gives each file (its name, version, row number or a partition), which the destination never reads from the files.
pub(crate) fn constant(source: &FilesSource, name: &str) -> bool {
    [SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN].contains(&name)
        || source
            .partition_types
            .iter()
            .any(|(partition, _)| partition == name)
}
