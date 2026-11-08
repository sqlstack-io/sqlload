//! Whether one batch's own files load exactly when the destination reads them itself.

use crate::adapters::contract::models::DirectBatch;
use crate::column_types::main::portable_type::portable_type;
use crate::column_types::models::ColumnType;
use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::main::batch_refusals::batch_refusals;
use crate::files_source::main::stored_read_of::stored_read_of;
use crate::files_source::models::{BatchFooters, StoredRead};
use crate::loader::models::FilesSource;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;

/// Why `batch`'s own files cannot be loaded exactly by the destination reading them itself: `planned`, the problem plan's checks give, else a column the batch's files hold in a way only SQLLoad reads (`batch_refusals`), else a read column its footers hold in different ways.
pub(in crate::adapters) fn batch_direct_problem(
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

/// Whether `name` is a value SQLLoad gives each file (its name, version, row number or a partition), which the destination never reads from the files.
fn constant(source: &FilesSource, name: &str) -> bool {
    [SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN].contains(&name)
        || source
            .partition_types
            .iter()
            .any(|(partition, _)| partition == name)
}
