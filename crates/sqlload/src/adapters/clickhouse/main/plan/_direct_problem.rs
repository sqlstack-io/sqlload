//! Whether ClickHouse may load a files table's files itself.

use crate::adapters::contract::main::_files_direct_problem::files_direct_problem;
use crate::column_types::models::ColumnType;
use crate::files_source::models::{FilesRead, SourceFormat};
use crate::loader::models::TableSpec;

/// Why ClickHouse cannot load the planned `read` of a files table exactly by reading its Parquet files itself, so SQLLoad reads them and sends Arrow instead; `None` when it can. Every column must load as the type the files hold it as, so ClickHouse's casts never differ from SQLLoad's; masks need SQLLoad's reader, and ClickHouse reads a Parquet time as a timestamp.
pub(in crate::adapters) fn direct_problem(spec: &TableSpec, read: &FilesRead) -> Option<String> {
    let files = spec.files.as_ref()?;
    let problems = [
        (files.encoding.format != SourceFormat::Parquet)
            .then(|| "only Parquet files are read by ClickHouse itself".to_owned()),
        (!spec.masked.columns.is_empty()).then(|| "the table masks columns".to_owned()),
        files_direct_problem(files, read),
        read.columns
            .iter()
            .find(|column| column.column_type == ColumnType::Time)
            .map(|column| {
                format!(
                    "{} is a time, which ClickHouse reads from Parquet as a timestamp",
                    column.name
                )
            }),
    ];
    problems.into_iter().flatten().next()
}
