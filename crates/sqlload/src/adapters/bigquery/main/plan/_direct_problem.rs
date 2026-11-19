//! Whether BigQuery may load a files table's files itself.

use crate::adapters::bigquery::_helpers::dialect::pull::unread_name;
use crate::adapters::contract::main::_custom_endpoint::custom_endpoint;
use crate::adapters::contract::main::_files_direct_problem::files_direct_problem;
use crate::column_types::models::ColumnType;
use crate::file_store::_helpers::aws_keys::environment_variable;
use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::models::{FilesRead, SourceFormat};
use crate::loader::models::{FilesSource, TableSpec};
use crate::store_paths::models::StoreKind;

/// Why BigQuery cannot load the planned `read` of a files table in GCS exactly by querying its Parquet files where they lie, so SQLLoad reads them instead; `None` when it can, or when the files aren't in GCS. BigQuery reports which file a row came from but not its position in it, so a table writing `_sqlload_source_row` keeps SQLLoad's reader unless it excludes that column. Every column must load as the type the files hold it as; an emulator's endpoint is SQLLoad's to follow; masks need SQLLoad's reader; a column name BigQuery would change keeps it too; and BigQuery reads a Parquet UUID as bytes, a list as a record, an unsigned 64-bit integer (which loads as `decimal(20,0)`, so that type keeps SQLLoad's reader) as a signed one, and JSON or a native type its own way.
pub(in crate::adapters) fn direct_problem(spec: &TableSpec, read: &FilesRead) -> Option<String> {
    let files = spec.files.as_ref()?;
    let StoreKind::Gcs { .. } = &files.location.store else {
        return None;
    };
    let problems = [
        (files.encoding.format != SourceFormat::Parquet)
            .then(|| "only Parquet files are read by BigQuery itself".to_owned()),
        custom_endpoint(&files.location.store, &environment_variable),
        read.columns.iter().any(|column| column.name == SOURCE_ROW_COLUMN).then(|| {
            format!(
                "BigQuery doesn't report a row's position in its file, so it can't fill {SOURCE_ROW_COLUMN}; to have BigQuery load the files itself, add {SOURCE_ROW_COLUMN} to exclude_columns"
            )
        }),
        (!spec.masked.columns.is_empty()).then(|| "the table masks columns".to_owned()),
        files_direct_problem(files, read),
        unread_name(
            files
                .table
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .filter(|name| !own_column(files, name)),
        ),
        read.columns.iter().find_map(|column| {
            let held = match &column.column_type {
                ColumnType::Uuid => "a UUID, which BigQuery reads from Parquet as bytes",
                ColumnType::Array(_) => "a list, which BigQuery reads from Parquet as a record",
                ColumnType::Json => "JSON, which BigQuery reads from Parquet as text",
                ColumnType::Native(_) => "a native type, which SQLLoad converts itself",
                ColumnType::Decimal {
                    precision: 20,
                    scale: 0,
                } => "decimal(20,0), the type of an unsigned 64-bit integer, which BigQuery reads from Parquet as a signed one and fails on values above 9223372036854775807",
                _ => return None,
            };
            Some(format!("{} is {held}", column.name))
        }),
    ];
    problems.into_iter().flatten().next()
}

/// Whether `name` is a value SQLLoad gives each file (its name, version, row number or a partition), which no file holds.
fn own_column(files: &FilesSource, name: &str) -> bool {
    [SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN].contains(&name)
        || files
            .partition_types
            .iter()
            .any(|(partition, _)| partition == name)
}
