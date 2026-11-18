//! Whether BigQuery may load a files table's Parquet files in GCS itself.

use crate::adapters::bigquery::constants::ROW_NUMBER_REASON;
use crate::adapters::contract::main::_files_direct_problem::files_direct_problem;
use crate::column_types::models::ColumnType;
use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::files_source::models::{FilesRead, SourceFormat};
use crate::loader::models::{FilesSource, TableSpec};
use crate::store_paths::models::StoreKind;

/// Why BigQuery cannot load the planned `read` of a files table exactly by reading its Parquet files where they lie in GCS, so SQLLoad reads them and sends Arrow instead; `None` when it can, or when the files aren't in GCS. This is the one place that chooses between the two routes: every column must load as the type the files hold it as, in a type BigQuery reads from Parquet as SQLLoad does, under a name it reads as written; masks need SQLLoad's reader; and a table that keeps `_sqlload_source_row` needs SQLLoad to number the rows, which BigQuery can't.
pub(in crate::adapters) fn direct_problem(spec: &TableSpec, read: &FilesRead) -> Option<String> {
    let files = spec.files.as_ref()?;
    let StoreKind::Gcs { endpoint, .. } = &files.location.store else {
        return None;
    };
    let problems = [
        (files.encoding.format != SourceFormat::Parquet)
            .then(|| "only Parquet files are read by BigQuery itself".to_owned()),
        endpoint.as_ref().map(|endpoint| {
            format!("the files are behind the endpoint {endpoint}, which BigQuery doesn't reach")
        }),
        (!spec.masked.columns.is_empty()).then(|| "the table masks columns".to_owned()),
        files_direct_problem(files, read),
        unread_column(files, read),
        read.columns
            .iter()
            .any(|column| column.name == SOURCE_ROW_COLUMN)
            .then(|| ROW_NUMBER_REASON.to_owned()),
    ];
    problems.into_iter().flatten().next()
}

/// A column BigQuery would not read from Parquet as SQLLoad does: a type it reads otherwise (UUIDs, JSON, lists and native types), or a name in the files it doesn't read as written.
fn unread_column(files: &FilesSource, read: &FilesRead) -> Option<String> {
    let typed = read.columns.iter().find_map(|column| {
        let held = match &column.column_type {
            ColumnType::Uuid => "a UUID, which BigQuery reads from Parquet as bytes",
            ColumnType::Json => "JSON, which BigQuery reads from Parquet as text",
            ColumnType::Array(_) => "a list, which BigQuery reads as its own array",
            ColumnType::Native(_) => "a native type, which SQLLoad converts",
            _ => return None,
        };
        Some(format!("{} is {held}", column.name))
    });
    typed.or_else(|| {
        files
            .table
            .columns
            .iter()
            .find(|column| !plain_name(&column.name))
            .map(|column| {
                format!(
                    "the files name a column {:?}, which BigQuery doesn't read from Parquet as written",
                    column.name
                )
            })
    })
}

/// Whether `name` is a plain GoogleSQL name: a letter or `_`, then letters, digits and `_`.
fn plain_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && chars.all(|rest| rest.is_ascii_alphanumeric() || rest == '_')
}
