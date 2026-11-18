//! Whether Databricks may load a files table's files itself.

use crate::adapters::contract::main::_files_direct_problem::files_direct_problem;
use crate::column_types::models::ColumnType;
use crate::files_source::models::{FilesRead, SourceFormat};
use crate::loader::models::TableSpec;
use crate::store_paths::models::StoreKind;

/// Why Databricks cannot load the planned `read` of a files table in S3 exactly by `COPY`ing its Parquet files where they lie, so SQLLoad reads them into the staging volume instead; `None` when it can, or when the files aren't in S3. Every column must load as the type the files hold it as, so Databricks' casts never differ from SQLLoad's; an external location reaches only AWS S3, not an S3-compatible endpoint; masks need SQLLoad's reader; and Databricks reads a Parquet time as text in its own format, a UUID as bytes, and lists as its own arrays.
pub(in crate::adapters) fn direct_problem(spec: &TableSpec, read: &FilesRead) -> Option<String> {
    let files = spec.files.as_ref()?;
    let StoreKind::S3 { endpoint, .. } = &files.location.store else {
        return None;
    };
    let problems = [
        (files.encoding.format != SourceFormat::Parquet)
            .then(|| "only Parquet files are read by Databricks itself".to_owned()),
        endpoint.as_ref().map(|endpoint| {
            format!("the files are on {endpoint}, which no Unity Catalog external location reaches")
        }),
        (!spec.masked.columns.is_empty()).then(|| "the table masks columns".to_owned()),
        files_direct_problem(files, read),
        read.columns.iter().find_map(|column| {
            let held = match &column.column_type {
                ColumnType::Time => {
                    "a time, which Databricks reads from Parquet as text in its own format"
                }
                ColumnType::Uuid => "a UUID, which Databricks reads from Parquet as bytes",
                ColumnType::Array(_) => "a list, which Databricks reads as its own array",
                ColumnType::Native(_) => "a type only SQLLoad's reader converts",
                _ => return None,
            };
            Some(format!("{} is {held}", column.name))
        }),
    ];
    problems.into_iter().flatten().next()
}
