//! Whether DuckDB may load a files table's files itself.

use crate::files_source::models::{FilesRead, SourceFormat, StoredRead};
use crate::loader::models::TableSpec;
use crate::store_paths::models::StoreKind;

/// Why DuckDB cannot load the planned `read` of a files table exactly by reading its Parquet files itself, so SQLLoad reads them instead; `None` when it can. Masks, and columns the files hold in a way a plain cast would not keep exactly all need SQLLoad's reader.
pub(in crate::adapters) fn direct_problem(spec: &TableSpec, read: &FilesRead) -> Option<String> {
    let files = spec.files.as_ref()?;
    let held = files.table.columns.iter().find_map(|column| {
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
        held.map(|held| format!("column {} is held as {held}", column.name))
    });
    let problems = [
        (files.encoding.format != SourceFormat::Parquet)
            .then(|| "only Parquet files are read by DuckDB itself".to_owned()),
        matches!(
            &files.location.store,
            StoreKind::S3 {
                endpoint: Some(_),
                ..
            }
        )
        .then(|| "the S3 connection sets a custom endpoint".to_owned()),
        files
            .table
            .columns
            .iter()
            .find(|column| ["filename", "file_row_number"].contains(&column.name.as_str()))
            .map(|column| {
                format!(
                    "the files have a column named {}, which DuckDB's file reader adds itself",
                    column.name
                )
            }),
        (!spec.masked.columns.is_empty()).then(|| "the table masks columns".to_owned()),
        held,
        read.files
            .iter()
            .find(|data| data.file.relative_key.contains(['*', '?', '[', '{']))
            .map(|data| {
                format!(
                    "the file name {} reads as a pattern",
                    data.file.relative_key
                )
            }),
    ];
    problems.into_iter().flatten().next()
}
