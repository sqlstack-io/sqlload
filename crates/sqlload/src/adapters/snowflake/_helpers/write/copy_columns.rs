//! The columns a direct `COPY` of a files table fills.

use crate::adapters::snowflake::models::{CopyColumn, CopyValue};
use crate::column_types::models::ColumnType;
use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::models::FilesRead;
use crate::loader::models::TableSpec;

/// The columns a direct `COPY` of a files table fills, in the order the files source reads them, each from the files, its path, or the file's name, version and row number.
pub(crate) fn copy_columns(spec: &TableSpec, read: &FilesRead) -> Vec<CopyColumn> {
    read.columns
        .iter()
        .map(|column| CopyColumn {
            name: column.name.clone(),
            column_type: column.column_type.clone(),
            value: match column.name.as_str() {
                SOURCE_FILE_COLUMN => CopyValue::SourceFile,
                SOURCE_VERSION_COLUMN => CopyValue::SourceVersion,
                SOURCE_ROW_COLUMN => CopyValue::SourceRow,
                name if partition_column(spec, name) => CopyValue::Partition,
                name => CopyValue::File {
                    file_type: file_type(spec, name),
                },
            },
        })
        .collect()
}

/// Whether `name` is one of the files source's partition columns.
pub(crate) fn partition_column(spec: &TableSpec, name: &str) -> bool {
    spec.files
        .iter()
        .flat_map(|files| files.partition_types.iter())
        .any(|(partition, _)| partition == name)
}

/// The type the known files hold column `name` as; a type with no portable equivalent is named as one the direct `COPY` never loads.
pub(crate) fn file_type(spec: &TableSpec, name: &str) -> Option<ColumnType> {
    let files = spec.files.as_ref()?;
    let found = files
        .table
        .columns
        .iter()
        .find(|found| found.name == name)?;
    Some(match &found.column_type {
        Ok(column_type) => column_type.clone(),
        Err(reason) => ColumnType::Native(format!("unsupported ({reason})")),
    })
}
