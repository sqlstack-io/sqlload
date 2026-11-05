//! Combine the footers of one batch's Parquet files with the columns known so far.

use crate::files_source::_helpers::plan::columns::{
    ColumnMerge, SampledColumn, drop_loader_columns, file_columns, merge_columns, schema_columns,
};
use crate::files_source::models::{BatchFooters, DataFile, FileColumn};

/// `known` plus the columns `files`' `footers` add (the loader's own and the `replaced` ones, whose values come from the path or the load, left out), and a problem for each column a file holds as another type, naming the file.
pub fn batch_columns(
    footers: &BatchFooters,
    files: &[DataFile],
    known: &[FileColumn],
    replaced: &[&str],
) -> (Vec<FileColumn>, Vec<String>) {
    let mut columns: Vec<SampledColumn> = known
        .iter()
        .map(|column| SampledColumn {
            name: column.name.clone(),
            column_type: Some(column.column_type.clone()),
        })
        .collect();
    let mut problems: Vec<String> = Vec::new();
    for data in files {
        let Some(footer) = footers.0.get(&data.file.relative_key) else {
            continue;
        };
        let (found, _) = drop_loader_columns(schema_columns(footer.schema()));
        let merge = ColumnMerge {
            key: &data.file.relative_key,
            replaced,
            widen_numbers: false,
        };
        let (merged, conflicts) = merge_columns(columns, found, merge);
        columns = merged;
        problems.extend(conflicts);
    }
    (file_columns(columns), problems)
}
