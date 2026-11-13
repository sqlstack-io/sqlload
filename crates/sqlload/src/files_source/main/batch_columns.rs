//! Combine the footers of one batch's Parquet files with the columns known so far.

use crate::files_source::_helpers::plan::columns::{
    ColumnMerge, SampledColumn, drop_loader_columns, file_columns, merge_columns, parquet_columns,
};
use crate::files_source::models::{BatchFooters, DataFile, FileColumn};

/// `known` plus the columns `files`' `footers` add (the loader's own and the `replaced` ones, whose values come from the path or the load, left out), and a problem for each column a file holds as another type, naming the file; a batch's refusals of the direct route are its own, from [`batch_refusals`](crate::files_source::main::batch_refusals::batch_refusals), so one batch never keeps later ones from it.
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
            direct_refusal: column.direct_refusal.clone(),
            stored_read: column.stored_read,
            lists: column.lists,
        })
        .collect();
    let mut problems: Vec<String> = Vec::new();
    for data in files {
        let Some(footer) = footers.parquet.get(&data.file.relative_key) else {
            continue;
        };
        let (mut found, _) = drop_loader_columns(parquet_columns(footer));
        for column in &mut found {
            column.direct_refusal = None;
            if let Some(seen) = known.iter().find(|seen| seen.name == column.name) {
                column.stored_read = seen.stored_read;
            }
        }
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
