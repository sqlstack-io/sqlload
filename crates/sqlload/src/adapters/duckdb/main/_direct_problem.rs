//! Whether DuckDB may load a files table's files itself.

use crate::adapters::contract::main::_files_direct_problem::files_direct_problem;
use crate::adapters::duckdb::_helpers::direct_checks::{custom_endpoint, reserved_name};
use crate::files_source::models::{FilesRead, SourceFormat};
use crate::loader::models::TableSpec;

/// Why DuckDB cannot load the planned `read` of a files table exactly by reading its Parquet files itself, so SQLLoad reads them instead; `None` when it can. DuckDB's casts differ from SQLLoad's (it rounds where SQLLoad truncates, and promotes files that disagree), so every column must load as the type the files hold it as; masks, custom S3 endpoints and names DuckDB would read as patterns also need SQLLoad's reader.
pub(in crate::adapters) fn direct_problem(spec: &TableSpec, read: &FilesRead) -> Option<String> {
    let files = spec.files.as_ref()?;
    let problems = [
        (files.encoding.format != SourceFormat::Parquet)
            .then(|| "only Parquet files are read by DuckDB itself".to_owned()),
        custom_endpoint(&files.location.store),
        files.table.columns.iter().find_map(reserved_name),
        (!spec.masked.columns.is_empty()).then(|| "the table masks columns".to_owned()),
        files_direct_problem(files, read),
    ];
    problems.into_iter().flatten().next()
}
