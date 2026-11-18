//! The warning for a BigQuery files table whose row numbers keep it on SQLLoad's reader.

use crate::adapters::bigquery::constants::ROW_NUMBER_REASON;
use crate::adapters::bigquery::main::plan::_direct_problem::direct_problem;
use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::files_source::models::FilesRead;
use crate::loader::models::TableSpec;

/// The warning plan shows when `_sqlload_source_row` is all that keeps BigQuery from reading the planned `read`'s Parquet files in GCS itself, with the change that lets it; `None` otherwise.
pub(in crate::adapters) fn row_number_warning(
    spec: &TableSpec,
    read: &FilesRead,
) -> Option<String> {
    (direct_problem(spec, read).as_deref() == Some(ROW_NUMBER_REASON)).then(|| {
        format!(
            "{SOURCE_ROW_COLUMN}: {ROW_NUMBER_REASON}. SQLLoad reads the files to number their rows, which is slower than BigQuery reading them itself.\nexclude_columns = [\"{SOURCE_ROW_COLUMN}\"] lets BigQuery load them itself, without row numbers."
        )
    })
}
