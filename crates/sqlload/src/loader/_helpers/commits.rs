use crate::batch_plan::main::file_groups::file_groups;
use crate::batch_plan::main::split_windows::split_windows;
use crate::batch_plan::models::WindowSplit;
use crate::column_types::models::ColumnType;
use crate::files_source::models::FilesRead;
use crate::loader::models::{CommitRule, ReadWindow, TableSpec};
use crate::postgres_source::models::ReadColumn;
use crate::write_sql::models::WriteMode;

/// What decides how a table's rows are cut into commits.
pub(crate) struct CommitRequest<'a> {
    pub(crate) spec: &'a TableSpec,
    pub(crate) window: Option<&'a ReadWindow>,
    pub(crate) cursor_type: Option<&'a ColumnType>,
    pub(crate) read_columns: &'a [ReadColumn],
    pub(crate) files: Option<&'a FilesRead>,
    pub(crate) mode: &'a WriteMode,
}

/// The commits of a table's load: a files source's batches of whole partitions; a cursor table's `batch` windows; an ordered Postgres read (an index leads with the cursor) committing along the way; the whole read for any other cursor table, since committing part of an unordered read could skip rows the next run never reads; about every 1 GB without a cursor.
pub(crate) fn commit_rule(request: &CommitRequest<'_>) -> Result<CommitRule, String> {
    let spec = request.spec;
    if let Some(files) = request.files {
        let by_value = match request.mode {
            WriteMode::DeleteInsert { cursor_column, .. } => Some(cursor_column.as_str()),
            _ => None,
        };
        return Ok(CommitRule::FileGroups(file_groups(&files.files, by_value)));
    }
    let Some(cursor) = &spec.cursor else {
        return Ok(CommitRule::Size);
    };
    if let (Some(step), Some(window)) = (spec.batch, request.window) {
        let windows = split_windows(&WindowSplit {
            from: window.from.as_deref(),
            to: window.to.as_deref(),
            inclusive: window.inclusive,
            step,
            cursor_type: request
                .cursor_type
                .filter(|known| spec.python.is_none() || **known != ColumnType::Text),
            now: chrono::Utc::now(),
        })?;
        return Ok(CommitRule::Windows(windows));
    }
    let plain_column = request
        .read_columns
        .iter()
        .find(|column| column.name == cursor.column)
        .is_some_and(|column| column.expression == quoted(&column.name));
    let indexed = spec.python.is_none()
        && plain_column
        && spec
            .source_table
            .indexed_columns
            .iter()
            .any(|name| *name == cursor.column);
    match (indexed, request.mode) {
        (true, WriteMode::Append | WriteMode::Merge { .. }) => Ok(CommitRule::Ordered {
            column: cursor.column.clone(),
        }),
        _ => Ok(CommitRule::Whole),
    }
}

/// `name` as Postgres quotes an identifier.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
