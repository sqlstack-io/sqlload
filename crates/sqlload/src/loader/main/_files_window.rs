//! Choose a files table's cursor window: whole partitions for delete_insert, rows after the newest files' maximum for append.

use crate::adapters::contract::main::cursor_bound::cursor_bound;
use crate::adapters::contract::main::cursor_text::cursor_text;
use crate::adapters::contract::models::{CursorValue, WriteMode};
use crate::adapters::files::main::check_window::check_window;
use crate::adapters::files::main::cursor_maximum::cursor_maximum;
use crate::adapters::files::main::partition_window::partition_window;
use crate::adapters::files::models::{PartitionBy, PartitionWindowRequest};
use crate::column_types::models::ColumnType;
use crate::loader::_helpers::table_plan::{Window, source_filter};
use crate::loader::constants::{EXCLUSIVE_BOUND, INCLUSIVE_BOUND};
use crate::loader::models::FilesWindowRequest;
use crate::project::models::WriteStrategy;

/// The source filter, write mode and `plan` note: partitions from the newest one (`delete_insert`) or rows after the newest files' maximum (`append`).
pub(in crate::loader) fn files_window(request: &FilesWindowRequest<'_>) -> Result<Window, String> {
    let (spec, options) = (request.spec, request.options);
    let backfill = options.start_cursor.is_some() || options.end_cursor.is_some();
    let (Some(cursor), Some(cursor_type), Some(partition)) =
        (&spec.cursor, request.cursor_type, request.partition)
    else {
        let mode = match (options.reload, spec.strategy) {
            (true, _) => WriteMode::Recreate,
            (false, WriteStrategy::Append) => WriteMode::Append,
            (false, _) => WriteMode::Replace,
        };
        return match backfill {
            true => Err("--start-cursor/--end-cursor need a cursor_column".to_owned()),
            false => Ok(Window {
                filter: None,
                bounds: (None, None),
                mode,
                note: None,
                inclusive: true,
                maximum: None,
            }),
        };
    };
    if backfill && options.reload {
        return Err(
            "--reload rewrites every file, so it takes no --start-cursor/--end-cursor".to_owned(),
        );
    }
    let flag = |value: &Option<String>| {
        value
            .as_deref()
            .map(|text| cursor_text(text, cursor_type))
            .transpose()
    };
    let (start, end) = (flag(&options.start_cursor)?, flag(&options.end_cursor)?);
    let newest = request
        .listed
        .partitions
        .last()
        .and_then(|partition| partition.value.as_deref())
        .filter(|_| !options.reload);
    let column = cursor.column.as_str();
    let expression = request
        .read_columns
        .iter()
        .find(|read| read.name == column)
        .map_or(column, |read| read.expression.as_str());
    let expression = match cursor_type {
        ColumnType::Text | ColumnType::Varchar(_) => format!("{expression} COLLATE \"C\""),
        _ => expression.to_owned(),
    };
    let mut data_maximum: Option<String> = None;
    let (from, to, note, operator) = match spec.strategy {
        WriteStrategy::DeleteInsert => {
            let window = partition_window(&PartitionWindowRequest {
                by: partition.by,
                cursor_column: column,
                cursor_type,
                newest,
                lookback: cursor.lookback,
                start: start.as_deref(),
                end: end.as_deref(),
                cursor_start: cursor.start.as_deref(),
                reload: options.reload,
            })?;
            (window.from, window.to, window.note, INCLUSIVE_BOUND)
        }
        _ => {
            let maximum = match (&start, newest) {
                (None, Some(newest)) => {
                    newest_maximum(request, partition.by, newest)?.map(|(maximum, from)| {
                        let note = format!("{column} from {maximum} ({from})");
                        (maximum, note)
                    })
                }
                _ => None,
            };
            let (from, note) = match (start, maximum, &cursor.start) {
                (Some(start), _, _) => (
                    Some(start.clone()),
                    format!("{column} from {start} (--start-cursor)"),
                ),
                (None, Some((maximum, note)), _) => {
                    data_maximum = Some(maximum.clone());
                    (Some(maximum), note)
                }
                (None, None, Some(start)) => (
                    Some(start.clone()),
                    format!("{column} first run, from cursor_start {start}"),
                ),
                (None, None, None) => (None, format!("{column} first run, everything")),
            };
            let operator = match (cursor.inclusive, &options.start_cursor) {
                (false, None) => EXCLUSIVE_BOUND,
                _ => INCLUSIVE_BOUND,
            };
            (from, end, note, operator)
        }
    };
    let mode = match (options.reload, spec.strategy) {
        (true, _) => WriteMode::Recreate,
        (false, WriteStrategy::DeleteInsert) => WriteMode::DeleteInsert {
            cursor_column: column.to_owned(),
            from: from.clone(),
            to: to.clone(),
            values: vec![],
        },
        (false, _) => WriteMode::Append,
    };
    check_window(partition.by, cursor_type, (from.as_deref(), to.as_deref()))?;
    Ok(Window {
        filter: source_filter(
            &expression,
            from.as_deref().map(|from| (operator, from)),
            to.as_deref(),
        ),
        bounds: (from, to),
        mode,
        note: Some(note),
        inclusive: operator == INCLUSIVE_BOUND,
        maximum: data_maximum,
    })
}

/// An append cursor's maximum so far, and where it came from: the newest folder's value for `{cursor}` partitions (exact), the newest load's statistics for `{cursor:date}`.
fn newest_maximum(
    request: &FilesWindowRequest<'_>,
    by: PartitionBy,
    newest: &str,
) -> Result<Option<(String, &'static str)>, String> {
    let (Some(cursor), Some(cursor_type)) = (&request.spec.cursor, request.cursor_type) else {
        return Ok(None);
    };
    let value = match cursor_type {
        ColumnType::TimestampTz => CursorValue::EpochMicros { zoned: true },
        ColumnType::TimestampNtz => CursorValue::EpochMicros { zoned: false },
        ColumnType::Date => CursorValue::Date,
        _ => CursorValue::Plain,
    };
    match by {
        PartitionBy::Value => Ok(Some((newest.to_owned(), "the newest partition"))),
        PartitionBy::Date => {
            let raw = cursor_maximum(request.footers, &cursor.column, cursor_type)?;
            let maximum = raw.map(|raw| cursor_bound(&raw, value, None)).transpose()?;
            Ok(maximum.map(|maximum| (maximum, "maximum in the newest load's files")))
        }
    }
}
