//! The rules a cursor read follows, as `plan` words them.

use chrono_tz::Tz;

use crate::column_types::models::ColumnType;
use crate::loader::models::TablePlan;
use crate::project::models::{NullCursor, OnCursorOutOfRange};

/// What a cursor read does with source rows whose cursor is NULL, for `plan`: `NULL cursors: included` or `skipped` as `null_cursor` says, and by default `fail the load` when the source reports a cursor column nullable; `None` when it reports one NOT NULL or reports nothing (a Python table), and for a files source, whose cursor comes from each file's path.
pub(crate) fn null_rule(table: &TablePlan) -> Option<&'static str> {
    let spec = &table.spec;
    let cursor = spec.cursor.as_ref().filter(|_| spec.files.is_none())?;
    let nullable = cursor.columns().all(|name| {
        spec.source_table
            .column(name)
            .is_some_and(|column| column.nullable)
    });
    match cursor.null_cursor {
        NullCursor::Include => Some("NULL cursors: included"),
        NullCursor::Ignore => Some("NULL cursors: skipped"),
        NullCursor::Fail if nullable => Some("NULL cursors: fail the load"),
        NullCursor::Fail => None,
    }
}

/// What a Python table's read does with rows its function returns outside a window; `None` for other tables.
pub(crate) fn range_rule(table: &TablePlan) -> Option<&'static str> {
    let spec = &table.spec;
    let cursor = spec.cursor.as_ref()?;
    if spec.python.is_none() || table.logged.is_some() {
        return None;
    }
    Some(match cursor.out_of_range {
        OnCursorOutOfRange::Fail => "rows outside a window fail the table",
        OnCursorOutOfRange::Skip => "rows outside a window are skipped",
        OnCursorOutOfRange::Warn => "rows outside a window load, with a warning",
    })
}

/// The zone a cursor of times (or one a Python table has not shown yet) reads times without an offset in, with where it comes from: `timezone` where it is set, else UTC for a Python table, whose `ctx` gets them; `None` otherwise.
pub(crate) fn zone_rule(table: &TablePlan) -> Option<(Tz, String)> {
    let spec = &table.spec;
    let cursor = spec.cursor.as_ref()?;
    let timed = table
        .read_columns
        .iter()
        .find(|read| read.name.eq_ignore_ascii_case(&cursor.column))
        .is_none_or(|read| {
            matches!(
                read.output,
                ColumnType::TimestampTz | ColumnType::TimestampNtz | ColumnType::Text
            )
        });
    let python = spec.python.is_some() && table.logged.is_none();
    match (timed, spec.timezone) {
        (false, _) => None,
        (true, Some((zone, at))) => Some((zone, format!("the {at}'s timezone"))),
        (true, None) => python.then(|| (Tz::UTC, "the default, as timezone is not set".to_owned())),
    }
}
