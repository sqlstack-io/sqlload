use crate::loader::models::{CursorSpec, SettingOrigin};
use crate::project::models::{TableSettings, WriteStrategy};

/// A table's value if set, otherwise its source's.
pub(crate) fn layered<T: Clone>(
    table: &Option<T>,
    source: &Option<T>,
) -> Option<(T, SettingOrigin)> {
    match (table, source) {
        (Some(value), _) => Some((value.clone(), SettingOrigin::Table)),
        (None, Some(value)) => Some((value.clone(), SettingOrigin::Source)),
        (None, None) => None,
    }
}

/// The cursor a table's settings describe, if any.
pub(crate) fn cursor(table: &TableSettings, source: &TableSettings) -> Option<CursorSpec> {
    let (column, _) = layered(&table.cursor_column, &source.cursor_column)?;
    Some(CursorSpec {
        column,
        start: layered(&table.cursor_start, &source.cursor_start).map(|(value, _)| value),
        lookback: layered(&table.lookback, &source.lookback).map(|(value, _)| value),
        inclusive: layered(
            &table.append_cursor_inclusive,
            &source.append_cursor_inclusive,
        )
        .is_none_or(|(value, _)| value),
    })
}

/// The default strategy: full replace without a cursor, merge with a cursor and key, delete_insert with a cursor alone.
pub(crate) fn default_strategy(cursor: Option<&CursorSpec>, has_key: bool) -> WriteStrategy {
    match (cursor, has_key) {
        (None, _) => WriteStrategy::Replace,
        (Some(_), true) => WriteStrategy::Merge,
        (Some(_), false) => WriteStrategy::DeleteInsert,
    }
}

/// Problems with a strategy and the keys and cursor it is given.
pub(crate) fn strategy_problems(
    strategy: WriteStrategy,
    unique_key: &[String],
    cursor: Option<&CursorSpec>,
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    match (strategy, cursor) {
        (WriteStrategy::Merge, _) if unique_key.is_empty() => problems.push(
            "write_strategy = \"merge\" needs a unique_key (the table has no primary key)".to_owned(),
        ),
        (WriteStrategy::Append, Some(cursor)) if cursor.lookback.is_some() => problems.push(
            "lookback re-inserts the window on every run with append; use delete_insert (no key needed) or merge"
                .to_owned(),
        ),
        (WriteStrategy::Replace, Some(_)) => problems.push(
            "write_strategy = \"replace\" reloads the whole table, so it takes no cursor_column".to_owned(),
        ),
        (WriteStrategy::DeleteInsert, None) => problems.push(
            "write_strategy = \"delete_insert\" replaces a cursor range, so it needs a cursor_column".to_owned(),
        ),
        _ => {}
    }
    problems
}
