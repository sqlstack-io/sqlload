use crate::files_source::models::AfterLoad;
use crate::loader::constants::PRIMARY_KEY_PLACEHOLDER;
use crate::loader::models::{CursorSpec, SettingOrigin};
use crate::project::models::{
    NullCursor, OnDuplicateKeys, SourceKind, TableOverride, TableSettings, WriteStrategy,
};
use crate::table_options::main::layered::layered;
use crate::table_options::models::OptionContext;

/// Problems with a strategy and the keys and cursor it is given.
pub(crate) fn strategy_problems(
    strategy: WriteStrategy,
    (unique_key, replace_key): (&[String], &[String]),
    cursor: Option<&CursorSpec>,
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    match (strategy, cursor) {
        (WriteStrategy::Merge, _) if unique_key.is_empty() => problems.push(format!(
            "write_strategy = \"merge\" needs a unique_key: the key columns, such as unique_key = [\"id\"], or unique_key = {PRIMARY_KEY_PLACEHOLDER:?} for the primary key the source reports"
        )),
        (WriteStrategy::Append, Some(cursor))
            if cursor.lookback.is_some() || cursor.lookback_units.is_some() =>
        {
            problems.push(
                "lookback re-inserts the window on every run with append; use delete_insert (no key needed) or merge"
                    .to_owned(),
            )
        }
        (WriteStrategy::Replace, Some(_)) => problems.push(
            "write_strategy = \"replace\" reloads the whole table, so it takes no cursor_column".to_owned(),
        ),
        (WriteStrategy::DeleteInsert, None) if replace_key.is_empty() => problems.push(
            "write_strategy = \"delete_insert\" replaces a cursor range or the rows of the key values it reads, so it needs a cursor_column or a replace_key. Fix: set cursor_column, or replace_key = [\"version\"] to replace every row of each version a load reads".to_owned(),
        ),
        _ => {}
    }
    if !replace_key.is_empty() && strategy != WriteStrategy::DeleteInsert {
        let merge = match strategy {
            WriteStrategy::Merge => " (merge matches rows one by one on unique_key)",
            _ => "",
        };
        problems.push(format!(
            "replace_key is for write_strategy = \"delete_insert\", which replaces the rows holding the key values a load reads; {strategy} does not delete by it. Fix: set write_strategy = \"delete_insert\" to replace those rows whole, or drop replace_key{merge}"
        ));
    }
    if let Some(cursor) = cursor.filter(|cursor| cursor.from_now && cursor.lookback_units.is_some())
    {
        problems.push(format!(
            "lookback_from = \"now\" counts lookback back from the time of the run, so {} needs a duration lookback, such as lookback = \"1d\", not a number of cursor units",
            cursor.label()
        ));
    }
    if let Some(cursor) = cursor.filter(|cursor| {
        cursor.from_now && cursor.lookback.is_none() && cursor.lookback_units.is_none()
    }) {
        problems.push(format!(
            "lookback_from = \"now\" counts lookback back from now, so {} needs a lookback",
            cursor.label()
        ));
    }
    problems
}

/// Why a `delete_insert` by `replace_key` (`keyed`) cannot load where `context` puts it: a files target replaces whole partition folders; a files source picks its files from the run log, not by a cursor; and on any other source a cursor would read part of a key value's rows while the commit deletes all of them, losing those the read never returned.
pub(crate) fn replace_key_problems(
    context: &OptionContext<'_>,
    cursor: Option<&CursorSpec>,
    keyed: bool,
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if !keyed {
        return problems;
    }
    if context.target.path.is_some() {
        problems.push("replace_key deletes rows by key in a database table, and a files target replaces whole partition folders. Fix: load into a database target, or drop replace_key and use a partition cursor_column".to_owned());
    }
    match (cursor, context.source.kind) {
        (Some(cursor), SourceKind::Files) => problems.push(format!(
            "cursor_column {} does not apply to a files source's delete_insert by replace_key, which reads each new or changed file once, oldest first, from the run log. Fix: remove cursor_column, or drop replace_key to replace partitions by the cursor",
            cursor.column
        )),
        (Some(cursor), _) => problems.push(format!(
            "cursor_column {} does not apply to delete_insert by replace_key: a load deletes every row of each key value it reads, so the rows of a key value outside the cursor range, which it never reads, would be lost. Fix: drop cursor_column, so every load reads the whole source and replaces each key value it holds; or use write_strategy = \"merge\" on a unique_key to read by the cursor",
            cursor.column
        )),
        (None, _) => {}
    }
    problems
}

/// Why the table's cursor columns and `null_cursor` cannot apply: an empty or repeating `cursor_column` list; a coalesced cursor where its value is one column's (a files source's partition or path column, a Python table's field, a files target's partitions, `delete_insert`'s replaced range); `null_cursor` on a table entry without a cursor; and `null_cursor = "include"`, which reads the NULL-cursor rows on every run, on anything but `merge`.
pub(crate) fn cursor_problems(
    context: &OptionContext<'_>,
    (settings, defaults): (&TableSettings, &TableSettings),
    strategy: WriteStrategy,
    cursor: Option<&CursorSpec>,
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let listed = layered(&settings.cursor_column, &defaults.cursor_column).map(|(value, _)| value);
    if listed.as_ref().is_some_and(Vec::is_empty) {
        problems.push("cursor_column = [] names no column; name one, or list the columns to coalesce, such as [\"updated_at\", \"created_at\"]".to_owned());
    }
    let listed = listed.unwrap_or_default();
    for (index, column) in listed.iter().enumerate() {
        if listed[..index]
            .iter()
            .any(|earlier| earlier.eq_ignore_ascii_case(column))
        {
            problems.push(format!(
                "cursor_column lists {column} twice; list each column once"
            ));
            break;
        }
    }
    if layered(&settings.null_cursor, &defaults.null_cursor)
        .is_some_and(|(_, origin)| origin == SettingOrigin::Table)
        && cursor.is_none()
    {
        problems.push("null_cursor says what a cursor read does with rows whose cursor is NULL, but the table has no cursor_column; remove it".to_owned());
    }
    let out_of_range = layered(
        &settings.on_cursor_out_of_range,
        &defaults.on_cursor_out_of_range,
    );
    match (out_of_range, context.source.kind) {
        (None, _) => {}
        (Some(_), SourceKind::Python) if cursor.is_none() => problems.push("on_cursor_out_of_range says what a load does with rows outside the cursor window, but the table has no cursor_column; remove it".to_owned()),
        (Some(_), SourceKind::Python) => {}
        (Some(_), kind) => problems.push(format!(
            "on_cursor_out_of_range checks the rows a Python table's function returns against its cursor window; SQLLoad filters a {} source's reads itself, so remove it",
            kind.name()
        )),
    }
    let Some(cursor) = cursor else {
        return problems;
    };
    let label = cursor.label();
    let coalesced = !cursor.coalesce.is_empty();
    let one_column = match context.source.kind {
        SourceKind::Files => Some(
            "a files source's cursor is one partition or path column, the same in every row of a file",
        ),
        SourceKind::Python => Some(
            "a Python table's cursor is one field of the rows it yields; yield the first set value as one field (such as row[\"updated_at\"] or row[\"created_at\"]) and name that field",
        ),
        _ if context.target.path.is_some() => {
            Some("a files target partitions and reads its maximum by one cursor column")
        }
        _ if strategy == WriteStrategy::DeleteInsert => Some(
            "delete_insert replaces the destination rows in a range of one cursor column; use merge, or name one column",
        ),
        _ => None,
    };
    if let Some(reason) = one_column.filter(|_| coalesced) {
        problems.push(format!(
            "cursor_column lists several columns to coalesce ({label}), but {reason}"
        ));
    }
    if cursor.null_cursor == NullCursor::Include {
        let refusal = match strategy {
            WriteStrategy::Append => Some((
                "append would append them again on every run, duplicating them",
                format!(
                    "use write_strategy = \"merge\" with a unique_key, or coalesce a column that is always set (such as cursor_column = [\"{}\", \"created_at\"]), or set null_cursor = \"ignore\" to skip them",
                    cursor.column
                ),
            )),
            WriteStrategy::DeleteInsert => Some((
                "they sit outside every cursor range delete_insert replaces, so it would insert them again on every run, duplicating them",
                "use write_strategy = \"merge\" with a unique_key, or set null_cursor = \"ignore\" to skip them".to_owned(),
            )),
            WriteStrategy::Merge | WriteStrategy::Replace => None,
        };
        if let Some((duplicates, fix)) = refusal {
            problems.push(format!(
                "null_cursor = \"include\" reads the rows whose {label} is NULL on every run, and {duplicates}; {fix}"
            ));
        }
    }
    problems
}

/// The `newest_by` columns of a merge, and the problems with `on_duplicate_keys` and `newest_by` (a source's apply only to its merge tables, and either one set on a table drops the other from its source): both are for `merge`, `newest_by` names columns that are neither in the key, masked nor repeated, and has nothing to pick under `on_duplicate_keys = "fail"`.
pub(crate) fn dedupe_settings(
    strategy: WriteStrategy,
    unique_key: &[String],
    (on_duplicate_keys, newest_by): (Option<OnDuplicateKeys>, Option<&[String]>),
    overrides: &TableOverride,
) -> (Vec<String>, Vec<String>) {
    let mut problems: Vec<String> = Vec::new();
    if strategy != WriteStrategy::Merge {
        problems.extend(
            [
                on_duplicate_keys.map(|_| "on_duplicate_keys"),
                newest_by.map(|_| "newest_by"),
            ]
            .into_iter()
            .flatten()
            .map(|name| format!("{name} is for write_strategy = \"merge\", which matches rows on unique_key; {strategy} keeps every row")),
        );
        return (Vec::new(), problems);
    }
    let Some(columns) = newest_by else {
        return (Vec::new(), problems);
    };
    if columns.is_empty() {
        problems.push("newest_by names a column, or a list compared in order, such as newest_by = \"updated_at\"".to_owned());
    }
    for (index, column) in columns.iter().enumerate() {
        if columns[..index].contains(column) {
            problems.push(format!(
                "newest_by names {column} twice; name each column once"
            ));
        }
        if unique_key
            .iter()
            .any(|key| key.eq_ignore_ascii_case(column))
        {
            problems.push(format!("newest_by column {column} is in the unique_key, so it is the same in every row it would compare; name another column"));
        }
        if overrides.mask.contains_key(column) {
            problems.push(format!(
                "newest_by column {column} is masked, so its values cannot say which row is newer"
            ));
        }
    }
    if on_duplicate_keys == Some(OnDuplicateKeys::Fail) {
        problems.push("newest_by picks the newest of the rows repeating a key, but on_duplicate_keys = \"fail\" keeps none; set one of them".to_owned());
    }
    (columns.to_vec(), problems)
}

/// Why a files source's `after_load` cannot apply to its table: only `append`, `merge` and `delete_insert` by a `replace_key` (`keyed`) never read a loaded file again, and a files target commits its own way.
pub(crate) fn after_load_problems(
    context: &OptionContext<'_>,
    (strategy, keyed): (WriteStrategy, bool),
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if context.source.after_load == AfterLoad::Keep {
        return problems;
    }
    if !matches!(strategy, WriteStrategy::Append | WriteStrategy::Merge) && !keyed {
        problems.push(format!(
            "after_load deletes or moves each file once it loads, so it needs write_strategy = \"append\" or \"merge\", or \"delete_insert\" with a replace_key; {strategy} reads loaded files again"
        ));
    }
    if context.target.path.is_some() {
        problems.push("after_load is for database targets so far, not a files target".to_owned());
    }
    problems
}

/// Why `on_overlapping_files` does not apply to a table loaded by `strategy`: it is for a `delete_insert` by `replace_key` (`keyed`) of a source of `files`, where two files of the folder can hold one key value.
pub(crate) fn overlapping_files_problem(
    files: bool,
    strategy: WriteStrategy,
    keyed: bool,
) -> Option<String> {
    const FOR: &str = "on_overlapping_files says which file wins when two files of a files source's folder hold one replace_key value";
    match (files, strategy, keyed) {
        (true, _, true) => None,
        (false, _, _) => Some(format!("{FOR}; this source reads no files, so remove it")),
        (true, WriteStrategy::Merge, _) => Some(format!(
            "{FOR}; a merge already lets the last file win each unique_key; remove it"
        )),
        (true, WriteStrategy::DeleteInsert, false) => Some(format!(
            "{FOR}; this delete_insert replaces a cursor range, not key values, so remove it, or set replace_key"
        )),
        (true, strategy, false) => Some(format!(
            "{FOR}; {strategy} replaces no key values, so remove it"
        )),
    }
}
