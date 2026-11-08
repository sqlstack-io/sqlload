use crate::project::models::{CheckName, TableChecks};
use crate::schema_plan::models::DesiredColumn;

/// The check's key in `checks`.
pub(crate) fn check_key(check: CheckName) -> &'static str {
    match check {
        CheckName::Unique => "unique",
        CheckName::NotNull => "not_null",
        CheckName::MinRows => "min_rows",
        CheckName::MaxRowDrop => "max_row_drop",
    }
}

/// The checks `checks` sets.
pub(crate) fn set_checks(checks: &TableChecks) -> Vec<CheckName> {
    [
        (CheckName::Unique, checks.unique.is_some()),
        (CheckName::NotNull, !checks.not_null.is_empty()),
        (CheckName::MinRows, checks.min_rows.is_some()),
        (CheckName::MaxRowDrop, checks.max_row_drop.is_some()),
    ]
    .into_iter()
    .filter_map(|(check, set)| set.then_some(check))
    .collect()
}

/// `checks` with each column named as the load writes it (matched ignoring case), so its queries quote the staged column's own name.
pub(crate) fn resolved(checks: &TableChecks, columns: &[DesiredColumn]) -> TableChecks {
    TableChecks {
        unique: checks.unique.as_ref().map(|key| loaded_names(columns, key)),
        not_null: loaded_names(columns, &checks.not_null),
        ..checks.clone()
    }
}

/// Each of `names` as [`loaded_name`] gives it.
fn loaded_names(columns: &[DesiredColumn], names: &[String]) -> Vec<String> {
    names
        .iter()
        .map(|name| loaded_name(columns, name))
        .collect()
}

/// The written column `name` matches ignoring case, else `name` as given.
fn loaded_name(columns: &[DesiredColumn], name: &str) -> String {
    columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))
        .map_or_else(|| name.to_owned(), |column| column.name.clone())
}

/// What a failed check leaves published: nothing, or the rows of earlier commits.
pub(crate) fn not_published(committed: u64) -> String {
    match committed {
        0 => "nothing was published".to_owned(),
        rows => format!(
            "this commit's rows were not published; the {rows} row(s) earlier commits published stay"
        ),
    }
}
