use crate::project::models::{CheckName, TableChecks};

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
