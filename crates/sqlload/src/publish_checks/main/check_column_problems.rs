//! Check columns the table does not load.

use crate::project::models::TableChecks;

/// A problem for each column `checks.unique` or `checks.not_null` names that is not among the `loaded` columns (matched ignoring case).
pub fn check_column_problems(checks: &TableChecks, loaded: &[String]) -> Vec<String> {
    let unique = checks
        .unique
        .iter()
        .flatten()
        .map(|column| ("unique", column));
    let not_null = checks.not_null.iter().map(|column| ("not_null", column));
    unique
        .chain(not_null)
        .filter(|(_, column)| !is_loaded(loaded, column))
        .map(|(key, column)| format!("checks.{key} names {column}, which the table does not load"))
        .collect()
}

/// Whether `column` is among the `loaded` columns, ignoring case.
fn is_loaded(loaded: &[String], column: &str) -> bool {
    loaded.iter().any(|name| name.eq_ignore_ascii_case(column))
}
