//! What is wrong with a table's checks, before its columns are known.

use crate::project::models::{CheckName, TableChecks, WriteStrategy};
use crate::publish_checks::_helpers::names::{check_key, set_checks};

/// Why `checks` cannot run on a table written with `strategy`, into a files target when `files_target`: files targets have no staged table to check; `unique` needs a column; `min_rows` and `max_row_drop` describe the whole table, which only `replace` publishes in one commit; `warn` names only checks the table sets.
pub fn check_problems(
    checks: &TableChecks,
    (strategy, files_target): (WriteStrategy, bool),
) -> Vec<String> {
    let set = set_checks(checks);
    if files_target && !set.is_empty() {
        return vec![
            "checks run on the staged rows of a database target before they are published; a files target has none, so remove checks".to_owned(),
        ];
    }
    let mut problems: Vec<String> = Vec::new();
    if checks.unique.as_ref().is_some_and(Vec::is_empty) {
        problems.push("checks.unique needs at least one column".to_owned());
    }
    let whole_table: Vec<&str> = [CheckName::MinRows, CheckName::MaxRowDrop]
        .into_iter()
        .filter(|check| set.contains(check))
        .map(check_key)
        .collect();
    if !whole_table.is_empty() && strategy != WriteStrategy::Replace {
        problems.push(format!(
            "only write_strategy = \"replace\" publishes the whole table in one commit, so only it takes checks.{}; other strategies publish batch by batch, so check those batches with unique and not_null",
            whole_table.join(" and checks.")
        ));
    }
    problems.extend(
        checks
            .warn
            .iter()
            .filter(|check| !set.contains(check))
            .map(|check| {
                format!(
                    "checks.warn lists {}, which the table does not check",
                    check_key(*check)
                )
            }),
    );
    problems
}
