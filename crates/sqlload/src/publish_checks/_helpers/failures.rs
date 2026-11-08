//! The checks a commit's staged rows fail.

use crate::adapters::contract::models::{CheckQuery, TableName};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::loader::errors::LoaderError;
use crate::project::models::{CheckName, TableChecks};
use crate::publish_checks::models::CheckedCommit;

/// Each check of `checks` that `commit`'s staged rows fail, with what failed.
pub(crate) async fn failures(
    commit: &CheckedCommit<'_>,
    checks: &TableChecks,
) -> Result<Vec<(CheckName, String)>, LoaderError> {
    let mut failures: Vec<(CheckName, String)> = Vec::new();
    let staged = match (checks.min_rows, checks.max_row_drop) {
        (None, None) => 0,
        _ => count(commit, commit.staging, CheckQuery::Rows).await?,
    };
    if let Some(key) = &checks.unique {
        failures.extend(repeated_keys(commit, key).await?);
    }
    for column in &checks.not_null {
        let nulls = count(commit, commit.staging, CheckQuery::Nulls(column)).await?;
        if nulls > 0 {
            failures.push((
                CheckName::NotNull,
                format!("{nulls} row(s) have no {column}"),
            ));
        }
    }
    if let Some(minimum) = checks.min_rows.filter(|minimum| staged < *minimum) {
        failures.push((
            CheckName::MinRows,
            format!("{staged} row(s) staged, fewer than the {minimum} required"),
        ));
    }
    if let Some(limit) = checks.max_row_drop.filter(|_| commit.exists) {
        let live = count(commit, commit.table, CheckQuery::Rows).await?;
        let dropped = live.saturating_sub(staged);
        if dropped.saturating_mul(100) > live.saturating_mul(u64::from(limit)) {
            failures.push((
                CheckName::MaxRowDrop,
                format!(
                    "the table would go from {live} to {staged} rows, losing more than {limit}%"
                ),
            ));
        }
    }
    Ok(failures)
}

/// The `unique` failure for `key`, with up to five repeated values; `None` when no value repeats.
async fn repeated_keys(
    commit: &CheckedCommit<'_>,
    key: &[String],
) -> Result<Option<(CheckName, String)>, LoaderError> {
    let repeated = count(commit, commit.staging, CheckQuery::Duplicates(key)).await?;
    if repeated == 0 {
        return Ok(None);
    }
    let query = sql_adapter(commit.warehouse)
        .render_check_query(commit.staging, CheckQuery::DuplicateExamples(key));
    let examples = text_values(commit.destination, query).await?;
    Ok(Some((
        CheckName::Unique,
        format!(
            "{repeated} value(s) of ({}) appear in more than one row, such as ({})",
            key.join(", "),
            examples.join("), (")
        ),
    )))
}

/// The single count `check` returns on `relation`.
async fn count(
    commit: &CheckedCommit<'_>,
    relation: &TableName,
    check: CheckQuery<'_>,
) -> Result<u64, LoaderError> {
    let query = sql_adapter(commit.warehouse).render_check_query(relation, check);
    let values = text_values(commit.destination, query).await?;
    let parsed = values.first().map(|value| value.trim().parse::<u64>());
    match parsed {
        Some(Ok(count)) => Ok(count),
        _ => Err(LoaderError::Invalid(format!(
            "a check's count on {} came back as {values:?}, not a number",
            relation.table
        ))),
    }
}
