//! The checks a commit's staged rows fail.

use crate::adapters::contract::models::{CheckQuery, TableName};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::loader::errors::LoaderError;
use crate::number_text::main::thousands::thousands;
use crate::project::models::{CheckName, TableChecks};
use crate::publish_checks::_helpers::names::{counted_rows, not_published};
use crate::publish_checks::constants::{EXAMPLE_CHARS, PERCENT};
use crate::publish_checks::models::CheckedCommit;

/// Each check of `checks` that `commit`'s staged rows fail, with what failed.
pub(crate) async fn failures(
    commit: &CheckedCommit<'_>,
    checks: &TableChecks,
) -> Result<Vec<(CheckName, String)>, LoaderError> {
    let mut failures: Vec<(CheckName, String)> = Vec::new();
    let counted =
        checks.min_rows.is_some() || checks.max_row_drop.is_some() || !checks.not_null.is_empty();
    let counts = match counted {
        true => counts_of(commit, commit.staging, &checks.not_null).await?,
        false => vec![0],
    };
    let staged = counts[0];
    if let Some(key) = &checks.unique {
        failures.extend(repeated_keys(commit, key, false).await?);
    }
    for (column, nulls) in checks.not_null.iter().zip(&counts[1..]) {
        if *nulls > 0 {
            failures.push((
                CheckName::NotNull,
                match nulls {
                    1 => format!("1 row has no {column}"),
                    _ => format!("{} have no {column}", counted_rows(*nulls)),
                },
            ));
        }
    }
    if let Some(minimum) = checks.min_rows.filter(|minimum| staged < *minimum) {
        failures.push((
            CheckName::MinRows,
            format!(
                "{} staged, fewer than the {} required",
                counted_rows(staged),
                thousands(minimum)
            ),
        ));
    }
    if let Some(limit) = checks.max_row_drop.filter(|_| commit.exists) {
        let live = counts_of(commit, commit.table, &[]).await?[0];
        let dropped = live.saturating_sub(staged);
        if dropped.saturating_mul(PERCENT) > live.saturating_mul(u64::from(limit)) {
            failures.push((
                CheckName::MaxRowDrop,
                format!(
                    "the table would go from {} to {}, losing more than {limit}%",
                    thousands(live),
                    counted_rows(staged)
                ),
            ));
        }
    }
    Ok(failures)
}

/// The `unique` failure for `key`, with up to five repeated values, each cut short; `None` when no value repeats. With `nulls`, a NULL in the key compares equal to a NULL, as a merge matches it; without, rows with one are skipped, as the `unique` check does.
pub(crate) async fn repeated_keys(
    commit: &CheckedCommit<'_>,
    key: &[String],
    nulls: bool,
) -> Result<Option<(CheckName, String)>, LoaderError> {
    let adapter = sql_adapter(commit.warehouse);
    let (count, examples) = match nulls {
        true => (
            CheckQuery::KeyDuplicates(key),
            CheckQuery::KeyDuplicateExamples(key),
        ),
        false => (
            CheckQuery::Duplicates(key),
            CheckQuery::DuplicateExamples(key),
        ),
    };
    let query = adapter.render_check_query(commit.staging, count);
    let repeated = single_count(commit, query).await?;
    if repeated == 0 {
        return Ok(None);
    }
    let query = adapter.render_check_query(commit.staging, examples);
    let examples: Vec<String> = text_values(commit.destination, query)
        .await
        .map_err(|error| could_not_run(commit, &error))?
        .iter()
        .map(|example| shortened(example))
        .collect();
    Ok(Some((
        CheckName::Unique,
        format!(
            "{} of ({}) {} in more than one row, such as ({})",
            match repeated {
                1 => "1 value".to_owned(),
                _ => format!("{} values", thousands(repeated)),
            },
            key.join(", "),
            match repeated {
                1 => "appears",
                _ => "appear",
            },
            examples.join("), (")
        ),
    )))
}

/// The rows of `commit` holding a NULL in a column of `key`: how many lack each column (those that any lack), and up to five of their keys, each cut short; `None` when no row does.
pub(crate) async fn null_keys(
    commit: &CheckedCommit<'_>,
    key: &[String],
) -> Result<Option<String>, LoaderError> {
    let counts = counts_of(commit, commit.staging, key).await?;
    let missing: Vec<String> = key
        .iter()
        .zip(&counts[1..])
        .filter(|(_, nulls)| **nulls > 0)
        .map(|(column, nulls)| format!("{nulls} row(s) have no {column}"))
        .collect();
    if missing.is_empty() {
        return Ok(None);
    }
    let query = sql_adapter(commit.warehouse)
        .render_check_query(commit.staging, CheckQuery::NullKeyExamples(key));
    let examples: Vec<String> = text_values(commit.destination, query)
        .await
        .map_err(|error| could_not_run(commit, &error))?
        .iter()
        .map(|example| shortened(example))
        .collect();
    Ok(Some(format!(
        "{}, such as ({}) of ({})",
        missing.join(", "),
        examples.join("), ("),
        key.join(", ")
    )))
}

/// `text` cut to [`EXAMPLE_CHARS`] characters, marked when cut.
fn shortened(text: &str) -> String {
    match text.chars().count() > EXAMPLE_CHARS {
        true => format!(
            "{}...",
            text.chars().take(EXAMPLE_CHARS).collect::<String>()
        ),
        false => text.to_owned(),
    }
}

/// The row count of `relation`, then its NULL count for each of `columns`, from one query.
async fn counts_of(
    commit: &CheckedCommit<'_>,
    relation: &TableName,
    columns: &[String],
) -> Result<Vec<u64>, LoaderError> {
    let query =
        sql_adapter(commit.warehouse).render_check_query(relation, CheckQuery::Counts(columns));
    let values = text_values(commit.destination, query)
        .await
        .map_err(|error| could_not_run(commit, &error))?;
    let parsed: Result<Vec<u64>, _> = values
        .first()
        .map(String::as_str)
        .unwrap_or_default()
        .split(',')
        .map(|count| count.trim().parse::<u64>())
        .collect();
    match parsed {
        Ok(counts) if counts.len() == columns.len() + 1 => Ok(counts),
        _ => Err(not_a_count(relation, &values)),
    }
}

/// The single count `query` returns.
pub(crate) async fn single_count(
    commit: &CheckedCommit<'_>,
    query: String,
) -> Result<u64, LoaderError> {
    let values = text_values(commit.destination, query)
        .await
        .map_err(|error| could_not_run(commit, &error))?;
    let parsed = values.first().map(|value| value.trim().parse::<u64>());
    match parsed {
        Some(Ok(count)) => Ok(count),
        _ => Err(not_a_count(commit.staging, &values)),
    }
}

/// The error for a check query whose result is not the counts it asked for.
fn not_a_count(relation: &TableName, values: &[String]) -> LoaderError {
    LoaderError::Invalid(format!(
        "a check's count on {} came back as {values:?}, not numbers",
        relation.table
    ))
}

/// A check query's failure, saying it was a check that could not run.
fn could_not_run(commit: &CheckedCommit<'_>, error: &LoaderError) -> LoaderError {
    LoaderError::Invalid(format!(
        "the checks could not run on {}.{}: {error}; {}",
        commit.table.schema,
        commit.table.table,
        not_published(commit.committed)
    ))
}
