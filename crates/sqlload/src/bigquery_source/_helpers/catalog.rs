//! The tables and views `include` matches, as source tables from their metadata.

use futures_util::{StreamExt, TryStreamExt, stream};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde_json::Value;

use crate::bigquery_api::main::list_datasets::list_datasets;
use crate::bigquery_api::main::list_tables::list_tables;
use crate::bigquery_api::main::table_metadata::table_metadata;
use crate::bigquery_source::_helpers::type_map::source_column;
use crate::bigquery_source::constants::{READ_API_TABLES, TABLES_AT_ONCE};
use crate::bigquery_source::errors::BigQuerySourceError;
use crate::bigquery_source::models::BigQueryReader;
use crate::postgres_source::models::{SourceColumn, SourceTable};

/// The reader's project's tables and views whose `dataset.table` matches `include` and not `exclude`, sorted, from metadata only.
pub(crate) async fn catalog_tables(
    reader: &BigQueryReader,
    (include, exclude): (&[String], &[String]),
) -> Result<Vec<SourceTable>, BigQuerySourceError> {
    let included = matcher(include)?;
    let excluded = matcher(exclude)?;
    let datasets = matcher(
        &include
            .iter()
            .map(|pattern| {
                pattern
                    .split_once('.')
                    .map_or(pattern.as_str(), |(dataset, _)| dataset)
                    .to_owned()
            })
            .collect::<Vec<String>>(),
    )?;
    let mut names: Vec<(String, String)> = Vec::new();
    for dataset in list_datasets(&reader.client, &reader.project).await? {
        if !datasets.is_match(&dataset) {
            continue;
        }
        for (table, _) in list_tables(&reader.client, (&reader.project, &dataset)).await? {
            let qualified = format!("{dataset}.{table}");
            if included.is_match(&qualified) && !excluded.is_match(&qualified) {
                names.push((dataset.clone(), table));
            }
        }
    }
    names.sort();
    stream::iter(names)
        .map(|name| described(reader, name))
        .buffered(TABLES_AT_ONCE)
        .try_filter_map(|table| async move { Ok(table) })
        .try_collect()
        .await
}

/// `dataset.table` as a source table, or `None` when it no longer exists.
async fn described(
    reader: &BigQueryReader,
    (dataset, table): (String, String),
) -> Result<Option<SourceTable>, BigQuerySourceError> {
    let found = table_metadata(&reader.client, (&reader.project, &dataset, &table)).await?;
    Ok(found.map(|found| listed_table((dataset, table), &found)))
}

/// One glob set for `dataset.table` patterns, matched exactly.
fn matcher(patterns: &[String]) -> Result<GlobSet, BigQuerySourceError> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = GlobBuilder::new(pattern).build().map_err(|error| {
            BigQuerySourceError::Invalid(format!("invalid table pattern {pattern:?}: {error}"))
        })?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|error| BigQuerySourceError::Invalid(error.to_string()))
}

/// `dataset.table` from `found`: its columns, declared primary key, row and byte counts, and description. Anything the Storage Read API cannot read directly (a view) has `estimated_bytes` -1, so it is read through a query.
pub(crate) fn listed_table((dataset, table): (String, String), found: &Value) -> SourceTable {
    let mut columns: Vec<SourceColumn> = Vec::new();
    for field in found["schema"]["fields"].as_array().into_iter().flatten() {
        columns.push(source_column(field));
    }
    let mut primary_key: Vec<String> = Vec::new();
    for column in found["tableConstraints"]["primaryKey"]["columns"]
        .as_array()
        .into_iter()
        .flatten()
    {
        primary_key.extend(column.as_str().map(str::to_owned));
    }
    let direct = READ_API_TABLES.contains(&found["type"].as_str().unwrap_or_default());
    SourceTable {
        schema: dataset,
        name: table,
        columns,
        primary_key,
        indexed_columns: Vec::new(),
        estimated_rows: count(&found["numRows"]),
        estimated_bytes: match direct {
            true => count(&found["numBytes"]).max(0),
            false => -1,
        },
        comment: found["description"].as_str().map(str::to_owned),
        listed_keys: Vec::new(),
    }
}

/// A count the REST API gives as a string; -1 when it gives none.
fn count(value: &Value) -> i64 {
    match value.as_str().map(str::parse::<i64>) {
        Some(Ok(number)) => number,
        _ => -1,
    }
}
