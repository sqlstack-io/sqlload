//! Discover a BigQuery source's tables: tables and views matching `include`, and query tables described by a dry run.

use futures_util::{StreamExt, TryStreamExt, stream};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

use crate::bigquery_api::main::list_tables::{list_datasets, list_tables};
use crate::bigquery_api::main::query_schema::query_schema;
use crate::bigquery_api::main::table_metadata::table_metadata;
use crate::bigquery_source::_helpers::type_map::source_column;
use crate::bigquery_source::constants::{READ_API_TABLES, TABLES_AT_ONCE};
use crate::bigquery_source::errors::BigQuerySourceError;
use crate::bigquery_source::models::BigQueryReader;
use crate::postgres_source::models::{SourceColumn, SourceTable};

/// Every table and view of the reader's project whose `dataset.table` matches `include` and not `exclude` (exactly, as BigQuery names are), from metadata, so nothing is billed; then one table per query in `queries` (name and SQL), described by a dry run. A view, or anything else the Storage Read API cannot read directly, has `estimated_bytes` -1, so it is read through a query.
pub async fn read_catalog(
    reader: &BigQueryReader,
    (include, exclude): (&[String], &[String]),
    queries: &[(String, String)],
) -> Result<Vec<SourceTable>, BigQuerySourceError> {
    let mut tables: Vec<SourceTable> = match include.is_empty() {
        true => Vec::new(),
        false => catalog_tables(reader, (include, exclude)).await?,
    };
    for (name, sql) in queries {
        let fields = query_schema(&reader.client, sql).await.map_err(|error| {
            BigQuerySourceError::Invalid(format!("query table {name}: {error}"))
        })?;
        tables.push(SourceTable {
            schema: String::new(),
            name: name.clone(),
            columns: fields.iter().map(source_column).collect(),
            primary_key: Vec::new(),
            indexed_columns: Vec::new(),
            estimated_rows: -1,
            estimated_bytes: -1,
            comment: None,
        });
    }
    Ok(tables)
}

async fn catalog_tables(
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
        .map(|(dataset, table)| async move {
            let found = table_metadata(&reader.client, (&reader.project, &dataset, &table)).await?;
            Ok::<Option<SourceTable>, BigQuerySourceError>(found.map(|found| {
                let count = |key: &str| {
                    found[key]
                        .as_str()
                        .and_then(|text| text.parse::<i64>().ok())
                        .unwrap_or(-1)
                };
                let direct = READ_API_TABLES.contains(&found["type"].as_str().unwrap_or_default());
                SourceTable {
                    columns: found["schema"]["fields"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(source_column)
                        .collect::<Vec<SourceColumn>>(),
                    primary_key: found["tableConstraints"]["primaryKey"]["columns"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|column| column.as_str().map(str::to_owned))
                        .collect(),
                    indexed_columns: Vec::new(),
                    estimated_rows: count("numRows"),
                    estimated_bytes: match direct {
                        true => count("numBytes").max(0),
                        false => -1,
                    },
                    comment: found["description"].as_str().map(str::to_owned),
                    schema: dataset,
                    name: table,
                }
            }))
        })
        .buffered(TABLES_AT_ONCE)
        .try_filter_map(|table| async move { Ok(table) })
        .try_collect()
        .await
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
