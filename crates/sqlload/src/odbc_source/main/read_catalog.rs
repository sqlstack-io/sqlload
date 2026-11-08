//! Discover an ODBC source's tables: catalog tables matching `include`, and query tables described without reading rows.

use crate::odbc_source::_helpers::catalog::{described, source_table, table_matcher};
use crate::odbc_source::_helpers::connect::connect;
use crate::odbc_source::_helpers::query::from_item;
use crate::odbc_source::constants::{ANY_NAME, TABLE_TYPES};
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::OdbcReader;
use crate::postgres_source::models::SourceTable;

/// Every table and view whose `schema.table` (or `table`, for a driver without schemas) matches `include` and not `exclude` (ignoring case), then one table per query in `queries` (name and SQL); columns as the driver describes a prepared `SELECT *`, keys as it reports them.
pub async fn read_catalog(
    reader: &OdbcReader,
    (include, exclude): (&[String], &[String]),
    queries: &[(String, String)],
) -> Result<Vec<SourceTable>, OdbcSourceError> {
    let reader = reader.clone();
    let patterns = (include.to_vec(), exclude.to_vec());
    let queries = queries.to_vec();
    tokio::task::spawn_blocking(move || catalog(&reader, &patterns, &queries))
        .await
        .map_err(|error| OdbcSourceError::Invalid(format!("the catalog read stopped: {error}")))?
}

fn catalog(
    reader: &OdbcReader,
    (include, exclude): &(Vec<String>, Vec<String>),
    queries: &[(String, String)],
) -> Result<Vec<SourceTable>, OdbcSourceError> {
    let connection = connect(reader)?;
    let mut tables: Vec<SourceTable> = Vec::new();
    if !include.is_empty() {
        let include = table_matcher(include)?;
        let exclude = table_matcher(exclude)?;
        let mut names: Vec<(String, String)> = Vec::new();
        for row in connection.tables("", ANY_NAME, ANY_NAME, TABLE_TYPES)? {
            let row = row?;
            let schema = row
                .schema
                .as_str()
                .ok()
                .flatten()
                .unwrap_or_default()
                .to_owned();
            let table = row
                .table
                .as_str()
                .ok()
                .flatten()
                .unwrap_or_default()
                .to_owned();
            let qualified = match schema.is_empty() {
                true => table.clone(),
                false => format!("{schema}.{table}"),
            };
            if include.is_match(&qualified) && !exclude.is_match(&qualified) {
                names.push((schema, table));
            }
        }
        for (schema, table) in names {
            let columns = described(&connection, &from_item(&schema, &table, None))?;
            let mut key: Vec<(i16, String)> = Vec::new();
            let schema_name = (!schema.is_empty()).then_some(schema.as_str());
            for row in connection.primary_keys(None, schema_name, &table)? {
                let row = row?;
                let column = row
                    .column
                    .as_str()
                    .ok()
                    .flatten()
                    .unwrap_or_default()
                    .to_owned();
                key.push((row.key_seq, column));
            }
            key.sort();
            let key = key.into_iter().map(|(_, column)| column).collect();
            tables.push(source_table((schema, table), columns, key));
        }
    }
    for (name, sql) in queries {
        let columns = described(&connection, &from_item("", name, Some(sql)))
            .map_err(|error| OdbcSourceError::Invalid(format!("query table {name}: {error}")))?;
        tables.push(source_table(
            (String::new(), name.clone()),
            columns,
            Vec::new(),
        ));
    }
    Ok(tables)
}
