//! Discover an ODBC source's tables: catalog tables matching `include`, and query tables described without reading rows.

use crate::odbc_source::_helpers::catalog::{catalog_tables, described, source_table};
use crate::odbc_source::_helpers::connect::connect;
use crate::odbc_source::_helpers::query::from_item;
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::{KeyUses, OdbcReader};
use crate::postgres_source::models::SourceTable;

/// Every table and view whose `schema.table` (or `table`, for a driver without schemas) matches `include` and not `exclude` (ignoring case), then one table per query in `queries` (name and SQL); columns as the driver describes a prepared `SELECT *`, and a table's key as the driver reports it where `keys` says the table uses it. The driver is asked for each `include` pattern's tables, or for every table once when that can't stand for them.
pub async fn read_catalog(
    reader: &OdbcReader,
    (include, exclude, keys): (&[String], &[String], &KeyUses),
    queries: &[(String, String)],
) -> Result<Vec<SourceTable>, OdbcSourceError> {
    let reader = reader.clone();
    let patterns = (include.to_vec(), exclude.to_vec(), keys.clone());
    let queries = queries.to_vec();
    tokio::task::spawn_blocking(move || catalog(&reader, &patterns, &queries))
        .await
        .map_err(|error| OdbcSourceError::Invalid(format!("the catalog read stopped: {error}")))?
}

fn catalog(
    reader: &OdbcReader,
    (include, exclude, keys): &(Vec<String>, Vec<String>, KeyUses),
    queries: &[(String, String)],
) -> Result<Vec<SourceTable>, OdbcSourceError> {
    let connection = connect(reader)?;
    let mut tables: Vec<SourceTable> = match include.is_empty() {
        true => Vec::new(),
        false => catalog_tables(&connection, (include, exclude), keys)?,
    };
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
