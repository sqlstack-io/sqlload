//! Tables as the driver lists them, and their columns as a prepared `SELECT` describes them.

use arrow_odbc::arrow_schema_from;
use arrow_odbc::odbc_api::Connection;
use arrow_odbc::odbc_api::parameter::VarCharArray;
use arrow_schema::{Field, Schema};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

use crate::column_types::main::portable_type::portable_type;
use crate::odbc_source::_helpers::query::from_item;
use crate::odbc_source::constants::{ANY_NAME, TABLE_TYPES};
use crate::odbc_source::errors::OdbcSourceError;
use crate::postgres_source::models::{PgType, SourceColumn, SourceTable};

/// One glob set for `schema.table` patterns, matched ignoring case as most drivers fold names.
pub(crate) fn table_matcher(patterns: &[String]) -> Result<GlobSet, OdbcSourceError> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = GlobBuilder::new(pattern)
            .case_insensitive(true)
            .build()
            .map_err(|error| {
                OdbcSourceError::Invalid(format!("invalid table pattern {pattern:?}: {error}"))
            })?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|error| OdbcSourceError::Invalid(error.to_string()))
}

/// The columns of `SELECT * FROM <from>`, as the driver describes the prepared statement; nothing is run.
pub(crate) fn described(
    connection: &Connection<'_>,
    from: &str,
) -> Result<Vec<SourceColumn>, OdbcSourceError> {
    let dbms: String = connection.database_management_system_name()?;
    let mut prepared = connection.prepare(&format!("SELECT * FROM {from}"))?;
    let schema: Schema = arrow_schema_from(&mut prepared, Some(&dbms), false)?;
    Ok(schema
        .fields()
        .iter()
        .map(|field| source_column(field))
        .collect())
}

/// A described column: its Arrow type shown as its source type, and the portable type that holds it.
fn source_column(field: &Field) -> SourceColumn {
    SourceColumn {
        name: field.name().clone(),
        postgres_type: field.data_type().to_string(),
        pg_type: PgType::Other,
        column_type: portable_type(field),
        nullable: field.is_nullable(),
        source_spelling: None,
    }
}

/// A source table of `columns`, its key as the driver reports it.
pub(crate) fn source_table(
    (schema, name): (String, String),
    columns: Vec<SourceColumn>,
    primary_key: Vec<String>,
) -> SourceTable {
    SourceTable {
        schema,
        name,
        columns,
        primary_key,
        indexed_columns: Vec::new(),
        estimated_rows: -1,
        estimated_bytes: -1,
        comment: None,
    }
}

/// The tables and views whose `schema.table` (or `table`, for a driver without schemas) matches `include` and not `exclude`, with their columns and keys.
pub(crate) fn catalog_tables(
    connection: &Connection<'_>,
    (include, exclude): (&[String], &[String]),
) -> Result<Vec<SourceTable>, OdbcSourceError> {
    let include = table_matcher(include)?;
    let exclude = table_matcher(exclude)?;
    let mut names: Vec<(String, String)> = Vec::new();
    for row in connection.tables("", ANY_NAME, ANY_NAME, TABLE_TYPES)? {
        let row = row?;
        let (schema, table) = (catalog_text(&row.schema)?, catalog_text(&row.table)?);
        let qualified = match schema.is_empty() {
            true => table.clone(),
            false => format!("{schema}.{table}"),
        };
        if include.is_match(&qualified) && !exclude.is_match(&qualified) {
            names.push((schema, table));
        }
    }
    let mut tables: Vec<SourceTable> = Vec::with_capacity(names.len());
    for (schema, table) in names {
        let columns = described(connection, &from_item(&schema, &table, None))?;
        let key = primary_key(connection, &schema, &table)?;
        tables.push(source_table((schema, table), columns, key));
    }
    Ok(tables)
}

/// The table's primary key columns in key order, as the driver reports them; empty when it has none.
fn primary_key(
    connection: &Connection<'_>,
    schema: &str,
    table: &str,
) -> Result<Vec<String>, OdbcSourceError> {
    let mut key: Vec<(i16, String)> = Vec::new();
    let schema_name = (!schema.is_empty()).then_some(schema);
    for row in connection.primary_keys(None, schema_name, table)? {
        let row = row?;
        key.push((row.key_seq, catalog_text(&row.column)?));
    }
    key.sort();
    let columns: Vec<String> = key.into_iter().map(|(_, column)| column).collect();
    Ok(columns)
}

/// A catalog field's text, empty when NULL; text that is not UTF-8 fails rather than being dropped.
fn catalog_text<const LENGTH: usize>(
    value: &VarCharArray<LENGTH>,
) -> Result<String, OdbcSourceError> {
    match value.as_str() {
        Ok(text) => Ok(text.unwrap_or_default().to_owned()),
        Err(error) => Err(OdbcSourceError::Invalid(format!(
            "the driver's catalog holds a name that is not UTF-8: {error}"
        ))),
    }
}
