//! Tables as the driver lists them, and their columns as a prepared `SELECT` describes them.

use arrow_odbc::arrow_schema_from;
use arrow_odbc::odbc_api::parameter::VarCharArray;
use arrow_odbc::odbc_api::{Connection, Cursor, IntoParameter};
use arrow_schema::{Field, Schema};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

use crate::column_types::main::portable_type::portable_type;
use crate::odbc_source::_helpers::query::from_item;
use crate::odbc_source::constants::{ANY_NAME, LISTED_KEYS_QUERY, TABLE_TYPES};
use crate::odbc_source::errors::OdbcSourceError;
use crate::postgres_source::models::{ListedKey, PgType, SourceColumn, SourceTable};

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
        listed_keys: Vec::new(),
    }
}

/// The tables and views whose `schema.table` (or `table`, for a driver without schemas) matches `include` and not `exclude`, with their columns and keys, and for a table the driver reports no primary key for, the keys `INFORMATION_SCHEMA` lists. A driver that refuses a schema pattern and reports no schema name length (MySQL Connector/ODBC, by default) is asked again with none; one reporting 0 for "no maximum" still lists every schema.
pub(crate) fn catalog_tables(
    connection: &Connection<'_>,
    (include, exclude): (&[String], &[String]),
) -> Result<Vec<SourceTable>, OdbcSourceError> {
    let include = table_matcher(include)?;
    let exclude = table_matcher(exclude)?;
    let mut names: Vec<(String, String, String)> = Vec::new();
    let listed = match connection.tables("", ANY_NAME, ANY_NAME, TABLE_TYPES) {
        Ok(listed) => listed,
        Err(refused) => match connection.max_schema_name_len() {
            Ok(0) => connection
                .tables("", "", ANY_NAME, TABLE_TYPES)
                .map_err(|_| refused)?,
            _ => return Err(refused.into()),
        },
    };
    for row in listed {
        let row = row?;
        let (catalog, schema, table) = (
            catalog_text(&row.catalog)?,
            catalog_text(&row.schema)?,
            catalog_text(&row.table)?,
        );
        let qualified = match schema.is_empty() {
            true => table.clone(),
            false => format!("{schema}.{table}"),
        };
        if include.is_match(&qualified) && !exclude.is_match(&qualified) {
            names.push((catalog, schema, table));
        }
    }
    let mut tables: Vec<SourceTable> = Vec::with_capacity(names.len());
    for (catalog, schema, table) in names {
        let columns = described(connection, &from_item(&schema, &table, None))?;
        let key = primary_key(connection, &schema, &table)?;
        let listed_keys = match key.is_empty() {
            true => listed_keys(connection, (&catalog, &schema), &table),
            false => Vec::new(),
        };
        tables.push(SourceTable {
            listed_keys,
            ..source_table((schema, table), columns, key)
        });
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

/// The primary key and unique constraints `INFORMATION_SCHEMA` lists for the table, primary key first; its schema there is the driver's schema, or its catalog for a driver without schemas (MySQL's database). They only explain an error, so they never fail the catalog read: none when the table has neither name, or the database has no `INFORMATION_SCHEMA` or answers with an error or text that is not UTF-8.
fn listed_keys(
    connection: &Connection<'_>,
    (catalog, schema): (&str, &str),
    table: &str,
) -> Vec<ListedKey> {
    let schema = match schema.is_empty() {
        true => catalog,
        false => schema,
    };
    if schema.is_empty() {
        return Vec::new();
    }
    let read = || -> Result<Vec<ListedKey>, OdbcSourceError> {
        let parameters = (&schema.into_parameter(), &table.into_parameter());
        let Some(mut cursor) = connection.execute(LISTED_KEYS_QUERY, parameters, None)? else {
            return Ok(Vec::new());
        };
        let mut keys: Vec<(String, ListedKey)> = Vec::new();
        let mut buffer: Vec<u8> = Vec::new();
        while let Some(mut row) = cursor.next_row()? {
            let mut text = |column: u16| -> Result<String, OdbcSourceError> {
                row.get_text(column, &mut buffer)?;
                String::from_utf8(buffer.clone())
                    .map_err(|error| OdbcSourceError::Invalid(error.to_string()))
            };
            let (constraint_type, name, column) = (text(1)?, text(2)?, text(3)?);
            match keys.last_mut() {
                Some((last, key)) if *last == name && key.constraint_type == constraint_type => {
                    key.columns.push(column);
                }
                _ => keys.push((
                    name,
                    ListedKey {
                        constraint_type,
                        columns: vec![column],
                    },
                )),
            }
        }
        Ok(keys.into_iter().map(|(_, key)| key).collect())
    };
    read().unwrap_or_default()
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
