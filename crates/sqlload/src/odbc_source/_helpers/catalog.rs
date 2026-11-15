//! Tables as the driver lists them for `include`, their columns as a prepared `SELECT` describes them, and their keys where a table uses them.

use arrow_odbc::arrow_schema_from;
use arrow_odbc::odbc_api::parameter::VarCharArray;
use arrow_odbc::odbc_api::{Connection, Cursor, IntoParameter};
use arrow_schema::{Field, Schema};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

use crate::column_types::main::portable_type::portable_type;
use crate::odbc_source::_helpers::query::from_item;
use crate::odbc_source::constants::{ANY_NAME, LISTED_KEYS_QUERY, TABLE_TYPES};
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::{KeyUse, KeyUses, ListedTable};
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
        repeat_of: None,
    }
}

/// A source table of `columns`, its key as the driver reports it.
pub(crate) fn source_table(
    (schema, name): (String, String),
    columns: Vec<SourceColumn>,
    primary_key: Vec<String>,
) -> SourceTable {
    SourceTable {
        split_pages: None,
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

/// The tables and views whose `schema.table` (or `table`, for a driver without schemas) matches `include` and not `exclude`, with their columns. Each `include` pattern is looked up on its own ([`narrowed`]); when that can't stand for every pattern, the driver lists every table once instead. A table's primary key is read only when `keys` says it is used, and for a table merging on it that the driver reports none for, the keys `INFORMATION_SCHEMA` lists.
pub(crate) fn catalog_tables(
    connection: &Connection<'_>,
    (include, exclude): (&[String], &[String]),
    keys: &KeyUses,
) -> Result<Vec<SourceTable>, OdbcSourceError> {
    let included = table_matcher(include)?;
    let exclude = table_matcher(exclude)?;
    let listed = match narrowed(connection, include) {
        Ok(listed) => listed,
        Err(reason) => {
            tracing::info!(
                reason,
                "listing every table the ODBC driver reports, as include can't be looked up pattern by pattern"
            );
            listed_tables(connection, (ANY_NAME, ANY_NAME), true)?.0
        }
    };
    let mut tables: Vec<SourceTable> = Vec::new();
    for ListedTable {
        catalog,
        schema,
        name: table,
    } in listed
    {
        let qualified = qualified_name(&schema, &table);
        if !included.is_match(&qualified) || exclude.is_match(&qualified) {
            continue;
        }
        let columns = described(connection, &from_item(&schema, &table, None))?;
        let used = keys.tables.get(&qualified).copied().unwrap_or(keys.source);
        let key = match used {
            KeyUse::Unused => Vec::new(),
            KeyUse::Read | KeyUse::Merged => primary_key(connection, &schema, &table)?,
        };
        let listed_keys = match (used, key.is_empty()) {
            (KeyUse::Merged, true) => listed_keys(connection, (&catalog, &schema), &table),
            _ => Vec::new(),
        };
        tables.push(SourceTable {
            listed_keys,
            ..source_table((schema, table), columns, key)
        });
    }
    Ok(tables)
}

/// The tables each `include` pattern's own lookup lists that it matches, each once, in the order found; or why the whole catalog must be listed instead: a pattern no lookup stands for ([`search_patterns`]), a lookup the driver refuses, or one that finds no table its pattern matches, as a driver comparing names case-sensitively finds none for `include`'s other case, or for a table that doesn't exist.
fn narrowed(connection: &Connection<'_>, include: &[String]) -> Result<Vec<ListedTable>, String> {
    let mut schemas = true;
    let mut found: Vec<ListedTable> = Vec::new();
    for pattern in include {
        let search = search_patterns(pattern)
            .ok_or_else(|| format!("{pattern:?} is not a schema.table name or * and ? pattern"))?;
        let matcher =
            table_matcher(std::slice::from_ref(pattern)).map_err(|error| error.to_string())?;
        let (listed, takes_schemas) = listed_tables(connection, (&search.0, &search.1), schemas)
            .map_err(|error| format!("looking up {pattern:?} failed: {error}"))?;
        schemas = takes_schemas;
        let matching: Vec<ListedTable> = listed
            .into_iter()
            .filter(|listed| matcher.is_match(qualified_name(&listed.schema, &listed.name)))
            .collect();
        if matching.is_empty() {
            return Err(format!(
                "looking up {pattern:?} found no table it matches (the driver may compare names case-sensitively)"
            ));
        }
        for table in matching {
            if !found.contains(&table) {
                found.push(table);
            }
        }
    }
    Ok(found)
}

/// `pattern` as the schema and table search patterns `SQLTables` takes: `*` as `%` and `?` as `_`. A `_` or `%` of its own stays the driver's wildcard, unescaped, as the driver's escape character can't be read safely: the lookup may then list more tables, which `include` drops. A pattern without a `.` names a table in any schema. `None` when no lookup stands for it: a character class, alternatives or an escape (`[`, `{`, `\`), more than one `.`, or `*` or `?` without a `.`, which could match across one.
pub(crate) fn search_patterns(pattern: &str) -> Option<(String, String)> {
    if pattern.contains(['[', ']', '{', '}', '\\']) {
        return None;
    }
    let search = |part: &str| part.replace('*', "%").replace('?', "_");
    match pattern.split_once('.') {
        Some((schema, table)) if !table.contains('.') => Some((search(schema), search(table))),
        None if !pattern.contains(['*', '?']) => Some((ANY_NAME.to_owned(), pattern.to_owned())),
        _ => None,
    }
}

/// The catalog, schema and name of each table and view `SQLTables` lists for the schema and table search patterns, and whether the driver takes a schema pattern of any schema, as `schemas` says it did before. A driver that refuses one and reports no schema name length (MySQL Connector/ODBC, by default) is asked again with none, and later lookups of any schema ask with none; one reporting 0 for "no maximum" still lists every schema.
fn listed_tables(
    connection: &Connection<'_>,
    (schema, table): (&str, &str),
    schemas: bool,
) -> Result<(Vec<ListedTable>, bool), OdbcSourceError> {
    let schema = match (schemas, schema) {
        (false, ANY_NAME) => "",
        _ => schema,
    };
    let (listed, schemas) = match connection.tables("", schema, table, TABLE_TYPES) {
        Ok(listed) => (listed, schemas),
        Err(refused) => match (schema, connection.max_schema_name_len()) {
            (ANY_NAME, Ok(0)) => (
                connection
                    .tables("", "", table, TABLE_TYPES)
                    .map_err(|_| refused)?,
                false,
            ),
            _ => return Err(refused.into()),
        },
    };
    let mut names: Vec<ListedTable> = Vec::new();
    for row in listed {
        let row = row?;
        names.push(ListedTable {
            catalog: catalog_text(&row.catalog)?,
            schema: catalog_text(&row.schema)?,
            name: catalog_text(&row.table)?,
        });
    }
    Ok((names, schemas))
}

/// `schema.table`, or `table` for a driver without schemas, as `include` and `exclude` match it.
fn qualified_name(schema: &str, table: &str) -> String {
    match schema.is_empty() {
        true => table.to_owned(),
        false => format!("{schema}.{table}"),
    }
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
