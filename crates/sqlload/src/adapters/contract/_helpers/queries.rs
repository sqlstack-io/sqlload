//! Queries and data statements: the shared rendering.

use chrono::NaiveDateTime;

use crate::adapters::contract::constants::{
    MAX_INSERT_ROWS, MAX_INSERT_SQL_BYTES, STAGING_SUFFIX, STAGING_TIME_DIGITS, STAGING_TIME_FORMAT,
};
use crate::adapters::contract::models::{
    CheckQuery, CursorQuery, CursorRead, CursorValue, TableName,
};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::load_naming::constants::CHILD_TABLE_SEPARATOR;
use crate::load_naming::main::metadata_name::metadata_name;
use crate::load_naming::models::{Compat, MetadataRole};
use crate::schema_plan::models::DesiredColumn;

/// A query for `MAX(column)` (of a coalesced cursor, `MAX(COALESCE(column, ...))`) over the rows of the request's scope, whose text never depends on session time zones or output formats; `None` if a lookback needs a time column. `MAX` skips a row whose cursor is NULL, so such rows never move it.
pub(crate) fn cursor_query<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &CursorQuery<'_>,
) -> Option<CursorRead> {
    let column = match request.coalesce.is_empty() {
        true => adapter.render_identifier(request.column),
        false => format!(
            "COALESCE({})",
            std::iter::once(request.column)
                .chain(request.coalesce.iter().map(String::as_str))
                .map(|name| adapter.render_identifier(name))
                .collect::<Vec<String>>()
                .join(", ")
        ),
    };
    let column = match request.column_type {
        ColumnType::Text | ColumnType::Varchar(_) => adapter.render_binary_text(&column),
        _ => column,
    };
    let target = adapter.render_qualified_name(request.table);
    let value = match request.column_type {
        ColumnType::TimestampTz | ColumnType::TimestampNtz => CursorValue::EpochMicros {
            zoned: request.column_type == &ColumnType::TimestampTz,
        },
        ColumnType::Date => CursorValue::Date,
        _ if request.lookback.is_some() => return None,
        _ => CursorValue::Plain,
    };
    let scope: Vec<String> = request
        .scope
        .iter()
        .map(|(name, column_type, text)| {
            format!(
                "{} = CAST({} AS {})",
                adapter.render_identifier(name),
                adapter.render_loader_value_literal(text),
                adapter.render_loader_logical_type(column_type).sql
            )
        })
        .collect();
    let filter = match scope.is_empty() {
        true => String::new(),
        false => format!(" WHERE {}", scope.join(" AND ")),
    };
    Some(CursorRead {
        sql: format!(
            "SELECT {} FROM {target}{filter}",
            adapter.render_cursor_text(&column, value)
        ),
        value,
    })
}

/// `SELECT DISTINCT file || '|' || version`; without a version column, just the file names.
pub(crate) fn loaded_files_query<A: SqlAdapter + ?Sized>(
    adapter: &A,
    table: &TableName,
    file_column: &str,
    version_column: Option<&str>,
) -> String {
    let file = adapter.render_identifier(file_column);
    let expression = match version_column {
        Some(version) => format!(
            "{file} || '|' || COALESCE({}, '')",
            adapter.render_identifier(version)
        ),
        None => file,
    };
    format!(
        "SELECT DISTINCT {expression} FROM {}",
        adapter.render_qualified_name(table)
    )
}

/// The query listing the tables in `parent`'s schema named as its child tables are, `<parent>__<field>` (the staging tables among them too), read from `columns`, the warehouse's `information_schema.columns` that lists that schema: each name follows `1` when the table has a parent-id column under either naming, as a child table does, and `0` when not.
pub(crate) fn child_tables_query<A: SqlAdapter + ?Sized>(
    adapter: &A,
    (parent, columns): (&TableName, &str),
) -> String {
    let schema = adapter.render_loader_value_literal(&adapter.stored_table_name(&parent.schema));
    let prefix = adapter.stored_table_name(&format!("{}{CHILD_TABLE_SEPARATOR}", parent.table));
    let parent_ids = [Compat::Sqlload, Compat::Dlt]
        .map(|compat| {
            let name = metadata_name(compat, MetadataRole::ParentId);
            adapter.render_loader_value_literal(&adapter.stored_name(name))
        })
        .join(", ");
    format!(
        "SELECT max(CASE WHEN column_name IN ({parent_ids}) THEN '1' ELSE '0' END) || table_name FROM {columns} WHERE table_schema = {schema} AND left(table_name, {}) = {} GROUP BY table_name",
        prefix.chars().count(),
        adapter.render_loader_value_literal(&prefix)
    )
}

/// The query listing the candidate staging tables in `table`'s schema, chosen by the adapter's staging filter.
pub(crate) fn staging_tables_query<A: SqlAdapter + ?Sized>(
    adapter: &A,
    table: &TableName,
    database: Option<&str>,
) -> String {
    let catalog = table
        .database
        .as_deref()
        .or(database)
        .map(|name| format!("{}.", adapter.render_identifier(name)))
        .unwrap_or_default();
    let schema = adapter.render_loader_value_literal(&adapter.stored_name(&table.schema));
    let chosen = adapter.render_staging_filter(&format!("{STAGING_SUFFIX}_"));
    format!(
        "SELECT table_name FROM {catalog}information_schema.tables WHERE table_schema = {schema} AND {chosen}"
    )
}

/// `DROP` statements for the tables among `found`, in `schema_of`'s schema, that the adapter judges stale staging tables.
pub(crate) fn stale_staging<A: SqlAdapter + ?Sized>(
    adapter: &A,
    schema_of: &TableName,
    found: &[String],
    before: NaiveDateTime,
) -> Vec<String> {
    found
        .iter()
        .filter(|name| adapter.is_stale_staging(name, before))
        .map(|name| {
            let table = TableName {
                table: name.clone(),
                ..schema_of.clone()
            };
            format!(
                "DROP TABLE IF EXISTS {}",
                adapter.render_qualified_name(&table)
            )
        })
        .collect()
}

/// The time in a staging table's name.
pub(crate) fn staging_time(name: &str) -> Option<NaiveDateTime> {
    let lower = name.to_ascii_lowercase();
    let (_, after) = lower.rsplit_once(&format!("{STAGING_SUFFIX}_"))?;
    let digits = after.get(..STAGING_TIME_DIGITS)?;
    match NaiveDateTime::parse_from_str(digits, STAGING_TIME_FORMAT) {
        Ok(time) => Some(time),
        Err(_) => None,
    }
}

/// `INSERT` statements adding `rows` to staging table `table`, each at most 16,384 rows and about 900 KiB of SQL; the values are cast to the columns' types, because `VALUES` infers its own. A row too long for a statement still gets one of its own, which the warehouse refuses with its own message.
pub(crate) fn insert_rows<A: SqlAdapter + ?Sized>(
    adapter: &A,
    table: &TableName,
    columns: &[DesiredColumn],
    rows: &[Vec<Option<String>>],
) -> Vec<String> {
    let names: Vec<String> = columns
        .iter()
        .map(|column| adapter.render_identifier(&column.name))
        .collect();
    let casts: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            format!(
                "CAST(column{} AS {})",
                index + 1,
                adapter.render_staging_type(&column.column_type)
            )
        })
        .collect();
    let opening = format!(
        "INSERT INTO {} ({}) SELECT {} FROM VALUES ",
        adapter.render_qualified_name(table),
        names.join(", "),
        casts.join(", ")
    );
    let mut statements: Vec<String> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut bytes = opening.len();
    for row in rows {
        let values: Vec<String> = row
            .iter()
            .map(|value| match value {
                Some(text) => adapter.render_loader_value_literal(text),
                None => "NULL".to_owned(),
            })
            .collect();
        let tuple = format!("({})", values.join(", "));
        let over = current.len() == MAX_INSERT_ROWS || bytes + tuple.len() > MAX_INSERT_SQL_BYTES;
        if over && !current.is_empty() {
            statements.push(format!("{opening}{}", current.join(", ")));
            current.clear();
            bytes = opening.len();
        }
        bytes += tuple.len() + 2;
        current.push(tuple);
    }
    if !current.is_empty() {
        statements.push(format!("{opening}{}", current.join(", ")));
    }
    statements
}

/// The query `check` runs on `relation`: counts and example values as text, in SQL every warehouse runs.
pub(crate) fn check_query<A: SqlAdapter + ?Sized>(
    adapter: &A,
    relation: &TableName,
    check: CheckQuery<'_>,
) -> String {
    let from = adapter.render_qualified_name(relation);
    let count = adapter.render_text_cast("COUNT(*)");
    let names_of = |columns: &[String]| -> Vec<String> {
        columns
            .iter()
            .map(|column| adapter.render_identifier(column))
            .collect()
    };
    let grouped = |columns: &[String], nulls: bool| {
        let names = names_of(columns);
        let present: Vec<String> = names
            .iter()
            .map(|name| format!("{name} IS NOT NULL"))
            .collect();
        let filter = match nulls {
            true => String::new(),
            false => format!(" WHERE {}", present.join(" AND ")),
        };
        (
            names.clone(),
            format!(
                "FROM {from}{filter} GROUP BY {} HAVING COUNT(*) > 1",
                names.join(", ")
            ),
        )
    };
    let examples = |names: &[String], nulls: bool| -> String {
        let texts: Vec<String> = names
            .iter()
            .map(|name| match nulls {
                true => format!("COALESCE({}, 'NULL')", adapter.render_text_cast(name)),
                false => adapter.render_text_cast(name),
            })
            .collect();
        texts.join(" || ', ' || ")
    };
    match check {
        CheckQuery::Counts(columns) => {
            let counts: Vec<String> = std::iter::once(count.clone())
                .chain(columns.iter().map(|column| {
                    adapter.render_text_cast(&format!(
                        "COUNT(*) - COUNT({})",
                        adapter.render_identifier(column)
                    ))
                }))
                .collect();
            format!("SELECT {} FROM {from}", counts.join(" || ',' || "))
        }
        CheckQuery::Duplicates(columns) | CheckQuery::KeyDuplicates(columns) => format!(
            "SELECT {count} FROM (SELECT 1 AS repeated {}) repeated_keys",
            grouped(columns, matches!(check, CheckQuery::KeyDuplicates(_))).1
        ),
        CheckQuery::DuplicateExamples(columns) | CheckQuery::KeyDuplicateExamples(columns) => {
            let nulls = matches!(check, CheckQuery::KeyDuplicateExamples(_));
            let (names, rest) = grouped(columns, nulls);
            format!(
                "SELECT {} AS example {rest} ORDER BY 1 LIMIT 5",
                examples(&names, nulls)
            )
        }
        CheckQuery::KeyValues(columns, limit) => format!(
            "SELECT DISTINCT {} AS example FROM {from} ORDER BY 1 LIMIT {limit}",
            examples(&names_of(columns), true)
        ),
        CheckQuery::KeyFiles {
            key,
            file_column,
            held_by,
        } => {
            let values = key_text(adapter, key);
            let column = adapter.render_identifier(file_column);
            let length = adapter.render_text_cast(&adapter.render_text_length(&column));
            let held = held_by.map_or_else(String::new, |held_by| {
                format!(
                    " WHERE {values} IN (SELECT {values} FROM {})",
                    adapter.render_qualified_name(held_by)
                )
            });
            format!(
                "SELECT DISTINCT {length} || ':' || {column} || {values} AS example FROM {from}{held} ORDER BY 1"
            )
        }
        CheckQuery::NullKeyFiles(columns, file_column) => {
            let missing: Vec<String> = names_of(columns)
                .iter()
                .map(|name| format!("{name} IS NULL"))
                .collect();
            format!(
                "SELECT DISTINCT {} AS example FROM {from} WHERE {} ORDER BY 1",
                adapter.render_identifier(file_column),
                missing.join(" OR ")
            )
        }
        CheckQuery::NullKeyExamples(columns) => {
            let names = names_of(columns);
            let missing: Vec<String> = names.iter().map(|name| format!("{name} IS NULL")).collect();
            format!(
                "SELECT {} AS example FROM {from} WHERE {} ORDER BY 1 LIMIT 5",
                examples(&names, true),
                missing.join(" OR ")
            )
        }
    }
}

/// `key`'s columns of a row as one text, each `NULL` for a NULL, joined by `, `.
fn key_text<A: SqlAdapter + ?Sized>(adapter: &A, key: &[String]) -> String {
    let texts: Vec<String> = key
        .iter()
        .map(|column| {
            let name = adapter.render_identifier(column);
            format!("COALESCE({}, 'NULL')", adapter.render_text_cast(&name))
        })
        .collect();
    texts.join(" || ', ' || ")
}

/// `DELETE FROM staging` of each row whose `key` values (converted to the table's types as the commit's replace step matches them, a NULL matching a NULL) a row of a later one of `files` (its `file_column`, in order) holds too: the rows ranked by a window over the key, which the warehouse can sort or hash, then found by file and `row_column`, which numbers each file's rows.
pub(crate) fn superseded_delete<A: SqlAdapter + ?Sized>(
    adapter: &A,
    staging: &TableName,
    (key, file_column, row_column): (&[&DesiredColumn], &str, &str),
    files: &[&str],
) -> String {
    let column = adapter.render_identifier(file_column);
    let row = adapter.render_identifier(row_column);
    let listed: Vec<String> = files
        .iter()
        .enumerate()
        .map(|(at, file)| format!("({}, {at})", adapter.render_loader_value_literal(file)))
        .collect();
    let staged = adapter.render_qualified_name(staging);
    let names: Vec<String> = key
        .iter()
        .map(|column| adapter.render_source_expression_cast(&column.name, &column.column_type))
        .collect();
    format!(
        "DELETE FROM {staged} AS __sqlload_earlier WHERE EXISTS (SELECT 1 FROM (SELECT {column} AS __sqlload_of, {row} AS __sqlload_at, __sqlload_order, MAX(__sqlload_order) OVER (PARTITION BY {}) AS __sqlload_newest FROM {staged} JOIN (VALUES {}) AS __sqlload_files (__sqlload_file, __sqlload_order) ON __sqlload_file = {column}) AS __sqlload_ranked WHERE __sqlload_of = __sqlload_earlier.{column} AND __sqlload_at = __sqlload_earlier.{row} AND __sqlload_order < __sqlload_newest)",
        names.join(", "),
        listed.join(", ")
    )
}
