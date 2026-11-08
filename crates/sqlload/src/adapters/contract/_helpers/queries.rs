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
use crate::schema_plan::models::DesiredColumn;

/// A query for `MAX(column)` whose text never depends on session time zones or output formats; `None` if a lookback needs a time column.
pub(crate) fn cursor_query<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &CursorQuery<'_>,
) -> Option<CursorRead> {
    let column = adapter.render_identifier(request.column);
    let target = adapter.render_qualified_name(request.table);
    let value = match request.column_type {
        ColumnType::TimestampTz | ColumnType::TimestampNtz => CursorValue::EpochMicros {
            zoned: request.column_type == &ColumnType::TimestampTz,
        },
        ColumnType::Date => CursorValue::Date,
        _ if request.lookback.is_some() => return None,
        _ => CursorValue::Plain,
    };
    Some(CursorRead {
        sql: format!(
            "SELECT {} FROM {target}",
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
    let grouped = |columns: &[String]| {
        let names: Vec<String> = columns
            .iter()
            .map(|column| adapter.render_identifier(column))
            .collect();
        let present: Vec<String> = names
            .iter()
            .map(|name| format!("{name} IS NOT NULL"))
            .collect();
        (
            names.clone(),
            format!(
                "FROM {from} WHERE {} GROUP BY {} HAVING COUNT(*) > 1",
                present.join(" AND "),
                names.join(", ")
            ),
        )
    };
    match check {
        CheckQuery::Rows => format!("SELECT {count} FROM {from}"),
        CheckQuery::Duplicates(columns) => format!(
            "SELECT {count} FROM (SELECT 1 AS repeated {}) repeated_keys",
            grouped(columns).1
        ),
        CheckQuery::DuplicateExamples(columns) => {
            let (names, rest) = grouped(columns);
            let texts: Vec<String> = names
                .iter()
                .map(|name| adapter.render_text_cast(name))
                .collect();
            format!(
                "SELECT {} AS example {rest} ORDER BY 1 LIMIT 5",
                texts.join(" || ', ' || ")
            )
        }
        CheckQuery::Nulls(column) => format!(
            "SELECT {count} FROM {from} WHERE {} IS NULL",
            adapter.render_identifier(column)
        ),
    }
}
