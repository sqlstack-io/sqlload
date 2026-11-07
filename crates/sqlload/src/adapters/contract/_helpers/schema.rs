//! Creating and changing tables: the shared rendering.

use crate::adapters::contract::_helpers::identifiers::with_suffix;
use crate::adapters::contract::_helpers::rebuild::rebuild_statement;
use crate::adapters::contract::constants::STAGING_SUFFIX;
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnChange, DesiredColumn, LiveColumn};

/// `CREATE SCHEMA IF NOT EXISTS` for `table`'s schema.
pub(crate) fn create_schema<A: SqlAdapter + ?Sized>(adapter: &A, table: &TableName) -> String {
    let schema = adapter.render_identifier(&table.schema);
    match &table.database {
        Some(database) => format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{schema}",
            adapter.render_identifier(database)
        ),
        None => format!("CREATE SCHEMA IF NOT EXISTS {schema}"),
    }
}

/// `CREATE TABLE IF NOT EXISTS` with every column nullable.
pub(crate) fn create_table<A: SqlAdapter + ?Sized>(
    adapter: &A,
    table: &TableName,
    columns: &[DesiredColumn],
) -> String {
    let definitions: Vec<String> = columns
        .iter()
        .map(|column| {
            format!(
                "{} {}",
                adapter.render_identifier(&column.name),
                adapter.render_loader_logical_type(&column.column_type).sql
            )
        })
        .collect();
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        adapter.render_qualified_name(table),
        definitions.join(", ")
    )
}

/// `table` with the staging suffix and `staging_id`, kept to letters, digits and underscores.
pub(crate) fn staging_name(table: &TableName, staging_id: &str) -> TableName {
    let safe_id: String = staging_id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
        .collect();
    with_suffix(table, &format!("{STAGING_SUFFIX}_{safe_id}"))
}

/// The staging table for `table` under `staging_id`, and the statement creating it.
pub(crate) fn staging_table<A: SqlAdapter + ?Sized>(
    adapter: &A,
    table: &TableName,
    columns: &[DesiredColumn],
    staging_id: &str,
) -> (TableName, String) {
    let staging = adapter.staging_relation_name(table, staging_id);
    let definitions: Vec<String> = columns
        .iter()
        .map(|column| {
            format!(
                "{} {}",
                adapter.render_identifier(&column.name),
                adapter.render_staging_type(&column.column_type)
            )
        })
        .collect();
    let create = format!(
        "CREATE OR REPLACE {} {} ({})",
        adapter.staging_table_kind(),
        adapter.render_qualified_name(&staging),
        definitions.join(", ")
    );
    (staging, create)
}

/// `build` from the table's `definition` when there is one, else from `columns`.
pub(crate) fn build_table<A: SqlAdapter + ?Sized>(
    adapter: &A,
    build: &TableName,
    _table: &TableName,
    (columns, _differing, definition): (&[DesiredColumn], Option<&[LiveColumn]>, Option<&str>),
) -> Vec<String> {
    let name = adapter.render_qualified_name(build);
    if let Some(definition) = definition {
        return vec![format!("CREATE OR REPLACE TABLE {name}{definition}")];
    }
    let definitions: Vec<String> = columns
        .iter()
        .map(|column| {
            format!(
                "{} {}",
                adapter.render_identifier(&column.name),
                adapter.render_loader_logical_type(&column.column_type).sql
            )
        })
        .collect();
    vec![format!(
        "CREATE OR REPLACE TABLE {name} ({})",
        definitions.join(", ")
    )]
}

/// The statements applying `changes` to `table`, whose current columns are `live`: added columns, in-place widening, and one rewrite for the columns that need it.
pub(crate) fn alter_statements<A: SqlAdapter + ?Sized>(
    adapter: &A,
    table: &TableName,
    changes: &[ColumnChange],
    live: &[LiveColumn],
) -> Vec<String> {
    let target = adapter.render_qualified_name(table);
    let mut statements: Vec<String> = Vec::new();
    let mut rebuilt: Vec<(&str, &str)> = Vec::new();
    for change in changes {
        match change {
            ColumnChange::Add { column, sql_type } => statements.push(format!(
                "ALTER TABLE {target} ADD COLUMN IF NOT EXISTS {} {sql_type}",
                adapter.render_identifier(&column.name)
            )),
            ColumnChange::Widen {
                column, sql_type, ..
            } => statements.push(adapter.render_alter_column_type(
                &target,
                &adapter.render_exact_identifier(column),
                sql_type,
            )),
            ColumnChange::Rebuild {
                column, sql_type, ..
            } => rebuilt.push((column, sql_type)),
        }
    }
    if !rebuilt.is_empty() {
        statements.insert(0, rebuild_statement(adapter, &target, (live, &rebuilt)));
    }
    statements
}

/// The statements changing `staging`, made for `old` columns, to hold `new` ones: columns added, and changed types cast in place or by rewriting the staging table where the warehouse cannot widen in place.
pub(crate) fn staging_changes<A: SqlAdapter + ?Sized>(
    adapter: &A,
    staging: &TableName,
    (old, new): (&[DesiredColumn], &[DesiredColumn]),
    built: Option<&[LiveColumn]>,
) -> Vec<String> {
    let staging_type = |column_type: &ColumnType| match built {
        Some(_) => adapter.render_loader_logical_type(column_type).sql,
        None => adapter.render_staging_type(column_type),
    };
    let started: &[LiveColumn] = built.unwrap_or_default();
    let target = adapter.render_qualified_name(staging);
    let mut statements: Vec<String> = Vec::new();
    let mut rebuilt: Vec<(String, String)> = Vec::new();
    for column in new {
        let sql_type = staging_type(&column.column_type);
        let stored = adapter.stored_name(&column.name);
        let known = old
            .iter()
            .find(|known| known.name == column.name)
            .map(|known| &known.column_type)
            .or_else(|| {
                started
                    .iter()
                    .find(|known| adapter.is_stored_as(&known.name, &stored))
                    .map(|known| &known.column_type)
            });
        match known {
            None => statements.push(format!(
                "ALTER TABLE {target} ADD COLUMN {} {sql_type}",
                adapter.render_identifier(&column.name)
            )),
            Some(known) if staging_type(known) != sql_type => {
                rebuilt.push((stored, sql_type));
            }
            Some(_) => {}
        }
    }
    match (adapter.widens_staging_by_rebuild(), rebuilt.is_empty()) {
        (_, true) => {}
        (true, false) => {
            let mut live: Vec<LiveColumn> = old
                .iter()
                .map(|column| LiveColumn {
                    name: adapter.stored_name(&column.name),
                    column_type: column.column_type.clone(),
                    fraction_digits: None,
                })
                .collect();
            for column in started {
                if !live.iter().any(|known| known.name == column.name) {
                    live.push(column.clone());
                }
            }
            let casts: Vec<(&str, &str)> = rebuilt
                .iter()
                .map(|(name, sql_type)| (name.as_str(), sql_type.as_str()))
                .collect();
            statements.insert(0, rebuild_statement(adapter, &target, (&live, &casts)));
        }
        (false, false) => statements.extend(rebuilt.iter().map(|(name, sql_type)| {
            adapter.render_alter_column_type(
                &target,
                &adapter.render_exact_identifier(name),
                sql_type,
            )
        })),
    }
    statements
}
