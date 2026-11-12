//! Databricks' writes: each one statement, since the Statement Execution API runs every statement in its own transaction.

use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::constants::{TIMESTAMP_NTZ, TIMESTAMP_NTZ_FEATURE, TYPE_WIDENING};
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnChange, LiveColumn};

/// The statements applying `request.mode` from `staging`, each atomic on its own: a `delete_insert` is one `MERGE` that inserts every staged row and deletes the table's rows in the range, so it keeps `DELETE` then `INSERT`'s meaning (rows with a NULL cursor included) in one commit.
pub(crate) fn write_statements<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    staging: &TableName,
) -> Vec<String> {
    if let Some(indexes) = request.built {
        return adapter.render_swap(request.table, staging, indexes);
    }
    let target = adapter.render_qualified_name(request.table);
    let source = adapter.render_qualified_name(staging);
    let names: Vec<String> = request
        .columns
        .iter()
        .map(|column| adapter.render_identifier(&column.name))
        .collect();
    let values: Vec<String> = request
        .columns
        .iter()
        .map(|column| {
            let value = adapter.render_source_expression_cast(&column.name, &column.column_type);
            match request.hashed.contains(&column.name) {
                true => format!("sha2({value}, 256)"),
                false => value,
            }
        })
        .collect();
    let insert_from = format!(
        "({}) SELECT {} FROM {source}",
        names.join(", "),
        values.join(", ")
    );
    match request.mode {
        WriteMode::Recreate => {
            adapter.render_replace_table_from_relation(request, &source, &values)
        }
        WriteMode::Merge { unique_key, .. } => {
            vec![adapter.render_merge(request, unique_key, &source, &values)]
        }
        WriteMode::Replace => vec![format!("INSERT OVERWRITE {target} {insert_from}")],
        WriteMode::Append => vec![format!("INSERT INTO {target} {insert_from}")],
        WriteMode::Changes(change) => adapter
            .render_changes(request, change, staging)
            .into_iter()
            .filter(|statement| !matches!(statement.as_str(), "BEGIN" | "COMMIT"))
            .collect(),
        WriteMode::DeleteInsert { .. } => {
            let condition = range_condition(adapter, request);
            let selected: Vec<String> = values
                .iter()
                .zip(&names)
                .map(|(value, name)| format!("{value} AS {name}"))
                .collect();
            let inserted: Vec<String> = names
                .iter()
                .map(|name| format!("__source.{name}"))
                .collect();
            let deleted = match condition {
                Some(condition) => {
                    format!("WHEN NOT MATCHED BY SOURCE AND {condition} THEN DELETE")
                }
                None => "WHEN NOT MATCHED BY SOURCE THEN DELETE".to_owned(),
            };
            vec![format!(
                "MERGE INTO {target} AS __target USING (SELECT {} FROM {source}) AS __source ON FALSE WHEN NOT MATCHED THEN INSERT ({}) VALUES ({}) {deleted}",
                selected.join(", "),
                names.join(", "),
                inserted.join(", ")
            )]
        }
    }
}

/// The table rows a `delete_insert` replaces, on `__target`; `None` for every row.
fn range_condition<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
) -> Option<String> {
    let WriteMode::DeleteInsert {
        cursor_column,
        from,
        to,
        values,
    } = request.mode
    else {
        return None;
    };
    let cursor_type = request
        .columns
        .iter()
        .find(|column| column.name == *cursor_column)
        .map_or(ColumnType::Text, |column| column.column_type.clone());
    let sql_type = adapter.render_loader_logical_type(&cursor_type).sql;
    let cast = |text: &str| {
        format!(
            "CAST({} AS {sql_type})",
            adapter.render_loader_value_literal(text)
        )
    };
    let column = format!("__target.{}", adapter.render_identifier(cursor_column));
    if !values.is_empty() {
        let listed: Vec<String> = values.iter().map(|value| cast(value)).collect();
        return Some(format!("{column} IN ({})", listed.join(", ")));
    }
    let bounds: Vec<String> = [(from, ">="), (to, "<")]
        .into_iter()
        .filter_map(|(bound, operator)| {
            let bound = bound.as_ref()?;
            Some(format!("{column} {operator} {}", cast(bound)))
        })
        .collect();
    (!bounds.is_empty()).then(|| bounds.join(" AND "))
}

/// The statements applying `changes` to `table`: one rewrite for the columns that need it, the added columns in one `ADD COLUMNS`, then in-place widening, with the table features they need turned on first.
pub(crate) fn change_statements<A: SqlAdapter + ?Sized>(
    adapter: &A,
    table: &TableName,
    changes: &[ColumnChange],
    live: &[LiveColumn],
) -> Vec<String> {
    let target = adapter.render_qualified_name(table);
    let mut added: Vec<String> = Vec::new();
    let mut widened: Vec<String> = Vec::new();
    let mut rebuilt: Vec<(&str, &str)> = Vec::new();
    for change in changes {
        match change {
            ColumnChange::Add { column, sql_type } => {
                added.push(format!(
                    "{} {sql_type}",
                    adapter.render_identifier(&column.name)
                ));
            }
            ColumnChange::Widen {
                column, sql_type, ..
            } => widened.push(adapter.render_alter_column_type(
                &target,
                &adapter.render_exact_identifier(column),
                sql_type,
            )),
            ColumnChange::Rebuild {
                column, sql_type, ..
            } => rebuilt.push((column, sql_type)),
        }
    }
    let mut statements: Vec<String> = Vec::new();
    if !rebuilt.is_empty() {
        let select: Vec<String> = live
            .iter()
            .map(|column| rebuilt_column(adapter, column, &rebuilt))
            .collect();
        statements.push(adapter.render_rebuild_table(&target, &select.join(", ")));
    }
    let mut features: Vec<&str> = Vec::new();
    if added.iter().any(|column| column.ends_with(TIMESTAMP_NTZ)) {
        features.push(TIMESTAMP_NTZ_FEATURE);
    }
    if !widened.is_empty() {
        features.push(TYPE_WIDENING);
    }
    if !features.is_empty() {
        statements.push(format!(
            "ALTER TABLE {target} SET TBLPROPERTIES ({})",
            features.join(", ")
        ));
    }
    if !added.is_empty() {
        statements.push(format!(
            "ALTER TABLE {target} ADD COLUMNS ({})",
            added.join(", ")
        ));
    }
    statements.extend(widened);
    statements
}

/// A select-list entry keeping `column`, cast when it is one of `rebuilt`.
fn rebuilt_column<A: SqlAdapter + ?Sized>(
    adapter: &A,
    column: &LiveColumn,
    rebuilt: &[(&str, &str)],
) -> String {
    let name = adapter.render_exact_identifier(&column.name);
    match rebuilt.iter().find(|(rebuilt, _)| *rebuilt == column.name) {
        Some((_, sql_type)) => format!("CAST({name} AS {sql_type}) AS {name}"),
        None => name,
    }
}
