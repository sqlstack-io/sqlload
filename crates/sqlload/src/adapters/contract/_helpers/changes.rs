use crate::adapters::contract::models::{
    ChangeApply, ChangeWrite, TableName, WriteStatementsRequest,
};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::schema_plan::constants::{
    CDC_COMMITTED_AT_COLUMN, CDC_DELETED_AT_COLUMN, CDC_LSN_COLUMN, CDC_OP_COLUMN, CDC_PREFIX,
    CDC_UNCHANGED_COLUMN,
};

/// The `_sqlload_op` of a delete.
const DELETE_OP: &str = "delete";
/// The `_sqlload_op` a truncate leaves on soft-deleted rows.
const TRUNCATE_OP: &str = "truncate";

/// Statements applying a batch of captured changes from `staging` in one transaction, each guarded by `_sqlload_lsn` so a replay changes nothing: a changelog inserts the rows newer than the table's newest; `merge` applies the batch's truncate to older rows (soft deletes keep their `_sqlload_lsn`, so a row the same transaction inserts again still merges), then merges each key's newest change where it is newer than the row's, keeping the live value of every column `_sqlload_unchanged` (`,a,b,`) names.
pub(crate) fn change_statements<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    change: &ChangeWrite,
    staging: &TableName,
) -> Vec<String> {
    let target = adapter.render_qualified_name(request.table);
    let source = adapter.render_qualified_name(staging);
    let id = |name: &str| adapter.render_identifier(name);
    let written: Vec<(String, String, &str)> = request
        .columns
        .iter()
        .filter(|column| {
            column.name != CDC_UNCHANGED_COLUMN || change.apply == ChangeApply::Changelog
        })
        .map(|column| {
            (
                id(&column.name),
                adapter.render_source_expression_cast(&column.name, &column.column_type),
                column.name.as_str(),
            )
        })
        .collect();
    let names: Vec<&str> = written.iter().map(|(name, ..)| name.as_str()).collect();
    let lsn = id(CDC_LSN_COLUMN);
    let mut statements = vec!["BEGIN".to_owned()];
    if change.apply == ChangeApply::Changelog {
        let values: Vec<&str> = written
            .iter()
            .map(|(_, value, ..)| value.as_str())
            .collect();
        statements.push(format!(
            "INSERT INTO {target} ({}) SELECT {} FROM {source} WHERE {} > (SELECT COALESCE(MAX({lsn}), -1) FROM {target})",
            names.join(", "),
            values.join(", "),
            adapter.render_source_expression_cast(CDC_LSN_COLUMN, &ColumnType::Int),
        ));
        statements.push("COMMIT".to_owned());
        return statements;
    }
    let text = |value: &str| adapter.render_loader_value_literal(value);
    if let Some(truncation) = &change.truncated {
        let at = format!(
            "CAST({} AS {})",
            text(&truncation.committed_at),
            adapter
                .render_loader_logical_type(&ColumnType::TimestampTz)
                .sql
        );
        statements.push(match change.apply {
            ChangeApply::HardDeletes => {
                format!("DELETE FROM {target} WHERE {lsn} < {}", truncation.lsn)
            }
            _ => format!(
                "UPDATE {target} SET {} = {at}, {} = {} WHERE {lsn} < {} AND {} IS NULL",
                id(CDC_DELETED_AT_COLUMN),
                id(CDC_OP_COLUMN),
                text(TRUNCATE_OP),
                truncation.lsn,
                id(CDC_DELETED_AT_COLUMN),
            ),
        });
    }
    let keys: Vec<String> = change.unique_key.iter().map(|key| id(key)).collect();
    let unchanged = id(CDC_UNCHANGED_COLUMN);
    let selected: Vec<String> = written
        .iter()
        .map(|(name, value, ..)| format!("{value} AS {name}"))
        .chain([format!(
            "{} AS {unchanged}",
            adapter.render_source_expression_cast(CDC_UNCHANGED_COLUMN, &ColumnType::Text)
        )])
        .collect();
    let join: Vec<String> = keys
        .iter()
        .map(|key| format!("__target.{key} = __source.{key}"))
        .collect();
    let newer = format!("__source.{lsn} > COALESCE(__target.{lsn}, -1)");
    let is_delete = format!("__source.{} = {}", id(CDC_OP_COLUMN), text(DELETE_OP));
    let metadata: Vec<String> = written
        .iter()
        .filter(|(name, _, raw)| raw.starts_with(CDC_PREFIX) && *name != id(CDC_DELETED_AT_COLUMN))
        .map(|(name, ..)| format!("{name} = __source.{name}"))
        .collect();
    let mut updates: Vec<String> = Vec::new();
    for (name, _, raw) in &written {
        if raw.starts_with(CDC_PREFIX) || keys.contains(name) {
            continue;
        }
        let listed = text(&format!(",{raw},"));
        updates.push(format!(
            "{name} = CASE WHEN POSITION({listed} IN __source.{unchanged}) > 0 THEN __target.{name} ELSE __source.{name} END"
        ));
    }
    let soft = change.apply == ChangeApply::SoftDeletes;
    if soft {
        updates.push(format!("{} = NULL", id(CDC_DELETED_AT_COLUMN)));
    }
    updates.extend(metadata.iter().cloned());
    let deleted = match soft {
        true => {
            let mut set = vec![format!(
                "{} = __source.{}",
                id(CDC_DELETED_AT_COLUMN),
                id(CDC_COMMITTED_AT_COLUMN)
            )];
            set.extend(metadata.iter().cloned());
            format!("UPDATE SET {}", set.join(", "))
        }
        false => "DELETE".to_owned(),
    };
    let inserted: Vec<String> = names
        .iter()
        .map(|name| format!("__source.{name}"))
        .collect();
    statements.push(format!(
        "MERGE INTO {target} AS __target USING (SELECT {} FROM {source}) AS __source ON {} WHEN MATCHED AND {newer} AND {is_delete} THEN {deleted} WHEN MATCHED AND {newer} THEN UPDATE SET {} WHEN NOT MATCHED AND NOT {is_delete} THEN INSERT ({}) VALUES ({})",
        selected.join(", "),
        join.join(" AND "),
        updates.join(", "),
        names.join(", "),
        inserted.join(", "),
    ));
    statements.push("COMMIT".to_owned());
    statements
}
