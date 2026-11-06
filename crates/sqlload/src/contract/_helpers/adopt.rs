//! The declarations that adopt a live table, as `sqb contract generate` adopts one.

use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::ColumnType;
use crate::contract::_helpers::compare::{agrees, is_live, is_own};
use crate::contract::models::{AdoptedColumn, Adoption};
use crate::loader::models::TableSpec;
use crate::schema_plan::models::LiveColumn;

/// The changes that make `spec`'s declared `columns` match `live`, or `None` when there are none: every undeclared live column with its live type, and with `overwrite` declared columns of another type retyped and ones the table lacks removed. A database or files table declares only columns its source has, spelled as the source spells them, so the project still validates; a Python table declares every column but flattened ones. Masked columns stay under `mask`, and SQLLoad's own columns are never declared.
pub(crate) fn adoption(spec: &TableSpec, live: &[LiveColumn], overwrite: bool) -> Option<Adoption> {
    let masked = |name: &str| {
        spec.masked
            .columns
            .keys()
            .any(|column| column.eq_ignore_ascii_case(name))
    };
    let declared: Vec<(&String, &ColumnType)> = spec
        .pinned
        .iter()
        .filter(|(name, _)| !masked(name))
        .collect();
    let mut columns: Vec<AdoptedColumn> = Vec::new();
    let mut paste: Vec<(String, String)> = Vec::new();
    for column in live
        .iter()
        .filter(|column| !is_own(&column.name) && !masked(&column.name))
    {
        let physical = column.column_type.to_string();
        let found = declared
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(&column.name));
        let (name, adopted) = match found {
            Some((name, declared_type))
                if !overwrite || agrees(spec.warehouse, declared_type, &column.column_type) =>
            {
                paste.push(((*name).clone(), declared_type.to_string()));
                continue;
            }
            Some((name, declared_type)) => ((*name).clone(), Some(declared_type.to_string())),
            None => match declarable(spec, &column.name) {
                Some(name) => (name, None),
                None => continue,
            },
        };
        paste.push((name.clone(), physical.clone()));
        columns.push(AdoptedColumn {
            column: name,
            declared_type: adopted,
            physical_type: physical,
        });
    }
    let mut removed: Vec<String> = Vec::new();
    for (name, declared_type) in declared.iter().filter(|(name, _)| !is_live(live, name)) {
        match overwrite {
            true => removed.push((*name).clone()),
            false => paste.push(((*name).clone(), declared_type.to_string())),
        }
    }
    if columns.is_empty() && removed.is_empty() {
        return None;
    }
    let source_key = spec.source_table.qualified_name();
    let location = match &spec.python {
        Some(python) => python.module.clone(),
        None => format!(
            "[sources.{}.tables.{}]",
            toml_edit::Key::new(spec.source_name.as_str()).display_repr(),
            toml_edit::Key::new(source_key.as_str()).display_repr()
        ),
    };
    Some(Adoption {
        location,
        columns,
        removed,
        paste: spec.python.as_ref().map(|_| python_columns(&paste)),
        toml_key: (spec.source_name.clone(), source_key),
        description: (spec.files.is_none() && spec.python.is_none())
            .then(|| spec.description.clone()),
    })
}

/// The name to declare live column `live_name` under, or `None` when it cannot be declared: the source's own spelling for database and files tables (`None` when the source lacks it), and for Python tables the name the function most likely yields (Snowflake stores `id` as `ID`), unless a flattened object produced it.
fn declarable(spec: &TableSpec, live_name: &str) -> Option<String> {
    if spec.python.is_none() {
        return spec
            .source_table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(live_name))
            .map(|column| column.name.clone());
    }
    let lowered = live_name.to_lowercase();
    let name = match sql_adapter(spec.warehouse).stored_name(&lowered) == live_name {
        true => lowered,
        false => live_name.to_owned(),
    };
    let flattened = spec.flatten.iter().any(|object| {
        name.to_ascii_lowercase()
            .starts_with(&format!("{}__", object.to_ascii_lowercase()))
    });
    (!flattened).then_some(name)
}

/// A Python table's `columns={...}` argument.
fn python_columns(columns: &[(String, String)]) -> String {
    let quoted = |text: &str| serde_json::to_string(text).unwrap_or_default();
    let entries: Vec<String> = columns
        .iter()
        .map(|(name, column_type)| format!("{}: {}", quoted(name), quoted(column_type)))
        .collect();
    format!("columns={{{}}}", entries.join(", "))
}
