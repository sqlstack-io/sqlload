//! A created table's layout: its engine, keys and settings, with SQLBuild's ClickHouse field names.

use crate::adapters::clickhouse::constants::{
    DEFAULT_ENGINE, EMPTY_SORTING_KEY, NULLABLE_KEY_SETTING,
};
use crate::adapters::clickhouse::models::ClickHouseSql;
use crate::adapters::contract::types::SqlAdapter;
use crate::loader::models::TableLayout;
use crate::project::models::LayoutSetting;

/// `(a, b)` from key expressions as written; `None` for none.
fn key_tuple(expressions: Option<&[String]>) -> Option<String> {
    let written: Vec<&str> = expressions
        .unwrap_or_default()
        .iter()
        .map(|expression| expression.trim())
        .filter(|expression| !expression.is_empty())
        .collect();
    (!written.is_empty()).then(|| format!("({})", written.join(", ")))
}

/// A setting's value as ClickHouse reads it: booleans as `1`/`0`, text quoted.
fn setting_value(value: &LayoutSetting) -> String {
    match value {
        LayoutSetting::Bool(flag) => u8::from(*flag).to_string(),
        LayoutSetting::Integer(number) => number.to_string(),
        LayoutSetting::Text(text) => ClickHouseSql.render_loader_value_literal(text),
    }
}

/// The `ENGINE`, `ORDER BY`, `PARTITION BY`, `PRIMARY KEY` and `SETTINGS` clauses of a table laid out as `layout`: `MergeTree` by default, sorted by `order_by`, else the `unique_key` columns, else unsorted. Every loaded column is `Nullable`, so a table with a sorting, partitioning or primary key gets `allow_nullable_key = 1` unless its settings set it.
pub(crate) fn layout_clauses(layout: &TableLayout, unique_key: &[String]) -> String {
    let unique: Vec<String> = unique_key
        .iter()
        .map(|key| ClickHouseSql.render_identifier(key))
        .collect();
    let order_by = key_tuple(layout.order_by.as_deref()).or_else(|| key_tuple(Some(&unique)));
    let partition_by = layout
        .partition_by
        .as_deref()
        .map(str::trim)
        .filter(|expression| !expression.is_empty());
    let primary_key = key_tuple(layout.primary_key.as_deref());
    let mut clauses = vec![
        format!(
            "ENGINE = {}",
            layout
                .engine
                .as_deref()
                .map(str::trim)
                .filter(|engine| !engine.is_empty())
                .unwrap_or(DEFAULT_ENGINE)
        ),
        format!(
            "ORDER BY {}",
            order_by.as_deref().unwrap_or(EMPTY_SORTING_KEY)
        ),
    ];
    clauses.extend(partition_by.map(|expression| format!("PARTITION BY {expression}")));
    clauses.extend(primary_key.as_ref().map(|key| format!("PRIMARY KEY {key}")));
    let keyed = order_by.is_some() || partition_by.is_some() || primary_key.is_some();
    let mut settings: Vec<String> = layout
        .table_settings
        .iter()
        .map(|(name, value)| format!("{name} = {}", setting_value(value)))
        .collect();
    if keyed && !layout.table_settings.contains_key(NULLABLE_KEY_SETTING) {
        settings.insert(0, format!("{NULLABLE_KEY_SETTING} = 1"));
    }
    if !settings.is_empty() {
        clauses.push(format!("SETTINGS {}", settings.join(", ")));
    }
    clauses.join(" ")
}

/// Why `layout` cannot lay out a table SQLLoad loads: an engine outside the non-replicated `MergeTree` family, whose partitions it could not swap.
pub(crate) fn layout_problems(layout: &TableLayout) -> Vec<String> {
    let Some(engine) = layout.engine.as_deref() else {
        return Vec::new();
    };
    let name = engine.split('(').next().unwrap_or_default().trim();
    match name.ends_with(DEFAULT_ENGINE) && !name.starts_with("Replicated") {
        true => Vec::new(),
        false => vec![format!(
            "engine {engine:?}: SQLLoad loads ClickHouse tables of the MergeTree family on one server (such as MergeTree or ReplacingMergeTree), as it swaps their partitions; replicated and distributed tables are not supported yet"
        )],
    }
}
