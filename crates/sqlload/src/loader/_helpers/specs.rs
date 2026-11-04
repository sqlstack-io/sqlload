use std::collections::BTreeMap;

use crate::column_types::main::parse_type::parse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::loader::_helpers::columns::{
    ColumnRules, ColumnSelection, apply_column_rules, destination_name,
};
use crate::loader::_helpers::resolve::ResolvedTarget;
use crate::loader::_helpers::settings::{cursor, default_strategy, layered, strategy_problems};
use crate::loader::models::{SettingOrigin, TableSpec};
use crate::postgres_source::models::SourceTable;
use crate::project::models::{SourceConfig, TableOverride};
use crate::similar_names::main::nearest_name::nearest_name;
use crate::write_sql::models::TableName;

/// The source a table belongs to and where it loads.
pub(crate) struct SourceContext<'a> {
    pub(crate) name: &'a str,
    pub(crate) source: &'a SourceConfig,
    pub(crate) target: &'a ResolvedTarget,
}

/// The columns a table drops and the types it pins.
pub(crate) struct ColumnChoices {
    pub(crate) excluded: Vec<(String, String)>,
    pub(crate) pinned: BTreeMap<String, ColumnType>,
}

/// The source's and the table's column rules applied to `table`.
pub(crate) fn column_selection(
    context: &SourceContext<'_>,
    table: &SourceTable,
    overrides: &TableOverride,
) -> Result<ColumnSelection, String> {
    let source = context.source;
    let rules = ColumnRules {
        exclude: [
            source.exclude_columns.clone(),
            overrides.exclude_columns.clone(),
        ]
        .concat(),
        allow: [
            source.allow_columns.clone(),
            overrides.allow_columns.clone(),
        ]
        .concat(),
        skip_tables_with: source.skip_tables_with_columns.clone(),
    };
    let selection = apply_column_rules(table, &rules)?;
    Ok(match selection {
        ColumnSelection::Keep(excluded) => ColumnSelection::Keep(
            excluded
                .into_iter()
                .filter(|(column, _)| !overrides.mask.contains_key(column))
                .collect(),
        ),
        skip => skip,
    })
}

/// The table's pinned column types, parsed for `warehouse`.
pub(crate) fn pinned_types(
    overrides: &TableOverride,
    warehouse: Warehouse,
) -> Result<BTreeMap<String, ColumnType>, Vec<String>> {
    let mut pinned: BTreeMap<String, ColumnType> = BTreeMap::new();
    let mut problems: Vec<String> = Vec::new();
    for (column, text) in &overrides.columns {
        match parse_type(text, &[warehouse]) {
            Ok(parsed) => {
                pinned.insert(column.clone(), parsed.column_type);
            }
            Err(error) => problems.push(format!("column {column}: {error}")),
        }
    }
    for column in overrides.mask.keys() {
        pinned.insert(column.clone(), ColumnType::Text);
    }
    match problems.is_empty() {
        true => Ok(pinned),
        false => Err(problems),
    }
}

/// Problems with `columns` and `mask` keys that name no source column, so a typo never loads a column unmasked.
fn column_reference_problems(table: &SourceTable, overrides: &TableOverride) -> Vec<String> {
    let names: Vec<&str> = table
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let keys = overrides
        .columns
        .keys()
        .map(|key| ("columns", key))
        .chain(overrides.mask.keys().map(|key| ("mask", key)));
    keys.filter(|(_, key)| !names.contains(&key.as_str()))
        .map(|(setting, key)| unknown_column(setting, key, &names))
        .collect()
}

fn unknown_column(setting: &str, key: &str, names: &[&str]) -> String {
    let hint = match nearest_name(key, names.iter().copied()) {
        Some(nearest) => format!("; did you mean {nearest:?}?"),
        None => String::new(),
    };
    format!(
        "{setting} {key:?} is not a column of the source table (names are case-sensitive){hint}"
    )
}

/// The table's spec with every setting layered (table over source over default), or its problems.
pub(crate) fn build_spec(
    context: &SourceContext<'_>,
    table: &SourceTable,
    overrides: &TableOverride,
    columns: ColumnChoices,
) -> Result<TableSpec, Vec<String>> {
    let settings = &overrides.settings;
    let defaults = &context.source.defaults;
    let cursor = cursor(settings, defaults);
    let unique_key = layered(&settings.unique_key, &defaults.unique_key)
        .map_or_else(|| table.primary_key.clone(), |(key, _)| key);
    let (strategy, strategy_origin) = layered(&settings.write_strategy, &defaults.write_strategy)
        .unwrap_or_else(|| {
            (
                default_strategy(cursor.as_ref(), !unique_key.is_empty()),
                SettingOrigin::Default,
            )
        });
    let mut problems = strategy_problems(strategy, &unique_key, cursor.as_ref());
    problems.extend(column_reference_problems(table, overrides));
    problems.extend(overrides.mask.keys().filter_map(|masked| {
        let is_cursor = cursor.as_ref().is_some_and(|cursor| cursor.column == *masked);
        let is_key = unique_key.contains(masked);
        (is_cursor || is_key).then(|| {
            format!("mask {masked}: a masked column cannot be the cursor_column or part of the unique_key")
        })
    }));
    if !problems.is_empty() {
        return Err(problems);
    }
    let name = overrides
        .name
        .clone()
        .unwrap_or_else(|| destination_name(&context.source.table_name, context.name, table));
    Ok(TableSpec {
        source_name: context.name.to_owned(),
        source_table: table.clone(),
        target: context.target.name.clone(),
        destination: TableName {
            database: context.target.database.clone(),
            schema: context.target.schema.clone(),
            table: name,
        },
        warehouse: context.target.warehouse,
        strategy,
        strategy_origin,
        unique_key,
        cursor,
        pinned: columns.pinned,
        excluded_columns: columns.excluded,
        masked: overrides.mask.clone(),
        contract: layered(&settings.contract, &defaults.contract)
            .map(|(value, _)| value)
            .unwrap_or_default(),
        on_schema_change: layered(&settings.on_schema_change, &defaults.on_schema_change)
            .map(|(value, _)| value)
            .unwrap_or_default(),
        type_change_rebuild: layered(&settings.type_change_rebuild, &defaults.type_change_rebuild)
            .map(|(value, _)| value)
            .unwrap_or_default(),
        listed: false,
        new_tables: context.source.new_tables,
    })
}
