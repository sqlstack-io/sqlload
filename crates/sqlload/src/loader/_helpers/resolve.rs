use crate::adapters::dispatch::main::dialect::connection_warehouse::connection_warehouse;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;
use crate::load_naming::main::is_load_column::is_load_column;
use crate::load_naming::main::loaded_name::loaded_name;
use crate::load_naming::models::Naming;
use crate::loader::_helpers::specs::ColumnSelection;
use crate::loader::_helpers::specs::table_name;
use crate::loader::_helpers::specs::{
    ColumnChoices, SourceContext, build_spec, column_selection, pinned_types, table_naming,
};
use crate::loader::constants::PRIMARY_KEY_PLACEHOLDER;
use crate::loader::models::{ResolvedTables, SkippedTable, TableSpec};
use crate::postgres_source::models::SourceTable;
use crate::project::main::motherduck_path::motherduck_path;
use crate::project::models::{
    ConnectionConfig, ConstantColumns, ProjectConfig, SourceConfig, SourceKind, TableOverride,
    TableSettings, UnnestSetting,
};
use crate::ssh_tunnel::models::SshTunnel;
use std::collections::BTreeMap;

/// The specs for `discovered` tables of source `source_name`, with problems and skipped tables.
pub(crate) fn resolve_tables(
    project: &ProjectConfig,
    source_name: &str,
    discovered: &[SourceTable],
) -> ResolvedTables {
    let mut resolved = ResolvedTables::default();
    let Some(source) = project.sources.get(source_name) else {
        resolved
            .problems
            .push(format!("source {source_name:?} does not exist"));
        return resolved;
    };
    let target = match source_target(project, source) {
        Ok(target) => target,
        Err(problem) => {
            resolved
                .problems
                .push(format!("[sources.{source_name}]: {problem}"));
            return resolved;
        }
    };
    for table in discovered {
        let qualified = table.qualified_name();
        let overrides = source.tables.get(&qualified).cloned().unwrap_or_default();
        let context = SourceContext {
            name: source_name,
            source,
            target: &target,
        };
        match resolve_table(&context, table, &overrides) {
            Ok(Ok(mut spec)) => {
                let query = overrides.sql.is_some();
                spec.approved =
                    !source.discovers_tables() || query || source.include.contains(&qualified);
                spec.tunnel = project
                    .connections
                    .get(&source.connection)
                    .filter(|_| {
                        matches!(source.kind, SourceKind::Postgres | SourceKind::PostgresCdc)
                    })
                    .and_then(|connection| match connection {
                        ConnectionConfig::Postgres { ssh, .. } => {
                            ssh.as_ref().map(SshTunnel::route)
                        }
                        _ => None,
                    });
                resolved.specs.push(spec);
            }
            Ok(Err(reason)) => resolved.skipped.push(SkippedTable {
                source_name: source_name.to_owned(),
                source_table: qualified,
                reason,
            }),
            Err(problems) => resolved.problems.extend(
                problems
                    .into_iter()
                    .map(|problem| format!("{qualified}: {problem}")),
            ),
        }
    }
    resolved
        .problems
        .extend(merged_names(source_name, source, &resolved.specs));
    for listed in source.tables.keys() {
        if !discovered
            .iter()
            .any(|table| table.qualified_name() == *listed)
        {
            resolved.problems.push(format!(
                "{listed}: listed under [sources.{source_name}.tables] but not found in the source (or not selected by include/exclude)"
            ));
        }
    }
    resolved
}

/// A problem for each pair of `specs` (one source's) that dlt's naming loads into one table although their names differ (`order-items` and `order_items`), where one would overwrite or mix with the other's rows.
fn merged_names(source_name: &str, source: &SourceConfig, specs: &[TableSpec]) -> Vec<String> {
    let written = |spec: &TableSpec| {
        let name = source
            .tables
            .get(&spec.source_table.qualified_name())
            .and_then(|overrides| overrides.name.as_deref());
        table_name(
            (source_name, source),
            name,
            (&spec.source_table, (Naming::Sqlload, None)),
        )
    };
    let mut problems: Vec<String> = Vec::new();
    for (index, spec) in specs.iter().enumerate() {
        let clash = specs[..index].iter().find(|earlier| {
            (spec.naming == Naming::Dlt || earlier.naming == Naming::Dlt)
                && earlier.destination.table == spec.destination.table
                && written(earlier) != written(spec)
        });
        if let Some(earlier) = clash {
            problems.push(format!(
                "{} and {}: both load as table {} under naming = \"dlt\"; give one a name of its own with name",
                earlier.source_table.qualified_name(),
                spec.source_table.qualified_name(),
                spec.destination.table
            ));
        }
    }
    problems
}

/// One table's spec, `Ok(Err(reason))` if the column rules skip it, or its problems.
fn resolve_table(
    context: &SourceContext<'_>,
    table: &SourceTable,
    overrides: &TableOverride,
) -> Result<Result<TableSpec, String>, Vec<String>> {
    let mut excluded =
        match column_selection(context, table, overrides).map_err(|problem| vec![problem])? {
            ColumnSelection::Skip(reason) => return Ok(Err(reason)),
            ColumnSelection::Keep(excluded) => excluded,
        };
    let naming = table_naming(
        (&overrides.settings, &context.source.defaults),
        context.target.naming,
    );
    if naming == Naming::Sqlload {
        let pinned = pinned_types(overrides, context.target.warehouse)?;
        return build_spec(
            context,
            table,
            overrides,
            ColumnChoices { excluded, pinned },
        )
        .map(Ok);
    }
    let rule = (
        naming,
        sql_adapter(context.target.warehouse).max_identifier_length(),
    );
    let overrides = loaded_overrides(overrides, rule);
    let source = SourceConfig {
        defaults: loaded_settings(&context.source.defaults, rule),
        ..context.source.clone()
    };
    let context = SourceContext {
        name: context.name,
        source: &source,
        target: context.target,
    };
    excluded.retain(|(column, _)| {
        !overrides
            .mask
            .contains_key(&loaded_name(naming, column, rule.1))
    });
    let loaded = loaded_table(table, &excluded, rule)?;
    let pinned = pinned_types(&overrides, context.target.warehouse)?;
    build_spec(
        &context,
        &loaded,
        &overrides,
        ColumnChoices { excluded, pinned },
    )
    .map(Ok)
}

/// Where a source's tables go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedTarget {
    pub(crate) name: String,
    pub(crate) warehouse: Warehouse,
    pub(crate) database: Option<String>,
    pub(crate) schema: String,
    /// The table folder template, for a files target.
    pub(crate) path: Option<String>,
    /// How its tables name the columns a load adds, unless a source or table says.
    pub(crate) naming: Option<Naming>,
}

/// The target `source` loads into: its own, the project default, or the only target.
pub(crate) fn source_target(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<ResolvedTarget, String> {
    let name = source
        .destination
        .clone()
        .or_else(|| project.default_target.clone())
        .or_else(|| {
            (project.targets.len() == 1)
                .then(|| project.targets.keys().next().cloned())
                .flatten()
        })
        .ok_or("the source has no destination")?;
    let target = project
        .targets
        .get(&name)
        .ok_or_else(|| format!("target {name:?} does not exist"))?;
    let connection = project
        .connections
        .get(&target.connection)
        .ok_or_else(|| format!("connection {:?} does not exist", target.connection))?;
    let Some((warehouse, database)) = connection_warehouse(connection) else {
        return Err(format!(
            "target {name:?} uses a {} connection, which is not a destination",
            connection.kind()
        ));
    };
    if !sql_adapter(warehouse).runs_sql() {
        return Ok(ResolvedTarget {
            name,
            warehouse,
            database: None,
            schema: String::new(),
            path: Some(target.path_template().to_owned()),
            naming: target.naming,
        });
    }
    let schema = source
        .schema
        .clone()
        .or_else(|| target.loader_schema.clone())
        .ok_or_else(|| format!("target {name:?} has no loader_schema"))?;
    Ok(ResolvedTarget {
        name,
        warehouse,
        database: match connection {
            ConnectionConfig::MotherDuck { database, .. } => target
                .database
                .as_deref()
                .or(database.as_deref())
                .and_then(|database| motherduck_path(database).name)
                .map(str::to_owned),
            _ => target.database.clone().or(database),
        },
        schema,
        path: None,
        naming: target.naming,
    })
}

/// How a table's names load: its naming and its destination's name limit.
pub(crate) type NameRule = (Naming, Option<usize>);

/// `table` with each column it keeps named as it loads under `rule`, remembering how the source spells it, and its key and indexed columns renamed with them; an excluded column (`excluded`) keeps its name. The problems: two columns loading under one name, which `exclude_columns` on one of them resolves, and one renamed onto a column the load adds.
pub(crate) fn loaded_table(
    table: &SourceTable,
    excluded: &[(String, String)],
    rule: NameRule,
) -> Result<SourceTable, Vec<String>> {
    let mut loaded = table.clone();
    let mut renamed: BTreeMap<String, String> = BTreeMap::new();
    let mut taken: BTreeMap<String, String> = BTreeMap::new();
    let mut problems: Vec<String> = Vec::new();
    for column in &mut loaded.columns {
        let kept = !excluded.iter().any(|(name, _)| *name == column.name);
        let name = match kept {
            true => loaded_name(rule.0, &column.name, rule.1),
            false => column.name.clone(),
        };
        if name != column.name && is_load_column(rule.0, &name) {
            problems.push(format!(
                "column {:?} loads as {name}, a column the load adds; exclude it with exclude_columns",
                column.name
            ));
        }
        if let Some(other) = taken.insert(name.clone(), column.name.clone()) {
            problems.push(format!(
                "columns {other:?} and {:?} both load as {name} under naming = \"dlt\"; exclude one with exclude_columns",
                column.name
            ));
        }
        if name != column.name {
            renamed.insert(column.name.clone(), name.clone());
            column.source_spelling = Some(
                column
                    .source_spelling
                    .take()
                    .unwrap_or_else(|| column.name.clone()),
            );
            column.name = name;
        }
    }
    let rename = |names: &mut Vec<String>| {
        for name in names {
            if let Some(loaded) = renamed.get(name) {
                name.clone_from(loaded);
            }
        }
    };
    rename(&mut loaded.primary_key);
    rename(&mut loaded.indexed_columns);
    match problems.is_empty() {
        true => Ok(loaded),
        false => Err(problems),
    }
}

/// `overrides` with every column it names as the column loads under `rule`, so a setting may name a column either way: its pins, masks, `unnest` paths (each part) and settings. `exclude_columns` and `allow_columns` match the source's names, and `flatten` the yielded objects', so they stay as written.
pub(crate) fn loaded_overrides(overrides: &TableOverride, rule: NameRule) -> TableOverride {
    let name = |column: &str| loaded_name(rule.0, column, rule.1);
    TableOverride {
        settings: loaded_settings(&overrides.settings, rule),
        columns: renamed_keys(&overrides.columns, &name),
        mask: renamed_keys(&overrides.mask, &name),
        unnest: overrides
            .unnest
            .as_ref()
            .map(|unnest| loaded_unnest(unnest, &name)),
        ..overrides.clone()
    }
}

/// `settings` with every column they name as the column loads under `rule`: the unique key, cursor, constant columns, dedup sort, checks, and the bare column names among ClickHouse's `order_by`, `primary_key` and `partition_by`.
pub(crate) fn loaded_settings(settings: &TableSettings, rule: NameRule) -> TableSettings {
    let name = |column: &str| loaded_name(rule.0, column, rule.1);
    let names = |columns: &Vec<String>| {
        columns
            .iter()
            .map(|column| match column == PRIMARY_KEY_PLACEHOLDER {
                true => column.clone(),
                false => name(column),
            })
            .collect()
    };
    let keyed = |expression: &str| match is_plain_name(expression) {
        true => name(expression),
        false => expression.to_owned(),
    };
    let keys = |expressions: &Vec<String>| {
        expressions
            .iter()
            .map(|expression| keyed(expression))
            .collect()
    };
    let mut checks = settings.checks.clone();
    if let Some(checks) = &mut checks {
        checks.unique = checks.unique.as_ref().map(names);
        checks.not_null = names(&checks.not_null);
    }
    TableSettings {
        unique_key: settings.unique_key.as_ref().map(names),
        cursor_column: settings.cursor_column.as_deref().map(name),
        constant_columns: settings
            .constant_columns
            .as_ref()
            .map(|constants| ConstantColumns(renamed_keys(&constants.0, &name))),
        dedup_sort: settings
            .dedup_sort
            .as_ref()
            .map(|sort| renamed_keys(sort, &name)),
        checks,
        order_by: settings.order_by.as_ref().map(keys),
        primary_key: settings.primary_key.as_ref().map(keys),
        partition_by: settings.partition_by.as_deref().map(keyed),
        ..settings.clone()
    }
}

/// Whether a layout entry is a column's bare name (`OrderID`) rather than an expression (`toDate(CreatedAt)`), which the naming leaves as written.
fn is_plain_name(expression: &str) -> bool {
    let mut characters = expression.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

/// `unnest` with each part of each dotted path renamed by `name`; `"all"` as it is.
fn loaded_unnest(unnest: &UnnestSetting, name: &impl Fn(&str) -> String) -> UnnestSetting {
    let mut loaded = unnest.clone();
    if let UnnestSetting::Paths(paths) = &mut loaded {
        for path in paths {
            *path = path.split('.').map(name).collect::<Vec<_>>().join(".");
        }
    }
    loaded
}

/// `map` with each key renamed by `name`.
fn renamed_keys<T: Clone>(
    map: &BTreeMap<String, T>,
    name: &impl Fn(&str) -> String,
) -> BTreeMap<String, T> {
    map.iter()
        .map(|(key, value)| (name(key), value.clone()))
        .collect()
}
