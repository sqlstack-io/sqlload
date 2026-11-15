use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::dialect::connection_warehouse::connection_warehouse;
use crate::adapters::dispatch::main::dialect::files_destination::files_destination;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::models::Destination;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::load_naming::main::is_load_column::is_load_column;
use crate::load_naming::main::loaded_name::loaded_name;
use crate::load_naming::models::Naming;
use crate::loader::_helpers::specs::ColumnSelection;
use crate::loader::_helpers::specs::missing_description;
use crate::loader::_helpers::specs::table_name;
use crate::loader::_helpers::specs::{
    ColumnChoices, SourceContext, build_spec, column_selection, pinned_types, table_naming,
};
use crate::loader::_helpers::table_plan::{blocked_plan, plan_table};
use crate::loader::constants::{
    DATE_TEXT_LENGTH, NO_TRANSFORM_HOST, PRIMARY_KEY_PLACEHOLDER, RANGE_NEEDS_CURSOR,
};
use crate::loader::errors::LoaderError;
use crate::loader::models::{
    PlanOptions, ProjectPlan, RangeFlags, ResolvedTables, SkippedTable, TablePlan, TableSpec,
    TableTransform, TypedCursors, UnreadSource,
};
use crate::postgres_source::models::SourceTable;
use crate::project::main::config_at::config_at;
use crate::project::main::motherduck_path::motherduck_path;
use crate::project::models::{
    ConfigLines, ConnectionConfig, ConstantColumns, NewTables, NullCursor, ProjectConfig,
    SourceConfig, SourceKind, TableOverride, TableSettings, UnnestSetting,
};
use crate::schema_plan::models::SchemaPlan;
use crate::similar_names::main::nearest_name::nearest_name;
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
        let shown = || {
            let rule = (
                table_naming((&overrides.settings, &source.defaults), target.naming),
                sql_adapter(target.warehouse).max_identifier_length(),
            );
            let name = table_name(
                (source_name, source),
                overrides.name.as_deref(),
                (table, rule),
            );
            target_display_name(&target, name)
        };
        let transform = table_transform(project, (source, &overrides), &qualified);
        let resolved_table = match (resolve_table(&context, table, &overrides), transform) {
            (Err(problems), _) => Err(problems),
            (Ok(_), Err(problem)) => Err(vec![problem]),
            (Ok(found), Ok(transform)) => Ok(found.map(|mut spec| {
                spec.transform = transform;
                spec
            })),
        };
        match resolved_table {
            Ok(Ok(mut spec)) => {
                let lines = &project.config_lines;
                spec.settings_at = settings_at(lines, source_name, &qualified);
                spec.unique_key_at = config_at(
                    lines,
                    &["sources", source_name, "tables", &qualified, "unique_key"],
                )
                .or_else(|| config_at(lines, &["sources", source_name, "unique_key"]));
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
                destination: shown(),
                reason,
            }),
            Err(problems) => {
                let destination = shown();
                let place = settings_at(&project.config_lines, source_name, &qualified);
                for problem in problems {
                    let problem = format!("{qualified}: {problem}");
                    resolved
                        .problem_names
                        .push((problem.clone(), destination.clone()));
                    resolved
                        .problem_places
                        .push((problem.clone(), place.clone()));
                    resolved.problems.push(problem);
                }
            }
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

/// The `transform` a table runs (its own, else its source's) with the version `project` imported it at, or why it cannot run.
fn table_transform(
    project: &ProjectConfig,
    (source, overrides): (&SourceConfig, &TableOverride),
    qualified: &str,
) -> Result<Option<TableTransform>, String> {
    let Some(function) = overrides
        .settings
        .transform
        .clone()
        .or_else(|| source.defaults.transform.clone())
    else {
        return Ok(None);
    };
    match project.transforms.get(&function) {
        Some(Ok(version)) => Ok(Some(TableTransform {
            function,
            version: version.clone(),
            older: 0,
            creates_log: false,
        })),
        Some(Err(reason)) => Err(format!("transform {function}: {reason}")),
        None => Err(NO_TRANSFORM_HOST.replace("{place}", &format!("table {qualified}"))),
    }
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

/// The name `plan` shows table `name` of `target` by: `schema.table` as the warehouse displays it, or the table alone in a files target, which has no schema.
pub(crate) fn target_display_name(target: &ResolvedTarget, name: String) -> String {
    display_name(
        target.warehouse,
        &TableName {
            database: target.database.clone(),
            schema: target.schema.clone(),
            table: name,
        },
    )
}

/// The name `plan` shows `table` of a `warehouse` by: `schema.table` as the warehouse displays it, or the table alone where it has no schema (a files target).
pub(crate) fn display_name(warehouse: Warehouse, table: &TableName) -> String {
    match table.schema.is_empty() {
        true => table.table.clone(),
        false => sql_adapter(warehouse).render_display_name(table),
    }
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

/// `settings` with every column they name as the column loads under `rule`: the unique and replace keys, cursor, constant columns, dedup sort, checks, and the bare column names among ClickHouse's `order_by`, `primary_key` and `partition_by`.
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
        replace_key: settings.replace_key.as_ref().map(names),
        cursor_column: settings.cursor_column.as_ref().map(names),
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

/// Where `qualified`'s settings are written in the project file: its `[sources.*.tables]` entry, else its source's section.
fn settings_at(lines: &ConfigLines, source_name: &str, qualified: &str) -> Option<String> {
    config_at(lines, &["sources", source_name, "tables", qualified])
        .or_else(|| config_at(lines, &["sources", source_name]))
}

/// The tables source `source_name`, which could not be read, names itself, by display name: each named exactly in `include` or under `tables`, or a files source's one table; and whether `include` also has patterns.
pub(crate) fn unread_source(
    project: &ProjectConfig,
    (source_name, source): (&str, &SourceConfig),
) -> UnreadSource {
    let Ok(target) = source_target(project, source) else {
        return UnreadSource {
            connection: source.connection.clone(),
            variables: project
                .connection_variables
                .get(&source.connection)
                .cloned()
                .unwrap_or_default(),
            ..UnreadSource::default()
        };
    };
    let pattern = |name: &String| name.contains(['*', '?', '[']);
    let named: Vec<String> = match source.kind {
        SourceKind::Files => vec![source_name.to_owned()],
        _ => source
            .include
            .iter()
            .filter(|name| !pattern(name))
            .chain(source.tables.keys())
            .cloned()
            .collect(),
    };
    let unset = TableSettings::default();
    let mut tables: Vec<String> = Vec::new();
    for qualified in named {
        let overrides = source.tables.get(&qualified);
        let rule = (
            table_naming(
                (
                    overrides.map_or(&unset, |overrides| &overrides.settings),
                    &source.defaults,
                ),
                target.naming,
            ),
            sql_adapter(target.warehouse).max_identifier_length(),
        );
        let mut listed = SourceTable::of_columns(qualified.clone(), Vec::new());
        if let Some((schema, name)) = qualified
            .split_once('.')
            .filter(|_| source.discovers_tables())
        {
            listed.schema = schema.to_owned();
            listed.name = name.to_owned();
        }
        let name = overrides.and_then(|overrides| overrides.name.as_deref());
        let shown = target_display_name(
            &target,
            table_name((source_name, source), name, (&listed, rule)),
        );
        if !tables.contains(&shown) {
            tables.push(shown);
        }
    }
    tables.sort();
    UnreadSource {
        tables,
        patterns: source.include.iter().any(pattern),
        connection: source.connection.clone(),
        variables: project
            .connection_variables
            .get(&source.connection)
            .cloned()
            .unwrap_or_default(),
    }
}

/// Each of `specs`' names a selector may use (see [`spec_names`]).
pub(crate) fn destination_names(specs: &[TableSpec]) -> Vec<String> {
    specs.iter().flat_map(spec_names).collect()
}

/// The names a selector may pick `spec` by: its destination table, the destination's display name (`schema.table`, as `plan` shows it) and its source's `schema.table`.
pub(crate) fn spec_names(spec: &TableSpec) -> [String; 3] {
    [
        spec.destination.table.clone(),
        display_name(spec.warehouse, &spec.destination),
        spec.source_table.qualified_name(),
    ]
}

/// `planned`, the selected tables of one source, added to `plan`, which is returned (`unselected` and `found`, the destinations and options, tell whether this is the source's first load): a table that appeared after the source's first load (`first_load`: none of its tables, selected or not, existed yet) and is not named exactly in `include` is skipped or fails as `new_tables` says, and a table without a description is blocked.
pub(crate) async fn admit_tables(
    mut plan: ProjectPlan,
    (source_name, source): (&String, &SourceConfig),
    (planned, unselected): (Vec<TablePlan>, &[TableSpec]),
    found: (&BTreeMap<String, Destination>, &PlanOptions),
) -> Result<ProjectPlan, LoaderError> {
    let first_load = first_load(&planned, unselected, found).await?;
    let mut planned = planned;
    if let Some(problem) = partial_capture(source_name, source.kind, (&planned, unselected)) {
        for table in &mut planned {
            table.schema = blocked_plan(vec![problem.clone()]);
        }
    }
    for mut table in planned {
        let spec = &table.spec;
        let qualified = spec.source_table.qualified_name();
        let is_new = !first_load && !table.exists && !spec.approved;
        let has_settings = source.tables.contains_key(&qualified);
        match (is_new, spec.new_tables, has_settings) {
            (true, NewTables::Skip, true) => plan.skipped.push(SkippedTable {
                source_name: source_name.clone(),
                source_table: qualified,
                destination: display_name(spec.warehouse, &spec.destination),
                reason: "new table (new_tables = \"skip\") has settings but is not approved; add it to include".to_owned(),
            }),
            (true, NewTables::Skip, false) => plan.skipped.push(SkippedTable {
                source_name: source_name.clone(),
                source_table: qualified,
                destination: display_name(spec.warehouse, &spec.destination),
                reason: "new table (new_tables = \"skip\"); add it to include to sync it".to_owned(),
            }),
            (true, NewTables::Fail, _) => {
                plan.failed_sources.insert(source_name.clone());
                let problem = format!(
                    "{qualified}: new table and new_tables = \"fail\"; add it to include or exclude it"
                );
                plan.problem_names.push((
                    problem.clone(),
                    display_name(spec.warehouse, &spec.destination),
                ));
                plan.problems.push(problem);
            }
            _ => {
                if let Some(problem) = missing_description(source_name, source, &spec.source_table)
                {
                    table.schema = blocked_plan(vec![problem]);
                }
                plan.tables.push(table);
            }
        }
    }
    Ok(plan)
}

/// Why the selection cannot load `postgres_cdc` source `name`, which blocks every table it picks: it picks some of its tables (`planned`) but not all (`unselected`), and the slot they share would confirm the left-out tables' changes unwritten. A plan problem alone would not stop the picked tables loading.
fn partial_capture(
    name: &str,
    kind: SourceKind,
    (planned, unselected): (&[TablePlan], &[TableSpec]),
) -> Option<String> {
    let partial = kind == SourceKind::PostgresCdc && !planned.is_empty() && !unselected.is_empty();
    partial.then(|| {
        let left_out: Vec<String> = unselected
            .iter()
            .map(|spec| spec.source_table.qualified_name())
            .collect();
        format!(
            "this selection leaves out {} of source {name}, whose changes share its one slot and would be skipped; select the whole source, or move those tables to another postgres_cdc source",
            left_out.join(", ")
        )
    })
}

/// Whether this is the source's first load: none of its tables exists in the destination, whether selected (`planned`) or not (`unselected`, looked up only while none is found, a table that cannot be read counting as absent), so `--select` cannot make a later table look present at the first load.
async fn first_load(
    planned: &[TablePlan],
    unselected: &[TableSpec],
    (destinations, options): (&BTreeMap<String, Destination>, &PlanOptions),
) -> Result<bool, LoaderError> {
    if planned.iter().any(|table| table.exists) {
        return Ok(false);
    }
    for spec in unselected {
        let Some(destination) = destinations.get(&spec.target) else {
            continue;
        };
        let exists = match files_destination(destination) {
            Some(_) => plan_table(spec, destination, options)
                .await
                .is_ok_and(|table| table.exists),
            None => live_columns(destination, &spec.destination)
                .await
                .is_ok_and(|live| live.is_some()),
        };
        if exists {
            return Ok(false);
        }
    }
    Ok(true)
}

/// A warning for each selector matching no source or table, with the nearest name: `load` would otherwise say only `0 selected`.
pub(crate) fn unmatched_selectors(
    project: &ProjectConfig,
    options: &PlanOptions,
    names: &[String],
) -> Vec<String> {
    let known: Vec<&str> = names
        .iter()
        .map(String::as_str)
        .chain(project.sources.keys().map(String::as_str))
        .collect();
    options
        .select
        .iter()
        .chain(&options.exclude)
        .filter(|selector| !known.contains(&selector.as_str()))
        .map(
            |selector| match nearest_name(selector, known.iter().copied()) {
                Some(nearest) => format!("no table matches {selector}; did you mean {nearest}?"),
                None => format!("no table matches {selector}"),
            },
        )
        .collect()
}

/// The cursors a typed range flag applies to: dates and timestamps (`-ts`), or integers (`-int`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorFamily {
    Time,
    Integer,
}

/// A typed range flag given: its name, its value and the cursors it applies to.
type GivenFlag<'a> = (&'static str, &'a String, CursorFamily);

/// One table's `options` and `schema` once its cursor, `cursor_type` (`None` while a Python table's cursor is unseen), is known: its family's typed range as `start_cursor`/`end_cursor`, and `schema` blocked by why the typed flags or the lookback can't apply, or warned of the rows a ranged `--reload` drops.
pub(crate) fn ranged_table(
    options: &PlanOptions,
    spec: &TableSpec,
    cursor_type: Option<&ColumnType>,
    mut schema: SchemaPlan,
) -> (PlanOptions, SchemaPlan) {
    let (ranged, problem) = match table_range(options, spec, cursor_type, false) {
        Ok(ranged) => (ranged, None),
        Err(problem) => (options.clone(), Some(problem)),
    };
    if let Some(problem) = problem.filter(|_| schema.blocked.is_none()) {
        schema = blocked_plan(vec![problem]);
    }
    schema.warnings.extend(reload_range(spec, &ranged));
    (ranged, schema)
}

/// `options` for one table whose cursor is `cursor_type` (`None` while a Python table's cursor is unseen), into a files target when `files_target`: with typed flags, its family's range as `start_cursor`/`end_cursor`; or why the typed flags or its `lookback` can't apply to it.
pub(crate) fn table_range(
    options: &PlanOptions,
    spec: &TableSpec,
    cursor_type: Option<&ColumnType>,
    files_target: bool,
) -> Result<PlanOptions, String> {
    let ranged = table_options(options, spec, cursor_type)?;
    lookback_problem(spec, cursor_type, files_target).map_or(Ok(ranged), Err)
}

/// The typed flags given, in the order `plan` names them.
fn given_flags(typed: &TypedCursors) -> Vec<GivenFlag<'_>> {
    let flags = [
        ("--start-cursor-ts", &typed.start_ts, CursorFamily::Time),
        ("--end-cursor-ts", &typed.end_ts, CursorFamily::Time),
        (
            "--start-cursor-int",
            &typed.start_int,
            CursorFamily::Integer,
        ),
        ("--end-cursor-int", &typed.end_int, CursorFamily::Integer),
    ];
    let mut given: Vec<GivenFlag<'_>> = Vec::new();
    for (flag, value, family) in flags {
        if let Some(value) = value {
            given.push((flag, value, family));
        }
    }
    given
}

/// [`table_range`] without the lookback.
fn table_options(
    options: &PlanOptions,
    spec: &TableSpec,
    cursor_type: Option<&ColumnType>,
) -> Result<PlanOptions, String> {
    let typed = &options.typed_cursors;
    let given = given_flags(typed);
    if given.is_empty() {
        return Ok(options.clone());
    }
    let Some(cursor) = &spec.cursor else {
        return Err(RANGE_NEEDS_CURSOR.to_owned());
    };
    let named: Vec<&str> = given.iter().map(|(flag, _, _)| *flag).collect();
    let has = |family: CursorFamily| given.iter().any(|(_, _, given)| *given == family);
    let family = match cursor_type {
        Some(ColumnType::Int | ColumnType::SmallInt) => CursorFamily::Integer,
        Some(ColumnType::Date | ColumnType::TimestampTz | ColumnType::TimestampNtz) => {
            CursorFamily::Time
        }
        Some(other) => {
            return Err(format!(
                "cursor {} is {other}, which takes --start-cursor/--end-cursor, not {}: the -ts flags are for date and timestamp cursors and the -int flags for integer ones",
                cursor.label(),
                named.join(", ")
            ));
        }
        None => match (has(CursorFamily::Time), has(CursorFamily::Integer)) {
            (true, false) => CursorFamily::Time,
            (false, true) => CursorFamily::Integer,
            _ => {
                return Err(format!(
                    "cursor {} has no type until the table first loads, so it can't choose between the -ts and -int flags; give one family's flags, or --start-cursor/--end-cursor",
                    cursor.label()
                ));
            }
        },
    };
    if !has(family) {
        let (given_kind, wanted) = match family {
            CursorFamily::Time => ("integer", "--start-cursor-ts/--end-cursor-ts"),
            CursorFamily::Integer => ("date and timestamp", "--start-cursor-int/--end-cursor-int"),
        };
        return Err(format!(
            "{} {} for {given_kind} cursors, but cursor {} is {}; give this table {wanted}, or leave it out of the selection",
            named.join(", "),
            match named.len() {
                1 => "is",
                _ => "are",
            },
            cursor.label(),
            cursor_type.map(ToString::to_string).unwrap_or_default(),
        ));
    }
    if cursor_type == Some(&ColumnType::Date)
        && let Some((flag, value, _)) = given.iter().find(|(_, value, family)| {
            *family == CursorFamily::Time && value.trim().len() > DATE_TEXT_LENGTH
        })
    {
        return Err(format!(
            "{flag} {value} has a time of day, but cursor {} is a date; give a date, such as {}",
            cursor.label(),
            value.trim().get(..DATE_TEXT_LENGTH).unwrap_or_default()
        ));
    }
    let (start, end, range_flags) = match family {
        CursorFamily::Time => (
            &typed.start_ts,
            &typed.end_ts,
            RangeFlags {
                start: "--start-cursor-ts",
                end: "--end-cursor-ts",
            },
        ),
        CursorFamily::Integer => (
            &typed.start_int,
            &typed.end_int,
            RangeFlags {
                start: "--start-cursor-int",
                end: "--end-cursor-int",
            },
        ),
    };
    Ok(PlanOptions {
        start_cursor: start.clone(),
        end_cursor: end.clone(),
        range_flags,
        ..options.clone()
    })
}

/// Why `spec`'s `lookback` can't apply to its cursor, `cursor_type` (`None` while a Python table's cursor is unseen), or into a files target (`files_target`): an integer lookback that counts back no units, or on anything but an integer cursor; a duration on anything but a date or timestamp cursor.
fn lookback_problem(
    spec: &TableSpec,
    cursor_type: Option<&ColumnType>,
    files_target: bool,
) -> Option<String> {
    let cursor = spec.cursor.as_ref()?;
    let label = cursor.label();
    if let Some(units) = cursor.lookback_units {
        if units < 1 {
            return Some(format!(
                "lookback = {units} counts back no cursor units; give 1 or more, or leave lookback out"
            ));
        }
        if files_target {
            return Some(format!(
                "lookback = {units} counts back integer cursor units, which a files target's window can't: it follows its newest partition; leave lookback out"
            ));
        }
        return match cursor_type? {
            ColumnType::Int | ColumnType::SmallInt => None,
            time @ (ColumnType::Date | ColumnType::TimestampTz | ColumnType::TimestampNtz) => {
                Some(format!(
                    "lookback = {units} counts integer cursor units, but cursor {label} is {time}; use a duration, such as lookback = \"1d\""
                ))
            }
            other => Some(format!(
                "lookback needs a date, timestamp or integer cursor, but cursor {label} is {other}; leave lookback out"
            )),
        };
    }
    let duration = cursor.lookback?;
    match cursor_type? {
        ColumnType::Date | ColumnType::TimestampTz | ColumnType::TimestampNtz => None,
        ColumnType::Int | ColumnType::SmallInt => Some(format!(
            "lookback = \"{}\" is a duration, but cursor {label} is an integer; use a number of cursor units, such as lookback = 1",
            humantime::format_duration(duration)
        )),
        other => Some(format!(
            "lookback needs a date, timestamp or integer cursor, but cursor {label} is {other}; leave lookback out"
        )),
    }
}

/// What a `--reload` with a cursor range (`options`, as one table's [`table_range`] gave them) leaves out of the rebuilt table: the rows outside that range are dropped; rows with a NULL cursor are kept under `null_cursor = "include"`, which reads them with the first window, and dropped under `"ignore"` (under `"fail"`, the load fails on them).
fn reload_range(spec: &TableSpec, options: &PlanOptions) -> Option<String> {
    let RangeFlags {
        start: start_flag,
        end: end_flag,
    } = options.range_flags;
    let (start, end) = (
        options.start_cursor.as_deref().filter(|_| options.reload),
        options.end_cursor.as_deref().filter(|_| options.reload),
    );
    let (kept, dropped) = match (start, end) {
        (Some(start), Some(end)) => (
            format!("from {start} ({start_flag}), below {end} ({end_flag})"),
            format!("rows before {start} and from {end} on are dropped"),
        ),
        (Some(start), None) => (
            format!("from {start} ({start_flag})"),
            format!("rows before {start} are dropped"),
        ),
        (None, Some(end)) => (
            format!("below {end} ({end_flag})"),
            format!("rows from {end} on are dropped"),
        ),
        (None, None) => return None,
    };
    let nulls = match spec.cursor.as_ref().map(|cursor| cursor.null_cursor) {
        Some(NullCursor::Include) => "; rows with a NULL cursor are kept",
        Some(NullCursor::Ignore) => "; rows with a NULL cursor are dropped",
        Some(NullCursor::Fail) | None => "",
    };
    Some(format!(
        "--reload rebuilds the table with only the rows {kept}: {dropped}{nulls}"
    ))
}
