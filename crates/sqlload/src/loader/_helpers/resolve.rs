use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::dialect::connection_warehouse::connection_warehouse;
use crate::adapters::dispatch::main::dialect::files_destination::files_destination;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::models::Destination;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::files_source::models::{AfterLoad, OnEmpty};
use crate::load_events::main::begin_step::begin_step;
use crate::load_events::main::end_step::end_step;
use crate::load_events::models::StepEnd;
use crate::load_naming::constants::SQLLOAD_PREFIX;
use crate::load_naming::main::is_load_column::is_load_column;
use crate::load_naming::main::loaded_columns::loaded_columns;
use crate::load_naming::main::loaded_name::loaded_name;
use crate::load_naming::models::{
    ColumnName, Compat, LoadedColumn, LoadedNames, NameRules, Naming, NumberedName, RenameColumn,
};
use crate::load_naming::types::RenameFunction;
use crate::loader::_helpers::destination::folder_claims;
use crate::loader::_helpers::run_log::kept_numbered;
use crate::loader::_helpers::sources::scoped_tables;
use crate::loader::_helpers::specs::ColumnSelection;
use crate::loader::_helpers::specs::missing_description;
use crate::loader::_helpers::specs::table_name;
use crate::loader::_helpers::specs::{
    ColumnChoices, SourceContext, build_spec, column_selection, layered, pinned_types,
    table_compat, table_naming,
};
#[cfg(feature = "odbc")]
use crate::loader::_helpers::table_plan::block_refused_bounds;
use crate::loader::_helpers::table_plan::{blocked_plan, blocked_table, plan_table};
use crate::loader::constants::{
    DATE_TEXT_LENGTH, NO_RENAME_HOST, NO_TRANSFORM_HOST, PRIMARY_KEY_PLACEHOLDER,
    RANGE_NEEDS_CURSOR, TOMBSTONE_SUFFIX,
};
use crate::loader::errors::LoaderError;
use crate::loader::models::{DrainedFiles, FoundSource, FoundSources, SourceReader};
use crate::loader::models::{
    PlanOptions, ProjectPlan, RangeFlags, ResolvedTables, SkippedTable, TablePlan, TableSpec,
    TableTransform, TypedCursors, UnreadSource,
};
use crate::postgres_source::models::SourceTable;
use crate::project::main::config_at::config_at;
use crate::project::main::motherduck_path::motherduck_path;
use crate::project::models::{
    ConfigLines, ConnectionConfig, ConstantColumns, JsonType, NewTables, NullCursor, PathsSetting,
    ProjectConfig, RenameSetting, SendMode, SourceConfig, SourceKind, TableOverride, TableSettings,
    WriteStrategy,
};
use crate::python_sources::types::PythonHost;
use crate::schema_plan::models::SchemaPlan;
use crate::similar_names::main::nearest_name::nearest_name;
use crate::ssh_tunnel::models::SshTunnel;
use std::collections::BTreeMap;

/// The specs for `discovered` tables of source `source_name`, with problems and skipped tables, including each table listed under `[sources.NAME.tables]` that discovery didn't find.
pub(crate) fn resolve_tables(
    project: &ProjectConfig,
    source_name: &str,
    discovered: &[SourceTable],
) -> ResolvedTables {
    let mut resolved = resolve_known(project, source_name, discovered, &KnownNames::default());
    let Some(source) = project.sources.get(source_name) else {
        return resolved;
    };
    for (listed, table) in &source.tables {
        if discovered
            .iter()
            .any(|found| found.qualified_name() == *listed)
        {
            continue;
        }
        resolved.problems.push(match &table.tombstones_of {
            Some(main) => format!(
                "{main}: deletes_from table {listed:?} not found in the source (or excluded by exclude)"
            ),
            None => format!(
                "{listed}: listed under [sources.{source_name}.tables] but not found in the source (or not selected by include/exclude)"
            ),
        });
    }
    resolved
}

/// [`resolve_tables`] for tables planned before, whose names `known` keeps. It resolves only the tables given, so it doesn't check the source's other listed tables, which discovery already did.
pub(crate) fn resolve_known(
    project: &ProjectConfig,
    source_name: &str,
    discovered: &[SourceTable],
    known: &KnownNames,
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
        let bare = overrides.tombstones_of.as_ref().map(|_| SourceConfig {
            defaults: TableSettings::default(),
            ..source.clone()
        });
        let context = SourceContext {
            name: source_name,
            source: bare.as_ref().unwrap_or(source),
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
        let call = |function: &str, columns: &[RenameColumn]| {
            renamed_by(project, (source_name, &qualified), function, columns)
        };
        let resolved_table = match (
            resolve_table(&context, (table, known), &overrides, &call),
            transform,
        ) {
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
    tombstone_destinations(&mut resolved.specs);
    resolved
        .problems
        .extend(merged_names(source_name, source, &resolved.specs));
    resolved
}

/// Each tombstone table among `specs` loading beside the table whose `deletes_from` it is, as `<table>__sqlload_deletes`, with only that table's `unique_key` columns and its own cursor.
fn tombstone_destinations(specs: &mut [TableSpec]) {
    let named: Vec<(String, TableName, Vec<String>)> = specs
        .iter()
        .map(|spec| {
            let key = spec.source_table.qualified_name();
            (key, spec.destination.clone(), spec.unique_key.clone())
        })
        .collect();
    for spec in specs.iter_mut() {
        let main = spec.tombstones_of.as_ref();
        let Some((_, destination, unique_key)) = named.iter().find(|(key, ..)| Some(key) == main)
        else {
            continue;
        };
        spec.destination = TableName {
            table: format!("{}{TOMBSTONE_SUFFIX}", destination.table),
            ..destination.clone()
        };
        let cursor = spec.cursor.as_ref().map(|cursor| cursor.column.clone());
        let dropped: Vec<(String, String)> = spec
            .source_table
            .columns
            .iter()
            .filter(|column| {
                !unique_key.contains(&column.name) && Some(&column.name) != cursor.as_ref()
            })
            .filter(|column| !is_excluded(&spec.excluded_columns, &column.name))
            .map(|column| {
                let reason = "deletes_from keeps only the unique_key columns and the cursor";
                (column.name.clone(), reason.to_owned())
            })
            .collect();
        spec.excluded_columns.extend(dropped);
    }
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

/// A problem for each pair of `specs` (one source's) that the naming loads into one table although their names differ (`order-items` and `order_items`), where one would overwrite or mix with the other's rows.
fn merged_names(source_name: &str, source: &SourceConfig, specs: &[TableSpec]) -> Vec<String> {
    let written = |spec: &TableSpec| {
        let name = source
            .tables
            .get(&spec.source_table.qualified_name())
            .and_then(|overrides| overrides.name.as_deref());
        table_name(
            (source_name, source),
            name,
            (&spec.source_table, (Naming::Source, None)),
        )
    };
    let mut problems: Vec<String> = Vec::new();
    for (index, spec) in specs.iter().enumerate() {
        let clash = specs[..index].iter().find(|earlier| {
            earlier.destination.table == spec.destination.table && written(earlier) != written(spec)
        });
        if let Some(earlier) = clash {
            problems.push(format!(
                "{} and {}: both load as table {}; give one a name of its own with name",
                earlier.source_table.qualified_name(),
                spec.source_table.qualified_name(),
                spec.destination.table
            ));
        }
    }
    problems
}

/// A table's rename function by the name `@sl.rename` registered it under: the names it gives the columns, by position, or why it gives none.
type RenameCall<'a> = &'a dyn Fn(&str, &[RenameColumn]) -> Result<BTreeMap<usize, String>, String>;

/// One table's spec, `Ok(Err(reason))` if the column rules skip it, or its problems: every column it keeps named as it loads (see [`loaded_table`]), and the settings naming columns by either name.
fn resolve_table(
    context: &SourceContext<'_>,
    (table, known): (&SourceTable, &KnownNames),
    overrides: &TableOverride,
    rename_call: RenameCall<'_>,
) -> Result<Result<TableSpec, String>, Vec<String>> {
    let mut excluded =
        match column_selection(context, table, overrides).map_err(|problem| vec![problem])? {
            ColumnSelection::Skip(reason) => return Ok(Err(reason)),
            ColumnSelection::Keep(excluded) => excluded,
        };
    let settings = (&overrides.settings, &context.source.defaults);
    let rules = NameRules {
        naming: table_naming(settings, context.target.naming),
        max_length: sql_adapter(context.target.warehouse).max_identifier_length(),
        renamed: match layered(&overrides.settings.rename, &context.source.defaults.rename) {
            Some((RenameSetting::Map(map), _)) => map,
            _ => BTreeMap::new(),
        },
        on_duplicate_names: layered(
            &overrides.settings.on_duplicate_names,
            &context.source.defaults.on_duplicate_names,
        )
        .map(|(value, _)| value)
        .unwrap_or_default(),
        kept: known.numbered.clone(),
        existing: known.existing.clone(),
        excluded: [&context.source.exclude_columns, &overrides.exclude_columns]
            .into_iter()
            .flatten()
            .cloned()
            .collect(),
        allowed: [&context.source.allow_columns, &overrides.allow_columns]
            .into_iter()
            .flatten()
            .cloned()
            .chain(overrides.mask.keys().map(|name| globset::escape(name)))
            .collect(),
        function: match layered(&overrides.settings.rename, &context.source.defaults.rename) {
            Some((RenameSetting::Function(function), _))
                if context.source.kind != SourceKind::Python =>
            {
                Some(function)
            }
            _ => None,
        },
    };
    let compat = table_compat(settings, context.target.compat);
    let rename = |columns: &[RenameColumn]| match &rules.function {
        Some(function) if !columns.is_empty() => rename_call(function, columns),
        _ => Ok(BTreeMap::new()),
    };
    let names = match known.named {
        Some(_) => same_names(table),
        None => loaded_table_names(table, (&excluded, compat), &rules, &rename)?,
    };
    let mut problems = unknown_renames(context, overrides, &names);
    let loaded = renamed_table(table, &names);
    let name = |column: &str| setting_name(column, (&names, known), &rules);
    let unknown = unknown_setting_columns(
        context,
        (table, &names, compat),
        (overrides, &excluded),
        &name,
    );
    let overrides = loaded_overrides(overrides, &name);
    let source = SourceConfig {
        defaults: loaded_settings(&context.source.defaults, &name),
        ..context.source.clone()
    };
    let context = SourceContext {
        name: context.name,
        source: &source,
        target: context.target,
    };
    excluded.retain(|(column, _)| !overrides.mask.contains_key(&name(column)));
    let pinned = pinned_types(&overrides, context.target.warehouse);
    let spec = match pinned {
        Ok(pinned) => build_spec(
            &context,
            &loaded,
            &overrides,
            ColumnChoices { excluded, pinned },
        ),
        Err(found) => Err(found),
    };
    match spec {
        Ok(mut spec) if problems.is_empty() => {
            spec.setting_problems = used_setting_problems(&spec, unknown);
            spec.listed =
                (context.source.kind != SourceKind::Python).then(|| Box::new(table.clone()));
            spec.names = names;
            spec.name_rules = NameRules {
                function: layered(&overrides.settings.rename, &context.source.defaults.rename)
                    .and_then(|(rename, _)| match rename {
                        RenameSetting::Function(function) => Some(function),
                        RenameSetting::Map(_) => None,
                    }),
                ..rules
            };
            Ok(Ok(spec))
        }
        Ok(_) => Err(problems),
        Err(found) => {
            problems.extend(found);
            Err(problems)
        }
    }
}

/// What a re-plan already knows of a table's names: the names numbering gave before, which stay; and, for a Python table whose batches arrive named, that its columns are already loaded names, with each yielded name's loaded one.
#[derive(Debug, Clone, Default)]
pub(crate) struct KnownNames {
    pub(crate) numbered: Vec<NumberedName>,
    pub(crate) named: Option<BTreeMap<String, String>>,
    /// The columns the table already has, when it exists.
    pub(crate) existing: Vec<String>,
}

/// What a load re-planning `table` as new columns arrive knows of its names: the numbers this load and earlier ones gave, which stay, and the columns the table has.
pub(crate) fn known_names(table: &TablePlan) -> KnownNames {
    let names = &table.spec.names;
    KnownNames {
        numbered: names.numbered.iter().chain(&names.kept).cloned().collect(),
        named: None,
        existing: table
            .live
            .iter()
            .flatten()
            .map(|column| column.name.clone())
            .collect(),
    }
}

/// The names of `table`'s columns through the name pipeline under `rules` (an excluded column, and one a files source adds such as `_sqlload_source_file`, keeps its own), or its problems, with a column loading as one the load adds under `compat`.
fn loaded_table_names(
    table: &SourceTable,
    (excluded, compat): (&[(String, String)], Compat),
    rules: &NameRules,
    rename: RenameFunction<'_>,
) -> Result<LoadedNames, Vec<String>> {
    let kept: Vec<ColumnName> = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| !is_excluded(excluded, &column.name))
        .filter(|(_, column)| !column.name.starts_with(SQLLOAD_PREFIX))
        .map(|(index, column)| ColumnName {
            position: index + 1,
            source: column
                .repeat_of
                .clone()
                .unwrap_or_else(|| column.name.clone()),
            read_as: column.name.clone(),
        })
        .collect();
    let names = loaded_columns(&kept, rules, rename)?;
    let problems: Vec<String> = names
        .columns
        .iter()
        .filter(|column| column.loaded != column.source && is_load_column(compat, &column.loaded))
        .map(|column| {
            format!(
                "column {:?} loads as {}, a column the load adds; rename it, or exclude it with exclude_columns",
                column.source, column.loaded
            )
        })
        .collect();
    match problems.is_empty() {
        true => Ok(names),
        false => Err(problems),
    }
}

/// `table`'s columns under their own names, for a table whose columns are already loaded names.
fn same_names(table: &SourceTable) -> LoadedNames {
    LoadedNames {
        columns: table
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| LoadedColumn {
                position: index + 1,
                source: column.name.clone(),
                read_as: column.name.clone(),
                loaded: column.name.clone(),
                change: None,
            })
            .collect(),
        numbered: Vec::new(),
        kept: Vec::new(),
        replaces_record: false,
    }
}

/// A problem for each key of a table's own `rename` map that names none of its columns, by source or default name; a source's map may name columns of other tables. A Python table's columns are known only at load.
fn unknown_renames(
    context: &SourceContext<'_>,
    overrides: &TableOverride,
    names: &LoadedNames,
) -> Vec<String> {
    let Some(RenameSetting::Map(map)) = &overrides.settings.rename else {
        return Vec::new();
    };
    if context.source.kind == SourceKind::Python {
        return Vec::new();
    }
    map.keys()
        .filter(|key| !names_column(names, key))
        .map(|key| format!("rename {key:?}: the table has no such column"))
        .collect()
}

/// Each setting and its problem for each column a setting of `table` names (its keys, cursor, `newest_by` and a ClickHouse layout's bare names; `checks` match ignoring case and are checked where they apply) that the table has under neither its source name nor its loaded one (`name`), nor as a constant, a column the load adds or an excluded column (which plan names as excluded), with the nearest column in both spellings; a different case is a miss too. A Python table's columns, and those flattening or unnesting adds, are known only from its rows, so such a table is not checked here.
fn unknown_setting_columns(
    context: &SourceContext<'_>,
    (table, names, compat): (&SourceTable, &LoadedNames, Compat),
    (overrides, excluded): (&TableOverride, &[(String, String)]),
    name: &dyn Fn(&str) -> String,
) -> Vec<(&'static str, String)> {
    if context.source.kind == SourceKind::Python
        || table.columns.is_empty()
        || overrides.flatten.is_some()
        || overrides.unnest.is_some()
    {
        return Vec::new();
    }
    let (settings, defaults) = (&overrides.settings, &context.source.defaults);
    let lists = [
        (
            "unique_key",
            &settings.unique_key,
            &defaults.unique_key,
            false,
        ),
        (
            "replace_key",
            &settings.replace_key,
            &defaults.replace_key,
            false,
        ),
        (
            "cursor_column",
            &settings.cursor_column,
            &defaults.cursor_column,
            false,
        ),
        ("newest_by", &settings.newest_by, &defaults.newest_by, false),
        ("order_by", &settings.order_by, &defaults.order_by, true),
        (
            "primary_key",
            &settings.primary_key,
            &defaults.primary_key,
            true,
        ),
    ];
    let mut named: Vec<(&'static str, &str)> = Vec::new();
    for (setting, own, inherited, layout) in lists {
        for column in own.as_ref().or(inherited.as_ref()).into_iter().flatten() {
            if !layout || is_plain_name(column) {
                named.push((setting, column));
            }
        }
    }
    let partition_by = settings
        .partition_by
        .as_ref()
        .or(defaults.partition_by.as_ref());
    if let Some(column) = partition_by.filter(|column| is_plain_name(column)) {
        named.push(("partition_by", column));
    }
    let constants = settings
        .constant_columns
        .as_ref()
        .or(defaults.constant_columns.as_ref());
    let mut problems: Vec<(&'static str, String)> = Vec::new();
    for (setting, column) in named {
        let known = column == PRIMARY_KEY_PLACEHOLDER
            || column.starts_with(SQLLOAD_PREFIX)
            || is_load_column(compat, column)
            || names_column(names, column)
            || table
                .columns
                .iter()
                .any(|source| source.name == column || source.repeat_of.as_deref() == Some(column))
            || constants.is_some_and(|constants| {
                constants
                    .0
                    .keys()
                    .any(|constant| name(constant) == name(column))
            })
            || excluded
                .iter()
                .any(|(source, _)| name(source) == name(column));
        if !known {
            problems.push((
                setting,
                format!(
                    "{setting} column {column:?} is not a column of the table{}",
                    nearest_column_hint(column, names)
                ),
            ));
        }
    }
    problems
}

/// The problems of `unknown`, by the setting each is about, for the settings `spec` uses: a `unique_key` when the table merges, follows changes, or sorts by the key on a target that does so when no `order_by` is set; its `replace_key`, `newest_by` and cursor once `build_spec` kept them (it drops a source's that the table's strategy doesn't use); and every layout setting.
fn used_setting_problems(spec: &TableSpec, unknown: Vec<(&'static str, String)>) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    for (setting, problem) in unknown {
        let used = match setting {
            "unique_key" => {
                !spec.unique_key.is_empty()
                    && (spec.strategy == WriteStrategy::Merge
                        || spec.cdc.is_some()
                        || (sql_adapter(spec.warehouse).sorts_by_unique_key()
                            && spec.layout.order_by.as_ref().is_none_or(Vec::is_empty)))
            }
            "replace_key" => !spec.replace_key.is_empty(),
            "newest_by" => !spec.newest_by.is_empty(),
            "cursor_column" => spec.cursor.is_some(),
            _ => true,
        };
        if used {
            problems.push(problem);
        }
    }
    problems
}

/// `; did you mean …?` naming the column of `names` whose source or loaded name is nearest `column`, in both spellings when they differ; empty when the table has no columns.
fn nearest_column_hint(column: &str, names: &LoadedNames) -> String {
    let mut spellings: Vec<&str> = Vec::new();
    for loaded in &names.columns {
        spellings.push(&loaded.source);
        spellings.push(&loaded.loaded);
    }
    let Some(nearest) = nearest_name(column, spellings) else {
        return String::new();
    };
    let Some(found) = names
        .columns
        .iter()
        .find(|loaded| loaded.source == nearest || loaded.loaded == nearest)
    else {
        return String::new();
    };
    match found.source == found.loaded {
        true => format!("; did you mean {}?", found.source),
        false => format!(
            "; did you mean {} (loads as {})?",
            found.source, found.loaded
        ),
    }
}

/// Whether `key` names a column of `names`: by its source name, its read name or its loaded one.
fn names_column(names: &LoadedNames, key: &str) -> bool {
    names
        .columns
        .iter()
        .any(|column| column.source == key || column.loaded == key || column.read_as == key)
}

/// Whether `excluded`, the columns `exclude_columns` leaves out with their patterns, has `name`.
fn is_excluded(excluded: &[(String, String)], name: &str) -> bool {
    excluded.iter().any(|(column, _)| column == name)
}

/// `table` with each column under the name `names` gives it, remembering how the source spells it (`source_spelling`), and its key and indexed columns renamed with them.
fn renamed_table(table: &SourceTable, names: &LoadedNames) -> SourceTable {
    let mut loaded = table.clone();
    let mut renamed: BTreeMap<String, String> = BTreeMap::new();
    for column in &mut loaded.columns {
        let Some(name) = names
            .columns
            .iter()
            .find(|name| name.read_as == column.name)
        else {
            continue;
        };
        if name.loaded != column.name {
            renamed
                .entry(column.name.clone())
                .or_insert_with(|| name.loaded.clone());
            column.source_spelling = Some(
                column
                    .source_spelling
                    .take()
                    .unwrap_or_else(|| column.name.clone()),
            );
            column.name.clone_from(&name.loaded);
        }
    }
    let rename = |columns: &mut Vec<String>| {
        for name in columns {
            if let Some(loaded) = renamed.get(name) {
                name.clone_from(loaded);
            }
        }
    };
    rename(&mut loaded.primary_key);
    rename(&mut loaded.indexed_columns);
    loaded
}

/// The column a setting's `key` names, as it loads: on a Python table's re-plan, the loaded name of the field it names; else the column whose source name it is (the first, for a repeated header), else one loading under it, else `key` as `rules` would name a column of that name (a Python table's columns are known only at load). A dotted path (a nested field's) is named part by part: its first part as the table's column, the parts below it under `naming` alone, as a child table or a flattened object names its keys.
fn setting_name(
    key: &str,
    (names, known): (&LoadedNames, &KnownNames),
    rules: &NameRules,
) -> String {
    if let Some(loaded) = known.named.as_ref().and_then(|named| named.get(key)) {
        return loaded.clone();
    }
    let column = names
        .columns
        .iter()
        .find(|column| column.source == key)
        .or_else(|| names.columns.iter().find(|column| column.read_as == key));
    if let Some(column) = column {
        return column.loaded.clone();
    }
    if names.columns.iter().any(|column| column.loaded == key) {
        return key.to_owned();
    }
    if let Some((first, below)) = key.split_once('.') {
        let below = below
            .split('.')
            .map(|part| loaded_name(rules.naming, part, rules.max_length));
        return std::iter::once(setting_name(first, (names, known), rules))
            .chain(below)
            .collect::<Vec<_>>()
            .join(".");
    }
    let named = loaded_name(rules.naming, key, rules.max_length);
    rules
        .renamed
        .get(key)
        .or_else(|| rules.renamed.get(&named))
        .cloned()
        .unwrap_or(named)
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
    /// How its tables and columns are named, unless a source or table says.
    pub(crate) naming: Option<Naming>,
    /// Whose names the columns a load adds take, unless a source or table says.
    pub(crate) compat: Option<Compat>,
    /// When its tables' rows reach it, unless a source or table says: as set, else at each commit on a warehouse billed while it runs queries (MotherDuck, Snowflake, Databricks), else each batch as it is read.
    pub(crate) send: SendMode,
    /// Where `send = "at_commit"` holds a commit's rows.
    pub(crate) staging_dir: Option<std::path::PathBuf>,
    /// `json_type = "variant"`: `json` columns are DuckDB `VARIANT` columns.
    pub(crate) variant_json: bool,
    /// A MotherDuck database, which DuckDB reaches but runs statements itself.
    pub(crate) motherduck: bool,
}

/// What each table of an `odbc` source reads the primary key its driver reports for: a merge on `unique_key = "{primary_key}"` merges on it; any other merge's plan notes whether it is the same key, a table without a `write_strategy` is suggested one by it, and a ClickHouse table (or one whose target can't be found) is sorted by it unless it sets its own key. Nothing else reads it, so the catalog doesn't ask the driver for it.
#[cfg(feature = "odbc")]
pub(crate) fn key_uses(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> crate::odbc_source::models::KeyUses {
    use crate::odbc_source::models::{KeyUse, KeyUses};
    let sorts = source_target(project, source).map_or(true, |target| {
        sql_adapter(target.warehouse).sorts_by_unique_key()
    });
    let key_use = |settings: &TableSettings| {
        let strategy = settings.write_strategy.or(source.defaults.write_strategy);
        let placeholder = settings
            .unique_key
            .as_ref()
            .or(source.defaults.unique_key.as_ref())
            .is_some_and(|key| key.iter().any(|column| column == PRIMARY_KEY_PLACEHOLDER));
        match (strategy, placeholder) {
            (Some(crate::project::models::WriteStrategy::Merge), true) => KeyUse::Merged,
            (None, _) | (Some(crate::project::models::WriteStrategy::Merge), false) => KeyUse::Read,
            _ if sorts => KeyUse::Read,
            _ => KeyUse::Unused,
        }
    };
    KeyUses {
        source: key_use(&TableSettings::default()),
        tables: source
            .tables
            .iter()
            .map(|(name, table)| (name.clone(), key_use(&table.settings)))
            .collect(),
    }
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
            compat: target.compat,
            send: target.send.unwrap_or(SendMode::PerBatch),
            staging_dir: target.staging_dir.clone(),
            variant_json: false,
            motherduck: false,
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
        compat: target.compat,
        send: target.send.unwrap_or(
            match matches!(connection, ConnectionConfig::MotherDuck { .. })
                || sql_adapter(warehouse).bills_while_running()
            {
                true => SendMode::AtCommit,
                false => SendMode::PerBatch,
            },
        ),
        staging_dir: target.staging_dir.clone(),
        variant_json: target.json_type == Some(JsonType::Variant),
        motherduck: matches!(connection, ConnectionConfig::MotherDuck { .. }),
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

/// `overrides` with every column it names as the column loads (`name`), so a setting may name a column either way: its pins, masks, parses, `unnest` paths (each part) and settings. `exclude_columns` and `allow_columns` match the source's names, and `flatten` the yielded objects', so they stay as written.
pub(crate) fn loaded_overrides(
    overrides: &TableOverride,
    name: &dyn Fn(&str) -> String,
) -> TableOverride {
    TableOverride {
        settings: loaded_settings(&overrides.settings, name),
        columns: renamed_keys(&overrides.columns, &name),
        mask: renamed_keys(&overrides.mask, &name),
        parse: renamed_keys(&overrides.parse, &name),
        unnest: overrides
            .unnest
            .as_ref()
            .map(|unnest| loaded_unnest(unnest, &name)),
        ..overrides.clone()
    }
}

/// `settings` with every column they name as the column loads (`name`): the unique and replace keys, cursor, constant columns, newest_by columns, checks, and the bare column names among ClickHouse's `order_by`, `primary_key` and `partition_by`.
pub(crate) fn loaded_settings(
    settings: &TableSettings,
    name: &dyn Fn(&str) -> String,
) -> TableSettings {
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
        newest_by: settings.newest_by.as_ref().map(names),
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
fn loaded_unnest(unnest: &PathsSetting, name: &impl Fn(&str) -> String) -> PathsSetting {
    let mut loaded = unnest.clone();
    if let PathsSetting::Paths(paths) = &mut loaded {
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

/// The names `function`, a `rename` function `@sl.rename` registered, gives `columns` of table `qualified` of source `source_name`, by position; or why it gives none: the function is unknown, failed or returned something else, or no Python host runs it.
fn renamed_by(
    project: &ProjectConfig,
    (source_name, qualified): (&str, &str),
    function: &str,
    columns: &[RenameColumn],
) -> Result<BTreeMap<usize, String>, String> {
    let problem = |problem: String| format!("rename {}: {problem}", shown_rename(function));
    if let Some(Err(reason)) = project.renames.get(function) {
        return Err(problem(reason.clone()));
    }
    let Some(host) = &project.python_host else {
        return Err(NO_RENAME_HOST.replace("{place}", &format!("table {qualified}")));
    };
    host_rename(host.as_ref(), function, (source_name, qualified), columns)
}

/// The name a `rename` function is shown by: one a Python table passed as itself goes by its registry key, `name#id`, shown without the id.
fn shown_rename(function: &str) -> &str {
    match function.rsplit_once('#') {
        Some((shown, id)) if !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_hexdigit()) => {
            shown
        }
        _ => function,
    }
}

/// The names `host`'s `rename` function `function` gives `columns` of table `table` of source `source`, by position, or why it gives none.
pub(crate) fn host_rename(
    host: &dyn PythonHost,
    function: &str,
    (source, table): (&str, &str),
    columns: &[RenameColumn],
) -> Result<BTreeMap<usize, String>, String> {
    let problem = |problem: String| format!("rename {}: {problem}", shown_rename(function));
    let columns = serde_json::to_string(columns).map_err(|error| problem(error.to_string()))?;
    let context = serde_json::json!({ "source": source, "table": table }).to_string();
    let returned = host
        .rename(function, Some(&columns), &context)
        .map_err(problem)?;
    let by_text: BTreeMap<String, String> =
        serde_json::from_str(&returned).map_err(|error| problem(error.to_string()))?;
    by_text
        .into_iter()
        .map(|(position, name)| match position.parse::<usize>() {
            Ok(position) => Ok((position, name)),
            Err(_) => Err(problem(format!(
                "returned the key {position:?}, not a position"
            ))),
        })
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

/// `plan`, planned again when the table's run log holds the names numbering gave its columns in earlier loads and this plan's numbering names them otherwise, so each keeps its number (except under `--reload`, whose load records its own numbering over theirs); a number whose source column has gone, which another column would take, blocks the table instead, as does a clash on a column the table has that no record says which source column holds. A Python table, whose columns are named as they arrive, takes the names and the table's columns for its load, which refuses such a clash then.
pub(crate) async fn with_kept_names(
    project: &ProjectConfig,
    spec: TableSpec,
    mut plan: TablePlan,
    (destination, options): (&Destination, &PlanOptions),
) -> Result<TablePlan, LoaderError> {
    let existing: Vec<String> = match plan.exists && !options.reload {
        true => plan
            .live
            .iter()
            .flatten()
            .map(|column| column.name.clone())
            .collect(),
        false => Vec::new(),
    };
    plan.spec.name_rules.existing.clone_from(&existing);
    let clash = !existing.is_empty()
        && plan.schema.blocked.is_none()
        && !plan.spec.names.numbered.is_empty();
    let logged = kept_numbered(&plan, destination).await?;
    plan.spec.names.replaces_record =
        options.reload && logged.as_ref().is_some_and(|logged| !logged.is_empty());
    let kept = logged.filter(|_| !options.reload);
    let Some(kept) = kept.or_else(|| clash.then(Vec::new)) else {
        return Ok(plan);
    };
    plan.spec.names.kept.clone_from(&kept);
    plan.spec.name_rules.kept.clone_from(&kept);
    let Some(listed) = spec.listed.as_deref() else {
        return Ok(plan);
    };
    let known = KnownNames {
        numbered: kept,
        named: None,
        existing,
    };
    let resolved = resolve_known(
        project,
        &spec.source_name,
        std::slice::from_ref(listed),
        &known,
    );
    let kept_spec = match (
        resolved.problems.is_empty(),
        resolved.specs.into_iter().next(),
    ) {
        (true, Some(kept_spec)) => kept_spec,
        _ => return Ok(blocked_table(&spec, resolved.problems)),
    };
    if kept_spec.names.columns == spec.names.columns {
        return Ok(plan);
    }
    let kept_spec = TableSpec {
        shared_destination: spec.shared_destination,
        source_end: spec.source_end.clone(),
        files: spec.files.clone(),
        python: spec.python.clone(),
        ..kept_spec
    };
    Box::pin(plan_table(&kept_spec, destination, options)).await
}

/// Every source's tables, resolved before any is planned so each knows which others share its destination table, with the folders they claim; a source whose tables cannot be read is a problem in the plan. Each source's discovery is a step, timed from its connection when it opens one.
pub(crate) async fn found_sources<'a>(
    project: &'a ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<FoundSources<'a>, LoaderError> {
    let mut found = FoundSources::default();
    let mut drained_sources: Vec<(usize, DrainedFiles)> = Vec::new();
    for (source_name, source) in &project.sources {
        begin_step(source_name, "listing tables");
        let scoped = scoped_tables((project, options), (source_name, source)).await;
        end_step(match &scoped {
            Ok(Ok((resolved, _))) => StepEnd::Discovered {
                tables: Some(resolved.specs.len()),
            },
            Ok(Err(None)) => StepEnd::Quiet,
            Ok(Err(Some(_))) | Err(_) => StepEnd::Discovered { tables: None },
        });
        let (mut resolved, files) = match scoped? {
            Ok(tables) => tables,
            Err(problem) => {
                found.unresolved.push(source_name.clone());
                if let Some(problem) = problem {
                    found.plan.unread_sources.insert(
                        source_name.clone(),
                        unread_source(project, (source_name, source)),
                    );
                    if problem.retryable {
                        found.plan.retryable_problems.push(problem.message.clone());
                    }
                    found.plan.failed_sources.insert(source_name.clone());
                    found.plan.problems.push(problem.message);
                }
                continue;
            }
        };
        if let Some(drained) = resolved.drained.take() {
            drained_sources.push((found.sources.len(), drained));
        }
        found
            .claims
            .extend(folder_claims(&resolved.specs, files.as_ref(), destinations));
        found.configured.extend(resolved.specs.iter().cloned());
        found.sources.push(FoundSource {
            name: source_name,
            source,
            resolved,
            files,
        });
    }
    let mut drained_specs: Vec<TableSpec> = Vec::new();
    for (index, drained) in drained_sources {
        let source = &mut found.sources[index];
        let shared = found
            .configured
            .iter()
            .any(|spec| spec.target == drained.target && spec.destination == drained.table);
        if shared {
            found.unresolved.push(source.name.clone());
        }
        found.drained.extend([
            drained.table.table.clone(),
            drained.skipped.destination.clone(),
            source.name.clone(),
        ]);
        drained_specs.extend(drained.spec);
        if !drained.in_scope {
            continue;
        }
        let exists = match destinations.get(&drained.target) {
            Some(destination) => live_columns(destination, &drained.table).await?.is_some(),
            None => false,
        };
        let path = source.source.path.as_deref().unwrap_or_default();
        let proves_folders = matches!(
            project.connections.get(&source.source.connection),
            Some(ConnectionConfig::Filesystem { .. })
        );
        let problem = match (exists, shared, options.reload) {
            (false, _, _) if source.source.on_empty == OnEmpty::Fail => Some(drained.problem),
            (false, _, _) if !proves_folders => Some(format!(
                "[sources.{}]: no files to load in {path}, and {} was never loaded: an object store or SFTP server lists a folder that doesn't exist as empty, so on_empty = \"skip\" skips one only once its table exists. Fix: check the source's path and connection, or load once files arrive",
                source.name, drained.table.table
            )),
            (_, true, _) => Some(format!(
                "[sources.{}]: no files to load in {path}, and other tables load into {} too: a load that cannot see this source could misread or remove its rows, so it fails until files come; run the others with --exclude {}",
                source.name, drained.table.table, source.name
            )),
            (true, false, true) if source.source.after_load == AfterLoad::Delete => Some(format!(
                "[sources.{}]: --reload would rebuild the table from the files still in its folder, and after_load has deleted or moved the ones already loaded",
                source.name
            )),
            _ => None,
        };
        match problem {
            Some(problem) => source.resolved.problems.push(problem),
            None => source.resolved.skipped.push(drained.skipped),
        }
    }
    found.configured.extend(drained_specs);
    Ok(found)
}

/// `planned`, each table blocked whose `where` fails on source `source_name`: the table's read narrowed to no rows (`(<where>) AND (1 = 0)`, which a database answers without scanning the table) is counted, or dry-run on BigQuery, so nothing is billed; the source is opened only when an unblocked table has a `where`. Then, on an ODBC source, each table still unblocked whose cursor bounds its driver refuses (see `block_refused_bounds`).
pub(crate) async fn block_failing_reads(
    project: &ProjectConfig,
    (source_name, source): (&str, &SourceConfig),
    mut planned: Vec<TablePlan>,
) -> Result<Vec<TablePlan>, LoaderError> {
    let filtered =
        |table: &TablePlan| table.schema.blocked.is_none() && table.spec.row_filter.is_some();
    let reader = match planned.iter().any(filtered) {
        true => Some(crate::loader::_helpers::sources::open_source(project, source, 1).await?),
        false => None,
    };
    for table in planned.iter_mut().filter(|table| filtered(table)) {
        let spec = &table.spec;
        let filter = spec
            .read_filter(Some(crate::loader::constants::NO_ROWS.to_owned()))
            .unwrap_or_default();
        if let Some(error) = failed_count(reader.as_ref(), &spec.source_table, &filter).await {
            let row_filter = spec.row_filter.clone().unwrap_or_default();
            table.schema = blocked_plan(vec![format!(
                "where {row_filter:?} fails on the source: {error}"
            )]);
        }
    }
    #[cfg(feature = "odbc")]
    if source.kind == crate::project::models::SourceKind::Odbc {
        block_refused_bounds(project, (source_name, source), &mut planned).await?;
    }
    #[cfg(not(feature = "odbc"))]
    let _ = source_name;
    Ok(planned)
}

/// Why counting the rows of `table` that `filter` selects fails on `reader`'s database (a dry run on BigQuery), or `None` when it runs, or there is no database to ask.
async fn failed_count(
    reader: Option<&SourceReader>,
    table: &SourceTable,
    filter: &str,
) -> Option<String> {
    let failed = match reader? {
        SourceReader::BigQuery(bigquery) => {
            crate::bigquery_source::main::dry_count::dry_count(bigquery, (table, None), filter)
                .await
                .err()
                .map(LoaderError::from)
        }
        reader => count_rows(reader, (table, None), filter).await.err(),
    };
    failed.map(|error| error.to_string())
}

/// How many rows of `table` (or of its query `sql`) `filter` selects, counted by the source's database; `None` for a files or Google Sheets source, which has no database to ask.
pub(crate) async fn count_rows(
    reader: &SourceReader,
    (table, sql): (&SourceTable, Option<&str>),
    filter: &str,
) -> Result<Option<u64>, LoaderError> {
    Ok(Some(match reader {
        SourceReader::Postgres(client, _) => {
            crate::postgres_source::main::count_rows::count_rows(client, table, filter).await?
        }
        SourceReader::Snowflake(snowflake) => {
            crate::snowflake_source::main::count_rows::count_rows(snowflake, (table, sql), filter)
                .await?
        }
        SourceReader::BigQuery(bigquery) => {
            crate::bigquery_source::main::count_rows::count_rows(bigquery, (table, sql), filter)
                .await?
        }
        SourceReader::Databricks(databricks) => {
            crate::databricks_source::main::count_rows::count_rows(databricks, (table, sql), filter)
                .await?
        }
        #[cfg(feature = "odbc")]
        SourceReader::Odbc(odbc) => {
            crate::odbc_source::main::count_rows::count_rows(odbc, (table, sql), filter).await?
        }
        #[cfg(feature = "duckdb")]
        SourceReader::DuckDb(duckdb) => {
            crate::duckdb_source::main::count_rows::count_rows(duckdb, (table, sql), filter).await?
        }
        SourceReader::Files(_) | SourceReader::Sheets(..) => return Ok(None),
    }))
}
