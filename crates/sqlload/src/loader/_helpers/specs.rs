use crate::adapters::contract::models::TableName;
use crate::column_types::main::parse_type::parse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::loader::_helpers::resolve::ResolvedTarget;
use crate::loader::models::{CursorSpec, SettingOrigin, TableSpec};
use crate::masking::models::MaskRules;
use crate::postgres_source::models::SourceTable;
use crate::project::models::{
    MaskMethod, SourceConfig, SourceKind, StateMode, TableOverride, TableSettings, WriteStrategy,
};
use crate::similar_names::main::nearest_name::nearest_name;
use globset::{Glob, GlobMatcher, GlobSet, GlobSetBuilder};
use std::collections::BTreeMap;

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
    let batch = layered(&settings.batch_size, &defaults.batch_size).map(|(value, _)| value);
    let uses_key = strategy == WriteStrategy::Merge;
    problems.extend(overrides.mask.keys().filter_map(|masked| {
        let is_cursor = cursor.as_ref().is_some_and(|cursor| cursor.column == *masked);
        let is_key = uses_key && unique_key.contains(masked);
        (is_cursor || is_key).then(|| {
            format!("mask {masked}: a masked column cannot be the cursor_column or part of the unique_key")
        })
    }));
    if context.source.mask_key.is_none()
        && overrides
            .mask
            .values()
            .any(|method| *method == MaskMethod::Hmac)
    {
        problems.push("mask uses \"hmac\" but the source sets no mask_key (for example mask_key = \"${ENV:MASK_KEY}\")".to_owned());
    }
    let files_description = match context.source.kind {
        SourceKind::Files => context.source.description.as_deref(),
        _ => None,
    };
    let description = overrides
        .description
        .as_deref()
        .or(files_description)
        .or(table.comment.as_deref())
        .map(str::trim)
        .filter(|text| !text.is_empty());
    if !overrides.flatten.is_empty() && context.source.kind != SourceKind::Python {
        problems.push(
            "flatten applies to Python tables; nested values from this source stay JSON".to_owned(),
        );
    }
    if !problems.is_empty() {
        return Err(problems);
    }
    let template = overrides
        .name
        .as_deref()
        .unwrap_or(context.source.table_name_template());
    let name = match (&overrides.name, template.contains('{')) {
        (Some(name), false) => name.clone(),
        _ => destination_name(template, context.name, table),
    };
    Ok(TableSpec {
        source_name: context.name.to_owned(),
        source_table: table.clone(),
        target: context.target.name.clone(),
        destination: TableName {
            database: context.target.database.clone(),
            schema: context.target.schema.clone(),
            table: name,
        },
        description: description.unwrap_or_default().to_owned(),
        warehouse: context.target.warehouse,
        strategy,
        strategy_origin,
        unique_key,
        cursor,
        batch,
        #[cfg(feature = "container-tests")]
        test_loader_route: layered(&settings.test_loader_route, &defaults.test_loader_route)
            .is_some_and(|(value, _)| value),
        #[cfg(not(feature = "container-tests"))]
        test_loader_route: false,
        source_end: None,
        pinned: columns.pinned,
        excluded_columns: columns.excluded,
        masked: MaskRules {
            columns: overrides.mask.clone(),
            key: context.source.mask_key.clone(),
        },
        contract: layered(&settings.contract, &defaults.contract)
            .map(|(value, _)| value)
            .unwrap_or_default(),
        on_schema_change: layered(&settings.on_schema_change, &defaults.on_schema_change)
            .map(|(value, _)| value)
            .unwrap_or_default(),
        auto_widen: layered(&settings.auto_widen, &defaults.auto_widen)
            .is_some_and(|(value, _)| value),
        type_change_rebuild: layered(&settings.type_change_rebuild, &defaults.type_change_rebuild)
            .map(|(value, _)| value)
            .unwrap_or_default(),
        flatten: overrides.flatten.clone(),
        allow_empty: layered(&settings.allow_empty, &defaults.allow_empty)
            .is_some_and(|(value, _)| value),
        logged: layered(&settings.state, &defaults.state)
            .is_some_and(|(value, _)| value == StateMode::Logged),
        approved: false,
        new_tables: context.source.new_tables,
        files: None,
        destination_path: context.target.path.as_ref().map(|target_path| {
            overrides
                .destination_path
                .clone()
                .or_else(|| context.source.destination_path.clone())
                .unwrap_or_else(|| target_path.clone())
        }),
        python: None,
    })
}

/// Why a table someone wrote is blocked without a description they wrote, with where to give it; `None` when it has one, or when it was discovered (matched by an `include` pattern, not named), which takes the source's own table comment or none (D203). Python tables, files sources (described by the source's own `description`), `[sources.*.tables]` entries and tables named exactly in `include` are written.
pub(crate) fn missing_description(
    source_name: &str,
    source: &SourceConfig,
    table: &SourceTable,
) -> Option<String> {
    let qualified = table.qualified_name();
    let entry = source.tables.get(&qualified);
    let written = source.kind != SourceKind::Postgres
        || entry.is_some()
        || source.include.contains(&qualified);
    let described = [
        entry.and_then(|entry| entry.description.as_deref()),
        source
            .description
            .as_deref()
            .filter(|_| source.kind == SourceKind::Files),
    ]
    .into_iter()
    .flatten()
    .any(|text| !text.trim().is_empty());
    let fix = match source.kind {
        SourceKind::Python => {
            "no description; add description=\"...\" to its table declaration, or a docstring to its function".to_owned()
        }
        SourceKind::Files => {
            format!("no description; add description = \"...\" under [sources.{source_name}]")
        }
        SourceKind::Postgres => format!(
            "no description; add description = \"...\" under [sources.{source_name}.tables.\"{qualified}\"]"
        ),
    };
    (written && !described).then_some(fix)
}

fn glob(pattern: &str) -> Result<Glob, String> {
    Glob::new(pattern).map_err(|error| format!("invalid column pattern {pattern:?}: {error}"))
}

/// A matcher for column name patterns such as `*email*`.
fn column_matcher(patterns: &[String]) -> Result<GlobSet, String> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(glob(pattern)?);
    }
    builder.build().map_err(|error| error.to_string())
}

/// Column selection rules for one table: the source's lists plus the table's.
#[derive(Debug, Default)]
pub(crate) struct ColumnRules {
    pub(crate) exclude: Vec<String>,
    pub(crate) allow: Vec<String>,
    pub(crate) skip_tables_with: Vec<String>,
}

/// What the column rules decide for one table.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ColumnSelection {
    /// Load the table without these columns, each with the rule that dropped it.
    Keep(Vec<(String, String)>),
    /// Do not load the table, for this reason.
    Skip(String),
}

/// Apply `rules` to `table`'s columns.
pub(crate) fn apply_column_rules(
    table: &SourceTable,
    rules: &ColumnRules,
) -> Result<ColumnSelection, String> {
    let allow = column_matcher(&rules.allow)?;
    let skip = column_matcher(&rules.skip_tables_with)?;
    let exclude: Vec<(&String, GlobMatcher)> = rules
        .exclude
        .iter()
        .map(|pattern| Ok((pattern, glob(pattern)?.compile_matcher())))
        .collect::<Result<_, String>>()?;
    let names: Vec<&str> = table
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .filter(|name| !allow.is_match(name))
        .collect();
    if let Some(column) = names.iter().find(|name| skip.is_match(name)) {
        return Ok(ColumnSelection::Skip(format!(
            "has column {column}, which matches skip_tables_with_columns"
        )));
    }
    let excluded: Vec<(String, String)> = names
        .iter()
        .filter_map(|name| excluded_by(name, &exclude))
        .collect();
    Ok(ColumnSelection::Keep(excluded))
}

/// The column and the `exclude_columns` pattern matching it, if one does.
fn excluded_by(name: &str, exclude: &[(&String, GlobMatcher)]) -> Option<(String, String)> {
    let (pattern, _) = exclude.iter().find(|(_, matcher)| matcher.is_match(name))?;
    Some((name.to_owned(), format!("exclude_columns {pattern}")))
}

/// A destination table name from `template` with `{source}`, `{schema}` and `{table}` filled in.
pub(crate) fn destination_name(template: &str, source: &str, table: &SourceTable) -> String {
    template
        .replace("{source}", source)
        .replace("{schema}", &table.schema)
        .replace("{table}", &table.name)
        .to_ascii_lowercase()
}

/// A table's value if set, otherwise its source's.
pub(crate) fn layered<T: Clone>(
    table: &Option<T>,
    source: &Option<T>,
) -> Option<(T, SettingOrigin)> {
    match (table, source) {
        (Some(value), _) => Some((value.clone(), SettingOrigin::Table)),
        (None, Some(value)) => Some((value.clone(), SettingOrigin::Source)),
        (None, None) => None,
    }
}

/// The cursor a table's settings describe, if any.
pub(crate) fn cursor(table: &TableSettings, source: &TableSettings) -> Option<CursorSpec> {
    let (column, _) = layered(&table.cursor_column, &source.cursor_column)?;
    Some(CursorSpec {
        column,
        start: layered(&table.cursor_start, &source.cursor_start).map(|(value, _)| value),
        lookback: layered(&table.lookback, &source.lookback).map(|(value, _)| value),
        inclusive: layered(
            &table.append_cursor_inclusive,
            &source.append_cursor_inclusive,
        )
        .is_none_or(|(value, _)| value),
    })
}

/// The default strategy: full replace without a cursor, merge with a cursor and key, delete_insert with a cursor alone.
pub(crate) fn default_strategy(cursor: Option<&CursorSpec>, has_key: bool) -> WriteStrategy {
    match (cursor, has_key) {
        (None, _) => WriteStrategy::Replace,
        (Some(_), true) => WriteStrategy::Merge,
        (Some(_), false) => WriteStrategy::DeleteInsert,
    }
}

/// Problems with a strategy and the keys and cursor it is given.
pub(crate) fn strategy_problems(
    strategy: WriteStrategy,
    unique_key: &[String],
    cursor: Option<&CursorSpec>,
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    match (strategy, cursor) {
        (WriteStrategy::Merge, _) if unique_key.is_empty() => problems.push(
            "write_strategy = \"merge\" needs a unique_key (the table has no primary key)".to_owned(),
        ),
        (WriteStrategy::Append, Some(cursor)) if cursor.lookback.is_some() => problems.push(
            "lookback re-inserts the window on every run with append; use delete_insert (no key needed) or merge"
                .to_owned(),
        ),
        (WriteStrategy::Replace, Some(_)) => problems.push(
            "write_strategy = \"replace\" reloads the whole table, so it takes no cursor_column".to_owned(),
        ),
        (WriteStrategy::DeleteInsert, None) => problems.push(
            "write_strategy = \"delete_insert\" replaces a cursor range, so it needs a cursor_column".to_owned(),
        ),
        _ => {}
    }
    problems
}
