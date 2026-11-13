use crate::adapters::contract::models::{ChangeApply, TableName};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::bigquery_source::constants::BIGQUERY_SOURCE;
use crate::cdc::constants::{
    BATCH_INTERVAL, CDC_FILES_PROBLEM, CDC_TARGETS_PROBLEM, FILES_BATCH_INTERVAL,
};
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::main::parse_type::parse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::files_source::main::constant_columns::constant_columns;
use crate::files_source::models::AfterLoad;
use crate::load_naming::models::Naming;
use crate::loader::_helpers::resolve::ResolvedTarget;
use crate::loader::_helpers::sources::sheet_rule;
use crate::loader::constants::{DEFAULT_MAX_DEPTH, ODBC_SOURCE, SNOWFLAKE_SOURCE, UNNEST_ALL};
use crate::loader::models::{
    CdcSpec, CursorSpec, SettingOrigin, TableLayout, TableSpec, WarehouseRoute, WarehouseSource,
};
use crate::masking::models::MaskRules;
use crate::postgres_source::models::SourceTable;
use crate::project::models::{
    CdcDeletes, CdcMode, Contract, LookbackFrom, MaskMethod, OnDuplicateKeys, SortDirection,
    SourceConfig, SourceKind, StateMode, TableOverride, TableSettings, UnloadStage, UnnestSetting,
    WriteStrategy,
};
use crate::publish_checks::main::check_problems::check_problems;
use crate::routing::models::Store;
use crate::row_batches::models::Unnest;
use crate::sheets_source::models::SheetRead;
use crate::similar_names::main::nearest_name::nearest_name;
use crate::snowflake_source::constants::USER_STAGE_FOLDER;
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

/// The table's pinned column types, parsed for `warehouse`; a type the warehouse cannot load yet is a problem, unless `mask` makes the column text.
pub(crate) fn pinned_types(
    overrides: &TableOverride,
    warehouse: Warehouse,
) -> Result<BTreeMap<String, ColumnType>, Vec<String>> {
    let mut pinned: BTreeMap<String, ColumnType> = BTreeMap::new();
    let mut problems: Vec<String> = Vec::new();
    for (column, text) in &overrides.columns {
        match parse_type(text, &[warehouse]) {
            Ok(parsed) => {
                problems.extend(
                    sql_adapter(warehouse)
                        .type_problem(&parsed.column_type)
                        .filter(|_| !overrides.mask.contains_key(column))
                        .map(|problem| format!("column {column}: {problem}")),
                );
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
    let cdc = cdc_spec(context);
    let (strategy, mut problems) = match &cdc {
        Some(cdc) if !sql_adapter(context.target.warehouse).captures_changes(cdc.apply) => (
            WriteStrategy::Append,
            vec![
                match sql_adapter(context.target.warehouse).runs_sql() {
                    true => CDC_TARGETS_PROBLEM,
                    false => CDC_FILES_PROBLEM,
                }
                .to_owned(),
            ],
        ),
        Some(_) => (WriteStrategy::Append, cdc_key_problems(table, &unique_key)),
        None => (
            strategy,
            strategy_problems(strategy, &unique_key, cursor.as_ref()),
        ),
    };
    problems.extend(column_reference_problems(table, overrides));
    let constants = match table_constants(context, table, (settings, defaults), &columns.excluded) {
        Ok(constants) => constants,
        Err(found) => {
            problems.extend(found);
            Vec::new()
        }
    };
    problems.extend(
        constants
            .iter()
            .filter(|(name, _, _)| {
                context.source.kind != SourceKind::Files
                    && cursor
                        .as_ref()
                        .is_some_and(|cursor| cursor.column.eq_ignore_ascii_case(name))
            })
            .map(|(name, _, _)| {
                format!("constant column {name} cannot be the cursor_column: it holds one value")
            }),
    );
    let merges = strategy == WriteStrategy::Merge;
    let table_sorts = settings.dedup_sort.is_some();
    let on_duplicate_keys = layered(&settings.on_duplicate_keys, &defaults.on_duplicate_keys)
        .filter(|(_, origin)| *origin == SettingOrigin::Table || (merges && !table_sorts));
    let table_fails = on_duplicate_keys == Some((OnDuplicateKeys::Fail, SettingOrigin::Table));
    let dedup_sort = layered(&settings.dedup_sort, &defaults.dedup_sort)
        .filter(|(_, origin)| *origin == SettingOrigin::Table || (merges && !table_fails))
        .map(|(value, _)| value);
    let (dedup_sort, found) = dedupe_settings(
        strategy,
        &unique_key,
        (
            on_duplicate_keys.map(|(value, _)| value),
            dedup_sort.as_ref(),
        ),
        overrides,
    );
    problems.extend(found);
    let checks = layered(&settings.checks, &defaults.checks).map(|(value, _)| value);
    problems.extend(
        checks
            .iter()
            .flat_map(|checks| check_problems(checks, (strategy, context.target.path.is_some()))),
    );
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
        SourceKind::Files | SourceKind::GoogleSheets => context.source.description.as_deref(),
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
    problems.extend(flatten_problems(context, overrides));
    let contract = layered(&settings.contract, &defaults.contract)
        .map(|(value, _)| value)
        .unwrap_or_default();
    let (nesting, found) = nesting(context, overrides, (strategy, contract));
    problems.extend(found);
    problems.extend(after_load_problems(context, strategy));
    let layout = table_layout(settings, defaults);
    problems.extend(sql_adapter(context.target.warehouse).layout_problems(&layout));
    if !problems.is_empty() {
        return Err(problems);
    }
    let name = table_name(
        (context.name, context.source),
        overrides.name.as_deref(),
        table,
    );
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
        naming: nesting.naming,
        on_duplicate_keys: on_duplicate_keys
            .map(|(value, _)| value)
            .unwrap_or_default(),
        dedup_sort,
        cursor,
        batch,
        constants,
        shared_destination: false,
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
        contract,
        on_schema_change: layered(&settings.on_schema_change, &defaults.on_schema_change)
            .map(|(value, _)| value)
            .unwrap_or_default(),
        auto_widen: layered(&settings.auto_widen, &defaults.auto_widen)
            .is_some_and(|(value, _)| value),
        type_change_rebuild: layered(&settings.type_change_rebuild, &defaults.type_change_rebuild)
            .map(|(value, _)| value)
            .unwrap_or_default(),
        flatten: overrides.flatten.clone(),
        row_id: nesting.row_id,
        unnest: nesting.unnest,
        max_depth: nesting.max_depth,
        allow_empty: layered(&settings.allow_empty, &defaults.allow_empty)
            .is_some_and(|(value, _)| value),
        logged: layered(&settings.state, &defaults.state)
            .is_some_and(|(value, _)| value == StateMode::Logged),
        after_load: context.source.after_load.clone(),
        approved: false,
        tunnel: None,
        new_tables: context.source.new_tables,
        checks,
        files: None,
        destination_path: context.target.path.as_ref().map(|target_path| {
            overrides
                .destination_path
                .clone()
                .or_else(|| context.source.destination_path.clone())
                .unwrap_or_else(|| target_path.clone())
        }),
        python: None,
        sheet: (context.source.kind == SourceKind::GoogleSheets).then(|| SheetRead {
            range: context.source.ranges.get(&table.name).cloned(),
            rule: sheet_rule(context.source, &table.name),
        }),
        cdc,
        warehouse_source: match context.source.kind {
            SourceKind::Odbc => Some(WarehouseSource {
                system: ODBC_SOURCE,
                sql: overrides.sql.clone(),
                route: WarehouseRoute::Query,
            }),
            SourceKind::BigQuery => Some(WarehouseSource {
                system: BIGQUERY_SOURCE,
                sql: overrides.sql.clone(),
                route: WarehouseRoute::Query,
            }),
            _ => warehouse_source(context.source, overrides),
        },
        layout,
    })
}

/// The table's `constant_columns` (its own map, else its source's), each with its type for the target and its value as text; or why they cannot load: a name the source table already has among its loaded columns, ignoring case, a type with no Arrow type or one the target cannot load, or an array. A files source's are checked as before by its reader, which fills them and replaces a file column of the same name (`plan` notes it).
fn table_constants(
    context: &SourceContext<'_>,
    table: &SourceTable,
    (settings, defaults): (&TableSettings, &TableSettings),
    excluded: &[(String, String)],
) -> Result<Vec<(String, ColumnType, String)>, Vec<String>> {
    let Some((written, _)) = layered(&settings.constant_columns, &defaults.constant_columns) else {
        return Ok(Vec::new());
    };
    let warehouse = context.target.warehouse;
    let constants = constant_columns(&written.0, &[warehouse]).map_err(|problem| vec![problem])?;
    let problems: Vec<String> = match context.source.kind {
        SourceKind::Files => Vec::new(),
        _ => table
            .columns
            .iter()
            .filter(|column| !is_excluded(excluded, &column.name))
            .filter_map(|column| clash(&constants, &column.name))
            .chain(
                constants
                    .iter()
                    .filter_map(|constant| constant_type_problem(constant, warehouse)),
            )
            .collect(),
    };
    match problems.is_empty() {
        true => Ok(constants),
        false => Err(problems),
    }
}

/// Whether `excluded` drops column `name`.
fn is_excluded(excluded: &[(String, String)], name: &str) -> bool {
    excluded.iter().any(|(excluded, _)| excluded == name)
}

/// The problem with source column `column` when one of `constants` has its name, ignoring case.
fn clash(constants: &[(String, ColumnType, String)], column: &str) -> Option<String> {
    constants
        .iter()
        .find(|(name, _, _)| name.eq_ignore_ascii_case(column))
        .map(|(name, _, _)| {
            format!(
                "constant column {name}: the source already has a column {column}, and a constant never replaces it; rename the constant, or exclude the source's column"
            )
        })
}

/// Why a constant of this type cannot load into `warehouse`: an array, a type with no Arrow type, or one the warehouse cannot load.
fn constant_type_problem(
    (name, column_type, _): &(String, ColumnType, String),
    warehouse: Warehouse,
) -> Option<String> {
    match (column_type, arrow_field(name, column_type)) {
        (ColumnType::Array(_), _) => Some(format!(
            "constant column {name}: an array cannot be a constant"
        )),
        (_, None) => Some(format!(
            "constant column {name}: {column_type} has no Arrow type, so a constant cannot be one; pin a portable type"
        )),
        _ => sql_adapter(warehouse)
            .type_problem(column_type)
            .map(|problem| format!("constant column {name}: {problem}")),
    }
}

/// How a `snowflake` source's table is read: unloaded to the source's `stage`, or to the user stage when it sets none.
fn warehouse_source(source: &SourceConfig, overrides: &TableOverride) -> Option<WarehouseSource> {
    (source.kind == SourceKind::Snowflake).then(|| {
        let (store, stage) = match &source.stage {
            Some(UnloadStage {
                url: Some(url),
                storage_integration: Some(integration),
                ..
            }) => (
                Store::ObjectStore(url.clone()),
                format!(
                    "external: Snowflake writes to {url} through storage integration {integration}; the files are read there and removed after the read"
                ),
            ),
            Some(UnloadStage {
                name: Some(name), ..
            }) => (
                Store::ObjectStore(format!("@{name}")),
                format!(
                    "stage {name}: Snowflake writes the files there, and SQLLoad reads them and removes them after the read"
                ),
            ),
            _ => (
                Store::InternalStage(USER_STAGE_FOLDER.to_owned()),
                "internal: no stage is set, so Snowflake writes to your user stage, and SQLLoad fetches the files in parallel, decrypts them and removes them after the read".to_owned(),
            ),
        };
        WarehouseSource {
            system: SNOWFLAKE_SOURCE,
            sql: overrides.sql.clone(),
            route: WarehouseRoute::Unload { store, stage },
        }
    })
}

/// The table's layout, each field layered table over source; `table_settings` merged, the table's value winning per setting.
fn table_layout(table: &TableSettings, source: &TableSettings) -> TableLayout {
    let mut table_settings = source.table_settings.clone().unwrap_or_default();
    table_settings.extend(table.table_settings.clone().unwrap_or_default());
    TableLayout {
        engine: layered(&table.engine, &source.engine).map(|(value, _)| value),
        order_by: layered(&table.order_by, &source.order_by).map(|(value, _)| value),
        partition_by: layered(&table.partition_by, &source.partition_by).map(|(value, _)| value),
        primary_key: layered(&table.primary_key, &source.primary_key).map(|(value, _)| value),
        table_settings,
    }
}

/// A `postgres_cdc` source's stream settings, with the default slot and publication names.
fn cdc_spec(context: &SourceContext<'_>) -> Option<CdcSpec> {
    let source = context.source;
    (source.kind == SourceKind::PostgresCdc).then(|| {
        let default_name = format!("sqlload_{}", context.name.to_lowercase());
        CdcSpec {
            apply: match (source.cdc_mode, source.deletes) {
                (CdcMode::Changelog, _) => ChangeApply::Changelog,
                (CdcMode::Merge, CdcDeletes::Soft) => ChangeApply::SoftDeletes,
                (CdcMode::Merge, CdcDeletes::Hard) => ChangeApply::HardDeletes,
            },
            slot: source.slot.clone().unwrap_or_else(|| default_name.clone()),
            publication: source.publication.clone().unwrap_or(default_name),
            batch_interval: source.batch_interval.unwrap_or(
                match sql_adapter(context.target.warehouse).runs_sql() {
                    true => BATCH_INTERVAL,
                    false => FILES_BATCH_INTERVAL,
                },
            ),
        }
    })
}

/// Why a table cannot be captured: it needs a primary key, which merge matches rows on, and without which publishing it would make Postgres refuse the application's own updates and deletes on it; and its `unique_key` must be within that key, the only columns Postgres sends with a delete by default.
fn cdc_key_problems(table: &SourceTable, unique_key: &[String]) -> Vec<String> {
    let qualified = table.qualified_name();
    if table.primary_key.is_empty() {
        return vec![format!(
            "{qualified} has no primary key; kind = \"postgres_cdc\" captures tables with one in this release (publishing a table without a replica identity makes Postgres refuse its updates and deletes); add a primary key, or exclude it"
        )];
    }
    let outside: Vec<&str> = unique_key
        .iter()
        .filter(|column| !table.primary_key.contains(column))
        .map(String::as_str)
        .collect();
    match outside.is_empty() {
        true => Vec::new(),
        false => vec![format!(
            "{qualified}: unique_key column {} is not in the primary key, and Postgres sends only the primary key with a delete, so deletes could not find their rows; kind = \"postgres_cdc\" merges on the primary key in this release, so drop unique_key",
            outside.join(", ")
        )],
    }
}

/// Why a table someone wrote is blocked without a description they wrote, with where to give it; `None` when it has one, or when it was discovered (matched by an `include` pattern, not named), which takes the source's own table comment or none (D203). Python tables, files sources (described by the source's own `description`), `[sources.*.tables]` entries and tables named exactly in `include` are written.
pub(crate) fn missing_description(
    source_name: &str,
    source: &SourceConfig,
    table: &SourceTable,
) -> Option<String> {
    let qualified = table.qualified_name();
    let entry = source.tables.get(&qualified);
    let written =
        !source.discovers_tables() || entry.is_some() || source.include.contains(&qualified);
    let described = [
        entry.and_then(|entry| entry.description.as_deref()),
        source
            .description
            .as_deref()
            .filter(|_| matches!(source.kind, SourceKind::Files | SourceKind::GoogleSheets)),
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
        SourceKind::GoogleSheets => format!(
            "no description; add description = \"...\" under [sources.{source_name}.tables.{qualified}], or under [sources.{source_name}] for every table"
        ),
        SourceKind::Postgres
        | SourceKind::PostgresCdc
        | SourceKind::Snowflake
        | SourceKind::Odbc
        | SourceKind::BigQuery => format!(
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

/// Listed objects that are paths rather than top-level names, or that another column setting names: after flattening, no column carries the object's own name, so an exclusion, mask or pin on it would silently stop applying.
fn flatten_problems(context: &SourceContext<'_>, overrides: &TableOverride) -> Vec<String> {
    let exclude: Vec<String> = [
        context.source.exclude_columns.clone(),
        overrides.exclude_columns.clone(),
    ]
    .concat();
    overrides
        .flatten
        .iter()
        .filter_map(|object| {
            let excluded = excluding_pattern(object, &exclude);
            let setting = match (excluded, overrides.mask.contains_key(object), overrides.columns.contains_key(object)) {
                _ if object.contains('.') => {
                    return Some(format!(
                        "flatten {object}: flatten names top-level objects, not paths; list {} instead (its nested values stay JSON)",
                        object.split('.').next().unwrap_or_default()
                    ));
                }
                (Some(pattern), _, _) => format!("exclude_columns {pattern}"),
                (None, true, _) => "mask".to_owned(),
                (None, false, true) => "columns".to_owned(),
                (None, false, false) => return None,
            };
            Some(format!(
                "flatten {object}: {setting} also names {object}, which flattening replaces with {object}__<key> columns; name those columns instead, or leave {object} unflattened"
            ))
        })
        .collect()
}

/// The first `exclude_columns` pattern matching the object `name` but not its `name__<key>` columns, which would stop applying once it is flattened.
fn excluding_pattern<'a>(name: &str, exclude: &'a [String]) -> Option<&'a String> {
    let keys = format!("{name}__");
    exclude.iter().find(|pattern| {
        glob(pattern).is_ok_and(|glob| {
            let matcher = glob.compile_matcher();
            matcher.is_match(name) && !matcher.is_match(&keys)
        })
    })
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

/// The destination table name of `table` from source `source_name`: `name` (its `name` setting) when it has no placeholder, else that template (or the source's) filled in.
pub(crate) fn table_name(
    (source_name, source): (&str, &SourceConfig),
    name: Option<&str>,
    table: &SourceTable,
) -> String {
    match name {
        Some(name) if !name.contains('{') => name.to_owned(),
        _ => destination_name(
            name.unwrap_or(source.table_name_template()),
            source_name,
            table,
        ),
    }
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
        lookahead: layered(&table.lookahead, &source.lookahead).map(|(value, _)| value),
        from_now: layered(&table.lookback_from, &source.lookback_from)
            .is_some_and(|(value, _)| value == LookbackFrom::Now),
        inclusive: layered(
            &table.append_cursor_inclusive,
            &source.append_cursor_inclusive,
        )
        .is_some_and(|(value, _)| value),
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

/// The `dedup_sort` column and direction of a merge, and the problems with `on_duplicate_keys` and `dedup_sort` (a source's apply only to its merge tables, and either one set on a table drops the other from its source): both are for `merge`, `dedup_sort` names one column that is neither in the key nor masked, and has nothing to pick under `on_duplicate_keys = "fail"`.
fn dedupe_settings(
    strategy: WriteStrategy,
    unique_key: &[String],
    (on_duplicate_keys, dedup_sort): (
        Option<OnDuplicateKeys>,
        Option<&BTreeMap<String, SortDirection>>,
    ),
    overrides: &TableOverride,
) -> (Option<(String, SortDirection)>, Vec<String>) {
    let mut problems: Vec<String> = Vec::new();
    if strategy != WriteStrategy::Merge {
        problems.extend(
            [
                on_duplicate_keys.map(|_| "on_duplicate_keys"),
                dedup_sort.map(|_| "dedup_sort"),
            ]
            .into_iter()
            .flatten()
            .map(|name| format!("{name} is for write_strategy = \"merge\", which matches rows on unique_key; {strategy} keeps every row")),
        );
        return (None, problems);
    }
    let Some(sort) = dedup_sort else {
        return (None, problems);
    };
    let mut entries = sort.iter();
    let (column, direction) = match (entries.next(), entries.next()) {
        (Some((column, direction)), None) => (column.clone(), *direction),
        _ => {
            problems.push(
                "dedup_sort names one column and its direction, such as { updated_at = \"desc\" }"
                    .to_owned(),
            );
            return (None, problems);
        }
    };
    if unique_key
        .iter()
        .any(|key| key.eq_ignore_ascii_case(&column))
    {
        problems.push(format!("dedup_sort column {column} is in the unique_key, so it is the same in every row it would rank; name another column"));
    }
    if overrides.mask.contains_key(&column) {
        problems.push(format!(
            "dedup_sort column {column} is masked, so its values cannot rank rows"
        ));
    }
    if on_duplicate_keys == Some(OnDuplicateKeys::Fail) {
        problems.push("dedup_sort picks which repeated row a merge keeps, but on_duplicate_keys = \"fail\" keeps none; set one of them".to_owned());
    }
    (Some((column, direction)), problems)
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
    if let Some(cursor) = cursor.filter(|cursor| cursor.from_now && cursor.lookback.is_none()) {
        problems.push(format!(
            "lookback_from = \"now\" counts lookback back from now, so {} needs a lookback",
            cursor.column
        ));
    }
    problems
}

/// Why a files source's `after_load` cannot apply to its table: only `append` and `merge` never read a loaded file again, and a files target commits its own way.
fn after_load_problems(context: &SourceContext<'_>, strategy: WriteStrategy) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if context.source.after_load == AfterLoad::Keep {
        return problems;
    }
    if !matches!(strategy, WriteStrategy::Append | WriteStrategy::Merge) {
        problems.push(format!(
            "after_load deletes or moves each file once it loads, so it needs write_strategy = \"append\" or \"merge\"; {strategy} reads loaded files again"
        ));
    }
    if context.target.path.is_some() {
        problems.push("after_load is for database targets so far, not a files target".to_owned());
    }
    problems
}

/// What a table's nesting settings come to.
struct Nesting {
    naming: Naming,
    row_id: bool,
    unnest: Unnest,
    max_depth: u32,
}

/// The table's naming (its own, its source's, then its target's), whether its rows get an id (always under dlt's naming), its `unnest` and `max_depth`, and the problems with them: `unnest` names list fields by dotted paths whose ancestors it also names, no deeper than `max_depth`, that no column setting names, on a table whose writes can carry child tables.
fn nesting(
    context: &SourceContext<'_>,
    overrides: &TableOverride,
    (strategy, contract): (WriteStrategy, Contract),
) -> (Nesting, Vec<String>) {
    let settings = &overrides.settings;
    let defaults = &context.source.defaults;
    let naming = layered(&settings.naming, &defaults.naming)
        .map(|(value, _)| value)
        .or(context.target.naming)
        .unwrap_or_default();
    let row_id = layered(&settings.row_id, &defaults.row_id).map(|(value, _)| value);
    let max_depth = overrides.max_depth.unwrap_or(DEFAULT_MAX_DEPTH);
    let logged = layered(&settings.state, &defaults.state)
        .is_some_and(|(value, _)| value == StateMode::Logged);
    let mut problems: Vec<String> = Vec::new();
    if naming == Naming::Dlt && row_id == Some(false) {
        problems.push("row_id = false: naming = \"dlt\" gives every row its _dlt_id; drop row_id or use naming = \"sqlload\"".to_owned());
    }
    let unnest = match &overrides.unnest {
        None => Unnest::None,
        Some(UnnestSetting::Word(word)) if word == UNNEST_ALL => Unnest::All,
        Some(UnnestSetting::Word(word)) => {
            problems.push(format!(
                "unnest = {word:?}: list the dotted paths of the list fields to unnest, or write \"{UNNEST_ALL}\""
            ));
            Unnest::None
        }
        Some(UnnestSetting::Paths(paths)) => {
            problems.extend(path_problems(paths, max_depth));
            problems.extend(column_setting_problems(context, overrides, paths));
            Unnest::Paths(paths.clone())
        }
    };
    match unnest {
        Unnest::None if overrides.max_depth.is_some() => {
            problems.push(
                "max_depth limits the child tables unnest makes, and this table sets no unnest"
                    .to_owned(),
            );
        }
        Unnest::None => {}
        Unnest::All | Unnest::Paths(_) => {
            problems.extend(refused_writes(context, (strategy, contract)));
            problems.extend(unbuilt_writes(context, strategy, logged));
            if row_id == Some(false) {
                problems.push("row_id = false: unnest gives every row an id, which its child rows name as their parent's; drop row_id".to_owned());
            }
        }
    }
    (
        Nesting {
            naming,
            row_id: row_id.unwrap_or(naming == Naming::Dlt || unnest != Unnest::None),
            unnest,
            max_depth,
        },
        problems,
    )
}

/// The `unnest` paths with an empty part, a parent path not also listed, or more parts than `max_depth`.
fn path_problems(paths: &[String], max_depth: u32) -> Vec<String> {
    paths
        .iter()
        .filter_map(|path| path_problem(path, paths, max_depth))
        .collect()
}

/// What is wrong with the `unnest` path `path` among `paths`: an empty part, a parent path not also listed, or more parts than `max_depth`.
fn path_problem(path: &str, paths: &[String], max_depth: u32) -> Option<String> {
    let parts: Vec<&str> = path.split('.').collect();
    let parent = path.rsplit_once('.').map(|(parent, _)| parent);
    let listed = parent.is_none_or(|parent| paths.iter().any(|listed| listed == parent));
    match (parts.iter().any(|part| part.trim().is_empty()), listed) {
        (true, _) => Some(format!(
            "unnest {path:?}: a path names a field at each level, joined by \".\""
        )),
        (false, false) => Some(format!(
            "unnest {path:?}: its parent list {:?} must be unnested too; add it",
            parent.unwrap_or_default()
        )),
        (false, true) if parts.len() > max_depth as usize => Some(format!(
            "unnest {path:?}: it is {} levels deep, past max_depth = {max_depth}; raise max_depth or drop it",
            parts.len()
        )),
        (false, true) => None,
    }
}

/// The top-level `unnest` fields a `mask`, `columns` or `exclude_columns` entry also names: unnesting takes the field out of the table, so that setting would silently stop applying.
fn column_setting_problems(
    context: &SourceContext<'_>,
    overrides: &TableOverride,
    paths: &[String],
) -> Vec<String> {
    let exclude: Vec<&String> = context
        .source
        .exclude_columns
        .iter()
        .chain(&overrides.exclude_columns)
        .collect();
    paths
        .iter()
        .filter(|path| !path.contains('.'))
        .filter_map(|field| {
            let excluded = matching_pattern(field, &exclude);
            let setting = match (excluded, overrides.mask.contains_key(field), overrides.columns.contains_key(field)) {
                (Some(pattern), _, _) => format!("exclude_columns {pattern}"),
                (None, true, _) => "mask".to_owned(),
                (None, false, true) => "columns".to_owned(),
                (None, false, false) => return None,
            };
            Some(format!(
                "unnest {field:?}: {setting} also names {field}, which unnesting moves to child tables; drop one of them"
            ))
        })
        .collect()
}

/// The first of `patterns` matching `field`.
fn matching_pattern<'a>(field: &str, patterns: &[&'a String]) -> Option<&'a String> {
    patterns
        .iter()
        .find(|pattern| Glob::new(pattern).is_ok_and(|glob| glob.compile_matcher().is_match(field)))
        .copied()
}

/// What this build cannot yet do with `unnest` while its writers are taught to: a merge, and warehouses other than DuckDB and Postgres; and a `state = "logged"` table, whose commit can wait on its state while its child rows would not.
fn unbuilt_writes(
    context: &SourceContext<'_>,
    strategy: WriteStrategy,
    logged: bool,
) -> Vec<String> {
    let warehouse = context.target.warehouse;
    [
        (strategy == WriteStrategy::Merge).then(|| "write_strategy = \"merge\"".to_owned()),
        (context.target.path.is_none() && !sql_adapter(warehouse).writes_child_tables())
            .then(|| format!("a {warehouse} target")),
        logged.then(|| "state = \"logged\"".to_owned()),
    ]
    .into_iter()
    .flatten()
    .map(|what| format!("unnest does not yet write child tables with {what}; drop unnest, and its lists stay JSON columns"))
    .collect()
}

/// Why this table's writes cannot carry child tables yet: a files target, `delete_insert`, captured changes or an enforced contract.
fn refused_writes(
    context: &SourceContext<'_>,
    (strategy, contract): (WriteStrategy, Contract),
) -> Vec<String> {
    [
        context.target.path.is_some().then_some("a files target"),
        (strategy == WriteStrategy::DeleteInsert).then_some("write_strategy = \"delete_insert\""),
        (context.source.kind == SourceKind::PostgresCdc).then_some("kind = \"postgres_cdc\""),
        (contract == Contract::Enforced).then_some("contract = \"enforced\""),
    ]
    .into_iter()
    .flatten()
    .map(|what| format!("unnest does not yet write child tables with {what}; drop unnest, and its lists stay JSON columns"))
    .collect()
}
