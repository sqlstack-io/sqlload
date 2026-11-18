use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};

use crate::adapters::contract::models::{ChangeApply, TableName, WriteMode};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::files::constants::{CURSOR_PLACEHOLDER_START, TABLE_PLACEHOLDER};
use crate::bigquery_source::constants::BIGQUERY_SOURCE;
use crate::cdc::constants::{
    BATCH_INTERVAL, CDC_FILES_PROBLEM, CDC_TARGETS_PROBLEM, FILES_BATCH_INTERVAL,
};
use crate::column_parse::main::column_parse::column_parse;
use crate::column_parse::main::time_zone::time_zone;
use crate::column_parse::models::ColumnParse;
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::main::is_variant::is_variant;
use crate::column_types::main::parse_type::parse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::databricks_source::constants::DATABRICKS_SOURCE;
use crate::duckdb_source::constants::{DUCKDB_SOURCE, MOTHERDUCK_SOURCE};
use crate::files_source::main::constant_columns::constant_columns;
use crate::files_source::main::path_template::path_template;
use crate::files_source::models::FileColumn;
use crate::files_source::models::{
    AfterLoad, FilesOrder, FilesTable, PathValues, SourceFormat, StoredRead, TextChecks,
};
use crate::load_naming::main::loaded_name::loaded_name;
use crate::load_naming::models::{Compat, LoadedNames, NameRules, Naming};
use crate::loader::_helpers::destination::read_as_text;
use crate::loader::_helpers::python::field_type;
use crate::loader::_helpers::resolve::{NameRule, ResolvedTarget};
use crate::loader::_helpers::run_log::overlapping_files_problem;
use crate::loader::_helpers::sources::sheet_rule;
use crate::loader::_helpers::table_plan::source_cursor;
use crate::loader::constants::{
    ODBC_SOURCE, PRIMARY_KEY, PRIMARY_KEY_PLACEHOLDER, SNOWFLAKE_SOURCE, UNNEST_ALL,
};
use crate::loader::errors::LoaderError;
use crate::loader::models::{
    CdcSpec, CursorSpec, FilesTransform, SettingOrigin, TableLayout, TableSpec, WarehouseRoute,
    WarehouseSource,
};
use crate::loader::models::{SourceReader, TablePlan};
use crate::masking::models::MaskRules;
use crate::postgres_source::models::{PgType, SourceColumn, SourceTable};
use crate::postgres_source::models::{ReadColumn, WireType};
use crate::project::constants::DEFAULT_MAX_DEPTH;
use crate::project::main::table_flatten::table_flatten;
use crate::project::models::{
    CdcMode, CursorBounds, Deletes, Lookback, LookbackFrom, MaskMethod, NullCursor, NullKeys,
    OnDuplicateKeys, PathsSetting, ProjectConfig, SourceConfig, SourceKind, StateMode,
    TableOverride, TableSettings, UnloadStage, WriteStrategy,
};
use crate::publish_checks::main::check_problems::check_problems;
use crate::python_sources::types::PythonHost;
use crate::routing::models::Store;
use crate::row_batches::constants::FLATTEN_SEPARATOR;
use crate::row_batches::main::merge_schema::merge_schema;
use crate::row_batches::models::{Flatten, Unnest};
use crate::row_deletes::main::deletes_problems::deletes_problems;
use crate::row_deletes::main::deletes_spec::deletes_spec;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::sheets_source::models::SheetRead;
use crate::similar_names::main::close_name::close_name;
use crate::similar_names::main::nearest_name::nearest_name;
use crate::snowflake_source::constants::USER_STAGE_FOLDER;
use globset::{Glob, GlobMatcher, GlobSet, GlobSetBuilder};
use std::collections::{BTreeMap, BTreeSet};

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

/// Whether `columns` key `key` declares a column of one of the table's child tables (`items.sku`): its path before the last dot is a listed `unnest` path, or under `unnest = "all"` it has one.
pub(crate) fn is_child_column(key: &str, overrides: &TableOverride) -> bool {
    match &overrides.unnest {
        Some(PathsSetting::Paths(paths)) => key
            .rsplit_once('.')
            .is_some_and(|(path, _)| paths.iter().any(|listed| listed == path)),
        Some(PathsSetting::Word(word)) if word == UNNEST_ALL => key.contains('.'),
        _ => false,
    }
}

/// Problems with `columns`, `mask` and `parse` keys that name no source column, so a typo never loads a column unmasked; a `columns` key declaring a child table's column is not checked, as its field may be missing from the rows read first.
fn column_reference_problems(table: &SourceTable, overrides: &TableOverride) -> Vec<String> {
    let names: Vec<&str> = table
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let keys = overrides
        .columns
        .keys()
        .filter(|key| !is_child_column(key, overrides))
        .map(|key| ("columns", key))
        .chain(overrides.mask.keys().map(|key| ("mask", key)))
        .chain(overrides.parse.keys().map(|key| ("parse", key)));
    keys.filter(|(_, key)| !names.contains(&key.as_str()))
        .map(|(setting, key)| unknown_column(setting, key, &names))
        .collect()
}

/// A problem for each `parse` key naming a column a database's catalog already types as a date or a time, which holds no text to parse; files, Sheets and Python columns are typed from their values, and read as text where they are parsed.
fn typed_parse_problems(
    kind: SourceKind,
    table: &SourceTable,
    overrides: &TableOverride,
) -> Vec<String> {
    if matches!(
        kind,
        SourceKind::Files | SourceKind::GoogleSheets | SourceKind::Python
    ) {
        return Vec::new();
    }
    table
        .columns
        .iter()
        .filter(|column| overrides.parse.contains_key(&column.name))
        .filter_map(|column| match &column.column_type {
            Ok(
                column_type @ (ColumnType::Date
                | ColumnType::Time
                | ColumnType::TimestampTz
                | ColumnType::TimestampNtz),
            ) => Some(format!(
                "parse {}: the source already types the column as {column_type}, so it holds no text to parse; drop its parse",
                column.name
            )),
            _ => None,
        })
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
    let deletes = layered(&settings.deletes, &defaults.deletes);
    let cdc = cdc_spec(context, deletes.map(|(value, _)| value));
    let (strategy, strategy_origin) = match (
        layered(&settings.write_strategy, &defaults.write_strategy),
        &cdc,
    ) {
        (Some(set), _) => set,
        (None, Some(_)) => (WriteStrategy::Append, SettingOrigin::Source),
        (None, None) => {
            return Err(vec![missing_strategy_problem(
                context,
                table,
                cursor.as_ref(),
            )]);
        }
    };
    let merges = strategy == WriteStrategy::Merge && cdc.is_none();
    let replace_key: Vec<String> = layered(&settings.replace_key, &defaults.replace_key)
        .filter(|(_, origin)| {
            *origin == SettingOrigin::Table
                || strategy == WriteStrategy::DeleteInsert
                || strategy_origin != SettingOrigin::Table
        })
        .map(|(key, _)| key)
        .unwrap_or_default();
    let keyed = strategy == WriteStrategy::DeleteInsert && !replace_key.is_empty();
    let (unique_key, key_from_primary_key) =
        match table_key(context, table, (settings, defaults), merges) {
            Ok(key) => key,
            Err(problem) => return Err(vec![problem]),
        };
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
            strategy_problems(strategy, (&unique_key, &replace_key), cursor.as_ref()),
        ),
    };
    problems.extend(replace_key_problems(context, cursor.as_ref(), keyed));
    problems.extend(column_reference_problems(table, overrides));
    problems.extend(typed_parse_problems(context.source.kind, table, overrides));
    problems.extend(cursor_problems(
        context,
        (settings, defaults),
        strategy,
        cursor.as_ref(),
    ));
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
                    && cursor.as_ref().is_some_and(|cursor| cursor.names(name))
            })
            .map(|(name, _, _)| {
                format!("constant column {name} cannot be the cursor_column: it holds one value")
            }),
    );
    let table_sorts = settings.newest_by.is_some();
    let on_duplicate_keys = layered(&settings.on_duplicate_keys, &defaults.on_duplicate_keys)
        .filter(|(_, origin)| *origin == SettingOrigin::Table || (merges && !table_sorts));
    let table_fails = on_duplicate_keys == Some((OnDuplicateKeys::Fail, SettingOrigin::Table));
    let newest_by = layered(&settings.newest_by, &defaults.newest_by)
        .filter(|(_, origin)| *origin == SettingOrigin::Table || (merges && !table_fails))
        .map(|(value, _)| value);
    let (newest_by, found) = dedupe_settings(
        strategy,
        &unique_key,
        (
            on_duplicate_keys.map(|(value, _)| value),
            newest_by.as_deref(),
        ),
        overrides,
    );
    problems.extend(found);
    if context.source.order == FilesOrder::Path
        && !(strategy == WriteStrategy::Merge
            || (strategy == WriteStrategy::DeleteInsert && !replace_key.is_empty()))
    {
        problems.push(format!("order = \"path\" orders the files a merge or a delete_insert by replace_key applies one at a time; {strategy} loads them without an order, so remove it"));
    }
    let own_strategy = strategy_origin == SettingOrigin::Table && !keyed;
    let overlapping = layered(
        &settings.on_overlapping_files,
        &defaults.on_overlapping_files,
    );
    let overlapping = overlapping
        .filter(|(_, origin)| !own_strategy || *origin == SettingOrigin::Table)
        .map(|(value, _)| value);
    let files = context.source.kind == SourceKind::Files;
    problems.extend(overlapping.and_then(|_| overlapping_files_problem(files, strategy, keyed)));
    let matched_key = match keyed {
        true => &replace_key,
        false => &unique_key,
    };
    let (null_keys, not_null_keys, found) = key_nulls(
        (table, &constants),
        (strategy, matched_key, keyed),
        layered(&settings.null_keys, &defaults.null_keys)
            .filter(|(_, origin)| *origin == SettingOrigin::Table || merges || keyed)
            .map(|(value, _)| value),
    );
    problems.extend(found);
    let deletes = deletes
        .filter(|(_, origin)| *origin == SettingOrigin::Table || merges || cdc.is_some())
        .map(|(value, _)| value);
    let checks = layered(&settings.checks, &defaults.checks).map(|(value, _)| value);
    problems.extend(checks.iter().flat_map(|checks| {
        let target = (context.target.path.is_some(), deletes.is_some());
        check_problems(checks, (strategy, target.0, target.1))
    }));
    let batch = layered(&settings.batch_size, &defaults.batch_size).map(|(value, _)| value);
    let uses_key = strategy == WriteStrategy::Merge;
    problems.extend(overrides.mask.keys().filter_map(|masked| {
        let is_cursor = cursor.as_ref().is_some_and(|cursor| cursor.names(masked));
        let is_key = (uses_key && unique_key.contains(masked)) || replace_key.contains(masked);
        (is_cursor || is_key).then(|| {
            format!("mask {masked}: a masked column cannot be the cursor_column or part of the unique_key or replace_key")
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
    let contract = layered(&settings.contract, &defaults.contract)
        .map(|(value, _)| value)
        .unwrap_or_default();
    let (nesting, found) = nesting(context, overrides, strategy);
    problems.extend(found);
    let unnests = !matches!(nesting.unnest, Unnest::None);
    let kind = (strategy, context.source.kind);
    problems.extend(deletes_problems(
        deletes,
        kind,
        context.target.warehouse,
        unnests,
    ));
    let from = overrides.deletes_from.as_ref();
    let (deletes, checks) = deletes_spec((deletes.filter(|_| cdc.is_none()), from), checks);
    if keyed && !matches!(nesting.unnest, Unnest::None) {
        problems.push("replace_key does not carry child tables yet, whose rows hang off the replaced rows by their ids. Fix: drop unnest, or use write_strategy = \"merge\" on a unique_key".to_owned());
    }
    problems.extend(after_load_problems(context, (strategy, keyed)));
    let layout = table_layout(settings, defaults);
    let is_duckdb = sql_adapter(context.target.warehouse).parses_variant_pins();
    problems.extend(sql_adapter(context.target.warehouse).layout_problems(&layout));
    let parses = table_parses(overrides).unwrap_or_else(|found| {
        problems.extend(found);
        BTreeMap::new()
    });
    let timezone = table_timezone((settings, defaults)).unwrap_or_else(|problem| {
        problems.push(problem);
        None
    });
    if !problems.is_empty() {
        return Err(problems);
    }
    let name = table_name(
        (context.name, context.source),
        overrides.name.as_deref(),
        (
            table,
            (
                nesting.naming,
                sql_adapter(context.target.warehouse).max_identifier_length(),
            ),
        ),
    );
    Ok(TableSpec {
        setting_problems: Vec::new(),
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
        key_from_primary_key,
        settings_at: None,
        unique_key_at: None,
        naming: nesting.naming,
        compat: nesting.compat,
        send: layered(&settings.send, &defaults.send).map_or(context.target.send, |(send, _)| send),
        staging_dir: context.target.staging_dir.clone(),
        variant_json: context.target.variant_json,
        names: LoadedNames::default(),
        name_rules: NameRules::default(),
        listed: None,
        on_duplicate_keys: on_duplicate_keys
            .map(|(value, _)| value)
            .unwrap_or_default(),
        null_keys,
        not_null_keys,
        replace_key,
        on_overlapping_files: overlapping.unwrap_or_default(),
        newest_by,
        cursor,
        batch,
        batch_concurrency: settings.batch_concurrency.or(defaults.batch_concurrency),
        constants,
        shared_destination: false,
        #[cfg(feature = "container-tests")]
        test_loader_route: layered(&settings.test_loader_route, &defaults.test_loader_route)
            .is_some_and(|(value, _)| value),
        #[cfg(not(feature = "container-tests"))]
        test_loader_route: false,
        source_end: None,
        pinned: columns
            .pinned
            .iter()
            .filter(|(key, _)| !is_child_column(key, overrides))
            .map(
                |(key, column_type)| match is_duckdb && is_variant(column_type) {
                    true => (key.clone(), ColumnType::Json),
                    false => (key.clone(), column_type.clone()),
                },
            )
            .collect(),
        variants: columns
            .pinned
            .iter()
            .filter(|(key, column_type)| {
                is_duckdb && is_variant(column_type) && !is_child_column(key, overrides)
            })
            .map(|(key, _)| key.clone())
            .collect(),
        child_pinned: columns
            .pinned
            .into_iter()
            .filter(|(key, _)| is_child_column(key, overrides))
            .collect(),
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
        infer_types: layered(&settings.infer_types, &defaults.infer_types)
            .is_none_or(|(value, _)| value),
        type_change_rebuild: layered(&settings.type_change_rebuild, &defaults.type_change_rebuild)
            .map(|(value, _)| value)
            .unwrap_or_default(),
        flatten: nesting.flatten,
        row_id: nesting.row_id,
        root_rows: false,
        unnest: nesting.unnest,
        max_depth: nesting.max_depth,
        allow_empty: layered(&settings.allow_empty, &defaults.allow_empty)
            .is_some_and(|(value, _)| value),
        logged: layered(&settings.state, &defaults.state)
            .is_some_and(|(value, _)| value == StateMode::Logged),
        after_load: context.source.after_load.clone(),
        files_order: context.source.order,
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
        deletes,
        tombstones_of: overrides.tombstones_of.clone(),
        warehouse_source: match context.source.kind {
            kind @ (SourceKind::Odbc
            | SourceKind::BigQuery
            | SourceKind::Databricks
            | SourceKind::DuckDb
            | SourceKind::MotherDuck) => Some(WarehouseSource {
                system: match kind {
                    SourceKind::Odbc => ODBC_SOURCE,
                    SourceKind::BigQuery => BIGQUERY_SOURCE,
                    SourceKind::Databricks => DATABRICKS_SOURCE,
                    SourceKind::DuckDb => DUCKDB_SOURCE,
                    _ => MOTHERDUCK_SOURCE,
                },
                sql: overrides.sql.clone(),
                route: WarehouseRoute::Query,
            }),
            _ => warehouse_source(context, overrides),
        },
        row_filter: layered(&settings.row_filter, &defaults.row_filter)
            .filter(|_| overrides.sql.is_none() && overrides.tombstones_of.is_none())
            .map(|(value, _)| value),
        layout,
        transform: None,
        parses,
        timezone,
    })
}

/// The table's `parse` entries read, or their problems; a column cannot be both pinned in `columns` and parsed, as `parse` gives its type.
fn table_parses(overrides: &TableOverride) -> Result<BTreeMap<String, ColumnParse>, Vec<String>> {
    let mut problems: Vec<String> = Vec::new();
    let mut parses: BTreeMap<String, ColumnParse> = BTreeMap::new();
    for (column, setting) in &overrides.parse {
        match column_parse(setting) {
            Ok(_) if overrides.columns.contains_key(column) => problems.push(format!(
                "parse {column}: the column is also pinned in columns, and parse gives its type; drop the pin"
            )),
            Ok(parse) => {
                parses.insert(column.clone(), parse);
            }
            Err(problem) => problems.push(format!("parse {column}: {problem}")),
        }
    }
    match problems.is_empty() {
        true => Ok(parses),
        false => Err(problems),
    }
}

/// The table's `timezone` (its own, else its source's) and where it is set, or why it is not an IANA zone.
fn table_timezone(
    (settings, defaults): (&TableSettings, &TableSettings),
) -> Result<Option<(chrono_tz::Tz, SettingOrigin)>, String> {
    match layered(&settings.timezone, &defaults.timezone) {
        Some((name, origin)) => time_zone(&name)
            .map(|zone| Some((zone, origin)))
            .map_err(|problem| format!("timezone: {problem}")),
        None => Ok(None),
    }
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

/// How a `snowflake` source's table is read: unloaded to the source's `stage`, or to the user stage when it sets none; a Snowflake target writing through the source's own connection, on its warehouse (the source sets no `warehouse` of its own), may select the rows itself.
fn warehouse_source(
    context: &SourceContext<'_>,
    overrides: &TableOverride,
) -> Option<WarehouseSource> {
    let source = context.source;
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
            route: WarehouseRoute::Unload {
                store,
                stage,
                same_connection: context.target.session_connection.as_ref()
                    == Some(&source.connection)
                    && source.warehouse.is_none(),
            },
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
fn cdc_spec(context: &SourceContext<'_>, deletes: Option<Deletes>) -> Option<CdcSpec> {
    let source = context.source;
    (source.kind == SourceKind::PostgresCdc).then(|| {
        let default_name = format!("sqlload_{}", context.name.to_lowercase());
        CdcSpec {
            apply: match (source.cdc_mode, deletes.unwrap_or(Deletes::Soft)) {
                (CdcMode::Changelog, _) => ChangeApply::Changelog,
                (CdcMode::Merge, Deletes::Soft) => ChangeApply::SoftDeletes,
                (CdcMode::Merge, Deletes::Hard) => ChangeApply::HardDeletes,
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
        | SourceKind::BigQuery
        | SourceKind::Databricks
        | SourceKind::DuckDb
        | SourceKind::MotherDuck => format!(
            "no description; add description = \"...\" under [sources.{source_name}.tables.\"{qualified}\"]"
        ),
    };
    (written && !described).then_some(fix)
}

fn glob(pattern: &str) -> Result<Glob, String> {
    Glob::new(pattern).map_err(|error| format!("invalid column pattern {pattern:?}: {error}"))
}

/// A matcher for column name patterns such as `*email*`.
pub(crate) fn column_matcher(patterns: &[String]) -> Result<GlobSet, String> {
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

/// The destination table name of `table` from source `source_name`: `name` (its `name` setting) when it has no placeholder, else that template (or the source's) filled in, in lower case under `naming = "source"`; as it loads under `rule`.
pub(crate) fn table_name(
    (source_name, source): (&str, &SourceConfig),
    name: Option<&str>,
    (table, rule): (&SourceTable, NameRule),
) -> String {
    let name = match name {
        Some(name) if !name.contains('{') => name.to_owned(),
        _ => {
            let filled = destination_name(
                name.unwrap_or(source.table_name_template()),
                source_name,
                table,
            );
            match rule.0 {
                Naming::Source => filled.to_ascii_lowercase(),
                Naming::SnakeCase => filled,
            }
        }
    };
    loaded_name(rule.0, &name, rule.1)
}

/// A destination table name from `template` with `{source}`, `{schema}` and `{table}` filled in.
pub(crate) fn destination_name(template: &str, source: &str, table: &SourceTable) -> String {
    template
        .replace("{source}", source)
        .replace("{schema}", &table.schema)
        .replace("{table}", &table.name)
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
    let (columns, _) = layered(&table.cursor_column, &source.cursor_column)?;
    let (column, coalesce) = columns.split_first()?;
    Some(CursorSpec {
        column: column.clone(),
        coalesce: coalesce.to_vec(),
        null_cursor: layered(&table.null_cursor, &source.null_cursor)
            .map(|(value, _)| value)
            .unwrap_or_default(),
        out_of_range: layered(
            &table.on_cursor_out_of_range,
            &source.on_cursor_out_of_range,
        )
        .map(|(value, _)| value)
        .unwrap_or_default(),
        start: layered(&table.cursor_start, &source.cursor_start).map(|(value, _)| value),
        lookback: match layered(&table.lookback, &source.lookback) {
            Some((Lookback::Duration(duration), _)) => Some(duration),
            _ => None,
        },
        lookback_units: match layered(&table.lookback, &source.lookback) {
            Some((Lookback::Units(units), _)) => Some(units),
            _ => None,
        },
        lookahead: layered(&table.lookahead, &source.lookahead).map(|(value, _)| value),
        from_now: layered(&table.lookback_from, &source.lookback_from)
            .is_some_and(|(value, _)| value == LookbackFrom::Now),
        inclusive: layered(
            &table.append_cursor_inclusive,
            &source.append_cursor_inclusive,
        )
        .is_some_and(|(value, _)| value),
        literals: layered(&table.cursor_bounds, &source.cursor_bounds)
            .is_some_and(|(value, _)| value == CursorBounds::Literals),
    })
}

/// A table's `unique_key` (table over source) and whether a `merge` takes it from the primary key its source reports, set as `"{primary_key}"`. A `merge` table without one gets none, so it is refused; `"{primary_key}"` on it is refused for a source that reports no keys, and for a table whose source reports none. A table that doesn't merge (a `postgres_cdc` table included) keeps the primary key its source reports unless it names columns: it only orders and identifies rows (a ClickHouse table's sorting key, a CDC table's merge key), so a source's `"{primary_key}"` never fails it.
fn table_key(
    context: &SourceContext<'_>,
    table: &SourceTable,
    (settings, defaults): (&TableSettings, &TableSettings),
    merges: bool,
) -> Result<(Vec<String>, bool), String> {
    let set = layered(&settings.unique_key, &defaults.unique_key).map(|(key, _)| key);
    let placeholder = set
        .as_ref()
        .is_some_and(|key| key.iter().any(|column| column == PRIMARY_KEY_PLACEHOLDER));
    match set {
        None if merges => Ok((Vec::new(), false)),
        None => Ok((table.primary_key.clone(), false)),
        Some(key) if !placeholder => Ok((key, false)),
        Some(key) if key.len() > 1 => Err(format!(
            "{}: unique_key {PRIMARY_KEY_PLACEHOLDER:?} stands for the whole primary key, so it can't be listed with other columns; set unique_key = {PRIMARY_KEY_PLACEHOLDER:?} or name every key column",
            table.qualified_name()
        )),
        Some(_) if !merges => Ok((table.primary_key.clone(), false)),
        Some(_)
            if matches!(
                context.source.kind,
                SourceKind::Python | SourceKind::Files | SourceKind::GoogleSheets
            ) =>
        {
            Err(format!(
                "{} (kind = \"{}\"): unique_key = {PRIMARY_KEY_PLACEHOLDER:?} merges on the primary key the source reports, and a {} source reports none. Fix: name the key columns, such as unique_key = [\"id\"]",
                table.qualified_name(),
                context.source.kind.name(),
                context.source.kind.name()
            ))
        }
        Some(_) if table.primary_key.is_empty() => Err(no_primary_key_problem(context, table)),
        Some(_) => match nullable_key_problem(context, table) {
            Some(problem) => Err(problem),
            None => Ok((table.primary_key.clone(), true)),
        },
    }
}

/// Why a table that neither it nor its source gives a `write_strategy` can't be planned, with the line to add at source and at table level: `replace` without a cursor, otherwise `merge` on a key, `delete_insert` or `append`, which the table must choose.
fn missing_strategy_problem(
    context: &SourceContext<'_>,
    table: &SourceTable,
    cursor: Option<&CursorSpec>,
) -> String {
    let qualified = table.qualified_name();
    let files = context.source.kind == SourceKind::Files;
    let suggested = match (cursor, table.primary_key.is_empty()) {
        (None, _) => "write_strategy = \"replace\"".to_owned(),
        (Some(_), _) if files => "write_strategy = \"delete_insert\"".to_owned(),
        (Some(_), false) => format!(
            "write_strategy = \"merge\" with unique_key = {PRIMARY_KEY_PLACEHOLDER:?} (or the key columns)"
        ),
        (Some(_), true) => {
            "write_strategy = \"merge\" with unique_key = [the key columns]".to_owned()
        }
    };
    let alternatives = match cursor {
        None => "\"append\", or \"merge\" with a unique_key",
        Some(_) if files => {
            "\"append\", or \"merge\" with unique_key columns and no cursor_column, as a files merge reads each new file once"
        }
        Some(_) => {
            "\"delete_insert\" or \"append\", which can load a row updated since an earlier load again beside its old version"
        }
    };
    let standalone = context.source.table_name.as_deref() == Some("{table}")
        && context.source.tables.len() == 1
        && context.source.tables.contains_key(context.name);
    let fix = match context.source.kind {
        SourceKind::Python if standalone => {
            format!("set {suggested} in @sl.table(...); or choose {alternatives}")
        }
        SourceKind::Python => format!(
            "set {suggested} in sl.source({:?}, ...) for all its tables, or in @{}.table(...) for this one; or choose {alternatives}",
            context.name, context.name
        ),
        _ => format!(
            "add {suggested} under [sources.{}] for all its tables, or under [sources.{}.tables.{qualified:?}] for this one; or choose {alternatives}",
            context.name, context.name
        ),
    };
    format!(
        "{qualified} (kind = \"{}\"): no write_strategy is set on the table or its source, and SQLLoad doesn't choose one. Fix: {fix}",
        context.source.kind.name()
    )
}

/// Whether `table`'s column `key` can hold NULL: the source does not declare it NOT NULL, or doesn't list it.
fn can_be_null(table: &SourceTable, key: &str) -> bool {
    table
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(key))
        .is_none_or(|column| column.nullable)
}

/// Why a table set to `unique_key = "{primary_key}"` cannot merge on the key its source reports: a column of it can be NULL (a unique index on a nullable column, which MySQL Connector/ODBC reports as the primary key), so the source can hold several different rows with a NULL there, which a merge matching NULLs would merge into one.
fn nullable_key_problem(context: &SourceContext<'_>, table: &SourceTable) -> Option<String> {
    let nullable: Vec<&str> = table
        .primary_key
        .iter()
        .filter(|key| can_be_null(table, key))
        .map(String::as_str)
        .collect();
    (!nullable.is_empty()).then(|| {
        format!(
            "{} (kind = \"{}\"): unique_key = {PRIMARY_KEY_PLACEHOLDER:?} takes the key the source reports, ({}), and {} can be NULL there, so the source can hold different rows with a NULL in it, which a merge matching NULLs would merge into one. Fix: name the unique_key columns, ones that identify a row and are never NULL, such as unique_key = [\"id\"]",
            table.qualified_name(),
            context.source.kind.name(),
            table.primary_key.join(", "),
            nullable.join(", ")
        )
    })
}

/// Why a table set to `unique_key = "{primary_key}"` has no key: its source reports no primary key. It names the keys the database lists (a listed primary key means the driver's key lookup is wrong) and suggests the first whose columns are never NULL, as a merge inserts rows with a NULL key again.
fn no_primary_key_problem(context: &SourceContext<'_>, table: &SourceTable) -> String {
    let shown: Vec<String> = table
        .listed_keys
        .iter()
        .map(|key| format!("{} ({})", key.constraint_type, key.columns.join(", ")))
        .collect();
    let listed = match (table.listed_keys.first(), context.source.kind.name()) {
        (None, _) => String::new(),
        (Some(_), "postgres" | "postgres_cdc") => {
            format!(". The table has {}", shown.join(" and "))
        }
        (Some(first), _) => format!(
            ". The ODBC driver reported no primary key, but the database's INFORMATION_SCHEMA lists {}{}",
            shown.join(" and "),
            match first.constraint_type == PRIMARY_KEY {
                true =>
                    ", so the driver's key lookup is wrong (MariaDB Connector/ODBC 3.1 finds none in MySQL 8.4)",
                false => "",
            }
        ),
    };
    let never_null: BTreeSet<&str> = table
        .columns
        .iter()
        .filter(|column| !column.nullable)
        .map(|column| column.name.as_str())
        .collect();
    let example = table
        .listed_keys
        .iter()
        .find(|key| {
            key.columns
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<&str>>()
                .is_subset(&never_null)
        })
        .map_or_else(|| "[\"id\"]".to_owned(), |key| format!("{:?}", key.columns));
    format!(
        "{} (kind = \"{}\"): unique_key = {PRIMARY_KEY_PLACEHOLDER:?}, but the source reports no primary key for the table{listed}. Fix: name the key columns, such as unique_key = {example}",
        table.qualified_name(),
        context.source.kind.name()
    )
}

/// The `newest_by` columns of a merge, and the problems with `on_duplicate_keys` and `newest_by` (a source's apply only to its merge tables, and either one set on a table drops the other from its source): both are for `merge`, `newest_by` names columns that are neither in the key, masked nor repeated, and has nothing to pick under `on_duplicate_keys = "fail"`.
fn dedupe_settings(
    strategy: WriteStrategy,
    unique_key: &[String],
    (on_duplicate_keys, newest_by): (Option<OnDuplicateKeys>, Option<&[String]>),
    overrides: &TableOverride,
) -> (Vec<String>, Vec<String>) {
    let mut problems: Vec<String> = Vec::new();
    if strategy != WriteStrategy::Merge {
        problems.extend(
            [
                on_duplicate_keys.map(|_| "on_duplicate_keys"),
                newest_by.map(|_| "newest_by"),
            ]
            .into_iter()
            .flatten()
            .map(|name| format!("{name} is for write_strategy = \"merge\", which matches rows on unique_key; {strategy} keeps every row")),
        );
        return (Vec::new(), problems);
    }
    let Some(columns) = newest_by else {
        return (Vec::new(), problems);
    };
    if columns.is_empty() {
        problems.push("newest_by names a column, or a list compared in order, such as newest_by = \"updated_at\"".to_owned());
    }
    for (index, column) in columns.iter().enumerate() {
        if columns[..index].contains(column) {
            problems.push(format!(
                "newest_by names {column} twice; name each column once"
            ));
        }
        if unique_key
            .iter()
            .any(|key| key.eq_ignore_ascii_case(column))
        {
            problems.push(format!("newest_by column {column} is in the unique_key, so it is the same in every row it would compare; name another column"));
        }
        if overrides.mask.contains_key(column) {
            problems.push(format!(
                "newest_by column {column} is masked, so its values cannot say which row is newer"
            ));
        }
    }
    if on_duplicate_keys == Some(OnDuplicateKeys::Fail) {
        problems.push("newest_by picks the newest of the rows repeating a key, but on_duplicate_keys = \"fail\" keeps none; set one of them".to_owned());
    }
    (columns.to_vec(), problems)
}

/// A table's `null_keys` (as set, else the default) and the columns of `unique_key` (the key it matches rows on: a merge's `unique_key`, a `delete_insert`'s `replace_key` when `keyed`) whose values are never NULL, so they compare with `=`: the source table's columns it declares NOT NULL and constant columns, or every key column under `null_keys = "fail"`, which refuses a commit holding a NULL in one (a `postgres_cdc` table's key, which its merge matches on, included); and the problem with `null_keys` set on a table that matches no key.
fn key_nulls(
    (table, constants): (&SourceTable, &[(String, ColumnType, String)]),
    (strategy, unique_key, keyed): (WriteStrategy, &[String], bool),
    null_keys: Option<NullKeys>,
) -> (NullKeys, Vec<String>, Vec<String>) {
    let problems: Vec<String> = null_keys
        .filter(|_| strategy != WriteStrategy::Merge && !keyed)
        .map(|_| format!("null_keys is for write_strategy = \"merge\", which matches rows on unique_key, and \"delete_insert\" with a replace_key; {strategy} keeps every row"))
        .into_iter()
        .collect();
    let null_keys = null_keys.unwrap_or_default();
    let never_null = |key: &String| {
        null_keys == NullKeys::Fail
            || constants
                .iter()
                .any(|(name, _, _)| name.eq_ignore_ascii_case(key))
            || !can_be_null(table, key)
    };
    let not_null: Vec<String> = unique_key
        .iter()
        .filter(|key| never_null(key))
        .cloned()
        .collect();
    (null_keys, not_null, problems)
}

/// Why a `delete_insert` by `replace_key` (`keyed`) cannot load where `context` puts it: a files target replaces whole partition folders; a files source picks its files from the run log, not by a cursor; and on any other source a cursor would read part of a key value's rows while the commit deletes all of them, losing those the read never returned.
fn replace_key_problems(
    context: &SourceContext<'_>,
    cursor: Option<&CursorSpec>,
    keyed: bool,
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if !keyed {
        return problems;
    }
    if context.target.path.is_some() {
        problems.push("replace_key deletes rows by key in a database table, and a files target replaces whole partition folders. Fix: load into a database target, or drop replace_key and use a partition cursor_column".to_owned());
    }
    match (cursor, context.source.kind) {
        (Some(cursor), SourceKind::Files) => problems.push(format!(
            "cursor_column {} does not apply to a files source's delete_insert by replace_key, which reads each new or changed file once, oldest first, from the run log. Fix: remove cursor_column, or drop replace_key to replace partitions by the cursor",
            cursor.column
        )),
        (Some(cursor), _) => problems.push(format!(
            "cursor_column {} does not apply to delete_insert by replace_key: a load deletes every row of each key value it reads, so the rows of a key value outside the cursor range, which it never reads, would be lost. Fix: drop cursor_column, so every load reads the whole source and replaces each key value it holds; or use write_strategy = \"merge\" on a unique_key to read by the cursor",
            cursor.column
        )),
        (None, _) => {}
    }
    problems
}

/// Problems with a strategy and the keys and cursor it is given.
pub(crate) fn strategy_problems(
    strategy: WriteStrategy,
    (unique_key, replace_key): (&[String], &[String]),
    cursor: Option<&CursorSpec>,
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    match (strategy, cursor) {
        (WriteStrategy::Merge, _) if unique_key.is_empty() => problems.push(format!(
            "write_strategy = \"merge\" needs a unique_key: the key columns, such as unique_key = [\"id\"], or unique_key = {PRIMARY_KEY_PLACEHOLDER:?} for the primary key the source reports"
        )),
        (WriteStrategy::Append, Some(cursor))
            if cursor.lookback.is_some() || cursor.lookback_units.is_some() =>
        {
            problems.push(
                "lookback re-inserts the window on every run with append; use delete_insert (no key needed) or merge"
                    .to_owned(),
            )
        }
        (WriteStrategy::Replace, Some(_)) => problems.push(
            "write_strategy = \"replace\" reloads the whole table, so it takes no cursor_column".to_owned(),
        ),
        (WriteStrategy::DeleteInsert, None) if replace_key.is_empty() => problems.push(
            "write_strategy = \"delete_insert\" replaces a cursor range or the rows of the key values it reads, so it needs a cursor_column or a replace_key. Fix: set cursor_column, or replace_key = [\"version\"] to replace every row of each version a load reads".to_owned(),
        ),
        _ => {}
    }
    if !replace_key.is_empty() && strategy != WriteStrategy::DeleteInsert {
        let merge = match strategy {
            WriteStrategy::Merge => " (merge matches rows one by one on unique_key)",
            _ => "",
        };
        problems.push(format!(
            "replace_key is for write_strategy = \"delete_insert\", which replaces the rows holding the key values a load reads; {strategy} does not delete by it. Fix: set write_strategy = \"delete_insert\" to replace those rows whole, or drop replace_key{merge}"
        ));
    }
    if let Some(cursor) = cursor.filter(|cursor| cursor.from_now && cursor.lookback_units.is_some())
    {
        problems.push(format!(
            "lookback_from = \"now\" counts lookback back from the time of the run, so {} needs a duration lookback, such as lookback = \"1d\", not a number of cursor units",
            cursor.label()
        ));
    }
    if let Some(cursor) = cursor.filter(|cursor| {
        cursor.from_now && cursor.lookback.is_none() && cursor.lookback_units.is_none()
    }) {
        problems.push(format!(
            "lookback_from = \"now\" counts lookback back from now, so {} needs a lookback",
            cursor.label()
        ));
    }
    problems
}

/// Why the table's cursor columns and `null_cursor` cannot apply: an empty or repeating `cursor_column` list; a coalesced cursor where its value is one column's (a files source's partition or path column, a Python table's field, a files target's partitions, `delete_insert`'s replaced range); `null_cursor` on a table entry without a cursor; and `null_cursor = "include"`, which reads the NULL-cursor rows on every run, on anything but `merge`.
fn cursor_problems(
    context: &SourceContext<'_>,
    (settings, defaults): (&TableSettings, &TableSettings),
    strategy: WriteStrategy,
    cursor: Option<&CursorSpec>,
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let listed = layered(&settings.cursor_column, &defaults.cursor_column).map(|(value, _)| value);
    if listed.as_ref().is_some_and(Vec::is_empty) {
        problems.push("cursor_column = [] names no column; name one, or list the columns to coalesce, such as [\"updated_at\", \"created_at\"]".to_owned());
    }
    let listed = listed.unwrap_or_default();
    for (index, column) in listed.iter().enumerate() {
        if listed[..index]
            .iter()
            .any(|earlier| earlier.eq_ignore_ascii_case(column))
        {
            problems.push(format!(
                "cursor_column lists {column} twice; list each column once"
            ));
            break;
        }
    }
    if layered(&settings.null_cursor, &defaults.null_cursor)
        .is_some_and(|(_, origin)| origin == SettingOrigin::Table)
        && cursor.is_none()
    {
        problems.push("null_cursor says what a cursor read does with rows whose cursor is NULL, but the table has no cursor_column; remove it".to_owned());
    }
    let out_of_range = layered(
        &settings.on_cursor_out_of_range,
        &defaults.on_cursor_out_of_range,
    );
    match (out_of_range, context.source.kind) {
        (None, _) => {}
        (Some(_), SourceKind::Python) if cursor.is_none() => problems.push("on_cursor_out_of_range says what a load does with rows outside the cursor window, but the table has no cursor_column; remove it".to_owned()),
        (Some(_), SourceKind::Python) => {}
        (Some(_), kind) => problems.push(format!(
            "on_cursor_out_of_range checks the rows a Python table's function returns against its cursor window; SQLLoad filters a {} source's reads itself, so remove it",
            kind.name()
        )),
    }
    let Some(cursor) = cursor else {
        return problems;
    };
    let label = cursor.label();
    let coalesced = !cursor.coalesce.is_empty();
    let one_column = match context.source.kind {
        SourceKind::Files => Some(
            "a files source's cursor is one partition or path column, the same in every row of a file",
        ),
        SourceKind::Python => Some(
            "a Python table's cursor is one field of the rows it yields; yield the first set value as one field (such as row[\"updated_at\"] or row[\"created_at\"]) and name that field",
        ),
        _ if context.target.path.is_some() => {
            Some("a files target partitions and reads its maximum by one cursor column")
        }
        _ if strategy == WriteStrategy::DeleteInsert => Some(
            "delete_insert replaces the destination rows in a range of one cursor column; use merge, or name one column",
        ),
        _ => None,
    };
    if let Some(reason) = one_column.filter(|_| coalesced) {
        problems.push(format!(
            "cursor_column lists several columns to coalesce ({label}), but {reason}"
        ));
    }
    if cursor.null_cursor == NullCursor::Include {
        let refusal = match strategy {
            WriteStrategy::Append => Some((
                "append would append them again on every run, duplicating them",
                format!(
                    "use write_strategy = \"merge\" with a unique_key, or coalesce a column that is always set (such as cursor_column = [\"{}\", \"created_at\"]), or set null_cursor = \"ignore\" to skip them",
                    cursor.column
                ),
            )),
            WriteStrategy::DeleteInsert => Some((
                "they sit outside every cursor range delete_insert replaces, so it would insert them again on every run, duplicating them",
                "use write_strategy = \"merge\" with a unique_key, or set null_cursor = \"ignore\" to skip them".to_owned(),
            )),
            WriteStrategy::Merge | WriteStrategy::Replace => None,
        };
        if let Some((duplicates, fix)) = refusal {
            problems.push(format!(
                "null_cursor = \"include\" reads the rows whose {label} is NULL on every run, and {duplicates}; {fix}"
            ));
        }
    }
    problems
}

/// How many of `table`'s source rows have a NULL cursor, which its read's bounds never select, when the table fails on them (`null_cursor` unset), counted by the database: `SELECT count(*) ... WHERE <cursor> IS NULL`. `None` when nothing is counted: the read has no bounds (a first run reads those rows too), `null_cursor` includes or skips them, the source reports a cursor column NOT NULL, or the source is not a database: a Python table's rows are counted as they arrive, and a files source's cursor is each file's path value, never NULL.
pub(crate) async fn null_cursor_rows(
    reader: &SourceReader,
    table: &TablePlan,
) -> Result<Option<u64>, LoaderError> {
    let spec = &table.spec;
    let (Some(cursor), Some(window)) = (&spec.cursor, &table.window) else {
        return Ok(None);
    };
    let bounded = window.from.is_some() || window.to.is_some();
    let not_null = cursor.columns().any(|name| {
        spec.source_table
            .column(name)
            .is_some_and(|column| !column.nullable)
    });
    if !bounded || not_null || cursor.null_cursor != NullCursor::Fail {
        return Ok(None);
    }
    let cursor_type = table
        .read_columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(&cursor.column))
        .map(|column| &column.output);
    let Some(expression) = source_cursor(spec, &table.read_columns, cursor_type) else {
        return Ok(None);
    };
    let filter = spec
        .read_filter(Some(format!("{expression} IS NULL")))
        .unwrap_or_default();
    let sql = spec
        .warehouse_source
        .as_ref()
        .and_then(|source| source.sql.as_deref());
    crate::loader::_helpers::resolve::count_rows(reader, (&spec.source_table, sql), &filter).await
}

/// Why `spec`'s load fails: `rows` source rows have a NULL cursor, which its incremental read never selects, so they would never load; with the fixes `plan` accepts for the table: `null_cursor = "include"` on `merge`, a coalesced cursor where one may be listed (not a Python table, `delete_insert` or a files target), and `null_cursor = "ignore"`.
pub(crate) fn null_cursor_problem(spec: &TableSpec, rows: u64) -> String {
    let (cursor, label) = match &spec.cursor {
        Some(cursor) => (cursor.column.as_str(), cursor.label()),
        None => ("", String::new()),
    };
    let (have, they) = match rows {
        1 => ("row has", "it"),
        _ => ("rows have", "they"),
    };
    let coalesces = spec.python.is_none()
        && spec.strategy != WriteStrategy::DeleteInsert
        && spec.destination_path.is_none();
    let fixes: Vec<String> = [
        (spec.strategy == WriteStrategy::Merge)
            .then(|| "set null_cursor = \"include\" to read them on every run".to_owned()),
        coalesces.then(|| {
            format!(
                "list a column that is always set to coalesce with it (such as cursor_column = [\"{cursor}\", \"created_at\"])"
            )
        }),
        Some("set null_cursor = \"ignore\" to skip them".to_owned()),
    ]
    .into_iter()
    .flatten()
    .collect();
    let fixes = match fixes.split_last() {
        Some((last, [])) => last.clone(),
        Some((last, rest)) => format!("{}, or {last}", rest.join(", ")),
        None => String::new(),
    };
    format!(
        "{rows} source {have} a NULL {label}, which an incremental read never selects, so {they} would never load; {fixes}"
    )
}

/// Why a files source's `after_load` cannot apply to its table: only `append`, `merge` and `delete_insert` by a `replace_key` (`keyed`) never read a loaded file again, and a files target commits its own way.
fn after_load_problems(
    context: &SourceContext<'_>,
    (strategy, keyed): (WriteStrategy, bool),
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if context.source.after_load == AfterLoad::Keep {
        return problems;
    }
    if !matches!(strategy, WriteStrategy::Append | WriteStrategy::Merge) && !keyed {
        problems.push(format!(
            "after_load deletes or moves each file once it loads, so it needs write_strategy = \"append\" or \"merge\", or \"delete_insert\" with a replace_key; {strategy} reads loaded files again"
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
    compat: Compat,
    row_id: bool,
    unnest: Unnest,
    max_depth: u32,
    flatten: Flatten,
}

/// The table's naming and compat (each its own, its source's, then its target's), whether its rows get an id (always under `compat = "dlt"`), its `unnest` (under `compat = "dlt"`, `"all"` for JSON and NDJSON files on a warehouse), `max_depth` and `flatten` (see [`table_flatten`]), and the problems with them: `unnest` names list fields by dotted paths whose ancestors it also names, no deeper than `max_depth`, that no column setting names, on a table whose writes can carry child tables.
fn nesting(
    context: &SourceContext<'_>,
    overrides: &TableOverride,
    strategy: WriteStrategy,
) -> (Nesting, Vec<String>) {
    let settings = &overrides.settings;
    let defaults = &context.source.defaults;
    let naming = table_naming((settings, defaults), context.target.naming);
    let compat = table_compat((settings, defaults), context.target.compat);
    let row_id = layered(&settings.row_id, &defaults.row_id).map(|(value, _)| value);
    let max_depth = overrides.max_depth.unwrap_or(DEFAULT_MAX_DEPTH);
    let mut problems: Vec<String> = Vec::new();
    if compat == Compat::Dlt && row_id == Some(false) {
        problems.push(
            "row_id = false: compat = \"dlt\" gives every row its _dlt_id; drop row_id".to_owned(),
        );
    }
    let json_files = context.source.kind == SourceKind::Files
        && matches!(
            context.source.format,
            SourceFormat::Json | SourceFormat::Ndjson
        );
    let unnest = match &overrides.unnest {
        None if compat == Compat::Dlt && json_files && context.target.path.is_none() => Unnest::All,
        None => Unnest::None,
        Some(PathsSetting::Paths(paths)) if paths.is_empty() => Unnest::None,
        Some(PathsSetting::Word(word)) if word == UNNEST_ALL => Unnest::All,
        Some(PathsSetting::Word(word)) => {
            problems.push(format!(
                "unnest = {word:?}: list the dotted paths of the list fields to unnest, or write \"{UNNEST_ALL}\""
            ));
            Unnest::None
        }
        Some(PathsSetting::Paths(paths)) => {
            problems.extend(path_problems(paths, max_depth));
            problems.extend(column_setting_problems(context, overrides, paths));
            Unnest::Paths(paths.clone())
        }
    };
    let transform = settings.transform.is_some() || defaults.transform.is_some();
    let (flatten, found) = table_flatten(
        (context.source, Some(overrides)),
        (compat, naming, transform),
    );
    problems.extend(found);
    match unnest {
        Unnest::None if overrides.max_depth.is_some() && flatten.is_none() => {
            problems.push(
                "max_depth limits the child tables unnest makes and the objects flatten flattens, and this table sets neither"
                    .to_owned(),
            );
        }
        Unnest::None => {}
        Unnest::All | Unnest::Paths(_) => {
            problems.extend(refused_writes(context, overrides, strategy));
            if row_id == Some(false) {
                problems.push("row_id = false: unnest gives every row an id, which its child rows name as their parent's; drop row_id".to_owned());
            }
        }
    }
    (
        Nesting {
            naming,
            compat,
            row_id: row_id.unwrap_or(compat == Compat::Dlt || unnest != Unnest::None),
            unnest,
            max_depth,
            flatten,
        },
        problems,
    )
}

/// How a table names what it loads: its own `naming`, else its source's, else its target's (`target`), else snake_case.
pub(crate) fn table_naming(
    (settings, defaults): (&TableSettings, &TableSettings),
    target: Option<Naming>,
) -> Naming {
    layered(&settings.naming, &defaults.naming)
        .map(|(value, _)| value)
        .or(target)
        .unwrap_or_default()
}

/// Whose names the columns a table's load adds take: its own `compat`, else its source's, else its target's (`target`), else SQLLoad's.
pub(crate) fn table_compat(
    (settings, defaults): (&TableSettings, &TableSettings),
    target: Option<Compat>,
) -> Compat {
    layered(&settings.compat, &defaults.compat)
        .map(|(value, _)| value)
        .or(target)
        .unwrap_or_default()
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

/// Why this table's writes cannot carry child tables yet: captured changes on a files target; on a files target, `"all"` (whose earlier child tables a load cannot find), a merge or `delete_insert` (which would rewrite a child table's files), and a path that would not give each child table a folder of its own (without `{table}`, or partitioned by the cursor, which child rows do not carry).
fn refused_writes(
    context: &SourceContext<'_>,
    overrides: &TableOverride,
    strategy: WriteStrategy,
) -> Vec<String> {
    let template = context.target.path.as_ref().map(|target_path| {
        overrides
            .destination_path
            .as_ref()
            .or(context.source.destination_path.as_ref())
            .unwrap_or(target_path)
    });
    let files = |refused: bool, what: String| (template.is_some() && refused).then_some(what);
    let all = matches!(&overrides.unnest, Some(PathsSetting::Word(word)) if word == UNNEST_ALL);
    let rewrites = matches!(strategy, WriteStrategy::Merge | WriteStrategy::DeleteInsert);
    let files_target = template.is_some();
    let template = template.map(String::as_str).unwrap_or_default();
    [
        (context.source.kind == SourceKind::PostgresCdc && files_target)
            .then(|| "kind = \"postgres_cdc\" on a files target".to_owned()),
        files(all, "unnest = \"all\" on a files target (list the paths)".to_owned()),
        files(
            rewrites,
            format!("write_strategy = \"{strategy}\" on a files target"),
        ),
        files(
            !template.contains(TABLE_PLACEHOLDER),
            format!("a files path without {TABLE_PLACEHOLDER} ({template:?}), as each child table needs its own folder"),
        ),
        files(
            template.contains(CURSOR_PLACEHOLDER_START),
            format!("a files path partitioned by the cursor ({template:?})"),
        ),
    ]
    .into_iter()
    .flatten()
    .map(|what| format!("unnest does not yet write child tables with {what}; drop unnest, and its lists stay JSON columns"))
    .collect()
}

/// A text column for each column `overrides` pins in `columns` or masks, one per name it loads as under `rule` (a pin and a mask may spell one column two ways), so a table whose files are gone still resolves with them.
pub(crate) fn referenced_columns(
    overrides: Option<&TableOverride>,
    rule: NameRule,
) -> Vec<SourceColumn> {
    let mut loaded: BTreeSet<String> = BTreeSet::new();
    let names: Vec<&String> = overrides
        .into_iter()
        .flat_map(|table| table.columns.keys().chain(table.mask.keys()))
        .filter(|name| loaded.insert(loaded_name(rule.0, name, rule.1)))
        .collect();
    names
        .into_iter()
        .map(|name| SourceColumn {
            name: name.clone(),
            postgres_type: String::new(),
            pg_type: PgType::Other,
            column_type: Ok(ColumnType::Text),
            nullable: true,
            source_spelling: None,
            repeat_of: None,
        })
        .collect()
}

/// Problems with `allow_columns` patterns matching an object that flattening replaced with its `<object>__<key>` columns (`objects`, as the sampled records held them) but none of its sampled `columns`, so they would silently stop applying (a setting naming the object itself keeps it whole; see [`table_flatten`]); flattening refuses such an `exclude_columns` pattern itself.
pub(crate) fn flattened_object_problems(
    (source, overrides): (&SourceConfig, Option<&TableOverride>),
    (columns, objects): (&[FileColumn], &[String]),
) -> Result<Vec<String>, String> {
    let sampled: BTreeSet<&str> = columns.iter().map(|column| column.name.as_str()).collect();
    let own = |pick: fn(&TableOverride) -> &Vec<String>| overrides.map(pick).into_iter().flatten();
    let patterns = own(|table| &table.allow_columns)
        .chain(&source.allow_columns)
        .filter(|pattern| pattern.contains(['*', '?', '[', '{']));
    let mut problems: Vec<String> = Vec::new();
    for pattern in patterns {
        let matcher = Glob::new(pattern)
            .map_err(|error| format!("invalid column pattern {pattern:?}: {error}"))?
            .compile_matcher();
        for object in objects {
            let prefix = format!("{object}{FLATTEN_SEPARATOR}");
            let keys: Vec<&str> = sampled
                .iter()
                .copied()
                .filter(|name| name.starts_with(&prefix))
                .collect();
            if keys.is_empty()
                || !matcher.is_match(object)
                || keys.iter().any(|key| matcher.is_match(key))
            {
                continue;
            }
            problems.push(format!(
                "allow_columns {pattern} names {object}, which flatten replaces with {}; name those columns instead, or keep {object} whole with columns = {{ {object} = \"json\" }}",
                keys.join(", ")
            ));
        }
    }
    Ok(problems)
}

/// The source spellings of `spec`'s cursor columns when every one is an integer; `None` otherwise.
pub(crate) fn integer_cursor(spec: &TableSpec) -> Option<Vec<String>> {
    let mut columns: Vec<String> = Vec::new();
    for name in spec.cursor.as_ref()?.columns() {
        let column = spec
            .source_table
            .columns
            .iter()
            .find(|column| column.name == name)?;
        if !matches!(
            column.column_type,
            Ok(ColumnType::Int | ColumnType::SmallInt)
        ) {
            return None;
        }
        columns.push(
            column
                .source_spelling
                .clone()
                .unwrap_or_else(|| column.name.clone()),
        );
    }
    Some(columns)
}

/// The key a commit by `mode` refuses a NULL in under `null_keys = "fail"`, and the setting naming it: the key the mode matches rows on, or, under `--reload` (which builds the table anew and matches nothing), the key `spec`'s strategy matches on, so a reload never lets in a row its loads refuse.
pub(crate) fn checked_key<'a>(
    spec: &'a TableSpec,
    mode: &'a WriteMode,
) -> Option<(&'a [String], &'static str)> {
    match (mode, spec.strategy) {
        (WriteMode::Recreate, WriteStrategy::Merge) => Some((&spec.unique_key, "unique_key")),
        (WriteMode::Recreate, WriteStrategy::DeleteInsert) if !spec.replace_key.is_empty() => {
            Some((&spec.replace_key, "replace_key"))
        }
        _ => mode.staged_key(),
    }
}

/// What files source `source_name` fills from each file's path and its configuration, and the types of those columns in table order: `partition_columns`, `path_columns` captures, then its table's `constant_columns`; a `native(...)` type, which SQLLoad's reader cannot build, is refused.
pub(crate) fn path_values(
    (source_name, source): (&str, &SourceConfig),
    warehouse: Warehouse,
) -> Result<(PathValues, Vec<(String, ColumnType)>), LoaderError> {
    let mut types: Vec<(String, ColumnType)> = Vec::new();
    for (name, text) in &source.partition_columns {
        let parsed = parse_type(text, &[warehouse])
            .map_err(|error| LoaderError::Invalid(format!("partition column {name}: {error}")))?;
        if matches!(parsed.column_type, ColumnType::Array(_)) {
            return Err(LoaderError::Invalid(format!(
                "partition column {name}: an array cannot come from a path"
            )));
        }
        types.push((name.clone(), parsed.column_type));
    }
    let template = source
        .path_columns
        .as_deref()
        .map(|text| path_template(text, &[warehouse]))
        .transpose()
        .map_err(LoaderError::Invalid)?;
    types.extend(
        template
            .iter()
            .flat_map(|template| template.captures.clone()),
    );
    let constants = constant_columns(&source.table_constants(source_name), &[warehouse])
        .map_err(LoaderError::Invalid)?;
    let typed = types
        .iter()
        .map(|(name, column_type)| (name, column_type))
        .chain(
            constants
                .iter()
                .map(|(name, column_type, _)| (name, column_type)),
        );
    for (name, column_type) in typed {
        if arrow_field(name, column_type).is_none() {
            return Err(LoaderError::Invalid(format!(
                "column {name}: {column_type} has no Arrow type, so a partition, path or constant column cannot be one; pin a portable type"
            )));
        }
    }
    let values = PathValues {
        partition_columns: source.partition_columns.keys().cloned().collect(),
        template,
        unmatched: source.unmatched_paths.unwrap_or_default(),
        constants: constants
            .iter()
            .map(|(name, _, text)| (name.clone(), text.clone()))
            .collect(),
    };
    types.extend(
        constants
            .into_iter()
            .map(|(name, column_type, _)| (name, column_type)),
    );
    Ok((values, types))
}

/// Columns a JSON or CSV source's table pins in `columns` that sampling did not see, so a key found only in older files can be pinned; a name close to a sampled one (a typo, or another case) is an error, and a `mask` key alone never adds a column. A pin naming a sampled column by the name `rule`'s naming gives it is one sampling saw.
pub(crate) fn pinned_unsampled(
    overrides: Option<&TableOverride>,
    (table, added, rule): (&FilesTable, &[&str], NameRule),
    format: SourceFormat,
) -> Result<Vec<String>, LoaderError> {
    let Some(overrides) = overrides.filter(|_| format != SourceFormat::Parquet) else {
        return Ok(Vec::new());
    };
    let sampled: Vec<&str> = table
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let loaded = |name: &str| loaded_name(rule.0, name, rule.1);
    let seen: Vec<String> = sampled
        .iter()
        .chain(added)
        .map(|name| loaded(name))
        .collect();
    let mut unsampled: Vec<String> = Vec::new();
    for name in overrides.columns.keys() {
        if seen.contains(&loaded(name)) || is_child_column(name, overrides) {
            continue;
        }
        let known = sampled.iter().chain(added).copied();
        if let Some(close) = close_name(name, known) {
            return Err(LoaderError::Invalid(format!(
                "pinned column {name} was not seen in the sampled files; did you mean {close:?}? (pin a name exactly as it appears in the files)"
            )));
        }
        unsampled.push(name.clone());
    }
    Ok(unsampled)
}

/// The read columns of a files source: the file's types, or the pinned ones (converted while reading); a parsed column reads as text, for `parse` to convert.
pub(crate) fn files_read_columns(spec: &TableSpec) -> Result<Vec<ReadColumn>, Vec<String>> {
    let mut problems: Vec<String> = Vec::new();
    let mut columns: Vec<ReadColumn> = Vec::new();
    for column in &spec.source_table.columns {
        let excluded = spec
            .excluded_columns
            .iter()
            .any(|(excluded, _)| *excluded == column.name);
        let output = match (spec.pinned.get(&column.name), &column.column_type) {
            _ if excluded => continue,
            _ if spec.parses.contains_key(&column.name) => ColumnType::Text,
            (Some(pinned), _) => pinned.clone(),
            (None, Ok(column_type)) if read_as_text(spec, &column.name, column_type) => {
                ColumnType::Text
            }
            (None, Ok(column_type)) => column_type.clone(),
            (None, Err(reason)) => {
                problems.push(format!("column {}: {reason}", column.name));
                continue;
            }
        };
        columns.push(ReadColumn {
            name: column.name.clone(),
            expression: column.name.clone(),
            wire: WireType::Text,
            output,
        });
    }
    match problems.is_empty() {
        true => Ok(columns),
        false => Err(problems),
    }
}

/// The function and the host that run table `name`'s `transform` (its own, else its source's), when it has one the host imported.
pub(crate) fn runnable_transform(
    project: &ProjectConfig,
    source: &SourceConfig,
    name: &str,
) -> Option<(Arc<dyn PythonHost>, String)> {
    let function = source
        .tables
        .get(name)
        .and_then(|table| table.settings.transform.clone())
        .or_else(|| source.defaults.transform.clone())?;
    let host = project.python_host.clone()?;
    matches!(project.transforms.get(&function), Some(Ok(_))).then_some((host, function))
}

/// The columns of what a `transform` returned across its batches, typed as a Python table's yields are: a column only ever NULL as text.
pub(crate) fn returned_columns(batches: &[RecordBatch]) -> Vec<(String, ColumnType)> {
    let merged = batches.iter().fold(Schema::empty(), |seen, batch| {
        merge_schema(&seen, &batch.schema())
    });
    merged
        .fields()
        .iter()
        .map(|field| {
            let column_type = match field.data_type() {
                DataType::Null => ColumnType::Text,
                _ => field_type(field),
            };
            (field.name().clone(), column_type)
        })
        .collect()
}

/// How each file of a files table with a `transform` is read for it: `table`'s sampled columns, less those `exclude_columns` drops (`patterns` holds it and `allow_columns`), as stored (Parquet) or as the workbook holds them (xlsx), JSON's nested values as JSON and every other value as text, so a value sampling did not see never fails the read.
pub(crate) fn transform_input(
    table: &FilesTable,
    format: SourceFormat,
    (exclude, allow): (Vec<String>, Vec<String>),
) -> Result<FilesTransform, LoaderError> {
    let excluded = column_matcher(&exclude).map_err(LoaderError::Invalid)?;
    let allowed = column_matcher(&allow).map_err(LoaderError::Invalid)?;
    let mut input: Vec<DesiredColumn> = Vec::new();
    for column in &table.columns {
        if excluded.is_match(&column.name) && !allowed.is_match(&column.name) {
            continue;
        }
        let sampled = column
            .column_type
            .clone()
            .map_err(|reason| LoaderError::Invalid(format!("column {}: {reason}", column.name)));
        let column_type = match (format, sampled) {
            (SourceFormat::Parquet | SourceFormat::Xlsx, sampled) => sampled?,
            (
                SourceFormat::Json | SourceFormat::Ndjson,
                Ok(nested @ (ColumnType::Json | ColumnType::Array(_))),
            ) => nested,
            _ => ColumnType::Text,
        };
        input.push(DesiredColumn {
            name: column.name.clone(),
            column_type,
            origin: ColumnOrigin::Source,
        });
    }
    Ok(FilesTransform {
        checks: TextChecks {
            known: input.iter().map(|column| column.name.clone()).collect(),
            exclude,
            allow,
            inferred: Vec::new(),
        },
        input,
        nanos: table
            .columns
            .iter()
            .filter(|column| column.stored_read == StoredRead::Nanos)
            .map(|column| column.name.clone())
            .collect(),
        sample: String::new(),
    })
}
