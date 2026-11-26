use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};

use crate::adapters::contract::models::{TableName, WriteMode};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::bigquery_source::constants::BIGQUERY_SOURCE;
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::main::is_variant::is_variant;
use crate::column_types::main::parse_type::parse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::databricks_source::constants::DATABRICKS_SOURCE;
use crate::duckdb_source::constants::{DUCKDB_SOURCE, MOTHERDUCK_SOURCE};
use crate::files_source::main::constant_columns::constant_columns;
use crate::files_source::main::path_template::path_template;
use crate::files_source::models::FileColumn;
use crate::files_source::models::{FilesTable, PathValues, SourceFormat, StoredRead, TextChecks};
use crate::load_naming::main::loaded_name::loaded_name;
use crate::load_naming::models::{LoadedNames, NameRules, Naming};
use crate::loader::_helpers::destination::read_as_text;
use crate::loader::_helpers::python::field_type;
use crate::loader::_helpers::resolve::{NameRule, ResolvedTarget};
use crate::loader::_helpers::sources::sheet_rule;
use crate::loader::_helpers::table_plan::source_cursor;
use crate::loader::constants::{
    ODBC_SOURCE, PRIMARY_KEY, PRIMARY_KEY_PLACEHOLDER, PYTHON_MISSING_COLUMN, SNOWFLAKE_SOURCE,
    UNNEST_ALL,
};
use crate::loader::errors::LoaderError;
use crate::loader::models::{
    CursorSpec, FilesTransform, TableSpec, WarehouseRoute, WarehouseSource,
};
use crate::loader::models::{SourceReader, TablePlan};
use crate::masking::models::MaskRules;
use crate::postgres_source::models::{PgType, SourceColumn, SourceTable};
use crate::postgres_source::models::{ReadColumn, WireType};
use crate::project::models::{
    NullCursor, NullKeys, PathsSetting, ProjectConfig, SourceConfig, SourceKind, StateMode,
    TableOverride, TableSettings, UnloadStage, WriteStrategy,
};
use crate::python_sources::types::PythonHost;
use crate::routing::models::Store;
use crate::row_batches::constants::FLATTEN_SEPARATOR;
use crate::row_batches::main::merge_schema::merge_schema;
use crate::row_deletes::main::deletes_spec::deletes_spec;
use crate::schema_plan::main::missing_columns_warnings::missing_columns_warnings;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::sheets_source::models::SheetRead;
use crate::similar_names::main::close_name::close_name;
use crate::similar_names::main::nearest_name::nearest_name;
use crate::snowflake_source::constants::USER_STAGE_FOLDER;
use crate::table_options::main::layered::layered;
use crate::table_options::main::placeholder_problem::placeholder_problem;
use crate::table_options::main::table_options::table_options;
use crate::table_options::main::table_strategy::table_strategy;
use crate::table_options::models::{OptionContext, OptionTarget, TableOptions, TableStrategy};
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
    let target = option_target(context.target);
    let options_context = OptionContext {
        name: context.name,
        source: context.source,
        target: &target,
    };
    let chosen = match table_strategy(&options_context, overrides) {
        Ok(chosen) => chosen,
        Err(cursor) => {
            return Err(vec![missing_strategy_problem(
                context,
                table,
                cursor.as_deref(),
            )]);
        }
    };
    let (unique_key, key_from_primary_key) =
        match table_key(context, table, (settings, defaults), chosen.merges) {
            Ok(key) => key,
            Err(problem) => return Err(vec![problem]),
        };
    let (options, mut problems) = table_options(&options_context, overrides, &chosen, &unique_key);
    if let Some(cdc) = &chosen.cdc
        && sql_adapter(context.target.warehouse).captures_changes(cdc.apply)
    {
        problems.extend(cdc_key_problems(table, &unique_key));
    }
    let TableStrategy {
        cursor,
        cdc,
        strategy_origin,
        replace_key,
        keyed,
        ..
    } = chosen;
    let strategy = options.strategy;
    problems.extend(column_reference_problems(table, overrides));
    problems.extend(typed_parse_problems(context.source.kind, table, overrides));
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
    let matched_key = match keyed {
        true => &replace_key,
        false => &unique_key,
    };
    let (null_keys, not_null_keys) = key_nulls((table, &constants), matched_key, options.null_keys);
    let batch = layered(&settings.batch_size, &defaults.batch_size).map(|(value, _)| value);
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
    let from = overrides.deletes_from.as_ref();
    let (deletes, checks) = deletes_spec(
        (options.deletes.filter(|_| cdc.is_none()), from),
        options.checks,
    );
    let is_duckdb = sql_adapter(context.target.warehouse).parses_variant_pins();
    let TableOptions {
        on_duplicate_keys,
        newest_by,
        overlapping,
        nesting,
        layout,
        parses,
        timezone,
        ..
    } = options;
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
                route: WarehouseRoute::Query {
                    same_connection: kind == SourceKind::BigQuery
                        && context.target.session_connection.as_ref()
                            == Some(&context.source.connection),
                },
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
        missing_columns: Vec::new(),
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
                Store::ObjectStore(format!("{}/", url.trim_end_matches('/'))),
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

/// A table's `unique_key` (table over source) and whether a `merge` takes it from the primary key its source reports, set as `"{primary_key}"`. A `merge` table without one gets none, so it is refused; `"{primary_key}"` on it is refused for a source that reports no keys, and for a table whose source reports none. A table that doesn't merge (a `postgres_cdc` table included) keeps the primary key its source reports unless it names columns: it only orders and identifies rows (a ClickHouse table's sorting key, a CDC table's merge key), so a source's `"{primary_key}"` never fails it.
fn table_key(
    context: &SourceContext<'_>,
    table: &SourceTable,
    (settings, defaults): (&TableSettings, &TableSettings),
    merges: bool,
) -> Result<(Vec<String>, bool), String> {
    let set = layered(&settings.unique_key, &defaults.unique_key).map(|(key, _)| key);
    let qualified = table.qualified_name();
    if let Some(problem) =
        placeholder_problem(context.source.kind, &qualified, set.as_deref(), merges)
    {
        return Err(problem);
    }
    let placeholder = set
        .as_ref()
        .is_some_and(|key| key.iter().any(|column| column == PRIMARY_KEY_PLACEHOLDER));
    match set {
        None if merges => Ok((Vec::new(), false)),
        None => Ok((table.primary_key.clone(), false)),
        Some(key) if !placeholder => Ok((key, false)),
        Some(_) if !merges => Ok((table.primary_key.clone(), false)),
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

/// A table's `null_keys` (as set, else the default) and the columns of `unique_key` (the key it matches rows on: a merge's `unique_key`, a `delete_insert`'s `replace_key` when keyed) whose values are never NULL, so they compare with `=`: the source table's columns it declares NOT NULL and constant columns, or every key column under `null_keys = "fail"`, which refuses a commit holding a NULL in one (a `postgres_cdc` table's key, which its merge matches on, included).
fn key_nulls(
    (table, constants): (&SourceTable, &[(String, ColumnType, String)]),
    unique_key: &[String],
    null_keys: Option<NullKeys>,
) -> (NullKeys, Vec<String>) {
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
    (null_keys, not_null)
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

/// Columns a files source's table pins in `columns` that no sampled file holds (its JSON or CSV records, or its Parquet footers), which load as NULL where the files lack them unless `on_schema_change = "fail"` refuses them, so a key found only in older files can be pinned; a name close to a sampled one (a typo, or another case) is an error, and a `mask` key alone never adds a column. A pin naming a sampled column by the name `rule`'s naming gives it is one sampling saw.
pub(crate) fn pinned_unsampled(
    overrides: Option<&TableOverride>,
    (table, added, rule): (&FilesTable, &[&str], NameRule),
) -> Result<Vec<String>, LoaderError> {
    let Some(overrides) = overrides else {
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

/// The declared columns none of a Python table's `rows` rows had (its yielded fields merged as `merged`), and a warning for each; or, under `on_schema_change = "fail"`, the failure. A table that yielded no rows lacks none.
pub(crate) fn python_missing(
    spec: &TableSpec,
    merged: &Schema,
    rows: u64,
) -> Result<(Vec<String>, Vec<String>), LoaderError> {
    let missing = match rows {
        0 => Vec::new(),
        _ => missing_declared(spec, merged),
    };
    let warnings = missing_columns_warnings(&missing, PYTHON_MISSING_COLUMN, spec.on_schema_change)
        .map_err(LoaderError::Invalid)?;
    Ok((missing, warnings))
}

/// The columns `spec` declares (pinned in `columns`, or parsed) that no field of `merged` is, ignoring case, by the name each loads as; a child table's and a masked column's are left out, as a masked column the rows lack fails the table already.
fn missing_declared(spec: &TableSpec, merged: &Schema) -> Vec<String> {
    let had = |name: &str| {
        merged
            .fields()
            .iter()
            .any(|field| field.name().eq_ignore_ascii_case(name))
    };
    let mut missing: Vec<String> = Vec::new();
    for name in spec.pinned.keys().chain(spec.parses.keys()) {
        let left_out =
            spec.child_pinned.contains_key(name) || spec.masked.columns.contains_key(name);
        if !left_out && !had(name) && !missing.contains(name) {
            missing.push(name.clone());
        }
    }
    missing
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

/// What the table's options read of `target`.
fn option_target(target: &ResolvedTarget) -> OptionTarget {
    OptionTarget {
        warehouse: target.warehouse,
        path: target.path.clone(),
        naming: target.naming,
        compat: target.compat,
    }
}
