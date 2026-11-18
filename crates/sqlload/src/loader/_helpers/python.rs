use crate::adapters::contract::constants::STAGING_SUFFIX;
use crate::adapters::contract::main::cursor_text::cursor_text;
use crate::adapters::contract::main::newest_by_problem::newest_by_problem;
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::models::WriteMode;
use crate::adapters::contract::models::WriteStatementsRequest;
use crate::adapters::contract::models::{ChangeApply, ChangeWrite};
use crate::adapters::dispatch::main::dialect::files_destination::files_destination;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::execute_ddl::execute_ddl;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::main::session::single_text::single_text;
use crate::adapters::dispatch::main::session::table_names::table_names;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
use crate::batch_plan::main::outside_text::outside_text;
use crate::batch_plan::main::window_at::window_at;
use crate::batch_plan::main::window_rows::window_rows;
use crate::batch_plan::models::CursorWindow;
use crate::batch_plan::models::{OutsideRows, WindowRows};
use crate::cdc::constants::DELETE_OP;
use crate::column_parse::main::parsed_batch::parsed_batch;
use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::main::binary_text::binary_columns_as_text;
use crate::column_types::main::combined_type::combined_type;
use crate::column_types::models::ColumnType;
use crate::http_source::main::literal_path_problems::literal_path_problems;
use crate::http_source::models::LiteralPaths;
use crate::load_naming::constants::CHILD_TABLE_SEPARATOR;
use crate::load_naming::main::child_table_name::child_table_name;
use crate::load_naming::main::metadata_name::metadata_name;
use crate::load_naming::main::name_lines::name_lines;
use crate::load_naming::main::named_batch::named_batch;
use crate::load_naming::models::{Compat, LoadedNames, MetadataRole, NameMemo, RenameColumn};
use crate::loader::_helpers::publish::replaces_roots;
use crate::loader::_helpers::publish::staged_columns;
use crate::loader::_helpers::resolve::{KnownNames, host_rename, resolve_known};
use crate::loader::_helpers::run_log::{log_empty_run, record_numbered, run_entry};
use crate::loader::_helpers::specs::is_child_column;
use crate::loader::_helpers::specs::null_cursor_problem;
use crate::loader::_helpers::table_plan::append_problem;
use crate::loader::_helpers::table_plan::replan_columns;
use crate::loader::_helpers::table_plan::schema_statements;
use crate::loader::constants::DROPPED_CHUNK;
use crate::loader::constants::EMPTY_REPLACE_PROBLEM;
use crate::loader::constants::ENFORCED_UNDECLARED_PROBLEM;
use crate::loader::constants::NO_DATA_NOTE;
use crate::loader::constants::NO_STATE;
use crate::loader::constants::PYTHON_COLUMN_TYPE;
use crate::loader::constants::SHOWN_OUTSIDE_WINDOWS;
use crate::loader::constants::UNKNOWN_CURSOR_TYPE;
use crate::loader::errors::LoaderError;
use crate::loader::main::_plan_files::plan_files_table;
use crate::loader::models::ChildFeed;
use crate::loader::models::ChildTables;
use crate::loader::models::CommitRange;
use crate::loader::models::CommitRule;
use crate::loader::models::PlanOptions;
use crate::loader::models::Publisher;
use crate::loader::models::PythonFeed;
use crate::loader::models::PythonLoad;
use crate::loader::models::PythonTable;
use crate::loader::models::ReadWindow;
use crate::loader::models::TableLayout;
use crate::loader::models::TableLoaded;
use crate::loader::models::TablePlan;
use crate::loader::models::TableSpec;
use crate::masking::models::MaskRules;
use crate::memory_budget::constants::TABLE_SHARE_PARTS;
use crate::number_text::main::thousands::thousands;
use crate::postgres_source::models::PgType;
use crate::postgres_source::models::SourceColumn;
use crate::postgres_source::models::SourceTable;
use crate::project::constants::FILES_CONNECTION_KINDS;
use crate::project::models::CheckName;
use crate::project::models::Contract;
use crate::project::models::NullCursor;
use crate::project::models::OnCursorOutOfRange;
use crate::project::models::OnSchemaChange;
use crate::project::models::ProjectConfig;
use crate::project::models::SourceConfig;
use crate::publish_checks::main::check_column_problems::check_column_problems;
use crate::python_sources::models::CursorRead;
use crate::python_sources::models::TableRead;
use crate::python_sources::types::BatchStream;
use crate::python_sources::types::PythonHost;
use crate::python_sources::types::Streamed;
use crate::row_batches::main::choose_lists::choose_lists;
use crate::row_batches::main::conform_batch::conform_batch;
use crate::row_batches::main::flattened_columns::flattened_columns;
use crate::row_batches::main::merge_schema::merge_schema;
use crate::row_batches::main::read_types::read_types;
use crate::row_batches::main::settled_schema::settled_schema;
use crate::row_batches::models::ChildBatch;
use crate::row_batches::models::Flatten;
use crate::row_batches::models::Unnest;
use crate::row_batches::models::Unnested;
use crate::row_batches::types::ListChoices;
use crate::schema_plan::constants::ROOT_ROW_COLUMN;
use crate::schema_plan::constants::{CDC_LSN_COLUMN, CDC_OP_COLUMN, CDC_UNCHANGED_COLUMN};
use crate::schema_plan::models::DesiredColumn;
use crate::schema_plan::models::LiveColumn;
use crate::schema_plan::models::SchemaPlan;
use crate::schema_plan::models::TableAction;
use crate::similar_names::main::close_name::close_name;
use arrow_array::Array;
use arrow_array::BooleanArray;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_array::cast::AsArray;
use arrow_cast::cast::CastOptions;
use arrow_cast::cast::cast_with_options;
use arrow_cast::display::ArrayFormatter;
use arrow_cast::display::FormatOptions;
use arrow_ord::sort::SortOptions;
use arrow_ord::sort::sort_to_indices;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use arrow_schema::TimeUnit;
use arrow_select::filter::filter_record_batch;
use chrono_tz::Tz;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

/// A Python source's tables as planning sees them before any data: the declared (`columns`), masked and parsed columns only.
pub(crate) fn python_tables(source: &SourceConfig) -> Vec<SourceTable> {
    let mut tables: Vec<SourceTable> = Vec::new();
    for (name, table) in &source.tables {
        let mut columns: Vec<SourceColumn> = Vec::new();
        for column in table
            .columns
            .keys()
            .chain(table.mask.keys())
            .chain(table.parse.keys())
        {
            if !columns.iter().any(|known| known.name == *column) && !is_child_column(column, table)
            {
                columns.push(python_column(column, ColumnType::Text));
            }
        }
        tables.push(SourceTable {
            split_pages: None,
            schema: String::new(),
            name: name.clone(),
            columns,
            primary_key: Vec::new(),
            indexed_columns: Vec::new(),
            estimated_rows: -1,
            estimated_bytes: -1,
            comment: None,
            listed_keys: Vec::new(),
        });
    }
    tables
}

/// What a Python table's spec records about it, with the `sl.http` paths its function writes as literals among `paths`, by table name.
pub(crate) fn python_table(
    module: &str,
    table: &SourceTable,
    paths: Option<&BTreeMap<String, LiteralPaths>>,
) -> PythonTable {
    PythonTable {
        module: module.to_owned(),
        declared: table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        lists: ListChoices::new(),
        literal_paths: paths.and_then(|paths| paths.get(&table.name)).cloned(),
    }
}

/// Why a Python table can't load, found before it is planned: an enforced contract with no declared columns, or an `sl.http` path its function writes as a literal that load would refuse.
pub(crate) fn python_problems(spec: &TableSpec) -> Vec<String> {
    let Some(python) = &spec.python else {
        return Vec::new();
    };
    let mut problems: Vec<String> = Vec::new();
    if python.declared.is_empty() && spec.contract == Contract::Enforced {
        problems.push(ENFORCED_UNDECLARED_PROBLEM.to_owned());
    }
    problems.extend(python.literal_paths.iter().flat_map(literal_path_problems));
    problems
}

/// The child tables `spec`'s table has in the destination (tables named `<table>__...` that have a parent-id column, staging tables and the swap tables named after them, in any case, left out); none without `unnest`. A table named so without that column is another table, and so are the child tables of such a table (`orders__notes__items` beside a table `orders__notes`).
pub(crate) async fn live_child_tables(
    destination: &Destination,
    spec: &TableSpec,
) -> Result<Vec<String>, LoaderError> {
    if spec.unnest == Unnest::None || spec.destination_path.is_some() {
        return Ok(Vec::new());
    }
    let adapter = sql_adapter(spec.warehouse);
    let found = table_names(
        destination,
        adapter.list_child_tables_sql(&spec.destination),
    )
    .await?;
    let (children, others): (Vec<&String>, Vec<&String>) =
        found.iter().partition(|marked| marked.starts_with('1'));
    let others: Vec<&str> = others.iter().filter_map(|marked| marked.get(1..)).collect();
    Ok(children
        .into_iter()
        .filter_map(|marked| marked.get(1..))
        .filter(|name| !name.to_ascii_lowercase().contains(STAGING_SUFFIX))
        .filter(|name| !is_child_of_any(name, &others))
        .map(str::to_owned)
        .collect())
}

/// Whether `table` is named as a child table of one of `parents` is (`orders__notes__items` of `orders__notes`).
fn is_child_of_any(table: &str, parents: &[&str]) -> bool {
    parents.iter().any(|parent| {
        table
            .strip_prefix(parent)
            .is_some_and(|rest| rest.starts_with(CHILD_TABLE_SEPARATOR))
    })
}

/// The dotted path each of `spec`'s child tables among `tables` holds, by its name as the load writes it (`orders__items__parts` holds `items.parts`; Snowflake's `ORDERS__ITEMS` holds `items`; Postgres lists the children of `ORDERS` folded, as `orders__items`): the path of a `child_pinned` column whose table it is, or else the name's parts. Only the pins know a path whose field holds `__` (a list in an object `compat = "dlt"` flattened: `orders__items__dims__tags` holds `items.dims__tags`) or a name dlt shortened.
pub(crate) fn child_paths(spec: &TableSpec, tables: Vec<String>) -> Vec<(String, String)> {
    let pinned = pinned_tables(spec, &tables);
    tables
        .into_iter()
        .filter_map(|table| {
            let path = pinned
                .get(&table)
                .cloned()
                .or_else(|| named_child_path(spec, &table))?;
            Some((path, table))
        })
        .collect()
}

/// Each child table path `spec`'s `child_pinned` keys name: every dotted prefix of a key (`items.dims__tags.value` names `items` and `items.dims__tags`).
fn pinned_child_paths(spec: &TableSpec) -> BTreeSet<&str> {
    let mut paths: BTreeSet<&str> = BTreeSet::new();
    for key in spec.child_pinned.keys() {
        for (at, _) in key.match_indices('.') {
            paths.insert(&key[..at]);
        }
    }
    paths
}

/// Each of `tables` (as the warehouse lists them) that a `child_pinned` path's child table is, with that path: the table spelt exactly as the path's is, or else, when no pin spells it exactly, one spelt so in another case (Postgres lists an upper-case parent's children folded), so `items` and `Items` keep their own paths.
fn pinned_tables(spec: &TableSpec, tables: &[String]) -> BTreeMap<String, String> {
    let adapter = sql_adapter(spec.warehouse);
    let named: Vec<(&str, String)> = pinned_child_paths(spec)
        .into_iter()
        .map(|path| (path, adapter.stored_table_name(&unnested_table(spec, path))))
        .collect();
    let mut found: BTreeMap<String, String> = BTreeMap::new();
    for exact in [true, false] {
        for (path, name) in &named {
            for table in tables {
                let spelt = match exact {
                    true => table == name,
                    false => table.eq_ignore_ascii_case(name),
                };
                if spelt {
                    found
                        .entry(table.clone())
                        .or_insert_with(|| (*path).to_owned());
                }
            }
        }
    }
    found
}

/// The path `table`'s name gives, its parts below `spec`'s table joined by dots; `None` when it is not named as a child of that table.
fn named_child_path(spec: &TableSpec, table: &str) -> Option<String> {
    let prefix = format!("{}{CHILD_TABLE_SEPARATOR}", spec.destination.table);
    if !table.starts_with(&sql_adapter(spec.warehouse).stored_table_name(&prefix)) {
        return None;
    }
    Some(
        source_spelling(table, spec)
            .get(prefix.len()..)?
            .replace(CHILD_TABLE_SEPARATOR, "."),
    )
}

/// `spec` with each top-level field whose child table is among `children` chosen as a list field under `unnest = "all"`: an earlier load chose it, so this one keeps it one whatever its sampled values show (a value that is not a list then fails the load).
pub(crate) fn with_live_children(spec: &TableSpec, children: &[String]) -> TableSpec {
    let mut chosen = spec.clone();
    if spec.unnest != Unnest::All || children.is_empty() {
        return chosen;
    }
    let adapter = sql_adapter(spec.warehouse);
    let live = |field: &str| {
        let table = child_table_name(
            (spec.naming, adapter.max_identifier_length()),
            &spec.destination.table,
            field,
        );
        children.contains(&adapter.stored_table_name(&table))
    };
    if let Some(files) = &mut chosen.files {
        for column in &mut files.table.columns {
            if live(&column.name) {
                column.lists = Some(true);
            }
        }
    }
    if let Some(python) = &mut chosen.python {
        let prefix = adapter.stored_table_name(&format!(
            "{}{CHILD_TABLE_SEPARATOR}",
            spec.destination.table
        ));
        for name in children {
            let field = name
                .strip_prefix(&prefix)
                .filter(|field| !field.contains(CHILD_TABLE_SEPARATOR));
            if let Some(field) = field {
                python.lists.insert(field.to_owned(), true);
            }
        }
    }
    chosen
}

/// Under `unnest = "all"`, the top-level columns the plan chose as list fields or not: from the sampled files, or from a Python table's rows so far.
pub(crate) fn list_choices(spec: &TableSpec) -> ListChoices {
    if spec.unnest != Unnest::All {
        return ListChoices::new();
    }
    let sampled = spec
        .files
        .iter()
        .flat_map(|files| &files.table.columns)
        .filter_map(|column| {
            Some((
                spec.source_table.loaded_as(&column.name).to_owned(),
                column.lists?,
            ))
        });
    let yielded = spec.python.iter().flat_map(|python| python.lists.clone());
    sampled.chain(yielded).collect()
}

/// A Python table's spec with the live table's data columns added (as the source has them, so `plan` shows no change for them), or `None` when there are none to add; its `constant_columns` are never the source's.
pub(crate) fn with_live_columns(
    spec: &TableSpec,
    live: Option<&[LiveColumn]>,
) -> Option<TableSpec> {
    spec.python.as_ref()?;
    let mut added: Vec<SourceColumn> = Vec::new();
    let constant = |name: &str| {
        spec.constants
            .iter()
            .any(|(constant, _, _)| constant.eq_ignore_ascii_case(name))
    };
    for column in live? {
        if !is_metadata(spec.compat, &column.name)
            && !constant(&column.name)
            && !has_column(&spec.source_table, &column.name)
        {
            let name = source_spelling(&column.name, spec);
            added.push(python_column(&name, column.column_type.clone()));
        }
    }
    if added.is_empty() {
        return None;
    }
    let mut augmented = spec.clone();
    augmented.source_table.columns.extend(added);
    Some(augmented)
}

/// The name a table function most likely yields for live column `stored`: its lower-case form when the warehouse stores that as `stored` (upper case on Snowflake), else `stored` itself.
pub(crate) fn source_spelling(stored: &str, spec: &TableSpec) -> String {
    let lower = stored.to_lowercase();
    match sql_adapter(spec.warehouse).stored_name(&lower) == stored {
        true => lower,
        false => stored.to_owned(),
    }
}

/// What the yielded data showed: its merged schema, the columns that only ever held NULL, and the rows.
pub(crate) struct YieldedData<'a> {
    pub(crate) schema: &'a Schema,
    pub(crate) null_columns: &'a BTreeSet<String>,
    /// True on the last plan, once every row is in: every column is typed then, and every newest_by column must be written unless there are no rows to merge.
    pub(crate) settled: bool,
    pub(crate) rows: u64,
}

/// A Python table as its yielded data shows it: every column of the data typed by inference (the live table's type when the inferred one fits it, or when the column only held NULL), plus declared columns no row had.
pub(crate) fn inferred_table(
    (planned, compat): (&SourceTable, Compat),
    declared: &[String],
    data: &YieldedData<'_>,
    live: Option<&[LiveColumn]>,
) -> SourceTable {
    let mut columns: Vec<SourceColumn> = Vec::new();
    for field in data.schema.fields() {
        if !is_metadata(compat, field.name()) {
            let live_type = live_type(live, field.name());
            let column_type = match (data.null_columns.contains(field.name()), live_type) {
                (true, Some(live_type)) => live_type,
                (_, live_type) => fitting_type(field_type(field), live_type),
            };
            columns.push(python_column(field.name(), column_type));
        }
    }
    for name in declared {
        let seen = columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(name));
        let planned_column = planned.columns.iter().find(|column| column.name == *name);
        if let (false, Some(column)) = (seen, planned_column) {
            columns.push(column.clone());
        }
    }
    SourceTable {
        columns,
        ..planned.clone()
    }
}

/// Why the declared `columns`, `mask` and `parse` keys do not match the data: a key that names no column but is close to one (a different case or a typo), or a masked column the data lacks while it has rows; masking must never miss the real column.
pub(crate) fn declared_problems(spec: &TableSpec, data: &YieldedData<'_>) -> Vec<String> {
    let names: Vec<&str> = data
        .schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .filter(|name| !is_metadata(spec.compat, name))
        .collect();
    let keys = spec
        .pinned
        .keys()
        .filter(|key| !spec.masked.columns.contains_key(*key))
        .map(|key| ("columns", key))
        .chain(spec.masked.columns.keys().map(|key| ("mask", key)))
        .chain(spec.parses.keys().map(|key| ("parse", key)));
    let mut problems: Vec<String> = Vec::new();
    for (setting, key) in keys {
        if names.contains(&key.as_str()) {
            continue;
        }
        match (close_name(key, names.iter().copied()), setting) {
            (Some(close), _) => problems.push(format!(
                "{setting} key {key} names no column of the data; did you mean {close}?"
            )),
            (None, "mask") if data.rows > 0 => {
                problems.push(format!("mask {key} names no column of the data"));
            }
            (None, _) => {}
        }
    }
    problems
}

/// The live table's type of column `name`, if it has one.
fn live_type(live: Option<&[LiveColumn]>, name: &str) -> Option<ColumnType> {
    live?
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))
        .map(|column| column.column_type.clone())
}

/// The live type when the inferred values fit it (pinned > live > inferred), else the inferred type, which schema planning then widens or blocks.
pub(crate) fn fitting_type(inferred: ColumnType, live: Option<ColumnType>) -> ColumnType {
    match live {
        Some(live) if combined_type(live.clone(), inferred.clone()) == live => live,
        _ => inferred,
    }
}

fn has_column(table: &SourceTable, name: &str) -> bool {
    table
        .columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(name))
}

/// The planned type of the cursor column (matched ignoring case, as warehouses store names), `unknown` before the table's first load.
pub(crate) fn cursor_type_name(table: &TablePlan, column: &str) -> String {
    match table
        .read_columns
        .iter()
        .find(|read| read.name.eq_ignore_ascii_case(column))
    {
        Some(read) => read.output.to_string(),
        None => UNKNOWN_CURSOR_TYPE.to_owned(),
    }
}

/// The schema changes the load made, one line each, as `apply` would print them.
pub(crate) fn change_lines(table: &TablePlan) -> Vec<String> {
    let name = sql_adapter(table.spec.warehouse).render_display_name(&table.spec.destination);
    match &table.schema.action {
        TableAction::Create => vec![format!(
            "created  {name}  {} columns",
            table.schema.write_columns.len()
        )],
        TableAction::Alter(changes) => changes
            .iter()
            .map(|change| format!("altered  {name}  {change}"))
            .collect(),
        TableAction::Recreate | TableAction::Unchanged => Vec::new(),
    }
}

/// Fail when writing no rows would empty a table that has rows: `replace` or `--reload` without `allow_empty` (an empty result must not wipe a table by accident).
pub(crate) async fn refuse_emptying(
    table: &TablePlan,
    destination: &Destination,
) -> Result<(), LoaderError> {
    let replaces = matches!(table.mode, WriteMode::Replace | WriteMode::Recreate);
    if !replaces || table.spec.allow_empty || !table.exists {
        return Ok(());
    }
    let adapter = sql_adapter(table.spec.warehouse);
    let has_rows = match adapter.runs_sql() {
        false => true,
        true => {
            let query = adapter.render_any_row_query(&table.spec.destination);
            single_text(destination, query).await?.is_some()
        }
    };
    match has_rows {
        true => Err(LoaderError::Invalid(
            EMPTY_REPLACE_PROBLEM.replace("{table}", &table.outcome_name()),
        )),
        false => Ok(()),
    }
}

/// Whether a target's connection `name` may also be a Python source's own: only an s3 or filesystem one (project checks refuse the rest).
pub(crate) fn shareable_connection(project: &ProjectConfig, name: &str) -> bool {
    project
        .connections
        .get(name)
        .is_some_and(|connection| FILES_CONNECTION_KINDS.contains(&connection.kind()))
}

/// The largest non-NULL value of cursor `column` (matched ignoring case) in `batch`, as cursor text; `None` when it has none.
pub(crate) fn batch_cursor_max(
    batch: &RecordBatch,
    column: &str,
    column_type: &ColumnType,
) -> Result<Option<String>, String> {
    let schema = batch.schema();
    let Some((position, _)) = schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, field)| field.name().eq_ignore_ascii_case(column))
    else {
        return Ok(None);
    };
    let values = batch.column(position);
    let options = SortOptions {
        descending: true,
        nulls_first: false,
    };
    let sorted = sort_to_indices(values.as_ref(), Some(options), Some(1))
        .map_err(|error| error.to_string())?;
    let Some(index) = sorted.values().first().map(|index| *index as usize) else {
        return Ok(None);
    };
    if values.is_null(index) {
        return Ok(None);
    }
    let formatter = ArrayFormatter::try_new(values.as_ref(), &FormatOptions::default())
        .map_err(|error| error.to_string())?;
    cursor_text(&formatter.value(index).to_string(), column_type, Tz::UTC).map(Some)
}

/// The portable type of a column the crossing produced.
pub(crate) fn field_type(field: &Field) -> ColumnType {
    let json = field
        .metadata()
        .get(ARROW_TYPE_METADATA_KEY)
        .is_some_and(|kind| *kind == ColumnType::Json.to_string());
    match field.data_type() {
        DataType::Boolean => ColumnType::Bool,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => ColumnType::Int,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => ColumnType::Float,
        DataType::Decimal128(precision, scale) => ColumnType::Decimal {
            precision: *precision,
            scale: u8::try_from(*scale).unwrap_or_default(),
        },
        DataType::Date32 | DataType::Date64 => ColumnType::Date,
        DataType::Time32(_) | DataType::Time64(_) => ColumnType::Time,
        DataType::Timestamp(_, Some(_)) => ColumnType::TimestampTz,
        DataType::Timestamp(
            TimeUnit::Microsecond | TimeUnit::Millisecond | TimeUnit::Second | TimeUnit::Nanosecond,
            None,
        ) => ColumnType::TimestampNtz,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => ColumnType::Binary,
        DataType::Utf8 if json => ColumnType::Json,
        DataType::List(_) => ColumnType::Json,
        _ => ColumnType::Text,
    }
}

fn python_column(name: &str, column_type: ColumnType) -> SourceColumn {
    SourceColumn {
        name: name.to_owned(),
        postgres_type: PYTHON_COLUMN_TYPE.to_owned(),
        pg_type: PgType::Other,
        column_type: Ok(column_type),
        nullable: true,
        source_spelling: None,
        repeat_of: None,
    }
}

/// Whether `name` is a column the load adds under `compat`.
fn is_metadata(compat: Compat, name: &str) -> bool {
    [
        MetadataRole::LoadId,
        MetadataRole::LoadedAt,
        MetadataRole::RowId,
    ]
    .into_iter()
    .any(|role| name.eq_ignore_ascii_case(metadata_name(compat, role)))
}

/// Call the table function on a blocking thread; the host holds the GIL only to start it.
pub(crate) async fn start_read(
    host: Arc<dyn PythonHost>,
    request: TableRead,
) -> Result<Box<dyn BatchStream>, LoaderError> {
    tokio::task::spawn_blocking(move || host.read_table(&request))
        .await
        .map_err(|error| {
            LoaderError::Invalid(format!("the table function's thread failed: {error}"))
        })?
        .map_err(LoaderError::Invalid)
}

/// The position the table function left in `ctx.state` once it finished, as JSON text.
pub(crate) async fn final_state(
    host: Arc<dyn PythonHost>,
    table: String,
) -> Result<Option<String>, LoaderError> {
    tokio::task::spawn_blocking(move || host.table_state(&table))
        .await
        .map_err(|error| {
            LoaderError::Invalid(format!("the table function's thread failed: {error}"))
        })?
        .map_err(LoaderError::Invalid)
}

/// One yielded batch as the plan reads it: in the schema merged so far (`merged`, all-NULL columns as text), each read column converted to its planned type (excluded columns dropped), and only the rows inside `window` when it is given (and those outside it under `on_cursor_out_of_range = "warn"`), with those whose cursor is NULL under `null_cursor = "include"`; returns the batch, how many rows had a NULL cursor, and `seen`, the rows outside the window so far, with this batch's (numbered after `rows_before`).
pub(crate) fn typed_batch(
    batch: &RecordBatch,
    table: &TablePlan,
    (merged, window): (&SchemaRef, Option<&ReadWindow>),
    (rows_before, seen): (u64, OutsideRows),
) -> Result<WindowRows, LoaderError> {
    let read = read_schema(table);
    let conformed = conform_batch(batch, merged)
        .and_then(|conformed| binary_columns_as_text(&conformed, &read))
        .map_err(LoaderError::Invalid)?;
    let typed = read_types(&conformed, &read, rows_before).map_err(LoaderError::Invalid)?;
    let cursor = table.spec.cursor.as_ref();
    let include = cursor.is_some_and(|cursor| cursor.null_cursor == NullCursor::Include);
    let warns = cursor.is_some_and(|cursor| cursor.out_of_range == OnCursorOutOfRange::Warn);
    match window.and_then(|window| Some((window, cursor_type_of(table, &window.column)?))) {
        Some((window, cursor_type)) => window_rows(
            &typed,
            window,
            (&cursor_type, include, warns),
            (table.spec.zone(), rows_before, seen),
        ),
        None => Ok(WindowRows {
            kept: typed,
            nulls: 0,
            outside: seen,
        }),
    }
}

/// The planned type of `table`'s read column `column`.
fn cursor_type_of(table: &TablePlan, column: &str) -> Option<ColumnType> {
    table
        .read_columns
        .iter()
        .find(|read| read.name.eq_ignore_ascii_case(column))
        .map(|read| read.output.clone())
}

/// The batches' target schema: each read column in its planned type.
pub(crate) fn read_schema(table: &TablePlan) -> SchemaRef {
    let fields: Vec<Field> = table
        .read_columns
        .iter()
        .map(|column| {
            arrow_field(&column.name, &column.output)
                .unwrap_or_else(|| Field::new(&column.name, arrow_schema::DataType::Utf8, true))
        })
        .collect();
    Arc::new(Schema::new(fields))
}

impl PythonFeed {
    /// Call the function for each window (or once), committing each window, then commit the rest.
    pub(crate) async fn load(&mut self, load: &PythonLoad<'_>) -> Result<TableLoaded, LoaderError> {
        let windows = match &load.table.commits {
            CommitRule::Windows(windows) => Some(windows),
            _ => None,
        };
        let calls = windows.map_or(1, |windows| windows.count);
        let table = load.table.spec.source_table.name.clone();
        for index in 0..calls {
            let start = self.rows;
            let window: Option<CursorWindow> = windows
                .map(|windows| window_at(windows, index))
                .transpose()
                .map_err(LoaderError::Invalid)?;
            let stream =
                start_read(load.host.clone(), table_read(load, window.as_ref(), self)).await?;
            self.read(load, stream, window.as_ref()).await?;
            let fails = load
                .table
                .spec
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.null_cursor == NullCursor::Fail);
            if fails && self.null_cursors > 0 {
                return Err(LoaderError::Invalid(null_cursor_problem(
                    &load.table.spec,
                    self.null_cursors,
                )));
            }
            let read = window.as_ref().map(|window| self.read_window(window));
            if let Some(read) = read.or_else(|| self.table.window.clone()) {
                let outside = std::mem::take(&mut self.outside);
                self.out_of_window(outside, (&read, start))?;
            }
            if load.table.logged.is_some()
                && let Some(state) = final_state(load.host.clone(), table.clone()).await?
            {
                self.state = Some(state);
            }
            let state = self.state.clone();
            if let Some(window) = window
                && (self.publisher.is_some() || load.table.exists)
            {
                let publisher = self.publisher(load).await?;
                publisher.state = state;
                publisher
                    .cut(CommitRange::Window {
                        from: window.from.clone(),
                        to: window.to.clone(),
                    })
                    .await?;
            }
        }
        let (rows, windows) = self.unshown;
        if windows > 0 {
            let rows = match rows {
                1 => "1 more row".to_owned(),
                rows => format!("{} more rows", thousands(rows)),
            };
            let windows = match windows {
                1 => "1 later window".to_owned(),
                windows => format!("{} later windows", thousands(windows)),
            };
            self.warnings.push(format!("{rows} outside {windows}"));
        }
        let declared = load
            .table
            .spec
            .python
            .as_ref()
            .map_or(0, |python| python.declared.len());
        if self.rows == 0 {
            refuse_emptying(load.table, load.destination).await?;
        }
        if self.publisher.is_none() && self.rows == 0 && !load.table.exists && declared == 0 {
            empty_problem(load.table)?;
            let run = run_entry(
                load,
                load.table,
                (self.state.clone(), self.reconciled.clone()),
            );
            log_empty_run(run.as_ref(), load.table, load.destination).await?;
            return Ok(TableLoaded {
                rows: 0,
                changes: Vec::new(),
                note: Some(NO_DATA_NOTE.to_owned()),
                after_load: None,
                replaced_keys: None,
                transforms: Vec::new(),
                names: Vec::new(),
                warnings: std::mem::take(&mut self.warnings),
            });
        }
        let unsettled = self.publisher.is_none()
            || self
                .merged
                .fields()
                .iter()
                .any(|field| *field.data_type() == arrow_schema::DataType::Null);
        if unsettled {
            self.plan_data(load, true).await?;
        }
        let adapter = sql_adapter(self.table.spec.warehouse);
        let write_columns = &self.table.schema.write_columns;
        let merged_by = match self.rows {
            0 => &[][..],
            _ => &self.table.spec.newest_by[..],
        };
        if let Some(problem) = merged_by
            .iter()
            .find_map(|name| newest_by_problem(adapter, write_columns, name))
        {
            return Err(LoaderError::Invalid(problem));
        }
        let state = self.state.clone();
        let publisher = self.publisher(load).await?;
        let rows = publisher.finish(state).await?;
        let children = publisher.child_changes();
        let replaced_keys = publisher.replaced.clone();
        self.changes.extend(children);
        Box::pin(record_numbered(&self.table, load.destination)).await?;
        Ok(TableLoaded {
            rows,
            changes: std::mem::take(&mut self.changes),
            note: None,
            after_load: None,
            replaced_keys,
            transforms: Vec::new(),
            names: name_lines(&self.names.columns),
            warnings: std::mem::take(&mut self.warnings),
        })
    }

    /// Read one call of the table function to its end: each batch is typed and staged as it arrives (the plan and the table grow with new columns first), and a table without a cursor commits about every 1 GB; a logged table's `ctx.state` follows the rows that crossed with it, so its commits fall only there, recording that state.
    pub(crate) async fn read(
        &mut self,
        load: &PythonLoad<'_>,
        mut stream: Box<dyn BatchStream>,
        window: Option<&CursorWindow>,
    ) -> Result<(), LoaderError> {
        loop {
            let (returned, next) = tokio::task::spawn_blocking(move || {
                let next = stream.next_batch();
                (stream, next)
            })
            .await
            .map_err(|error| {
                LoaderError::Invalid(format!("the table function's thread failed: {error}"))
            })?;
            stream = returned;
            match next {
                None => return Ok(()),
                Some(Err(failure)) => return Err(LoaderError::Read(failure)),
                Some(Ok(Streamed::State(state))) => {
                    let state = (state != NO_STATE).then_some(state);
                    let sized = self.table.commits == CommitRule::Size;
                    self.state = state.clone();
                    if let Some(publisher) = &mut self.publisher {
                        publisher.state = state;
                        if sized && publisher.full() {
                            publisher.cut(CommitRange::Planned).await?;
                        }
                    }
                }
                Some(Ok(Streamed::Batch(batch))) => self.batch(load, batch, window).await?,
            }
        }
    }

    /// Refuse a column that one batch has as a field and another made by flattening a listed object.
    fn check_flattened(&mut self, batch: &RecordBatch) -> Result<(), LoaderError> {
        let made = flattened_columns(&batch.schema());
        for field in batch.schema().fields() {
            let flattened = made.contains(field.name());
            let seen = *self
                .flattened
                .entry(field.name().clone())
                .or_insert(flattened);
            if seen != flattened {
                return Err(LoaderError::Invalid(format!(
                    "flatten: column {} comes both from a field of the rows and from flattening a listed object; rename the field or leave the object unflattened",
                    field.name()
                )));
            }
        }
        Ok(())
    }

    /// `batch` with its fields named as they load (see [`named_batch`]); a `rename` function runs on a blocking thread, only when the batch brings fields not seen before.
    async fn named(
        &mut self,
        load: &PythonLoad<'_>,
        batch: RecordBatch,
    ) -> Result<RecordBatch, LoaderError> {
        let spec = &self.table.spec;
        let (host, memo, rules) = (
            Arc::clone(&load.host),
            std::mem::take(&mut self.names),
            spec.name_rules.clone(),
        );
        let table = (spec.source_name.clone(), spec.source_table.name.clone());
        let (named, memo) = tokio::task::spawn_blocking(move || {
            let rename = |columns: &[RenameColumn]| match &rules.function {
                Some(function) => {
                    host_rename(host.as_ref(), function, (&table.0, &table.1), columns)
                }
                None => Ok(BTreeMap::new()),
            };
            named_batch(batch, memo, &rules, &rename)
        })
        .await
        .map_err(|error| LoaderError::Invalid(format!("the rename's thread failed: {error}")))?;
        self.names = memo;
        named.map_err(LoaderError::Invalid)
    }

    /// Stage one batch: when it changes the merged schema, plan the table for the data so far first (one read of the live columns, the schema changes applied once).
    async fn batch(
        &mut self,
        load: &PythonLoad<'_>,
        batch: RecordBatch,
        window: Option<&CursorWindow>,
    ) -> Result<(), LoaderError> {
        self.check_flattened(&batch)?;
        let batch = self.named(load, batch).await?;
        let zone = self.table.spec.timezone.map_or(Tz::UTC, |(zone, _)| zone);
        let batch = parsed_batch(&batch, &self.table.spec.parses, zone, self.rows)
            .map_err(LoaderError::Invalid)?;
        let merged = merge_schema(&self.merged, &batch.schema());
        self.rows += batch.num_rows() as u64;
        let chosen = match (&self.table.spec.unnest, &mut self.table.spec.python) {
            (Unnest::All, Some(python)) => {
                choose_lists(&batch, &mut python.lists).map_err(LoaderError::Invalid)?
            }
            _ => false,
        };
        if merged != self.merged || self.publisher.is_none() || chosen {
            self.merged = merged;
            self.plan_data(load, false).await?;
        }
        let schema: SchemaRef = Arc::new(settled_schema(&self.merged));
        let read = match window {
            Some(window) => Some(self.read_window(window)),
            None => self.table.window.clone(),
        };
        let seen = std::mem::take(&mut self.outside);
        let before = self.rows - batch.num_rows() as u64;
        let rows = typed_batch(
            &batch,
            &self.table,
            (&schema, read.as_ref()),
            (before, seen),
        )?;
        let (typed, nulls) = (rows.kept, rows.nulls);
        self.outside = rows.outside;
        self.null_cursors += nulls as u64;
        let sized = self.table.commits == CommitRule::Size && self.table.logged.is_none();
        let publisher = self.publisher(load).await?;
        publisher.stage(typed, None).await?;
        match (sized, publisher.full()) {
            (true, true) => publisher.cut(CommitRange::Planned).await,
            _ => Ok(()),
        }
    }

    /// Plan the table for the data seen so far and bring the table (and the staging table) in line: created on the first batch, altered as later batches add or widen columns; columns that only held NULL wait for a typed value, or become text at the end (`settle`).
    pub(crate) async fn plan_data(
        &mut self,
        load: &PythonLoad<'_>,
        settle: bool,
    ) -> Result<(), LoaderError> {
        let nulls: BTreeSet<String> = self
            .merged
            .fields()
            .iter()
            .filter(|field| *field.data_type() == DataType::Null)
            .map(|field| field.name().clone())
            .collect();
        let live = self.table.live.as_deref();
        let typed_elsewhere = |name: &str| {
            live.is_some_and(|live| {
                live.iter()
                    .any(|column| column.name.eq_ignore_ascii_case(name))
            }) || load.table.spec.pinned.keys().any(|pinned| pinned == name)
        };
        let fields: Vec<_> = self
            .merged
            .fields()
            .iter()
            .filter(|field| {
                settle || !nulls.contains(field.name()) || typed_elsewhere(field.name())
            })
            .cloned()
            .collect();
        let schema = settled_schema(&Schema::new(fields));
        let data = YieldedData {
            schema: &schema,
            null_columns: &nulls,
            settled: settle,
            rows: self.rows,
        };
        let destination = match &self.publisher {
            Some(publisher) => &publisher.destination,
            None => load.destination,
        };
        let known = self
            .publisher
            .is_none()
            .then_some(self.table.live.as_deref());
        let replanned = replan(load, &self.table, (&data, known, &self.names), destination).await?;
        let old = staged_columns(&self.table);
        let changes = bring_in_line(
            (load.destination, self.publisher.as_mut()),
            (&old, &replanned),
        )
        .await?;
        self.changes.extend(changes);
        self.table = TablePlan {
            live: replanned.live.clone().or(self.table.live.clone()),
            ..replanned
        };
        Ok(())
    }

    /// The publisher, opened on first use with the table as planned for the data.
    pub(crate) async fn publisher(
        &mut self,
        load: &PythonLoad<'_>,
    ) -> Result<&mut Publisher, LoaderError> {
        if self.publisher.is_none() {
            let run = run_entry(
                load,
                &self.table,
                (self.state.clone(), self.reconciled.clone()),
            );
            let mut publisher =
                Publisher::open(self.table.clone(), (load.destination, load.options), run).await?;
            publisher.state = self.state.clone();
            self.publisher = Some(publisher);
        }
        self.publisher
            .as_mut()
            .ok_or_else(|| LoaderError::Invalid("the table's load could not start".to_owned()))
    }

    /// Fail the table, or keep a warning, for `outside`, the rows the function returned outside `read` (numbered from the window's first row, after `start`), as `on_cursor_out_of_range` says; a few windows' warnings show, and the rest are counted.
    fn out_of_window(
        &mut self,
        outside: OutsideRows,
        (read, start): (&ReadWindow, u64),
    ) -> Result<(), LoaderError> {
        let spec = &self.table.spec;
        let rule = spec
            .cursor
            .as_ref()
            .map(|cursor| cursor.out_of_range)
            .unwrap_or_default();
        let Some(cursor_type) = cursor_type_of(&self.table, &read.column) else {
            return Ok(());
        };
        if outside.count() == 0 {
            return Ok(());
        }
        let outside = OutsideRows {
            first: outside.first.map(|(row, value)| (row - start, value)),
            ..outside
        };
        let text = outside_text(&outside, read, (&cursor_type, spec.zone()), rule)?;
        match rule {
            OnCursorOutOfRange::Fail => return Err(LoaderError::Invalid(text)),
            _ if self.warnings.len() < SHOWN_OUTSIDE_WINDOWS => self.warnings.push(text),
            _ => self.unshown = (self.unshown.0 + outside.count(), self.unshown.1 + 1),
        }
        Ok(())
    }

    fn read_window(&self, window: &CursorWindow) -> ReadWindow {
        ReadWindow {
            column: self
                .table
                .spec
                .cursor
                .as_ref()
                .map(|cursor| cursor.column.clone())
                .unwrap_or_default(),
            from: window.from.clone(),
            to: window.to.clone(),
            inclusive: window.inclusive,
            maximum: None,
        }
    }
}

/// Fail a table whose function yielded no rows when its `min_rows` check needs some (unless the check only warns).
fn empty_problem(table: &TablePlan) -> Result<(), LoaderError> {
    let minimum = table.spec.checks.as_ref().and_then(|checks| {
        checks
            .min_rows
            .filter(|minimum| *minimum > 0 && !checks.warn.contains(&CheckName::MinRows))
    });
    match minimum {
        Some(minimum) => Err(LoaderError::Invalid(format!(
            "checks.min_rows failed: 0 rows staged, fewer than the {} required; nothing was published",
            thousands(minimum)
        ))),
        None => Ok(()),
    }
}

/// `planned` with its columns planned again for those `data` has (`known` being the live columns when they need no read); everything else stays as planned before the read, so it matches what the function was asked for.
async fn replan(
    load: &PythonLoad<'_>,
    planned: &TablePlan,
    (data, known, names): (&YieldedData<'_>, Option<Option<&[LiveColumn]>>, &NameMemo),
    destination: &Destination,
) -> Result<TablePlan, LoaderError> {
    let spec = &planned.spec;
    let problems = declared_problems(spec, data);
    if !problems.is_empty() {
        return Err(LoaderError::Invalid(problems.join("; ")));
    }
    let declared = spec
        .python
        .as_ref()
        .map(|python| python.declared.clone())
        .unwrap_or_default();
    let table = inferred_table(
        (&load.table.spec.source_table, load.table.spec.compat),
        &declared,
        data,
        planned.live.as_deref(),
    );
    let named = KnownNames {
        numbered: Vec::new(),
        named: Some(names.loaded.clone()),
        existing: Vec::new(),
    };
    let resolved = resolve_known(load.project, &spec.source_name, &[table], &named);
    let problems: Vec<String> = resolved
        .problems
        .into_iter()
        .chain(resolved.skipped.into_iter().map(|skipped| skipped.reason))
        .collect();
    let mut inferred = match (problems.is_empty(), resolved.specs.into_iter().next()) {
        (true, Some(inferred)) => inferred,
        _ => return Err(LoaderError::Invalid(problems.join("; "))),
    };
    inferred.python = spec.python.clone();
    inferred.approved = true;
    inferred.name_rules = spec.name_rules.clone();
    inferred.names = LoadedNames {
        columns: names.columns.clone(),
        numbered: names.numbered.clone(),
        ..spec.names.clone()
    };
    let columns = replan_columns(
        &inferred,
        (destination, planned.live.as_deref()),
        known,
        load.options,
    )
    .await?;
    let cursor_type = columns.spec.cursor.as_ref().and_then(|cursor| {
        columns
            .schema
            .write_columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&cursor.column))
            .map(|column| &column.column_type)
    });
    if let Some(problem) = append_problem(&columns.spec, cursor_type) {
        return Err(LoaderError::Invalid(problem));
    }
    let adapter = sql_adapter(columns.spec.warehouse);
    let seen = |name: &&String| {
        let typed = |field: &Arc<arrow_schema::Field>| {
            field.name().eq_ignore_ascii_case(name)
                && !data
                    .null_columns
                    .iter()
                    .any(|null| null.eq_ignore_ascii_case(name))
        };
        (data.settled && data.rows > 0) || data.schema.fields().iter().any(typed)
    };
    let newest_by = columns.spec.newest_by.iter().filter(seen);
    if let Some(problem) = newest_by
        .filter_map(|name| newest_by_problem(adapter, &columns.schema.write_columns, name))
        .next()
    {
        return Err(LoaderError::Invalid(problem));
    }
    let loaded: Vec<String> = columns
        .schema
        .write_columns
        .iter()
        .map(|column| column.name.clone())
        .chain(data.null_columns.iter().cloned())
        .collect();
    let problems: Vec<String> = columns
        .spec
        .checks
        .iter()
        .flat_map(|checks| check_column_problems(checks, &loaded))
        .collect();
    if !problems.is_empty() {
        return Err(LoaderError::Invalid(problems.join("; ")));
    }
    Ok(TablePlan {
        spec: columns.spec,
        read_columns: columns.read_columns,
        live: columns.live,
        schema: columns.schema,
        ..planned.clone()
    })
}

/// What the table function is called with: the window to read (`ctx.cursor_value` / `ctx.cursor_end`) and the latest `ctx.state`; connections that targets write to are withheld, except the source's own s3 or filesystem connection.
fn table_read(
    load: &PythonLoad<'_>,
    window: Option<&CursorWindow>,
    feed: &PythonFeed,
) -> TableRead {
    let table = load.table;
    let spec = &table.spec;
    let connection = load
        .project
        .sources
        .get(&spec.source_name)
        .map(|source| source.connection.clone())
        .filter(|name| !name.is_empty());
    let withheld: Vec<String> = load
        .project
        .targets
        .values()
        .map(|target| target.connection.clone())
        .filter(|name| {
            Some(name) != connection.as_ref() || !shareable_connection(load.project, name)
        })
        .collect();
    let mut connections = load.project.connection_settings.clone();
    connections.retain(|name, _| !withheld.contains(name));
    let timezone: Option<String> = spec.timezone.map(|(zone, _)| zone.name().to_owned());
    let cursor = table.window.as_ref().map(|planned| CursorRead {
        column: planned.column.clone(),
        column_type: cursor_type_name(&feed.table, &planned.column),
        start: window.map_or(planned.from.clone(), |window| window.from.clone()),
        end: window.map_or(planned.to.clone(), |window| window.to.clone()),
        inclusive: window.map_or(planned.inclusive, |window| window.inclusive),
        timezone: timezone.clone(),
    });
    TableRead {
        source: spec.source_name.clone(),
        table: spec.source_table.name.clone(),
        connection,
        connections,
        withheld,
        cursor,
        logged: table.logged.is_some(),
        state: feed.state.clone(),
        is_reload: load.options.reload,
        flatten: spec.flatten.clone(),
        masked: spec.masked.columns.keys().cloned().collect(),
        arrays: spec
            .pinned
            .iter()
            .filter(|(_, column_type)| matches!(column_type, ColumnType::Array(_)))
            .map(|(name, _)| name.clone())
            .collect(),
        naming: (
            spec.naming,
            sql_adapter(spec.warehouse).max_identifier_length(),
        ),
        zone: spec.timezone.map(|(zone, _)| zone),
        source_part: (load.project.memory.budget.as_ref())
            .map(|budget| budget.table_share / TABLE_SHARE_PARTS),
    }
}

/// Bring a table planned from its data in line with `replanned`, through `publisher`'s connection once it is open (`destination` before): its schema changes run, and an open publisher's rows so far are settled and its staging table altered from the `old` staged columns, before it takes the new plan. Returns the `created ...` and `altered ...` lines.
pub(crate) async fn bring_in_line(
    (destination, publisher): (&Destination, Option<&mut Publisher>),
    (old, replanned): (&[DesiredColumn], &TablePlan),
) -> Result<Vec<String>, LoaderError> {
    let mut changes: Vec<String> = Vec::new();
    let destination = match &publisher {
        Some(publisher) => &publisher.destination,
        None => destination,
    };
    if let Some(statements) = schema_statements(replanned) {
        execute_ddl(destination, statements).await?;
        changes.extend(change_lines(replanned));
    }
    if let Some(publisher) = publisher {
        publisher.settle_rows().await?;
        publisher.reopen_rows();
        if let Some(staging) = &publisher.staging {
            let altered = sql_adapter(replanned.spec.warehouse).render_staging_changes(
                staging,
                (old, &staged_columns(replanned)),
                publisher.started_with(),
            );
            execute_ddl(&publisher.destination, altered).await?;
        }
        publisher.table = replanned.clone();
        publisher.widen_files().await?;
    }
    Ok(changes)
}

/// A child table's plan before its first rows: the parent's settings, with columns inferred from its rows as a Python table's are, each row given an id, and no key, cursor, checks or column rules of the parent's; a `replace` replaces it with the parent, every other load appends to it.
fn child_plan(parent: &TablePlan, (path, table): (&str, &str)) -> TablePlan {
    let spec = &parent.spec;
    let child = TableSpec {
        source_table: SourceTable {
            name: table.to_owned(),
            columns: Vec::new(),
            primary_key: Vec::new(),
            indexed_columns: Vec::new(),
            comment: None,
            ..spec.source_table.clone()
        },
        destination: TableName {
            table: table.to_owned(),
            ..spec.destination.clone()
        },
        description: format!(
            "The items of the {path} lists of {}.",
            spec.destination.table
        ),
        unique_key: Vec::new(),
        not_null_keys: Vec::new(),
        replace_key: Vec::new(),
        newest_by: Vec::new(),
        cursor: None,
        batch: None,
        constants: Vec::new(),
        shared_destination: false,
        source_end: None,
        pinned: spec
            .child_pinned
            .iter()
            .filter_map(|(key, column_type)| {
                let column = key.strip_prefix(path)?.strip_prefix('.')?;
                (!column.contains('.')).then(|| (column.to_owned(), column_type.clone()))
            })
            .collect(),
        child_pinned: BTreeMap::new(),
        excluded_columns: Vec::new(),
        masked: MaskRules::default(),
        logged: false,
        files: None,
        flatten: Flatten::default(),
        row_id: true,
        root_rows: matches!(parent.mode, WriteMode::Merge { .. }),
        unnest: Unnest::None,
        python: Some(PythonTable {
            module: String::new(),
            declared: Vec::new(),
            lists: ListChoices::new(),
            literal_paths: None,
        }),
        sheet: None,
        transform: None,
        parses: BTreeMap::new(),
        cdc: None,
        checks: None,
        warehouse_source: None,
        layout: TableLayout::default(),
        ..spec.clone()
    };
    TablePlan {
        spec: child,
        read_columns: Vec::new(),
        live: None,
        schema: SchemaPlan {
            action: TableAction::Create,
            write_columns: Vec::new(),
            warnings: Vec::new(),
            blocked: None,
        },
        mode: match parent.mode {
            WriteMode::Replace => WriteMode::Replace,
            WriteMode::Recreate => WriteMode::Recreate,
            _ => WriteMode::Append,
        },
        source_filter: None,
        source_parameters: Vec::new(),
        cursor_note: None,
        window: None,
        route: parent.route.clone(),
        files: None,
        output: None,
        exists: false,
        logged: None,
        commits: CommitRule::Size,
        stage: parent.stage.clone(),
        planned_stage: None,
        fetch_memory: None,
    }
}

/// The ids a child batch's rows were given, from its row id column.
fn given_ids(batch: &RecordBatch, compat: Compat) -> Result<Vec<String>, LoaderError> {
    let name = metadata_name(compat, MetadataRole::RowId);
    let ids = batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            LoaderError::Invalid(format!("a child table's rows have no {name} column"))
        })?;
    Ok(ids
        .iter()
        .map(|id| id.unwrap_or_default().to_owned())
        .collect())
}

/// The child table the items of `spec`'s list field at dotted `path` land in, such as `orders__items__parts` for `items.parts`.
pub(crate) fn unnested_table(spec: &TableSpec, path: &str) -> String {
    let mut table = spec.destination.table.clone();
    for field in path.split('.') {
        table = child_table_name(
            (
                spec.naming,
                sql_adapter(spec.warehouse).max_identifier_length(),
            ),
            &table,
            field,
        );
    }
    table
}

/// The staged ordinal of each child row's root, from the root's row in the parent's batch.
fn root_ordinals_of(root_rows: &[usize], ordinals: &[i64]) -> Vec<i64> {
    root_rows
        .iter()
        .map(|row| ordinals.get(*row).copied().unwrap_or_default())
        .collect()
}

impl ChildTables {
    /// Stage the child rows of one of the parent's batches: a child table is planned (and created or altered) when its rows bring new columns, then they wait in its staging table for the parent's next commit.
    pub(crate) async fn stage(
        &mut self,
        (parent, (load_id, loaded_at)): (&TablePlan, (&str, &str)),
        (batches, ordinals): (Vec<ChildBatch>, Option<&[i64]>),
    ) -> Result<(), LoaderError> {
        for child in batches {
            let roots = ordinals.map(|ordinals| root_ordinals_of(&child.root_rows, ordinals));
            let below =
                |path: &String| child.path == *path || child.path.starts_with(&format!("{path}."));
            if self.ignored.iter().any(below) {
                continue;
            }
            let index = match self.feeds.iter().position(|feed| feed.path == child.path) {
                Some(index) => index,
                None => {
                    let table = self
                        .child_table(parent, (&child.path, &child.table))
                        .await?;
                    if !self.admits((parent, &child, &table)).await? {
                        self.ignored.push(child.path.clone());
                        continue;
                    }
                    self.feeds.push(ChildFeed {
                        table,
                        path: child.path.clone(),
                        merged: Schema::empty(),
                        publisher: None,
                    });
                    self.feeds.len() - 1
                }
            };
            let feed = &mut self.feeds[index];
            let merged = merge_schema(&feed.merged, &child.batch.schema());
            if merged != feed.merged || feed.publisher.is_none() {
                feed.merged = merged;
                let changes = feed.plan((&self.destination, &self.options), false).await?;
                self.changes.extend(changes);
            }
            let ids = given_ids(&child.batch, parent.spec.compat)?;
            let schema: SchemaRef = Arc::new(settled_schema(&feed.merged));
            let typed = typed_batch(
                &child.batch,
                &feed.table,
                (&schema, None),
                (0, OutsideRows::default()),
            )?
            .kept;
            let publisher = match &mut feed.publisher {
                Some(publisher) => publisher,
                None => {
                    let mut opened = Publisher::open(
                        feed.table.clone(),
                        (&self.destination, &self.options),
                        None,
                    )
                    .await?;
                    opened.load_id = load_id.to_owned();
                    opened.child = true;
                    opened.loaded_at = loaded_at.to_owned();
                    feed.publisher.insert(opened)
                }
            };
            publisher.stage_rows(typed, (Some(ids), roots)).await?;
        }
        Ok(())
    }

    /// The plan of the child table at `path` before its first rows (see [`child_plan`]); on a files target with its folder's files, as the target's plan reads them, and refused while another load holds the folder.
    async fn child_table(
        &self,
        parent: &TablePlan,
        (path, table): (&str, &str),
    ) -> Result<TablePlan, LoaderError> {
        let planned = child_plan(parent, (path, table));
        let Some(files) = files_destination(&self.destination) else {
            return Ok(planned);
        };
        let read = plan_files_table(&planned.spec, files, &self.options).await?;
        if let Some(blocked) = &read.schema.blocked {
            return Err(LoaderError::Invalid(format!(
                "child table {table}: {}",
                blocked.reasons.join("; ")
            )));
        }
        Ok(TablePlan {
            live: read.live,
            exists: read.exists,
            output: read.output,
            ..planned
        })
    }

    /// Whether the child table `table` is there already: on a files target, whether its folder holds files.
    async fn exists(&self, table: &TablePlan) -> Result<bool, LoaderError> {
        match files_destination(&self.destination) {
            Some(_) => Ok(table.exists),
            None => Ok(live_columns(&self.destination, &table.spec.destination)
                .await?
                .is_some()),
        }
    }

    /// Whether `child`'s table may be filled: always on the table's first load; after it, a child table that is not there yet is a schema change, which `on_schema_change` decides like a new column: added, left out with its rows and everything below (`"ignore"`, `false`), or refused.
    async fn admits(
        &self,
        (parent, child, table): (&TablePlan, &ChildBatch, &TablePlan),
    ) -> Result<bool, LoaderError> {
        let spec = &parent.spec;
        if !parent.exists || spec.on_schema_change == OnSchemaChange::AppendNewColumns {
            return Ok(true);
        }
        if self.exists(table).await? {
            return Ok(true);
        }
        match spec.on_schema_change {
            OnSchemaChange::Fail => Err(LoaderError::Invalid(format!(
                "new list field {} would make the child table {} and on_schema_change = \"fail\"",
                child.path, child.table
            ))),
            _ => {
                tracing::warn!(table = %child.table, field = %child.path, "new list field is not loaded (on_schema_change = \"ignore\")");
                Ok(false)
            }
        }
    }

    /// Commit every child table's rows so far, before the parent's commit (of `staging`'s rows by `mode`, `None` for a files target's) names them: under a merge or `delete_insert`, once the rows of the roots it replaces are cleared (see [`Self::clear_replaced`]).
    pub(crate) async fn cut(
        &mut self,
        (parent, destination, commit): (&TablePlan, &Destination, Option<(&TableName, &WriteMode)>),
    ) -> Result<(), LoaderError> {
        if let Some(commit) = commit {
            self.clear_replaced((parent, destination, commit)).await?;
        }
        for publisher in self
            .feeds
            .iter_mut()
            .filter_map(|feed| feed.publisher.as_mut())
        {
            publisher.cut(CommitRange::Planned).await?;
        }
        Ok(())
    }

    /// Finish every child table before the parent (`commit`, its last commit's staging table and mode, when one follows): columns that only held NULL become text, and what waits commits; under a `replace` (or `--reload`) an existing child table no rows reached (a listed one, or under `"all"` any named as the table's) is emptied too, so it never keeps an earlier load's items.
    pub(crate) async fn finish(
        &mut self,
        (parent, (load_id, loaded_at)): (&TablePlan, (&str, &str)),
        (destination, commit): (&Destination, Option<(&TableName, &WriteMode)>),
    ) -> Result<(), LoaderError> {
        if let Some(commit) = commit {
            self.clear_replaced((parent, destination, commit)).await?;
        }
        let replaced = matches!(parent.mode, WriteMode::Replace | WriteMode::Recreate);
        let spec = &parent.spec;
        let adapter = sql_adapter(spec.warehouse);
        let candidates: Vec<(String, String)> = match &spec.unnest {
            Unnest::Paths(paths) if replaced => paths
                .iter()
                .map(|path| (path.clone(), unnested_table(spec, path)))
                .collect(),
            Unnest::All if replaced => {
                child_paths(spec, live_child_tables(&self.destination, spec).await?)
            }
            _ => Vec::new(),
        };
        for (path, table) in &candidates {
            let fed = self.feeds.iter().any(|feed| {
                adapter.stored_table_name(&feed.table.spec.destination.table)
                    == adapter.stored_table_name(table)
            });
            if fed {
                continue;
            }
            let mut feed = ChildFeed {
                table: self.child_table(parent, (path, table)).await?,
                path: path.clone(),
                merged: Schema::empty(),
                publisher: None,
            };
            if !self.exists(&feed.table).await? {
                continue;
            }
            feed.plan((&self.destination, &self.options), true).await?;
            let mut opened =
                Publisher::open(feed.table.clone(), (&self.destination, &self.options), None)
                    .await?;
            opened.load_id = load_id.to_owned();
            opened.child = true;
            opened.loaded_at = loaded_at.to_owned();
            feed.publisher = Some(opened);
            self.feeds.push(feed);
        }
        for feed in &mut self.feeds {
            let unsettled = feed
                .merged
                .fields()
                .iter()
                .any(|field| *field.data_type() == DataType::Null);
            if unsettled {
                let changes = feed.plan((&self.destination, &self.options), true).await?;
                self.changes.extend(changes);
            }
            if let Some(publisher) = &mut feed.publisher {
                publisher.finish(None).await?;
            }
        }
        Ok(())
    }

    /// Before the commit of the parent's rows staged in `staging` (on the parent's `destination`) by `mode`, every child table's rows whose root that commit replaces are deleted, so a root's items are replaced by its new ones, an empty or missing list leaving none: under a merge the roots it merges (and the staged child rows of the parent rows it drops, a repeated key's others, leave each child table's staging table), under `delete_insert` the rows in its window or partitions. Each runs on its own, before the child and parent commits: a failure between them leaves a root without its items until a rerun replaces it again.
    async fn clear_replaced(
        &mut self,
        (parent, destination, (staging, mode)): (
            &TablePlan,
            &Destination,
            (&TableName, &WriteMode),
        ),
    ) -> Result<(), LoaderError> {
        if !replaces_roots(mode) {
            return Ok(());
        }
        let spec = &parent.spec;
        let adapter = sql_adapter(spec.warehouse);
        for publisher in self
            .feeds
            .iter_mut()
            .filter_map(|feed| feed.publisher.as_mut())
        {
            publisher.flush_batch().await?;
            Box::pin(publisher.send_held()).await?;
        }
        let request = WriteStatementsRequest {
            table: &spec.destination,
            columns: &parent.schema.write_columns,
            mode,
            hashed: &[],
            built: None,
            spared_load: None,
        };
        let dropped: Vec<i64> = match adapter.render_dropped_ordinals(&request, staging) {
            Some(query) => text_values(destination, query)
                .await?
                .iter()
                .map(|text| {
                    text.parse::<i64>().map_err(|error| {
                        LoaderError::Invalid(format!("a dropped row's ordinal {text}: {error}"))
                    })
                })
                .collect::<Result<_, _>>()?,
            None => Vec::new(),
        };
        let root_row = adapter.render_identifier(ROOT_ROW_COLUMN);
        for publisher in self
            .feeds
            .iter_mut()
            .filter_map(|feed| feed.publisher.as_mut())
        {
            let Some(child_staging) = publisher.staging.clone() else {
                continue;
            };
            let deletes: Vec<String> = dropped
                .chunks(DROPPED_CHUNK)
                .map(|chunk| {
                    let listed: Vec<String> = chunk.iter().map(i64::to_string).collect();
                    adapter.render_delete_staged(
                        &child_staging,
                        &format!("{root_row} IN ({})", listed.join(", ")),
                    )
                })
                .collect();
            execute(&publisher.destination, deletes).await?;
        }
        let root_name = metadata_name(spec.compat, MetadataRole::RootId);
        let row_id_name = metadata_name(spec.compat, MetadataRole::RowId);
        let row_id = adapter.render_identifier(row_id_name);
        let root = adapter.render_identifier(root_name);
        let mut tables: Vec<String> = self.merged.clone();
        for feed in self.feeds.iter().filter(|feed| feed.publisher.is_some()) {
            let table = &feed.table.spec.destination.table;
            if !tables
                .iter()
                .any(|known| adapter.stored_table_name(known) == adapter.stored_table_name(table))
            {
                tables.push(table.clone());
            }
        }
        let mut rooted: Vec<(usize, TableName)> = Vec::new();
        for (index, table) in tables.into_iter().enumerate() {
            let name = TableName {
                table,
                ..spec.destination.clone()
            };
            let has_root = live_columns(destination, &name)
                .await?
                .is_some_and(|columns| {
                    columns
                        .iter()
                        .any(|column| column.name.eq_ignore_ascii_case(root_name))
                });
            if has_root {
                rooted.push((index, name));
            }
        }
        if rooted.is_empty() {
            return Ok(());
        }
        let mut roots = match mode {
            WriteMode::DeleteInsert { .. } => format!(
                "{root} IN (SELECT {row_id} FROM {}{})",
                adapter.render_qualified_name(&spec.destination),
                adapter
                    .render_replaced_rows(&request)
                    .map(|condition| format!(" WHERE {condition}"))
                    .unwrap_or_default()
            ),
            _ => format!(
                "{root} IN ({})",
                adapter.render_kept_row_ids(&request, staging, row_id_name)
            ),
        };
        if let Some(rekeyed) = adapter.render_rekeyed_row_ids(&request, staging, row_id_name) {
            let found = text_values(destination, format!("{rekeyed} LIMIT 1")).await?;
            if !found.is_empty() {
                roots = format!("{roots} OR {root} IN ({rekeyed})");
            }
        }
        let load_id = self
            .feeds
            .iter()
            .find_map(|feed| feed.publisher.as_ref())
            .map(|publisher| publisher.load_id.clone());
        let deletes: Vec<String> = rooted
            .iter()
            .flat_map(|(index, name)| {
                let scratch = TableName {
                    table: format!("{}_{index}", staging.table),
                    ..staging.clone()
                };
                let roots = match mode {
                    WriteMode::Changes(change) => changed_roots(
                        (spec, &name.table),
                        (staging, change),
                        (root_name, row_id_name),
                        load_id.as_deref(),
                    ),
                    _ => roots.clone(),
                };
                adapter.render_clear_rows(name, &roots, &scratch)
            })
            .collect();
        execute(destination, deletes).await
    }

    /// Close every child table's publisher; best effort.
    pub(crate) async fn close(&mut self) {
        for publisher in self
            .feeds
            .iter_mut()
            .filter_map(|feed| feed.publisher.as_mut())
        {
            publisher.close().await;
        }
    }
}

/// `unnested` without its deletes' child rows when `table` applies captured changes (a changelog keeps them): with `REPLICA IDENTITY FULL` a delete carries its row's old lists, whose items stay loaded under soft deletes and are cleared under hard ones.
pub(crate) fn without_deleted_items(
    table: &TablePlan,
    batch: &RecordBatch,
    mut unnested: Unnested,
) -> Result<Unnested, String> {
    let WriteMode::Changes(change) = &table.mode else {
        return Ok(unnested);
    };
    let Ok(index) = batch.schema().index_of(CDC_OP_COLUMN) else {
        return Ok(unnested);
    };
    if change.apply == ChangeApply::Changelog {
        return Ok(unnested);
    }
    let ops = cast_with_options(
        batch.column(index),
        &DataType::Utf8,
        &CastOptions::default(),
    )
    .map_err(|error| error.to_string())?;
    let ops = ops.as_string::<i32>();
    let deleted: Vec<bool> = (0..ops.len())
        .map(|row| ops.is_valid(row) && ops.value(row) == DELETE_OP)
        .collect();
    for child in &mut unnested.children {
        let keep: BooleanArray = child
            .root_rows
            .iter()
            .map(|row| Some(!deleted[*row]))
            .collect();
        child.batch =
            filter_record_batch(&child.batch, &keep).map_err(|error| error.to_string())?;
        child.root_rows.retain(|row| !deleted[*row]);
    }
    Ok(unnested)
}

/// Under captured changes, the condition choosing the rows of `table`, a child table of `spec`'s, whose roots the changes in `staging` replace: every changed root (in a changelog, rows a replay wrote before), but under `merge` a root keeps its items when its list field came unchanged (a TOASTed value the change left out) and, under soft deletes, when it is deleted, as its row stays; under hard deletes a truncate also takes the roots it removes. A child table under captured changes appends its rows straight in, so this load's own rows (`load_id`) are never among them. Under `unnest = "all"` the list field is the source column whose child table `table` is or is below, the longest when names overlap (`line__items` beside `line`), as a column name may hold the separator.
fn changed_roots(
    (spec, table): (&TableSpec, &str),
    (staging, change): (&TableName, &ChangeWrite),
    (root_name, row_id_name): (&str, &str),
    load_id: Option<&str>,
) -> String {
    let adapter = sql_adapter(spec.warehouse);
    let id = |name: &str| adapter.render_identifier(name);
    let text = |value: &str| adapter.render_loader_value_literal(value);
    let stored = adapter.stored_table_name(table);
    let field: Option<String> = match &spec.unnest {
        Unnest::Paths(paths) => paths
            .iter()
            .find(|path| adapter.stored_table_name(&unnested_table(spec, path)) == stored)
            .and_then(|path| path.split('.').next())
            .map(str::to_owned),
        _ => spec
            .source_table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .filter(|column| {
                let child = adapter.stored_table_name(&child_table_name(
                    (spec.naming, adapter.max_identifier_length()),
                    &spec.destination.table,
                    column,
                ));
                stored == child || stored.starts_with(&format!("{child}{CHILD_TABLE_SEPARATOR}"))
            })
            .max_by_key(|column| column.len())
            .map(str::to_owned),
    };
    let mut kept: Vec<String> = Vec::new();
    if let Some(field) = field.filter(|_| change.apply != ChangeApply::Changelog) {
        kept.push(format!(
            "POSITION({} IN COALESCE({}, {})) = 0",
            text(&format!(",{field},")),
            id(CDC_UNCHANGED_COLUMN),
            text("")
        ));
    }
    if change.apply == ChangeApply::SoftDeletes {
        kept.push(format!("{} <> {}", id(CDC_OP_COLUMN), text(DELETE_OP)));
    }
    let filter = match kept.is_empty() {
        true => String::new(),
        false => format!(" WHERE {}", kept.join(" AND ")),
    };
    let root = id(root_name);
    let row_id = id(row_id_name);
    let changed = format!(
        "{root} IN (SELECT {row_id} FROM {}{filter})",
        adapter.render_qualified_name(staging)
    );
    let replaced = match (&change.truncated, change.apply) {
        (Some(truncation), ChangeApply::HardDeletes) => format!(
            "{changed} OR {root} IN (SELECT {row_id} FROM {} WHERE {} < {})",
            adapter.render_qualified_name(&spec.destination),
            id(CDC_LSN_COLUMN),
            truncation.lsn
        ),
        _ => changed,
    };
    match load_id {
        Some(load_id) => format!(
            "({replaced}) AND {} <> {}",
            id(metadata_name(spec.compat, MetadataRole::LoadId)),
            text(load_id)
        ),
        None => replaced,
    }
}

impl ChildFeed {
    /// Plan the child table for its rows so far and bring it in line, as [`PythonFeed::plan_data`] does a Python table; returns the `created ...` and `altered ...` lines.
    async fn plan(
        &mut self,
        (destination, options): (&Destination, &PlanOptions),
        settle: bool,
    ) -> Result<Vec<String>, LoaderError> {
        let nulls: BTreeSet<String> = self
            .merged
            .fields()
            .iter()
            .filter(|field| *field.data_type() == DataType::Null)
            .map(|field| field.name().clone())
            .collect();
        let live = self.table.live.as_deref();
        let fields: Vec<_> = self
            .merged
            .fields()
            .iter()
            .filter(|field| {
                settle || !nulls.contains(field.name()) || live_type(live, field.name()).is_some()
            })
            .cloned()
            .collect();
        let schema = settled_schema(&Schema::new(fields));
        let data = YieldedData {
            schema: &schema,
            null_columns: &nulls,
            settled: settle,
            rows: 0,
        };
        let spec = TableSpec {
            source_table: inferred_table(
                (&self.table.spec.source_table, self.table.spec.compat),
                &[],
                &data,
                live,
            ),
            ..self.table.spec.clone()
        };
        let reader = match &self.publisher {
            Some(publisher) => &publisher.destination,
            None => destination,
        };
        let columns = replan_columns(&spec, (reader, live), None, options).await?;
        let replanned = TablePlan {
            exists: columns.live.is_some(),
            spec: columns.spec,
            read_columns: columns.read_columns,
            live: columns.live,
            schema: columns.schema,
            ..self.table.clone()
        };
        let old = staged_columns(&self.table);
        let changes =
            bring_in_line((destination, self.publisher.as_mut()), (&old, &replanned)).await?;
        self.table = TablePlan {
            live: replanned.live.clone().or(self.table.live.clone()),
            ..replanned
        };
        Ok(changes)
    }
}
