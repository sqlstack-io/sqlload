use crate::adapters::contract::constants::CURSOR_TIMESTAMP_FORMAT;
use crate::adapters::contract::constants::{STAGING_SUFFIX, SWAP_SUFFIX};
use crate::adapters::contract::main::cursor_text::cursor_text;
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::models::WriteMode;
use crate::adapters::contract::models::WriteStatementsRequest;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::execute_ddl::execute_ddl;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::main::session::single_text::single_text;
use crate::adapters::dispatch::main::session::table_names::table_names;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
use crate::batch_plan::main::window_at::window_at;
use crate::batch_plan::models::CursorWindow;
use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::main::combined_type::combined_type;
use crate::column_types::models::ColumnType;
use crate::load_naming::constants::CHILD_TABLE_SEPARATOR;
use crate::load_naming::main::child_table_name::child_table_name;
use crate::load_naming::main::loaded_name::loaded_name;
use crate::load_naming::main::metadata_name::metadata_name;
use crate::load_naming::models::{MetadataRole, Naming};
use crate::loader::_helpers::publish::staged_columns;
use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::_helpers::run_log::log_empty_run;
use crate::loader::_helpers::run_log::run_entry;
use crate::loader::_helpers::table_plan::append_problem;
use crate::loader::_helpers::table_plan::replan_columns;
use crate::loader::_helpers::table_plan::schema_statements;
use crate::loader::constants::DROPPED_CHUNK;
use crate::loader::constants::EMPTY_REPLACE_PROBLEM;
use crate::loader::constants::NO_DATA_NOTE;
use crate::loader::constants::NO_STATE;
use crate::loader::constants::PYTHON_COLUMN_TYPE;
use crate::loader::constants::UNKNOWN_CURSOR_TYPE;
use crate::loader::constants::UTC_OFFSET_SUFFIX;
use crate::loader::errors::LoaderError;
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
use crate::postgres_source::models::PgType;
use crate::postgres_source::models::SourceColumn;
use crate::postgres_source::models::SourceTable;
use crate::project::constants::FILES_CONNECTION_KINDS;
use crate::project::models::CheckName;
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
use crate::row_batches::main::json_lists::json_lists;
use crate::row_batches::main::merge_schema::merge_schema;
use crate::row_batches::main::settled_schema::settled_schema;
use crate::row_batches::models::ChildBatch;
use crate::row_batches::models::Unnest;
use crate::row_batches::types::ListChoices;
use crate::schema_plan::constants::ROOT_ROW_COLUMN;
use crate::schema_plan::models::DesiredColumn;
use crate::schema_plan::models::LiveColumn;
use crate::schema_plan::models::SchemaPlan;
use crate::schema_plan::models::TableAction;
use crate::similar_names::main::close_name::close_name;
use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::Date32Array;
use arrow_array::RecordBatch;
use arrow_array::Scalar;
use arrow_array::StringArray;
use arrow_array::TimestampMicrosecondArray;
use arrow_cast::cast::CastOptions;
use arrow_cast::cast::cast_with_options;
use arrow_cast::display::ArrayFormatter;
use arrow_cast::display::FormatOptions;
use arrow_ord::cmp::gt;
use arrow_ord::cmp::gt_eq;
use arrow_ord::cmp::lt;
use arrow_ord::sort::SortOptions;
use arrow_ord::sort::sort_to_indices;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use arrow_schema::TimeUnit;
use arrow_select::filter::filter_record_batch;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

/// A Python source's tables as planning sees them before any data: the declared (`columns`) and masked columns only.
pub(crate) fn python_tables(source: &SourceConfig) -> Vec<SourceTable> {
    let mut tables: Vec<SourceTable> = Vec::new();
    for (name, table) in &source.tables {
        let mut columns: Vec<SourceColumn> = Vec::new();
        for column in table.columns.keys().chain(table.mask.keys()) {
            if !columns.iter().any(|known| known.name == *column) {
                columns.push(python_column(column, ColumnType::Text));
            }
        }
        tables.push(SourceTable {
            schema: String::new(),
            name: name.clone(),
            columns,
            primary_key: Vec::new(),
            indexed_columns: Vec::new(),
            estimated_rows: -1,
            estimated_bytes: -1,
            comment: None,
        });
    }
    tables
}

/// What a Python table's spec records about it.
pub(crate) fn python_table(module: &str, table: &SourceTable) -> PythonTable {
    PythonTable {
        module: module.to_owned(),
        declared: table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        lists: ListChoices::new(),
    }
}

/// The child tables `spec`'s table has in the destination (tables named `<table>__...`, staging and swap tables left out); none without `unnest`.
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
    Ok(found
        .into_iter()
        .filter(|name| !name.contains(STAGING_SUFFIX) && !name.ends_with(SWAP_SUFFIX))
        .collect())
}

/// The dotted path each of `spec`'s child tables among `tables` holds, by its name as the load writes it (`orders__items__parts` holds `items.parts`; Snowflake's `ORDERS__ITEMS` holds `items`; Postgres lists the children of `ORDERS` folded, as `orders__items`).
pub(crate) fn child_paths(spec: &TableSpec, tables: Vec<String>) -> Vec<(String, String)> {
    let prefix = format!("{}{CHILD_TABLE_SEPARATOR}", spec.destination.table);
    let stored = sql_adapter(spec.warehouse).stored_name(&prefix);
    tables
        .into_iter()
        .filter(|table| table.starts_with(&stored))
        .filter_map(|table| {
            let path = source_spelling(&table, spec)
                .get(prefix.len()..)?
                .replace(CHILD_TABLE_SEPARATOR, ".");
            Some((path, table))
        })
        .collect()
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
        children.contains(&adapter.stored_name(&table))
    };
    if let Some(files) = &mut chosen.files {
        for column in &mut files.table.columns {
            if live(&column.name) {
                column.lists = Some(true);
            }
        }
    }
    if let Some(python) = &mut chosen.python {
        let prefix = adapter.stored_name(&format!(
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
        if !is_metadata(spec.naming, &column.name)
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

/// A yielded batch with its fields named as they load under `spec`'s naming: as yielded under SQLLoad's, normalised under dlt's; two fields loading as one name fail, naming both.
fn dlt_named(batch: RecordBatch, spec: &TableSpec) -> Result<RecordBatch, LoaderError> {
    if spec.naming == Naming::Sqlload {
        return Ok(batch);
    }
    let limit = sql_adapter(spec.warehouse).max_identifier_length();
    let schema = batch.schema();
    let mut fields: Vec<arrow_schema::FieldRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = loaded_name(spec.naming, field.name(), limit);
        if let Some(other) = fields.iter().find(|taken| *taken.name() == name) {
            let yielded = schema
                .fields()
                .iter()
                .find(|yielded| loaded_name(spec.naming, yielded.name(), limit) == *other.name())
                .map_or(name.clone(), |yielded| yielded.name().clone());
            return Err(LoaderError::Invalid(format!(
                "fields {yielded:?} and {:?} both load as {name} under naming = \"dlt\"; rename one in the table function",
                field.name()
            )));
        }
        fields.push(Arc::new(field.as_ref().clone().with_name(name)));
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        batch.columns().to_vec(),
    )
    .map_err(|error| LoaderError::Invalid(error.to_string()))
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
    pub(crate) rows: u64,
}

/// A Python table as its yielded data shows it: every column of the data typed by inference (the live table's type when the inferred one fits it, or when the column only held NULL), plus declared columns no row had.
pub(crate) fn inferred_table(
    (planned, naming): (&SourceTable, Naming),
    declared: &[String],
    data: &YieldedData<'_>,
    live: Option<&[LiveColumn]>,
) -> SourceTable {
    let mut columns: Vec<SourceColumn> = Vec::new();
    for field in data.schema.fields() {
        if !is_metadata(naming, field.name()) {
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

/// Why the declared `columns` and `mask` keys do not match the data: a key that names no column but is close to one (a different case or a typo), or a masked column the data lacks while it has rows; masking must never miss the real column.
pub(crate) fn declared_problems(spec: &TableSpec, data: &YieldedData<'_>) -> Vec<String> {
    let names: Vec<&str> = data
        .schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .filter(|name| !is_metadata(spec.naming, name))
        .collect();
    let keys = spec
        .pinned
        .keys()
        .filter(|key| !spec.masked.columns.contains_key(*key))
        .map(|key| ("columns", key))
        .chain(spec.masked.columns.keys().map(|key| ("mask", key)));
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
    cursor_text(&formatter.value(index).to_string(), column_type).map(Some)
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
    }
}

/// Whether `name` is a column the load adds under `naming`.
fn is_metadata(naming: Naming, name: &str) -> bool {
    [
        MetadataRole::LoadId,
        MetadataRole::LoadedAt,
        MetadataRole::RowId,
    ]
    .into_iter()
    .any(|role| name.eq_ignore_ascii_case(metadata_name(naming, role)))
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

/// One yielded batch as the plan reads it: in the schema merged so far (`merged`, all-NULL columns as text), each read column converted to its planned type (excluded columns dropped), and only the rows inside `window` when it is given; returns the batch and the rows left outside.
pub(crate) fn typed_batch(
    batch: &RecordBatch,
    table: &TablePlan,
    merged: &SchemaRef,
    window: Option<&ReadWindow>,
) -> Result<(RecordBatch, usize), LoaderError> {
    let conformed = conform_batch(batch, merged).map_err(LoaderError::Invalid)?;
    let typed = read_types(&conformed, &read_schema(table))?;
    let cursor_type = window.and_then(|window| {
        table
            .read_columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&window.column))
            .map(|column| column.output.clone())
    });
    let kept = match (window, &cursor_type) {
        (Some(window), Some(cursor_type)) => in_window(&typed, window, cursor_type)?,
        _ => typed,
    };
    let outside = batch.num_rows() - kept.num_rows();
    Ok((kept, outside))
}

/// The batches' target schema: each read column in its planned type.
fn read_schema(table: &TablePlan) -> SchemaRef {
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

/// `batch` with the target's columns (excluded columns dropped), each converted to its planned type (an array from the list, or from the JSON text a list arrives as); a value that does not convert is an error naming the column.
fn read_types(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch, LoaderError> {
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let arrays: Vec<ArrayRef> = target
        .fields()
        .iter()
        .map(|field| {
            let column = batch
                .column_by_name(field.name())
                .cloned()
                .unwrap_or_else(|| {
                    arrow_array::new_null_array(field.data_type(), batch.num_rows())
                });
            match (field.data_type(), column.data_type()) {
                (arrow_schema::DataType::List(_), arrow_schema::DataType::Utf8) => {
                    json_lists(&column, field.data_type()).map_err(|error| {
                        LoaderError::Invalid(format!("column {}: {error}", field.name()))
                    })
                }
                _ => cast_with_options(&column, field.data_type(), &strict).map_err(|error| {
                    LoaderError::Invalid(format!(
                        "column {}: {error}; pin a type for {} in columns that holds every value",
                        field.name(),
                        field.name()
                    ))
                }),
            }
        })
        .collect::<Result<_, LoaderError>>()?;
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(target.clone(), arrays, &options).map_err(invalid)
}

/// The rows of `batch` whose cursor is inside `window` (a NULL cursor is outside, as a source filter would have it).
fn in_window(
    batch: &RecordBatch,
    window: &ReadWindow,
    cursor_type: &ColumnType,
) -> Result<RecordBatch, LoaderError> {
    let schema = batch.schema();
    let Some((position, _)) = schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, field)| field.name().eq_ignore_ascii_case(&window.column))
    else {
        return Ok(batch.clone());
    };
    let mut kept = batch.clone();
    if let Some(from) = &window.from {
        let column = kept.column(position).clone();
        let bound = Scalar::new(bound_array(from, cursor_type, column.data_type())?);
        let mask = match window.inclusive {
            true => gt_eq(&column, &bound),
            false => gt(&column, &bound),
        }
        .map_err(invalid)?;
        kept = filter_record_batch(&kept, &mask).map_err(invalid)?;
    }
    if let Some(to) = &window.to {
        let column = kept.column(position).clone();
        let bound = Scalar::new(bound_array(to, cursor_type, column.data_type())?);
        let mask = lt(&column, &bound).map_err(invalid)?;
        kept = filter_record_batch(&kept, &mask).map_err(invalid)?;
    }
    Ok(kept)
}

/// A cursor bound as a one-value array of the cursor column's Arrow type `data_type`, converted from the bound's text (timestamps and dates parsed, every other type cast).
pub(crate) fn bound_array(
    text: &str,
    cursor_type: &ColumnType,
    data_type: &DataType,
) -> Result<ArrayRef, LoaderError> {
    let normal = match text.strip_suffix(UTC_OFFSET_SUFFIX) {
        Some(_) => text.to_owned(),
        None => cursor_text(text, cursor_type).map_err(LoaderError::Invalid)?,
    };
    let unreadable = || {
        LoaderError::Invalid(format!(
            "cursor bound {text:?} does not fit the cursor's type {cursor_type}"
        ))
    };
    let micros = || {
        NaiveDateTime::parse_from_str(
            normal.trim_end_matches(UTC_OFFSET_SUFFIX),
            CURSOR_TIMESTAMP_FORMAT,
        )
        .map(|time| time.and_utc().timestamp_micros())
        .map_err(|_| unreadable())
    };
    let parsed: ArrayRef = match cursor_type {
        ColumnType::TimestampTz => {
            Arc::new(TimestampMicrosecondArray::from(vec![micros()?]).with_timezone("UTC"))
        }
        ColumnType::TimestampNtz => Arc::new(TimestampMicrosecondArray::from(vec![micros()?])),
        ColumnType::Date => {
            let date = NaiveDate::parse_from_str(&normal, "%Y-%m-%d").map_err(|_| unreadable())?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default();
            let days = i32::try_from((date - epoch).num_days()).map_err(|_| unreadable())?;
            Arc::new(Date32Array::from(vec![days]))
        }
        _ => Arc::new(StringArray::from(vec![normal.clone()])),
    };
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    cast_with_options(&parsed, data_type, &strict).map_err(|_| unreadable())
}

fn invalid(error: impl std::fmt::Display) -> LoaderError {
    LoaderError::Invalid(error.to_string())
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
            let window: Option<CursorWindow> = windows
                .map(|windows| window_at(windows, index))
                .transpose()
                .map_err(LoaderError::Invalid)?;
            let stream =
                start_read(load.host.clone(), table_read(load, window.as_ref(), self)).await?;
            self.read(load, stream, window.as_ref()).await?;
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
        if self.outside > 0 {
            tracing::warn!(
                table = %load.table.spec.destination.table,
                rows = self.outside,
                "skipped rows outside the cursor window; use ctx.cursor_value to read only new data"
            );
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
        let state = self.state.clone();
        let publisher = self.publisher(load).await?;
        let rows = publisher.finish(state).await?;
        let children = publisher.child_changes();
        self.changes.extend(children);
        Ok(TableLoaded {
            rows,
            changes: std::mem::take(&mut self.changes),
            note: None,
            after_load: None,
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

    /// Stage one batch: when it changes the merged schema, plan the table for the data so far first (one read of the live columns, the schema changes applied once).
    async fn batch(
        &mut self,
        load: &PythonLoad<'_>,
        batch: RecordBatch,
        window: Option<&CursorWindow>,
    ) -> Result<(), LoaderError> {
        self.check_flattened(&batch)?;
        let batch = dlt_named(batch, &self.table.spec)?;
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
        let (typed, outside) = typed_batch(&batch, &self.table, &schema, read.as_ref())?;
        self.outside += outside as u64;
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
        let replanned = replan(load, &self.table, (&data, known), destination).await?;
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
            "checks.min_rows failed: 0 row(s) staged, fewer than the {minimum} required; nothing was published"
        ))),
        None => Ok(()),
    }
}

/// `planned` with its columns planned again for those `data` has (`known` being the live columns when they need no read); everything else stays as planned before the read, so it matches what the function was asked for.
async fn replan(
    load: &PythonLoad<'_>,
    planned: &TablePlan,
    (data, known): (&YieldedData<'_>, Option<Option<&[LiveColumn]>>),
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
        (&load.table.spec.source_table, load.table.spec.naming),
        &declared,
        data,
        planned.live.as_deref(),
    );
    let mut project = load.project.clone();
    if let Some(source) = project.sources.get_mut(&spec.source_name) {
        source.tables.retain(|name, _| *name == table.name);
    }
    let resolved = resolve_tables(&project, &spec.source_name, &[table]);
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
    let cursor = table.window.as_ref().map(|planned| CursorRead {
        column: planned.column.clone(),
        column_type: cursor_type_name(&feed.table, &planned.column),
        start: window.map_or(planned.from.clone(), |window| window.from.clone()),
        end: window.map_or(planned.to.clone(), |window| window.to.clone()),
        inclusive: window.map_or(planned.inclusive, |window| window.inclusive),
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
        dedup_sort: None,
        cursor: None,
        batch: None,
        constants: Vec::new(),
        shared_destination: false,
        source_end: None,
        pinned: BTreeMap::new(),
        excluded_columns: Vec::new(),
        masked: MaskRules::default(),
        logged: false,
        files: None,
        destination_path: None,
        flatten: Vec::new(),
        row_id: true,
        root_rows: matches!(parent.mode, WriteMode::Merge { .. }),
        unnest: Unnest::None,
        python: Some(PythonTable {
            module: String::new(),
            declared: Vec::new(),
            lists: ListChoices::new(),
        }),
        sheet: None,
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
    }
}

/// The ids a child batch's rows were given, from its row id column.
fn given_ids(batch: &RecordBatch, naming: Naming) -> Result<Vec<String>, LoaderError> {
    let name = metadata_name(naming, MetadataRole::RowId);
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
                    if !self.admits((parent, &child)).await? {
                        self.ignored.push(child.path.clone());
                        continue;
                    }
                    self.feeds.push(ChildFeed {
                        table: child_plan(parent, (&child.path, &child.table)),
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
            let ids = given_ids(&child.batch, parent.spec.naming)?;
            let schema: SchemaRef = Arc::new(settled_schema(&feed.merged));
            let (typed, _) = typed_batch(&child.batch, &feed.table, &schema, None)?;
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
                    opened.loaded_at = loaded_at.to_owned();
                    feed.publisher.insert(opened)
                }
            };
            publisher.stage_rows(typed, (Some(ids), roots)).await?;
        }
        Ok(())
    }

    /// Whether `child`'s table may be filled: always on the table's first load; after it, a child table that is not there yet is a schema change, which `on_schema_change` decides like a new column: added, left out with its rows and everything below (`"ignore"`, `false`), or refused.
    async fn admits(
        &self,
        (parent, child): (&TablePlan, &ChildBatch),
    ) -> Result<bool, LoaderError> {
        let spec = &parent.spec;
        if !parent.exists || spec.on_schema_change == OnSchemaChange::AppendNewColumns {
            return Ok(true);
        }
        let table = TableName {
            table: child.table.clone(),
            ..spec.destination.clone()
        };
        if live_columns(&self.destination, &table).await?.is_some() {
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

    /// Commit every child table's rows so far, before the parent's commit names them (under a merge, once the rows of the roots it merges are cleared, see [`Self::clear_merged`]).
    pub(crate) async fn cut(
        &mut self,
        (parent, destination, staging): (&TablePlan, &Destination, Option<&TableName>),
    ) -> Result<(), LoaderError> {
        self.clear_merged((parent, destination, staging)).await?;
        for publisher in self
            .feeds
            .iter_mut()
            .filter_map(|feed| feed.publisher.as_mut())
        {
            publisher.cut(CommitRange::Planned).await?;
        }
        Ok(())
    }

    /// Finish every child table before the parent: columns that only held NULL become text, and what waits commits; under a `replace` (or `--reload`) an existing child table no rows reached (a listed one, or under `"all"` any named as the table's) is emptied too, so it never keeps an earlier load's items.
    pub(crate) async fn finish(
        &mut self,
        (parent, (load_id, loaded_at)): (&TablePlan, (&str, &str)),
        (destination, staging): (&Destination, Option<&TableName>),
    ) -> Result<(), LoaderError> {
        self.clear_merged((parent, destination, staging)).await?;
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
                adapter.stored_name(&feed.table.spec.destination.table)
                    == adapter.stored_name(table)
            });
            if fed {
                continue;
            }
            let mut feed = ChildFeed {
                table: child_plan(parent, (path, table)),
                path: path.clone(),
                merged: Schema::empty(),
                publisher: None,
            };
            if live_columns(&self.destination, &feed.table.spec.destination)
                .await?
                .is_none()
            {
                continue;
            }
            feed.plan((&self.destination, &self.options), true).await?;
            let mut opened =
                Publisher::open(feed.table.clone(), (&self.destination, &self.options), None)
                    .await?;
            opened.load_id = load_id.to_owned();
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

    /// Under a merge, before the commit of the parent's rows staged in `staging` (on the parent's `destination`): the staged child rows of the parent rows the merge drops (a repeated key's others) leave each child table's staging table, and every child table's rows whose root is being merged are deleted, so a root's items are replaced by its new ones, an empty or missing list leaving none. Each runs on its own, before the child and parent commits: a failure between them leaves a root without its items until a rerun merges it again.
    async fn clear_merged(
        &mut self,
        (parent, destination, staging): (&TablePlan, &Destination, Option<&TableName>),
    ) -> Result<(), LoaderError> {
        let (WriteMode::Merge { .. }, Some(staging)) = (&parent.mode, staging) else {
            return Ok(());
        };
        let spec = &parent.spec;
        let adapter = sql_adapter(spec.warehouse);
        for publisher in self
            .feeds
            .iter_mut()
            .filter_map(|feed| feed.publisher.as_mut())
        {
            publisher.flush_batch().await?;
        }
        let request = WriteStatementsRequest {
            table: &spec.destination,
            columns: &parent.schema.write_columns,
            mode: &parent.mode,
            hashed: &[],
            built: None,
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
        let root_name = metadata_name(spec.naming, MetadataRole::RootId);
        let row_id_name = metadata_name(spec.naming, MetadataRole::RowId);
        let row_id = adapter.render_identifier(row_id_name);
        let root = adapter.render_identifier(root_name);
        let mut tables: Vec<String> = self.merged.clone();
        for feed in self.feeds.iter().filter(|feed| feed.publisher.is_some()) {
            let table = &feed.table.spec.destination.table;
            if !tables
                .iter()
                .any(|known| adapter.stored_name(known) == adapter.stored_name(table))
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
        let mut roots = format!(
            "{root} IN (SELECT {row_id} FROM {})",
            adapter.render_qualified_name(staging)
        );
        if let Some(rekeyed) = adapter.render_rekeyed_row_ids(&request, staging, row_id_name) {
            let found = text_values(destination, format!("{rekeyed} LIMIT 1")).await?;
            if !found.is_empty() {
                roots = format!("{roots} OR {root} IN ({rekeyed})");
            }
        }
        let deletes: Vec<String> = rooted
            .iter()
            .flat_map(|(index, name)| {
                let scratch = TableName {
                    table: format!("{}_{index}", staging.table),
                    ..staging.clone()
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
            rows: 0,
        };
        let spec = TableSpec {
            source_table: inferred_table(
                (&self.table.spec.source_table, self.table.spec.naming),
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
