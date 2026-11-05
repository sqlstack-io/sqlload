use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::main::combined_type::combined_type;
use crate::column_types::models::ColumnType;
use crate::loader::_helpers::destination::single_text;
use crate::loader::constants::{
    EMPTY_REPLACE_PROBLEM, PYTHON_COLUMN_TYPE, UNKNOWN_CURSOR_TYPE, UTC_OFFSET_SUFFIX,
};
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, PythonTable, ReadWindow, TablePlan, TableSpec};
use crate::postgres_source::models::{PgType, SourceColumn, SourceTable};
use crate::project::constants::FILES_CONNECTION_KINDS;
use crate::project::models::{ProjectConfig, SourceConfig};
use crate::python_sources::models::TableRead;
use crate::python_sources::types::{BatchStream, PythonHost};
use crate::row_batches::main::conform_batch::conform_batch;
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
use crate::schema_plan::models::{LiveColumn, TableAction};
use crate::similar_names::main::close_name::close_name;
use crate::write_sql::constants::CURSOR_TIMESTAMP_FORMAT;
use crate::write_sql::main::cursor_text::cursor_text;
use crate::write_sql::main::display_name::display_name;
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::main::stored_name::stored_name;
use crate::write_sql::models::WriteMode;
use arrow_array::Array;
use arrow_array::{
    ArrayRef, Date32Array, RecordBatch, Scalar, StringArray, TimestampMicrosecondArray,
};
use arrow_cast::cast::{CastOptions, cast_with_options};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_ord::cmp::{gt, gt_eq, lt};
use arrow_ord::sort::{SortOptions, sort_to_indices};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_select::filter::filter_record_batch;
use chrono::{NaiveDate, NaiveDateTime};
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
    }
}

/// A Python table's spec with the live table's data columns added (as the source has them, so `plan` shows no change for them), or `None` when there are none to add.
pub(crate) fn with_live_columns(
    spec: &TableSpec,
    live: Option<&[LiveColumn]>,
) -> Option<TableSpec> {
    spec.python.as_ref()?;
    let mut added: Vec<SourceColumn> = Vec::new();
    for column in live? {
        if !is_metadata(&column.name) && !has_column(&spec.source_table, &column.name) {
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
    match stored_name(&lower, spec.warehouse) == stored {
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
    planned: &SourceTable,
    declared: &[String],
    data: &YieldedData<'_>,
    live: Option<&[LiveColumn]>,
) -> SourceTable {
    let mut columns: Vec<SourceColumn> = Vec::new();
    for field in data.schema.fields() {
        if !is_metadata(field.name()) {
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
        .filter(|name| !is_metadata(name))
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
    let name = display_name(&table.spec.destination, table.spec.warehouse);
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
    let has_rows = match destination {
        Destination::Files(_) => true,
        destination => {
            let query = format!(
                "SELECT 'x' FROM {} LIMIT 1",
                qualified_name(&table.spec.destination, table.spec.warehouse)
            );
            single_text(destination, query).await?.is_some()
        }
    };
    let name = display_name(&table.spec.destination, table.spec.warehouse);
    match has_rows {
        true => Err(LoaderError::Invalid(
            EMPTY_REPLACE_PROBLEM.replace("{table}", &name),
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
    }
}

fn is_metadata(name: &str) -> bool {
    name.eq_ignore_ascii_case(LOAD_ID_COLUMN) || name.eq_ignore_ascii_case(LOADED_AT_COLUMN)
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

/// `batch` with the target's columns (excluded columns dropped), each converted to its planned type; a value that does not convert is an error naming the column.
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
            cast_with_options(&column, field.data_type(), &strict).map_err(|error| {
                LoaderError::Invalid(format!(
                    "column {}: {error}; pin a type for {} in columns that holds every value",
                    field.name(),
                    field.name()
                ))
            })
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
