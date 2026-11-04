use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::main::combined_type::combined_type;
use crate::column_types::models::ColumnType;
use crate::loader::constants::{PYTHON_COLUMN_TYPE, UNKNOWN_CURSOR_TYPE};
use crate::loader::constants::{SPILL_CHUNK_BYTES, UTC_OFFSET_SUFFIX};
use crate::loader::errors::LoaderError;
use crate::loader::models::{PythonTable, TableSpec};
use crate::loader::models::{ReadWindow, SpilledBatches, TablePlan};
use crate::postgres_source::models::{PgType, SourceColumn, SourceTable};
use crate::project::models::SourceConfig;
use crate::python_sources::models::TableRead;
use crate::python_sources::types::{BatchStream, PythonHost};
use crate::row_batches::main::conform_batch::conform_batch;
use crate::row_batches::main::merge_schema::merge_schema;
use crate::row_batches::main::settled_schema::settled_schema;
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
use crate::schema_plan::models::LiveColumn;
use crate::write_sql::constants::CURSOR_TIMESTAMP_FORMAT;
use crate::write_sql::main::cursor_text::cursor_text;
use arrow_array::{
    ArrayRef, Date32Array, Float64Array, Int64Array, RecordBatch, Scalar, StringArray,
    TimestampMicrosecondArray,
};
use arrow_cast::cast::{CastOptions, cast_with_options};
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use arrow_ord::cmp::{gt, gt_eq, lt};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use chrono::{NaiveDate, NaiveDateTime};
use std::fs::File;
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
            added.push(python_column(&column.name, column.column_type.clone()));
        }
    }
    if added.is_empty() {
        return None;
    }
    let mut augmented = spec.clone();
    augmented.source_table.columns.extend(added);
    Some(augmented)
}

/// A Python table as its yielded data shows it: every column of `schema` typed by inference (the live table's type when the inferred one fits it), plus declared columns no row had.
pub(crate) fn inferred_table(
    planned: &SourceTable,
    schema: &Schema,
    live: Option<&[LiveColumn]>,
) -> SourceTable {
    let mut columns: Vec<SourceColumn> = Vec::new();
    for field in schema.fields() {
        if !is_metadata(field.name()) {
            let column_type = fitting_type(field_type(field), live_type(live, field.name()));
            columns.push(python_column(field.name(), column_type));
        }
    }
    for declared in &planned.columns {
        if !columns.iter().any(|column| column.name == declared.name) {
            columns.push(declared.clone());
        }
    }
    SourceTable {
        columns,
        ..planned.clone()
    }
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

/// The planned type of the cursor column, `unknown` before the table's first load.
pub(crate) fn cursor_type_name(table: &TablePlan, column: &str) -> String {
    match table.read_columns.iter().find(|read| read.name == column) {
        Some(read) => read.output.to_string(),
        None => UNKNOWN_CURSOR_TYPE.to_owned(),
    }
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

/// Read every batch the table function yields into temporary Arrow IPC files of about [`SPILL_CHUNK_BYTES`] each, merging their schemas; the first error stops the read.
pub(crate) async fn spill_batches(
    mut stream: Box<dyn BatchStream>,
) -> Result<SpilledBatches, LoaderError> {
    let directory = tempfile::Builder::new()
        .prefix("sqlload-python-")
        .tempdir()
        .map_err(|error| LoaderError::Invalid(format!("cannot create a spill folder: {error}")))?;
    let mut spill = Spill {
        merged: Schema::empty(),
        pending: Vec::new(),
        pending_bytes: 0,
        chunks: Vec::new(),
        rows: 0,
    };
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
            None => break,
            Some(Err(reason)) => return Err(LoaderError::Invalid(reason)),
            Some(Ok(batch)) => spill.add(batch, directory.path())?,
        }
    }
    spill.write_chunk(directory.path())?;
    Ok(SpilledBatches {
        directory,
        chunks: spill.chunks,
        schema: Arc::new(settled_schema(&spill.merged)),
        rows: spill.rows,
    })
}

/// Batches waiting to be written as one chunk, and the chunks written.
struct Spill {
    merged: Schema,
    pending: Vec<RecordBatch>,
    pending_bytes: usize,
    chunks: Vec<std::path::PathBuf>,
    rows: u64,
}

impl Spill {
    fn add(&mut self, batch: RecordBatch, directory: &std::path::Path) -> Result<(), LoaderError> {
        self.merged = merge_schema(&self.merged, &batch.schema());
        self.rows += batch.num_rows() as u64;
        self.pending_bytes += batch.get_array_memory_size();
        self.pending.push(batch);
        match self.pending_bytes >= SPILL_CHUNK_BYTES {
            true => self.write_chunk(directory),
            false => Ok(()),
        }
    }

    /// Write the pending batches as one batch in one IPC file, in the schema merged so far.
    fn write_chunk(&mut self, directory: &std::path::Path) -> Result<(), LoaderError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let schema: SchemaRef = Arc::new(self.merged.clone());
        let conformed: Vec<RecordBatch> = self
            .pending
            .drain(..)
            .map(|batch| conform_batch(&batch, &schema))
            .collect::<Result<_, String>>()
            .map_err(LoaderError::Invalid)?;
        self.pending_bytes = 0;
        let chunk = concat_batches(&schema, &conformed).map_err(invalid)?;
        let path = directory.join(format!("{:06}.arrow", self.chunks.len()));
        let file = File::create(&path).map_err(|error| {
            LoaderError::Invalid(format!("cannot write {}: {error}", path.display()))
        })?;
        let mut writer = FileWriter::try_new(file, &schema).map_err(invalid)?;
        writer.write(&chunk).map_err(invalid)?;
        writer.finish().map_err(invalid)?;
        self.chunks.push(path);
        Ok(())
    }
}

/// Pass each spilled chunk to `on_batch` in the plan's read types, keeping only rows inside the cursor window; returns the rows passed.
pub(crate) async fn replay_batches(
    spilled: &SpilledBatches,
    table: &TablePlan,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, LoaderError> {
    let target = read_schema(table);
    let cursor_type = table.window.as_ref().and_then(|window| {
        table
            .read_columns
            .iter()
            .find(|column| column.name == window.column)
            .map(|column| column.output.clone())
    });
    let mut rows: u64 = 0;
    let mut outside: u64 = 0;
    for path in &spilled.chunks {
        let file = File::open(path).map_err(|error| {
            LoaderError::Invalid(format!("cannot read {}: {error}", path.display()))
        })?;
        let reader = FileReader::try_new(file, None).map_err(invalid)?;
        for batch in reader {
            let batch = conform_batch(&batch.map_err(invalid)?, &spilled.schema)
                .map_err(LoaderError::Invalid)?;
            let typed = read_types(&batch, &target)?;
            let kept = match (&table.window, &cursor_type) {
                (Some(window), Some(cursor_type)) => in_window(&typed, window, cursor_type)?,
                _ => typed,
            };
            outside += (batch.num_rows() - kept.num_rows()) as u64;
            rows += kept.num_rows() as u64;
            on_batch(kept).await.map_err(LoaderError::Invalid)?;
        }
    }
    if outside > 0 {
        tracing::warn!(
            table = %table.spec.destination.table,
            rows = outside,
            "skipped rows outside the cursor window; use ctx.cursor_value to read only new data"
        );
    }
    Ok(rows)
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
    let Some(column) = batch.column_by_name(&window.column).cloned() else {
        return Ok(batch.clone());
    };
    let mut kept = batch.clone();
    if let Some(from) = &window.from {
        let bound = Scalar::new(bound_array(from, cursor_type)?);
        let column = kept
            .column_by_name(&window.column)
            .cloned()
            .unwrap_or(column.clone());
        let mask = match window.inclusive {
            true => gt_eq(&column, &bound),
            false => gt(&column, &bound),
        }
        .map_err(invalid)?;
        kept = filter_record_batch(&kept, &mask).map_err(invalid)?;
    }
    if let Some(to) = &window.to {
        let bound = Scalar::new(bound_array(to, cursor_type)?);
        let column = kept
            .column_by_name(&window.column)
            .cloned()
            .unwrap_or(column);
        let mask = lt(&column, &bound).map_err(invalid)?;
        kept = filter_record_batch(&kept, &mask).map_err(invalid)?;
    }
    Ok(kept)
}

/// A cursor bound as a one-value array of the cursor column's Arrow type.
fn bound_array(text: &str, cursor_type: &ColumnType) -> Result<ArrayRef, LoaderError> {
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
    let array: ArrayRef = match cursor_type {
        ColumnType::Int | ColumnType::SmallInt => Arc::new(Int64Array::from(vec![
            normal.parse::<i64>().map_err(|_| unreadable())?,
        ])),
        ColumnType::Float => Arc::new(Float64Array::from(vec![
            normal.parse::<f64>().map_err(|_| unreadable())?,
        ])),
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
        _ => Arc::new(StringArray::from(vec![normal])),
    };
    Ok(array)
}

fn invalid(error: impl std::fmt::Display) -> LoaderError {
    LoaderError::Invalid(error.to_string())
}
