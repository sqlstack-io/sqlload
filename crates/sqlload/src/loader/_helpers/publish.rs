use crate::adapters::contract::constants::COMMIT_STATEMENT;
use crate::adapters::contract::constants::SWAP_SUFFIX;
use crate::adapters::contract::main::cursor_text::cursor_text;
use crate::adapters::contract::main::staging_id::staging_id;
use crate::adapters::contract::models::BoundParameter;
use crate::adapters::contract::models::ChangeApply;
use crate::adapters::contract::models::TableName;
use crate::adapters::contract::models::TableShape;
use crate::adapters::contract::models::WriteMode;
use crate::adapters::contract::models::WriteStatementsRequest;
use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::models::{DirectBatch, DirectLoaded, UnloadedBatch};
use crate::adapters::contract::types::TableWriter;
use crate::adapters::dispatch::main::dialect::source_dialect::source_dialect;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::route::sends_rows::sends_rows;
use crate::adapters::dispatch::main::session::ddl_guard::ddl_guard;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::execute_ddl::execute_ddl;
use crate::adapters::dispatch::main::session::table_destination::table_destination;
use crate::adapters::dispatch::main::session::table_shape::table_shape;
use crate::adapters::dispatch::main::write::file_keys::file_keys;
use crate::adapters::dispatch::main::write::load_files::load_files;
use crate::adapters::dispatch::main::write::load_unloaded::load_unloaded;
use crate::adapters::dispatch::main::write::open_table_writer::open_table_writer;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::dispatch::models::Writer;
use crate::adapters::files::models::ReplacedFiles;
use crate::batch_plan::constants::BATCH_BYTES;
use crate::batch_plan::main::cut_index::cut_index;
use crate::batch_plan::main::last_value::last_value;
use crate::batch_plan::main::window_at::window_at;
use crate::batch_plan::models::CursorWindow;
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::ColumnType;
use crate::commit_spool::main::free_bytes::free_bytes;
use crate::commit_spool::main::held_estimate::held_estimate;
use crate::commit_spool::models::Spool;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::models::OpenFolder;
use crate::file_store::models::OpenFolder as StoreFolder;
use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::main::after_load::after_load;
use crate::files_source::main::batch_columns::batch_columns;
use crate::files_source::main::batch_footers::batch_footers;
use crate::files_source::main::batch_headers::batch_headers;
use crate::files_source::main::key_columns::key_columns;
use crate::files_source::models::AfterLoad;
use crate::load_events::main::emit::emit;
use crate::load_events::models::EventKind;
use crate::number_text::main::memory_text::memory_text;
use crate::project::models::SendMode;

use crate::batch_plan::main::window_concurrency::window_concurrency;
use crate::files_source::models::BatchFooters;
use crate::files_source::models::DataFile;
use crate::files_source::models::{FilesRead, FilesTable, SourceFormat, TextChecks};
use crate::load_naming::main::metadata_name::metadata_name;
use crate::load_naming::main::name_lines::name_lines;
use crate::load_naming::models::{Compat, MetadataRole};
use crate::loader::_helpers::destination::files_read_as;
use crate::loader::_helpers::destination::nanosecond_columns;
use crate::loader::_helpers::destination::{commit_mode, deletes_range, piece_mode};
use crate::loader::_helpers::destination::{give_back, keep_in_background, leased_folder};
use crate::loader::_helpers::python::list_choices;
use crate::loader::_helpers::python::live_child_tables;
use crate::loader::_helpers::python::unnested_table;
use crate::loader::_helpers::python::without_deleted_items;
use crate::loader::_helpers::resolve::{known_names, resolve_known};
use crate::loader::_helpers::run_log::append_run;
use crate::loader::_helpers::run_log::ensure_runs_table;
use crate::loader::_helpers::run_log::fold_maximum;
use crate::loader::_helpers::run_log::logs_files;
use crate::loader::_helpers::run_log::{
    commit_transform, in_file, listed_sources, merge_rebuild, merged_file_text, merged_name,
    merged_record, swap_rebuild, with_file_keys, with_transform,
};
use crate::loader::_helpers::run_log::{record_numbered, recorded_names, transformed};
use crate::loader::_helpers::sources::ReadPart;
use crate::loader::_helpers::sources::files_source_parts;
use crate::loader::_helpers::sources::read_source;
use crate::loader::_helpers::sources::unarchived;
use crate::loader::_helpers::sources::unload_source;
use crate::loader::_helpers::specs::checked_key;
use crate::loader::_helpers::specs::{null_cursor_problem, null_cursor_rows};
use crate::loader::_helpers::table_plan::{
    cursor_binding, source_cursor, source_filter, with_null_cursors,
};
use crate::loader::_helpers::table_plan::{replan_columns, schema_statements};
use crate::loader::constants::EXCLUSIVE_BOUND;
use crate::loader::constants::INCLUSIVE_BOUND;
use crate::loader::constants::MERGE_PUBLISHED_ONCE;
use crate::loader::constants::MERGE_RUN_ROWS;
use crate::loader::constants::PROGRESS_EVERY;
use crate::loader::constants::PUBLISHED_ONCE;
use crate::loader::constants::RANGE_PUBLISHED_ONCE;
use crate::loader::constants::REPLACED_KEYS_SHOWN;
use crate::loader::constants::SNAPSHOT_BEGIN;
use crate::loader::errors::LoaderError;
use crate::loader::main::_files_repair::files_to_replace;
use crate::loader::models::ChildTables;
use crate::loader::models::CommitRange;
use crate::loader::models::CommitRule;
use crate::loader::models::FilesSink;
use crate::loader::models::FilesSource;
use crate::loader::models::FolderSwap;
use crate::loader::models::Pieces;
use crate::loader::models::Publisher;
use crate::loader::models::ReplacedKeys;
use crate::loader::models::RunEntry;
use crate::loader::models::SourceReader;
use crate::loader::models::TableLoaded;
use crate::loader::models::TablePlan;
use crate::loader::models::TransformRun;
use crate::loader::models::{LoadRun, PlanOptions};
use crate::masking::main::mask_batch::mask_batch;
use crate::memory_budget::constants::TABLE_SHARE_PARTS;
use crate::number_text::main::thousands::thousands;
use crate::postgres_source::errors::PostgresError;
use crate::project::models::{NullCursor, NullKeys};
use crate::publish_checks::main::key_sources::key_sources;
use crate::publish_checks::main::marked_rows::marked_rows;
use crate::publish_checks::main::older_rows::older_rows;
use crate::publish_checks::main::refuse_null_keys::refuse_null_keys;
use crate::publish_checks::main::refuse_repeated_keys::refuse_repeated_keys;
use crate::publish_checks::main::run_checks::run_checks;
use crate::publish_checks::main::staged_keys::staged_keys;
use crate::publish_checks::models::CheckedCommit;
use crate::row_batches::main::unnest_batch::unnest_batch;
use crate::row_batches::models::Unnest;
use crate::row_batches::models::UnnestRequest;
use crate::row_deletes::main::marked_note::marked_note;
use crate::row_ids::main::key_row_ids::key_row_ids;
use crate::row_ids::main::random_row_ids::random_row_ids;
use crate::run_log::main::insert_run::insert_run;
use crate::run_log::main::runs_table::runs_table;
use crate::run_log::models::RunOutcome;
use crate::run_log::models::RunRecord;
use crate::schema_plan::constants::{
    CDC_DELETED_AT_COLUMN, CDC_LSN_COLUMN, CDC_SEQ_COLUMN, MERGE_ROW_COLUMN, ROOT_ROW_COLUMN,
};
use crate::schema_plan::models::ColumnOrigin;
use crate::schema_plan::models::DesiredColumn;
use crate::schema_plan::models::LiveColumn;
use crate::snowflake_source::models::UnloadedFiles;
use crate::table_lease::main::lease_now::lease_now;
use crate::table_lease::main::take_lease::take_lease;
use crate::time_text::main::now_utc_text::now_utc_text;
use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::Int64Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_array::TimestampMicrosecondArray;
use arrow_array::cast::AsArray;
use arrow_array::new_null_array;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_cast::{CastOptions, cast, cast_with_options};
use arrow_schema::Field;
use arrow_schema::Schema;
use chrono::DateTime;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::loader::_helpers::table_plan::{
    added_constants, is_direct, publishes_once, pulls_unloaded,
};

/// Whether `append` can write the batches straight into the table: not a logged table (its rows wait for a changed `ctx.state`), and a writer that can (on Snowflake, a route loading files into the table itself).
fn writes_directly(table: &TablePlan, writer: &Writer) -> bool {
    table.mode == WriteMode::Append && table.logged.is_none() && writer.writes_directly()
}

/// Where the batches go, from the publisher's fields: `staging` when there is one, and `columns` as the staged batches carry them.
pub(in crate::loader) fn write_target<'a>(
    table: &'a TablePlan,
    staging: Option<&'a TableName>,
    columns: &'a [DesiredColumn],
    ((load_id, batch), (direct, builds)): ((&'a str, u64), (bool, bool)),
) -> WriteTarget<'a> {
    WriteTarget {
        table: &table.spec.destination,
        staging,
        columns,
        write_columns: &table.schema.write_columns,
        compat: table.spec.compat,
        load_id,
        batch,
        direct,
        builds,
        constants: &table.spec.constants,
    }
}

/// Whether a commit by `mode` replaces some of the table's rows, whose child rows it clears first: a merge, `delete_insert`, and captured changes (a changelog's only on a replay, whose rows keep their ids).
pub(crate) fn replaces_roots(mode: &WriteMode) -> bool {
    matches!(
        mode,
        WriteMode::Merge { .. } | WriteMode::DeleteInsert { .. } | WriteMode::Changes(_)
    )
}

/// Under a write that replaces rows (see [`replaces_roots`]), the child tables of `table` already in the destination that its commits clear the rows of their replaced roots from: every one under `"all"`, the listed paths' otherwise.
async fn merged_children(
    table: &TablePlan,
    destination: &Destination,
) -> Result<Vec<String>, LoaderError> {
    if !replaces_roots(&table.mode) {
        return Ok(Vec::new());
    }
    let spec = &table.spec;
    let adapter = sql_adapter(spec.warehouse);
    let listed: Option<Vec<String>> = match &spec.unnest {
        Unnest::Paths(paths) => Some(
            paths
                .iter()
                .map(|path| adapter.stored_table_name(&unnested_table(spec, path)))
                .collect(),
        ),
        _ => None,
    };
    Ok(live_child_tables(destination, spec)
        .await?
        .into_iter()
        .filter(|live| {
            listed
                .as_ref()
                .is_none_or(|listed| listed.contains(&adapter.stored_table_name(live)))
        })
        .collect())
}

/// The columns the staged batches carry: the read columns as the source gives them, the constant columns the load adds, then the `_sqlload_*` metadata.
pub(crate) fn staged_columns(table: &TablePlan) -> Vec<DesiredColumn> {
    let choices = list_choices(&table.spec);
    let mut columns: Vec<DesiredColumn> = table
        .read_columns
        .iter()
        .map(|column| DesiredColumn {
            name: column.name.clone(),
            column_type: column.output.clone(),
            origin: ColumnOrigin::Source,
        })
        .filter(|column| !table.spec.unnest.takes(&column.name, &choices))
        .collect();
    columns.extend(added_constants(&table.spec, &table.read_columns).map(
        |(name, column_type, _)| DesiredColumn {
            name: name.clone(),
            column_type: column_type.clone(),
            origin: ColumnOrigin::Constant,
        },
    ));
    let read: Vec<&str> = table
        .read_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    columns.extend(
        table
            .schema
            .write_columns
            .iter()
            .filter(|column| {
                column.origin == ColumnOrigin::Metadata && !read.contains(&column.name.as_str())
            })
            .cloned(),
    );
    columns.extend(numbers_rows(table).then(|| DesiredColumn {
        name: MERGE_ROW_COLUMN.to_owned(),
        column_type: ColumnType::Int,
        origin: ColumnOrigin::Metadata,
    }));
    columns.extend(table.spec.root_rows.then(|| DesiredColumn {
        name: ROOT_ROW_COLUMN.to_owned(),
        column_type: ColumnType::Int,
        origin: ColumnOrigin::Metadata,
    }));
    columns
}

/// The columns a row's id is the hash of, so a row keeps its id (and its child rows theirs) on every load: a merge's `unique_key`; with `unnest`, a `postgres_cdc` table's key, in its snapshot as in its changes, and in a changelog the change's `_sqlload_lsn` and `_sqlload_seq` before it, so a replayed change gets its ids again; `None` for random ids.
fn id_key(table: &TablePlan) -> Option<Vec<String>> {
    let cdc = table
        .spec
        .cdc
        .as_ref()
        .filter(|_| table.spec.unnest != Unnest::None);
    match (&table.mode, cdc) {
        (WriteMode::Merge { unique_key, .. }, _) => Some(unique_key.clone()),
        (_, Some(cdc)) if cdc.apply == ChangeApply::Changelog => Some(
            [CDC_LSN_COLUMN, CDC_SEQ_COLUMN]
                .map(str::to_owned)
                .into_iter()
                .chain(table.spec.unique_key.iter().cloned())
                .collect(),
        ),
        (_, Some(_)) => Some(table.spec.unique_key.clone()),
        _ => None,
    }
}

/// Each row's id when the table gives its rows one: the hash of its key (see [`id_key`]), otherwise a fresh random id.
fn row_ids(table: &TablePlan, batch: &RecordBatch) -> Result<Option<Vec<String>>, String> {
    if !table.spec.row_id {
        return Ok(None);
    }
    let Some(unique_key) = id_key(table) else {
        return random_row_ids(batch.num_rows()).map(Some);
    };
    let schema = batch.schema();
    let mut key: Vec<(String, usize)> = Vec::with_capacity(unique_key.len());
    for name in &unique_key {
        let index = schema.index_of(name).map_err(|_| {
            format!("the staged rows have no unique_key column {name} to give their row ids from")
        })?;
        key.push((name.clone(), index));
    }
    key_row_ids(batch, &key).map(Some)
}

/// Under a merge, the ordinal each of `batch`'s rows is staged with, which its child rows carry so a dropped repeated row's leave with it: a files source's `_sqlload_source_row` when the merge ranks by it, otherwise from `first`, as `_sqlload_row` numbers them; `None` without a merge.
fn root_ordinals(
    table: &TablePlan,
    batch: &RecordBatch,
    first: i64,
) -> Result<Option<Vec<i64>>, String> {
    let WriteMode::Merge { dedupe, .. } = &table.mode else {
        return Ok(None);
    };
    let rows = i64::try_from(batch.num_rows()).unwrap_or(i64::MAX);
    let Some(dedupe) = dedupe
        .as_ref()
        .filter(|dedupe| dedupe.ordinal != MERGE_ROW_COLUMN)
    else {
        return Ok(Some((first..first + rows).collect()));
    };
    let column = batch
        .column_by_name(&dedupe.ordinal)
        .ok_or_else(|| format!("the staged rows have no {} column", dedupe.ordinal))?;
    let ordinals =
        cast(column, &arrow_schema::DataType::Int64).map_err(|error| error.to_string())?;
    Ok(Some(
        ordinals
            .as_primitive::<arrow_array::types::Int64Type>()
            .iter()
            .map(|ordinal| ordinal.unwrap_or_default())
            .collect(),
    ))
}

/// `batch` with `_sqlload_root_row` added last: each row's root row's ordinal, `roots`.
fn with_root_rows(batch: RecordBatch, roots: Vec<i64>) -> Result<RecordBatch, LoaderError> {
    let schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    fields.push(Arc::new(Field::new(
        ROOT_ROW_COLUMN,
        arrow_schema::DataType::Int64,
        false,
    )));
    columns.push(Arc::new(Int64Array::from(roots)));
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
    )
    .map_err(|error| LoaderError::Invalid(error.to_string()))
}

/// Whether the table's staged rows carry `_sqlload_row`: a merge keeping the last of the rows sharing a key, from a source without its own row numbers.
fn numbers_rows(table: &TablePlan) -> bool {
    matches!(&table.mode, WriteMode::Merge { dedupe: Some(dedupe), .. } if dedupe.ordinal == MERGE_ROW_COLUMN)
}

/// `batch` with `constants` added after its columns, each its value in every row, converted strictly to its type's Arrow type; the batch's schema metadata is kept.
pub(crate) fn with_constants(
    batch: &RecordBatch,
    constants: &[&(String, ColumnType, String)],
) -> Result<RecordBatch, String> {
    let rows = batch.num_rows();
    let schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    for (name, column_type, value) in constants {
        let field = arrow_field(name, column_type)
            .ok_or_else(|| format!("constant column {name}: {column_type} has no Arrow type"))?;
        let text: ArrayRef = Arc::new(StringArray::from(vec![value.as_str(); rows]));
        let column = cast_with_options(&text, field.data_type(), &strict).map_err(|error| {
            format!("constant column {name}: {value:?} does not convert to {column_type}: {error}")
        })?;
        fields.push(Arc::new(field));
        columns.push(column);
    }
    let schema = Schema::new_with_metadata(fields, schema.metadata().clone());
    RecordBatch::try_new(Arc::new(schema), columns).map_err(|error| error.to_string())
}

/// `batch` with this load's id and load time added under `compat`, so the staged files carry them, then each row's id from `row_ids` when the table gives rows one, a NULL `_sqlload_deleted_at` when it soft-deletes rows (`soft_deletes`), so a merge clears it on a row whose key came back, and with `first_row` its rows numbered from it in `_sqlload_row`.
fn with_metadata(
    batch: &RecordBatch,
    ((compat, load_id), row_ids): ((Compat, &str), Option<Vec<String>>),
    (loaded_at_micros, first_row, soft_deletes): (i64, Option<i64>, bool),
) -> Result<RecordBatch, String> {
    let rows = batch.num_rows();
    let schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let loaded_at =
        TimestampMicrosecondArray::from(vec![loaded_at_micros; rows]).with_timezone("UTC");
    fields.push(Arc::new(Field::new(
        metadata_name(compat, MetadataRole::LoadId),
        arrow_schema::DataType::Utf8,
        true,
    )));
    fields.push(Arc::new(Field::new(
        metadata_name(compat, MetadataRole::LoadedAt),
        loaded_at.data_type().clone(),
        true,
    )));
    columns.push(Arc::new(StringArray::from(vec![load_id; rows])));
    columns.push(Arc::new(loaded_at));
    if let Some(ids) = row_ids {
        fields.push(Arc::new(Field::new(
            metadata_name(compat, MetadataRole::RowId),
            arrow_schema::DataType::Utf8,
            true,
        )));
        columns.push(Arc::new(StringArray::from(ids)));
    }
    if soft_deletes {
        let deleted_at = TimestampMicrosecondArray::new_null(rows).with_timezone("UTC");
        fields.push(Arc::new(Field::new(
            CDC_DELETED_AT_COLUMN,
            deleted_at.data_type().clone(),
            true,
        )));
        columns.push(Arc::new(deleted_at));
    }
    if let Some(first) = first_row {
        fields.push(Arc::new(Field::new(
            MERGE_ROW_COLUMN,
            arrow_schema::DataType::Int64,
            false,
        )));
        columns.push(Arc::new(Int64Array::from_iter_values(
            (0..i64::try_from(rows).unwrap_or(i64::MAX)).map(|row| first + row),
        )));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| error.to_string())
}

/// `parent`, the rows unnesting left, with a NULL column for each column of `masked` it took that the table still has among `staged`: a column the plan kept, chosen as a list field from later rows (`unnest = "all"`), stays in the table empty.
fn with_planned_columns(
    (masked, parent): (&RecordBatch, RecordBatch),
    staged: &[DesiredColumn],
) -> Result<RecordBatch, String> {
    let schema = masked.schema();
    let mut fields: Vec<Arc<Field>> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        let kept = parent.column_by_name(field.name()).cloned();
        let planned = staged.iter().any(|column| column.name == *field.name());
        let column = match (kept, planned) {
            (Some(kept), _) => kept,
            (None, true) => new_null_array(field.data_type(), masked.num_rows()),
            (None, false) => continue,
        };
        fields.push(field.clone());
        columns.push(column);
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            parent.schema().metadata().clone(),
        )),
        columns,
    )
    .map_err(|error| error.to_string())
}

/// `batch` with only the columns the table writes, as a build table holds no others (an enforced contract leaves undeclared columns out of a table that unnests, whose rows are read rather than copied).
fn written_only(batch: RecordBatch, written: &[DesiredColumn]) -> Result<RecordBatch, LoaderError> {
    let names: BTreeSet<&str> = written.iter().map(|column| column.name.as_str()).collect();
    let kept: Vec<usize> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| names.contains(field.name().as_str()))
        .map(|(position, _)| position)
        .collect();
    batch
        .project(&kept)
        .map_err(|error| LoaderError::Invalid(error.to_string()))
}

impl Publisher {
    /// A table's load ready for its first batch: its own connection, and (unless the batches go straight into the table) a fresh staging table. A merge `--reload`'s build table is created last, so no failure here leaves it behind; from then on `close` drops it.
    pub(crate) async fn open(
        table: TablePlan,
        (destination, options): (&Destination, &PlanOptions),
        run: Option<RunEntry>,
    ) -> Result<Self, LoaderError> {
        let destination = table_destination(destination)?;
        let writer = open_table_writer(&destination, table.stage.clone())?;
        let runs_sql = sql_adapter(table.spec.warehouse).runs_sql();
        if run.is_some() && runs_sql {
            ensure_runs_table(&table, &destination).await?;
        }
        let load_id = uuid::Uuid::new_v4().to_string();
        let files = match (runs_sql, &table.output) {
            (false, Some(output)) => {
                let live = open_folder(&output.path.folder)?;
                let lease = take_lease(&live, &load_id, lease_now())
                    .await?
                    .map_err(LoaderError::Transient)?;
                let (replaced, staged) =
                    match files_to_replace((&live, &lease), &table, output).await {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            give_back(&live, &lease).await;
                            return Err(error);
                        }
                    };
                let keeper = keep_in_background(&live, &lease);
                let (folder, replaced, swap) = match staged {
                    Some((staged, held)) => (
                        staged,
                        ReplacedFiles::default(),
                        Some(FolderSwap {
                            live,
                            replaced: replaced.keys(&BTreeSet::new()).into_iter().collect(),
                            held,
                        }),
                    ),
                    None => (live, replaced, None),
                };
                Some(FilesSink {
                    output: output.clone(),
                    folder,
                    replaced,
                    writer: None,
                    deleted: BTreeSet::new(),
                    reading: Vec::new(),
                    lease,
                    keeper: Some(keeper),
                    swap,
                })
            }
            (false, None) => {
                return Err(LoaderError::Invalid(format!(
                    "{}: a files target table has no files planned",
                    table.spec.destination.table
                )));
            }
            _ => None,
        };
        let direct = files.is_none()
            && table.spec.checks.is_none()
            && table.spec.unnest == Unnest::None
            && !table.spec.root_rows
            && writes_directly(&table, &writer);
        let at_end = matches!(table.mode, WriteMode::Replace | WriteMode::Recreate);
        let keeps_tables = sql_adapter(table.spec.warehouse).keeps_tables();
        let builds =
            at_end && files.is_none() && !keeps_tables && !sends_rows(table.stage.as_ref());
        let mut shape: Option<TableShape> = None;
        let staging = match direct || files.is_some() {
            true => None,
            false if builds => {
                let id = staging_id(&load_id, chrono::Utc::now().naive_utc());
                shape = match table.mode {
                    WriteMode::Replace => {
                        table_shape(&destination, &table.spec.destination).await?
                    }
                    _ => None,
                };
                let differing = table
                    .live
                    .as_deref()
                    .filter(|_| table.mode == WriteMode::Recreate);
                let adapter = sql_adapter(table.spec.warehouse);
                let build = adapter.staging_relation_name(&table.spec.destination, &id);
                let create = adapter.render_create_build_table(
                    &build,
                    &table.spec.destination,
                    (
                        &table.schema.write_columns,
                        differing,
                        shape.as_ref().map(|shape| shape.definition.as_str()),
                    ),
                );
                execute_ddl(&destination, create).await?;
                Some(build)
            }
            false => {
                let (staging, create) = sql_adapter(table.spec.warehouse)
                    .render_create_staging_table(
                        &table.spec.destination,
                        &staged_columns(&table),
                        &staging_id(&load_id, chrono::Utc::now().naive_utc()),
                    );
                execute_ddl(&destination, vec![create]).await?;
                Some(staging)
            }
        };
        let committed_state = run.as_ref().and_then(|run| run.record.state.clone());
        let children = match table.spec.unnest {
            Unnest::None => None,
            _ => Some(ChildTables {
                merged: merged_children(&table, &destination).await?,
                choices: list_choices(&table.spec),
                live: match table.spec.unnest {
                    Unnest::All => live_child_tables(&destination, &table.spec).await?,
                    _ => Vec::new(),
                },
                destination: table_destination(&destination)?,
                options: options.clone(),
                feeds: Vec::new(),
                ignored: Vec::new(),
                changes: Vec::new(),
            }),
        };
        let rebuild = match records_files(&table) {
            true => merge_rebuild(&table, &destination).await?,
            false => None,
        };
        let spool = holds_commits(&table)
            .then(|| room_to_hold(&table, options.batch_bytes.unwrap_or(BATCH_BYTES)))
            .transpose()?;
        let publisher = Self {
            children,
            writer,
            at_end,
            builds,
            indexes: shape.map(|shape| shape.indexes).unwrap_or_default(),
            table,
            destination,
            run,
            load_id,
            loaded_at: now_utc_text(),
            direct,
            staging,
            batch: 1,
            arrow_bytes: 0,
            batch_rows: 0,
            progressed_at: std::time::Instant::now(),
            child: false,
            staged_ordinal: 0,
            waiting_rows: 0,
            waiting_range: None,
            committed_rows: 0,
            commits: 0,
            maximum: None,
            state: committed_state.clone(),
            committed_state,
            batch_bytes: options.batch_bytes.unwrap_or(BATCH_BYTES),
            files,
            merged_file: None,
            replaced: None,
            marked: None,
            older: 0,
            run_keys: Default::default(),
            fed_files: Vec::new(),
            settled_files: Vec::new(),
            rebuild,
            transform: None,
            spool,
            pieces: Pieces::default(),
        };
        publisher.create_marker_table().await?;
        Ok(publisher)
    }

    /// Stage one batch of the source's rows (masked, with the load's metadata added); `state` is a Python table's `ctx.state` when its last row crossed.
    pub(crate) async fn stage(
        &mut self,
        batch: RecordBatch,
        state: Option<String>,
    ) -> Result<(), LoaderError> {
        if state.is_some() {
            self.state = state;
        }
        let masked = mask_batch(batch, &self.table.spec.masked).map_err(LoaderError::Invalid)?;
        self.stage_masked(masked).await
    }

    /// Stage one batch of the source's rows already masked; masking leaves the cursor as it is, as a masked cursor is refused at plan.
    pub(crate) async fn stage_masked(&mut self, masked: RecordBatch) -> Result<(), LoaderError> {
        if masked.num_rows() == 0 {
            return Ok(());
        }
        self.maximum = fold_maximum(self.run.as_ref(), &masked, self.maximum.take())
            .map_err(LoaderError::Invalid)?;
        let constants: Vec<&(String, ColumnType, String)> =
            added_constants(&self.table.spec, &self.table.read_columns).collect();
        let masked = with_constants(&masked, &constants).map_err(LoaderError::Invalid)?;
        if self.files.is_some() && self.children.is_none() {
            self.batch_rows += masked.num_rows() as u64;
            self.arrow_bytes += masked.get_array_memory_size();
            self.report_rows();
            return self.stage_files(masked).await;
        }
        let ids = row_ids(&self.table, &masked).map_err(LoaderError::Invalid)?;
        let masked = match &mut self.children {
            Some(children) => {
                let spec = &self.table.spec;
                for (name, chosen) in list_choices(spec) {
                    children.choices.entry(name).or_insert(chosen);
                }
                let rooted = id_key(&self.table).is_some()
                    || matches!(self.table.mode, WriteMode::DeleteInsert { .. });
                let given = ids.as_deref().unwrap_or_default();
                let unnested = unnest_batch(
                    &masked,
                    &UnnestRequest {
                        table: &spec.destination.table,
                        unnest: &spec.unnest,
                        max_depth: spec.max_depth,
                        naming: spec.naming,
                        compat: spec.compat,
                        name_limit: sql_adapter(spec.warehouse).max_identifier_length(),
                        flatten: &spec.flatten,
                        live: &children.live,
                        ids: given,
                        roots: rooted.then_some(given),
                    },
                    &mut children.choices,
                )
                .and_then(|unnested| without_deleted_items(&self.table, &masked, unnested))
                .map_err(LoaderError::Invalid)?;
                let ordinals = root_ordinals(&self.table, &masked, self.staged_ordinal)
                    .map_err(LoaderError::Invalid)?;
                children
                    .stage(
                        (&self.table, (&self.load_id, &self.loaded_at)),
                        (unnested.children, ordinals.as_deref()),
                    )
                    .await?;
                with_planned_columns((&masked, unnested.parent), &staged_columns(&self.table))
                    .map_err(LoaderError::Invalid)?
            }
            None => masked,
        };
        self.stage_rows(masked, (ids, None)).await
    }

    /// Stage `masked`, rows as the table's columns hold them, with this load's metadata and each row's id from `ids` when the table gives rows one (a child table's are its parent's to give).
    pub(crate) async fn stage_rows(
        &mut self,
        masked: RecordBatch,
        (ids, roots): (Option<Vec<String>>, Option<Vec<i64>>),
    ) -> Result<(), LoaderError> {
        let micros = DateTime::parse_from_rfc3339(&self.loaded_at)
            .map(|time| time.timestamp_micros())
            .map_err(|error| LoaderError::Invalid(error.to_string()))?;
        let first_row = numbers_rows(&self.table).then_some(self.staged_ordinal);
        self.staged_ordinal += i64::try_from(masked.num_rows()).unwrap_or(i64::MAX);
        let batch = with_metadata(
            &masked,
            ((self.table.spec.compat, &self.load_id), ids),
            (micros, first_row, self.table.spec.soft_deletes()),
        )
        .map_err(LoaderError::Invalid)?;
        let batch = match roots {
            Some(roots) => with_root_rows(batch, roots)?,
            None => batch,
        };
        let batch = match self.builds {
            true => written_only(batch, &self.table.schema.write_columns)?,
            false => batch,
        };
        self.batch_rows += batch.num_rows() as u64;
        self.arrow_bytes += batch.get_array_memory_size();
        self.report_rows();
        if self.files.is_some() {
            return self.stage_files(batch).await;
        }
        self.write_rows(batch).await
    }

    /// A `table_progress` event for the batch being read, with the rows so far, at most once per `PROGRESS_EVERY`; a batch's end reports it anyway (see [`Self::flush_batch`]).
    fn report_rows(&mut self) {
        if self.progressed_at.elapsed() < PROGRESS_EVERY {
            return;
        }
        self.progressed_at = std::time::Instant::now();
        self.report(EventKind::TableProgress {
            unit: "batch".to_owned(),
            count: self.batch,
            rows: self.committed_rows + self.waiting_rows + self.batch_rows,
        });
    }

    /// Send `kind` for this table; a child table's publisher sends nothing (see [`Publisher::child`]).
    fn report(&self, kind: EventKind) {
        if !self.child {
            emit(Some(&self.table.outcome_name()), kind);
        }
    }

    /// The `created ...`, `altered ...` and `loaded ...` lines of the child tables filled so far.
    pub(crate) fn child_changes(&mut self) -> Vec<String> {
        self.children
            .as_mut()
            .map(|children| std::mem::take(&mut children.changes))
            .unwrap_or_default()
    }

    /// Whether the batch holds about 1 GB (Parquet staged on Snowflake, with the zstd files `send = "at_commit"` holds for it, which come within a few percent of that Parquet, Arrow elsewhere), where a size-cut table may end it.
    pub(crate) fn full(&self) -> bool {
        let bytes = match (self.writer.staged_bytes(), &self.spool) {
            (Some(staged), Some(spool)) => staged + spool.held_bytes(self.batch),
            (Some(staged), None) => staged,
            (None, _) => self.arrow_bytes,
        };
        bytes >= self.batch_bytes
    }

    /// A point where the rows so far may commit, replacing `range` under `delete_insert`: the batch is loaded (one `COPY` for all its files), then committed unless the table commits only at the end (a `replace`, or a read the warehouse publishes once, see [`publishes_once`], short of a merge's [`MERGE_RUN_ROWS`]), or is logged and its `ctx.state` has not changed since the last commit (its rows then wait in the staging table).
    pub(crate) async fn cut(&mut self, range: CommitRange) -> Result<(), LoaderError> {
        self.flush_batch().await?;
        let unfinished = self.partition_unfinished(&range)?;
        self.waiting_range = Some(match (self.waiting_range.take(), range) {
            (Some(CommitRange::Window { from, .. }), CommitRange::Window { to, .. }) => {
                CommitRange::Window { from, to }
            }
            (Some(CommitRange::Values(mut values)), CommitRange::Values(more)) => {
                values.extend(more);
                CommitRange::Values(values)
            }
            (_, range) => range,
        });
        let unchanged = self.run.is_some() && self.state == self.committed_state;
        let held = publishes_once(&self.table)
            && !(matches!(self.table.mode, WriteMode::Merge { .. })
                && self.waiting_rows >= MERGE_RUN_ROWS);
        if self.direct || self.at_end || unchanged || unfinished || held {
            return Ok(());
        }
        self.commit(false).await
    }

    /// The end of the read: the last batch is loaded and everything waiting commits (`replace` swaps the staging table in, `--reload` rebuilds the table from it, and a `delete_insert` read of no rows still clears its range, since its rows were deleted upstream); a logged table records `state`, where its function left `ctx.state`, even without rows; a swapped-in or rebuilt table gets its description as its comment again (a table refilled in place keeps its own), where a failure only warns, as its rows are already in.
    pub(crate) async fn finish(&mut self, state: Option<String>) -> Result<u64, LoaderError> {
        self.stopped()?;
        self.finish_pieces().await?;
        self.flush_batch().await?;
        if state.is_some() {
            self.state = state;
        }
        let empty_range = self.commits == 0
            && deletes_range(&self.table.mode)
            && !matches!(self.table.commits, CommitRule::Windows(_));
        let pending = self.waiting_rows > 0
            || (self.waiting_range.is_some() && self.files.is_none())
            || self.at_end
            || empty_range
            || matches!(&self.table.mode, WriteMode::Changes(change) if change.truncated.is_some())
            || matches!(self.table.mode, WriteMode::MarkMissing(_))
            || self.leftover_deletes()
            || self.pieces.unfinished;
        if let Some(children) = self.children.as_mut().filter(|_| self.direct || !pending) {
            Box::pin(children.finish(
                (&self.table, (&self.load_id, &self.loaded_at)),
                (&self.destination, None),
            ))
            .await?;
        }
        if !self.direct && pending {
            self.report(EventKind::TableCommitting {
                rows: self.waiting_rows + self.committed_rows,
            });
        }
        match (self.direct, pending, &self.run) {
            (false, true, _) => self.commit(true).await?,
            (_, _, Some(_)) if self.commits == 0 || self.state != self.committed_state => {
                let record = self.record(0)?;
                append_run(&self.table, &self.destination, &record).await?;
            }
            _ => {}
        }
        if let Some(rebuild) = &self.rebuild {
            swap_rebuild(&self.table, &self.destination, rebuild).await?;
        }
        let spec = &self.table.spec;
        let adapter = sql_adapter(spec.warehouse);
        let swapped = self.at_end || self.rebuild.is_some();
        let comment = adapter
            .render_table_comment(&spec.destination, &spec.description)
            .filter(|_| swapped && self.files.is_none() && !adapter.keeps_tables());
        if let Some(comment) = comment
            && let Err(error) = execute(&self.destination, vec![comment]).await
        {
            tracing::warn!(table = %spec.destination.table, %error, "the swapped-in table kept no description: setting its comment failed");
        }
        if !self.child
            && let Err(error) = self.writer.checkpoint(self.pieces.large > 0).await
        {
            tracing::warn!(table = %spec.destination.table, %error, "the table loaded, but DuckDB's checkpoint after it failed: raise memory_limit, or lower batch_size, before the next load");
        }
        Ok(self.committed_rows)
    }

    /// Drop the staging table and its swap table, and the staged files of a batch that did not load; best effort, after success or failure.
    pub(crate) async fn close(&mut self) {
        self.spool = None;
        if let Some(children) = &mut self.children {
            Box::pin(children.close()).await;
        }
        self.abandon_files().await;
        self.abandon_pieces().await;
        if let Some(sink) = self.files.as_mut() {
            if let Some((stop, renewal)) = sink.keeper.take() {
                drop(stop);
                if let Err(error) = renewal.await {
                    tracing::warn!(%error, "the lease's renewal task failed");
                }
            }
            give_back(leased_folder(sink), &sink.lease).await;
        }
        self.writer.rollback_batch().await;
        let warehouse = self.table.spec.warehouse;
        if let Some(staging) = &self.staging {
            let aside = TableName {
                table: format!("{}{SWAP_SUFFIX}", staging.table),
                ..staging.clone()
            };
            let build = self.rebuild.as_ref().map(|rebuild| rebuild.build.clone());
            let drops: Vec<String> = [staging.clone(), aside]
                .into_iter()
                .chain(build)
                .map(|table| {
                    format!(
                        "DROP TABLE IF EXISTS {}",
                        sql_adapter(warehouse).render_qualified_name(&table)
                    )
                })
                .collect();
            if let Err(error) = execute_ddl(&self.destination, drops).await {
                tracing::warn!(%error, table = %staging.table, "cannot drop the staging table");
            }
        }
        let columns = staged_columns(&self.table);
        let target = write_target(
            &self.table,
            self.staging.as_ref(),
            &columns,
            ((&self.load_id, self.batch), (self.direct, self.builds)),
        );
        self.writer.remove_batch_files(&target).await;
    }

    /// The live columns a build table started with, which stay; `None` when the table holds staged rows. A `--reload` rebuilds with the current columns only.
    pub(crate) fn started_with(&self) -> Option<&[LiveColumn]> {
        self.builds.then(|| match self.table.mode {
            WriteMode::Replace => self.table.live.as_deref().unwrap_or_default(),
            _ => &[],
        })
    }

    /// Wait until every row sent without files is in the staging table; under `send = "at_commit"`, the held rows are sent first, before a schema change makes them unlike the rows after it: the batches that ended are loaded, and the open batch's rows written, leaving it open for its end to load or commit as it would have.
    pub(crate) async fn settle_rows(&mut self) -> Result<(), LoaderError> {
        if let Some(spool) = self.spool.as_mut() {
            let ended = spool.take_ended(self.batch);
            let open = spool.take_open_files(self.batch);
            for held in ended {
                Box::pin(self.send_files((held.batch, &held.files), true)).await?;
            }
            Box::pin(self.send_files((self.batch, &open), false)).await?;
        }
        self.writer.settle_rows().await
    }

    /// The row channel again after a schema change, which may have replaced the staging table and its pipe.
    pub(crate) fn reopen_rows(&mut self) {
        self.writer.reopen_rows();
    }

    /// Load the batch where it goes: `COPY` its staged files (straight into the table, committing, or into the staging table) and remove them; DuckDB commits a straight batch's transaction. Then start the next batch.
    pub(crate) async fn flush_batch(&mut self) -> Result<(), LoaderError> {
        match self.spool.is_some() {
            true if self.batch_rows == 0 => return Ok(()),
            true if self.direct => Box::pin(self.send_held()).await?,
            true => {}
            false => {
                if self.batch_rows > 0 && (self.direct || self.builds) {
                    let into = (self.staging.clone())
                        .unwrap_or_else(|| self.table.spec.destination.clone());
                    let group = self.check_row_group(&into).await?;
                    self.pieces.group = group.or(self.pieces.group);
                }
                let columns = staged_columns(&self.table);
                let target = write_target(
                    &self.table,
                    self.staging.as_ref(),
                    &columns,
                    ((&self.load_id, self.batch), (self.direct, self.builds)),
                );
                self.writer.finish_files(&target).await?;
                if self.batch_rows == 0 {
                    return Ok(());
                }
                self.writer.settle_rows().await?;
                self.writer.load_batch(&target).await?;
                self.writer.next_batch();
                if self.direct || self.builds {
                    self.pieces.held = 0;
                }
            }
        }
        match self.direct {
            true => {
                self.committed_rows += self.batch_rows;
                self.commits += 1;
            }
            false => self.waiting_rows += self.batch_rows,
        }
        self.batch += 1;
        self.batch_rows = 0;
        self.arrow_bytes = 0;
        self.progressed_at = std::time::Instant::now();
        self.report(EventKind::TableProgress {
            unit: "batch".to_owned(),
            count: self.batch - 1,
            rows: self.committed_rows + self.waiting_rows,
        });
        Ok(())
    }

    /// Load a files batch by having the destination `COPY` the files itself, where the plan chose that; a batch it cannot load that way fails, never going through SQLLoad. `None` when the plan has SQLLoad read the files (`hmac` masking, or a destination that does not read files itself).
    pub(crate) async fn load_direct(
        &mut self,
        (files, footers, folder): (&[DataFile], &BatchFooters, Option<&OpenFolder>),
        range: &CommitRange,
    ) -> Result<Option<DirectLoaded>, LoaderError> {
        let Some(read) = &self.table.files else {
            return Ok(None);
        };
        if !is_direct(self.table.route.as_ref()) {
            return Ok(None);
        }
        let columns = staged_columns(&self.table);
        let mode = commit_mode(&self.table.mode, Some(range.clone()));
        let batch = DirectBatch {
            spec: &self.table.spec,
            read,
            write_columns: &self.table.schema.write_columns,
            target: write_target(
                &self.table,
                self.staging.as_ref(),
                &columns,
                ((&self.load_id, self.batch), (self.direct, self.builds)),
            ),
            loaded_at: &self.loaded_at,
            files,
            footers,
            mode: &mode,
            live: self.table.live.as_deref(),
            source_folder: folder,
        };
        match load_files(&mut self.writer, &batch).await? {
            Some(loaded) => Ok(Some(loaded)),
            None => Err(LoaderError::Invalid(format!(
                "{}: the plan has the destination load these files itself, but it cannot, and files never go through SQLLoad instead",
                self.table.spec.destination.table
            ))),
        }
    }

    /// Load one unload's `files` by having the destination read them itself, where the plan chose that; the destination must load them so.
    pub(crate) async fn load_unloaded(
        &mut self,
        files: &UnloadedFiles,
    ) -> Result<DirectLoaded, LoaderError> {
        let columns = staged_columns(&self.table);
        let batch = UnloadedBatch {
            files,
            read: &self.table.read_columns,
            write_columns: &self.table.schema.write_columns,
            target: write_target(
                &self.table,
                self.staging.as_ref(),
                &columns,
                ((&self.load_id, self.batch), (self.direct, self.builds)),
            ),
            loaded_at: &self.loaded_at,
        };
        let loaded = load_unloaded(&mut self.writer, &batch).await?.ok_or_else(|| {
            LoaderError::Invalid(format!(
                "{}: the plan has the destination load the unloaded files itself, but it cannot",
                self.table.spec.destination.table
            ))
        })?;
        self.note_direct(&loaded);
        Ok(loaded)
    }

    /// Pass `part` of the table's rows on: the destination loads the unloaded files itself where the plan chose that, otherwise SQLLoad stages the rows.
    async fn pass_part(
        &mut self,
        reader: &SourceReader,
        part: ReadPart,
    ) -> Result<(), LoaderError> {
        let table = self.table.clone();
        if pulls_unloaded(table.route.as_ref()) {
            unload_source(reader, &table, part, async |files| {
                self.load_unloaded(files).await
            })
            .await?;
            return Ok(());
        }
        read_source(reader, &table, part, async |batch| stage(self, batch).await).await?;
        Ok(())
    }

    /// Note what a direct batch did; a batch that is not yet committed waits for `cut`.
    pub(crate) fn note_direct(&mut self, loaded: &DirectLoaded) {
        match loaded {
            DirectLoaded::Committed(rows) => {
                self.committed_rows += rows;
                self.commits += 1;
                self.settled_files.append(&mut self.fed_files);
            }
            DirectLoaded::Staged(rows) => self.waiting_rows += rows,
        }
    }

    /// Apply the write strategy to the waiting rows in one transaction, with a logged table's record and the emptying of the staging table for the next batches.
    pub(in crate::loader) async fn commit(&mut self, last: bool) -> Result<(), LoaderError> {
        self.stopped()?;
        if self.files.is_some() {
            self.waiting_range = None;
            if let Some(children) = &mut self.children {
                match last {
                    true => {
                        Box::pin(children.finish(
                            (&self.table, (&self.load_id, &self.loaded_at)),
                            (&self.destination, None),
                        ))
                        .await?
                    }
                    false => Box::pin(children.cut((&self.table, &self.destination, None))).await?,
                }
            }
            return self.commit_files(last).await;
        }
        Box::pin(self.send_held()).await?;
        let Some(staging) = self.staging.clone() else {
            return Ok(());
        };
        let spec = &self.table.spec;
        let mode = match self.pieces.piece {
            true => piece_mode(&self.table.mode),
            false => commit_mode(&self.table.mode, self.waiting_range.take()),
        };
        let warehouse = spec.warehouse;
        let runs = runs_table(&spec.destination);
        let merging_file = self.merged_file.is_some();
        let file = self.merged_file.clone();
        let (ran, taken) = match self.pieces.piece {
            true => (None, None),
            false => (
                commit_transform(&self.table, self.transform.as_mut()),
                self.merged_file.take(),
            ),
        };
        let mut merged = taken.map(|file| {
            let record = (file, self.waiting_rows);
            with_transform(
                &self.table,
                merged_record(spec, (&self.load_id, self.commits + 1), record),
            )
        });
        let held = self.rebuild.as_ref().and_then(|_| merged.take());
        let record = match (&self.run, merged) {
            _ if self.pieces.piece => None,
            (Some(_), _) => {
                let record = self.record(self.waiting_rows)?;
                Some(with_transform(&self.table, record))
            }
            (None, Some(record)) => Some(record),
            (None, None) => ran.filter(|_| held.is_none()),
        };
        let run_insert = record
            .map(|record| insert_run(&runs, &record, warehouse).map_err(LoaderError::Invalid))
            .transpose()?;
        let spec = &self.table.spec;
        let target = (self.rebuild.as_ref())
            .map_or(&spec.destination, |rebuild| &rebuild.build)
            .clone();
        let target = &target;
        let statements = sql_adapter(warehouse).render_commit(
            &WriteStatementsRequest {
                table: target,
                columns: &self.table.schema.write_columns,
                mode: &mode,
                hashed: &[],
                built: self.builds.then_some(self.indexes.as_slice()),
                spared_load: (self.pieces.committed > 0).then_some((
                    metadata_name(spec.compat, MetadataRole::LoadId),
                    self.load_id.as_str(),
                )),
            },
            &staging,
            (run_insert, self.at_end),
        );
        let checked = CheckedCommit {
            destination: &self.destination,
            warehouse,
            staging: &staging,
            table: target,
            exists: self.table.exists,
            columns: &self.table.schema.write_columns,
            committed: self.committed_rows,
        };
        if let (Some((key, setting)), NullKeys::Fail) = (checked_key(spec, &mode), spec.null_keys) {
            refuse_null_keys(&checked, (key, setting), merging_file).await?;
        }
        if let WriteMode::Merge {
            unique_key,
            dedupe: None,
            ..
        } = &mode
        {
            refuse_repeated_keys(&checked, unique_key, merging_file).await?;
        }
        if let Some(checks) = &spec.checks {
            run_checks(&checked, checks).await?;
        }
        if let WriteMode::MarkMissing(mark) = &mode {
            self.marked = Some(marked_rows(&checked, mark).await?);
        }
        self.older += older_rows(&checked, &mode).await?;
        let replaced = match &mode {
            WriteMode::DeleteInsert { replace_key, .. } if !replace_key.is_empty() => {
                let limit = file
                    .as_ref()
                    .map_or(REPLACED_KEYS_SHOWN as u64 + 1, |_| i64::MAX as u64);
                let mut values = staged_keys(&checked, (replace_key.as_slice(), limit)).await?;
                if let Some(file) = file.as_deref() {
                    let earlier = std::mem::take(&mut self.run_keys);
                    self.run_keys = with_file_keys(&self.table, (file, &values), earlier)
                        .map_err(LoaderError::Invalid)?;
                    let named = (
                        replace_key.as_slice(),
                        SOURCE_FILE_COLUMN,
                        merged_name(file),
                    );
                    let sources = key_sources(&checked, named).await?;
                    listed_sources(&self.table, file, &sources).map_err(LoaderError::Invalid)?;
                }
                values.truncate(REPLACED_KEYS_SHOWN + 1);
                Some((replace_key.clone(), values))
            }
            _ => None,
        };
        if let Some(children) = &mut self.children {
            match last {
                true => {
                    Box::pin(children.finish(
                        (&self.table, (&self.load_id, &self.loaded_at)),
                        (&self.destination, Some((&staging, &mode))),
                    ))
                    .await?
                }
                false => {
                    Box::pin(children.cut((
                        &self.table,
                        &self.destination,
                        Some((&staging, &mode)),
                    )))
                    .await?
                }
            }
        }
        let swaps = self.at_end && !sql_adapter(warehouse).keeps_tables();
        let mut statements = statements;
        let first = match swaps {
            true => sql_adapter(warehouse).unguarded_statements(&statements),
            false => 0,
        };
        let guarded = statements.split_off(first);
        if !statements.is_empty() {
            execute(&self.destination, statements).await?;
        }
        let _held = match swaps {
            true => ddl_guard(&self.destination).await,
            false => None,
        };
        self.execute_checked(guarded, (target, last)).await?;
        if let Some(rebuild) = self.rebuild.as_mut() {
            rebuild.records.extend(held);
        }
        self.committed_rows += self.waiting_rows;
        self.waiting_rows = 0;
        self.commits += 1;
        if let Some(replaced) = replaced {
            self.replaced = Some(ReplacedKeys::added(self.replaced.take(), replaced));
        }
        self.end_commit(last);
        self.settled_files.append(&mut self.fed_files);
        Ok(())
    }

    /// The logged table's record for a commit of `rows`: this load's id, the commit's number, the position so far and `ctx.state` when its last row crossed.
    pub(crate) fn record(&mut self, rows: u64) -> Result<RunRecord, LoaderError> {
        let Some(run) = &mut self.run else {
            return Err(LoaderError::Invalid(
                "only a logged table has run records".to_owned(),
            ));
        };
        run.record.state = self.state.clone();
        run.record.numbered = recorded_names(&self.table.spec);
        let mut record = run.finished(RunOutcome {
            run_id: self.load_id.clone(),
            recorded_at: now_utc_text(),
            rows,
            loaded_maximum: self.maximum.clone(),
            files: Vec::new(),
        });
        record.batch = Some(self.commits + 1);
        run.newest = Some(record.recorded_at.clone());
        Ok(record)
    }
}

impl Publisher {
    /// Before a files batch: read its Parquet files' footers once, or have the destination list a JSON batch's keys where it reads the files itself (its `COPY` would skip a key it is not given), and combine them with the columns known so far; when they add columns, plan the table for them (one read of the live columns) and apply the schema changes once, to the table and the staging table. Returns the footers, which reading the files reuses.
    async fn widen_for(
        &mut self,
        run: &LoadRun<'_>,
        folder: &StoreFolder,
        files: &[DataFile],
    ) -> Result<BatchFooters, LoaderError> {
        let spec = &self.table.spec;
        let Some(source_files) = spec
            .files
            .as_ref()
            .filter(|files| files.transform.is_none())
        else {
            return Ok(BatchFooters::default());
        };
        if source_files.encoding.record.is_some() {
            let direct = self.table.files.is_some() && is_direct(self.table.route.as_ref());
            return match (source_files.encoding.format, direct) {
                (SourceFormat::Csv, true) => Ok(BatchFooters {
                    headers: batch_headers(folder, files, &source_files.encoding).await?,
                    ..BatchFooters::default()
                }),
                _ => Ok(BatchFooters::default()),
            };
        }
        let replaced: Vec<&str> = source_files
            .partition_types
            .iter()
            .map(|(name, _)| name.as_str())
            .chain([SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN])
            .collect();
        let (columns, footers) = match source_files.encoding.format {
            SourceFormat::Parquet => {
                let footers = batch_footers(folder, files).await?;
                let (columns, problems) =
                    batch_columns(&footers, files, &source_files.table.columns, &replaced);
                if !problems.is_empty() {
                    return Err(LoaderError::Invalid(problems.join("; ")));
                }
                (columns, footers)
            }
            SourceFormat::Csv => {
                let (Some(read), true) = (&self.table.files, is_direct(self.table.route.as_ref()))
                else {
                    return Ok(BatchFooters::default());
                };
                let headers = batch_headers(folder, files, &source_files.encoding).await?;
                let mut found: Vec<(String, Option<ColumnType>)> = Vec::new();
                for name in headers.values().flatten() {
                    if !found.iter().any(|(seen, _)| seen == name) {
                        found.push((name.clone(), None));
                    }
                }
                let columns = key_columns(read, &source_files.table.columns, found)
                    .map_err(LoaderError::Invalid)?;
                (
                    columns,
                    BatchFooters {
                        headers,
                        ..BatchFooters::default()
                    },
                )
            }
            SourceFormat::Xlsx | SourceFormat::Xml => return Ok(BatchFooters::default()),
            SourceFormat::Json | SourceFormat::Ndjson => {
                let (Some(read), true) = (&self.table.files, is_direct(self.table.route.as_ref()))
                else {
                    return Ok(BatchFooters::default());
                };
                let Some(found) = file_keys(&self.writer, spec, files).await? else {
                    return Ok(BatchFooters::default());
                };
                let columns = key_columns(read, &source_files.table.columns, found)
                    .map_err(LoaderError::Invalid)?;
                (columns, BatchFooters::default())
            }
        };
        if columns == source_files.table.columns {
            return Ok(footers);
        }
        let source = run.project.sources.get(&spec.source_name).ok_or_else(|| {
            LoaderError::Invalid(format!("source {:?} does not exist", spec.source_name))
        })?;
        let found = FilesTable {
            columns,
            ..source_files.table.clone()
        };
        let (source_table, files_source) = files_source_parts(
            &spec.source_name,
            source,
            (spec.warehouse, (spec.naming, spec.compat)),
            (source_files.location.clone(), found),
        )?;
        let known = known_names(&self.table);
        let resolved = resolve_known(run.project, &spec.source_name, &[source_table], &known);
        let mut widened = match (resolved.problems.first(), resolved.specs.into_iter().next()) {
            (None, Some(widened)) => widened,
            (problem, _) => {
                return Err(LoaderError::Invalid(problem.cloned().unwrap_or_else(
                    || "the files' new columns leave the table unselected".to_owned(),
                )));
            }
        };
        widened.files = Some(FilesSource {
            archive: source_files.archive.clone(),
            ..files_source
        });
        widened.approved = spec.approved;
        let replanned_columns = replan_columns(
            &widened,
            (&self.destination, self.table.live.as_deref()),
            None,
            run.options,
        )
        .await?;
        let (read_columns, live, schema) = (
            replanned_columns.read_columns,
            replanned_columns.live,
            replanned_columns.schema,
        );
        let (read_as, inferred) =
            files_read_as(&widened, &read_columns, &source_files.partition_types);
        let nanos = widened.files.as_ref().map_or_else(Vec::new, |files| {
            nanosecond_columns(&widened, &files.table.columns, live.as_deref())
        });
        let replanned = TablePlan {
            files: self.table.files.clone().map(|read| FilesRead {
                columns: read_as,
                checks: TextChecks {
                    inferred,
                    ..read.checks
                },
                nanos,
                ..read
            }),
            spec: widened,
            read_columns,
            live,
            schema,
            ..self.table.clone()
        };
        self.settle_rows().await?;
        self.reopen_rows();
        let old = staged_columns(&self.table);
        if let Some(statements) = schema_statements(&replanned) {
            execute_ddl(&self.destination, statements).await?;
        }
        if let Some(staging) = &self.staging {
            let changes = sql_adapter(replanned.spec.warehouse).render_staging_changes(
                staging,
                (&old, &staged_columns(&replanned)),
                self.started_with(),
            );
            execute_ddl(&self.destination, changes).await?;
        }
        if let Some(rebuild) = &self.rebuild {
            let changes = sql_adapter(replanned.spec.warehouse).render_staging_changes(
                &rebuild.build,
                (
                    &self.table.schema.write_columns,
                    &replanned.schema.write_columns,
                ),
                Some(&[]),
            );
            execute_ddl(&self.destination, changes).await?;
        }
        self.table = replanned;
        self.widen_files().await?;
        Ok(footers)
    }
}

/// Run `statement` on a Postgres source: its windows' reads share one snapshot, so a row moving between windows mid-load is read once.
async fn snapshot(reader: &SourceReader, statement: &str) -> Result<(), LoaderError> {
    match reader {
        SourceReader::Postgres(client, _) => Ok(client
            .batch_execute(statement)
            .await
            .map_err(PostgresError::from)?),
        _ => Ok(()),
    }
}

/// The cursor value at row `index` of `values` as cursor text, where an ordered read's commit ends.
fn cut_value(values: &dyn Array, index: usize, cursor_type: &ColumnType) -> Result<String, String> {
    let formatter = ArrayFormatter::try_new(values, &FormatOptions::default())
        .map_err(|error| error.to_string())?;
    cursor_text(
        &formatter.value(index).to_string(),
        cursor_type,
        chrono_tz::Tz::UTC,
    )
}

/// Load a table's rows from `reader` commit by commit, as its plan cuts them; a failure stops the table there, keeping what committed before. A table whose source has rows with a NULL cursor, which its read would skip, fails first (see [`null_cursor_rows`]).
pub(crate) async fn publish_table(
    reader: &SourceReader,
    table: &TablePlan,
    (destination, run): (&Destination, &LoadRun<'_>),
) -> Result<TableLoaded, LoaderError> {
    if let Some(rows) = null_cursor_rows(reader, table)
        .await?
        .filter(|rows| *rows > 0)
    {
        return Err(LoaderError::Invalid(null_cursor_problem(&table.spec, rows)));
    }
    let mut publisher = Publisher::open(table.clone(), (destination, run.options), None).await?;
    let transform = run.host.clone().map(TransformRun::new);
    publisher.transform = transform.clone().map(|transform| (transform, 0));
    let published = feed(reader, table, publisher, (run, transform.as_ref())).await;
    let (mut publisher, result) = published;
    publisher.close().await;
    let rows = result?;
    let recorded = publisher
        .transform
        .as_ref()
        .map_or(0, |(_, recorded)| *recorded);
    let transforms = transformed(table, (destination, transform, recorded)).await;
    Box::pin(record_numbered(table, destination)).await?;
    let report = match reader {
        SourceReader::Files(folder) => {
            let read = table
                .files
                .as_ref()
                .map(|read| unarchived(table, &read.files))
                .unwrap_or_default();
            after_load(
                folder,
                &table.spec.after_load,
                (&read, &publisher.settled_files),
            )
            .await
        }
        _ => None,
    };
    Ok(TableLoaded {
        rows,
        changes: publisher.child_changes(),
        note: marked(table, (rows, publisher.marked))
            .or_else(|| older_note(table, publisher.older)),
        after_load: report,
        replaced_keys: publisher.replaced.clone(),
        transforms,
        names: name_lines(&table.spec.names.columns),
        warnings: Vec::new(),
    })
}

/// What `table`'s key scan did, reading `keys` keys and marking `marked` rows; `None` for other loads.
fn marked(table: &TablePlan, (keys, marked): (u64, Option<u64>)) -> Option<String> {
    let WriteMode::MarkMissing(mark) = &table.mode else {
        return None;
    };
    Some(marked_note(mark.soft, (keys, marked.unwrap_or_default())))
}

/// How many keys `table`'s merge left alone, the table holding a newer row of each by `newest_by`: `(12 older rows skipped: newest_by updated_at)`; `None` when it skipped none.
fn older_note(table: &TablePlan, older: u64) -> Option<String> {
    let rows = match older {
        0 => return None,
        1 => "1 older row".to_owned(),
        _ => format!("{} older rows", thousands(older)),
    };
    Some(format!(
        "({rows} skipped: newest_by {})",
        table.spec.newest_by.join(", ")
    ))
}

/// Pass the rows to `publisher`, cutting commits where the plan says; returns the publisher for its clean-up, and the rows committed.
async fn feed(
    reader: &SourceReader,
    table: &TablePlan,
    mut publisher: Publisher,
    (run, transform): (&LoadRun<'_>, Option<&TransformRun>),
) -> (Publisher, Result<u64, LoaderError>) {
    let part = (run.project.memory.budget.as_ref()).map(|budget| {
        usize::try_from(budget.table_share / TABLE_SHARE_PARTS).unwrap_or(usize::MAX)
    });
    let batch_bytes = match (run.options.batch_bytes, part) {
        (Some(set), Some(part)) => Some(set.min(part)),
        (set, part) => set.or(part),
    };
    let fed = async {
        match &table.commits {
            CommitRule::Windows(windows) if window_concurrency(table) > 1 => {
                let limit = window_concurrency(table);
                publisher
                    .feed_windows(reader, (windows, limit, batch_bytes))
                    .await?;
            }
            CommitRule::Windows(windows) => {
                snapshot(reader, SNAPSHOT_BEGIN).await?;
                for index in 0..windows.count {
                    let window = &window_at(windows, index).map_err(LoaderError::Invalid)?;
                    let (filter, parameters) = window_filter(table, window, index == 0);
                    let part = ReadPart {
                        filter,
                        parameters,
                        batch_bytes,
                        ..ReadPart::default()
                    };
                    publisher.pass_part(reader, part).await?;
                    publisher
                        .cut(CommitRange::Window {
                            from: window.from.clone(),
                            to: window.to.clone(),
                        })
                        .await?;
                }
                snapshot(reader, COMMIT_STATEMENT).await?;
            }
            CommitRule::FileGroups(groups) => {
                let planned = table.files.as_ref().map(|files| files.files.as_slice());
                let merging = records_files(table);
                for group in groups {
                    let files: Vec<DataFile> = group
                        .files
                        .iter()
                        .filter_map(|index| planned.and_then(|files| files.get(*index)).cloned())
                        .collect();
                    let merged = merging.then(|| merged_file_text(table, &files));
                    publisher.merged_file = merged.clone();
                    if table.spec.after_load != AfterLoad::Keep {
                        publisher.fed_files.extend(unarchived(table, &files));
                    }
                    let fed_group = async {
                        let footers = match reader {
                            SourceReader::Files(folder) => {
                                publisher.widen_for(run, folder, &files).await?
                            }
                            _ => BatchFooters::default(),
                        };
                        let range = match group.values.is_empty() {
                            true => CommitRange::Planned,
                            false => CommitRange::Values(group.values.clone()),
                        };
                        let folder = match reader {
                            SourceReader::Files(folder) => Some(folder),
                            _ => None,
                        };
                        match publisher
                            .load_direct((&files, &footers, folder), &range)
                            .await?
                        {
                            Some(DirectLoaded::Committed(rows)) => {
                                publisher.note_direct(&DirectLoaded::Committed(rows));
                                return Ok(());
                            }
                            Some(staged) => {
                                publisher.note_direct(&staged);
                                publisher.cut(range).await?;
                                return Ok(());
                            }
                            None => {}
                        }
                        publisher.note_read(&files);
                        let part = ReadPart {
                            files: Some(files),
                            footers,
                            transform: transform.cloned(),
                            ..ReadPart::default()
                        };
                        let current = publisher.table.clone();
                        read_source(reader, &current, part, async |batch| {
                            stage(&mut publisher, batch).await
                        })
                        .await?;
                        publisher.cut(range).await
                    };
                    fed_group.await.map_err(|error| match &merged {
                        Some(name) => in_file(name, error),
                        None => error,
                    })?;
                }
            }
            CommitRule::Ordered {
                column,
                cursor_type,
            } => {
                let part = ReadPart {
                    order_by: Some(format!("\"{}\" NULLS FIRST", column.replace('"', "\"\""))),
                    batch_bytes,
                    ..ReadPart::default()
                };
                let mut last: Option<String> = None;
                let mut cut_from = table.window.as_ref().and_then(|window| window.from.clone());
                read_source(reader, table, part, async |batch| {
                    let values = batch.column_by_name(column).cloned().ok_or_else(|| {
                        LoaderError::Invalid(format!("the read has no cursor column {column}"))
                    })?;
                    let at = match publisher.full() {
                        true => cut_index(values.as_ref(), last.as_deref())
                            .map_err(LoaderError::Invalid)?,
                        false => batch.num_rows(),
                    };
                    last = last_value(values.as_ref())
                        .map_err(LoaderError::Invalid)?
                        .or(last.take());
                    stage(&mut publisher, batch.slice(0, at)).await?;
                    if at < batch.num_rows() {
                        let cut_at = cut_value(values.as_ref(), at, cursor_type)
                            .map_err(LoaderError::Invalid)?;
                        let range = CommitRange::Window {
                            from: cut_from.replace(cut_at.clone()),
                            to: Some(cut_at),
                        };
                        publisher.cut(range).await?;
                        stage(&mut publisher, batch.slice(at, batch.num_rows() - at)).await?;
                    }
                    Ok(())
                })
                .await?;
                if deletes_range(&table.mode) {
                    let to = table.window.as_ref().and_then(|window| window.to.clone());
                    publisher
                        .cut(CommitRange::Window { from: cut_from, to })
                        .await?;
                }
            }
            CommitRule::Size => {
                let part = ReadPart {
                    batch_bytes,
                    transform: transform.cloned(),
                    ..ReadPart::default()
                };
                match pulls_unloaded(table.route.as_ref()) {
                    true => publisher.pass_part(reader, part).await?,
                    false => {
                        read_source(reader, table, part, async |batch| {
                            stage(&mut publisher, batch).await?;
                            match publisher.full() {
                                true => publisher.cut(CommitRange::Planned).await,
                                false => Ok(()),
                            }
                        })
                        .await?;
                    }
                }
            }
            CommitRule::Whole(_) => {
                let part = ReadPart {
                    batch_bytes,
                    transform: transform.cloned(),
                    ..ReadPart::default()
                };
                publisher.pass_part(reader, part).await?;
                if deletes_range(&table.mode) {
                    publisher.cut(CommitRange::Planned).await?;
                }
            }
        }
        let rows = publisher.finish(None).await?;
        let fed_files = std::mem::take(&mut publisher.fed_files);
        publisher.settled_files.extend(fed_files);
        Ok(rows)
    }
    .await;
    (publisher, fed)
}

/// Whether `table` loads a files source's files one commit and run record per file: a merge, or `delete_insert` by `replace_key` (see [`logs_files`]).
pub(crate) fn records_files(table: &TablePlan) -> bool {
    table.files.is_some() && logs_files(&table.spec)
}

async fn stage(publisher: &mut Publisher, batch: RecordBatch) -> Result<(), LoaderError> {
    publisher.stage(batch, None).await
}

/// The source filter reading one cursor window, in the source's dialect, and the values bound to its placeholders: an ODBC source's date and timestamp bounds (`split_windows` cuts only number and time cursors, never a text one). Under `null_cursor = "include"` the `first` window reads the rows whose cursor is NULL too, once per run.
pub(crate) fn window_filter(
    table: &TablePlan,
    window: &CursorWindow,
    first: bool,
) -> (Option<String>, Vec<BoundParameter>) {
    let Some((cursor, column)) = table.spec.cursor.as_ref().and_then(|cursor| {
        table
            .read_columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&cursor.column))
            .map(|column| (cursor, column))
    }) else {
        return (None, Vec::new());
    };
    let Some(expression) = source_cursor(&table.spec, &table.read_columns, Some(&column.output))
    else {
        return (None, Vec::new());
    };
    let operator = match window.inclusive {
        true => INCLUSIVE_BOUND,
        false => EXCLUSIVE_BOUND,
    };
    let source = table.spec.warehouse_source.as_ref();
    let (filter, parameters) = source_filter(
        &expression,
        (
            Some(&column.output),
            source.and_then(|source| source_dialect(source.system)),
        ),
        (
            window.from.as_deref().map(|from| (operator, from)),
            window.to.as_deref(),
        ),
        cursor_binding(&table.spec, None),
    );
    match cursor.null_cursor == NullCursor::Include && first {
        true => (with_null_cursors(filter, &expression), parameters),
        false => (filter, parameters),
    }
}

/// Whether the table's rows wait in local files until their commit sends them: `send = "at_commit"` on a database target, where SQLLoad sends the rows itself rather than the target reading the source's files.
pub(crate) fn holds_commits(table: &TablePlan) -> bool {
    table.spec.send == SendMode::AtCommit
        && sql_adapter(table.spec.warehouse).runs_sql()
        && !reads_files_itself(table)
}

/// How `plan` words a run [published once](crate::loader::_helpers::table_plan::publishes_once): a merge also commits once [`MERGE_RUN_ROWS`] rows wait, so the keys it matches stay a bounded set.
pub(crate) fn published_once(table: &TablePlan) -> &'static str {
    match table.mode {
        WriteMode::Merge { .. } => MERGE_PUBLISHED_ONCE,
        WriteMode::DeleteInsert { .. } => RANGE_PUBLISHED_ONCE,
        _ => PUBLISHED_ONCE,
    }
}

/// Whether the target loads the table's files itself: a files source's, or a warehouse source's unload.
pub(crate) fn reads_files_itself(table: &TablePlan) -> bool {
    is_direct(table.route.as_ref()) || pulls_unloaded(table.route.as_ref())
}

/// The table's spool, in its `staging_dir` or the system's temporary folder, once that has room for about what a commit holds (see [`held_estimate`]), where the source and the file system say; commits are cut at `commit_bytes`.
fn room_to_hold(table: &TablePlan, commit_bytes: usize) -> Result<Spool, LoaderError> {
    let spec = &table.spec;
    let parent = spec.staging_dir.clone().unwrap_or_else(std::env::temp_dir);
    let estimated = estimated_bytes(spec.source_table.estimated_bytes);
    let once = matches!(table.mode, WriteMode::Replace | WriteMode::Recreate);
    let needed = held_estimate(estimated, once, commit_bytes as u64);
    if let (Some(needed), Some(free)) = (needed, free_bytes(&parent))
        && needed > free
    {
        return Err(LoaderError::Invalid(format!(
            "{}: send = \"at_commit\" holds about {} of rows in {} until each commit, and it has {} free; set staging_dir on the target to a folder with room, or send = \"per_batch\"",
            spec.destination.table,
            memory_text(needed),
            parent.display(),
            memory_text(free)
        )));
    }
    Spool::new(&parent).map_err(LoaderError::Invalid)
}

/// The plan's `send` line for the table (see [`send_line`]) and its `memory` line, where the target's peak memory doesn't follow the source's fetch budget, as labelled lines.
pub(crate) fn send_lines(table: &TablePlan) -> impl Iterator<Item = (&'static str, String)> {
    let memory = sql_adapter(table.spec.warehouse).memory_note();
    (send_line(table).map(|text| ("send", text)).into_iter())
        .chain(memory.map(|text| ("memory", text.to_owned())))
}

/// The plan's `send` line for a table under `send = "at_commit"`: about how much each commit holds locally before its burst, or why the setting doesn't apply; `None` when each batch is sent as it is read.
fn send_line(table: &TablePlan) -> Option<String> {
    let spec = &table.spec;
    if spec.send == SendMode::PerBatch || !sql_adapter(spec.warehouse).runs_sql() {
        return None;
    }
    if reads_files_itself(table) {
        return Some(format!("n/a: {} reads the files itself", spec.warehouse));
    }
    let estimated = estimated_bytes(spec.source_table.estimated_bytes);
    let once = matches!(table.mode, WriteMode::Replace | WriteMode::Recreate);
    Some(
        match (held_estimate(estimated, once, BATCH_BYTES as u64), once) {
            (Some(bytes), true) => format!(
                "at commit: about {} staged locally, then one burst",
                memory_text(bytes)
            ),
            (Some(bytes), false) => format!(
                "at commit: up to about {} staged locally per commit, then one burst each",
                memory_text(bytes)
            ),
            (None, true) => "at commit: staged locally, then one burst".to_owned(),
            (None, false) => "at commit: staged locally, then one burst per commit".to_owned(),
        },
    )
}

/// The source's estimate of a table's size in bytes, which it gives as a negative number or zero when it has none.
fn estimated_bytes(estimate: i64) -> Option<u64> {
    match u64::try_from(estimate) {
        Ok(0) | Err(_) => None,
        Ok(bytes) => Some(bytes),
    }
}
