use crate::adapters::contract::main::bound_parameter::bound_parameter;
use crate::adapters::contract::main::cursor_bound::cursor_bound;
use crate::adapters::contract::main::cursor_from_now::cursor_from_now;
use crate::adapters::contract::main::cursor_text::cursor_text;
use crate::adapters::contract::models::BoundParameter;
use crate::adapters::contract::models::CursorQuery;
use crate::adapters::contract::models::MergeDedupe;
use crate::adapters::contract::models::NowShift;
use crate::adapters::contract::models::WriteMode;
use crate::adapters::contract::models::{ChangeApply, ChangeWrite};
use crate::adapters::dispatch::main::dialect::files_destination::files_destination;
use crate::adapters::dispatch::main::dialect::source_binds_text::source_binds_text;
use crate::adapters::dispatch::main::dialect::source_dialect::source_dialect;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::route::choose_stage::choose_stage;
use crate::adapters::dispatch::main::route::direct_blocked::direct_blocked;
use crate::adapters::dispatch::main::route::route_warning::route_warning;
use crate::adapters::dispatch::main::route::stage_misses::stage_misses;
use crate::adapters::dispatch::main::route::unload_blocked::unload_blocked;
use crate::adapters::dispatch::main::route::writer_accepts::writer_accepts;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::main::session::single_text::single_text;
use crate::adapters::dispatch::main::session::write_problems::write_problems;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::files::main::output_problems::output_problems;
use crate::adapters::files::models::{OutputCheck, PartitionBy, TablePath};
use crate::adapters::snowflake::models::StageRoute;
use crate::batch_plan::main::file_groups::file_groups;
use crate::batch_plan::main::split_windows::split_windows;
use crate::batch_plan::models::{FileGroup, WindowSplit};
use crate::bigquery_source::constants::BIGQUERY_SOURCE;
use crate::cdc::constants::{CDC_PARTITION_PROBLEM, CDC_RELOAD_PROBLEM, PLAIN_JSON_TYPE};
use crate::cdc::main::metadata_read_columns::metadata_read_columns;
use crate::column_types::models::ColumnType;
use crate::column_types::models::Warehouse;
use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::files_source::models::FilesRead;
use crate::files_source::models::{Compression, SourceFormat};
use crate::load_naming::main::is_load_column::is_load_column;
use crate::loader::_helpers::python::list_choices;
use crate::loader::_helpers::python::live_child_tables;
use crate::loader::_helpers::python::with_live_children;
use crate::loader::_helpers::python::with_live_columns;
use crate::loader::_helpers::sources::column_fix;
use crate::loader::_helpers::sources::files_read_columns;
use crate::loader::_helpers::sources::plan_files;
use crate::loader::_helpers::sources::truncated_nanoseconds;
use crate::loader::_helpers::sources::{FilesPlanRequest, LoadedSource, with_live_types};
use crate::loader::constants::CHECKS_LOADER_WARNING;
use crate::loader::constants::ENFORCED_UNDECLARED_PROBLEM;
use crate::loader::constants::KEPT_TABLES_RELOAD_PROBLEM;
use crate::loader::errors::LoaderError;
use crate::loader::main::_files_schema::files_schema;
use crate::loader::main::_logged_plan::with_run_log;
use crate::loader::main::_plan_files::plan_files_table;
use crate::loader::models::CommitRule;
use crate::loader::models::CursorSpec;
use crate::loader::models::PlanOptions;
use crate::loader::models::ReadWindow;
use crate::loader::models::ReplannedColumns;
use crate::loader::models::SourceEnd;
use crate::loader::models::TablePlan;
use crate::loader::models::TableSpec;
use crate::loader::models::WholeReason;
use crate::loader::models::{WarehouseRoute, WarehouseSource};
use crate::postgres_source::main::plan_read::plan_read;
use crate::postgres_source::models::SourceTable;
use crate::postgres_source::models::{PgType, ReadColumn};
use crate::project::models::BatchStep;
use crate::project::models::Contract;
use crate::project::models::MaskMethod;
use crate::project::models::WriteStrategy;
use crate::project::models::{FilesFormat, OutputEncoding};
use crate::project::models::{OnDuplicateKeys, SortDirection};
use crate::publish_checks::main::check_column_problems::check_column_problems;
use crate::routing::main::plan_route::plan_route;
use crate::routing::models::FileFormat;
use crate::routing::models::FileSet;
use crate::routing::models::ReaderOffer;
use crate::routing::models::Route;
use crate::routing::models::RouteOverrides;
use crate::routing::models::Step;
use crate::routing::models::Store;
use crate::row_batches::models::Unnest;
use crate::schema_plan::constants::{CDC_PREFIX, CDC_UNCHANGED_COLUMN, MERGE_ROW_COLUMN};
use crate::schema_plan::main::metadata_columns::metadata_columns;
use crate::schema_plan::main::plan_schema::plan_schema;
use crate::schema_plan::models::Blocked;
use crate::schema_plan::models::ColumnChange;
use crate::schema_plan::models::ColumnOrigin;
use crate::schema_plan::models::DesiredColumn;
use crate::schema_plan::models::LiveColumn;
use crate::schema_plan::models::SchemaPlan;
use crate::schema_plan::models::SchemaRules;
use crate::schema_plan::models::TableAction;
use crate::snowflake_source::main::read_columns::read_columns as unload_read_columns;
use crate::store_paths::models::StoreKind;

/// The source table without its excluded columns.
pub(crate) fn selected_columns(spec: &TableSpec) -> SourceTable {
    let mut table = spec.source_table.clone();
    table.columns.retain(|column| {
        !spec
            .excluded_columns
            .iter()
            .any(|(excluded, _)| *excluded == column.name)
    });
    table
}

/// The columns the table should have: the read columns (pinned, typed by the source, or inferred from sampled values for Python tables and JSON, NDJSON and CSV files, whose partition columns are declared; a files source reads its constant columns itself), then the constant columns the source does not read, then the metadata columns.
pub(crate) fn desired_columns(spec: &TableSpec, read: &[ReadColumn]) -> Vec<DesiredColumn> {
    let sampled = spec.python.is_some()
        || spec
            .files
            .as_ref()
            .is_some_and(|files| files.encoding.format != SourceFormat::Parquet);
    let partition = |name: &str| {
        spec.files.as_ref().is_some_and(|files| {
            files
                .partition_types
                .iter()
                .any(|(column, _)| column == spec.source_table.spelling(name))
        })
    };
    let mut desired: Vec<DesiredColumn> = read
        .iter()
        .map(|column| DesiredColumn {
            name: column.name.clone(),
            column_type: column.output.clone(),
            origin: match (
                column.name == SOURCE_ROW_COLUMN,
                spec.pinned.contains_key(&column.name),
            ) {
                (true, _) => ColumnOrigin::Metadata,
                (false, true) => ColumnOrigin::Pinned,
                (false, false) if sampled && !partition(&column.name) => ColumnOrigin::Inferred,
                (false, false) => ColumnOrigin::Source,
            },
        })
        .collect();
    let choices = list_choices(spec);
    desired.retain(|column| !spec.unnest.takes(&column.name, &choices));
    desired.extend(
        added_constants(spec, read).map(|(name, column_type, _)| DesiredColumn {
            name: name.clone(),
            column_type: column_type.clone(),
            origin: ColumnOrigin::Constant,
        }),
    );
    let merged = spec
        .cdc
        .as_ref()
        .is_some_and(|cdc| cdc.apply != ChangeApply::Changelog);
    desired.retain(|column| !(merged && column.name == CDC_UNCHANGED_COLUMN));
    for column in desired
        .iter_mut()
        .filter(|column| is_load_column(spec.naming, &column.name))
    {
        column.origin = ColumnOrigin::Metadata;
    }
    desired.extend(metadata_columns(spec.naming, spec.row_id));
    desired
}

/// The table's `constant_columns` the source does not read, which the load adds to every batch: all of them, except a files source's, whose reader fills them.
pub(crate) fn added_constants<'a>(
    spec: &'a TableSpec,
    read: &'a [ReadColumn],
) -> impl Iterator<Item = &'a (String, ColumnType, String)> {
    spec.constants
        .iter()
        .filter(|(name, _, _)| !is_read(read, name))
}

/// Whether the source reads a column `name`.
fn is_read(read: &[ReadColumn], name: &str) -> bool {
    read.iter().any(|column| column.name == name)
}

/// A schema plan that stops the table for `reasons`.
pub(crate) fn blocked_plan(reasons: Vec<String>) -> SchemaPlan {
    SchemaPlan {
        action: TableAction::Unchanged,
        write_columns: Vec::new(),
        warnings: Vec::new(),
        blocked: Some(Blocked {
            reasons,
            fix_flag: None,
        }),
    }
}

/// A table that cannot load, for `reasons`.
pub(crate) fn blocked_table(spec: &TableSpec, reasons: Vec<String>) -> TablePlan {
    TablePlan {
        spec: spec.clone(),
        read_columns: Vec::new(),
        live: None,
        schema: blocked_plan(reasons),
        mode: WriteMode::Append,
        source_filter: None,
        source_parameters: Vec::new(),
        cursor_note: None,
        window: None,
        route: None,
        files: None,
        output: None,
        exists: false,
        logged: None,
        commits: CommitRule::Whole(WholeReason::Blocked),
        stage: None,
        planned_stage: None,
    }
}

/// One table's plan against its live destination table; a Python table's live columns count as its source's, since it has no catalog.
pub(crate) async fn plan_table(
    spec: &TableSpec,
    destination: &Destination,
    options: &PlanOptions,
) -> Result<TablePlan, LoaderError> {
    let undeclared = spec
        .python
        .as_ref()
        .is_some_and(|python| python.declared.is_empty());
    if undeclared && spec.contract == Contract::Enforced {
        return Ok(blocked_table(
            spec,
            vec![ENFORCED_UNDECLARED_PROBLEM.to_owned()],
        ));
    }
    let children = match spec.unnest {
        Unnest::All => live_child_tables(destination, spec).await?,
        _ => Vec::new(),
    };
    let spec = &with_live_children(spec, &children);
    let plan = plan_table_once(spec, destination, options).await?;
    let augmented = with_live_columns(spec, plan.live.as_deref())
        .or_else(|| with_live_types(spec, plan.live.as_deref()));
    let mut plan = match augmented {
        Some(augmented) => plan_table_once(&augmented, destination, options).await?,
        None => plan,
    };
    plan.schema.warnings.extend(ignored_batch(&plan));
    plan.schema.warnings.extend(split_ranking(&plan));
    with_run_log(plan, destination, options).await
}

/// Why the plan cannot write `spec` as `schema` plans it, when nothing blocked it first: its `cursor_column`, when `unwritten_cursor` names it, or its `dedup_sort` column is not among the columns written. A Python table's columns are known only once it yields, so its `dedup_sort` is not checked here.
fn unwritten_column(
    spec: &TableSpec,
    schema: &SchemaPlan,
    unwritten_cursor: Option<&str>,
) -> Option<String> {
    if schema.blocked.is_some() {
        return None;
    }
    if let Some(name) = unwritten_cursor {
        return Some(format!(
            "cursor_column {name} is not written to the table (excluded, not in the enforced contract, or ignored by on_schema_change)"
        ));
    }
    let (name, _) = spec.dedup_sort.as_ref().filter(|_| spec.python.is_none())?;
    let column = schema
        .write_columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name));
    match column.map(|column| &column.column_type) {
        None => Some(format!(
            "dedup_sort column {name} is not written to the table (not a column of the source, excluded, not in the enforced contract, or ignored by on_schema_change)"
        )),
        Some(column_type @ (ColumnType::Json | ColumnType::Array(_))) => Some(format!(
            "dedup_sort column {name} is {column_type}, which has no order to rank rows by; name a column of another type"
        )),
        Some(_) => None,
    }
}

/// `mode` and `schema` once `unordered_merge` checked how a files merge orders its rows: its warning added, or the plan blocked by its problem.
fn ordered_mode(mode: WriteMode, mut schema: SchemaPlan) -> (WriteMode, SchemaPlan) {
    match unordered_merge(mode, &schema) {
        Ok((mode, warning)) => {
            schema.warnings.extend(warning);
            (mode, schema)
        }
        Err(problem) => (WriteMode::Append, blocked_plan(vec![problem])),
    }
}

/// `mode` for a files merge whose rows are ranked by `_sqlload_source_row` when that column is not written (excluded, or left out by the contract), so no row is known to be last: without `dedup_sort`, repeated keys fail as before 0.39.0, with a warning saying why; with it, the problem, as ties need that order.
fn unordered_merge(
    mode: WriteMode,
    schema: &SchemaPlan,
) -> Result<(WriteMode, Option<String>), String> {
    let WriteMode::Merge {
        unique_key,
        dedupe: Some(dedupe),
    } = &mode
    else {
        return Ok((mode, None));
    };
    if schema.blocked.is_some() {
        return Ok((mode, None));
    }
    let ordered = dedupe.ordinal != SOURCE_ROW_COLUMN
        || schema
            .write_columns
            .iter()
            .any(|column| column.name == SOURCE_ROW_COLUMN);
    match (ordered, &dedupe.sort) {
        (true, _) => Ok((mode, None)),
        (false, Some(_)) => Err(format!(
            "dedup_sort breaks ties by row order, which a files merge takes from {SOURCE_ROW_COLUMN}; stop excluding it"
        )),
        (false, None) => Ok((
            WriteMode::Merge {
                unique_key: unique_key.clone(),
                dedupe: None,
            },
            Some(format!(
                "{SOURCE_ROW_COLUMN} is not written, so a file's rows have no order to pick the last of a repeated key by: a file repeating its unique_key fails; write {SOURCE_ROW_COLUMN} to keep its last row instead"
            )),
        )),
    }
}

/// What `dedup_sort` cannot rank when a merge may commit more than once: each commit ranks only its own rows, so a later commit overwrites a key an earlier one wrote whatever its sort value.
fn split_ranking(plan: &TablePlan) -> Option<String> {
    let WriteMode::Merge {
        dedupe: Some(MergeDedupe {
            sort: Some((column, _)),
            ..
        }),
        ..
    } = &plan.mode
    else {
        return None;
    };
    let (commits, remedy) = match &plan.commits {
        CommitRule::Windows(windows) if windows.count > 1 => (
            format!("once per window ({} windows)", windows.count),
            "; set batch_size = \"none\" to load it in one commit",
        ),
        CommitRule::FileGroups(groups) if groups.len() > 1 => {
            (format!("once per file ({} files)", groups.len()), "")
        }
        CommitRule::Ordered { .. } | CommitRule::Size => (
            "about every 1 GB".to_owned(),
            "; set batch_size = \"none\" to load it in one commit",
        ),
        _ => return None,
    };
    Some(format!(
        "dedup_sort ranks repeated keys within one commit, and this table commits {commits}: a later commit still overwrites a key an earlier one wrote, whatever its {column} value{remedy}"
    ))
}

/// Why the table's `batch_size` setting has no effect, if it has none: a files source commits by whole partitions, `replace` and `--reload` swap the whole table in once, and windows need a cursor.
fn ignored_batch(plan: &TablePlan) -> Option<String> {
    let spec = &plan.spec;
    let step = spec.batch?;
    let reason = match (&spec.files, &plan.mode, &spec.cursor, step) {
        (Some(_), _, _, _) => "a files source commits in batches of whole partitions",
        (None, WriteMode::Replace, _, _) => "replace swaps the whole table in once",
        (None, WriteMode::Recreate, _, _) => "--reload rebuilds the whole table once",
        (None, _, None, BatchStep::Duration(_) | BatchStep::Integer(_)) => {
            "windows need a cursor_column"
        }
        _ => return None,
    };
    Some(format!("batch_size is ignored: {reason}"))
}

/// A table's columns and schema plan once more during its load, when its source shows new columns; nothing else about its plan is read or checked again. `known` is the live columns when they are known to be current (a Python table's first batch, just after plan); otherwise one read of them, except that a files target has no SQL to read them with and keeps `stale`, what plan saw.
pub(crate) async fn replan_columns(
    spec: &TableSpec,
    (destination, stale): (&Destination, Option<&[LiveColumn]>),
    known: Option<Option<&[LiveColumn]>>,
    options: &PlanOptions,
) -> Result<ReplannedColumns, LoaderError> {
    let live = match (known, destination) {
        (Some(known), _) => known.map(<[LiveColumn]>::to_vec),
        (None, destination) if files_destination(destination).is_some() => {
            stale.map(<[LiveColumn]>::to_vec)
        }
        (None, _) => live_columns(destination, &spec.destination).await?,
    };
    let spec = with_live_columns(spec, live.as_deref())
        .or_else(|| with_live_types(spec, live.as_deref()))
        .unwrap_or_else(|| spec.clone());
    let read = read_columns(&spec).map_err(|problems| LoaderError::Invalid(problems.join("; ")))?;
    let desired = desired_columns(&spec, &read);
    let schema = match files_destination(destination) {
        Some(_) => files_schema(&spec, &desired, live.as_deref(), options),
        None => {
            let rules = SchemaRules {
                warehouse: spec.warehouse,
                contract: spec.contract,
                on_schema_change: spec.on_schema_change,
                auto_widen: spec.auto_widen,
                type_change_rebuild: spec.type_change_rebuild,
                rebuild_allowed: options.rebuild_allowed,
                naming: spec.naming,
            };
            let compared = live.as_deref().filter(|_| !options.reload);
            let mut schema = plan_schema(&desired, compared, rules);
            if options.reload && live.is_some() {
                schema.action = TableAction::Recreate;
            }
            schema
        }
    };
    if let Some(blocked) = &schema.blocked {
        let what = match spec.python {
            Some(_) => "the yielded data does",
            None => "the files' new columns do",
        };
        return Err(LoaderError::Invalid(format!(
            "{what} not fit the table: {}",
            blocked.reasons.join("; ")
        )));
    }
    Ok(ReplannedColumns {
        spec,
        read_columns: read,
        live,
        schema,
    })
}

/// How a table's columns are read: by name (files and Python tables, whose batches carry the names) or by Postgres expression.
pub(crate) fn read_columns(spec: &TableSpec) -> Result<Vec<ReadColumn>, Vec<String>> {
    match (&spec.files, &spec.python) {
        (Some(_), _) | (_, Some(_)) => files_read_columns(spec),
        (None, None)
            if spec
                .warehouse_source
                .as_ref()
                .is_some_and(|source| source.system == BIGQUERY_SOURCE) =>
        {
            crate::bigquery_source::main::read_columns::read_columns(
                &selected_columns(spec),
                &spec.pinned,
            )
        }
        #[cfg(feature = "odbc")]
        (None, None)
            if spec
                .warehouse_source
                .as_ref()
                .is_some_and(|source| source.route == WarehouseRoute::Query) =>
        {
            crate::odbc_source::main::read_columns::read_columns(
                &selected_columns(spec),
                &spec.pinned,
            )
        }
        (None, None) if spec.warehouse_source.is_some() => {
            unload_read_columns(&selected_columns(spec), &spec.pinned)
        }
        (None, None) if spec.sheet.is_some() => files_read_columns(spec),
        (None, None) => {
            let selected = selected_columns(spec);
            let mut read = plan_read(&selected, &spec.pinned)?;
            if let Some(cdc) = &spec.cdc {
                let problems = streamed_array_problems(&selected, &read);
                if !problems.is_empty() {
                    return Err(problems);
                }
                read.extend(metadata_read_columns(cdc.apply, 0));
            }
            Ok(read)
        }
    }
}

/// A captured table's read columns: the data columns it writes (its snapshot stages them, its stream is matched to them by name and its snapshot record lists them), the list fields its child tables take, and every change column.
pub(crate) fn captured_read(
    spec: &TableSpec,
    read: Vec<ReadColumn>,
    schema: &SchemaPlan,
) -> Vec<ReadColumn> {
    let written: Vec<&str> = schema
        .write_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let choices = list_choices(spec);
    read.into_iter()
        .filter(|column| {
            column.name.starts_with(CDC_PREFIX)
                || written.contains(&column.name.as_str())
                || spec.unnest.takes(&column.name, &choices)
        })
        .collect()
}

/// A captured table's write: its changes, applied by its source's `cdc_mode`; any other table keeps `mode`.
pub(crate) fn captured_mode(spec: &TableSpec, mode: WriteMode) -> WriteMode {
    match &spec.cdc {
        Some(cdc) => WriteMode::Changes(ChangeWrite {
            apply: cdc.apply,
            unique_key: spec.unique_key.clone(),
            truncated: None,
        }),
        None => mode,
    }
}

/// Why `append` cannot follow this cursor: a date's last day may still gain rows, and reading after it would lose them while reading from it would append that day again; files sources are exempt, since they skip files already loaded rather than reading after the cursor.
pub(crate) fn append_problem(spec: &TableSpec, cursor_type: Option<&ColumnType>) -> Option<String> {
    let cursor = spec.cursor.as_ref()?;
    (spec.strategy == WriteStrategy::Append
        && cursor_type == Some(&ColumnType::Date)
        && spec.files.is_none())
    .then(|| {
        format!(
            "write_strategy = \"append\" cannot follow the date cursor {}: rows added later to its last day would be lost (or, with append_cursor_inclusive = true, the whole day appended again on every run); use delete_insert, which replaces the last day (no key needed), or merge",
            cursor.column
        )
    })
}

/// A warning for `append_cursor_inclusive = true` on a cursor read: every run appends the rows at the cursor's maximum again.
pub(crate) fn inclusive_append_warning(spec: &TableSpec) -> Option<String> {
    let cursor = spec.cursor.as_ref()?;
    (spec.strategy == WriteStrategy::Append
        && cursor.inclusive
        && spec.files.is_none())
    .then(|| {
        format!(
            "append_cursor_inclusive = true reads from the maximum {} itself, so every run appends the rows at that value again; leave it unset to read after it, or use delete_insert or merge to pick up late rows without duplicates",
            cursor.column
        )
    })
}

/// What `--resnapshot` does to a captured table's source, for `plan`.
pub(crate) fn resnapshot_warning(spec: &TableSpec, options: &PlanOptions) -> Option<String> {
    spec.cdc.as_ref().filter(|_| options.resnapshot).map(|cdc| {
        format!(
            "--resnapshot drops and recreates slot {}, then copies every table of {} again",
            cdc.slot, spec.source_name
        )
    })
}

/// Array and `json` columns of a captured table read as JSON or as `array(T)`: the stream sends them as Postgres text (`{a,b}` for an array, the original spacing and duplicate keys for `json`), which would not load as JSON or would differ from the snapshot's jsonb rows, so they must be pinned to text (`jsonb` streams as the snapshot reads it).
fn streamed_array_problems(table: &SourceTable, read: &[ReadColumn]) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    for column in &table.columns {
        let as_json = read.iter().any(|read| {
            read.name == column.name
                && matches!(read.output, ColumnType::Json | ColumnType::Array(_))
        });
        let streamed_as_text = column.pg_type == PgType::Array
            || (column.pg_type == PgType::Json && column.postgres_type == PLAIN_JSON_TYPE);
        if streamed_as_text && as_json {
            problems.push(format!(
                "{}: kind = \"postgres_cdc\" streams {} values as the source wrote them, which the snapshot normalises as jsonb; pin it with columns = {{ {} = \"text\" }}, or exclude it",
                column.name, column.postgres_type, column.name
            ));
        }
    }
    problems
}

/// The read window for a load that filters its own rows, from the window chosen.
pub(crate) fn read_window(spec: &TableSpec, window: &Window) -> Option<ReadWindow> {
    spec.cursor.as_ref().map(|cursor| ReadWindow {
        column: cursor.column.clone(),
        from: window.bounds.0.clone(),
        to: window.bounds.1.clone(),
        inclusive: window.inclusive,
        maximum: window.maximum.clone(),
    })
}

/// Why `--reload` cannot run for `spec`: a CDC table's, or one on a warehouse that keeps tables, which never drops and recreates one.
fn reload_refusal(spec: &TableSpec, options: &PlanOptions) -> Option<&'static str> {
    match (spec.cdc.is_some(), options.reload) {
        (true, true) => Some(CDC_RELOAD_PROBLEM),
        (false, true) if sql_adapter(spec.warehouse).keeps_tables() => {
            Some(KEPT_TABLES_RELOAD_PROBLEM)
        }
        _ => None,
    }
}

/// `schema` blocked when the table's checks name columns it does not load; a Python table's columns are checked once its rows show them.
fn checked_columns(spec: &TableSpec, schema: SchemaPlan) -> SchemaPlan {
    let loaded: Vec<String> = schema
        .write_columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let problems = match (&spec.checks, &spec.python, &schema.blocked) {
        (Some(checks), None, None) => check_column_problems(checks, &loaded),
        _ => Vec::new(),
    };
    match problems.is_empty() {
        true => schema,
        false => blocked_plan(problems),
    }
}

/// `schema` with what stops or endangers writing `mode` into the existing table: blocked by its problems, with its warnings added.
async fn checked_writes(
    destination: &Destination,
    spec: &TableSpec,
    mode: &WriteMode,
    mut schema: SchemaPlan,
) -> Result<SchemaPlan, LoaderError> {
    let (problems, warnings) = write_problems(destination, &spec.destination, mode).await?;
    schema.warnings.extend(warnings);
    Ok(match problems.is_empty() {
        true => schema,
        false => blocked_plan(problems),
    })
}

async fn plan_table_once(
    spec: &TableSpec,
    destination: &Destination,
    options: &PlanOptions,
) -> Result<TablePlan, LoaderError> {
    if let Some(files) = files_destination(destination) {
        return plan_files_table(spec, files, options).await;
    }
    let live = live_columns(destination, &spec.destination).await?;
    if let Some(refusal) = reload_refusal(spec, options) {
        return Ok(TablePlan {
            exists: live.is_some(),
            live,
            ..blocked_table(spec, vec![refusal.to_owned()])
        });
    }
    let read_columns = match read_columns(spec) {
        Ok(read) => read,
        Err(problems) => {
            return Ok(TablePlan {
                spec: spec.clone(),
                read_columns: Vec::new(),
                exists: live.is_some(),
                live,
                schema: blocked_plan(problems),
                mode: WriteMode::Append,
                source_filter: None,
                source_parameters: Vec::new(),
                cursor_note: None,
                window: None,
                route: None,
                files: None,
                output: None,
                logged: None,
                commits: CommitRule::Whole(WholeReason::Blocked),
                stage: None,
                planned_stage: None,
            });
        }
    };
    let desired = desired_columns(spec, &read_columns);
    let rules = SchemaRules {
        warehouse: spec.warehouse,
        contract: spec.contract,
        on_schema_change: spec.on_schema_change,
        auto_widen: spec.auto_widen,
        type_change_rebuild: spec.type_change_rebuild,
        rebuild_allowed: options.rebuild_allowed,
        naming: spec.naming,
    };
    let compared = live.as_deref().filter(|_| !options.reload);
    let mut schema = plan_schema(&desired, compared, rules);
    if options.reload && live.is_some() {
        schema.action = TableAction::Recreate;
    }
    let read_columns = match spec.cdc {
        Some(_) => captured_read(spec, read_columns, &schema),
        None => read_columns,
    };
    let cursor_type = spec.cursor.as_ref().and_then(|cursor| {
        desired
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&cursor.column))
            .map(|column| &column.column_type)
    });
    let unseen_python_cursor =
        spec.python.is_some() && spec.cursor.is_some() && cursor_type.is_none();
    let cursor_type = match unseen_python_cursor {
        true => Some(&ColumnType::Text),
        false => cursor_type,
    };
    let cursor_name = spec.cursor.as_ref().map(|cursor| cursor.column.as_str());
    let written = unseen_python_cursor
        || cursor_name.is_some_and(|name| {
            schema
                .write_columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(name))
        });
    if let Some(problem) = unwritten_column(spec, &schema, cursor_name.filter(|_| !written)) {
        schema = blocked_plan(vec![problem]);
    }
    let cursor_in_table = live.as_deref().is_some_and(|live| {
        live.iter()
            .any(|column| cursor_name.is_some_and(|name| column.name.eq_ignore_ascii_case(name)))
    });
    let cursor_expression = source_cursor(spec, &read_columns, cursor_type);
    let lacks_cursor =
        live.is_some() && cursor_name.is_some() && !cursor_in_table && !options.reload;
    let restart_duplicates = match spec.strategy {
        WriteStrategy::Append => true,
        WriteStrategy::DeleteInsert => options.end_cursor.is_some(),
        WriteStrategy::Merge | WriteStrategy::Replace => false,
    };
    if let Some(name) =
        cursor_name.filter(|_| lacks_cursor && restart_duplicates && schema.blocked.is_none())
    {
        schema = blocked_plan(vec![format!(
            "the table has no {name} column yet, so this load would read every row again and duplicate the rows already there; {}",
            column_fix(spec.warehouse)
        )]);
    }
    if let Some(problem) = append_problem(spec, cursor_type).filter(|_| schema.blocked.is_none()) {
        schema = blocked_plan(vec![problem]);
    }
    schema.warnings.extend(inclusive_append_warning(spec));
    let request = WindowRequest {
        spec,
        destination,
        table_exists: cursor_in_table,
        cursor_type,
        cursor_expression: cursor_expression.as_deref(),
        options,
    };
    let window = match schema.blocked {
        Some(_) => Ok(None),
        None => cursor_window(&request).await?.map(Some),
    };
    let read = match &window {
        Ok(Some(window)) => read_window(spec, window),
        _ => None,
    };
    let ((source_filter, source_parameters), bounds, mode, cursor_note) = match window {
        Ok(Some(window)) => (
            (window.filter, window.parameters),
            window.bounds,
            window.mode,
            window.note,
        ),
        Ok(None) => ((None, Vec::new()), (None, None), WriteMode::Append, None),
        Err(problem) => {
            schema = blocked_plan(vec![problem]);
            ((None, Vec::new()), (None, None), WriteMode::Append, None)
        }
    };
    let written: Vec<String> = schema
        .write_columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let files = match (&spec.files, &schema.blocked) {
        (Some(files), None) => {
            let request = FilesPlanRequest {
                spec,
                files,
                loaded: LoadedSource::Query(destination),
                live: live.as_deref(),
                read_columns: &read_columns,
                written: &written,
                bounds: (bounds.0.as_deref(), bounds.1.as_deref()),
                options,
            };
            match plan_files(&request).await? {
                Ok(read) => Some(read),
                Err(problem) => {
                    schema = blocked_plan(vec![problem]);
                    None
                }
            }
        }
        _ => None,
    };
    let source_filter = source_filter.filter(|_| spec.files.is_none());
    schema.warnings.extend(resnapshot_warning(spec, options));
    let (mode, mut schema) = ordered_mode(captured_mode(spec, mode), schema);
    let exists = live.is_some();
    schema = checked_columns(spec, schema);
    if schema.blocked.is_none() && exists {
        schema = checked_writes(destination, spec, &mode, schema).await?;
    }
    let chosen = match &schema.blocked {
        None => {
            let direct_append = mode == WriteMode::Append && !spec.logged;
            match choose_stage(destination, (&spec.destination, exists, direct_append)).await {
                Some(Ok(chosen)) => Some(chosen),
                Some(Err(problem)) => {
                    schema = blocked_plan(vec![problem]);
                    None
                }
                None => None,
            }
        }
        Some(_) => None,
    };
    let (stage, planned_stage) = chosen.unzip();
    let planned_stage = planned_stage.flatten();
    schema
        .warnings
        .extend(route_warning(stage.as_ref()).map(str::to_owned));
    schema
        .warnings
        .extend(hmac_warnings(spec, stage.as_ref(), destination));
    schema
        .warnings
        .extend(truncated_nanoseconds(spec, live.as_deref()));
    let route = match schema.blocked {
        Some(_) => None,
        None => match table_route(spec, files.as_ref(), stage.as_ref(), destination) {
            Ok(route) => Some(route),
            Err(problem) => {
                schema = blocked_plan(vec![problem]);
                None
            }
        },
    };
    schema.warnings.extend(copy_warning(route.as_ref()));
    let commits = commit_rule(&CommitRequest {
        spec,
        window: read.as_ref(),
        cursor_type,
        read_columns: &read_columns,
        files: files.as_ref(),
        mode: &mode,
    });
    let commits = match commits {
        Ok(commits) => commits,
        Err(problem) => {
            if schema.blocked.is_none() {
                schema = blocked_plan(vec![problem]);
            }
            CommitRule::Whole(WholeReason::Blocked)
        }
    };
    Ok(TablePlan {
        spec: spec.clone(),
        read_columns,
        live,
        schema,
        mode,
        source_filter,
        source_parameters,
        cursor_note,
        window: read,
        route,
        files,
        output: None,
        exists,
        logged: None,
        commits,
        stage,
        planned_stage,
    })
}

/// The statements that bring the table's schema in line with its plan, a created or rebuilt table getting its description as its comment; `None` when there is nothing to run: unchanged, rebuilt by the load itself, or a files target.
pub(crate) fn schema_statements(table: &TablePlan) -> Option<Vec<String>> {
    let spec = &table.spec;
    let warehouse = spec.warehouse;
    if table.output.is_some() {
        return None;
    }
    match &table.schema.action {
        TableAction::Unchanged | TableAction::Recreate => None,
        TableAction::Create => {
            let adapter = sql_adapter(warehouse);
            let mut statements = vec![
                adapter.render_create_schema(&spec.destination),
                adapter.render_create_loaded_table(spec, &table.schema.write_columns),
            ];
            if let WriteMode::Merge { unique_key, .. } = &table.mode {
                statements.extend(adapter.render_key_index(&spec.destination, unique_key));
            }
            statements.extend(adapter.render_table_comment(&spec.destination, &spec.description));
            Some(statements)
        }
        TableAction::Alter(changes) => {
            let live = table.live.as_deref().unwrap_or_default();
            let adapter = sql_adapter(warehouse);
            let mut statements = adapter.render_change_columns(&spec.destination, changes, live);
            if changes
                .iter()
                .any(|change| matches!(change, ColumnChange::Rebuild { .. }))
            {
                statements
                    .extend(adapter.render_table_comment(&spec.destination, &spec.description));
            }
            Some(statements)
        }
    }
}

/// What a table's source offers: rows as Arrow, plus the files themselves for a files source, with why the destination must not read them itself.
fn reader_offer(
    spec: &TableSpec,
    read: Option<&FilesRead>,
    stage: Option<&StageRoute>,
) -> ReaderOffer {
    let existing_files = spec.files.as_ref().map(|files| FileSet {
        store: match files.location.store {
            StoreKind::S3 { .. } | StoreKind::Sftp { .. } => {
                Store::ObjectStore(files.location.to_string())
            }
            StoreKind::Filesystem { .. } => Store::Local(files.location.to_string()),
        },
        format: match files.encoding.format {
            SourceFormat::Parquet => FileFormat::Parquet,
            SourceFormat::Ndjson | SourceFormat::Json => FileFormat::Json,
            SourceFormat::Csv => FileFormat::Csv,
            SourceFormat::Xlsx => FileFormat::Xlsx,
            SourceFormat::Xml => FileFormat::Xml,
        },
    });
    let direct_blocked = read.and_then(|read| direct_blocked(spec, read, stage));
    let hmac: Vec<&str> = spec
        .masked
        .columns
        .iter()
        .filter(|(_, method)| **method == MaskMethod::Hmac)
        .map(|(name, _)| name.as_str())
        .collect();
    let nesting = match (spec.unnest == Unnest::None, spec.row_id) {
        (false, _) => Some("the table unnests lists into child tables, which SQLLoad writes"),
        (true, true) => Some("the table gives each row an id, which SQLLoad makes"),
        (true, false) => None,
    };
    let needs_loader = match (hmac.is_empty(), spec.test_loader_route, &spec.checks) {
        (false, _, _) => Some(format!(
            "{} masked with hmac, whose key must never appear in SQL",
            hmac.join(", ")
        )),
        (true, true, _) => Some("the table sets test_loader_route (tests only)".to_owned()),
        (true, false, Some(_)) => {
            Some("the table has checks, which run on the rows SQLLoad stages".to_owned())
        }
        (true, false, None) if nesting.is_some() => nesting.map(str::to_owned),
        (true, false, None) => spec.files.as_ref().and_then(|files| {
            match (
                &files.location.store,
                files.encoding.format,
                files.encoding.compression,
            ) {
                (StoreKind::Sftp { .. }, _, _) => {
                    Some("no destination reads files over SFTP itself".to_owned())
                }
                (_, _, Compression::Zip) => {
                    Some("no destination reads ZIP archives' members itself".to_owned())
                }
                (_, format @ (SourceFormat::Xlsx | SourceFormat::Xml), _) => {
                    Some(format!("no destination reads {format} files itself"))
                }
                _ => None,
            }
        }),
    };
    let unload_to: Vec<Store> = match &spec.warehouse_source {
        Some(WarehouseSource {
            route: WarehouseRoute::Unload { store, .. },
            ..
        }) => vec![store.clone()],
        Some(WarehouseSource {
            route: WarehouseRoute::Query,
            ..
        })
        | None => Vec::new(),
    };
    let direct_blocked = match unload_to.is_empty() {
        true => direct_blocked,
        false => unload_blocked(spec),
    };
    ReaderOffer {
        arrow: unload_to.is_empty(),
        existing_files,
        unload_to,
        estimated_bytes: match u64::try_from(spec.source_table.estimated_bytes) {
            Ok(bytes) => Some(bytes),
            Err(_) => None,
        },
        direct_blocked,
        needs_loader,
        copy_to_internal_stage: spec
            .files
            .as_ref()
            .is_some_and(|files| files.copy_to_internal_stage),
        out_of_reach: spec
            .files
            .as_ref()
            .is_some_and(|files| stage_misses(stage, &files.location)),
    }
}

/// Whether `table`, a `delete_insert` read in windows or cursor-ordered batches, stages them all and publishes once at the end (see [`publishes_ranges_once`](crate::adapters::contract::types::SqlAdapter::publishes_ranges_once)); a files source's groups still commit one by one, as their values would not fit one statement.
pub(crate) fn publishes_once(table: &TablePlan) -> bool {
    matches!(table.mode, WriteMode::DeleteInsert { .. })
        && matches!(
            table.commits,
            CommitRule::Windows(_) | CommitRule::Ordered { .. }
        )
        && sql_adapter(table.spec.warehouse).publishes_ranges_once()
}

/// One commit per file, in the order planned: a files source's merge applies each file on its own, so a key repeated across files ends with the newest file's value.
fn one_file_each(count: usize) -> Vec<FileGroup> {
    (0..count)
        .map(|index| FileGroup {
            files: vec![index],
            values: Vec::new(),
        })
        .collect()
}

/// Whether `route` has the destination load a warehouse source's unloaded files itself.
pub(crate) fn pulls_unloaded(route: Option<&Route>) -> bool {
    route.is_some_and(|route| {
        matches!(
            route.steps.as_slice(),
            [Step::Unload(_), Step::DestinationPulls(_)]
                | [
                    Step::Unload(_),
                    Step::CopyFiles(Store::Local(_)),
                    Step::DestinationPulls(_)
                ]
        )
    })
}

/// Whether `route` has the destination load a files source's own files.
pub(crate) fn is_direct(route: Option<&Route>) -> bool {
    route.is_some_and(|route| {
        matches!(
            route.steps.as_slice(),
            [Step::UseFiles(_), Step::DestinationPulls(_)]
                | [
                    Step::UseFiles(_),
                    Step::CopyFiles(_),
                    Step::DestinationPulls(_)
                ]
        )
    })
}

/// A warning for each `hmac`-masked column of a files source whose files `destination` would otherwise read itself: every file then goes through SQLLoad.
fn hmac_warnings(
    spec: &TableSpec,
    stage: Option<&StageRoute>,
    destination: &Destination,
) -> Vec<String> {
    let reads_itself = writer_accepts(destination, stage)
        .reads_files_itself
        .is_some();
    let checked = reads_itself && spec.files.is_some() && spec.checks.is_some();
    spec.masked
        .columns
        .iter()
        .filter(|(_, method)| reads_itself && spec.files.is_some() && **method == MaskMethod::Hmac)
        .map(|(name, _)| {
            format!(
                "{name}: hmac masking pulls every file through the loader (the key must never be in SQL), which is much slower than a direct load. mask = \"hash\" stays on the direct route."
            )
        })
        .chain(checked.then(|| CHECKS_LOADER_WARNING.to_owned()))
        .collect()
}

/// The warning for files `copy_to_internal_stage` downloads and uploads, which a storage integration would spare.
fn copy_warning(route: Option<&Route>) -> Option<String> {
    let [
        Step::UseFiles(files),
        Step::CopyFiles(Store::InternalStage(stage)),
        Step::DestinationPulls(_),
    ] = route?.steps.as_slice()
    else {
        return None;
    };
    Some(format!(
        "copy_to_internal_stage: every file is downloaded and uploaded to {stage} (much slower than a direct load).\nA storage integration for {} avoids this.",
        files.store
    ))
}

/// The route a table's data takes into `destination`; `read` is a files table's planned read.
pub(crate) fn table_route(
    spec: &TableSpec,
    read: Option<&FilesRead>,
    stage: Option<&StageRoute>,
    destination: &Destination,
) -> Result<Route, String> {
    let reader = reader_offer(spec, read, stage);
    plan_route(
        &reader,
        &writer_accepts(destination, stage),
        RouteOverrides::default(),
    )
    .map_err(|error| error.to_string())
}

/// What decides one table's read window.
pub(crate) struct WindowRequest<'a> {
    pub(crate) spec: &'a TableSpec,
    pub(crate) destination: &'a Destination,
    pub(crate) table_exists: bool,
    pub(crate) cursor_type: Option<&'a ColumnType>,
    /// The cursor's select expression in the source, which filters compare against.
    pub(crate) cursor_expression: Option<&'a str>,
    pub(crate) options: &'a PlanOptions,
}

/// The source filter, the write mode and a note for `plan`.
pub(crate) struct Window {
    pub(crate) filter: Option<String>,
    /// The values bound to `filter`'s `?` placeholders, in order: an ODBC source's cursor bounds.
    pub(crate) parameters: Vec<BoundParameter>,
    /// The cursor range read: `from <= cursor < to`, each open when `None`.
    pub(crate) bounds: (Option<String>, Option<String>),
    pub(crate) mode: WriteMode,
    pub(crate) note: Option<String>,
    /// Whether the lower bound itself is read.
    pub(crate) inclusive: bool,
    /// The destination's cursor maximum, without lookback, when it was read.
    pub(crate) maximum: Option<String>,
}

fn write_mode(
    spec: &TableSpec,
    reload: bool,
    from: Option<String>,
    to: Option<String>,
) -> WriteMode {
    match (reload, spec.strategy) {
        (true, _) => WriteMode::Recreate,
        (false, WriteStrategy::Append) => WriteMode::Append,
        (false, WriteStrategy::Replace) => WriteMode::Replace,
        (false, WriteStrategy::Merge) => WriteMode::Merge {
            unique_key: spec.unique_key.clone(),
            dedupe: (spec.on_duplicate_keys == OnDuplicateKeys::Dedupe).then(|| MergeDedupe {
                ordinal: match spec.files.is_some() {
                    true => SOURCE_ROW_COLUMN.to_owned(),
                    false => MERGE_ROW_COLUMN.to_owned(),
                },
                sort: spec
                    .dedup_sort
                    .as_ref()
                    .map(|(column, direction)| (column.clone(), *direction == SortDirection::Desc)),
            }),
        },
        (false, WriteStrategy::DeleteInsert) => WriteMode::DeleteInsert {
            cursor_column: spec
                .cursor
                .as_ref()
                .map(|cursor| cursor.column.clone())
                .unwrap_or_default(),
            from,
            to,
            values: Vec::new(),
        },
    }
}

/// The source expression of `spec`'s cursor among `read_columns`, as its filter compares it: a text cursor bytewise, in code point order, in the source's own dialect (`render_binary_text`), as the destination's maximum is read, whatever collation the source column has; any other cursor, and a source no filter reaches, as it is. `None` when the cursor is not read.
pub(crate) fn source_cursor(
    spec: &TableSpec,
    read_columns: &[ReadColumn],
    cursor_type: Option<&ColumnType>,
) -> Option<String> {
    let name = spec.cursor.as_ref()?.column.as_str();
    let expression = read_columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))?
        .expression
        .as_str();
    let text = matches!(cursor_type, Some(ColumnType::Text | ColumnType::Varchar(_)));
    let dialect = match (text, &spec.warehouse_source) {
        (false, _) => None,
        (true, Some(source)) => source_dialect(source.system),
        (true, None) => {
            (spec.python.is_none() && spec.files.is_none()).then_some(Warehouse::Postgres)
        }
    };
    Some(dialect.map_or_else(
        || expression.to_owned(),
        |dialect| sql_adapter(dialect).render_binary_text(expression),
    ))
}

/// `column >= 'from' AND column < 'to'`, from the bounds that are set, as literals of the source's `dialect` (Postgres's when `None`): a numeric cursor's plain-number bounds as numbers (`render_cursor_number`), as BigQuery compares a number with no string; a text, date or wall-clock timestamp cursor's bounds as `?` placeholders when `parameters` collects them, in order, to bind as their type (an ODBC source's: no literal reads alike everywhere, since some databases read a backslash in a string as an escape, OpenEdge compares no string with a timestamp, and SQL Server's `{ts}` escape is a `datetime` that rounds milliseconds); any other bound as the dialect's string literal, so a value holding a quote or a backslash stays one string.
pub(crate) fn source_filter(
    column: &str,
    (cursor_type, dialect): (Option<&ColumnType>, Option<Warehouse>),
    (lower, upper): (Option<(&str, &str)>, Option<&str>),
    mut parameters: Option<&mut Vec<BoundParameter>>,
) -> Option<String> {
    let adapter = sql_adapter(dialect.unwrap_or(Warehouse::Postgres));
    let mut literal = |value: &str| match (
        cursor_type.filter(|cursor_type| numeric_cursor(cursor_type) && is_number(value)),
        parameters
            .as_mut()
            .zip(cursor_type.and_then(|cursor_type| bound_parameter(value, cursor_type))),
    ) {
        (Some(cursor_type), _) => adapter.render_cursor_number(value, cursor_type),
        (None, Some((parameters, parameter))) => {
            parameters.push(parameter);
            "?".to_owned()
        }
        (None, None) => adapter.render_loader_value_literal(value),
    };
    let mut bounds: Vec<String> = Vec::new();
    if let Some((operator, value)) = lower {
        bounds.push(format!("{column} {operator} {}", literal(value)));
    }
    if let Some(value) = upper {
        bounds.push(format!("{column} < {}", literal(value)));
    }
    (!bounds.is_empty()).then(|| bounds.join(" AND "))
}

/// Whether `spec`'s cursor bounds bind as parameters of the cursor's type: on a source that binds them (ODBC), unless the cursor column is pinned to a type other than text, which may not be the source column's own (a `varchar` read as a timestamp), so its bounds stay literals the database converts; text binds as text whatever the column, as a literal would convert, without a database reading its backslashes.
pub(crate) fn binds_cursor(spec: &TableSpec) -> bool {
    let pinned_other = spec.cursor.as_ref().is_some_and(|cursor| {
        spec.pinned.iter().any(|(name, column_type)| {
            name.eq_ignore_ascii_case(&cursor.column)
                && !matches!(column_type, ColumnType::Text | ColumnType::Varchar(_))
        })
    });
    !pinned_other
        && spec
            .warehouse_source
            .as_ref()
            .is_some_and(|source| source_binds_text(source.system))
}

/// Whether a cursor of `column_type` compares as a number.
fn numeric_cursor(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::SmallInt | ColumnType::Int | ColumnType::Decimal { .. } | ColumnType::Float
    )
}

/// Whether `value` is a plain decimal number (a sign, digits with at most one point, and an optional exponent), safe to write unquoted.
fn is_number(value: &str) -> bool {
    let digits = |text: &str| text.chars().all(|character| character.is_ascii_digit());
    let unsigned = value.strip_prefix(['-', '+']).unwrap_or(value);
    let (mantissa, exponent) = unsigned.split_once(['e', 'E']).unwrap_or((unsigned, "0"));
    let exponent = exponent.strip_prefix(['-', '+']).unwrap_or(exponent);
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    !(whole.is_empty() && fraction.is_empty())
        && digits(whole)
        && digits(fraction)
        && !exponent.is_empty()
        && digits(exponent)
}

/// Where the read starts, and why: a `--start-cursor`, the destination's maximum (read after it by `append` from a database or Python source, unless `append_cursor_inclusive`; a files source reads from it, skipping files already loaded), `cursor_start`, or nothing; with the destination's maximum (without lookback) when it was read.
async fn lower_bound(
    request: &WindowRequest<'_>,
    cursor: &CursorSpec,
    cursor_type: &ColumnType,
    start_flag: Option<&str>,
) -> Result<Result<(Option<String>, String, Option<String>), String>, LoaderError> {
    let options = request.options;
    if let Some(start) = start_flag {
        return Ok(Ok((
            Some(start.to_owned()),
            format!("{} from {start} (--start-cursor)", cursor.column),
            None,
        )));
    }
    let cursor_start = match cursor
        .start
        .as_deref()
        .map(|start| cursor_text(start, cursor_type))
    {
        Some(Ok(start)) => Some(start),
        Some(Err(problem)) => return Ok(Err(format!("cursor_start: {problem}"))),
        None => None,
    };
    if cursor.from_now && !options.reload {
        let lookback = cursor.lookback.unwrap_or_default();
        let from = match cursor_from_now(chrono::Utc::now(), cursor_type, NowShift::Back(lookback))
        {
            Ok(from) => from,
            Err(problem) => return Ok(Err(problem)),
        };
        let note = format!(
            "{} from {from} (now minus lookback {})",
            cursor.column,
            humantime::format_duration(lookback)
        );
        return Ok(Ok((Some(from), note, None)));
    }
    if options.reload || !request.table_exists {
        let label = if options.reload {
            "reload"
        } else {
            "first run"
        };
        return Ok(Ok(match &cursor_start {
            Some(start) => (
                Some(start.clone()),
                format!("{} {label}, from cursor_start {start}", cursor.column),
                None,
            ),
            None => (None, format!("{} {label}, everything", cursor.column), None),
        }));
    }
    let spec = request.spec;
    let query = sql_adapter(spec.warehouse).render_max_cursor(&CursorQuery {
        table: &spec.destination,
        column: &cursor.column,
        column_type: cursor_type,
        lookback: cursor.lookback,
        scope: match spec.shared_destination {
            true => &spec.constants,
            false => &[],
        },
    });
    let Some(query) = query else {
        return Ok(Err(format!(
            "lookback needs a timestamp or date cursor, but {} is {cursor_type}",
            cursor.column
        )));
    };
    let lookback = cursor
        .lookback
        .map(|lookback| format!(" minus lookback {}", humantime::format_duration(lookback)))
        .unwrap_or_default();
    let raw = single_text(request.destination, query.sql).await?;
    let bounds = raw.as_deref().map(|raw| {
        Ok::<(String, String), String>((
            cursor_bound(raw, query.value, cursor.lookback)?,
            cursor_bound(raw, query.value, None)?,
        ))
    });
    let (maximum, data_maximum) = match bounds {
        Some(Ok((bound, data))) => (Some(bound), Some(data)),
        Some(Err(problem)) => return Ok(Err(problem)),
        None => (None, None),
    };
    Ok(Ok(match (maximum, &cursor_start) {
        (Some(maximum), _) => {
            let bound = match request.spec.strategy == WriteStrategy::Append
                && !cursor.inclusive
                && request.spec.files.is_none()
            {
                true => "after",
                false => "from",
            };
            let note = format!(
                "{} {bound} {maximum} (destination maximum{lookback})",
                cursor.column
            );
            (Some(maximum), note, data_maximum)
        }
        (None, Some(start)) => (
            Some(start.clone()),
            format!("{} empty table, from cursor_start {start}", cursor.column),
            None,
        ),
        (None, None) => (
            None,
            format!("{} empty table, everything", cursor.column),
            None,
        ),
    }))
}

/// The read window for one table, or why it cannot be read as asked.
pub(crate) async fn cursor_window(
    request: &WindowRequest<'_>,
) -> Result<Result<Window, String>, LoaderError> {
    let spec = request.spec;
    let options = request.options;
    let backfill = options.start_cursor.is_some() || options.end_cursor.is_some();
    let (Some(cursor), Some(cursor_type)) = (&spec.cursor, request.cursor_type) else {
        if backfill {
            return Ok(Err(
                "--start-cursor/--end-cursor need a cursor_column".to_owned()
            ));
        }
        return Ok(Ok(Window {
            filter: None,
            parameters: Vec::new(),
            bounds: (None, None),
            mode: write_mode(spec, options.reload, None, None),
            note: None,
            inclusive: true,
            maximum: None,
        }));
    };
    if backfill && options.reload {
        return Ok(Err(
            "--reload rebuilds the whole table, so it takes no --start-cursor/--end-cursor"
                .to_owned(),
        ));
    }
    let flag = |value: &Option<String>| {
        value
            .as_deref()
            .map(|text| cursor_text(text, cursor_type))
            .transpose()
    };
    let (start_flag, to) = match (flag(&options.start_cursor), flag(&options.end_cursor)) {
        (Ok(start), Ok(end)) => (start, end),
        (Err(problem), _) | (_, Err(problem)) => return Ok(Err(problem)),
    };
    let (from, note, maximum) =
        match lower_bound(request, cursor, cursor_type, start_flag.as_deref()).await? {
            Ok(bound) => bound,
            Err(problem) => return Ok(Err(problem)),
        };
    let inclusive = match (spec.strategy, &options.start_cursor) {
        (WriteStrategy::Append, None) => cursor.inclusive || maximum.is_none(),
        _ => true,
    };
    let operator = match inclusive {
        true => ">=",
        false => ">",
    };
    let ahead = match (&to, cursor.lookahead) {
        (None, Some(lookahead)) => {
            match cursor_from_now(chrono::Utc::now(), cursor_type, NowShift::Ahead(lookahead)) {
                Ok(end) => Some((end, lookahead)),
                Err(problem) => return Ok(Err(problem)),
            }
        }
        _ => None,
    };
    let (to, note) = match (to, ahead) {
        (Some(end), _) => (
            Some(end.clone()),
            format!("{note}, below {end} (--end-cursor)"),
        ),
        (None, Some((end, lookahead))) => (
            Some(end.clone()),
            format!(
                "{note}, below {end} (now plus lookahead {})",
                humantime::format_duration(lookahead)
            ),
        ),
        (None, None) => (None, note),
    };
    let source = request.spec.warehouse_source.as_ref();
    let mut parameters: Vec<BoundParameter> = Vec::new();
    let filter = source_filter(
        request.cursor_expression.unwrap_or_default(),
        (
            request.cursor_type,
            source.and_then(|source| source_dialect(source.system)),
        ),
        (from.as_deref().map(|from| (operator, from)), to.as_deref()),
        binds_cursor(spec).then_some(&mut parameters),
    );
    Ok(Ok(Window {
        filter,
        parameters,
        bounds: (from.clone(), to.clone()),
        mode: write_mode(spec, options.reload, from, to),
        note: Some(note),
        inclusive,
        maximum,
    }))
}

/// What decides how a table's rows are cut into commits.
pub(crate) struct CommitRequest<'a> {
    pub(crate) spec: &'a TableSpec,
    pub(crate) window: Option<&'a ReadWindow>,
    pub(crate) cursor_type: Option<&'a ColumnType>,
    pub(crate) read_columns: &'a [ReadColumn],
    pub(crate) files: Option<&'a FilesRead>,
    pub(crate) mode: &'a WriteMode,
}

/// The commits of a table's load: a files source's batches of whole partitions; a cursor table's `batch_size` windows; an ordered Postgres read (an index leads with a non-text cursor) committing along the way; the whole read for any other cursor table, since committing part of an unordered read could skip rows the next run never reads, for `batch_size = "none"`, and for `append` without a cursor or logged state, which would have nowhere to resume from; about every 1 GB for other tables without a cursor.
pub(crate) fn commit_rule(request: &CommitRequest<'_>) -> Result<CommitRule, String> {
    let spec = request.spec;
    if let Some(files) = request.files {
        let by_value = match request.mode {
            WriteMode::DeleteInsert { cursor_column, .. } => Some(cursor_column.as_str()),
            _ => None,
        };
        if matches!(request.mode, WriteMode::Merge { .. }) {
            return Ok(CommitRule::FileGroups(one_file_each(files.files.len())));
        }
        return Ok(CommitRule::FileGroups(file_groups(&files.files, by_value)));
    }
    if spec.batch == Some(BatchStep::Whole) {
        return Ok(CommitRule::Whole(WholeReason::Chosen));
    }
    let Some(cursor) = &spec.cursor else {
        return Ok(match request.mode {
            WriteMode::Append if !spec.logged => {
                CommitRule::Whole(WholeReason::AppendWithoutCursor)
            }
            _ => CommitRule::Size,
        });
    };
    if let (Some(step), Some(window)) = (spec.batch, request.window) {
        let to = match (&window.to, &spec.source_end) {
            (Some(to), _) => Some(to.as_str()),
            (None, Some(SourceEnd::Maximum { end, .. })) => Some(end.as_str()),
            (None, Some(SourceEnd::Empty)) => window.from.as_deref(),
            (None, None) => None,
        };
        let windows = split_windows(&WindowSplit {
            from: window.from.as_deref(),
            to,
            inclusive: window.inclusive,
            step,
            cursor_type: request
                .cursor_type
                .filter(|known| spec.python.is_none() || **known != ColumnType::Text),
            now: chrono::Utc::now(),
        })?;
        return Ok(CommitRule::Windows(windows));
    }
    let plain_column = request
        .read_columns
        .iter()
        .find(|column| column.name == cursor.column)
        .is_some_and(|column| column.expression == quoted(&column.name));
    let indexed = spec.python.is_none()
        && plain_column
        && spec.source_table.indexed_columns.contains(&cursor.column);
    let ordered_mode = matches!(
        request.mode,
        WriteMode::Append | WriteMode::Merge { .. } | WriteMode::DeleteInsert { .. }
    );
    match (indexed && ordered_mode, request.cursor_type) {
        (true, Some(ColumnType::Text | ColumnType::Varchar(_))) => {
            Ok(CommitRule::Whole(WholeReason::TextCursor))
        }
        (true, Some(cursor_type)) => Ok(CommitRule::Ordered {
            column: cursor.column.clone(),
            cursor_type: cursor_type.clone(),
        }),
        _ => Ok(CommitRule::Whole(WholeReason::Unordered)),
    }
}

/// `name` as Postgres quotes an identifier.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The cursor's type, and why the table's strategy and cursor do not suit its path; a captured table's path is checked by [`captured_problems`] instead.
pub(crate) fn check_output(
    spec: &TableSpec,
    path: &TablePath,
    desired: &[DesiredColumn],
) -> (Option<ColumnType>, Vec<String>) {
    let cursor_name = spec.cursor.as_ref().map(|cursor| cursor.column.as_str());
    let cursor_type = desired
        .iter()
        .find(|column| cursor_name.is_some_and(|name| column.name.eq_ignore_ascii_case(name)))
        .map(|column| column.column_type.clone());
    let problems = match spec.cdc {
        Some(_) => Vec::new(),
        None => output_problems(&OutputCheck {
            strategy: spec.strategy,
            cursor_column: cursor_name,
            cursor_type: cursor_type.as_ref(),
            partition: path.partition.as_ref(),
        }),
    };
    (cursor_type, problems)
}

/// Why a captured table cannot load into its files: its changes are one changelog, which partition folders by a cursor would split, and `--reload` would leave as it is (`--resnapshot` copies it again).
pub(crate) fn captured_problems(
    spec: &TableSpec,
    path: &TablePath,
    options: &PlanOptions,
) -> Vec<String> {
    let captured = spec.cdc.is_some();
    [
        (captured && path.partition.is_some(), CDC_PARTITION_PROBLEM),
        (captured && options.reload, CDC_RELOAD_PROBLEM),
    ]
    .into_iter()
    .filter(|(applies, _)| *applies)
    .map(|(_, problem)| problem.to_owned())
    .collect()
}

/// A files table's schema with what `--resnapshot` does among its warnings, and its read columns, cut to those it writes when captured.
pub(crate) fn captured_columns(
    spec: &TableSpec,
    options: &PlanOptions,
    mut schema: SchemaPlan,
    read: Vec<ReadColumn>,
) -> (SchemaPlan, Vec<ReadColumn>) {
    schema.warnings.extend(resnapshot_warning(spec, options));
    let read = match spec.cdc {
        Some(_) => captured_read(spec, read, &schema),
        None => read,
    };
    (schema, read)
}

/// Why the table cannot be written in the target's format: a `postgres_cdc` table and an `append` cursor by `{cursor:date}` read their position from Parquet statistics, which CSV files lack; `single_file` names each file after its table, so `append` would overwrite it and a files source could not record the files it read against a load id.
pub(crate) fn encoding_problems(
    spec: &TableSpec,
    path: &TablePath,
    encoding: &OutputEncoding,
) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    let csv = encoding.format == FilesFormat::Csv;
    if csv && spec.cdc.is_some() {
        problems.push(
            "postgres_cdc reads its position back from Parquet statistics, so it writes files targets as Parquet; drop format = \"csv\"".to_owned(),
        );
    }
    let by_date = path
        .partition
        .as_ref()
        .is_some_and(|partition| partition.by == PartitionBy::Date);
    if csv && spec.strategy == WriteStrategy::Append && spec.cursor.is_some() && by_date {
        problems.push(
            "append by a {cursor:date} partition reads the newest files' maximum from Parquet statistics, which CSV files lack; partition by {cursor}, or use delete_insert or replace".to_owned(),
        );
    }
    if encoding.single_file && spec.files.is_some() {
        problems.push(
            "single_file can't record which source files a load from a files source read, as its file carries no load id; drop single_file".to_owned(),
        );
    }
    if encoding.single_file && spec.strategy == WriteStrategy::Append {
        problems.push(
            "single_file names each file after its table, so append would overwrite the last one; use replace or delete_insert, or drop single_file".to_owned(),
        );
    }
    problems
}
