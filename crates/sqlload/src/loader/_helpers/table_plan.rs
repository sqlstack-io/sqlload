use crate::adapters::contract::main::cursor_bound::cursor_bound;
use crate::adapters::contract::main::cursor_from_now::cursor_from_now;
use crate::adapters::contract::main::cursor_text::cursor_text;
use crate::adapters::contract::models::CursorQuery;
use crate::adapters::contract::models::NowShift;
use crate::adapters::contract::models::WriteMode;
use crate::adapters::contract::models::{ChangeApply, ChangeWrite};
use crate::adapters::dispatch::main::dialect::files_destination::files_destination;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::route::choose_stage::choose_stage;
use crate::adapters::dispatch::main::route::direct_blocked::direct_blocked;
use crate::adapters::dispatch::main::route::route_warning::route_warning;
use crate::adapters::dispatch::main::route::stage_misses::stage_misses;
use crate::adapters::dispatch::main::route::writer_accepts::writer_accepts;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::main::session::single_text::single_text;
use crate::adapters::dispatch::main::session::write_problems::write_problems;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::files::main::output_problems::output_problems;
use crate::adapters::files::models::{OutputCheck, TablePath};
use crate::adapters::snowflake::models::StageRoute;
use crate::batch_plan::main::file_groups::file_groups;
use crate::batch_plan::main::split_windows::split_windows;
use crate::batch_plan::models::WindowSplit;
use crate::cdc::constants::{CDC_PARTITION_PROBLEM, CDC_RELOAD_PROBLEM, PLAIN_JSON_TYPE};
use crate::cdc::main::metadata_read_columns::metadata_read_columns;
use crate::column_types::models::ColumnType;
use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::files_source::models::FilesRead;
use crate::files_source::models::SourceFormat;
use crate::loader::_helpers::python::with_live_columns;
use crate::loader::_helpers::sources::column_fix;
use crate::loader::_helpers::sources::files_read_columns;
use crate::loader::_helpers::sources::plan_files;
use crate::loader::_helpers::sources::truncated_nanoseconds;
use crate::loader::_helpers::sources::{FilesPlanRequest, LoadedSource};
use crate::loader::constants::KEPT_TABLES_RELOAD_PROBLEM;
use crate::loader::constants::{
    CHECKS_LOADER_WARNING, ENFORCED_UNDECLARED_PROBLEM, UNLOADED_TIMES,
};
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
use crate::publish_checks::main::check_column_problems::check_column_problems;
use crate::routing::main::plan_route::plan_route;
use crate::routing::models::FileFormat;
use crate::routing::models::FileSet;
use crate::routing::models::ReaderOffer;
use crate::routing::models::Route;
use crate::routing::models::RouteOverrides;
use crate::routing::models::Step;
use crate::routing::models::Store;
use crate::schema_plan::constants::{CDC_PREFIX, CDC_UNCHANGED_COLUMN};
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

/// The columns the table should have: the read columns (pinned, typed by the source, or inferred from sampled values for Python tables and JSON, NDJSON and CSV files, whose partition columns are declared), then the metadata columns.
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
                .any(|(column, _)| column == name)
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
    let merged = spec
        .cdc
        .as_ref()
        .is_some_and(|cdc| cdc.apply != ChangeApply::Changelog);
    desired.retain(|column| !(merged && column.name == CDC_UNCHANGED_COLUMN));
    for column in desired
        .iter_mut()
        .filter(|column| column.name.starts_with(CDC_PREFIX))
    {
        column.origin = ColumnOrigin::Metadata;
    }
    desired.extend(metadata_columns());
    desired
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
    let plan = plan_table_once(spec, destination, options).await?;
    let mut plan = match with_live_columns(spec, plan.live.as_deref()) {
        Some(augmented) => plan_table_once(&augmented, destination, options).await?,
        None => plan,
    };
    plan.schema.warnings.extend(ignored_batch(&plan));
    with_run_log(plan, destination, options).await
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
    let spec = with_live_columns(spec, live.as_deref()).unwrap_or_else(|| spec.clone());
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
        (None, None) if spec.warehouse_source.is_some() => {
            unload_read_columns(&selected_columns(spec), &spec.pinned)
        }
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

/// A captured table's read columns: the data columns it writes (its snapshot stages them, its stream is matched to them by name and its snapshot record lists them), and every change column.
pub(crate) fn captured_read(read: Vec<ReadColumn>, schema: &SchemaPlan) -> Vec<ReadColumn> {
    let written: Vec<&str> = schema
        .write_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    read.into_iter()
        .filter(|column| {
            column.name.starts_with(CDC_PREFIX) || written.contains(&column.name.as_str())
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

/// Array and `json` columns of a captured table read as JSON: the stream sends them as Postgres text (`{a,b}` for an array, the original spacing and duplicate keys for `json`), which would not load as JSON or would differ from the snapshot's jsonb rows, so they must be pinned to text (`jsonb` streams as the snapshot reads it).
fn streamed_array_problems(table: &SourceTable, read: &[ReadColumn]) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    for column in &table.columns {
        let as_json = read
            .iter()
            .any(|read| read.name == column.name && read.output == ColumnType::Json);
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
    };
    let compared = live.as_deref().filter(|_| !options.reload);
    let mut schema = plan_schema(&desired, compared, rules);
    if options.reload && live.is_some() {
        schema.action = TableAction::Recreate;
    }
    let read_columns = match spec.cdc {
        Some(_) => captured_read(read_columns, &schema),
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
    if let Some(name) = cursor_name.filter(|_| !written && schema.blocked.is_none()) {
        schema = blocked_plan(vec![format!(
            "cursor_column {name} is not written to the table (excluded, not in the enforced contract, or ignored by on_schema_change)"
        )]);
    }
    let cursor_in_table = live.as_deref().is_some_and(|live| {
        live.iter()
            .any(|column| cursor_name.is_some_and(|name| column.name.eq_ignore_ascii_case(name)))
    });
    let cursor_expression = read_columns
        .iter()
        .find(|column| cursor_name.is_some_and(|name| column.name.eq_ignore_ascii_case(name)))
        .map(|column| column.expression.as_str());
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
        cursor_expression,
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
    let (source_filter, bounds, mode, cursor_note) = match window {
        Ok(Some(window)) => (window.filter, window.bounds, window.mode, window.note),
        Ok(None) => (None, (None, None), WriteMode::Append, None),
        Err(problem) => {
            schema = blocked_plan(vec![problem]);
            (None, (None, None), WriteMode::Append, None)
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
    let mode = captured_mode(spec, mode);
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
            if let WriteMode::Merge { unique_key } = &table.mode {
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
            StoreKind::S3 { .. } => Store::ObjectStore(files.location.to_string()),
            StoreKind::Filesystem { .. } => Store::Local(files.location.to_string()),
        },
        format: match files.encoding.format {
            SourceFormat::Parquet => FileFormat::Parquet,
            SourceFormat::Ndjson | SourceFormat::Json => FileFormat::Json,
            SourceFormat::Csv => FileFormat::Csv,
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
    let needs_loader = match (hmac.is_empty(), spec.test_loader_route, &spec.checks) {
        (false, _, _) => Some(format!(
            "{} masked with hmac, whose key must never appear in SQL",
            hmac.join(", ")
        )),
        (true, true, _) => Some("the table sets test_loader_route (tests only)".to_owned()),
        (true, false, Some(_)) => {
            Some("the table has checks, which run on the rows SQLLoad stages".to_owned())
        }
        (true, false, None) => None,
    };
    let unload_to: Vec<Store> = match &spec.warehouse_source {
        Some(WarehouseSource {
            route: WarehouseRoute::Unload { store, .. },
            ..
        }) => vec![store.clone()],
        None => Vec::new(),
    };
    let direct_blocked = match unload_to.is_empty() {
        true => direct_blocked,
        false => Some(UNLOADED_TIMES.to_owned()),
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

/// `"column" >= 'from' AND "column" < 'to'` for Postgres, from the bounds that are set.
pub(crate) fn source_filter(
    column: &str,
    lower: Option<(&str, &str)>,
    upper: Option<&str>,
) -> Option<String> {
    let literal = |value: &str| format!("'{}'", value.replace('\'', "''"));
    let bounds: Vec<String> = lower
        .map(|(operator, value)| format!("{column} {operator} {}", literal(value)))
        .into_iter()
        .chain(upper.map(|value| format!("{column} < {}", literal(value))))
        .collect();
    (!bounds.is_empty()).then(|| bounds.join(" AND "))
}

/// Where the read starts, and why: a `--start-cursor`, the destination's maximum, `cursor_start`, or nothing; with the destination's maximum (without lookback) when it was read.
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
            let note = format!(
                "{} from {maximum} (destination maximum{lookback})",
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
    Ok(Ok(Window {
        filter: source_filter(
            request.cursor_expression.unwrap_or_default(),
            from.as_deref().map(|from| (operator, from)),
            to.as_deref(),
        ),
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
        Some(_) => captured_read(read, &schema),
        None => read,
    };
    (schema, read)
}
