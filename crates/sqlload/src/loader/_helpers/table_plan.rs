use crate::batch_plan::main::file_groups::file_groups;
use crate::batch_plan::main::split_windows::split_windows;
use crate::batch_plan::models::WindowSplit;
use crate::column_types::models::ColumnType;
use crate::external_stage::main::direct_problem::direct_problem;
use crate::external_stage::main::stage_route::stage_route;
use crate::external_stage::models::RouteRequest;
use crate::external_stage::models::{CopyColumn, CopyValue, DirectCheck};
use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::models::FilesRead;
use crate::files_source::models::{DataFile, SourceFormat};
use crate::loader::_helpers::destination::live_columns;
use crate::loader::_helpers::destination::single_text;
use crate::loader::_helpers::python::with_live_columns;
use crate::loader::_helpers::sources::FilesPlanRequest;
use crate::loader::_helpers::sources::files_read_columns;
use crate::loader::_helpers::sources::plan_files;
use crate::loader::constants::ENFORCED_UNDECLARED_PROBLEM;
use crate::loader::errors::LoaderError;
use crate::loader::main::_files_schema::files_schema;
use crate::loader::main::_logged_plan::with_run_log;
use crate::loader::main::_plan_files::plan_files_table;
use crate::loader::models::CommitRule;
use crate::loader::models::CursorSpec;
use crate::loader::models::Destination;
use crate::loader::models::PlanOptions;
use crate::loader::models::ReadWindow;
use crate::loader::models::ReplannedColumns;
use crate::loader::models::SourceEnd;
use crate::loader::models::TablePlan;
use crate::loader::models::TableSpec;
use crate::loader::models::WholeReason;
use crate::postgres_source::main::plan_read::plan_read;
use crate::postgres_source::models::ReadColumn;
use crate::postgres_source::models::SourceTable;
use crate::project::models::BatchStep;
use crate::project::models::Contract;
use crate::project::models::MaskMethod;
use crate::project::models::WriteStrategy;
use crate::routing::main::plan_route::plan_route;
use crate::routing::models::FileFormat;
use crate::routing::models::FileSet;
use crate::routing::models::ReaderOffer;
use crate::routing::models::Route;
use crate::routing::models::RouteOverrides;
use crate::routing::models::Step;
use crate::routing::models::Store;
use crate::routing::models::WriterAccepts;
use crate::schema_plan::main::metadata_columns::metadata_columns;
use crate::schema_plan::main::plan_schema::plan_schema;
use crate::schema_plan::models::Blocked;
use crate::schema_plan::models::ColumnOrigin;
use crate::schema_plan::models::DesiredColumn;
use crate::schema_plan::models::LiveColumn;
use crate::schema_plan::models::SchemaPlan;
use crate::schema_plan::models::SchemaRules;
use crate::schema_plan::models::TableAction;
use crate::snowflake_destination::constants::USER_STAGE;
use crate::snowflake_destination::models::{ExternalStage, StageRoute};
use crate::store_paths::models::StoreKind;
use crate::write_sql::main::alter_statements::alter_statements;
use crate::write_sql::main::create_schema::create_schema;
use crate::write_sql::main::create_table::create_table;
use crate::write_sql::main::cursor_bound::cursor_bound;
use crate::write_sql::main::cursor_query::cursor_query;
use crate::write_sql::main::cursor_text::cursor_text;
use crate::write_sql::models::CursorQuery;
use crate::write_sql::models::WriteMode;

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

/// The columns the table should have: the read columns (pinned or from the source), then the metadata columns.
pub(crate) fn desired_columns(spec: &TableSpec, read: &[ReadColumn]) -> Vec<DesiredColumn> {
    let mut desired: Vec<DesiredColumn> = read
        .iter()
        .map(|column| DesiredColumn {
            name: column.name.clone(),
            column_type: column.output.clone(),
            origin: match spec.pinned.contains_key(&column.name) {
                true => ColumnOrigin::Pinned,
                false => ColumnOrigin::Source,
            },
        })
        .collect();
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
        passed_over: Vec::new(),
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

/// Why the table's `batch` setting has no effect, if it has none: a files source commits by whole partitions, `replace` and `--reload` swap the whole table in once, and windows need a cursor.
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
    Some(format!("batch is ignored: {reason}"))
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
        (None, Destination::Files(_)) => stale.map(<[LiveColumn]>::to_vec),
        (None, _) => live_columns(destination, &spec.destination).await?,
    };
    let spec = with_live_columns(spec, live.as_deref()).unwrap_or_else(|| spec.clone());
    let read = read_columns(&spec).map_err(|problems| LoaderError::Invalid(problems.join("; ")))?;
    let desired = desired_columns(&spec, &read);
    let schema = match destination {
        Destination::Files(_) => files_schema(&spec, &desired, live.as_deref(), options),
        _ => {
            let rules = SchemaRules {
                warehouse: spec.warehouse,
                contract: spec.contract,
                on_schema_change: spec.on_schema_change,
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
        (None, None) => plan_read(&selected_columns(spec), &spec.pinned),
    }
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

async fn plan_table_once(
    spec: &TableSpec,
    destination: &Destination,
    options: &PlanOptions,
) -> Result<TablePlan, LoaderError> {
    if let Destination::Files(files) = destination {
        return plan_files_table(spec, files, options).await;
    }
    let live = live_columns(destination, &spec.destination).await?;
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
                passed_over: Vec::new(),
                planned_stage: None,
            });
        }
    };
    let desired = desired_columns(spec, &read_columns);
    let rules = SchemaRules {
        warehouse: spec.warehouse,
        contract: spec.contract,
        on_schema_change: spec.on_schema_change,
        type_change_rebuild: spec.type_change_rebuild,
        rebuild_allowed: options.rebuild_allowed,
    };
    let compared = live.as_deref().filter(|_| !options.reload);
    let mut schema = plan_schema(&desired, compared, rules);
    if options.reload && live.is_some() {
        schema.action = TableAction::Recreate;
    }
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
            "the table has no {name} column yet, so this load would read every row again and duplicate the rows already there; run it once with --reload"
        )]);
    }
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
                destination,
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
    let exists = live.is_some();
    let chosen = match (destination, &schema.blocked) {
        (Destination::Snowflake(snowflake), None) => {
            let request = RouteRequest {
                destination: &spec.destination,
                table_exists: exists,
                direct_append: mode == WriteMode::Append && !spec.logged,
                snowflake,
            };
            match stage_route(&request).await {
                Ok(chosen) => Some(chosen),
                Err(problem) => {
                    schema = blocked_plan(vec![problem]);
                    None
                }
            }
        }
        _ => None,
    };
    let (choice, planned_stage) = chosen.unzip();
    let planned_stage = planned_stage.flatten();
    let (stage, passed_over) = match choice {
        Some(choice) => (Some(choice.route), choice.passed_over),
        None => (None, Vec::new()),
    };
    if stage == Some(StageRoute::Insert) {
        schema.warnings.push(
            "rows load by batched INSERT, the slowest route; no stage or streaming was usable"
                .to_owned(),
        );
    }
    let route = match table_route(spec, (files.as_ref(), &mode), stage.as_ref(), destination) {
        Ok(route) => Some(route),
        Err(problem) => {
            schema = blocked_plan(vec![problem]);
            None
        }
    };
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
        passed_over,
        planned_stage,
    })
}

/// The statements that bring the table's schema in line with its plan; `None` when there is nothing to run: unchanged, rebuilt by the load itself, or a files target.
pub(crate) fn schema_statements(table: &TablePlan) -> Option<Vec<String>> {
    let spec = &table.spec;
    let warehouse = spec.warehouse;
    if table.output.is_some() {
        return None;
    }
    match &table.schema.action {
        TableAction::Unchanged | TableAction::Recreate => None,
        TableAction::Create => Some(vec![
            create_schema(&spec.destination, warehouse),
            create_table(&spec.destination, &table.schema.write_columns, warehouse),
        ]),
        TableAction::Alter(changes) => {
            let live = table.live.as_deref().unwrap_or_default();
            Some(alter_statements(
                &spec.destination,
                changes,
                live,
                warehouse,
            ))
        }
    }
}

/// What a destination can take.
fn writer_accepts(destination: &Destination, stage: Option<&StageRoute>) -> WriterAccepts {
    let stored = |store: Store| WriterAccepts {
        arrow: false,
        pulls_from: vec![store.clone()],
        pull_formats: vec![FileFormat::Parquet],
        staging: vec![store],
        rows_into: None,
    };
    match (destination, stage) {
        #[cfg(feature = "duckdb")]
        (Destination::DuckDb(_), _) => WriterAccepts {
            arrow: true,
            ..WriterAccepts::default()
        },
        (Destination::Files(_), _) => WriterAccepts {
            arrow: true,
            ..WriterAccepts::default()
        },
        (Destination::Snowflake(_), Some(StageRoute::External(stage))) => {
            stored(Store::ObjectStore(stage.location.to_string()))
        }
        (Destination::Snowflake(_), Some(StageRoute::Internal { name })) => {
            stored(Store::InternalStage(format!("@{name}")))
        }
        (Destination::Snowflake(_), Some(StageRoute::TargetTable)) => {
            stored(Store::InternalStage("@%table".to_owned()))
        }
        (Destination::Snowflake(_), Some(StageRoute::StagingTable)) => stored(
            Store::InternalStage("the staging table's @%table".to_owned()),
        ),
        (Destination::Snowflake(_), Some(StageRoute::Streaming { .. })) => WriterAccepts {
            rows_into: Some("Snowpipe Streaming into a transient staging table".to_owned()),
            ..WriterAccepts::default()
        },
        (Destination::Snowflake(_), Some(StageRoute::Insert)) => WriterAccepts {
            rows_into: Some("batched INSERT into a transient staging table".to_owned()),
            ..WriterAccepts::default()
        },
        (Destination::Snowflake(_), _) => stored(Store::InternalStage(format!("@{USER_STAGE}"))),
    }
}

/// What a table's source offers: rows as Arrow, plus the files themselves for a files source, with why the destination must not read them itself.
fn reader_offer(
    spec: &TableSpec,
    (read, mode): (Option<&FilesRead>, &WriteMode),
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
    let direct_blocked = match (read, stage) {
        (Some(read), Some(StageRoute::External(stage))) => {
            direct_check(spec, (read, read.files.as_slice()), mode, stage)
        }
        _ => None,
    };
    ReaderOffer {
        arrow: true,
        existing_files,
        unload_to: Vec::new(),
        estimated_bytes: match u64::try_from(spec.source_table.estimated_bytes) {
            Ok(bytes) => Some(bytes),
            Err(_) => None,
        },
        direct_blocked,
    }
}

/// Why Snowflake must not `COPY` these of a files table's files straight from `stage`.
pub(crate) fn direct_check(
    spec: &TableSpec,
    (read, files): (&FilesRead, &[DataFile]),
    mode: &WriteMode,
    stage: &ExternalStage,
) -> Option<String> {
    let source = spec.files.as_ref()?;
    let masks: Vec<(String, MaskMethod)> = spec
        .masked
        .columns
        .iter()
        .map(|(name, method)| (name.clone(), *method))
        .collect();
    direct_problem(&DirectCheck {
        stage_url: &stage.location,
        source: &source.location,
        format: source.encoding.format,
        files,
        columns: &copy_columns(spec, read),
        masks: &masks,
        whole_table: matches!(mode, WriteMode::Replace | WriteMode::Recreate),
        shaped_by_loader: spec.shaped_by_loader,
    })
}

/// The columns a direct `COPY` of a files table fills, in the order the files source reads them, each from the files, its path, or the file's name, version and row number.
pub(crate) fn copy_columns(spec: &TableSpec, read: &FilesRead) -> Vec<CopyColumn> {
    read.columns
        .iter()
        .map(|column| CopyColumn {
            name: column.name.clone(),
            column_type: column.column_type.clone(),
            value: match column.name.as_str() {
                SOURCE_FILE_COLUMN => CopyValue::SourceFile,
                SOURCE_VERSION_COLUMN => CopyValue::SourceVersion,
                SOURCE_ROW_COLUMN => CopyValue::SourceRow,
                name if partition_column(spec, name) => CopyValue::Partition,
                name => CopyValue::File {
                    file_type: file_type(spec, name),
                },
            },
        })
        .collect()
}

/// Whether `name` is one of the files source's partition columns.
fn partition_column(spec: &TableSpec, name: &str) -> bool {
    spec.files
        .iter()
        .flat_map(|files| files.partition_types.iter())
        .any(|(partition, _)| partition == name)
}

/// The type the known files hold column `name` as; a type with no portable equivalent is named as one the direct `COPY` never loads.
fn file_type(spec: &TableSpec, name: &str) -> Option<ColumnType> {
    let files = spec.files.as_ref()?;
    let found = files
        .table
        .columns
        .iter()
        .find(|found| found.name == name)?;
    Some(match &found.column_type {
        Ok(column_type) => column_type.clone(),
        Err(reason) => ColumnType::Native(format!("unsupported ({reason})")),
    })
}

/// Whether `route` has the destination load a files source's own files.
pub(crate) fn is_direct(route: Option<&Route>) -> bool {
    route.is_some_and(|route| {
        matches!(
            route.steps.as_slice(),
            [Step::UseFiles(_), Step::DestinationPulls(_)]
        )
    })
}

/// The route a table's data takes into `destination`; `read` is a files table's planned read.
pub(crate) fn table_route(
    spec: &TableSpec,
    read: (Option<&FilesRead>, &WriteMode),
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
    let query = cursor_query(&CursorQuery {
        table: &spec.destination,
        column: &cursor.column,
        column_type: cursor_type,
        lookback: cursor.lookback,
        warehouse: spec.warehouse,
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
    let inclusive = !matches!(
        (spec.strategy, cursor.inclusive, &options.start_cursor),
        (WriteStrategy::Append, false, None)
    );
    let operator = match inclusive {
        true => ">=",
        false => ">",
    };
    let note = match &to {
        Some(end) => format!("{note}, below {end} (--end-cursor)"),
        None => note,
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

/// The commits of a table's load: a files source's batches of whole partitions; a cursor table's `batch` windows; an ordered Postgres read (an index leads with a non-text cursor) committing along the way; the whole read for any other cursor table, since committing part of an unordered read could skip rows the next run never reads, for `batch = "whole"`, and for `append` without a cursor or logged state, which would have nowhere to resume from; about every 1 GB for other tables without a cursor.
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
