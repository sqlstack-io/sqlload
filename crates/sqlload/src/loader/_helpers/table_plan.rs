use crate::column_types::models::ColumnType;
use crate::loader::_helpers::commits::{CommitRequest, commit_rule};
use crate::loader::_helpers::destination::live_columns;
use crate::loader::_helpers::python::with_live_columns;
use crate::loader::_helpers::route::table_route;
use crate::loader::_helpers::sources::{FilesPlanRequest, files_read_columns, plan_files};
use crate::loader::_helpers::window::{Window, WindowRequest, cursor_window};
use crate::loader::constants::ENFORCED_UNDECLARED_PROBLEM;
use crate::loader::errors::LoaderError;
use crate::loader::main::_logged_plan::with_run_log;
use crate::loader::main::_plan_files::plan_files_table;
use crate::loader::models::{
    CommitRule, Destination, PlanOptions, ReadWindow, TablePlan, TableSpec,
};
use crate::postgres_source::main::plan_read::plan_read;
use crate::postgres_source::models::{ReadColumn, SourceTable};
use crate::project::models::{Contract, WriteStrategy};
use crate::schema_plan::main::metadata_columns::metadata_columns;
use crate::schema_plan::main::plan_schema::plan_schema;
use crate::schema_plan::models::{
    Blocked, ColumnOrigin, DesiredColumn, SchemaPlan, SchemaRules, TableAction,
};
use crate::write_sql::main::alter_statements::alter_statements;
use crate::write_sql::main::create_schema::create_schema;
use crate::write_sql::main::create_table::create_table;
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
        commits: CommitRule::Whole,
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
    let plan = match with_live_columns(spec, plan.live.as_deref()) {
        Some(augmented) => plan_table_once(&augmented, destination, options).await?,
        None => plan,
    };
    with_run_log(plan, destination, options).await
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
                commits: CommitRule::Whole,
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
    let route = match table_route(spec, destination) {
        Ok(route) => Some(route),
        Err(problem) => {
            schema = blocked_plan(vec![problem]);
            None
        }
    };
    let exists = live.is_some();
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
            CommitRule::Whole
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
