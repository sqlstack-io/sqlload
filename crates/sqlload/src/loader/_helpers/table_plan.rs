use crate::loader::_helpers::destination::live_columns;
use crate::loader::_helpers::route::table_route;
use crate::loader::_helpers::sources::{FilesPlanRequest, files_read_columns, plan_files};
use crate::loader::_helpers::window::{WindowRequest, cursor_window};
use crate::loader::errors::LoaderError;
use crate::loader::main::_plan_files::plan_files_table;
use crate::loader::models::{Destination, PlanOptions, TablePlan, TableSpec};
use crate::postgres_source::main::plan_read::plan_read;
use crate::postgres_source::models::{ReadColumn, SourceTable};
use crate::project::models::WriteStrategy;
use crate::schema_plan::main::metadata_columns::metadata_columns;
use crate::schema_plan::main::plan_schema::plan_schema;
use crate::schema_plan::models::{
    Blocked, ColumnOrigin, DesiredColumn, SchemaPlan, SchemaRules, TableAction,
};
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
        route: None,
        files: None,
        output: None,
    }
}

/// One table's plan against its live destination table.
pub(crate) async fn plan_table(
    spec: &TableSpec,
    destination: &Destination,
    options: &PlanOptions,
) -> Result<TablePlan, LoaderError> {
    if let Destination::Files(files) = destination {
        return plan_files_table(spec, files, options).await;
    }
    let live = live_columns(destination, &spec.destination).await?;
    let planned_read = match spec.files {
        Some(_) => files_read_columns(spec),
        None => plan_read(&selected_columns(spec), &spec.pinned),
    };
    let read_columns = match planned_read {
        Ok(read) => read,
        Err(problems) => {
            return Ok(TablePlan {
                spec: spec.clone(),
                read_columns: Vec::new(),
                live,
                schema: blocked_plan(problems),
                mode: WriteMode::Append,
                source_filter: None,
                cursor_note: None,
                route: None,
                files: None,
                output: None,
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
            .find(|column| column.name == cursor.column)
            .map(|column| &column.column_type)
    });
    let cursor_name = spec.cursor.as_ref().map(|cursor| cursor.column.as_str());
    let written = cursor_name.is_some_and(|name| {
        schema
            .write_columns
            .iter()
            .any(|column| column.name == name)
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
        .find(|column| Some(column.name.as_str()) == cursor_name)
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
    Ok(TablePlan {
        spec: spec.clone(),
        read_columns,
        live,
        schema,
        mode,
        source_filter,
        cursor_note,
        route,
        files,
        output: None,
    })
}
