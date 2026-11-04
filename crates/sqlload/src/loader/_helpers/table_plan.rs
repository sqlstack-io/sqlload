use crate::loader::_helpers::destination::live_columns;
use crate::loader::_helpers::route::table_route;
use crate::loader::_helpers::window::{WindowRequest, cursor_window};
use crate::loader::errors::LoaderError;
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
fn selected_columns(spec: &TableSpec) -> SourceTable {
    let mut table = spec.source_table.clone();
    table.columns.retain(|column| {
        !spec
            .excluded_columns
            .iter()
            .any(|(excluded, _)| *excluded == column.name)
    });
    table
}

fn desired_columns(spec: &TableSpec, read: &[ReadColumn]) -> Vec<DesiredColumn> {
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

fn blocked_plan(reasons: Vec<String>) -> SchemaPlan {
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

/// One table's plan against its live destination table.
pub(crate) async fn plan_table(
    spec: &TableSpec,
    destination: &Destination,
    options: &PlanOptions,
) -> Result<TablePlan, LoaderError> {
    let live = live_columns(destination, &spec.destination).await?;
    let read_columns = match plan_read(&selected_columns(spec), &spec.pinned) {
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
    let (source_filter, mode, cursor_note) = match window {
        Ok(Some(window)) => (window.filter, window.mode, window.note),
        Ok(None) => (None, WriteMode::Append, None),
        Err(problem) => {
            schema = blocked_plan(vec![problem]);
            (None, WriteMode::Append, None)
        }
    };
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
    })
}
