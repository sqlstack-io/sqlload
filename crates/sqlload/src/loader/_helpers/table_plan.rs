use crate::loader::_helpers::destination::live_columns;
use crate::loader::_helpers::window::{WindowRequest, cursor_window};
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, PlanOptions, TablePlan, TableSpec};
use crate::postgres_source::main::plan_read::plan_read;
use crate::postgres_source::models::{ReadColumn, SourceTable};
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
    if let Some(cursor) = spec.cursor.as_ref().filter(|_| cursor_type.is_none()) {
        schema = blocked_plan(vec![format!(
            "cursor_column {} is not a loaded column",
            cursor.column
        )]);
    }
    let request = WindowRequest {
        spec,
        destination,
        table_exists: live.is_some(),
        cursor_type,
        options,
    };
    let (source_filter, mode, cursor_note) = match cursor_window(&request).await? {
        Ok(window) => (window.filter, window.mode, window.note),
        Err(problem) => {
            schema = blocked_plan(vec![problem]);
            (None, WriteMode::Append, None)
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
    })
}
