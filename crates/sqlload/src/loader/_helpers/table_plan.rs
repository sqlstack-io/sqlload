use crate::column_types::models::ColumnType;
use crate::loader::_helpers::destination::{live_columns, single_text};
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, TablePlan, TableSpec};
use crate::postgres_source::main::plan_read::plan_read;
use crate::postgres_source::models::{ReadColumn, SourceTable};
use crate::project::models::WriteStrategy;
use crate::schema_plan::main::metadata_columns::metadata_columns;
use crate::schema_plan::main::plan_schema::plan_schema;
use crate::schema_plan::models::{
    Blocked, ColumnOrigin, DesiredColumn, LiveColumn, SchemaPlan, SchemaRules, TableAction,
};
use crate::write_sql::main::cursor_query::cursor_query;
use crate::write_sql::models::{CursorQuery, WriteMode};

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

/// The incremental window: the source filter, the write mode and a note for `plan`.
async fn cursor_window(
    spec: &TableSpec,
    destination: &Destination,
    live: Option<&[LiveColumn]>,
    cursor_type: Option<&ColumnType>,
) -> Result<Result<(Option<String>, WriteMode, Option<String>), String>, LoaderError> {
    let keyless = |from: Option<String>| match spec.strategy {
        WriteStrategy::Append => WriteMode::Append,
        WriteStrategy::Replace => WriteMode::Replace,
        WriteStrategy::Merge => WriteMode::Merge {
            unique_key: spec.unique_key.clone(),
        },
        WriteStrategy::DeleteInsert => WriteMode::DeleteInsert {
            cursor_column: spec
                .cursor
                .as_ref()
                .map(|cursor| cursor.column.clone())
                .unwrap_or_default(),
            from,
        },
    };
    let (Some(cursor), Some(cursor_type)) = (&spec.cursor, cursor_type) else {
        return Ok(Ok((None, keyless(None), None)));
    };
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
    let maximum = match live {
        Some(_) => single_text(destination, query).await?,
        None => None,
    };
    let (from, note) = match (&maximum, &cursor.start) {
        (Some(maximum), _) => (
            Some(maximum.clone()),
            format!(
                "{} from {maximum} (destination maximum{})",
                cursor.column,
                cursor
                    .lookback
                    .map(|lookback| format!(
                        " minus lookback {}",
                        humantime::format_duration(lookback)
                    ))
                    .unwrap_or_default()
            ),
        ),
        (None, Some(start)) => (
            Some(start.clone()),
            format!("{} first run, from cursor_start {start}", cursor.column),
        ),
        (None, None) => (None, format!("{} first run, everything", cursor.column)),
    };
    let operator = match (spec.strategy, cursor.inclusive) {
        (WriteStrategy::Append, false) => ">",
        _ => ">=",
    };
    let filter = from.as_ref().map(|from| {
        format!(
            "\"{}\" {operator} '{}'",
            cursor.column.replace('"', "\"\""),
            from.replace('\'', "''")
        )
    });
    Ok(Ok((filter, keyless(from), Some(note))))
}

/// One table's plan against its live destination table.
pub(crate) async fn plan_table(
    spec: &TableSpec,
    destination: &Destination,
    rebuild_allowed: bool,
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
        rebuild_allowed,
    };
    let mut schema = plan_schema(&desired, live.as_deref(), rules);
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
    let (source_filter, mode, cursor_note) =
        match cursor_window(spec, destination, live.as_deref(), cursor_type).await? {
            Ok(window) => window,
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
