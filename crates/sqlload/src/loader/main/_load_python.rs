//! Load a Python table batch by batch: call its table function (once per `batch` window), plan the table as the yielded columns appear, and stage and commit the rows as they come; nothing beyond the current batch is held.

use crate::batch_plan::models::CursorWindow;
use crate::loader::_helpers::python::{
    cursor_type_name, final_state, refuse_emptying, shareable_connection, start_read,
};
use crate::loader::_helpers::run_log::{log_empty_run, reconcile_before_read, run_entry};
use crate::loader::constants::NO_DATA_NOTE;
use crate::loader::errors::LoaderError;
use crate::loader::models::{CommitRange, CommitRule, PythonFeed, PythonLoad, TableLoaded};
use crate::python_sources::models::{CursorRead, TableRead};

/// Load the table; a failure stops it at the batch that failed, keeping what committed before. A first load without rows or declared columns creates nothing.
pub(in crate::loader) async fn load_python(
    load: &PythonLoad<'_>,
) -> Result<TableLoaded, LoaderError> {
    let reconciled = reconcile_before_read(load.table, load.destination).await?;
    let state = load
        .table
        .logged
        .as_ref()
        .and_then(|logged| logged.current.as_ref())
        .and_then(|record| record.state.clone())
        .filter(|_| !load.options.reload);
    let mut feed = PythonFeed {
        table: load.table.clone(),
        merged: arrow_schema::Schema::empty(),
        rows: 0,
        outside: 0,
        publisher: None,
        changes: Vec::new(),
        state,
        reconciled,
    };
    let loaded = read_windows(load, &mut feed).await;
    if let Some(publisher) = &mut feed.publisher {
        publisher.close().await;
    }
    loaded
}

/// Call the function for each window (or once), committing each window, then commit the rest.
async fn read_windows(
    load: &PythonLoad<'_>,
    feed: &mut PythonFeed,
) -> Result<TableLoaded, LoaderError> {
    let windows: Vec<Option<CursorWindow>> = match &load.table.commits {
        CommitRule::Windows(windows) => windows.iter().cloned().map(Some).collect(),
        _ => vec![None],
    };
    let table = load.table.spec.source_table.name.clone();
    for window in &windows {
        let stream = start_read(load.host.clone(), table_read(load, window.as_ref(), feed)).await?;
        feed.read(load, stream, window.as_ref()).await?;
        if load.table.logged.is_some()
            && let Some(state) = final_state(load.host.clone(), table.clone()).await?
        {
            feed.state = Some(state);
        }
        let state = feed.state.clone();
        if let Some(window) = window
            && (feed.publisher.is_some() || load.table.exists)
        {
            let publisher = feed.publisher(load).await?;
            publisher.state = state;
            publisher
                .cut(CommitRange::Window {
                    from: window.from.clone(),
                    to: window.to.clone(),
                })
                .await?;
        }
    }
    if feed.outside > 0 {
        tracing::warn!(
            table = %load.table.spec.destination.table,
            rows = feed.outside,
            "skipped rows outside the cursor window; use ctx.cursor_value to read only new data"
        );
    }
    let declared = load
        .table
        .spec
        .python
        .as_ref()
        .map_or(0, |python| python.declared.len());
    if feed.rows == 0 {
        refuse_emptying(load.table, load.destination).await?;
    }
    if feed.publisher.is_none() && feed.rows == 0 && !load.table.exists && declared == 0 {
        let run = run_entry(
            load,
            load.table,
            (feed.state.clone(), feed.reconciled.clone()),
        );
        log_empty_run(run.as_ref(), load.table, load.destination).await?;
        return Ok(TableLoaded {
            rows: 0,
            changes: Vec::new(),
            note: Some(NO_DATA_NOTE.to_owned()),
        });
    }
    let unsettled = feed.publisher.is_none()
        || feed
            .merged
            .fields()
            .iter()
            .any(|field| *field.data_type() == arrow_schema::DataType::Null);
    if unsettled {
        feed.plan_data(load, true).await?;
    }
    let state = feed.state.clone();
    let rows = feed.publisher(load).await?.finish(state).await?;
    Ok(TableLoaded {
        rows,
        changes: std::mem::take(&mut feed.changes),
        note: None,
    })
}

/// What the table function is called with: the window to read (`ctx.cursor_value` / `ctx.cursor_end`) and the latest `ctx.state`; connections that targets write to are withheld, except the source's own s3 or filesystem connection.
fn table_read(
    load: &PythonLoad<'_>,
    window: Option<&CursorWindow>,
    feed: &PythonFeed,
) -> TableRead {
    let table = load.table;
    let spec = &table.spec;
    let connection = load
        .project
        .sources
        .get(&spec.source_name)
        .map(|source| source.connection.clone())
        .filter(|name| !name.is_empty());
    let withheld: Vec<String> = load
        .project
        .targets
        .values()
        .map(|target| target.connection.clone())
        .filter(|name| {
            Some(name) != connection.as_ref() || !shareable_connection(load.project, name)
        })
        .collect();
    let mut connections = load.project.connection_settings.clone();
    connections.retain(|name, _| !withheld.contains(name));
    let cursor = table.window.as_ref().map(|planned| CursorRead {
        column: planned.column.clone(),
        column_type: cursor_type_name(&feed.table, &planned.column),
        start: window.map_or(planned.from.clone(), |window| window.from.clone()),
        end: window.map_or(planned.to.clone(), |window| window.to.clone()),
        inclusive: window.map_or(planned.inclusive, |window| window.inclusive),
    });
    TableRead {
        source: spec.source_name.clone(),
        table: spec.source_table.name.clone(),
        connection,
        connections,
        withheld,
        cursor,
        logged: table.logged.is_some(),
        state: feed.state.clone(),
        is_reload: load.options.reload,
    }
}
