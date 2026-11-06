//! Load a Python table batch by batch: call its table function (once per `batch_size` window), plan the table as the yielded columns appear, and stage and commit the rows as they come; nothing beyond the current batch is held.

use crate::loader::_helpers::run_log::reconcile_before_read;
use crate::loader::errors::LoaderError;
use crate::loader::models::{PythonFeed, PythonLoad, TableLoaded};

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
        flattened: std::collections::BTreeMap::new(),
    };
    let loaded = feed.load(load).await;
    if let Some(publisher) = &mut feed.publisher {
        publisher.close().await;
    }
    loaded
}
