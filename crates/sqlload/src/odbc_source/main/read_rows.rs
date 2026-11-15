//! Read a table or query through ODBC as Arrow batches.

use arrow_array::RecordBatch;
use arrow_odbc::arrow_schema_from;
use arrow_odbc::odbc_api::Cursor;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::SendError;

use crate::adapters::contract::models::BoundParameter;
use crate::odbc_source::_helpers::batches::{batch_reader, fetch_error, fetch_schema};
use crate::odbc_source::_helpers::columns::{fetch_shape, fetch_size};
use crate::odbc_source::_helpers::connect::connect;
use crate::odbc_source::_helpers::convert::{converted, output_schema};
use crate::odbc_source::_helpers::parameters::bound_parameters;
use crate::odbc_source::_helpers::query::select_query;
use crate::odbc_source::_helpers::values::read_values;
use crate::odbc_source::constants::BATCHES_AHEAD;

use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::{OdbcReadRequest, OdbcReader};

/// Run `request`'s `SELECT`, its parameters bound, on a blocking thread and pass its rows to `on_batch` in batches of at most `batch_rows`, each converted to the read columns' types, reading at most a few batches ahead; returns the row count. A table whose text or binary columns all fit a buffer under the long-column threshold is fetched in bulk, as many rows at a time as the memory budget holds; one with a longer or unbounded column is read value by value, every value at its own size, each batch ending once its values reach the budget. A text or binary value longer than the reader's cap fails the read instead of being cut.
pub async fn read_rows(
    reader: &OdbcReader,
    request: &OdbcReadRequest<'_>,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, OdbcSourceError> {
    let schema = output_schema(request.columns).map_err(OdbcSourceError::Invalid)?;
    let query = select_query(request);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(BATCHES_AHEAD);
    let owned = (
        reader.clone(),
        (request.batch_rows, request.memory_budget),
        request.parameters.to_vec(),
        request.table.qualified_name(),
    );
    let producer = tokio::task::spawn_blocking(move || {
        fetch(&owned.0, (&query, &owned.2), (owned.1, &owned.3), &sender)
    });
    let mut rows: u64 = 0;
    while let Some(batch) = receiver.recv().await {
        let batch = converted(&batch?, &schema).map_err(OdbcSourceError::Invalid)?;
        rows += batch.num_rows() as u64;
        on_batch(batch).await.map_err(OdbcSourceError::Consumer)?;
    }
    producer
        .await
        .map_err(|error| OdbcSourceError::Invalid(format!("the ODBC read stopped: {error}")))??;
    Ok(rows)
}

/// Fetch `query`'s rows, with `parameters` bound to its placeholders as `bound_parameters` binds them, into `sender` until they end, the receiver goes away, or a fetch fails (sent on as the error); see [`send_rows`]. Decimal columns are fetched as text, which `converted` parses without rounding, since drivers round values to the scale they describe (an unbounded `numeric` as `NUMERIC(28,6)`).
fn fetch(
    reader: &OdbcReader,
    (query, parameters): (&str, &[BoundParameter]),
    limits: ((usize, usize), &str),
    sender: &Sender<Result<RecordBatch, OdbcSourceError>>,
) -> Result<(), OdbcSourceError> {
    let connection = connect(reader)?;
    let dbms: String = connection.database_management_system_name()?;
    let mut prepared = connection.prepare(query)?;
    let parameters = bound_parameters(&mut prepared, &dbms, parameters);
    let Some(cursor) = prepared.execute(parameters.as_slice())? else {
        return Err(OdbcSourceError::Invalid(
            "the read returned no result set".to_owned(),
        ));
    };
    send_rows(reader, (cursor, dbms), limits, sender)
}

/// Send `cursor`'s rows on: in bulk, as many rows at a time as `budget` holds (at most `batch_rows`), or value by value when a column is too long to bind (see `read_values`), naming `table` in what it logs.
fn send_rows<C: Cursor>(
    reader: &OdbcReader,
    (mut cursor, dbms): (C, String),
    ((batch_rows, budget), table): ((usize, usize), &str),
    sender: &Sender<Result<RecordBatch, OdbcSourceError>>,
) -> Result<(), OdbcSourceError> {
    let schema = fetch_schema(&arrow_schema_from(&mut cursor, Some(&dbms), false)?);
    let shape = fetch_shape(&mut cursor, &schema, &reader.limits)?;
    if !shape.long_columns.is_empty() {
        let send = |batch| match sender.blocking_send(Ok(batch)) {
            Ok(()) => true,
            Err(SendError(_)) => false,
        };
        return read_values(
            (cursor, &schema),
            (budget, batch_rows),
            (&reader.limits, table),
            send,
        );
    }
    let rows = fetch_size(&shape, budget, batch_rows).rows;
    let batches = batch_reader(reader, cursor, (dbms, schema, rows))?;
    for batch in batches {
        let batch = batch.map_err(|error| fetch_error(&error.to_string()));
        let failed = batch.is_err();
        if sender.blocking_send(batch).is_err() || failed {
            break;
        }
    }
    Ok(())
}
