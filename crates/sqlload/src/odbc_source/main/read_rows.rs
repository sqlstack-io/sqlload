//! Read a table or query through ODBC as Arrow batches.

use arrow_array::RecordBatch;
use tokio::sync::mpsc::Sender;

use crate::adapters::contract::models::BoundParameter;
use crate::odbc_source::_helpers::batches::{batch_reader, fetch_error};
use crate::odbc_source::_helpers::connect::connect;
use crate::odbc_source::_helpers::convert::{converted, output_schema};
use crate::odbc_source::_helpers::parameters::bound_parameters;
use crate::odbc_source::_helpers::query::select_query;
use crate::odbc_source::constants::BATCHES_AHEAD;

use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::{OdbcReadRequest, OdbcReader};

/// Run `request`'s `SELECT`, its parameters bound, on a blocking thread and pass its rows to `on_batch` in batches of at most `batch_rows`, each converted to the read columns' types, reading at most a few batches ahead; returns the row count. A text or binary value longer than the reader's cap fails the read instead of being cut.
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
        request.batch_rows,
        request.parameters.to_vec(),
    );
    let producer =
        tokio::task::spawn_blocking(move || fetch(&owned.0, (&query, &owned.2), owned.1, &sender));
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

/// Fetch `query`'s rows, with `parameters` bound to its placeholders as `bound_parameters` binds them, into `sender` until they end, the receiver goes away, or a fetch fails (sent on as the error). Decimal columns are fetched as text, which `converted` parses without rounding, since drivers round values to the scale they describe (an unbounded `numeric` as `NUMERIC(28,6)`).
fn fetch(
    reader: &OdbcReader,
    (query, parameters): (&str, &[BoundParameter]),
    batch_rows: usize,
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
    let batches = batch_reader(reader, cursor, (dbms, batch_rows))?;
    for batch in batches {
        let batch = batch.map_err(|error| fetch_error(&error.to_string()));
        let failed = batch.is_err();
        if sender.blocking_send(batch).is_err() || failed {
            break;
        }
    }
    Ok(())
}
