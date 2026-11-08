//! Read a table or query through ODBC as Arrow batches.

use arrow_array::RecordBatch;
use arrow_odbc::OdbcReaderBuilder;
use tokio::sync::mpsc::Sender;

use crate::odbc_source::_helpers::connect::connect;
use crate::odbc_source::_helpers::convert::{converted, output_schema};
use crate::odbc_source::_helpers::query::select_query;
use crate::odbc_source::constants::BATCHES_AHEAD;
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::{OdbcReadRequest, OdbcReader};

/// Run `request`'s `SELECT` on a blocking thread and pass its rows to `on_batch` in batches of at most `batch_rows`, each converted to the read columns' types, reading at most a few batches ahead; returns the row count. A text or binary value longer than the reader's cap fails the read instead of being cut.
pub async fn read_rows(
    reader: &OdbcReader,
    request: &OdbcReadRequest<'_>,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, OdbcSourceError> {
    let schema = output_schema(request.columns).map_err(OdbcSourceError::Invalid)?;
    let query = select_query(request);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(BATCHES_AHEAD);
    let owned = (reader.clone(), request.batch_rows);
    let producer = tokio::task::spawn_blocking(move || fetch(&owned.0, &query, owned.1, &sender));
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

/// Fetch `query`'s rows into `sender` until they end, the receiver goes away, or a fetch fails (sent on as the error).
fn fetch(
    reader: &OdbcReader,
    query: &str,
    batch_rows: usize,
    sender: &Sender<Result<RecordBatch, OdbcSourceError>>,
) -> Result<(), OdbcSourceError> {
    let connection = connect(reader)?;
    let Some(cursor) = connection.execute(query, (), None)? else {
        return Err(OdbcSourceError::Invalid(
            "the read returned no result set".to_owned(),
        ));
    };
    let mut builder = OdbcReaderBuilder::new();
    builder
        .with_max_num_rows_per_batch(batch_rows)
        .with_fallibale_allocations(true);
    if let Some(size) = reader.limits.max_text_size {
        builder.with_max_text_size(size);
    }
    if let Some(size) = reader.limits.max_binary_size {
        builder.with_max_binary_size(size);
    }
    for batch in builder.build(cursor)? {
        let batch = batch.map_err(|error| {
            OdbcSourceError::Invalid(format!(
                "{error}; a value longer than its buffer fails the read rather than being cut, so raise max_text_size or max_binary_size on the source"
            ))
        });
        let failed = batch.is_err();
        if sender.blocking_send(batch).is_err() || failed {
            break;
        }
    }
    Ok(())
}
