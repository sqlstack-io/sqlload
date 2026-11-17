//! The tasks downloading a result's files, each decoded into Arrow batches.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinSet;

use crate::databricks_api::main::download_link::download_link;
use crate::databricks_api::main::result_chunk::result_chunk;
use crate::databricks_api::models::{ArrowResult, DatabricksClient, ResultLink};
use crate::databricks_source::constants::{BATCHES_AHEAD, DOWNLOAD_ATTEMPTS, LINKS_AT_ONCE};
use crate::databricks_source::errors::DatabricksSourceError;

/// One task per chunk of `result`, at most [`LINKS_AT_ONCE`] downloading at a time, each sending its file's batches on the returned channel, which holds at most [`BATCHES_AHEAD`]; a task keeps its download slot until its batches are taken, so memory stays bounded.
pub(crate) fn download_chunks(
    client: &DatabricksClient,
    result: ArrowResult,
) -> (
    JoinSet<Result<(), DatabricksSourceError>>,
    Receiver<RecordBatch>,
) {
    let (sender, receiver) = channel::<RecordBatch>(BATCHES_AHEAD);
    let slots = Arc::new(Semaphore::new(LINKS_AT_ONCE));
    let statement = Arc::new(result.statement_id);
    let mut chunks: JoinSet<Result<(), DatabricksSourceError>> = JoinSet::new();
    for index in 0..result.chunk_count {
        let link = result
            .links
            .iter()
            .find(|link| link.chunk_index == index)
            .cloned();
        let (client, slots, statement, sender) = (
            client.clone(),
            slots.clone(),
            statement.clone(),
            sender.clone(),
        );
        chunks.spawn(async move {
            let _slot = slots
                .acquire_owned()
                .await
                .map_err(|error| DatabricksSourceError::Invalid(error.to_string()))?;
            read_chunk(&client, (&statement, index), link, &sender).await
        });
    }
    (chunks, receiver)
}

/// Download chunk `index` (through `link`, or a link fetched for it) and send its batches; a failed download is tried again through a fresh link, up to [`DOWNLOAD_ATTEMPTS`] in all.
async fn read_chunk(
    client: &DatabricksClient,
    (statement, index): (&str, usize),
    link: Option<ResultLink>,
    sender: &Sender<RecordBatch>,
) -> Result<(), DatabricksSourceError> {
    let mut link = link;
    let mut attempt = 1;
    let body = loop {
        let current = match link.take() {
            Some(link) => link,
            None => result_chunk(client, (statement, index)).await?,
        };
        match download_link(client, &current).await {
            Ok(body) => break body,
            Err(error) if attempt < DOWNLOAD_ATTEMPTS => {
                tracing::debug!(%error, attempt, chunk = index, "downloading a Databricks result file again");
                attempt += 1;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let invalid = |error: arrow_schema::ArrowError| {
        DatabricksSourceError::Invalid(format!("result chunk {index}: {error}"))
    };
    for batch in StreamReader::try_new(Cursor::new(body), None).map_err(invalid)? {
        sender.send(batch.map_err(invalid)?).await.map_err(|_| {
            DatabricksSourceError::Invalid("the reader stopped taking batches".to_owned())
        })?;
    }
    Ok(())
}
