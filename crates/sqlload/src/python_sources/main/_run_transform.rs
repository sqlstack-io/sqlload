//! Run a `transform` function on one file's or range's rows, passing on each batch it returns or yields as it crosses.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use tokio::sync::mpsc::Sender;

use crate::python_sources::models::{TransformCall, TransformRan};
use crate::python_sources::types::{PythonHost, Streamed};

/// Call `call`'s function on `rows` (in `schema`) on a blocking thread and pass each batch it returned or yielded to `on_batch` as it crosses, one batch waiting at most, so a generator's output never piles up; how the call went, or why it failed (its exception, or what it returned cannot load). An error of `on_batch` stops the call and is returned as it is.
pub(in crate::python_sources) async fn run_transform<E>(
    host: Arc<dyn PythonHost>,
    call: TransformCall,
    (rows, schema): (Vec<RecordBatch>, SchemaRef),
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), E>,
) -> Result<Result<TransformRan, String>, E> {
    let rows_in = rows.iter().map(RecordBatch::num_rows).sum::<usize>() as u64;
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<RecordBatch>(1);
    let crossing =
        tokio::task::spawn_blocking(move || cross(host.as_ref(), &call, (rows, schema), &sender));
    while let Some(batch) = receiver.recv().await {
        if let Err(error) = on_batch(batch).await {
            drop(receiver);
            let _ = crossing.await;
            return Err(error);
        }
    }
    let crossed = match crossing.await {
        Ok(crossed) => crossed,
        Err(error) => Err(format!("the transform's thread failed: {error}")),
    };
    Ok(crossed.map(|(dicts, spent)| TransformRan {
        dicts,
        rows_in,
        seconds: spent.as_secs_f64(),
    }))
}

/// Call the function and send each batch on until it ends or nothing takes them: whether it gave dicts, and the time spent other than waiting for a batch to be taken.
fn cross(
    host: &dyn PythonHost,
    call: &TransformCall,
    (rows, schema): (Vec<RecordBatch>, SchemaRef),
    sender: &Sender<RecordBatch>,
) -> Result<(bool, Duration), String> {
    let started = Instant::now();
    let mut waited = Duration::ZERO;
    let mut stream = host.transform(call, rows, schema)?;
    while let Some(next) = stream.next_batch() {
        if let Streamed::Batch(batch) = next.map_err(|error| error.message)? {
            let waiting = Instant::now();
            if sender.blocking_send(batch).is_err() {
                break;
            }
            waited += waiting.elapsed();
        }
    }
    Ok((
        stream.crossed_dicts(),
        started.elapsed().saturating_sub(waited),
    ))
}
