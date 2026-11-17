//! Run reads several at once and take their results in order.

use std::future::Future;
use std::sync::Arc;

use futures_util::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Run `read(index)` for each index in `0..count`, up to `limit` at once, and pass each result to `consume` in index order as soon as it and every result before it are in. At most `limit` results are held at once, counting the reads running, those waiting for their turn and the one `consume` works on: a read starts, in index order, only once the result `limit` places before it is consumed, so a running read never waits on `consume`. The first error, from a read or from `consume`, ends it: no result after a failed read is consumed, and reads still running are dropped.
pub async fn read_in_order<T, E, Read, Reading>(
    (count, limit): (u64, usize),
    mut read: Read,
    mut consume: impl AsyncFnMut(T) -> Result<(), E>,
) -> Result<(), E>
where
    Read: FnMut(u64) -> Reading,
    Reading: Future<Output = Result<T, E>>,
{
    let limit = limit.max(1);
    let permits = Arc::new(Semaphore::new(limit));
    let (sender, mut receiver) =
        tokio::sync::mpsc::channel::<(Result<T, E>, Option<OwnedSemaphorePermit>)>(limit);
    let produce = async move {
        let mut reads = std::pin::pin!(
            futures_util::stream::iter(0..count)
                .then(|index| {
                    let permits = Arc::clone(&permits);
                    async move { (index, permits.acquire_owned().await.ok()) }
                })
                .map(|(index, permit)| {
                    let reading = read(index);
                    async move { (reading.await, permit) }
                })
                .buffered(limit)
        );
        while let Some((result, permit)) = reads.next().await {
            let failed = result.is_err();
            if sender.send((result, permit)).await.is_err() || failed {
                break;
            }
        }
    };
    let take = async move {
        while let Some((result, permit)) = receiver.recv().await {
            consume(result?).await?;
            drop(permit);
        }
        Ok(())
    };
    let (mut produce, mut take) = (std::pin::pin!(produce), std::pin::pin!(take));
    let mut produced = false;
    loop {
        tokio::select! {
            () = &mut produce, if !produced => produced = true,
            taken = &mut take => break taken,
        }
    }
}
