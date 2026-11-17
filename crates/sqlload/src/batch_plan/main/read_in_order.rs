//! Run reads several at once and take their results in order.

use std::future::Future;

use futures_util::StreamExt;

/// Run `read(index)` for each index in `0..count`, up to `limit` at once, and pass each result to `consume` in index order as soon as it and every result before it are in; reads go on while `consume` works, at most `limit` results ahead of it. The first error, from a read or from `consume`, ends it: no result after a failed read is consumed, and reads still running are dropped.
pub async fn read_in_order<T, E, Read, Reading>(
    (count, limit): (u64, usize),
    read: Read,
    mut consume: impl AsyncFnMut(T) -> Result<(), E>,
) -> Result<(), E>
where
    Read: FnMut(u64) -> Reading,
    Reading: Future<Output = Result<T, E>>,
{
    let limit = limit.max(1);
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Result<T, E>>(limit);
    let produce = async move {
        let mut reads = std::pin::pin!(
            futures_util::stream::iter(0..count)
                .map(read)
                .buffered(limit)
        );
        while let Some(result) = reads.next().await {
            let failed = result.is_err();
            if sender.send(result).await.is_err() || failed {
                break;
            }
        }
    };
    let take = async move {
        while let Some(result) = receiver.recv().await {
            consume(result?).await?;
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
