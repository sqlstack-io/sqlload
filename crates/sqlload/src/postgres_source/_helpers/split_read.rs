//! Read one table over several connections in `ctid` page ranges, all in one exported snapshot.

use tokio_postgres::Client;

use crate::postgres_source::constants::{SPLIT_PAGES_SQL, SPLIT_READ_MIN_PAGES};
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::main::share_snapshot::share_snapshot;
use crate::postgres_source::models::CopyRequest;

/// The `ctid` ranges splitting the read over `request.helpers` and the client, the last one open-ended; `None` to read in one pass (no helpers, an ordered read, not a plain table, Postgres before 14, or too small to gain).
async fn page_ranges(
    client: &Client,
    request: &CopyRequest<'_>,
) -> Result<Option<Vec<String>>, PostgresError> {
    if request.helpers.is_empty() || request.order_by.is_some() {
        return Ok(None);
    }
    let row = client
        .query_opt(
            SPLIT_PAGES_SQL,
            &[&request.table.schema, &request.table.name],
        )
        .await?;
    let Some((true, pages)) = row.map(|row| (row.get::<_, bool>(0), row.get::<_, i64>(1))) else {
        return Ok(None);
    };
    let readers = request.helpers.len() as i64 + 1;
    if pages < SPLIT_READ_MIN_PAGES.max(readers) {
        return Ok(None);
    }
    let step = pages / readers;
    Ok(Some(
        (0..readers)
            .map(|index| match index + 1 == readers {
                true => format!("ctid >= '({},0)'::tid", index * step),
                false => format!(
                    "ctid >= '({},0)'::tid AND ctid < '({},0)'::tid",
                    index * step,
                    (index + 1) * step
                ),
            })
            .collect(),
    ))
}

/// `request.filter` narrowed to each range, or the filter alone when the read is not split.
fn range_filters(request: &CopyRequest<'_>, ranges: Option<&Vec<String>>) -> Vec<Option<String>> {
    let Some(ranges) = ranges else {
        return vec![request.filter.clone()];
    };
    ranges
        .iter()
        .map(|range| {
            Some(
                request
                    .filter
                    .as_ref()
                    .map_or_else(|| range.clone(), |filter| format!("({filter}) AND {range}")),
            )
        })
        .collect()
}

/// The readers for `request` (the client, plus helpers for a split read) and each one's filter; a split read's readers share one snapshot, and one that cannot share it falls back to the client alone.
pub(crate) async fn split_readers<'a>(
    client: &'a Client,
    request: &CopyRequest<'a>,
) -> Result<(Vec<&'a Client>, Vec<Option<String>>), PostgresError> {
    let mut ranges = page_ranges(client, request).await?;
    let helpers: Vec<&Client> = request
        .helpers
        .iter()
        .take(ranges.as_ref().map_or(0, |ranges| ranges.len() - 1))
        .collect();
    let readers: Vec<&Client> = std::iter::once(client).chain(helpers).collect();
    if ranges.is_some() && share_snapshot(&readers).await.is_err() {
        for helper in readers.iter().skip(1) {
            let _ = helper.batch_execute("ROLLBACK").await;
        }
        client.batch_execute("ROLLBACK").await?;
        ranges = None;
    }
    let readers: Vec<&Client> = readers
        .into_iter()
        .take(ranges.as_ref().map_or(1, Vec::len))
        .collect();
    Ok((readers, range_filters(request, ranges.as_ref())))
}
