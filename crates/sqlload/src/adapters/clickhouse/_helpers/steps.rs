//! Statements as the runner carries them out: sent as they are, or a publish step it carries out itself.

use crate::adapters::clickhouse::_helpers::http::send;
use crate::adapters::clickhouse::constants::{EXCHANGE_UNSUPPORTED_CODES, PUBLISH_PREFIX};
use crate::adapters::clickhouse::models::{ClickHouseDestination, PublishStep};
use crate::http::errors::ApiError;

/// The publish step `statement` stands for, if it is one.
pub(crate) fn publish_step(statement: &str) -> Result<Option<PublishStep>, ApiError> {
    let Some(json) = statement.strip_prefix(PUBLISH_PREFIX) else {
        return Ok(None);
    };
    serde_json::from_str(json)
        .map(Some)
        .map_err(|error| ApiError::Protocol(format!("a malformed publish step: {error}")))
}

/// Run one statement, or the exchange it stands for; a partition publish here is a mistake, as it only follows from a write.
pub(crate) async fn run_step(
    destination: &ClickHouseDestination,
    statement: &str,
) -> Result<(), ApiError> {
    match publish_step(statement)? {
        None => send(destination, None, statement.as_bytes().to_vec())
            .await
            .map(|_| ()),
        Some(PublishStep::Exchange {
            target,
            build,
            aside,
        }) => exchange(destination, (&target, &build, &aside)).await,
        Some(PublishStep::Partitions { .. }) => Err(ApiError::Protocol(
            "a partition publish cannot run inside another".to_owned(),
        )),
    }
}

/// Swap `build` in for `target` with `EXCHANGE TABLES`, atomic in an `Atomic` database; where the database cannot exchange tables, by one `RENAME TABLE` moving `target` to `aside`, `build` to `target` and `aside` to `build`, so `build` holds the old table either way.
pub(crate) async fn exchange(
    destination: &ClickHouseDestination,
    (target, build, aside): (&str, &str, &str),
) -> Result<(), ApiError> {
    let exchanged = send(
        destination,
        None,
        format!("EXCHANGE TABLES {target} AND {build}").into_bytes(),
    )
    .await;
    match exchanged {
        Ok(_) => Ok(()),
        Err(error)
            if error
                .code()
                .is_some_and(|code| EXCHANGE_UNSUPPORTED_CODES.contains(&code)) =>
        {
            tracing::debug!(%error, "EXCHANGE TABLES is unsupported here; swapping by renames");
            send(
                destination,
                None,
                format!(
                    "RENAME TABLE {target} TO {aside}, {build} TO {target}, {aside} TO {build}"
                )
                .into_bytes(),
            )
            .await
            .map(|_| ())
        }
        Err(error) => Err(error),
    }
}
