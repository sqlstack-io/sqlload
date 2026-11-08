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
        Some(PublishStep::Exchange { target, build }) => {
            exchange(destination, &target, &build).await
        }
        Some(PublishStep::Partitions { .. }) => Err(ApiError::Protocol(
            "a partition publish cannot run inside another".to_owned(),
        )),
    }
}

/// Swap `build` in for `target` with `EXCHANGE TABLES`, so `build` holds the old table. A database that cannot exchange tables (an `Ordinary` one) is refused: the `RENAME TABLE` that could stand in is not atomic, and a server failing midway would leave no table.
pub(crate) async fn exchange(
    destination: &ClickHouseDestination,
    target: &str,
    build: &str,
) -> Result<(), ApiError> {
    send(
        destination,
        None,
        format!("EXCHANGE TABLES {target} AND {build}").into_bytes(),
    )
    .await
    .map(|_| ())
    .map_err(|error| match error.code() {
        Some(code) if EXCHANGE_UNSUPPORTED_CODES.contains(&code) => ApiError::Protocol(format!(
            "{target} is in a database that cannot swap tables atomically with EXCHANGE TABLES; load into an Atomic database (ClickHouse's default) instead ({error})"
        )),
        _ => error,
    })
}
