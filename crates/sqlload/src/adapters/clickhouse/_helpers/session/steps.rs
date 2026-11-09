//! Statements as the runner carries them out: sent as they are, or a publish step it carries out itself.

use crate::adapters::clickhouse::_helpers::dialect::publish::{
    partition_rows_query, replace_partition,
};
use crate::adapters::clickhouse::_helpers::session::http::send;
use crate::adapters::clickhouse::constants::{
    EXCHANGE_UNSUPPORTED_CODES, LIGHTWEIGHT_DELETE_MODE, LIGHTWEIGHT_DELETE_REFUSED_CODE,
    PUBLISH_PREFIX,
};
use crate::adapters::clickhouse::models::{ClickHouseDestination, ClickHouseSql, PublishStep};
use crate::adapters::contract::types::SqlAdapter;
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

/// Run one statement, or the exchange, partition replace or row delete it stands for; a partition publish here is a mistake, as it only follows from a write.
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
        Some(PublishStep::ReplacePartition {
            target,
            copy,
            partition,
        }) => replace_or_drop(destination, (&target, &copy, &partition)).await,
        Some(PublishStep::DeleteRows { table, condition }) => {
            delete_rows(destination, (&table, &condition)).await
        }
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

/// Replace `target`'s `partition` with `copy`'s, or drop it when `copy` holds none of its rows, as ClickHouse 26.6 and later refuse to replace a partition from a table with no parts in it; either way one statement, so readers see the old partition or the new.
async fn replace_or_drop(
    destination: &ClickHouseDestination,
    (target, copy, partition): (&str, &str, &str),
) -> Result<(), ApiError> {
    let counted = send(
        destination,
        None,
        partition_rows_query(copy, partition).into_bytes(),
    )
    .await?;
    let statement = match counted.trim() {
        "0" => format!(
            "ALTER TABLE {target} DROP PARTITION ID {}",
            ClickHouseSql.render_loader_value_literal(partition)
        ),
        _ => replace_partition(target, copy, partition),
    };
    send(destination, None, statement.into_bytes())
        .await
        .map(|_| ())
}

/// Delete `table`'s rows matching `condition` with a lightweight `DELETE`, which only marks them (with [`LIGHTWEIGHT_DELETE_MODE`], and finished before it returns), or by rewriting their parts with `ALTER TABLE ... DELETE` where the table refuses one.
async fn delete_rows(
    destination: &ClickHouseDestination,
    (table, condition): (&str, &str),
) -> Result<(), ApiError> {
    let deleted = send(
        destination,
        None,
        format!(
            "DELETE FROM {table} WHERE {condition} SETTINGS {} = '{}', lightweight_deletes_sync = 2",
            LIGHTWEIGHT_DELETE_MODE.0, LIGHTWEIGHT_DELETE_MODE.1
        )
        .into_bytes(),
    )
    .await;
    match deleted {
        Ok(_) => Ok(()),
        Err(error) if error.code() == Some(LIGHTWEIGHT_DELETE_REFUSED_CODE) => {
            tracing::debug!(%error, "a lightweight DELETE is refused here; rewriting the parts");
            send(
                destination,
                None,
                format!("ALTER TABLE {table} DELETE WHERE {condition}").into_bytes(),
            )
            .await
            .map(|_| ())
        }
        Err(error) => Err(error),
    }
}
