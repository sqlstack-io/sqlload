//! Run SQL statements in order.

use crate::adapters::clickhouse::_helpers::dialect::publish::{
    PartitionPublish, delta_partitions_query, displaced_partitions_query, lightweight_delete_query,
    partition_statements,
};
use crate::adapters::clickhouse::_helpers::session::steps::{publish_step, run_step};
use crate::adapters::clickhouse::main::session::_query_rows::query_rows;
use crate::adapters::clickhouse::models::{ClickHouseDestination, PublishStep};
use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT};
use crate::http::errors::ApiError;

/// Run `statements` one after another, each committing on its own (ClickHouse has no transactions, so `BEGIN` and `COMMIT` are refused); a publish step is carried out by asking the server which partitions it touches and whether it can pin a lightweight `DELETE`, then running the statements that swap them in (the copy they are rebuilt in is dropped if one fails).
pub(in crate::adapters) async fn run_statements(
    destination: &ClickHouseDestination,
    statements: &[String],
) -> Result<(), ApiError> {
    if let Some(statement) = statements.iter().find(|statement| {
        [BEGIN_STATEMENT, COMMIT_STATEMENT, "ROLLBACK"]
            .contains(&statement.trim().to_ascii_uppercase().as_str())
    }) {
        return Err(ApiError::Protocol(format!(
            "{statement} was sent to ClickHouse, which has no transactions"
        )));
    }
    for statement in statements {
        let Some(PublishStep::Partitions {
            target,
            delta,
            copy,
            aside,
            displaced,
        }) = publish_step(statement)?
        else {
            run_step(destination, statement).await?;
            continue;
        };
        let first_values = |query: String| async move {
            let rows = query_rows(destination, &query).await?;
            Ok::<Vec<String>, ApiError>(
                rows.into_iter()
                    .filter_map(|row| row.into_iter().next().flatten())
                    .collect(),
            )
        };
        let new = first_values(delta_partitions_query(&delta)).await?;
        let (replaced, lightweight) = match &displaced {
            Some(condition) => (
                first_values(displaced_partitions_query(&target, condition)).await?,
                !first_values(lightweight_delete_query()).await?.is_empty(),
            ),
            None => (Vec::new(), false),
        };
        let publish = PartitionPublish {
            target: &target,
            delta: &delta,
            copy: &copy,
            aside: &aside,
            displaced: displaced.as_deref(),
            lightweight,
        };
        for step in partition_statements(&publish, (&new, &replaced)) {
            if let Err(error) = run_step(destination, &step).await {
                let drop = format!("DROP TABLE IF EXISTS {copy}");
                if let Err(cleanup) = run_step(destination, &drop).await {
                    tracing::warn!(error = %cleanup, "cannot drop the partition copy");
                }
                return Err(error);
            }
        }
    }
    Ok(())
}
