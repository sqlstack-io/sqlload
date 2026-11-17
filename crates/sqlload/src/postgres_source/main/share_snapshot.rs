//! Open one read-only snapshot on several connections.

use tokio_postgres::Client;

use crate::postgres_source::constants::SPLIT_BEGIN;
use crate::postgres_source::errors::PostgresError;

/// Open a read-only snapshot transaction on the first of `readers` and the same snapshot on the others, so their reads see one state of the database; [`end_snapshot`](crate::postgres_source::main::end_snapshot::end_snapshot) commits it.
pub async fn share_snapshot(readers: &[&Client]) -> Result<(), PostgresError> {
    let Some((first, others)) = readers.split_first() else {
        return Ok(());
    };
    first.batch_execute(SPLIT_BEGIN).await?;
    let snapshot: String = first
        .query_one("SELECT pg_export_snapshot()", &[])
        .await?
        .get(0);
    for other in others {
        other
            .batch_execute(&format!(
                "{SPLIT_BEGIN}; SET TRANSACTION SNAPSHOT '{snapshot}'"
            ))
            .await?;
    }
    Ok(())
}
