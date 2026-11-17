//! End a shared snapshot.

use tokio_postgres::Client;

use crate::postgres_source::errors::PostgresError;

/// Commit a shared snapshot on every one of `readers`, each one even after a failure; the first error wins.
pub async fn end_snapshot(readers: &[&Client]) -> Result<(), PostgresError> {
    let mut ended: Result<(), PostgresError> = Ok(());
    for reader in readers {
        let result = reader
            .batch_execute("COMMIT")
            .await
            .map_err(PostgresError::from);
        ended = ended.and(result);
    }
    ended
}
