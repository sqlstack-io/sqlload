/// Why reading from Postgres failed.
#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    #[error("cannot connect to Postgres at {address}: {source}")]
    Connect {
        address: String,
        source: tokio_postgres::Error,
    },
    #[error("Postgres query failed: {0}")]
    Query(#[from] tokio_postgres::Error),
    #[error("{table}.{column}: {reason}")]
    Value {
        table: String,
        column: String,
        reason: String,
    },
    #[error("malformed COPY data from {table}: {reason}")]
    Stream { table: String, reason: String },
    #[error("cannot build an Arrow batch for {table}: {reason}")]
    Batch { table: String, reason: String },
    #[error("{table}.{column} has type {postgres_type}, which cannot be read as {wanted}")]
    Unreadable {
        table: String,
        column: String,
        postgres_type: String,
        wanted: String,
    },
}
