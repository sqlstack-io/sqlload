use crate::postgres_source::_helpers::server_reason::server_reason;

/// Why reading from Postgres failed.
#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    #[error("cannot connect to Postgres at {address}: {}", server_reason(.source))]
    Connect {
        address: String,
        source: tokio_postgres::Error,
    },
    #[error(transparent)]
    Ssh(#[from] crate::ssh_tunnel::errors::SshError),
    #[error("cannot set up TLS for Postgres at {address}: {reason}")]
    Tls { address: String, reason: String },
    #[error("Postgres query failed: {}", server_reason(.0))]
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
    /// What the caller's batch consumer failed with, as it said it.
    #[error("{0}")]
    Consumer(String),
    #[error("{table}.{column} has type {postgres_type}, which cannot be read as {wanted}")]
    Unreadable {
        table: String,
        column: String,
        postgres_type: String,
        wanted: String,
    },
}
