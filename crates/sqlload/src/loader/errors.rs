use std::error::Error;
use std::io::ErrorKind;

#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::files::errors::FilesDestinationError;
use crate::file_store::errors::StoreError;
use crate::files_source::errors::FilesError;
use crate::http::constants::FIRST_SERVER_ERROR;
use crate::http::errors::ApiError;
use crate::loader::constants::{TRANSIENT_SERVER_CODES, TRANSIENT_SQLSTATES, TRANSIENT_STATUSES};
use crate::postgres_source::errors::PostgresError;
use crate::python_sources::errors::ReadError;
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::table_lease::errors::LeaseError;

/// Why planning or loading failed outside any one table's own problems.
#[derive(Debug, thiserror::Error)]
pub enum LoaderError {
    #[error(transparent)]
    Postgres(#[from] PostgresError),
    #[cfg(feature = "duckdb")]
    #[error(transparent)]
    DuckDb(#[from] DuckDbError),
    #[error(transparent)]
    Snowflake(#[from] ApiError),
    #[error(transparent)]
    Files(#[from] FilesError),
    #[error(transparent)]
    WarehouseSource(#[from] SnowflakeSourceError),
    #[error(transparent)]
    BigQuerySource(#[from] crate::bigquery_source::errors::BigQuerySourceError),
    #[cfg(feature = "odbc")]
    #[error(transparent)]
    Odbc(#[from] crate::odbc_source::errors::OdbcSourceError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    FilesDestination(#[from] FilesDestinationError),
    #[error(transparent)]
    Lease(#[from] LeaseError),
    #[error("{0}")]
    Invalid(String),
    /// A Python table's read failed.
    #[error(transparent)]
    Read(#[from] ReadError),
    /// A failure a rerun may pass: another run holds the table's lease, or the table's session dropped mid-load.
    #[error("{0}")]
    Transient(String),
    /// A table gained a column since the load was planned: nothing of the batch was written, and a new plan streams it.
    #[error("{0}")]
    Replan(String),
}

impl LoaderError {
    /// True when running the load again unchanged may succeed: a dropped, refused or timed-out connection, throttling or a server error that outlasted its retries, a table another run is loading, or a column that appeared mid-load. Problems with the configuration, the data, credentials or permissions are not.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Postgres(
                PostgresError::Connect { source, .. } | PostgresError::Query(source),
            ) => Self::postgres_transient(source),
            Self::Snowflake(error)
            | Self::WarehouseSource(SnowflakeSourceError::Api(error))
            | Self::BigQuerySource(crate::bigquery_source::errors::BigQuerySourceError::Api(
                error,
            )) => Self::api_transient(error),
            Self::Store(store)
            | Self::Files(FilesError::Store(store))
            | Self::WarehouseSource(SnowflakeSourceError::Store(store))
            | Self::FilesDestination(FilesDestinationError::Store(store))
            | Self::Lease(LeaseError::Store(store)) => match store {
                StoreError::Open { source, .. } | StoreError::Object { source, .. } => {
                    Self::store_transient(source)
                }
            },
            Self::Read(error) => error.retryable,
            Self::Transient(_) | Self::Replan(_) => true,
            other => Self::transient_cause(other),
        }
    }

    /// Whether `error`, or anything it was caused by, is a dropped, refused or timed-out connection.
    fn transient_cause(error: &(dyn Error + 'static)) -> bool {
        let mut cause = Some(error);
        while let Some(current) = cause {
            if let Some(io) = current.downcast_ref::<std::io::Error>()
                && matches!(
                    io.kind(),
                    ErrorKind::ConnectionRefused
                        | ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::NotConnected
                        | ErrorKind::BrokenPipe
                        | ErrorKind::TimedOut
                        | ErrorKind::UnexpectedEof
                        | ErrorKind::HostUnreachable
                        | ErrorKind::NetworkUnreachable
                        | ErrorKind::NetworkDown
                )
            {
                return true;
            }
            if let Some(http) = current.downcast_ref::<reqwest::Error>()
                && (http.is_timeout() || http.is_connect())
            {
                return true;
            }
            cause = current.source();
        }
        false
    }

    /// Whether a Postgres error may pass on a retry: the connection dropped or timed out, or the server answered with a connection, resource, shutdown, serialization or deadlock SQLSTATE; a server that refused the login or the SQL is not.
    fn postgres_transient(error: &tokio_postgres::Error) -> bool {
        match error.as_db_error() {
            Some(answer) => {
                let code = answer.code().code();
                TRANSIENT_SQLSTATES
                    .iter()
                    .any(|prefix| code.starts_with(prefix))
            }
            None => error.is_closed() || Self::transient_cause(error),
        }
    }

    /// Whether an object store failure may pass on a retry: its own retries ran out on a dropped or timed-out connection.
    fn store_transient(error: &object_store::Error) -> bool {
        match error {
            object_store::Error::Generic { source, .. } => Self::transient_cause(source.as_ref()),
            _ => false,
        }
    }

    /// Whether a warehouse or object store call may pass on a rerun: the connection failed, the service throttled or was unavailable, or it answered 500 with no code or a transient one.
    fn api_transient(error: &ApiError) -> bool {
        match error {
            ApiError::Transport(_) => true,
            ApiError::Status { status, code, .. } => {
                TRANSIENT_STATUSES.contains(status)
                    || (*status >= FIRST_SERVER_ERROR
                        && (code.is_empty() || TRANSIENT_SERVER_CODES.contains(&code.as_str())))
            }
            ApiError::Protocol(_) | ApiError::Auth(_) => false,
        }
    }
}
