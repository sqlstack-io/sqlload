use std::path::Path;
use std::sync::Arc;

use crate::adapters::contract::models::TableName;
use crate::adapters::session::errors::SessionError;

/// One session on the caller's connection, SQLLoad's alone until its last handle drops: SQLLoad may set session parameters on it (Snowflake's `TIMEZONE`) and runs one table's load on it, holding that table's temporary tables and transactions; every call blocks, and SQLLoad makes them from a blocking thread.
pub trait CallerSession: Send + Sync + std::fmt::Debug {
    /// Run one statement in autocommit. SQLLoad sends its own `BEGIN`, `COMMIT` and `ROLLBACK`, which the session runs as written, so a transaction spans the statements between them.
    fn execute(&self, statement: &str) -> Result<(), SessionError>;

    /// Run a query and return its rows, each value as text and `None` for NULL.
    fn query_text(&self, query: &str) -> Result<Vec<Vec<Option<String>>>, SessionError>;

    /// Run `statement`, a Postgres `COPY ... FROM STDIN (FORMAT binary)`, with `data` as its input; Postgres sessions only.
    fn copy_in(&self, statement: &str, data: &[u8]) -> Result<(), SessionError> {
        let _ = (statement, data);
        Err(SessionError::new("this session cannot run COPY FROM STDIN"))
    }

    /// Load the zstd Parquet file at `file` into `table`, whose columns it names (BigQuery, ClickHouse and Databricks sessions, which load files through their own client); the table exists, and the load appends.
    fn load_parquet(&self, table: &TableName, file: &Path) -> Result<(), SessionError> {
        let _ = (table, file);
        Err(SessionError::new("this session cannot load Parquet files"))
    }
}

/// The caller's connection to the warehouse a target loads into, which opens SQLLoad's sessions on it.
pub trait CallerConnection: Send + Sync + std::fmt::Debug {
    /// A new session, in autocommit, whose current database is the one tables load into, never one the caller uses for other work at the same time; its last handle may drop on one of SQLLoad's async workers, so dropping it must not block for long.
    fn open(&self) -> Result<Arc<dyn CallerSession>, SessionError>;

    /// What the connection connects as, for the line a destination check prints, such as `alice as LOADER, warehouse LOAD_WH`.
    fn describe(&self) -> String;
}
