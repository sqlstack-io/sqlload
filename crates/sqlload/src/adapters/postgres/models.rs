use std::sync::Arc;

use crate::project::models::ProjectConfig;

/// Postgres's SQL dialect.
#[derive(Debug, Clone, Copy, Default)]
pub struct PostgresSql;

/// A Postgres target: where it connects, and its session, opened on first use.
#[derive(Debug, Clone)]
pub struct PostgresDestination {
    /// The project, whose Postgres connection `connection` names how to connect.
    pub project: Arc<ProjectConfig>,
    pub connection: String,
    /// The database to connect to instead of the connection's (the target's `database`).
    pub database: Option<String>,
    /// The session: shared by planning, and one of its own for each table's load, whose temporary staging table lives in it.
    pub session: Arc<
        tokio::sync::Mutex<
            Option<(
                tokio_postgres::Client,
                crate::postgres_source::models::Encryption,
            )>,
        >,
    >,
    /// Whether a closed session is opened again: the shared one is; a table's own is not, since its temporary staging table went with it.
    pub reconnects: bool,
    /// Held while a load's tables change the schema, one at a time.
    pub ddl: Arc<tokio::sync::Mutex<()>>,
}

/// One table's write session on Postgres: each batch is sent by binary `COPY` into the temporary staging table on the table's own session.
#[derive(Debug)]
pub struct PostgresWriter {
    pub destination: PostgresDestination,
}
