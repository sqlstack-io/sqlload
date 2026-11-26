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

/// An existing table's unique indexes on a merge key, as `KEY_INDEX_SQL` reads them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct KeyIndexes {
    /// How many unique indexes `ON CONFLICT` can use on exactly the key.
    pub(crate) count: i64,
    /// How many of them treat NULLs as equal (`NULLS NOT DISTINCT`).
    pub(crate) nulls_equal: i64,
    /// The names of the others, joined by `, `.
    pub(crate) others: String,
    /// The statements that drop the others, joined by `; `: `ALTER TABLE ... DROP CONSTRAINT` for one backing a primary key or unique constraint, `DROP INDEX` otherwise.
    pub(crate) drops: String,
    /// The key columns the table declares NOT NULL (as stored), which never hold NULL whatever the source says.
    pub(crate) not_null: Vec<String>,
}

/// A foreign key referencing a table, as `REFERENCES_SQL` lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReferenceRow {
    /// The referencing table, `schema.table`.
    pub(crate) referencing: String,
    pub(crate) constraint: String,
    /// Its `ON DELETE` action as `confdeltype` spells it (`c`, `n`, `d`, `r` or `a`).
    pub(crate) action: String,
    pub(crate) deferred: bool,
    /// The table references itself.
    pub(crate) own: bool,
}

/// One table's write session on Postgres: each batch is sent by binary `COPY` into the temporary staging table on the table's own session.
#[derive(Debug)]
pub struct PostgresWriter {
    pub destination: PostgresDestination,
}
