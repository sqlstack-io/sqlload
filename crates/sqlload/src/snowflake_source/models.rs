use crate::postgres_source::models::{ReadColumn, SourceTable};
use crate::project::models::UnloadStage;
use crate::sql_api::models::SqlClient;
use crate::stage_api::models::DriverSession;

/// A connected Snowflake source: SQL API statements, a driver session for `GET`, and where it unloads.
#[derive(Debug)]
pub struct SnowflakeReader {
    pub(crate) sql: SqlClient,
    pub(crate) driver: DriverSession,
    pub(crate) http: reqwest::Client,
    pub(crate) stage: Option<UnloadStage>,
}

/// What one unload reads: a table or a query, its columns, and the rows' filter.
#[derive(Debug, Clone, Copy)]
pub struct UnloadRequest<'a> {
    pub table: &'a SourceTable,
    /// The query whose rows are the table; `None` reads the table itself.
    pub sql: Option<&'a str>,
    pub columns: &'a [ReadColumn],
    /// A condition on the source's columns, already rendered.
    pub filter: Option<&'a str>,
    pub batch_rows: usize,
}

/// Where one unload's files go, and how SQLLoad reads them back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UnloadFolder {
    /// An internal stage folder such as `@~/sqlload_unload/<run>/`, fetched with `GET`.
    Internal { stage_path: String },
    /// An S3 folder: `COPY INTO` this target (with `options` after its `FROM`), then read the files from `folder`.
    External {
        copy_target: String,
        options: String,
        folder: crate::store_paths::models::Location,
    },
}
