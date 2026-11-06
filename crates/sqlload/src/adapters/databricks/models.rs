use std::sync::Arc;

use crate::databricks_api::models::DatabricksClient;
use crate::loader::errors::LoaderError;
use crate::parquet_parts::models::PartWriter;

/// Databricks SQL's dialect.
#[derive(Debug, Clone, Copy, Default)]
pub struct DatabricksSql;

/// An authenticated Databricks workspace and SQL warehouse, sharing one HTTP pool.
#[derive(Debug, Clone)]
pub struct DatabricksDestination {
    pub api: DatabricksClient,
    pub catalog: String,
    /// The staging volume's path, `/Volumes/catalog/schema/volume`.
    pub volume: String,
    /// Held while a load's tables change the schema, one at a time.
    pub ddl: Arc<tokio::sync::Mutex<()>>,
}

/// One table's write session on Databricks: Parquet parts uploaded to the volume, then loaded by `COPY INTO`.
#[derive(Debug)]
pub struct DatabricksWriter {
    pub destination: DatabricksDestination,
    /// The batch's Parquet being written, and the names of its files uploaded so far.
    pub parts: PartWriter,
    pub staged: Vec<String>,
    /// The batch's files still uploading, a few at a time, while the next ones are read and encoded.
    pub uploads: tokio::task::JoinSet<Result<(), LoaderError>>,
}

/// One load folder of `<volume>/sqlload` as listed: its batch folders and every file under it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListedLoad {
    pub folder: String,
    pub batches: Vec<String>,
    pub files: Vec<crate::databricks_api::models::DirectoryEntry>,
}

/// What stale-folder cleanup removes: files known from the listing to be old, then folders, deepest first, each removed only if it is empty by then.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StaleRemovals {
    pub files: Vec<String>,
    pub folders: Vec<String>,
}
