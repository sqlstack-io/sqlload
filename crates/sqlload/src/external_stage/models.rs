use crate::column_types::models::ColumnType;
use crate::files_source::models::{DataFile, SourceFormat};
use crate::project::models::MaskMethod;
use crate::snowflake_destination::models::StageRoute;
use crate::store_paths::models::Location;
use crate::write_sql::models::TableName;

/// Where one column of a direct `COPY` gets its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyValue {
    /// The file's column of the same name; `file_type` is the type the files hold it as (`None` when no sampled file has it).
    File { file_type: Option<ColumnType> },
    /// The file's value of this partition column, from its path.
    Partition,
    /// The file's full name, as the files source records it.
    SourceFile,
    /// The file's version (ETag, or size and modification time), as the files source records it.
    SourceVersion,
    /// The row's number in its file, from 1.
    SourceRow,
    /// A SQL expression, the same for every row (load metadata).
    Literal(String),
}

/// One column a direct `COPY` fills.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyColumn {
    pub name: String,
    pub column_type: ColumnType,
    pub value: CopyValue,
}

/// Everything one table's direct `COPY` statements need.
#[derive(Debug, Clone, Copy)]
pub struct DirectCopy<'a> {
    /// The table copied into: the target itself (`append`) or the load's staging table.
    pub target: &'a TableName,
    /// The stage, as SQL (`@"DB"."SCHEMA"."_SQLLOAD_STAGE"`).
    pub stage: &'a str,
    pub stage_url: &'a Location,
    /// The files source's folder.
    pub source: &'a Location,
    pub files: &'a [DataFile],
    pub columns: &'a [CopyColumn],
}

/// One `COPY` statement and the files it loads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyBatch {
    pub sql: String,
    /// The files' keys below the source folder.
    pub keys: Vec<String>,
}

/// What decides whether Snowflake can load a files source's files itself, exactly as SQLLoad would.
#[derive(Debug, Clone, Copy)]
pub struct DirectCheck<'a> {
    pub stage_url: &'a Location,
    pub source: &'a Location,
    pub format: SourceFormat,
    pub files: &'a [DataFile],
    pub columns: &'a [CopyColumn],
    /// Masked columns and how.
    pub masks: &'a [(String, MaskMethod)],
    /// The table is rewritten as a whole across several commits (`replace`, `--reload`), so a hashed column's staging table could not last one session.
    pub whole_table: bool,
    /// `shaping = "loader"`.
    pub shaped_by_loader: bool,
}

/// How SQLLoad finds a stage object, and what it must point at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageLookup {
    /// `SHOW STAGES LIKE ... IN SCHEMA ...`.
    pub query: String,
    /// The stage's name as `SHOW STAGES` reports it.
    pub name: String,
}

/// A stage as `SHOW STAGES` reports it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShownStage {
    pub name: String,
    pub url: String,
    pub storage_integration: String,
}

/// One step of the route chain: the route it would use, or why it cannot be used.
pub type StepOutcome = Result<StageRoute, String>;

/// The route plan chose for a table's rows, and why earlier steps of the chain were passed over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteChoice {
    pub route: StageRoute,
    pub passed_over: Vec<String>,
}
