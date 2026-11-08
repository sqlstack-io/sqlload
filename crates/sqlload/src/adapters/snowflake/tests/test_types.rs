use crate::adapters::snowflake::models::CopyColumn;
use crate::files_source::models::SourceFormat;
use crate::project::models::MaskMethod;

pub(crate) struct CopyStagedTestCase {
    pub(crate) description: &'static str,
    pub(crate) files: usize,
    /// How many statements, and the start of the first.
    pub(crate) expected_copies: (usize, &'static str),
}

pub(crate) struct StaleFoldersTestCase {
    pub(crate) description: &'static str,
    /// Each staged file's name and `last_modified`, as `LIST` shows them.
    pub(crate) listed: &'static [(&'static str, &'static str)],
    pub(crate) expected_folders: &'static [&'static str],
}

pub(crate) struct SelectingCopyTestCase {
    pub(crate) description: &'static str,
    pub(crate) columns: Vec<(&'static str, crate::column_types::models::ColumnType)>,
    pub(crate) expected_copy: &'static str,
}

pub(crate) struct StageStatementTestCase {
    pub(crate) description: &'static str,
    /// Builds the statements under test, joined by `;\n`.
    pub(crate) build: fn() -> String,
    pub(crate) expected_sql: &'static str,
}

/// Where a direct-copy case's files live and where Snowflake reads them.
pub(crate) struct CopyFrom {
    /// The source folder below `s3://lake/`.
    pub(crate) source: &'static str,
    pub(crate) stage: &'static str,
    /// The folder below the stage the files are read from.
    pub(crate) folder: &'static str,
    /// What `METADATA$FILENAME` starts with for these files.
    pub(crate) filename_prefix: &'static str,
}

pub(crate) struct DirectCopiesTestCase {
    pub(crate) description: &'static str,
    /// File keys below the source folder, each with its `day` partition value.
    pub(crate) files: Vec<(String, &'static str)>,
    pub(crate) columns: Vec<CopyColumn>,
    pub(crate) from: CopyFrom,
    pub(crate) encoding: crate::files_source::models::FilesEncoding,
    /// Every file's CSV column names, as `batch_headers` reads them.
    pub(crate) header: &'static [&'static str],
    /// The number of statements and text the first holds (or the error text).
    pub(crate) expected_copies: Result<(usize, &'static str), &'static str>,
}

pub(crate) struct DirectProblemTestCase {
    pub(crate) description: &'static str,
    /// The source folder below `s3://lake/`.
    pub(crate) source: &'static str,
    pub(crate) format: SourceFormat,
    pub(crate) keys: &'static [&'static str],
    pub(crate) columns: Vec<CopyColumn>,
    pub(crate) masks: Vec<(String, MaskMethod)>,
    pub(crate) refused: Vec<(String, String)>,
    /// Copied unchanged to an internal stage (`copy_to_internal_stage`) instead of read under the stage.
    pub(crate) copied: bool,
    /// Each record loads whole (`shape = "record"`).
    pub(crate) whole: bool,
    pub(crate) expected_problem: Option<&'static str>,
}

pub(crate) struct StageProblemTestCase {
    pub(crate) description: &'static str,
    /// The stage as `SHOW STAGES` lists it: name, URL, storage integration.
    pub(crate) shown: &'static [(&'static str, &'static str, &'static str)],
    pub(crate) expected_problem: Option<&'static str>,
}

pub(crate) struct HashParityTestCase {
    pub(crate) description: &'static str,
    pub(crate) value: &'static str,
    /// The lowercase hex SHA-256 of the value's UTF-8 bytes, as Snowflake's `SHA2(value, 256)` returns it.
    pub(crate) expected_digest: &'static str,
}

pub(crate) struct StepFailureTestCase {
    pub(crate) description: &'static str,
    pub(crate) error: crate::http::errors::ApiError,
    /// Whether the chain moves on past the step.
    pub(crate) expected_unusable: bool,
}
