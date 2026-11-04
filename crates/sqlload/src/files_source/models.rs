use crate::column_types::models::ColumnType;
use crate::file_store::models::StoredFile;
use crate::schema_plan::models::DesiredColumn;

/// A data file and its partition values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFile {
    pub file: StoredFile,
    /// Values of the source's declared partition columns, from the file's `key=value` folders.
    pub partitions: Vec<(String, String)>,
}

/// One column in the files, and the portable type it maps to (or why it has none).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileColumn {
    pub name: String,
    pub column_type: Result<ColumnType, String>,
}

/// What a files source holds: its data files and their combined schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesTable {
    /// Every data file, sorted by key.
    pub files: Vec<DataFile>,
    /// Columns in the files, in first-seen order (newest file first).
    pub columns: Vec<FileColumn>,
    /// Problems found while combining footers (a column with different types in different files).
    pub problems: Vec<String>,
}

/// Which files to read and the columns to produce, in table order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesRead {
    pub files: Vec<DataFile>,
    /// Data columns, partition columns and the source-file column, as the table has them.
    pub columns: Vec<DesiredColumn>,
    pub batch_rows: usize,
}

/// A source file already in the destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedFile {
    pub name: String,
    /// The version it was loaded at; empty for rows loaded before versions were recorded.
    pub version: String,
}

/// What decides which files a load reads.
#[derive(Debug, Clone, Copy)]
pub struct FileSelection<'a> {
    pub files: &'a [DataFile],
    /// The folder as it prints, the prefix of every recorded source file name.
    pub location: &'a str,
    /// Source files already in the destination, with their versions (empty when unknown).
    pub loaded: &'a [LoadedFile],
    /// The partition column the cursor is on, and its type.
    pub cursor: Option<(&'a str, &'a ColumnType)>,
    pub from: Option<&'a str>,
    pub to: Option<&'a str>,
}
