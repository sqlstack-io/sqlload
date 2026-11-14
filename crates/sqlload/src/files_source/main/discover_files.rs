//! List a files source and work out its schema from Parquet footers or sampled JSON and CSV records.

use crate::file_store::models::OpenFolder;
use crate::files_source::_helpers::plan::folder::{listed_files, listed_table};
use crate::files_source::errors::FilesError;
use crate::files_source::models::{FilesEncoding, FilesTable, PathValues};

/// The `encoding` files under `folder` with their path `values`, and the schema of the newest ones without the loader's own columns, which this load fills; when a file's path lacks a value, only the listing and its problems, before any file is read.
pub async fn discover_files(
    folder: &OpenFolder,
    values: &PathValues,
    encoding: &FilesEncoding,
) -> Result<FilesTable, FilesError> {
    let listed = listed_files(folder, values, encoding).await?;
    listed_table(folder, listed, values, encoding).await
}
