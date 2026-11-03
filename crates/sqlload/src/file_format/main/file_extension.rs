//! The staged file name extension.

use crate::config::models::FileFormat;
use crate::file_format::constants::{JSON_EXTENSION, PARQUET_EXTENSION};

/// The file name extension for `format`.
pub fn file_extension(format: FileFormat) -> &'static str {
    match format {
        FileFormat::Json => JSON_EXTENSION,
        FileFormat::Parquet => PARQUET_EXTENSION,
    }
}
