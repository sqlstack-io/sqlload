//! The COPY statement that loads staged files into a table.

use crate::config::models::{Config, FileFormat};
use crate::file_format::_helpers::load_select::load_select;
use crate::file_format::constants::{JSON_FILE_FORMAT, PARQUET_FILE_FORMAT};

/// `COPY INTO table FROM source`: by column name for JSON, through the envelope's `SELECT` for Parquet; `files` is a leading-space `FILES = (...)` clause or empty.
pub fn copy_into(config: &Config, table: &str, source: &str, files: &str) -> String {
    match config.staging.format {
        FileFormat::Json => format!(
            "COPY INTO {table} FROM {source}{files} {JSON_FILE_FORMAT} MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
        ),
        FileFormat::Parquet => {
            let select = load_select(config);
            format!(
                "COPY INTO {table} {} FROM (SELECT {} FROM {source}){files} {PARQUET_FILE_FORMAT}",
                select.columns, select.expressions
            )
        }
    }
}
