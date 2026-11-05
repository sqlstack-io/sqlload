//! A parity case's source files, for other tests to load.

use bytes::Bytes;

use crate::parity::_helpers::files::case_files;
use crate::parity::models::ParityFiles;

/// The source files of `files`, Parquet with the embedded Arrow schema: each key and its bytes.
pub fn fixture_files(files: ParityFiles) -> Result<Vec<(&'static str, Bytes)>, String> {
    case_files(files, true)
}
