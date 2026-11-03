//! Encode one staged file.

use crate::batching::main::gzip_bytes::gzip_bytes;
use crate::config::models::{ColumnConfig, FileFormat};
use crate::file_format::_helpers::parquet::parquet_file;

/// The file body for `lines` (JSON objects keyed by column name) and how many uncompressed bytes they held.
pub fn encode_file(
    format: FileFormat,
    columns: &[ColumnConfig],
    lines: &[&str],
) -> Result<(Vec<u8>, u64), String> {
    let plain: u64 = lines.iter().map(|line| line.len() as u64 + 1).sum();
    let body = match format {
        FileFormat::Json => {
            let mut text: Vec<u8> = Vec::with_capacity(plain as usize);
            for line in lines {
                text.extend_from_slice(line.as_bytes());
                text.push(b'\n');
            }
            gzip_bytes(&text)
        }
        FileFormat::Parquet => parquet_file(columns, lines)?,
    };
    Ok((body, plain))
}
