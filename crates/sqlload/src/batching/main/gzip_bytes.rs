//! Gzip a staged file body.

/// Gzip `data`; staged files are always gzip so Snowflake loads them with `COMPRESSION = AUTO`.
pub fn gzip_bytes(data: &[u8]) -> Vec<u8> {
    crate::batching::_helpers::gzip::gzip(data)
}
