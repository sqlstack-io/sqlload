use std::io::Write;

use flate2::Compression;
use flate2::write::GzEncoder;

/// Gzip `data` at the default level.
pub(crate) fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(
        Vec::with_capacity(data.len() / 4 + 64),
        Compression::default(),
    );
    encoder
        .write_all(data)
        .and_then(|()| encoder.finish())
        .unwrap_or_default()
}
