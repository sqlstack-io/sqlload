use std::pin::Pin;

use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use futures_util::TryStreamExt;
use object_store::ObjectStoreExt;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;

use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::constants::READ_CHUNK_BYTES;
use crate::files_source::models::Codec;

const BYTE_ORDER_MARK: &[u8] = b"\xEF\xBB\xBF";

/// A file's decompressed bytes, read a chunk at a time as they arrive (the whole file is never held).
pub(crate) struct BytesReader {
    inner: Pin<Box<dyn AsyncRead + Send>>,
    started: bool,
}

impl BytesReader {
    /// The next chunk, without a leading UTF-8 byte order mark; empty at the end of the file.
    pub(crate) async fn next_chunk(&mut self) -> Result<Vec<u8>, String> {
        let mut chunk: Vec<u8> = Vec::with_capacity(READ_CHUNK_BYTES);
        let mut limited = (&mut self.inner).take(READ_CHUNK_BYTES as u64);
        limited
            .read_to_end(&mut chunk)
            .await
            .map_err(|error| format!("cannot read the file: {error}"))?;
        if !self.started && !chunk.is_empty() {
            self.started = true;
            if chunk.starts_with(BYTE_ORDER_MARK) {
                chunk.drain(..BYTE_ORDER_MARK.len());
            }
        }
        Ok(chunk)
    }
}

/// A streaming reader of `file`'s bytes, decompressed with `codec`.
pub(crate) async fn open_bytes(
    folder: &OpenFolder,
    file: &StoredFile,
    codec: Codec,
) -> Result<BytesReader, String> {
    let stream = folder
        .store
        .get(&file.path)
        .await
        .map_err(|error| format!("cannot read the file: {error}"))?
        .into_stream()
        .map_err(std::io::Error::other);
    let raw = StreamReader::new(stream);
    let inner: Pin<Box<dyn AsyncRead + Send>> = match codec {
        Codec::Plain => Box::pin(raw),
        Codec::Gzip => {
            let mut decoder = GzipDecoder::new(raw);
            decoder.multiple_members(true);
            Box::pin(decoder)
        }
        Codec::Zstd => {
            let mut decoder = ZstdDecoder::new(raw);
            decoder.multiple_members(true);
            Box::pin(decoder)
        }
    };
    Ok(BytesReader {
        inner,
        started: false,
    })
}

/// The error for a record longer than `max_record_bytes`, starting on `line`.
pub(crate) fn too_long(line: u64, max_record_bytes: usize) -> String {
    const MIB: usize = 1024 * 1024;
    let size = match max_record_bytes % MIB {
        0 => format!("{} MiB", max_record_bytes / MIB),
        _ => format!("{max_record_bytes} bytes"),
    };
    format!("line {line}: a record is longer than {size}")
}
