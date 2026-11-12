use std::pin::Pin;

use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use futures_util::TryStreamExt;
use object_store::ObjectStoreExt;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;

use encoding_rs::{DecoderResult, Encoding, WINDOWS_1252};

use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::constants::{CP1252_UNDEFINED, ENCODING_HINT, READ_CHUNK_BYTES};
use crate::files_source::models::{Charset, Codec};

const BYTE_ORDER_MARK: &[u8] = b"\xEF\xBB\xBF";

/// A file's decompressed bytes as UTF-8, read a chunk at a time as they arrive (the whole file is never held).
pub(crate) struct BytesReader {
    inner: Pin<Box<dyn AsyncRead + Send>>,
    started: bool,
    charset: Charset,
    decoder: Option<encoding_rs::Decoder>,
    /// Bytes of the file read so far, before decoding.
    read: u64,
    /// Line breaks decoded so far, outside UTF-8, to say where an undecodable byte is.
    lines: u64,
    /// Lines still to skip at the start of the file.
    skip: u64,
}

impl BytesReader {
    /// This reader, decoding the file from `charset` and dropping its first `skip_rows` lines.
    pub(crate) fn decoding(mut self, charset: Charset, skip_rows: u64) -> Self {
        self.charset = charset;
        self.skip = skip_rows;
        self
    }

    /// The next chunk as UTF-8, without a leading byte order mark or the lines to skip; empty at the end of the file. A byte the charset cannot decode is an error naming its line and offset (UTF-8 itself is checked by the CSV reader).
    pub(crate) async fn next_chunk(&mut self) -> Result<Vec<u8>, String> {
        loop {
            let raw = self.raw_chunk().await?;
            let end = raw.is_empty();
            let mut text = self.decoded(raw, end)?;
            while self.skip > 0 && !text.is_empty() {
                match text.iter().position(|byte| *byte == b'\n') {
                    Some(at) => {
                        text.drain(..=at);
                        self.skip -= 1;
                    }
                    None => text.clear(),
                }
            }
            if end || !text.is_empty() {
                return Ok(text);
            }
        }
    }

    /// The next chunk of the file's bytes, without a leading UTF-8 byte order mark when the charset is UTF-8.
    async fn raw_chunk(&mut self) -> Result<Vec<u8>, String> {
        let mut chunk: Vec<u8> = Vec::with_capacity(READ_CHUNK_BYTES);
        let mut limited = (&mut self.inner).take(READ_CHUNK_BYTES as u64);
        limited
            .read_to_end(&mut chunk)
            .await
            .map_err(|error| format!("cannot read the file: {error}"))?;
        if !self.started && !chunk.is_empty() {
            self.started = true;
            if self.charset == Charset::Utf8 && chunk.starts_with(BYTE_ORDER_MARK) {
                chunk.drain(..BYTE_ORDER_MARK.len());
                self.read += BYTE_ORDER_MARK.len() as u64;
            }
        }
        Ok(chunk)
    }

    /// `raw` decoded to UTF-8; `end` when it is the last of the file.
    fn decoded(&mut self, raw: Vec<u8>, end: bool) -> Result<Vec<u8>, String> {
        let text = match self.charset {
            Charset::Utf8 => {
                self.read += raw.len() as u64;
                return Ok(raw);
            }
            Charset::Latin1 => encoding_rs::mem::decode_latin1(&raw)
                .into_owned()
                .into_bytes(),
            Charset::Ascii => match raw.iter().position(|byte| !byte.is_ascii()) {
                Some(at) => return Err(self.undecodable(&raw, (at, 1), "ASCII")),
                None => raw.clone(),
            },
            Charset::Other(encoding) => self.decode_with(encoding, &raw, end)?,
        };
        self.read += raw.len() as u64;
        self.lines += text.iter().filter(|byte| **byte == b'\n').count() as u64;
        Ok(text)
    }

    /// `raw` decoded from `encoding`, a byte order mark of its own dropped; windows-1252's five undefined bytes are errors, as in Python's cp1252, rather than control characters.
    fn decode_with(
        &mut self,
        encoding: &'static Encoding,
        raw: &[u8],
        end: bool,
    ) -> Result<Vec<u8>, String> {
        if encoding == WINDOWS_1252
            && let Some(at) = raw.iter().position(|byte| CP1252_UNDEFINED.contains(byte))
        {
            return Err(self.undecodable(raw, (at, 1), encoding.name()));
        }
        let decoder = self
            .decoder
            .get_or_insert_with(|| encoding.new_decoder_with_bom_removal());
        let room = decoder
            .max_utf8_buffer_length_without_replacement(raw.len())
            .ok_or_else(|| "a chunk of the file is too large to decode".to_owned())?;
        let mut text = vec![0; room];
        let (result, read, written) =
            decoder.decode_to_utf8_without_replacement(raw, &mut text, end);
        text.truncate(written);
        match result {
            DecoderResult::InputEmpty => Ok(text),
            DecoderResult::Malformed(bad, after) => {
                let at = read.saturating_sub(usize::from(after) + usize::from(bad));
                let lines = text.iter().filter(|byte| **byte == b'\n').count() as u64;
                self.lines += lines;
                Err(self.undecodable_at(raw, (at, usize::from(bad)), encoding.name()))
            }
            DecoderResult::OutputFull => {
                Err("cannot decode the file: the decoder ran out of room".to_owned())
            }
        }
    }

    /// The error for `bad` bytes at `at` in `raw` that `charset` cannot decode, where `raw` is ASCII-compatible, so its line breaks count lines.
    fn undecodable(&self, raw: &[u8], bad: (usize, usize), charset: &str) -> String {
        let breaks = raw[..bad.0].iter().filter(|byte| **byte == b'\n').count() as u64;
        let line = self.lines + breaks + 1;
        self.undecodable_text(raw, bad, (line, charset))
    }

    /// The error for `bad` bytes at `at` in `raw` that `charset` cannot decode, after the line breaks already counted.
    fn undecodable_at(&self, raw: &[u8], bad: (usize, usize), charset: &str) -> String {
        self.undecodable_text(raw, bad, (self.lines + 1, charset))
    }

    fn undecodable_text(
        &self,
        raw: &[u8],
        (at, length): (usize, usize),
        (line, charset): (u64, &str),
    ) -> String {
        let shown: Vec<String> = raw[at..(at + length.max(1)).min(raw.len())]
            .iter()
            .map(|byte| format!("0x{byte:02X}"))
            .collect();
        format!(
            "line {line}, byte {}: {} is not valid {charset}; {ENCODING_HINT}",
            self.read + at as u64,
            shown.join(" ")
        )
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
        charset: Charset::Utf8,
        decoder: None,
        read: 0,
        lines: 0,
        skip: 0,
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
