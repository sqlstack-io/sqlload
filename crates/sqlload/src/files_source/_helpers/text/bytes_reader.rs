use std::pin::Pin;

use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use futures_util::TryStreamExt;
use object_store::ObjectStoreExt;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;

use encoding_rs::{DecoderResult, Encoding, WINDOWS_1252};

use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::archive::zip_archive::member_bytes;
use crate::files_source::constants::{
    CP1252_UNDEFINED, ENCODING_HINT, LAST_C1_CONTROL, MAX_WHOLE_FILE_BYTES, READ_CHUNK_BYTES,
    UTF8_SHARE, UTF16_BOMS,
};
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
    /// Whether a skipped line ended in a line feed, and whether skipped text held a carriage return: lines ending in a bare `\r` never end a skipped line.
    skipped_breaks: (bool, bool),
    /// What the bytes say about the encoding, under `csv.encoding_check` for `latin-1`, `cp1252` and `iso-8859-15`.
    evidence: Option<EncodingEvidence>,
}

/// What a file's bytes say about its encoding as a single-byte encoding reads them: its bytes above 0x7F, how many lie in valid UTF-8 sequences, and the line and byte of the first such sequence and of the first C1 control character (with the byte).
#[derive(Debug, Default)]
struct EncodingEvidence {
    /// Whether C1 control characters (0x80 to 0x9F) count: they do in `latin-1` and `iso-8859-15`, where no text holds them.
    controls: bool,
    high: u64,
    utf8: u64,
    first_utf8: Option<(u64, u64)>,
    first_control: Option<(u64, u64, u8)>,
    /// A UTF-8 sequence the last chunk ended inside.
    carry: Vec<u8>,
}

impl BytesReader {
    /// A reader of `inner`'s bytes as UTF-8, as they are.
    fn new(inner: Pin<Box<dyn AsyncRead + Send>>) -> Self {
        Self {
            inner,
            started: false,
            charset: Charset::Utf8,
            decoder: None,
            read: 0,
            lines: 0,
            skip: 0,
            skipped_breaks: (false, false),
            evidence: None,
        }
    }

    /// The rest of the file, whole, decoded.
    pub(crate) async fn whole(&mut self) -> Result<Vec<u8>, String> {
        let mut all: Vec<u8> = Vec::new();
        loop {
            let chunk = self.next_chunk().await?;
            if chunk.is_empty() {
                return Ok(all);
            }
            all.extend(chunk);
            if all.len() as u64 > MAX_WHOLE_FILE_BYTES {
                return Err(format!(
                    "the file is over {MAX_WHOLE_FILE_BYTES} bytes; SQLLoad reads workbooks and XML documents whole, up to that size"
                ));
            }
        }
    }

    /// This reader, decoding the file from `charset` and dropping its first `skip_rows` lines; with `check`, a file read as `latin-1`, `cp1252` or `iso-8859-15` whose bytes say it is UTF-8, or cp1252 (control characters), fails at its end.
    pub(crate) fn decoding(mut self, charset: Charset, (skip_rows, check): (u64, bool)) -> Self {
        self.charset = charset;
        self.skip = skip_rows;
        self.evidence = charset
            .checked()
            .filter(|_| check)
            .map(|controls| EncodingEvidence {
                controls,
                ..EncodingEvidence::default()
            });
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
                        self.skipped_breaks.0 = true;
                    }
                    None => {
                        self.skipped_breaks.1 |= text.contains(&b'\r');
                        text.clear();
                    }
                }
            }
            if end && self.skip > 0 && self.skipped_breaks == (false, true) {
                return Err("csv skip_rows found no line feed (\\n) in the file, whose lines end in a bare carriage return (\\r); skip_rows counts lines ending in \\n or \\r\\n, so convert the line ends or set skip_rows = 0".to_owned());
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
        if let Some(evidence) = self.evidence.as_mut() {
            evidence.note(&raw, (self.read, self.lines), end);
            let problem = end.then(|| evidence.problem(self.charset)).flatten();
            if let Some(problem) = problem {
                return Err(problem);
            }
        }
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
        let [little, big] = UTF16_BOMS;
        let opposite = match (encoding.name(), raw.get(..2)) {
            ("UTF-16LE", Some(start)) if start == big => Some("utf-16be"),
            ("UTF-16BE", Some(start)) if start == little => Some("utf-16le"),
            _ => None,
        };
        if self.decoder.is_none()
            && let Some(opposite) = opposite
        {
            return Err(format!(
                "line 1, byte 0: the file starts with a {opposite} byte order mark, but csv.encoding reads it as {}; set csv.encoding = \"{opposite}\"",
                encoding.name()
            ));
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
                let lines = text.iter().filter(|byte| **byte == b'\n').count() as u64;
                self.lines += lines;
                let behind = usize::from(after) + usize::from(bad);
                match read.checked_sub(behind) {
                    Some(at) => {
                        Err(self.undecodable_at(raw, (at, usize::from(bad)), encoding.name()))
                    }
                    None => Err(format!(
                        "line {}, byte {}: an incomplete {} sequence; {ENCODING_HINT}",
                        self.lines + 1,
                        (self.read + read as u64).saturating_sub(behind as u64),
                        encoding.name()
                    )),
                }
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

impl EncodingEvidence {
    /// Count the bytes of `raw`, the chunk after `read` bytes and `lines` line breaks; `end` when it is the file's last.
    fn note(&mut self, raw: &[u8], (read, lines): (u64, u64), end: bool) {
        let carried = self.carry.len() as u64;
        let mut bytes = std::mem::take(&mut self.carry);
        bytes.extend_from_slice(raw);
        let mut breaks: u64 = 0;
        let mut at = 0;
        while at < bytes.len() {
            let byte = bytes[at];
            breaks += u64::from(byte == b'\n');
            if byte.is_ascii() {
                at += 1;
                continue;
            }
            let place = (
                lines + breaks + 1,
                (read + at as u64).saturating_sub(carried),
            );
            let length = utf8_length(byte);
            let rest = &bytes[at + 1..];
            if !end
                && length > 1 + rest.len()
                && rest.iter().all(|next| (0x80..=0xBF).contains(next))
            {
                self.carry = bytes[at..].to_vec();
                return;
            }
            let valid = match bytes.get(at..at + length) {
                Some(sequence) if length > 0 => sequence
                    .utf8_chunks()
                    .all(|chunk| chunk.invalid().is_empty()),
                Some(_) | None => false,
            };
            if valid {
                self.high += length as u64;
                self.utf8 += length as u64;
                self.first_utf8.get_or_insert(place);
                at += length;
                continue;
            }
            self.high += 1;
            if self.controls && byte <= LAST_C1_CONTROL {
                self.first_control.get_or_insert((place.0, place.1, byte));
            }
            at += 1;
        }
    }

    /// The error for a whole file read as `charset` whose bytes say otherwise: mostly UTF-8 sequences, or a C1 control character.
    fn problem(&self, charset: Charset) -> Option<String> {
        let name = match charset {
            Charset::Other(encoding) => encoding.name(),
            Charset::Utf8 | Charset::Latin1 | Charset::Ascii => "latin-1",
        };
        let utf8 = self.utf8 > 0 && self.utf8 as f64 >= self.high as f64 * UTF8_SHARE;
        if let (true, Some((line, byte))) = (utf8, self.first_utf8) {
            return Some(format!(
                "line {line}, byte {byte}: the file's bytes above 0x7F are UTF-8 sequences, which {name} reads as two or three characters each; this file looks like UTF-8, so set csv.encoding = \"utf-8\", or csv.encoding_check = false to read it as {name}"
            ));
        }
        let (line, byte, value) = self.first_control?;
        Some(format!(
            "line {line}, byte {byte}: 0x{value:02X} is a control character in {name}, which text never holds; this file is probably cp1252, so set csv.encoding = \"cp1252\", or csv.encoding_check = false to read it as {name}"
        ))
    }
}

/// How many bytes the UTF-8 sequence that `lead` starts takes; 0 for a byte that starts none.
fn utf8_length(lead: u8) -> usize {
    match lead {
        0xC2..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF4 => 4,
        _ => 0,
    }
}

/// A streaming reader of `file`'s bytes, decompressed with `codec`.
pub(crate) async fn open_bytes(
    folder: &OpenFolder,
    file: &StoredFile,
    codec: Codec,
) -> Result<BytesReader, String> {
    if let Some(name) = &file.member {
        let member = member_bytes(folder, file, name).await?;
        return Ok(BytesReader::new(Box::pin(std::io::Cursor::new(member))));
    }
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
    Ok(BytesReader::new(inner))
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
