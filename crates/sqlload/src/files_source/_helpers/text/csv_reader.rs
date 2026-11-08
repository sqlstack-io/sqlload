use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_csv::reader::{Decoder, Format, ReaderBuilder};
use arrow_schema::{DataType, Field, Schema};
use regex::Regex;

use crate::files_source::_helpers::text::bytes_reader::BytesReader;
use crate::files_source::_helpers::text::csv_records::{FirstRecord, QuoteTracker};
use crate::files_source::constants::MAX_RECORD_BYTES;
use crate::files_source::models::CsvOptions;

/// Reads a CSV file's records in order as text columns, a chunk of bytes at a time.
pub(crate) struct CsvReader {
    bytes: BytesReader,
    /// `None` for a file with no records at all.
    decoder: Option<Decoder>,
    tracker: QuoteTracker,
    pending: Vec<u8>,
    offset: usize,
    at_end: bool,
    /// Whether the decoder was told the file ended (an empty `decode`), which completes a last record without a line break.
    end_sent: bool,
    names: Vec<String>,
    /// The line of the next data record: lines count records, the header included.
    next_line: u64,
}

impl CsvReader {
    /// A reader positioned at the start of `bytes`, with the column names read from the header (or `column_1`, ... without one).
    pub(crate) async fn open(
        bytes: BytesReader,
        options: &CsvOptions,
        batch_rows: usize,
    ) -> Result<Self, String> {
        Self::open_as(bytes, (options, batch_rows), true).await
    }

    /// A reader of whole lines: a header is skipped but never checked, and columns are `column_1`, ... either way.
    pub(crate) async fn open_lines(
        bytes: BytesReader,
        options: &CsvOptions,
        batch_rows: usize,
    ) -> Result<Self, String> {
        Self::open_as(bytes, (options, batch_rows), false).await
    }

    /// A reader positioned at the start of `bytes`; with `named`, its columns are the header's names, checked.
    pub(crate) async fn open_as(
        mut bytes: BytesReader,
        (options, batch_rows): (&CsvOptions, usize),
        named: bool,
    ) -> Result<Self, String> {
        let delimiter = ascii(options.delimiter, "delimiter")?;
        let quote = ascii(options.quote, "quote")?;
        let mut first_record = FirstRecord::new(delimiter, quote, MAX_RECORD_BYTES);
        let mut pending: Vec<u8> = Vec::new();
        let (first, at_end) = loop {
            let chunk = bytes.next_chunk().await?;
            pending.extend_from_slice(&chunk);
            if let Some(first) = first_record.feed(&chunk)? {
                break (first, chunk.is_empty());
            }
        };
        let names = column_names(first, options.header && named)?;
        let decoder = match names.is_empty() {
            true => None,
            false => Some(decoder(&names, options, batch_rows)?),
        };
        Ok(Self {
            bytes,
            decoder,
            tracker: QuoteTracker::new(delimiter, quote, MAX_RECORD_BYTES),
            pending,
            offset: 0,
            at_end,
            end_sent: false,
            names,
            next_line: 1 + u64::from(options.header),
        })
    }

    /// The file's columns, in order.
    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    /// The next batch of records and the line its first row is on; `None` at the end of the file.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<(RecordBatch, u64)>, String> {
        let Some(decoder) = self.decoder.as_mut() else {
            return Ok(None);
        };
        loop {
            if self.offset == self.pending.len() && !self.at_end {
                self.pending = self.bytes.next_chunk().await?;
                self.offset = 0;
                self.at_end = self.pending.is_empty();
            }
            let input = &self.pending[self.offset..];
            if !input.is_empty() {
                let decoded = decoder.decode(input).map_err(csv_error)?;
                self.tracker.push(&input[..decoded])?;
                self.offset += decoded;
            } else if self.at_end && !self.end_sent && decoder.capacity() > 0 {
                decoder.decode(&[]).map_err(csv_error)?;
                self.end_sent = true;
            }
            let done = self.end_sent && self.offset == self.pending.len();
            if done {
                self.tracker.finish()?;
            }
            if decoder.capacity() == 0 || done {
                match decoder.flush().map_err(csv_error)? {
                    Some(batch) => {
                        let line = self.next_line;
                        self.next_line += batch.num_rows() as u64;
                        return Ok(Some((batch, line)));
                    }
                    None if done => return Ok(None),
                    None => {}
                }
            }
        }
    }
}

/// An arrow-csv decoder reading every column as text, skipping the header itself so its line numbers count it.
fn decoder(names: &[String], options: &CsvOptions, batch_rows: usize) -> Result<Decoder, String> {
    let fields: Vec<Field> = names
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect();
    let format = Format::default()
        .with_header(options.header)
        .with_delimiter(ascii(options.delimiter, "delimiter")?)
        .with_quote(ascii(options.quote, "quote")?)
        .with_null_regex(null_regex(&options.null_values)?);
    Ok(ReaderBuilder::new(Arc::new(Schema::new(fields)))
        .with_format(format)
        .with_batch_size(batch_rows)
        .build_decoder())
}

/// A regex matching exactly the `null_values`; one that never matches when there are none.
fn null_regex(null_values: &[String]) -> Result<Regex, String> {
    let pattern = match null_values.is_empty() {
        true => r"[^\s\S]".to_owned(),
        false => {
            let alternatives: Vec<String> = null_values
                .iter()
                .map(|value| regex::escape(value))
                .collect();
            format!(r"\A(?:{})\z", alternatives.join("|"))
        }
    };
    Regex::new(&pattern).map_err(|error| format!("csv null_values: {error}"))
}

fn ascii(character: char, setting: &str) -> Result<u8, String> {
    match u8::try_from(character) {
        Ok(byte) if byte.is_ascii() => Ok(byte),
        _ => Err(format!(
            "csv {setting} must be one ASCII character, not {character:?}"
        )),
    }
}

/// The header's names, checked; or `column_1`, ... for a file without a header.
fn column_names(first: Vec<String>, header: bool) -> Result<Vec<String>, String> {
    if !header {
        return Ok((1..=first.len())
            .map(|position| format!("column_{position}"))
            .collect());
    }
    for (position, name) in first.iter().enumerate() {
        if name.is_empty() {
            return Err(format!("line 1: header field {} is empty", position + 1));
        }
        if first[..position].contains(name) {
            return Err(format!("line 1: column {name} appears twice in the header"));
        }
    }
    Ok(first)
}

fn csv_error(error: arrow_schema::ArrowError) -> String {
    let text = error.to_string();
    text.strip_prefix("Csv error: ")
        .map_or(text.clone(), str::to_owned)
}
