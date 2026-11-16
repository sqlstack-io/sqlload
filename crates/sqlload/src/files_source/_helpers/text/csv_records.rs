use csv_core::ReadRecordResult;

use crate::files_source::_helpers::text::bytes_reader::too_long;
use crate::files_source::constants::{ENCODING_HINT, ESCAPE_HINT, QUOTE_OFF_BYTE, QUOTE_OFF_HINT};

/// Where a [`QuoteTracker`] is within a record, as csv-core reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Position {
    FieldStart,
    Unquoted,
    Quoted,
    /// The escape character inside a quoted field: the next character is plain text.
    Escaped,
    /// A quote inside a quoted field: it ends the field, or is the first of a doubled quote.
    QuoteInQuoted,
}

/// How a CSV file quotes its fields: the delimiter, the quote (`None` with quoting off) and the escape inside quoted fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CsvQuoting {
    pub(crate) delimiter: u8,
    pub(crate) quote: Option<u8>,
    pub(crate) escape: Option<u8>,
}

/// Follows the bytes a CSV decoder consumes, so a quoted field the file never closes, text after a closing quote, or an endless record, is an error instead of a misread value or one huge field.
#[derive(Debug)]
pub(crate) struct QuoteTracker {
    quoting: CsvQuoting,
    max_record_bytes: usize,
    position: Position,
    record_bytes: usize,
    /// Records finished so far (blank lines are no records); lines count records, as arrow-csv's do.
    records: u64,
}

impl QuoteTracker {
    pub(crate) fn new(quoting: CsvQuoting, max_record_bytes: usize) -> Self {
        Self {
            quoting,
            max_record_bytes,
            position: Position::FieldStart,
            record_bytes: 0,
            records: 0,
        }
    }

    /// Follow `bytes`; text after a closing quote, or a record longer than the limit, is an error.
    pub(crate) fn push(&mut self, mut bytes: &[u8]) -> Result<(), String> {
        while let Some(&byte) = bytes.first() {
            let plain = self.plain_run(bytes);
            match plain {
                0 => {
                    self.position = self.next(byte)?;
                    bytes = &bytes[1..];
                }
                _ => {
                    self.record_bytes += plain;
                    bytes = &bytes[plain..];
                }
            }
            if self.record_bytes > self.max_record_bytes {
                return Err(too_long(self.records + 1, self.max_record_bytes));
            }
        }
        Ok(())
    }

    /// How many of `bytes`, from the start, are plain text of the field being read, which [`Self::next`] would only count: in an unquoted field, all but a line end, the delimiter and, with quoting off, its stand-in quote; in a quoted field, all but the quote and the escape.
    fn plain_run(&self, bytes: &[u8]) -> usize {
        let CsvQuoting {
            delimiter,
            quote,
            escape,
        } = self.quoting;
        let quoting_off = quote.is_none();
        let special = match self.position {
            Position::Unquoted => bytes.iter().position(|&byte| {
                byte == b'\n'
                    || byte == b'\r'
                    || byte == delimiter
                    || (quoting_off && byte == QUOTE_OFF_BYTE)
            }),
            Position::Quoted => bytes
                .iter()
                .position(|&byte| Some(byte) == quote || Some(byte) == escape),
            Position::FieldStart | Position::Escaped | Position::QuoteInQuoted => Some(0),
        };
        special.unwrap_or(bytes.len())
    }

    /// At the end of the file: an error when it ends inside a quoted field.
    pub(crate) fn finish(&self) -> Result<(), String> {
        match self.position {
            Position::Quoted | Position::Escaped => Err(format!(
                "line {}: unterminated quoted field; {}",
                self.records + 1,
                self.hint()
            )),
            Position::FieldStart | Position::Unquoted | Position::QuoteInQuoted => Ok(()),
        }
    }

    /// The settings that may fix a quoting error: an escape when none is set, or no quoting.
    fn hint(&self) -> String {
        match self.quoting.escape {
            None => format!("{ESCAPE_HINT}; {QUOTE_OFF_HINT}"),
            Some(_) => QUOTE_OFF_HINT.to_owned(),
        }
    }

    fn next(&mut self, byte: u8) -> Result<Position, String> {
        let quote = self.quoting.quote;
        let quoted = match self.position {
            Position::Escaped => Some(Position::Quoted),
            Position::Quoted | Position::QuoteInQuoted if Some(byte) == quote => {
                Some(match self.position {
                    Position::Quoted => Position::QuoteInQuoted,
                    _ => Position::Quoted,
                })
            }
            Position::Quoted if Some(byte) == self.quoting.escape => Some(Position::Escaped),
            Position::Quoted => Some(Position::Quoted),
            Position::FieldStart | Position::Unquoted | Position::QuoteInQuoted => None,
        };
        if let Some(position) = quoted {
            self.record_bytes += 1;
            return Ok(position);
        }
        let closed = self.position == Position::QuoteInQuoted;
        let unquoted = self.position == Position::FieldStart;
        Ok(match byte {
            b'\r' | b'\n' => {
                self.records += u64::from(self.record_bytes > 0);
                self.record_bytes = 0;
                Position::FieldStart
            }
            _ if byte == self.quoting.delimiter => {
                self.record_bytes += 1;
                Position::FieldStart
            }
            _ if closed => {
                return Err(format!(
                    "line {}: text after a closing quote; {}",
                    self.records + 1,
                    self.hint()
                ));
            }
            QUOTE_OFF_BYTE if quote.is_none() => {
                return Err(format!(
                    "line {}: 0x{QUOTE_OFF_BYTE:02X} is not valid UTF-8; {ENCODING_HINT}",
                    self.records + 1
                ));
            }
            _ if unquoted && Some(byte) == quote => {
                self.record_bytes += 1;
                Position::Quoted
            }
            _ => {
                self.record_bytes += 1;
                Position::Unquoted
            }
        })
    }
}

/// Reads a CSV file's first record from chunks as they arrive, without reading any byte twice.
#[derive(Debug)]
pub(crate) struct FirstRecord {
    reader: csv_core::Reader,
    max_record_bytes: usize,
    output: Vec<u8>,
    ends: Vec<usize>,
    written: usize,
    ended: usize,
    consumed: usize,
}

impl FirstRecord {
    pub(crate) fn new(quoting: CsvQuoting, max_record_bytes: usize) -> Self {
        Self {
            reader: csv_core::ReaderBuilder::new()
                .delimiter(quoting.delimiter)
                .quote(quoting.quote.unwrap_or(QUOTE_OFF_BYTE))
                .quoting(quoting.quote.is_some())
                .escape(quoting.escape)
                .build(),
            max_record_bytes,
            output: Vec::new(),
            ends: Vec::new(),
            written: 0,
            ended: 0,
            consumed: 0,
        }
    }

    /// Read the next chunk (empty at the end of the file): the first record's fields once it is complete, empty for a file with no records.
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Result<Option<Vec<String>>, String> {
        let mut input = chunk;
        loop {
            self.output.resize(self.written + input.len() + 1, 0);
            self.ends.resize(self.ended + input.len() + 1, 0);
            let (result, read, wrote, fields) = self.reader.read_record(
                input,
                &mut self.output[self.written..],
                &mut self.ends[self.ended..],
            );
            input = &input[read..];
            self.written += wrote;
            self.ended += fields;
            self.consumed += read;
            if self.consumed > self.max_record_bytes {
                return Err(too_long(1, self.max_record_bytes));
            }
            match result {
                ReadRecordResult::Record => return self.fields().map(Some),
                ReadRecordResult::End => return Ok(Some(Vec::new())),
                ReadRecordResult::InputEmpty => return Ok(None),
                ReadRecordResult::OutputFull | ReadRecordResult::OutputEndsFull => {}
            }
        }
    }

    fn fields(&self) -> Result<Vec<String>, String> {
        let mut start = 0;
        let mut fields: Vec<String> = Vec::new();
        for end in &self.ends[..self.ended] {
            let field = std::str::from_utf8(&self.output[start..*end]).map_err(|_| {
                format!("line 1: the first record is not valid UTF-8; {ENCODING_HINT}")
            })?;
            fields.push(field.to_owned());
            start = *end;
        }
        Ok(fields)
    }
}
