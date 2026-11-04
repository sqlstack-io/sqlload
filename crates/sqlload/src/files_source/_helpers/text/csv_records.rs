use csv_core::ReadRecordResult;

use crate::files_source::_helpers::text::bytes_reader::too_long;

/// Where a [`QuoteTracker`] is within a record, as csv-core reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Position {
    FieldStart,
    Unquoted,
    Quoted,
    /// A quote inside a quoted field: it ends the field, or is the first of a doubled quote.
    QuoteInQuoted,
}

/// Follows the bytes a CSV decoder consumes, so a quoted field the file never closes, or an endless record, is an error instead of one huge field.
#[derive(Debug)]
pub(crate) struct QuoteTracker {
    delimiter: u8,
    quote: u8,
    max_record_bytes: usize,
    position: Position,
    record_bytes: usize,
    /// Records finished so far (blank lines are no records); lines count records, as arrow-csv's do.
    records: u64,
}

impl QuoteTracker {
    pub(crate) fn new(delimiter: u8, quote: u8, max_record_bytes: usize) -> Self {
        Self {
            delimiter,
            quote,
            max_record_bytes,
            position: Position::FieldStart,
            record_bytes: 0,
            records: 0,
        }
    }

    /// Follow `bytes`; a record longer than the limit is an error.
    pub(crate) fn push(&mut self, bytes: &[u8]) -> Result<(), String> {
        for byte in bytes {
            self.position = self.next(*byte);
            if self.record_bytes > self.max_record_bytes {
                return Err(too_long(self.records + 1, self.max_record_bytes));
            }
        }
        Ok(())
    }

    /// At the end of the file: an error when it ends inside a quoted field.
    pub(crate) fn finish(&self) -> Result<(), String> {
        match self.position {
            Position::Quoted => Err(format!(
                "line {}: unterminated quoted field",
                self.records + 1
            )),
            Position::FieldStart | Position::Unquoted | Position::QuoteInQuoted => Ok(()),
        }
    }

    fn next(&mut self, byte: u8) -> Position {
        let unquoted = match self.position {
            Position::Quoted | Position::QuoteInQuoted if byte == self.quote => {
                self.record_bytes += 1;
                return match self.position {
                    Position::Quoted => Position::QuoteInQuoted,
                    _ => Position::Quoted,
                };
            }
            Position::Quoted => {
                self.record_bytes += 1;
                return Position::Quoted;
            }
            Position::FieldStart | Position::Unquoted | Position::QuoteInQuoted => {
                self.position == Position::FieldStart
            }
        };
        match byte {
            b'\r' | b'\n' => {
                self.records += u64::from(self.record_bytes > 0);
                self.record_bytes = 0;
                Position::FieldStart
            }
            _ if byte == self.delimiter => {
                self.record_bytes += 1;
                Position::FieldStart
            }
            _ if unquoted && byte == self.quote => {
                self.record_bytes += 1;
                Position::Quoted
            }
            _ => {
                self.record_bytes += 1;
                Position::Unquoted
            }
        }
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
    pub(crate) fn new(delimiter: u8, quote: u8, max_record_bytes: usize) -> Self {
        Self {
            reader: csv_core::ReaderBuilder::new()
                .delimiter(delimiter)
                .quote(quote)
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
            let field = std::str::from_utf8(&self.output[start..*end])
                .map_err(|_| "line 1: the first record is not UTF-8".to_owned())?;
            fields.push(field.to_owned());
            start = *end;
        }
        Ok(fields)
    }
}
