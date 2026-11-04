use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use bytes::{Buf, BytesMut};

use crate::column_types::main::arrow_field::arrow_field;
use crate::postgres_source::_helpers::builders::ColumnBuilder;
use crate::postgres_source::_helpers::values::{read_i16, read_i32};
use crate::postgres_source::constants::{
    COPY_SIGNATURE, COPY_TRAILER, HEADER_EXTENSION_OFFSET, HEADER_FIXED_LENGTH, NULL_FIELD_LENGTH,
};
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::ReadColumn;

/// Each field's byte range (`None` for NULL), and the offset just past the row.
type FieldSpans = (Vec<Option<(usize, usize)>>, usize);

/// Turns a binary `COPY` byte stream, fed in arbitrary chunks, into Arrow batches.
pub(crate) struct CopyDecoder {
    table: String,
    columns: Vec<ReadColumn>,
    builders: Vec<ColumnBuilder>,
    schema: SchemaRef,
    buffer: BytesMut,
    header_read: bool,
    finished: bool,
    rows: usize,
}

impl CopyDecoder {
    pub(crate) fn new(table: &str, columns: Vec<ReadColumn>) -> Result<Self, PostgresError> {
        let unreadable = |column: &ReadColumn, reason: String| PostgresError::Batch {
            table: table.to_owned(),
            reason: format!("{}: {reason}", column.name),
        };
        let builders = columns
            .iter()
            .map(|column| {
                ColumnBuilder::for_type(&column.output).map_err(|reason| unreadable(column, reason))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let fields = columns
            .iter()
            .map(|column| {
                arrow_field(&column.name, &column.output)
                    .ok_or_else(|| unreadable(column, "no Arrow representation".to_owned()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            table: table.to_owned(),
            columns,
            builders,
            schema: Arc::new(Schema::new(fields)),
            buffer: BytesMut::new(),
            header_read: false,
            finished: false,
            rows: 0,
        })
    }

    /// Rows decoded since the last batch was taken.
    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    /// Whether the stream's trailer has been read.
    pub(crate) fn finished(&self) -> bool {
        self.finished
    }

    /// Add a chunk of the stream and decode every complete row in the buffer.
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Result<(), PostgresError> {
        self.buffer.extend_from_slice(chunk);
        if !self.header_read && !self.read_header()? {
            return Ok(());
        }
        while !self.finished && self.read_tuple()? {}
        Ok(())
    }

    /// The rows decoded so far as one batch.
    pub(crate) fn take_batch(&mut self) -> Result<RecordBatch, PostgresError> {
        let arrays: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect();
        self.rows = 0;
        RecordBatch::try_new(self.schema.clone(), arrays).map_err(|error| PostgresError::Batch {
            table: self.table.clone(),
            reason: error.to_string(),
        })
    }

    fn stream_error(&self, reason: impl Into<String>) -> PostgresError {
        PostgresError::Stream {
            table: self.table.clone(),
            reason: reason.into(),
        }
    }

    fn read_header(&mut self) -> Result<bool, PostgresError> {
        if self.buffer.len() < HEADER_FIXED_LENGTH {
            return Ok(false);
        }
        if !self.buffer.starts_with(COPY_SIGNATURE) {
            return Err(self.stream_error("missing binary COPY signature"));
        }
        let extension = read_i32(&self.buffer[HEADER_EXTENSION_OFFSET..HEADER_FIXED_LENGTH])
            .map_err(|reason| self.stream_error(reason))?;
        let extension = usize::try_from(extension)
            .map_err(|_| self.stream_error("negative header extension length"))?;
        if self.buffer.len() < HEADER_FIXED_LENGTH + extension {
            return Ok(false);
        }
        self.buffer.advance(HEADER_FIXED_LENGTH + extension);
        self.header_read = true;
        Ok(true)
    }

    /// Decode one row if the buffer holds all of it; `false` when more bytes are needed.
    fn read_tuple(&mut self) -> Result<bool, PostgresError> {
        let Some(count) = self
            .buffer
            .get(..2)
            .map(read_i16)
            .transpose()
            .map_err(|reason| self.stream_error(reason))?
        else {
            return Ok(false);
        };
        if count == COPY_TRAILER {
            self.finished = true;
            self.buffer.advance(2);
            return Ok(true);
        }
        let expected = i16::try_from(self.columns.len())
            .map_err(|_| self.stream_error("too many columns for one COPY row"))?;
        if count != expected {
            return Err(self.stream_error(format!(
                "row has {count} fields, expected {}",
                self.columns.len()
            )));
        }
        let Some((fields, end)) = self.field_spans()? else {
            return Ok(false);
        };
        for (index, span) in fields.iter().enumerate() {
            let value = span.map(|(start, end)| &self.buffer[start..end]);
            let column = &self.columns[index];
            self.builders[index]
                .append(column.wire, value)
                .map_err(|reason| PostgresError::Value {
                    table: self.table.clone(),
                    column: column.name.clone(),
                    reason,
                })?;
        }
        self.buffer.advance(end);
        self.rows += 1;
        Ok(true)
    }

    /// Byte ranges of each field of the next row (`None` for NULL) and where the row ends; `None` if incomplete.
    fn field_spans(&self) -> Result<Option<FieldSpans>, PostgresError> {
        let mut offset = 2;
        let mut spans = Vec::with_capacity(self.columns.len());
        for _ in 0..self.columns.len() {
            let Some(length) = self.buffer.get(offset..offset + 4) else {
                return Ok(None);
            };
            let length = read_i32(length).map_err(|reason| self.stream_error(reason))?;
            offset += 4;
            if length == NULL_FIELD_LENGTH {
                spans.push(None);
                continue;
            }
            let length = usize::try_from(length)
                .map_err(|_| self.stream_error(format!("invalid field length {length}")))?;
            if self.buffer.len() < offset + length {
                return Ok(None);
            }
            spans.push(Some((offset, offset + length)));
            offset += length;
        }
        Ok(Some((spans, offset)))
    }
}
