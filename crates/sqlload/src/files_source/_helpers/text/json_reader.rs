use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde_json::Value;

use crate::column_types::models::ColumnType;
use crate::files_source::_helpers::text::bytes_reader::BytesReader;
use crate::files_source::_helpers::text::json_records::{JsonRecord, JsonSplitter};

/// Reads a JSON file's objects in order, a chunk of bytes at a time.
pub(crate) struct JsonReader {
    bytes: BytesReader,
    splitter: JsonSplitter,
    ready: VecDeque<JsonRecord>,
    finished: bool,
}

impl JsonReader {
    /// `by_line` reads one object per line (`ndjson`); otherwise objects may span lines or sit in one top-level array.
    pub(crate) fn new(bytes: BytesReader, by_line: bool) -> Self {
        Self {
            bytes,
            splitter: JsonSplitter::new(by_line),
            ready: VecDeque::new(),
            finished: false,
        }
    }

    /// Up to `limit` more objects; empty at the end of the file.
    pub(crate) async fn next_records(&mut self, limit: usize) -> Result<Vec<JsonRecord>, String> {
        while self.ready.len() < limit && !self.finished {
            let chunk = self.bytes.next_chunk().await?;
            self.finished = chunk.is_empty();
            let records = match self.finished {
                true => self.splitter.finish()?,
                false => self.splitter.push(&chunk)?,
            };
            self.ready.extend(records);
        }
        let take = limit.min(self.ready.len());
        Ok(self.ready.drain(..take).collect())
    }
}

/// `records` as a batch of text columns, one per `columns` entry (a missing key is NULL; a repeated key keeps its last value), and the line each row starts on.
pub(crate) fn records_batch(
    records: &[JsonRecord],
    columns: &[(String, ColumnType)],
) -> Result<(RecordBatch, Vec<u64>), String> {
    let index: HashMap<&str, usize> = columns
        .iter()
        .enumerate()
        .map(|(position, (name, _))| (name.as_str(), position))
        .collect();
    let mut values: Vec<Vec<Option<String>>> = vec![vec![None; records.len()]; columns.len()];
    for (row, record) in records.iter().enumerate() {
        for (key, value) in &record.fields {
            if let Some(position) = index.get(key.as_str()) {
                values[*position][row] = value_text(value, &columns[*position].1);
            }
        }
    }
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, _)| Field::new(name, DataType::Utf8, true))
        .collect();
    let arrays: Vec<ArrayRef> = values
        .into_iter()
        .map(|column| Arc::new(StringArray::from(column)) as ArrayRef)
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(records.len()));
    let batch = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)
        .map_err(|error| error.to_string())?;
    Ok((batch, records.iter().map(|record| record.line).collect()))
}

/// A value as text for a column of `column_type`: JSON text for `json` columns, objects and arrays; a string's own text otherwise.
fn value_text(value: &Value, column_type: &ColumnType) -> Option<String> {
    match (value, column_type) {
        (Value::Null, _) => None,
        (Value::String(text), column_type) if *column_type != ColumnType::Json => {
            Some(text.clone())
        }
        (value, _) => Some(value.to_string()),
    }
}
