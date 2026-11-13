use std::collections::VecDeque;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde_json::Value;

use crate::column_types::models::ColumnType;
use crate::files_source::_helpers::text::bytes_reader::BytesReader;
use crate::files_source::_helpers::text::checks::{ColumnChecks, misfit, value_fits};
use crate::files_source::_helpers::text::inference::json_value_type;
use crate::files_source::_helpers::text::json_records::{
    JsonRecord, JsonSplitter, flattened_record,
};
use crate::files_source::constants::MAX_RECORD_BYTES;

/// Reads a JSON file's objects in order, a chunk of bytes at a time.
pub(crate) struct JsonReader {
    bytes: BytesReader,
    splitter: JsonSplitter,
    ready: VecDeque<JsonRecord>,
    finished: bool,
    /// Whether objects flatten into `<field>__<key>` fields (see [`flattened_record`]).
    flatten_objects: bool,
    /// The fields flattening has made, and the object fields it replaced, so far.
    flattened: BTreeSet<String>,
}

impl JsonReader {
    /// `by_line` reads one object per line (`ndjson`); otherwise objects may span lines or sit in one top-level array.
    pub(crate) fn new(bytes: BytesReader, by_line: bool) -> Self {
        Self {
            bytes,
            splitter: JsonSplitter::new(by_line, MAX_RECORD_BYTES),
            ready: VecDeque::new(),
            finished: false,
            flatten_objects: false,
            flattened: BTreeSet::new(),
        }
    }

    /// This reader flattening objects when `flatten_objects` says so.
    pub(crate) fn flattening(mut self, flatten_objects: bool) -> Self {
        self.flatten_objects = flatten_objects;
        self
    }

    /// Whether this reader flattens objects.
    pub(crate) fn flattens(&self) -> bool {
        self.flatten_objects
    }

    /// The fields flattening has made, and the object fields it replaced, in the records read so far.
    pub(crate) fn flattened(&self) -> &BTreeSet<String> {
        &self.flattened
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
        let records: Vec<JsonRecord> = self.ready.drain(..take).collect();
        match self.flatten_objects {
            false => Ok(records),
            true => records
                .into_iter()
                .map(|record| flattened_record(record, &mut self.flattened))
                .collect(),
        }
    }
}

/// `records` as a batch of text columns, one per `columns` entry (a missing key is NULL; a repeated key keeps its last value), and the line each row starts on; a key the source lacks, or a value that does not fit its inferred type, is an error, except a NULL when `flattened` (an object that is NULL in this row and flattened in others, which a missing key already loads as).
pub(crate) fn records_batch(
    records: &[JsonRecord],
    columns: &[(String, ColumnType)],
    (checks, flattened): (&ColumnChecks, bool),
) -> Result<(RecordBatch, Vec<u64>), String> {
    let index: HashMap<&str, usize> = columns
        .iter()
        .enumerate()
        .map(|(position, (name, _))| (name.as_str(), position))
        .collect();
    let mut values: Vec<Vec<Option<String>>> = vec![vec![None; records.len()]; columns.len()];
    for (row, record) in records.iter().enumerate() {
        for field in &record.fields {
            let (key, value, nested) = field;
            let Some(position) = index.get(key.as_str()) else {
                if !(flattened && value.is_null()) {
                    checks.check_name("key", key, record.line)?;
                }
                continue;
            };
            check_value(checks, field, record.line)?;
            values[*position][row] = match nested {
                Some(text) => Some(text.clone()),
                None => value_text(value, &columns[*position].1),
            };
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

/// An error when `value` of an inferred column is not what sampling would have inferred (a JSON string in an int column, say).
fn check_value(
    checks: &ColumnChecks,
    (key, value, nested): &(String, Value, Option<String>),
    line: u64,
) -> Result<(), String> {
    match checks.inferred_type(key) {
        Some(column_type) if !value_fits(column_type, json_value_type(value)) => {
            let shown = nested.clone().unwrap_or_else(|| value.to_string());
            Err(misfit(line, key, &shown, column_type))
        }
        _ => Ok(()),
    }
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
