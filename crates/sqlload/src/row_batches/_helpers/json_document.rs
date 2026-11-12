use std::collections::HashMap;

use serde::de::DeserializeSeed;
use serde_json::Value;
use serde_json::de::SliceRead;
use serde_json::value::RawValue;

use crate::row_batches::_helpers::json_seed::{ArraySeed, ObjectSeed, RowSink};
use crate::row_batches::models::RowColumns;

type JsonDeserializer<'a> = serde_json::Deserializer<SliceRead<'a>>;

impl<F: FnMut(RowColumns) -> Result<(), String>> RowSink<F> {
    /// Read NDJSON `bytes`, one object per non-blank line.
    pub(crate) fn read_lines(&mut self, bytes: &[u8]) -> Result<(), String> {
        for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
            if line.trim_ascii().is_empty() {
                continue;
            }
            let mut deserializer: JsonDeserializer<'_> = serde_json::Deserializer::from_slice(line);
            ObjectSeed(&mut *self)
                .deserialize(&mut deserializer)
                .and_then(|()| deserializer.end())
                .map_err(|error| located(&error, index as u64 + 1))?;
        }
        Ok(())
    }

    /// Read a JSON document: an array of objects, objects one after another, or `null` (no rows).
    pub(crate) fn read_document(&mut self, bytes: &[u8]) -> Result<(), String> {
        let start = bytes.iter().position(|byte| !byte.is_ascii_whitespace());
        match start.map(|position| bytes[position]) {
            None => Ok(()),
            Some(b'[') => {
                let mut deserializer: JsonDeserializer<'_> =
                    serde_json::Deserializer::from_slice(bytes);
                ArraySeed(&mut *self)
                    .deserialize(&mut deserializer)
                    .and_then(|()| deserializer.end())
                    .map_err(|error| located(&error, 1))
            }
            Some(b'{') => self.read_objects(bytes),
            Some(_) => match serde_json::from_slice::<Value>(bytes) {
                Ok(Value::Null) => Ok(()),
                Ok(_) => Err(
                    "expected a JSON array of objects, or JSON objects one after another"
                        .to_owned(),
                ),
                Err(error) => Err(located(&error, 1)),
            },
        }
    }

    /// Read JSON objects one after another, until only whitespace is left.
    fn read_objects(&mut self, bytes: &[u8]) -> Result<(), String> {
        let mut deserializer: JsonDeserializer<'_> = serde_json::Deserializer::from_slice(bytes);
        loop {
            ObjectSeed(&mut *self)
                .deserialize(&mut deserializer)
                .map_err(|error| located(&error, 1))?;
            if let Ok(()) = deserializer.end() {
                return Ok(());
            }
        }
    }
}

/// The bytes of the value at dotted `path` (`data.items`) inside a JSON document.
pub(crate) fn select_path<'a>(bytes: &'a [u8], path: &str) -> Result<&'a [u8], String> {
    let mut current: &RawValue =
        serde_json::from_slice(bytes).map_err(|error| located(&error, 1))?;
    let mut walked: Vec<&str> = Vec::new();
    for segment in path.split('.') {
        let listed = current.get().trim_start().starts_with('[');
        let index = segment
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
            .filter(|_| listed)
            .map(str::parse::<usize>);
        current = match index {
            Some(Err(_)) | None => {
                let object: HashMap<String, &RawValue> = serde_json::from_str(current.get())
                    .map_err(|_| format!("path {path}: {} is not a JSON object", shown(&walked)))?;
                object.get(segment).copied().ok_or_else(|| {
                    format!("path {path}: {} has no key {segment:?}", shown(&walked))
                })?
            }
            Some(Ok(index)) => {
                let items: Vec<&RawValue> = serde_json::from_str(current.get())
                    .map_err(|_| format!("path {path}: {} is not a JSON list", shown(&walked)))?;
                items.get(index).copied().ok_or_else(|| {
                    format!("path {path}: {} has no item {segment}", shown(&walked))
                })?
            }
        };
        walked.push(segment);
    }
    Ok(current.get().as_bytes())
}

/// The part of the path walked so far, for error messages.
fn shown(walked: &[&str]) -> String {
    match walked.is_empty() {
        true => "the document".to_owned(),
        false => walked.join("."),
    }
}

/// A JSON error as `line N: reason`, counting lines from `first_line`.
fn located(error: &serde_json::Error, first_line: u64) -> String {
    let at = first_line + (error.line() as u64).saturating_sub(1);
    let text = error.to_string();
    let reason = text
        .rsplit_once(" at line ")
        .map_or(text.as_str(), |(reason, _)| reason);
    format!("line {at}: {reason}")
}
