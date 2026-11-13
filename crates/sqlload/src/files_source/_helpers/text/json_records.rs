use std::collections::BTreeSet;

use serde::de::{Deserialize, Deserializer, Error as _, MapAccess, Visitor};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

use crate::files_source::_helpers::text::bytes_reader::too_long;
use crate::row_batches::constants::FLATTEN_SEPARATOR;

/// One JSON object from a file: its keys and values in file order, and the line it starts on; a nested object or array is an empty placeholder value with its compact source text (keys in file order) beside it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JsonRecord {
    pub(crate) line: u64,
    pub(crate) fields: Vec<(String, Value, Option<String>)>,
}

struct OrderedObject(Vec<(String, Value, Option<String>)>);

impl<'de> Deserialize<'de> for OrderedObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(ObjectVisitor)
    }
}

struct ObjectVisitor;

impl<'de> Visitor<'de> for ObjectVisitor {
    type Value = OrderedObject;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut fields: Vec<(String, Value, Option<String>)> = Vec::new();
        while let Some((key, raw)) = map.next_entry::<String, Box<RawValue>>()? {
            let text = raw.get();
            let field = match text.as_bytes().first() {
                Some(b'{') => (
                    key,
                    Value::Object(Map::new()),
                    Some(nested_text(text).map_err(A::Error::custom)?),
                ),
                Some(b'[') => (
                    key,
                    Value::Array(Vec::new()),
                    Some(nested_text(text).map_err(A::Error::custom)?),
                ),
                _ => (
                    key,
                    serde_json::from_str::<Value>(text).map_err(A::Error::custom)?,
                    None,
                ),
            };
            fields.push(field);
        }
        Ok(OrderedObject(last_values(fields)))
    }
}

/// One field of a JSON record: its key, its value (an empty placeholder for an object or array) and the nested value's compact text.
type JsonField = (String, Value, Option<String>);

/// `record` with each field holding an object replaced where it stands, and recursively, by one field per key named `<field>__<key>`, as dlt flattens objects (an empty object, or a NULL inside an object, leaves none); with each name flattening made and each field it replaced, and those replaced fields alone (the objects: a key holding `__` is not one). A made name the record already has is an error naming it.
pub(crate) fn flattened_record(
    record: JsonRecord,
) -> Result<(JsonRecord, BTreeSet<String>, BTreeSet<String>), String> {
    let mut fields: Vec<JsonField> = Vec::with_capacity(record.fields.len());
    let mut flattened: BTreeSet<String> = BTreeSet::new();
    let mut objects: BTreeSet<String> = BTreeSet::new();
    for field in record.fields {
        flatten_field(field, "", (&mut fields, &mut flattened, &mut objects))?;
    }
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    if let Some((name, _, _)) = fields
        .iter()
        .find(|(name, _, _)| !seen.insert(name.as_str()))
    {
        return Err(format!(
            "line {}: naming = \"dlt\" flattens an object into the column {name}, which the record already has as a key; rename one of them",
            record.line
        ));
    }
    Ok((
        JsonRecord {
            line: record.line,
            fields,
        },
        flattened,
        objects,
    ))
}

/// Push `field`, named below `prefix` when an object holds it, onto `fields`, or its keys in its place when it holds an object (a NULL below a prefix leaves nothing); the object's name and names below a prefix join `flattened`, and the object's name `objects`.
fn flatten_field(
    (key, value, nested): JsonField,
    prefix: &str,
    (fields, flattened, objects): (
        &mut Vec<JsonField>,
        &mut BTreeSet<String>,
        &mut BTreeSet<String>,
    ),
) -> Result<(), String> {
    let name = match prefix.is_empty() {
        true => key,
        false => format!("{prefix}{FLATTEN_SEPARATOR}{key}"),
    };
    if let (Value::Object(_), Some(text)) = (&value, &nested) {
        let object: OrderedObject =
            serde_json::from_str(text).map_err(|error| format!("{name}: {error}"))?;
        flattened.insert(name.clone());
        objects.insert(name.clone());
        for inner in object.0 {
            flatten_field(inner, &name, (fields, flattened, objects))?;
        }
        return Ok(());
    }
    if !prefix.is_empty() {
        if value.is_null() {
            return Ok(());
        }
        flattened.insert(name.clone());
    }
    fields.push((name, value, nested));
    Ok(())
}

/// `fields` with only the last value of a key given more than once, as Snowflake's JSON reader keeps it, so both routes read and check the same value.
fn last_values(
    fields: Vec<(String, Value, Option<String>)>,
) -> Vec<(String, Value, Option<String>)> {
    let mut keys: Vec<&str> = fields.iter().map(|(key, _, _)| key.as_str()).collect();
    keys.sort_unstable();
    if !keys.windows(2).any(|pair| pair[0] == pair[1]) {
        return fields;
    }
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut kept: Vec<(String, Value, Option<String>)> = fields
        .into_iter()
        .rev()
        .filter(|(key, _, _)| seen.insert(key.clone()))
        .collect();
    kept.reverse();
    kept
}

/// Where a `json` file is between records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// Nothing but whitespace read yet.
    Unknown,
    /// Objects one after another.
    Objects,
    /// Just inside a top-level array's `[`.
    ArrayStart,
    /// Inside the array, after an element.
    ArrayElement,
    /// Inside the array, after a `,`.
    ArrayComma,
    /// After the top-level array's `]`.
    Closed,
}

/// Splits a JSON file into objects as its bytes arrive: one per line (`ndjson`), or objects one after another or in one top-level array (`json`).
#[derive(Debug)]
pub(crate) struct JsonSplitter {
    by_line: bool,
    max_record_bytes: usize,
    layout: Layout,
    record: Vec<u8>,
    record_line: u64,
    line: u64,
    depth: usize,
    in_string: bool,
    escaped: bool,
    /// Objects completed since the last `push`.
    completed: Vec<JsonRecord>,
}

impl JsonSplitter {
    /// `by_line` reads one object per line; an object longer than `max_record_bytes` is an error.
    pub(crate) fn new(by_line: bool, max_record_bytes: usize) -> Self {
        Self {
            by_line,
            max_record_bytes,
            layout: Layout::Unknown,
            record: Vec::new(),
            record_line: 1,
            line: 1,
            depth: 0,
            in_string: false,
            escaped: false,
            completed: Vec::new(),
        }
    }

    /// The objects completed by `chunk`.
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Result<Vec<JsonRecord>, String> {
        match self.by_line {
            true => self.push_lines(chunk)?,
            false => chunk.iter().try_for_each(|byte| self.push_byte(*byte))?,
        }
        Ok(std::mem::take(&mut self.completed))
    }

    /// The last object, at the end of the file; an unfinished object or array is an error.
    pub(crate) fn finish(&mut self) -> Result<Vec<JsonRecord>, String> {
        if self.by_line {
            let line = std::mem::take(&mut self.record);
            self.line_record(&line)?;
            return Ok(std::mem::take(&mut self.completed));
        }
        match (self.depth, self.layout) {
            (0, Layout::Unknown | Layout::Objects | Layout::Closed) => Ok(Vec::new()),
            (0, Layout::ArrayStart | Layout::ArrayElement | Layout::ArrayComma) => Err(format!(
                "line {}: the file ends before the top-level array's closing ]",
                self.line
            )),
            _ => Err(format!(
                "line {}: the file ends inside the object that starts here",
                self.record_line
            )),
        }
    }

    fn push_lines(&mut self, chunk: &[u8]) -> Result<(), String> {
        let mut rest = chunk;
        while let Some(end) = rest.iter().position(|byte| *byte == b'\n') {
            self.record.extend_from_slice(&rest[..end]);
            self.check_length(self.line)?;
            let line = std::mem::take(&mut self.record);
            self.line_record(&line)?;
            self.line += 1;
            rest = &rest[end + 1..];
        }
        self.record.extend_from_slice(rest);
        self.check_length(self.line)
    }

    /// An error once the record being read is longer than the limit.
    fn check_length(&self, line: u64) -> Result<(), String> {
        match self.record.len() > self.max_record_bytes {
            true => Err(too_long(line, self.max_record_bytes)),
            false => Ok(()),
        }
    }

    fn line_record(&mut self, line: &[u8]) -> Result<(), String> {
        if !line.trim_ascii().is_empty() {
            self.completed.push(parse_record(line, self.line)?);
        }
        Ok(())
    }

    fn push_byte(&mut self, byte: u8) -> Result<(), String> {
        if self.depth > 0 {
            self.record.push(byte);
            self.check_length(self.record_line)?;
            self.track(byte);
            if self.depth == 0 {
                let record = std::mem::take(&mut self.record);
                self.completed
                    .push(parse_record(&record, self.record_line)?);
                self.layout = match self.layout {
                    Layout::ArrayStart | Layout::ArrayComma => Layout::ArrayElement,
                    other => other,
                };
            }
        } else {
            self.between_records(byte)?;
        }
        if byte == b'\n' {
            self.line += 1;
        }
        Ok(())
    }

    /// Nesting and strings inside an object, so braces in strings do not count.
    fn track(&mut self, byte: u8) {
        match (self.in_string, self.escaped, byte) {
            (true, true, _) => self.escaped = false,
            (true, false, b'\\') => self.escaped = true,
            (true, false, b'"') => self.in_string = false,
            (true, false, _) => {}
            (false, _, b'"') => self.in_string = true,
            (false, _, b'{' | b'[') => self.depth += 1,
            (false, _, b'}' | b']') => self.depth -= 1,
            (false, _, _) => {}
        }
    }

    fn between_records(&mut self, byte: u8) -> Result<(), String> {
        let unexpected = |what: &str| {
            Err(format!(
                "line {}: unexpected {:?} {what}",
                self.line,
                char::from(byte)
            ))
        };
        match (self.layout, byte) {
            (_, b' ' | b'\t' | b'\r' | b'\n') => Ok(()),
            (Layout::Unknown, b'[') => {
                self.layout = Layout::ArrayStart;
                Ok(())
            }
            (Layout::ArrayElement, b',') => {
                self.layout = Layout::ArrayComma;
                Ok(())
            }
            (Layout::ArrayStart | Layout::ArrayElement, b']') => {
                self.layout = Layout::Closed;
                Ok(())
            }
            (Layout::ArrayComma, b']') => unexpected("after a trailing comma; expected an object"),
            (Layout::ArrayElement, _) => unexpected("between array elements; expected , or ]"),
            (Layout::Closed, _) => unexpected("after the top-level array"),
            (Layout::Unknown | Layout::Objects | Layout::ArrayStart | Layout::ArrayComma, b'{') => {
                self.layout = match self.layout {
                    Layout::Unknown => Layout::Objects,
                    other => other,
                };
                self.record_line = self.line;
                self.record.push(byte);
                self.depth = 1;
                Ok(())
            }
            (Layout::Unknown | Layout::Objects | Layout::ArrayStart | Layout::ArrayComma, _) => {
                unexpected("where a JSON object should start")
            }
        }
    }
}

/// The object in `bytes`, which starts on `line`; anything but one object is an error naming the line.
fn parse_record(bytes: &[u8], line: u64) -> Result<JsonRecord, String> {
    serde_json::from_slice::<OrderedObject>(bytes)
        .map(|object| JsonRecord {
            line,
            fields: object.0,
        })
        .map_err(|error| {
            let at = line + (error.line() as u64).saturating_sub(1);
            let text = error.to_string();
            let reason = text
                .rsplit_once(" at line ")
                .map_or(text.as_str(), |(reason, _)| reason);
            format!("line {at}: {reason}")
        })
}

/// The keys and raw values of one JSON object, in file order.
struct RawEntries(Vec<(String, Box<RawValue>)>);

impl<'de> Deserialize<'de> for RawEntries {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(RawEntriesVisitor)
    }
}

struct RawEntriesVisitor;

impl<'de> Visitor<'de> for RawEntriesVisitor {
    type Value = RawEntries;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut entries: Vec<(String, Box<RawValue>)> = Vec::new();
        while let Some(entry) = map.next_entry::<String, Box<RawValue>>()? {
            entries.push(entry);
        }
        Ok(RawEntries(entries))
    }
}

/// A nested object or array as compact JSON text: keys in file order, a key repeated in one object keeping its last value (in the place of its first), as a JSON parser reads it.
fn nested_text(text: &str) -> Result<String, serde_json::Error> {
    match text.as_bytes().first() {
        Some(b'{') => {
            let RawEntries(entries) = serde_json::from_str(text)?;
            let mut unique: Vec<(String, Box<RawValue>)> = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                match unique.iter_mut().find(|(known, _)| *known == key) {
                    Some(entry) => entry.1 = value,
                    None => unique.push((key, value)),
                }
            }
            let fields = unique
                .iter()
                .map(|(key, value)| {
                    Ok(format!(
                        "{}:{}",
                        serde_json::to_string(key)?,
                        nested_text(value.get())?
                    ))
                })
                .collect::<Result<Vec<String>, serde_json::Error>>()?;
            Ok(format!("{{{}}}", fields.join(",")))
        }
        Some(b'[') => {
            let items: Vec<Box<RawValue>> = serde_json::from_str(text)?;
            let items = items
                .iter()
                .map(|item| nested_text(item.get()))
                .collect::<Result<Vec<String>, serde_json::Error>>()?;
            Ok(format!("[{}]", items.join(",")))
        }
        _ => Ok(text.trim().to_owned()),
    }
}
