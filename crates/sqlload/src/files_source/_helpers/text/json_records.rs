use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
use serde_json::Value;

/// One JSON object from a file: its keys and values in file order, and the line it starts on.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JsonRecord {
    pub(crate) line: u64,
    pub(crate) fields: Vec<(String, Value)>,
}

struct OrderedObject(Vec<(String, Value)>);

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
        let mut fields: Vec<(String, Value)> = Vec::new();
        while let Some(field) = map.next_entry::<String, Value>()? {
            fields.push(field);
        }
        Ok(OrderedObject(fields))
    }
}

/// Where a `json` file is between records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// Nothing but whitespace read yet.
    Unknown,
    /// Objects one after another.
    Objects,
    /// Inside a top-level array, before an element or a `,`.
    Array { after_element: bool },
    /// After the top-level array's `]`.
    Closed,
}

/// Splits a JSON file into objects as its bytes arrive: one per line (`ndjson`), or objects one after another or in one top-level array (`json`).
#[derive(Debug)]
pub(crate) struct JsonSplitter {
    by_line: bool,
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
    pub(crate) fn new(by_line: bool) -> Self {
        Self {
            by_line,
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
            (0, Layout::Array { .. }) => Err(format!(
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
            let line = std::mem::take(&mut self.record);
            self.line_record(&line)?;
            self.line += 1;
            rest = &rest[end + 1..];
        }
        self.record.extend_from_slice(rest);
        Ok(())
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
            self.track(byte);
            if self.depth == 0 {
                let record = std::mem::take(&mut self.record);
                self.completed
                    .push(parse_record(&record, self.record_line)?);
                self.layout = match self.layout {
                    Layout::Array { .. } => Layout::Array {
                        after_element: true,
                    },
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
                self.layout = Layout::Array {
                    after_element: false,
                };
                Ok(())
            }
            (
                Layout::Array {
                    after_element: true,
                },
                b',',
            ) => {
                self.layout = Layout::Array {
                    after_element: false,
                };
                Ok(())
            }
            (Layout::Array { .. }, b']') => {
                self.layout = Layout::Closed;
                Ok(())
            }
            (
                Layout::Array {
                    after_element: true,
                },
                _,
            ) => unexpected("between array elements; expected , or ]"),
            (Layout::Closed, _) => unexpected("after the top-level array"),
            (Layout::Unknown | Layout::Objects | Layout::Array { .. }, b'{') => {
                self.layout = match self.layout {
                    Layout::Unknown => Layout::Objects,
                    other => other,
                };
                self.record_line = self.line;
                self.record.push(byte);
                self.depth = 1;
                Ok(())
            }
            (Layout::Unknown | Layout::Objects | Layout::Array { .. }, _) => {
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
