use serde::Deserialize;
use serde::de::{DeserializeSeed, Deserializer, Error as _, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;

use crate::row_batches::_helpers::cells::number_cell;
use crate::row_batches::_helpers::projection_seed::projected_values;
use crate::row_batches::models::{Cell, Projection, RowColumns};

/// Collects parsed rows and hands them to `on_rows` every `batch_rows` rows.
pub(crate) struct RowSink<F: FnMut(RowColumns) -> Result<(), String>> {
    columns: RowColumns,
    batch_rows: usize,
    on_rows: F,
    /// Which values of each object load, when not all of them.
    projection: Option<Projection>,
    /// Rows read so far, for errors naming a row.
    read: u64,
}

impl<F: FnMut(RowColumns) -> Result<(), String>> RowSink<F> {
    pub(crate) fn new(batch_rows: usize, projection: Option<Projection>, on_rows: F) -> Self {
        Self {
            columns: RowColumns::new(),
            batch_rows: batch_rows.max(1),
            on_rows,
            projection,
            read: 0,
        }
    }

    /// Hand on the rows gathered since the last full batch.
    pub(crate) fn finish(mut self) -> Result<(), String> {
        match self.columns.rows() {
            0 => Ok(()),
            _ => (self.on_rows)(std::mem::take(&mut self.columns)),
        }
    }

    fn end_row(&mut self) -> Result<(), String> {
        self.read += 1;
        self.columns.end_row();
        match self.columns.rows() >= self.batch_rows {
            true => (self.on_rows)(std::mem::take(&mut self.columns)),
            false => Ok(()),
        }
    }
}

/// Reads one JSON object as a row.
pub(crate) struct ObjectSeed<'s, F: FnMut(RowColumns) -> Result<(), String>>(
    pub(crate) &'s mut RowSink<F>,
);

impl<'de, F: FnMut(RowColumns) -> Result<(), String>> DeserializeSeed<'de> for ObjectSeed<'_, F> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        match self.0.projection.is_some() {
            true => {
                let raw = <&'de RawValue>::deserialize(deserializer)?;
                self.0.project(raw).map_err(D::Error::custom)
            }
            false => deserializer.deserialize_map(self),
        }
    }
}

impl<F: FnMut(RowColumns) -> Result<(), String>> RowSink<F> {
    /// Push the projected values of the row object `raw`: each field's value, NULL when missing, and the whole object for `keep_record`.
    fn project(&mut self, raw: &RawValue) -> Result<(), String> {
        let row = self.read + 1;
        let Self {
            projection: Some(projection),
            columns,
            ..
        } = self
        else {
            return Ok(());
        };
        let values = projected_values(raw, &projection.tree, projection.fields.len())?;
        for (field, value) in projection.fields.iter().zip(values) {
            let cell = match value {
                Some(value) => raw_cell(value.get()).map_err(|error| error.to_string())?,
                None => Cell::Null,
            };
            if field.required && cell == Cell::Null {
                return Err(format!(
                    "row {row}: field {} needs a value at {}, and the row has none",
                    field.name, field.text
                ));
            }
            columns.push(&field.name, cell);
        }
        if let Some(kept) = &projection.keep_record {
            columns.push(kept, Cell::Json(compact(raw.get())));
        }
        self.end_row()
    }
}

impl<'de, F: FnMut(RowColumns) -> Result<(), String>> Visitor<'de> for ObjectSeed<'_, F> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key::<Key<'de>>()? {
            let raw: &'de RawValue = map.next_value()?;
            let cell = raw_cell(raw.get()).map_err(A::Error::custom)?;
            self.0.columns.push(key.as_str(), cell);
        }
        self.0.end_row().map_err(A::Error::custom)
    }
}

/// Reads a JSON array of objects as rows.
pub(crate) struct ArraySeed<'s, F: FnMut(RowColumns) -> Result<(), String>>(
    pub(crate) &'s mut RowSink<F>,
);

impl<'de, F: FnMut(RowColumns) -> Result<(), String>> DeserializeSeed<'de> for ArraySeed<'_, F> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de, F: FnMut(RowColumns) -> Result<(), String>> Visitor<'de> for ArraySeed<'_, F> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON array of objects")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq.next_element_seed(ObjectSeed(&mut *self.0))?.is_some() {}
        Ok(())
    }
}

/// An object key, borrowed from the input unless it has escapes.
pub(crate) enum Key<'de> {
    Borrowed(&'de str),
    Owned(String),
}

impl Key<'_> {
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Key::Borrowed(text) => text,
            Key::Owned(text) => text,
        }
    }
}

impl<'de> Deserialize<'de> for Key<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(KeyVisitor)
    }
}

struct KeyVisitor;

impl<'de> Visitor<'de> for KeyVisitor {
    type Value = Key<'de>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an object key")
    }

    fn visit_borrowed_str<E: serde::de::Error>(self, text: &'de str) -> Result<Key<'de>, E> {
        Ok(Key::Borrowed(text))
    }

    fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Key<'de>, E> {
        Ok(Key::Owned(text.to_owned()))
    }
}

/// The cell for one raw JSON value; objects and lists become compact JSON text, keys in source order.
pub(crate) fn raw_cell(text: &str) -> Result<Cell, serde_json::Error> {
    match text.as_bytes().first() {
        Some(b'n') => Ok(Cell::Null),
        Some(b't') => Ok(Cell::Bool(true)),
        Some(b'f') => Ok(Cell::Bool(false)),
        Some(b'"') if !text.contains('\\') => Ok(Cell::Text(text[1..text.len() - 1].to_owned())),
        Some(b'"') => serde_json::from_str::<String>(text).map(Cell::Text),
        Some(b'{' | b'[') => Ok(Cell::Json(compact(text))),
        _ => Ok(number_cell(text)),
    }
}

/// Valid JSON `text` without the whitespace between tokens.
fn compact(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let (mut in_string, mut escaped, mut start) = (false, false, 0);
    for (position, byte) in text.bytes().enumerate() {
        match (in_string, escaped, byte) {
            (true, true, _) => escaped = false,
            (true, false, b'\\') => escaped = true,
            (_, _, b'"') => in_string = !in_string,
            (false, _, b' ' | b'\t' | b'\n' | b'\r') => {
                out.push_str(&text[start..position]);
                start = position + 1;
            }
            _ => {}
        }
    }
    out.push_str(&text[start..]);
    out
}
