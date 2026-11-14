//! Reading a row object once for a projection: only the keys on its paths are kept, and each object or indexed list on them is parsed once.

use serde::de::{DeserializeSeed, Deserializer, Error as _, IgnoredAny, MapAccess, Visitor};
use serde_json::de::StrRead;
use serde_json::value::RawValue;

use crate::row_batches::_helpers::json_seed::Key;
use crate::row_batches::main::item_position::item_position;
use crate::row_batches::models::PathNode;

/// Each field's value in the row object `raw`, by position, `None` where its path finds nothing; `fields` is how many fields `tree` holds.
pub(crate) fn projected_values<'de>(
    raw: &'de RawValue,
    tree: &PathNode,
    fields: usize,
) -> Result<Vec<Option<&'de RawValue>>, String> {
    if !raw.get().starts_with('{') {
        return Err("expected a JSON object".to_owned());
    }
    let mut values: Vec<Option<&'de RawValue>> = vec![None; fields];
    for index in &tree.ends {
        values[*index] = Some(raw);
    }
    if !tree.children.is_empty() {
        walk(raw, tree, &mut values)?;
    }
    Ok(values)
}

/// Visit the object `raw` once, keeping the values of `node`'s keys and walking down into those with keys below them.
fn walk<'de>(
    raw: &'de RawValue,
    node: &PathNode,
    values: &mut [Option<&'de RawValue>],
) -> Result<(), String> {
    let mut deserializer: serde_json::Deserializer<StrRead<'de>> =
        serde_json::Deserializer::from_str(raw.get());
    NodeSeed { node, values }
        .deserialize(&mut deserializer)
        .map_err(|error| reason(&error))
}

/// A JSON error's text without its position, which refers to the inner object rather than the input.
fn reason(error: &serde_json::Error) -> String {
    let text = error.to_string();
    text.rsplit_once(" at line ")
        .map_or(text.clone(), |(reason, _)| reason.to_owned())
}

struct NodeSeed<'n, 'v, 'de> {
    node: &'n PathNode,
    values: &'v mut [Option<&'de RawValue>],
}

impl<'de> DeserializeSeed<'de> for NodeSeed<'_, '_, 'de> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for NodeSeed<'_, '_, 'de> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key::<Key<'de>>()? {
            let child = Some(key.as_str())
                .filter(|key| !key.starts_with('['))
                .and_then(|key| self.node.children.get(key));
            let Some(child) = child else {
                map.next_value::<IgnoredAny>()?;
                continue;
            };
            let value: &'de RawValue = map.next_value()?;
            self.take(child, value).map_err(A::Error::custom)?;
        }
        Ok(())
    }
}

impl<'de> NodeSeed<'_, '_, 'de> {
    /// Record `value` for the fields ending at `child`, after clearing what an earlier copy of the same key left (the last copy wins), then walk into it when it is an object, or into the items its indexes name when it is a list.
    fn take(&mut self, child: &PathNode, value: &'de RawValue) -> Result<(), String> {
        for index in &child.under {
            self.values[*index] = None;
        }
        for index in &child.ends {
            self.values[*index] = Some(value);
        }
        if child.children.is_empty() {
            return Ok(());
        }
        match value.get().as_bytes().first() {
            Some(b'{') => walk(value, child, self.values),
            Some(b'[') if child.children.keys().any(|key| key.starts_with('[')) => {
                let items: Vec<&'de RawValue> =
                    serde_json::from_str(value.get()).map_err(|error| reason(&error))?;
                for (segment, below) in &child.children {
                    let item = item_position(segment, items.len())
                        .and_then(|position| items.get(position));
                    if let Some(item) = item {
                        self.take(below, item)?;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
