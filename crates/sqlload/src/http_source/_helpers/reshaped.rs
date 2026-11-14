//! Rows rebuilt before they load, for a `select` through more than one list, `sl.parent`, `sl.key` and `key_path`: each row wrapped with the values it takes from around it.

use serde_json::{Map, Value};

use crate::http_source::_helpers::selector::{rows_with_parents, selector, step};
use crate::http_source::models::{KeyPath, Request, RowContext, Selector};
use crate::row_batches::main::projection::projection;
use crate::row_batches::models::{BadValue, FieldFrom, FieldSpec, Projection};

/// The wrapper key holding the row itself.
const ROW_KEY: &str = "\u{0}row";

/// The most sent keys an error lists.
const SHOWN_SENT: usize = 5;

/// Whether `request`'s rows (at `rows`) are rebuilt before they load: its select goes through more than one list, a field reads from around the row, or `key_path` checks each record.
pub(crate) fn reshapes(request: &Request, rows: &Selector) -> bool {
    !rows.within.is_empty()
        || request.key_path.is_some()
        || request
            .fields
            .iter()
            .flatten()
            .any(|field| field.from != FieldFrom::Row)
}

/// The projection of `request`'s fields over wrapped rows: a row field's path under the row, a parent or key field at its own wrapper key, and `keep_record` as a field holding the row; each field's errors still show its path as written.
pub(crate) fn wrapped_projection(request: &Request) -> Result<Option<Projection>, String> {
    let Some(fields) = request.fields.as_deref() else {
        return Ok(None);
    };
    let mut wrapped: Vec<FieldSpec> = Vec::new();
    for (index, field) in fields.iter().enumerate() {
        let path = match field.from {
            FieldFrom::Row => format!(
                "$.{ROW_KEY}{}",
                field.path.strip_prefix('$').unwrap_or(&field.path)
            ),
            FieldFrom::Parent { .. } | FieldFrom::Key { .. } => format!("$.{}", wrapper_key(index)),
        };
        wrapped.push(FieldSpec {
            path,
            from: FieldFrom::Row,
            ..field.clone()
        });
    }
    if let Some(kept) = &request.keep_record {
        wrapped.push(FieldSpec {
            name: kept.clone(),
            path: format!("$.{ROW_KEY}"),
            required: false,
            parse: None,
            on_error: BadValue::Fail,
            from: FieldFrom::Row,
        });
    }
    let mut projected = projection(Some(&wrapped), None)?;
    if let Some(built) = &mut projected {
        for (column, field) in built.fields.iter_mut().zip(fields) {
            column.text = field.path.clone();
        }
    }
    Ok(projected)
}

/// The rows `rows` selects in `document`, as a JSON array to load: each checked against the keys `context` says its request sent, when `request` has a `key_path`, then wrapped with the values its parent and key fields read (their paths parsed in `fields`, `request`'s own projection), or left as they are when the request names no fields.
pub(crate) fn wrapped_rows(
    document: &Value,
    rows: &Selector,
    (request, fields): (&Request, Option<&Projection>),
    context: &RowContext,
) -> Result<String, String> {
    let specs = request.fields.as_deref().unwrap_or_default();
    let key_path = match &request.key_path {
        Some(key_path) => Some((key_path, selector(Some(&key_path.path))?.rows)),
        None => None,
    };
    let mut loaded: Vec<Value> = Vec::new();
    for (row, parents) in rows_with_parents(document, rows) {
        if let (Some((key_path, segments)), Some(sent)) = (&key_path, &context.sent) {
            checked_key(
                read_from(row, &parents, &key_path.from, segments),
                key_path,
                sent,
            )?;
        }
        let Some(fields) = fields else {
            loaded.push(row.clone());
            continue;
        };
        let mut wrapped = Map::new();
        wrapped.insert(ROW_KEY.to_owned(), row.clone());
        for (index, (spec, field)) in specs.iter().zip(&fields.fields).enumerate() {
            let value = match &spec.from {
                FieldFrom::Row => continue,
                FieldFrom::Parent { .. } => read_from(row, &parents, &spec.from, &field.path),
                FieldFrom::Key { name } => context.key.get(name),
            };
            wrapped.insert(wrapper_key(index), value.cloned().unwrap_or(Value::Null));
        }
        loaded.push(Value::Object(wrapped));
    }
    Ok(Value::Array(loaded).to_string())
}

fn wrapper_key(index: usize) -> String {
    format!("\u{0}field{index}")
}

/// The value at `segments` from the row, or from the object `from` names around it.
fn read_from<'a>(
    row: &'a Value,
    parents: &[&'a Value],
    from: &FieldFrom,
    segments: &[String],
) -> Option<&'a Value> {
    let start = match from {
        FieldFrom::Row => row,
        FieldFrom::Parent { levels } => parents.get(levels.checked_sub(1)?)?,
        FieldFrom::Key { .. } => return None,
    };
    segments
        .iter()
        .try_fold(start, |node, segment| step(node, segment))
}

/// Fails unless the record's key `value` is one of the keys its request `sent`, naming both.
fn checked_key(value: Option<&Value>, key_path: &KeyPath, sent: &[String]) -> Result<(), String> {
    let text = match value {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(number)) => number.to_string(),
        Some(_) | None => {
            return Err(format!(
                "a record has no key at key_path {}, so it can't be checked against the keys its request sent",
                key_path.path
            ));
        }
    };
    if sent.contains(&text) {
        return Ok(());
    }
    let mut listed = sent
        .iter()
        .take(SHOWN_SENT)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if sent.len() > SHOWN_SENT {
        listed.push_str(&format!(", … ({} keys)", sent.len()));
    }
    Err(format!(
        "a record's key {text} at key_path {} is not among the keys its request sent ({listed}), so the API answered for another key",
        key_path.path
    ))
}
