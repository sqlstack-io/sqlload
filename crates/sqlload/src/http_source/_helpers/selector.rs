//! Reading `select` and paginator pointers: `$`, `$.a.b`, and `$.a[*].b` for keys.

use serde_json::Value;

use crate::http_source::models::Selector;

/// `text` as a selector; `None` and `$` select the whole response.
pub(crate) fn selector(text: Option<&str>) -> Result<Selector, String> {
    let Some(text) = text else {
        return Ok(Selector::default());
    };
    let invalid = || {
        format!(
            "select {text:?} is not a path such as \"$\", \"$.items\" or \"$.items[*].id\"; use dotted field names after \"$\""
        )
    };
    let rest = text.strip_prefix('$').ok_or_else(invalid)?;
    let (rows, field) = match rest.split_once("[*]") {
        Some((rows, field)) => (rows, Some(field.strip_prefix('.').ok_or_else(invalid)?)),
        None => (rest, None),
    };
    let rows = segments(rows).ok_or_else(invalid)?;
    let field = match field {
        Some(field) if is_name(field) => Some(field.to_owned()),
        Some(_) => return Err(invalid()),
        None => None,
    };
    Ok(Selector { rows, field })
}

fn segments(text: &str) -> Option<Vec<String>> {
    match text {
        "" => Some(Vec::new()),
        _ => Some(
            text.strip_prefix('.')?
                .split('.')
                .map(indexed)
                .collect::<Option<Vec<Vec<String>>>>()?
                .concat(),
        ),
    }
}

/// A dotted path's part such as `sheets` or `sheets[0]` as its segments, an index kept as `[0]`; `None` when it is neither.
fn indexed(part: &str) -> Option<Vec<String>> {
    let (name, indexes) = part
        .split_once('[')
        .map_or((part, ""), |(name, rest)| (name, rest));
    let mut found = vec![is_name(name).then(|| name.to_owned())?];
    if !indexes.is_empty() {
        for index in format!("[{indexes}").split_inclusive(']') {
            let digits = index.strip_prefix('[')?.strip_suffix(']')?;
            if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            found.push(index.to_owned());
        }
    }
    Some(found)
}

/// The value under `node` at `segment`: an object's field, or a list's item for `[n]`.
pub(crate) fn step<'a>(node: &'a Value, segment: &str) -> Option<&'a Value> {
    match segment
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    {
        Some(index) => match index.parse::<usize>() {
            Ok(index) => node.as_array()?.get(index),
            Err(_) => None,
        },
        None => node.get(segment),
    }
}

fn is_name(text: &str) -> bool {
    !text.is_empty() && !text.contains(['.', '[', ']', '*', '$'])
}

/// The node `selector` points at in `document`, if the path exists.
pub(crate) fn selected_node<'a>(document: &'a Value, selector: &Selector) -> Option<&'a Value> {
    selector
        .rows
        .iter()
        .try_fold(document, |node, name| step(node, name))
}

/// Whether `selector`'s path is missing from `document`: a field or item on the way is absent, rather than the path ending at a value or passing a null.
pub(crate) fn missing_path(document: &Value, selector: &Selector) -> bool {
    let mut node = document;
    for name in &selector.rows {
        if node.is_null() {
            return false;
        }
        match step(node, name) {
            Some(next) => node = next,
            None => return true,
        }
    }
    false
}

/// The rows `selector` points at in `document`: an array's items, one object, or none.
pub(crate) fn selected_rows<'a>(document: &'a Value, selector: &Selector) -> Vec<&'a Value> {
    match selected_node(document, selector) {
        Some(Value::Array(items)) => items.iter().collect(),
        Some(Value::Null) | None => Vec::new(),
        Some(other) => vec![other],
    }
}

/// The value at `pointer` in `document` when it is a non-empty string or a number; `None` when missing, null, empty or another kind.
pub(crate) fn pointer_value(document: &Value, pointer: &Selector) -> Option<Value> {
    let node = pointer
        .rows
        .iter()
        .try_fold(document, |node, name| step(node, name))?;
    match node {
        Value::String(text) if !text.is_empty() => Some(node.clone()),
        Value::Number(_) => Some(node.clone()),
        _ => None,
    }
}

/// The text at `pointer` in `document` (a number as its digits); `None` when missing, null or empty.
pub(crate) fn pointer_text(document: &Value, pointer: &Selector) -> Option<String> {
    match pointer_value(document, pointer)? {
        Value::String(text) => Some(text),
        other => Some(other.to_string()),
    }
}
