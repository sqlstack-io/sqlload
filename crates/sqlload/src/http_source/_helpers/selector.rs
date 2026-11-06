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
        _ => text
            .strip_prefix('.')?
            .split('.')
            .map(|name| is_name(name).then(|| name.to_owned()))
            .collect(),
    }
}

fn is_name(text: &str) -> bool {
    !text.is_empty() && !text.contains(['.', '[', ']', '*', '$'])
}

/// The rows `selector` points at in `document`: an array's items, one object, or none.
pub(crate) fn selected_rows<'a>(document: &'a Value, selector: &Selector) -> Vec<&'a Value> {
    let node = selector
        .rows
        .iter()
        .try_fold(document, |node, name| node.get(name));
    match node {
        Some(Value::Array(items)) => items.iter().collect(),
        Some(Value::Null) | None => Vec::new(),
        Some(other) => vec![other],
    }
}

/// The text at `pointer` in `document` (a number as its digits); `None` when missing, null or empty.
pub(crate) fn pointer_text(document: &Value, pointer: &Selector) -> Option<String> {
    let node = pointer
        .rows
        .iter()
        .try_fold(document, |node, name| node.get(name))?;
    match node {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}
