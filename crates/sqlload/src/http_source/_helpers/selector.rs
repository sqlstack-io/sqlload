//! Reading `select` and paginator pointers: `$`, `$.a.b`, `$.a[*].b` for keys, and `$.a[*].b[*]` for rows inside lists.

use serde_json::Value;

use crate::http_source::models::Selector;
use crate::row_batches::main::item_position::item_position;

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
    let (rows, after) = match rest.split_once("[*]") {
        Some((rows, after)) => (rows, Some(after)),
        None => (rest, None),
    };
    let rows = segments(rows).ok_or_else(invalid)?;
    let (field, within) = match after.map(|after| (after, after.strip_prefix('.'))) {
        None => (None, Vec::new()),
        Some((_, Some(name))) if is_name(name) => (Some(name.to_owned()), Vec::new()),
        Some((_, Some(path))) => (None, within_segments(path).ok_or_else(invalid)?),
        Some((_, None)) => return Err(invalid()),
    };
    Ok(Selector {
        rows,
        field,
        within,
    })
}

/// The path after a select's first `[*]` (its leading dot gone), as segments with `[*]` for each list it goes through.
fn within_segments(text: &str) -> Option<Vec<String>> {
    let mut found: Vec<String> = Vec::new();
    for part in text.split('.') {
        let (mut name, mut lists) = (part, 0);
        while let Some(rest) = name.strip_suffix("[*]") {
            (name, lists) = (rest, lists + 1);
        }
        found.extend(indexed(name)?);
        found.extend(std::iter::repeat_n("[*]".to_owned(), lists));
    }
    Some(found)
}

/// The rows `selector` reaches in `document` through its lists, each with the objects its path leaves by a field name, nearest first, so the document is last.
pub(crate) fn rows_with_parents<'a>(
    document: &'a Value,
    selector: &Selector,
) -> Vec<(&'a Value, Vec<&'a Value>)> {
    let mut path: Vec<&str> = selector.rows.iter().map(String::as_str).collect();
    if !selector.within.is_empty() {
        path.push("[*]");
        path.extend(selector.within.iter().map(String::as_str));
    }
    walk(document, &path, Vec::new())
}

/// The rows at `path` under `node`, each with the objects around it, `parents` (nearest first) those above `node`.
fn walk<'a>(
    node: &'a Value,
    path: &[&str],
    parents: Vec<&'a Value>,
) -> Vec<(&'a Value, Vec<&'a Value>)> {
    match path.split_first() {
        None => match node {
            Value::Array(items) => items.iter().map(|item| (item, parents.clone())).collect(),
            Value::Null => Vec::new(),
            other => vec![(other, parents)],
        },
        Some((&"[*]", rest)) => {
            let mut found: Vec<(&'a Value, Vec<&'a Value>)> = Vec::new();
            for item in node.as_array().into_iter().flatten() {
                found.extend(walk(item, rest, parents.clone()));
            }
            found
        }
        Some((segment, rest)) => {
            let mut parents = parents;
            if !segment.starts_with('[') {
                parents.insert(0, node);
            }
            match step(node, segment) {
                Some(next) => walk(next, rest, parents),
                None => Vec::new(),
            }
        }
    }
}

/// How many objects are around each row `selector` reaches, for `sl.parent`: one per field name its path steps by.
pub(crate) fn enclosing_levels(selector: &Selector) -> usize {
    selector
        .rows
        .iter()
        .chain(&selector.within)
        .filter(|segment| !segment.starts_with('['))
        .count()
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

/// The value under `node` at `segment`: an object's field, or a list's item for `[n]`, or for `[-n]` counting from the end.
pub(crate) fn step<'a>(node: &'a Value, segment: &str) -> Option<&'a Value> {
    match segment.starts_with('[') {
        true => {
            let items = node.as_array()?;
            items.get(item_position(segment, items.len())?)
        }
        false => node.get(segment),
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
