//! A field path, such as `$.items[0].id`: the keys and list indexes it reads, or why it can't be read.

use crate::row_batches::_helpers::projected::list_index;
use crate::row_batches::constants::{PROJECTION_ROOT, PROJECTION_SEPARATOR};

/// `$.a.b[0]` as its segments: each key as written, and each list index as `[n]`, or `[-n]` counting from the end; `$` alone is the whole object. Otherwise why the path can't be read, after `"path" `, naming what it uses that field paths don't support.
pub fn field_path(text: &str) -> Result<Vec<String>, String> {
    let invalid = || {
        format!("{text:?} is not a path such as \"$.id\", \"$.address.city\" or \"$.items[0].id\"")
    };
    let unsupported = |what: String| {
        let hint = match what.starts_with("the wildcard") {
            true => {
                "which one column can't hold; take one item with [n] or [-1], or select the list's items as rows"
            }
            false => {
                "which field paths don't support; they take dotted names and [n] or [-1] indexes, such as \"$.items[0].id\""
            }
        };
        format!("{text:?} uses {what}, {hint}")
    };
    let mut rest = text.strip_prefix(PROJECTION_ROOT).ok_or_else(invalid)?;
    let mut segments: Vec<String> = Vec::new();
    while !rest.is_empty() {
        if rest.starts_with("..") {
            return Err(unsupported("recursive descent (..)".to_owned()));
        }
        if let Some(after) = rest.strip_prefix(PROJECTION_SEPARATOR) {
            let end = after
                .find([PROJECTION_SEPARATOR, '['])
                .unwrap_or(after.len());
            let key = &after[..end];
            match key {
                "*" => return Err(unsupported("the wildcard .*".to_owned())),
                "" => return Err(invalid()),
                _ if key.contains(']') => return Err(invalid()),
                _ => segments.push(key.to_owned()),
            }
            rest = &after[end..];
            continue;
        }
        let Some((inside, after)) = rest.strip_prefix('[').and_then(|r| r.split_once(']')) else {
            return Err(invalid());
        };
        let written = format!("[{inside}]");
        match inside {
            "*" => return Err(unsupported(format!("the wildcard {written}"))),
            _ if inside.starts_with('?') => {
                return Err(unsupported(format!("the filter {written}")));
            }
            _ if inside.starts_with(['\'', '"']) => {
                return Err(unsupported(format!("the bracketed key {written}")));
            }
            _ if inside.contains(':') => return Err(unsupported(format!("the slice {written}"))),
            _ => {}
        }
        let Some((from_end, number)) = list_index(inside).filter(|_| !segments.is_empty()) else {
            return Err(invalid());
        };
        segments.push(match from_end && number > 0 {
            true => format!("[-{number}]"),
            false => format!("[{number}]"),
        });
        rest = after;
    }
    Ok(segments)
}
