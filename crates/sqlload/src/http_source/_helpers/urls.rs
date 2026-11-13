//! Request URLs: paths under the base URL, fan-out placeholders, `Link` headers and redaction.

use reqwest::Url;

use crate::http_source::constants::{DOT_SEGMENTS, NEXT_RELATION, RELATION_PARAMETER};
use serde_json::{Map, Value};

/// `path` under `base` (`https://h/v1` + `/orders` is `https://h/v1/orders`), or `path` itself when it is a full URL.
pub(crate) fn request_url(base: &Url, path: &str) -> Result<Url, String> {
    let text = match path.starts_with("http://") || path.starts_with("https://") {
        true => path.to_owned(),
        false => format!(
            "{}/{}",
            base.as_str().trim_end_matches('/'),
            path.trim_start_matches('/')
        ),
    };
    Url::parse(&text).map_err(|error| format!("{path} is not a valid URL: {error}"))
}

/// Whether credentials meant for `base` may go to `url`: same scheme, host and port.
pub(crate) fn same_origin(base: &Url, url: &Url) -> bool {
    base.origin() == url.origin()
}

/// The `{name}` placeholders in `path`, in order.
pub(crate) fn placeholders(path: &str) -> Vec<String> {
    path.split('{')
        .skip(1)
        .filter_map(|part| part.split_once('}'))
        .map(|(name, _)| name.to_owned())
        .collect()
}

/// `path` with each placeholder replaced by its key's value, percent-encoded.
pub(crate) fn filled_path(path: &str, key: &Map<String, Value>) -> Result<String, String> {
    placeholders(path)
        .iter()
        .try_fold(path.to_owned(), |filled, name| {
            let value = match key.get(name) {
                Some(Value::String(text)) => text.clone(),
                Some(Value::Null) | None => {
                    return Err(format!(
                        "fan-out path {path} needs {name}, which the key {} lacks",
                        Value::Object(key.clone())
                    ));
                }
                Some(other) => other.to_string(),
            };
            Ok(filled.replace(&format!("{{{name}}}"), &encoded(&value)))
        })
}

/// The `{name}` placeholders in `body`'s string values, each once, in order; only names of letters, digits and `_` count, so other braces (a GraphQL query's) stay text.
pub(crate) fn body_placeholders(body: &Value) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let mut pending: Vec<&Value> = vec![body];
    while let Some(value) = pending.pop() {
        match value {
            Value::String(text) => {
                for name in text_placeholders(text) {
                    if !names.contains(&name) {
                        names.push(name);
                    }
                }
            }
            Value::Array(items) => pending.extend(items.iter().rev()),
            Value::Object(fields) => pending.extend(fields.values().rev()),
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
    names
}

/// The placeholders of one string value of a body.
fn text_placeholders(text: &str) -> Vec<String> {
    let mut names = placeholders(text);
    names.retain(|name| {
        name.starts_with(|first: char| first.is_ascii_alphabetic() || first == '_')
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    });
    names
}

/// `body` with its placeholders filled from `keys`: a string that is just `{name}` becomes the key's value as it is (a number stays a number), or, when `batched`, the list of every key's value in order; a placeholder inside other text becomes the value's text, which a batch cannot fill.
pub(crate) fn filled_body(
    body: &Value,
    keys: &[&Map<String, Value>],
    batched: bool,
) -> Result<Value, String> {
    match body {
        Value::String(text) => filled_text(text, keys, batched),
        Value::Array(items) => {
            let mut filled: Vec<Value> = Vec::with_capacity(items.len());
            for item in items {
                filled.push(filled_body(item, keys, batched)?);
            }
            Ok(Value::Array(filled))
        }
        Value::Object(fields) => {
            let mut filled = Map::new();
            for (name, value) in fields {
                filled.insert(name.clone(), filled_body(value, keys, batched)?);
            }
            Ok(Value::Object(filled))
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => Ok(body.clone()),
    }
}

/// One string value of a body, filled as [`filled_body`] says.
fn filled_text(text: &str, keys: &[&Map<String, Value>], batched: bool) -> Result<Value, String> {
    let names = text_placeholders(text);
    let Some(first) = names.first() else {
        return Ok(Value::String(text.to_owned()));
    };
    if names.len() == 1 && text == format!("{{{first}}}") {
        let mut values: Vec<Value> = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(key_value(key, first)?);
        }
        return Ok(match (batched, values.pop()) {
            (false, Some(value)) => value,
            (true, Some(last)) => {
                values.push(last);
                Value::Array(values)
            }
            (_, None) => Value::Array(Vec::new()),
        });
    }
    if batched {
        return Err(format!(
            "json_body value {text:?} holds a placeholder within other text; a batched fan-out fills only a value that is just the placeholder, such as \"{{id}}\", with the list of its keys"
        ));
    }
    let mut filled = text.to_owned();
    for name in &names {
        let value = match keys.first() {
            Some(key) => key_value(key, name)?,
            None => Value::Null,
        };
        let shown = match value {
            Value::String(value) => value,
            other => other.to_string(),
        };
        filled = filled.replace(&format!("{{{name}}}"), &shown);
    }
    Ok(Value::String(filled))
}

/// The value `key` gives the body placeholder `name`; a missing or NULL one is an error naming both.
fn key_value(key: &Map<String, Value>, name: &str) -> Result<Value, String> {
    match key.get(name) {
        Some(Value::Null) | None => Err(format!(
            "json_body needs {name}, which the key {} lacks",
            Value::Object(key.clone())
        )),
        Some(value) => Ok(value.clone()),
    }
}

/// `text` percent-encoded except for unreserved characters; `.` and `..` are encoded whole so a key never climbs the path.
pub(crate) fn encoded(text: &str) -> String {
    if DOT_SEGMENTS.contains(&text) {
        return text.replace('.', "%2E");
    }
    text.bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(byte).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

/// The `rel="next"` URL of a `Link` header.
pub(crate) fn link_header_next(header: &str) -> Option<String> {
    header.split('<').skip(1).find_map(next_target)
}

/// The target of one `Link` entry (`target>; rel="next"`, the `<` already removed) when one of its relations is `next`.
fn next_target(link: &str) -> Option<String> {
    let (target, parameters) = link.split_once('>')?;
    let next = parameters.split([';', ',']).any(is_next_relation);
    next.then(|| target.trim().to_owned())
}

/// Whether a `Link` parameter is `rel` with `next` among its space-separated relations, quoted or not.
fn is_next_relation(parameter: &str) -> bool {
    let relations = parameter
        .split_once('=')
        .filter(|(key, _)| key.trim().eq_ignore_ascii_case(RELATION_PARAMETER))
        .map(|(_, value)| value.trim().trim_matches('"'));
    relations.is_some_and(|relations| {
        relations
            .split_whitespace()
            .any(|relation| relation.eq_ignore_ascii_case(NEXT_RELATION))
    })
}

/// `text` with every secret replaced by `***`.
pub(crate) fn redacted(text: &str, secrets: &[String]) -> String {
    secrets
        .iter()
        .filter(|secret| !secret.is_empty())
        .fold(text.to_owned(), |text, secret| {
            text.replace(secret.as_str(), "***")
        })
}
