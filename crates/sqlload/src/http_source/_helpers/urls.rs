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
