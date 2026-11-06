//! Request URLs: paths under the base URL, fan-out placeholders, `Link` headers and redaction.

use reqwest::Url;
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
        .filter_map(|part| part.split_once('}').map(|(name, _)| name.to_owned()))
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

fn encoded(text: &str) -> String {
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
    header.split(',').find_map(|link| {
        let (target, parameters) = link.split_once(';')?;
        let next = parameters
            .split(';')
            .any(|parameter| parameter.trim().replace(' ', "") == "rel=\"next\"");
        next.then(|| {
            target
                .trim()
                .trim_start_matches('<')
                .trim_end_matches('>')
                .to_owned()
        })
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
