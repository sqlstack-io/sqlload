use serde_json::Value;

const MESSAGE_PREVIEW: usize = 300;

/// The service code and message from an error body, falling back to the start of the raw text.
pub(crate) fn error_fields(body: &str) -> (String, String) {
    let parsed: Value = match serde_json::from_str(body) {
        Ok(value) => value,
        Err(_) => Value::Null,
    };
    let code = field(&parsed, "code")
        .or_else(|| field(&parsed, "error_code"))
        .unwrap_or_default();
    let message =
        field(&parsed, "message").unwrap_or_else(|| body.chars().take(MESSAGE_PREVIEW).collect());
    (code, message)
}

fn field(value: &Value, name: &str) -> Option<String> {
    let found = value.get(name)?;
    Some(match found.as_str() {
        Some(text) => text.to_owned(),
        None => found.to_string(),
    })
}
