use serde_json::{Map, Number, Value as Json};
use toml::Value as Toml;

/// A JSON value from Python as TOML; JSON `null` has no TOML form and is an error naming `key`.
pub(crate) fn toml_value(key: &str, value: &Json) -> Result<Toml, String> {
    match value {
        Json::Null => Err(format!("{key} is None; leave the setting out instead")),
        Json::Bool(flag) => Ok(Toml::Boolean(*flag)),
        Json::Number(number) => match (number.as_i64(), number.as_f64()) {
            (Some(integer), _) => Ok(Toml::Integer(integer)),
            (None, Some(float)) => Ok(Toml::Float(float)),
            (None, None) => Err(format!("{key} = {number} is out of range")),
        },
        Json::String(text) => Ok(Toml::String(text.clone())),
        Json::Array(items) => items
            .iter()
            .map(|item| toml_value(key, item))
            .collect::<Result<Vec<Toml>, String>>()
            .map(Toml::Array),
        Json::Object(entries) => entries
            .iter()
            .map(|(name, item)| Ok((name.clone(), toml_value(&format!("{key}.{name}"), item)?)))
            .collect::<Result<toml::Table, String>>()
            .map(Toml::Table),
    }
}

/// A TOML value as JSON, dates and times as their TOML text.
pub(crate) fn json_value(value: &Toml) -> Json {
    match value {
        Toml::String(text) => Json::String(text.clone()),
        Toml::Integer(integer) => Json::from(*integer),
        Toml::Float(float) => Number::from_f64(*float).map_or(Json::Null, Json::Number),
        Toml::Boolean(flag) => Json::Bool(*flag),
        Toml::Datetime(datetime) => Json::String(datetime.to_string()),
        Toml::Array(items) => Json::Array(items.iter().map(json_value).collect()),
        Toml::Table(entries) => Json::Object(
            entries
                .iter()
                .map(|(name, item)| (name.clone(), json_value(item)))
                .collect::<Map<String, Json>>(),
        ),
    }
}
